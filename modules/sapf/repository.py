import aiomysql
from datetime import date
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from core.database import get_attendance_db_connection


CAMPUS_COLUMNS = ("school_code", "campus", "plantel", "escuela")
DATE_COLUMNS = ("fecha", "created_at", "updated_at")
DEPARTMENT_COLUMNS = ("department_email", "target_department", "original_department", "area", "departamento")
MOTIVE_COLUMNS = ("reason", "motivo", "resolution", "initial_action", "descripcion", "observaciones")
STATUS_OPEN_VALUES = ("0", "abierto", "open", "pendiente", "seguimiento")
STATUS_CLOSED_VALUES = ("1", "cerrado", "closed", "resuelto", "resolved")


def _dedupe(values: Iterable[Any]) -> List[str]:
    seen = set()
    out: List[str] = []
    for value in values or []:
        clean = str(value or "").strip()
        if not clean:
            continue
        key = clean.upper()
        if key in seen:
            continue
        seen.add(key)
        out.append(clean)
    return out


def _normalize_sql_values(values: Iterable[Any]) -> List[str]:
    return _dedupe(str(value or "").strip().upper() for value in values or [])


def _placeholders(count: int) -> str:
    return ", ".join(["%s"] * count)


async def _has_table(cur: aiomysql.DictCursor, table_name: str) -> bool:
    await cur.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = DATABASE()
          AND table_name = %s
        LIMIT 1
        """,
        (table_name,),
    )
    return bool(await cur.fetchone())


async def _get_columns(cur: aiomysql.DictCursor, table_name: str) -> set:
    if not await _has_table(cur, table_name):
        return set()
    await cur.execute(f"SHOW COLUMNS FROM `{table_name}`")
    return {str(row.get("Field") or "") for row in await cur.fetchall()}


def _first_existing(columns: set, candidates: Sequence[str]) -> Optional[str]:
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return None


def _nullif_expr(alias: str, column: str) -> str:
    return f"NULLIF(TRIM({alias}.`{column}`), '')"


def _coalesce_expr(alias: str, columns: set, candidates: Sequence[str], fallback: str) -> str:
    parts = [_nullif_expr(alias, column) for column in candidates if column in columns]
    if not parts:
        return f"'{fallback}'"
    return f"COALESCE({', '.join(parts)}, '{fallback}')"


def _date_expr(alias: str, columns: set) -> Optional[str]:
    column = _first_existing(columns, DATE_COLUMNS)
    return f"{alias}.`{column}`" if column else None


def _normalized_campus_clause(alias: str, columns: set, campus_values: Iterable[str]) -> Tuple[str, List[str]]:
    candidates = [column for column in CAMPUS_COLUMNS if column in columns]
    values = _normalize_sql_values(campus_values)
    if not candidates or not values:
        return "1=1", []

    per_column = []
    params: List[str] = []
    placeholders = _placeholders(len(values))
    for column in candidates:
        per_column.append(f"TRIM(UPPER({alias}.`{column}`)) IN ({placeholders})")
        params.extend(values)
    return f"({' OR '.join(per_column)})", params


def _deptos_join_and_expr(
    table_alias: str,
    table_columns: set,
    has_deptos_map: bool,
    map_campus_values: Iterable[str],
) -> Tuple[str, str, List[str]]:
    base_expr = _coalesce_expr(table_alias, table_columns, DEPARTMENT_COLUMNS, "Sin Departamento")
    if not has_deptos_map or "department_email" not in table_columns:
        return "", base_expr, []

    map_values = _normalize_sql_values(map_campus_values)
    campus_clause = "1=1"
    params: List[str] = []
    if map_values:
        campus_clause = f"TRIM(UPPER(dm.`campus`)) IN ({_placeholders(len(map_values))})"
        params.extend(map_values)

    join = (
        "LEFT JOIN deptos_map dm "
        f"ON LOWER(TRIM(dm.`email`)) = LOWER(TRIM({table_alias}.`department_email`)) "
        f"AND {campus_clause}"
    )
    expr = f"COALESCE(NULLIF(TRIM(dm.`department_name`), ''), {base_expr})"
    return join, expr, params


async def _fetch_monthly_for_table(
    cur: aiomysql.DictCursor,
    table_name: str,
    table_alias: str,
    campus_values: Iterable[str],
    map_campus_values: Iterable[str],
    start_date: date,
    end_date: date,
    source_label: str,
    has_deptos_map: bool,
) -> List[Dict[str, Any]]:
    columns = await _get_columns(cur, table_name)
    if not columns:
        return []

    date_expression = _date_expr(table_alias, columns)
    if not date_expression:
        return []

    campus_clause, campus_params = _normalized_campus_clause(table_alias, columns, campus_values)
    join_sql, dept_expr, join_params = _deptos_join_and_expr(table_alias, columns, has_deptos_map, map_campus_values)

    sql = f"""
        SELECT
            {dept_expr} AS department_name,
            YEAR({date_expression}) AS year,
            MONTH({date_expression}) AS month,
            DATE_FORMAT({date_expression}, '%%Y-%%m') AS period,
            %s AS source,
            COUNT(*) AS conteo
        FROM `{table_name}` {table_alias}
        {join_sql}
        WHERE {campus_clause}
          AND {date_expression} >= %s
          AND {date_expression} < DATE_ADD(%s, INTERVAL 1 DAY)
        GROUP BY department_name, year, month, period, source
        ORDER BY department_name ASC, year ASC, month ASC
    """
    params = [source_label, *join_params, *campus_params, start_date, end_date]
    await cur.execute(sql, params)
    return list(await cur.fetchall())


async def get_sapf_monthly_stats(
    map_campus: str,
    data_campuses: List[str],
    start_date: date,
    end_date: date,
) -> list:
    """
    Recupera actividad SAPF de forma tolerante a estructura legacy.

    El SAPF Next.js real filtra plantel de forma normalizada sobre cualquier
    columna tipo campus disponible (school_code/campus/plantel/escuela). Esta
    implementación porta esa semántica y evita asumir que school_code siempre
    existe o que el valor guardado siempre coincide de forma exacta.
    """
    campus_values = _dedupe([map_campus, *data_campuses])
    map_values = _dedupe([map_campus, *data_campuses])
    rows: List[Dict[str, Any]] = []

    conn = await get_attendance_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            has_deptos_map = await _has_table(cur, "deptos_map")
            rows.extend(await _fetch_monthly_for_table(
                cur, "fichas_atencion", "fa", campus_values, map_values,
                start_date, end_date, "ficha", has_deptos_map,
            ))
            rows.extend(await _fetch_monthly_for_table(
                cur, "seguimiento", "se", campus_values, map_values,
                start_date, end_date, "seguimiento", has_deptos_map,
            ))
        return rows
    finally:
        conn.close()


async def _fetch_motivos_for_table(
    cur: aiomysql.DictCursor,
    table_name: str,
    table_alias: str,
    campus_values: Iterable[str],
    map_campus_values: Iterable[str],
    start_date: date,
    end_date: date,
    source_label: str,
    has_deptos_map: bool,
) -> List[Dict[str, Any]]:
    columns = await _get_columns(cur, table_name)
    if not columns:
        return []

    date_expression = _date_expr(table_alias, columns)
    motive_expr = _coalesce_expr(table_alias, columns, MOTIVE_COLUMNS, "Sin Motivo")
    if not date_expression:
        return []

    campus_clause, campus_params = _normalized_campus_clause(table_alias, columns, campus_values)
    join_sql, dept_expr, join_params = _deptos_join_and_expr(table_alias, columns, has_deptos_map, map_campus_values)

    sql = f"""
        SELECT
            {dept_expr} AS department_name,
            {motive_expr} AS motivo,
            %s AS source,
            COUNT(*) AS conteo
        FROM `{table_name}` {table_alias}
        {join_sql}
        WHERE {campus_clause}
          AND {date_expression} >= %s
          AND {date_expression} < DATE_ADD(%s, INTERVAL 1 DAY)
        GROUP BY department_name, motivo, source
        ORDER BY conteo DESC, department_name ASC, motivo ASC
    """
    params = [source_label, *join_params, *campus_params, start_date, end_date]
    await cur.execute(sql, params)
    return list(await cur.fetchall())


async def get_sapf_motivos_stats(
    map_campus: str,
    data_campuses: List[str],
    start_date: date,
    end_date: date,
) -> list:
    """
    Extrae motivos SAPF usando la estructura real: fichas_atencion.reason es el
    motivo principal, con fallback a motivo/resolution cuando una base legacy lo
    usa. También incluye seguimientos para medir presión operativa posterior.
    """
    campus_values = _dedupe([map_campus, *data_campuses])
    map_values = _dedupe([map_campus, *data_campuses])
    rows: List[Dict[str, Any]] = []

    conn = await get_attendance_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            has_deptos_map = await _has_table(cur, "deptos_map")
            rows.extend(await _fetch_motivos_for_table(
                cur, "fichas_atencion", "fa", campus_values, map_values,
                start_date, end_date, "ficha", has_deptos_map,
            ))
            rows.extend(await _fetch_motivos_for_table(
                cur, "seguimiento", "se", campus_values, map_values,
                start_date, end_date, "seguimiento", has_deptos_map,
            ))
        return rows
    finally:
        conn.close()


def _status_case(alias: str, columns: set, values: Sequence[str]) -> str:
    if "status" not in columns:
        return "0"
    placeholders = ", ".join([f"'{v}'" for v in values])
    return f"LOWER(TRIM(CAST({alias}.`status` AS CHAR))) IN ({placeholders})"


def _parent_origin_condition(alias: str, columns: set) -> str:
    parts = []
    if "contact_method" in columns:
        parts.append(f"LOWER(TRIM({alias}.`contact_method`)) IN ('parent','padres','parent-portal','portal-padres','portal_padres')")
    if "created_by" in columns:
        parts.append(f"LOWER({alias}.`created_by`) LIKE '%%parent%%'")
    if "original_department" in columns:
        parts.append(f"LOWER({alias}.`original_department`) LIKE '%%padre%%'")
        parts.append(f"LOWER({alias}.`original_department`) LIKE '%%portal%%'")
    return f"({' OR '.join(parts)})" if parts else "0"


async def _fetch_fichas_overview(
    cur: aiomysql.DictCursor,
    campus_values: Iterable[str],
    map_campus_values: Iterable[str],
    start_date: date,
    end_date: date,
) -> Dict[str, Any]:
    columns = await _get_columns(cur, "fichas_atencion")
    if not columns:
        return {}
    date_expression = _date_expr("fa", columns)
    if not date_expression:
        return {}

    campus_clause, campus_params = _normalized_campus_clause("fa", columns, campus_values)
    has_deptos_map = await _has_table(cur, "deptos_map")
    join_sql, dept_expr, join_params = _deptos_join_and_expr("fa", columns, has_deptos_map, map_campus_values)
    open_case = _status_case("fa", columns, STATUS_OPEN_VALUES)
    closed_case = _status_case("fa", columns, STATUS_CLOSED_VALUES)
    parent_case = _parent_origin_condition("fa", columns)
    complaint_expr = "fa.`is_complaint` = 1" if "is_complaint" in columns else "0"
    resolution_hours = (
        "AVG(CASE WHEN " + closed_case + " THEN TIMESTAMPDIFF(HOUR, fa.`fecha`, COALESCE(fa.`updated_at`, NOW())) END)"
        if "fecha" in columns and "updated_at" in columns and "status" in columns else "NULL"
    )

    sql = f"""
        SELECT
            COUNT(*) AS total_fichas,
            SUM(CASE WHEN {open_case} THEN 1 ELSE 0 END) AS open_cases,
            SUM(CASE WHEN {closed_case} THEN 1 ELSE 0 END) AS closed_cases,
            SUM(CASE WHEN {complaint_expr} THEN 1 ELSE 0 END) AS complaints,
            SUM(CASE WHEN {parent_case} THEN 1 ELSE 0 END) AS parent_origin_cases,
            {resolution_hours} AS avg_resolution_hours
        FROM fichas_atencion fa
        {join_sql}
        WHERE {campus_clause}
          AND {date_expression} >= %s
          AND {date_expression} < DATE_ADD(%s, INTERVAL 1 DAY)
    """
    await cur.execute(sql, [*join_params, *campus_params, start_date, end_date])
    kpi = dict(await cur.fetchone() or {})

    area_sql = f"""
        SELECT {dept_expr} AS area, COUNT(*) AS conteo
        FROM fichas_atencion fa
        {join_sql}
        WHERE {campus_clause}
          AND {date_expression} >= %s
          AND {date_expression} < DATE_ADD(%s, INTERVAL 1 DAY)
        GROUP BY area
        ORDER BY conteo DESC, area ASC
        LIMIT 20
    """
    await cur.execute(area_sql, [*join_params, *campus_params, start_date, end_date])
    kpi["areas_from_fichas"] = list(await cur.fetchall())
    return kpi


async def _fetch_followups_count(
    cur: aiomysql.DictCursor,
    campus_values: Iterable[str],
    start_date: date,
    end_date: date,
) -> Dict[str, Any]:
    columns = await _get_columns(cur, "seguimiento")
    if not columns:
        return {"total_followups": 0}
    date_expression = _date_expr("se", columns)
    if not date_expression:
        return {"total_followups": 0}
    campus_clause, campus_params = _normalized_campus_clause("se", columns, campus_values)

    sql = f"""
        SELECT COUNT(*) AS total_followups
        FROM seguimiento se
        WHERE {campus_clause}
          AND {date_expression} >= %s
          AND {date_expression} < DATE_ADD(%s, INTERVAL 1 DAY)
    """
    await cur.execute(sql, [*campus_params, start_date, end_date])
    return dict(await cur.fetchone() or {"total_followups": 0})


async def get_sapf_overview_stats(
    map_campus: str,
    data_campuses: List[str],
    start_date: date,
    end_date: date,
) -> Dict[str, Any]:
    """Resumen ejecutivo SAPF compatible con la estructura real del Next.js."""
    campus_values = _dedupe([map_campus, *data_campuses])
    map_values = _dedupe([map_campus, *data_campuses])

    conn = await get_attendance_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            fichas = await _fetch_fichas_overview(cur, campus_values, map_values, start_date, end_date)
            followups = await _fetch_followups_count(cur, campus_values, start_date, end_date)
            return {**fichas, **followups, "matched_campus_values": _normalize_sql_values(campus_values)}
    finally:
        conn.close()
