from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import aiomysql

from core.database import get_attendance_db_connection


def _build_academic_where(filters: List[Dict[str, str]], table_alias: Optional[str] = None) -> Tuple[str, List[str]]:
    clean_filters = []
    for item in filters or []:
        campus = str(item.get("campus") or "").strip()
        nivel = str(item.get("nivel") or "").strip()
        if campus and nivel:
            clean_filters.append((campus, nivel))

    if not clean_filters:
        return "1 = 0", []

    prefix = f"{table_alias}." if table_alias else ""
    clauses: List[str] = []
    params: List[str] = []
    for campus, nivel in clean_filters:
        clauses.append(
            f"(LOWER(TRIM({prefix}campus)) = LOWER(%s) "
            f"AND LOWER(TRIM({prefix}nivel)) = LOWER(%s))"
        )
        params.extend([campus, nivel])
    return "(" + " OR ".join(clauses) + ")", params


def _reviewed_clause(alias: str = "p") -> str:
    prefix = f"{alias}." if alias else ""
    return (
        f"(CHAR_LENGTH(IFNULL(TRIM({prefix}revisa), '')) > 0 "
        f"OR CHAR_LENGTH(IFNULL(TRIM({prefix}revisa2), '')) > 0 "
        f"OR CHAR_LENGTH(IFNULL(TRIM({prefix}revisa3), '')) > 0 "
        f"OR CHAR_LENGTH(IFNULL(TRIM({prefix}feedback), '')) > 0 "
        f"OR CHAR_LENGTH(IFNULL(TRIM({prefix}feedback2), '')) > 0 "
        f"OR CHAR_LENGTH(IFNULL(TRIM({prefix}feedback3), '')) > 0)"
    )


async def count_active_academic_teachers(academic_filters: List[Dict[str, str]]) -> int:
    where_clause, params = _build_academic_where(academic_filters, "u")
    query = f"""
        SELECT COUNT(DISTINCT u.username) AS total
        FROM usuarios u
        WHERE {where_clause}
          AND u.username IS NOT NULL
          AND TRIM(u.username) <> ''
          AND COALESCE(u.coordinador, 0) = 0
          AND COALESCE(u.banned, 0) = 0
          AND (u.ISSSTE IS NULL OR u.ISSSTE = 0)
    """
    conn = await get_attendance_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, tuple(params))
            row = await cur.fetchone()
            return int((row or {}).get("total") or 0)
    finally:
        conn.close()


async def fetch_planning_review_totals(academic_filters: List[Dict[str, str]], start_date: date, end_date: date) -> Dict[str, Any]:
    user_where, user_params = _build_academic_where(academic_filters, "u")
    reviewed = _reviewed_clause("p")
    query = f"""
        SELECT
            COUNT(DISTINCT CONCAT_WS('|', p.docente, p.week, IFNULL(p.ciclo, ''))) AS submitted_units,
            COUNT(DISTINCT CASE WHEN {reviewed}
                THEN CONCAT_WS('|', p.docente, p.week, IFNULL(p.ciclo, '')) END) AS reviewed_units,
            COUNT(DISTINCT CASE WHEN NOT {reviewed}
                THEN CONCAT_WS('|', p.docente, p.week, IFNULL(p.ciclo, '')) END) AS pending_units,
            COUNT(DISTINCT p.docente) AS docentes_con_planeacion
        FROM planeaciones p
        JOIN usuarios u ON u.username = p.docente
        WHERE {user_where}
          AND p.created_at >= %s
          AND p.created_at < DATE_ADD(%s, INTERVAL 1 DAY)
          AND p.flagged = 0
          AND p.week IS NOT NULL
          AND p.docente IS NOT NULL
          AND TRIM(p.docente) <> ''
          AND COALESCE(u.coordinador, 0) = 0
          AND COALESCE(u.banned, 0) = 0
          AND (u.ISSSTE IS NULL OR u.ISSSTE = 0)
    """
    conn = await get_attendance_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, (*user_params, start_date, end_date))
            return await cur.fetchone() or {}
    finally:
        conn.close()


async def fetch_observation_teacher_totals(
    academic_filters: List[Dict[str, str]],
    observation_start: date,
    observation_end: date,
) -> Dict[str, Any]:
    user_where, user_params = _build_academic_where(academic_filters, "u")
    query = f"""
        SELECT
            COUNT(DISTINCT u.username) AS active_teachers,
            COUNT(DISTINCT obs.docente) AS observed_teachers,
            COUNT(obs.id) AS total_observations
        FROM usuarios u
        LEFT JOIN observaciones_form_submissions obs
               ON obs.docente = u.username
              AND obs.submission_date >= %s
              AND obs.submission_date < DATE_ADD(%s, INTERVAL 1 DAY)
        WHERE {user_where}
          AND u.username IS NOT NULL
          AND TRIM(u.username) <> ''
          AND COALESCE(u.coordinador, 0) = 0
          AND COALESCE(u.banned, 0) = 0
          AND (u.ISSSTE IS NULL OR u.ISSSTE = 0)
    """
    conn = await get_attendance_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, (observation_start, observation_end, *user_params))
            return await cur.fetchone() or {}
    finally:
        conn.close()



def _normalize_values(values: List[str] | str) -> List[str]:
    if isinstance(values, str):
        raw = [values]
    else:
        raw = list(values or [])
    out: List[str] = []
    seen = set()
    for value in raw:
        clean = " ".join(str(value or "").strip().upper().split())
        if clean and clean not in seen:
            seen.add(clean)
            out.append(clean)
    return out or [""]


def _text_alias_clause(column_expr: str, values: List[str] | str) -> Tuple[str, List[str]]:
    """Build an index-friendly exact-match alias clause.

    The corporate report must not scan large attendance/access tables with
    functions over the plantel column. Known source aliases are matched exactly;
    unresolved source shapes stay unavailable and are exposed in diagnostic.
    """
    raw_values = [values] if isinstance(values, str) else list(values or [])
    aliases: List[str] = []
    seen = set()
    for value in raw_values:
        clean = str(value or "").strip()
        if not clean:
            continue
        variants = [clean, " ".join(clean.upper().split())]
        for variant in variants:
            if variant and variant not in seen:
                seen.add(variant)
                aliases.append(variant)

    if not aliases:
        aliases = [""]
    placeholders = ", ".join(["%s" for _ in aliases])
    return f"{column_expr} IN ({placeholders})", aliases


async def fetch_attendance_rollup(plantel_values: List[str] | str, start_date: date, end_date: date) -> List[Dict[str, Any]]:
    """Lightweight attendance aggregate for the executive report.

    This intentionally avoids the detail endpoint's absent-student and gender joins.
    It returns one row per date/grade/group with enough data to compute roll-call
    and student-attendance scores.
    """
    plantel_clause, plantel_params = _text_alias_clause("A.plantel", plantel_values)
    query = f"""
        SELECT
            DATE(A.fecha) AS d_fecha,
            COUNT(*) AS records,
            SUM(CASE WHEN A.attendance = 1 THEN 1 ELSE 0 END) AS present,
            SUM(CASE WHEN A.attendance = 0 THEN 1 ELSE 0 END) AS absent,
            COUNT(DISTINCT CONCAT_WS('|', TRIM(A.grado), TRIM(A.grupo))) AS completed_lists
        FROM asistencia A
        WHERE {plantel_clause}
          AND A.fecha >= %s
          AND A.fecha < DATE_ADD(%s, INTERVAL 1 DAY)
          AND A.grado IS NOT NULL
          AND A.grupo IS NOT NULL
        GROUP BY DATE(A.fecha)
        ORDER BY DATE(A.fecha)
    """
    conn = await get_attendance_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, (*plantel_params, start_date, end_date))
            return await cur.fetchall()
    finally:
        conn.close()


async def fetch_husky_daily_scan_counts(plantel_values: List[str] | str, start_date: date, end_date: date) -> List[Dict[str, Any]]:
    from core.database import get_husky_db_connection

    direct_clause, direct_params = _text_alias_clause("B.plantel", plantel_values)
    chain_clause, chain_params = _text_alias_clause("B.plantel", plantel_values)
    query = f"""
        SELECT
            DATE(src.timestamp) AS fecha,
            LOWER(TRIM(src.tipo_accion)) AS tipo_accion,
            COUNT(DISTINCT src.user_id) AS total_scans
        FROM (
            SELECT A.timestamp, A.type AS tipo_accion, B.id AS user_id
            FROM acceso A
            JOIN users B ON B.id = A.ss_id
            WHERE {direct_clause}
              AND A.timestamp >= %s
              AND A.timestamp < DATE_ADD(%s, INTERVAL 1 DAY)
              AND A.timestamp IS NOT NULL
              AND A.type IS NOT NULL

            UNION ALL

            SELECT A.timestamp, A.type AS tipo_accion, B.id AS user_id
            FROM acceso A
            JOIN personas_autorizadas pa ON pa.id = A.ss_id
            JOIN users B ON pa.user_id = B.id
            WHERE {chain_clause}
              AND A.timestamp >= %s
              AND A.timestamp < DATE_ADD(%s, INTERVAL 1 DAY)
              AND A.timestamp IS NOT NULL
              AND A.type IS NOT NULL
        ) src
        WHERE src.user_id IS NOT NULL
          AND LOWER(TRIM(src.tipo_accion)) IN ('entrada', 'salida')
        GROUP BY DATE(src.timestamp), LOWER(TRIM(src.tipo_accion))
        ORDER BY DATE(src.timestamp)
    """
    conn = await get_husky_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, (*direct_params, start_date, end_date, *chain_params, start_date, end_date))
            return await cur.fetchall()
    finally:
        conn.close()


async def fetch_husky_tardy_daily_counts(plantel_values: List[str] | str, start_date: date, end_date: date, threshold_time: str) -> List[Dict[str, Any]]:
    from core.database import get_husky_db_connection

    direct_clause, direct_params = _text_alias_clause("B.plantel", plantel_values)
    chain_clause, chain_params = _text_alias_clause("B.plantel", plantel_values)
    query = f"""
        SELECT first_scan.scan_date AS date, COUNT(*) AS tardies
        FROM (
            SELECT DATE(src.timestamp) AS scan_date, src.user_id, MIN(src.timestamp) AS first_timestamp
            FROM (
                SELECT A.timestamp, B.id AS user_id
                FROM acceso A
                JOIN users B ON B.id = A.ss_id
                WHERE {direct_clause}
                  AND A.timestamp >= %s
              AND A.timestamp < DATE_ADD(%s, INTERVAL 1 DAY)
                  AND A.timestamp IS NOT NULL
                  AND LOWER(TRIM(A.type)) = 'entrada'

                UNION ALL

                SELECT A.timestamp, B.id AS user_id
                FROM acceso A
                JOIN personas_autorizadas pa ON pa.id = A.ss_id
                JOIN users B ON pa.user_id = B.id
                WHERE {chain_clause}
                  AND A.timestamp >= %s
              AND A.timestamp < DATE_ADD(%s, INTERVAL 1 DAY)
                  AND A.timestamp IS NOT NULL
                  AND LOWER(TRIM(A.type)) = 'entrada'
            ) src
            WHERE src.user_id IS NOT NULL
            GROUP BY DATE(src.timestamp), src.user_id
            HAVING TIME(MIN(src.timestamp)) > %s
        ) first_scan
        GROUP BY first_scan.scan_date
        ORDER BY first_scan.scan_date
    """
    conn = await get_husky_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, (*direct_params, start_date, end_date, *chain_params, start_date, end_date, threshold_time))
            return await cur.fetchall()
    finally:
        conn.close()


async def fetch_academic_filter_shape(academic_filters: List[Dict[str, str]], start_date: date, end_date: date) -> Dict[str, Any]:
    """Small audit counts to validate whether campus/nivel filters match source rows."""
    if not academic_filters:
        return {}
    first = academic_filters[0] or {}
    campus = str(first.get("campus") or "").strip()
    nivel = str(first.get("nivel") or "").strip()
    if not campus or not nivel:
        return {}
    query = """
        SELECT
            (SELECT COUNT(*) FROM observaciones_form_submissions obs
              WHERE LOWER(TRIM(obs.campus)) = LOWER(%s)
                AND LOWER(TRIM(obs.nivel)) = LOWER(%s)
                AND obs.submission_date >= %s
                AND obs.submission_date < DATE_ADD(%s, INTERVAL 1 DAY)) AS obs_exact,
            (SELECT COUNT(*) FROM observaciones_form_submissions obs
              WHERE LOWER(TRIM(obs.campus)) = LOWER(%s)
                AND obs.submission_date >= %s
                AND obs.submission_date < DATE_ADD(%s, INTERVAL 1 DAY)) AS obs_campus,
            (SELECT COUNT(*) FROM observaciones_form_submissions obs
              WHERE LOWER(TRIM(obs.nivel)) = LOWER(%s)
                AND obs.submission_date >= %s
                AND obs.submission_date < DATE_ADD(%s, INTERVAL 1 DAY)) AS obs_nivel,
            (SELECT COUNT(*) FROM planeaciones p
              WHERE LOWER(TRIM(p.campus)) = LOWER(%s)
                AND LOWER(TRIM(p.nivel)) = LOWER(%s)
                AND p.created_at >= %s
                AND p.created_at < DATE_ADD(%s, INTERVAL 1 DAY)
                AND p.flagged = 0) AS plan_exact,
            (SELECT COUNT(*) FROM planeaciones p
              WHERE LOWER(TRIM(p.campus)) = LOWER(%s)
                AND p.created_at >= %s
                AND p.created_at < DATE_ADD(%s, INTERVAL 1 DAY)
                AND p.flagged = 0) AS plan_campus,
            (SELECT COUNT(*) FROM planeaciones p
              WHERE LOWER(TRIM(p.nivel)) = LOWER(%s)
                AND p.created_at >= %s
                AND p.created_at < DATE_ADD(%s, INTERVAL 1 DAY)
                AND p.flagged = 0) AS plan_nivel
    """
    params = (
        campus, nivel, start_date, end_date,
        campus, start_date, end_date,
        nivel, start_date, end_date,
        campus, nivel, start_date, end_date,
        campus, start_date, end_date,
        nivel, start_date, end_date,
    )
    conn = await get_attendance_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, params)
            row = await cur.fetchone() or {}
            return {"campus": campus, "nivel": nivel, **{k: int(row.get(k) or 0) for k in row}}
    finally:
        conn.close()
