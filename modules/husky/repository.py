import aiomysql
from datetime import date
from typing import Iterable, List, Tuple

from core.database import get_husky_db_connection


def _normalize_codes(db_codes: Iterable[str] | str) -> List[str]:
    if isinstance(db_codes, str):
        raw_codes = [db_codes]
    else:
        raw_codes = list(db_codes or [])

    seen = set()
    codes: List[str] = []
    for code in raw_codes:
        clean = " ".join(str(code or "").strip().upper().split())
        if clean and clean not in seen:
            seen.add(clean)
            codes.append(clean)
    return codes or [""]


def _plantel_like_clause(alias: str, db_codes: Iterable[str] | str) -> Tuple[str, List[str]]:
    """Tolerant Husky campus filter.

    Husky users.plantel has changed between short codes, prefixed codes, and
    school names. Match exact, prefix, and contained aliases so missing aliases
    become unavailable data instead of fake zero activity.
    """
    codes = _normalize_codes(db_codes)
    expr = f"TRIM(UPPER({alias}.plantel))"
    exact = ", ".join(["%s" for _ in codes])
    parts = [f"{expr} IN ({exact})"]
    params: List[str] = list(codes)
    for code in codes:
        parts.append(f"{expr} LIKE %s")
        params.append(f"{code}%")
        parts.append(f"{expr} LIKE %s")
        params.append(f"%{code}%")
    return "(" + " OR ".join(parts) + ")", params


async def get_daily_scans(db_codes: Iterable[str] | str, start_date: date, end_date: date) -> list:
    """Obtain scan statistics from Husky Pass using both identity paths.

    Existing reports historically used a direct acceso.ss_id -> users.id path
    for student tardies, while other scans can use acceso.ss_id ->
    personas_autorizadas.id -> users.id. Unioning both and counting distinct
    users per day/type prevents a single join assumption from flattening the
    access charts to zero.
    """
    direct_clause, direct_params = _plantel_like_clause("B", db_codes)
    chain_clause, chain_params = _plantel_like_clause("B", db_codes)
    query = f"""
        SELECT
            DATE(src.timestamp) AS fecha,
            LOWER(TRIM(src.tipo_accion)) AS tipo_accion,
            COUNT(DISTINCT src.user_id) AS total_scans
        FROM (
            SELECT
                A.timestamp,
                A.type AS tipo_accion,
                B.id AS user_id
            FROM acceso A
            JOIN users B ON B.id = A.ss_id
            WHERE {direct_clause}
              AND DATE(A.timestamp) BETWEEN %s AND %s
              AND A.timestamp IS NOT NULL
              AND A.type IS NOT NULL

            UNION ALL

            SELECT
                A.timestamp,
                A.type AS tipo_accion,
                B.id AS user_id
            FROM acceso A
            JOIN personas_autorizadas pa ON pa.id = A.ss_id
            JOIN users B ON pa.user_id = B.id
            WHERE {chain_clause}
              AND DATE(A.timestamp) BETWEEN %s AND %s
              AND A.timestamp IS NOT NULL
              AND A.type IS NOT NULL
        ) src
        WHERE src.user_id IS NOT NULL
          AND LOWER(TRIM(src.tipo_accion)) IN ('entrada', 'salida')
        GROUP BY DATE(src.timestamp), LOWER(TRIM(src.tipo_accion))
        ORDER BY DATE(src.timestamp) ASC
    """

    conn = await get_husky_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            params = (*direct_params, start_date, end_date, *chain_params, start_date, end_date)
            await cur.execute(query, params)
            return await cur.fetchall()
    finally:
        conn.close()


async def fetch_plantel_retardos(db_codes: Iterable[str] | str, start_date: date, end_date: date, threshold_time: str) -> list:
    """Fetch student tardies using both known Husky identity paths.

    The result is deduplicated to the first entrance per student per day before
    applying the tardy threshold, matching the health-report intent while still
    supporting newer authorized-person scans.
    """
    direct_clause, direct_params = _plantel_like_clause("B", db_codes)
    chain_clause, chain_params = _plantel_like_clause("B", db_codes)
    query = f"""
        SELECT
            MIN(first_scan.id) AS id,
            COALESCE(
                NULLIF(TRIM(CONCAT_WS(' ', MAX(first_scan.nombreA), MAX(first_scan.paternoA), MAX(first_scan.maternoA))), ''),
                NULLIF(TRIM(MAX(first_scan.username)), ''),
                'Desconocido'
            ) AS student_fullname,
            COALESCE(NULLIF(TRIM(MAX(first_scan.username)), ''), 'N/A') AS matricula,
            DATE(MIN(first_scan.timestamp)) AS date,
            TIME(MIN(first_scan.timestamp)) AS time
        FROM (
            SELECT src.*
            FROM (
                SELECT
                    A.id,
                    A.timestamp,
                    B.id AS user_id,
                    B.username,
                    ap.nombreA,
                    ap.paternoA,
                    ap.maternoA
                FROM acceso A
                JOIN users B ON B.id = A.ss_id
                LEFT JOIN alumno_pa ap ON ap.user_id = B.id
                WHERE {direct_clause}
                  AND DATE(A.timestamp) BETWEEN %s AND %s
                  AND A.timestamp IS NOT NULL
                  AND LOWER(TRIM(A.type)) = 'entrada'
                  AND DAYOFWEEK(A.timestamp) NOT IN (1, 7)

                UNION ALL

                SELECT
                    A.id,
                    A.timestamp,
                    B.id AS user_id,
                    B.username,
                    ap.nombreA,
                    ap.paternoA,
                    ap.maternoA
                FROM acceso A
                JOIN personas_autorizadas pa ON pa.id = A.ss_id
                JOIN users B ON pa.user_id = B.id
                LEFT JOIN alumno_pa ap ON ap.user_id = B.id
                WHERE {chain_clause}
                  AND DATE(A.timestamp) BETWEEN %s AND %s
                  AND A.timestamp IS NOT NULL
                  AND LOWER(TRIM(A.type)) = 'entrada'
                  AND DAYOFWEEK(A.timestamp) NOT IN (1, 7)
            ) src
            WHERE src.user_id IS NOT NULL
        ) first_scan
        GROUP BY DATE(first_scan.timestamp), first_scan.user_id
        HAVING TIME(MIN(first_scan.timestamp)) > %s
        ORDER BY MIN(first_scan.timestamp) ASC
    """

    conn = await get_husky_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            params = (*direct_params, start_date, end_date, *chain_params, start_date, end_date, threshold_time)
            await cur.execute(query, params)
            return await cur.fetchall()
    finally:
        conn.close()
