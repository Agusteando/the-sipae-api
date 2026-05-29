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

    Historical Husky records can store `users.plantel` as a short code, a long
    label, or a prefixed label. Exact normalized comparisons keep labels safe;
    LIKE catches legacy strings such as "1 - PT" or PMA/PMB variants.
    """
    codes = _normalize_codes(db_codes)
    expr = f"TRIM(UPPER({alias}.plantel))"
    exact = ", ".join(["%s" for _ in codes])
    like_codes = [code for code in codes if len(code) <= 12]
    like_clause = " OR ".join([f"{expr} LIKE %s" for _ in like_codes])
    clause = f"({expr} IN ({exact})"
    params: List[str] = list(codes)
    if like_clause:
        clause += f" OR {like_clause}"
        params.extend([f"%{code}%" for code in like_codes])
    clause += ")"
    return clause, params


async def get_daily_scans(db_codes: Iterable[str] | str, start_date: date, end_date: date) -> list:
    """Obtain scan statistics from Husky Pass using tolerant plantel aliases."""
    plantel_clause, plantel_params = _plantel_like_clause("B", db_codes)
    query = f"""
        SELECT
            DATE(A.timestamp) as fecha,
            A.type as tipo_accion,
            COUNT(DISTINCT B.id) as total_scans
        FROM acceso A
        LEFT JOIN personas_autorizadas pa ON pa.id = A.ss_id
        LEFT JOIN users B ON pa.user_id = B.id
        WHERE {plantel_clause}
          AND DATE(A.timestamp) BETWEEN %s AND %s
          AND A.timestamp IS NOT NULL
        GROUP BY DATE(A.timestamp), A.type
        ORDER BY DATE(A.timestamp) ASC
    """

    conn = await get_husky_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, (*plantel_params, start_date, end_date))
            return await cur.fetchall()
    finally:
        conn.close()


async def fetch_plantel_retardos(db_codes: Iterable[str] | str, start_date: date, end_date: date, threshold_time: str) -> list:
    """Fetch student tardies using both known Husky identity paths.

    The health report historically got tardies through `alumno_pa.user_id =
    acceso.ss_id`. Some newer scan aggregations use `acceso.ss_id ->
    personas_autorizadas.id -> users.id`. The global dashboard must not depend
    on only one of those shapes, so both are unioned and then deduplicated to the
    first valid entrance per student per day.
    """
    direct_clause, direct_params = _plantel_like_clause("B", db_codes)
    chain_clause, chain_params = _plantel_like_clause("B", db_codes)
    query = f"""
        SELECT
            MIN(src.id) AS id,
            COALESCE(
                NULLIF(TRIM(CONCAT_WS(' ', MAX(src.nombreA), MAX(src.paternoA), MAX(src.maternoA))), ''),
                NULLIF(TRIM(MAX(src.username)), ''),
                'Desconocido'
            ) AS student_fullname,
            COALESCE(NULLIF(TRIM(MAX(src.username)), ''), 'N/A') AS matricula,
            DATE(MIN(src.timestamp)) AS date,
            TIME(MIN(src.timestamp)) AS time
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
            JOIN alumno_pa ap ON ap.user_id = A.ss_id
            JOIN users B ON ap.user_id = B.id
            WHERE {direct_clause}
              AND DATE(A.timestamp) BETWEEN %s AND %s
              AND A.timestamp IS NOT NULL
              AND LOWER(A.type) = 'entrada'
              AND COALESCE(A.suspension_efectiva, 0) = 0
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
            LEFT JOIN personas_autorizadas pa ON pa.id = A.ss_id
            LEFT JOIN users B ON pa.user_id = B.id
            LEFT JOIN alumno_pa ap ON ap.user_id = B.id
            WHERE {chain_clause}
              AND DATE(A.timestamp) BETWEEN %s AND %s
              AND A.timestamp IS NOT NULL
              AND LOWER(A.type) = 'entrada'
              AND COALESCE(A.suspension_efectiva, 0) = 0
              AND DAYOFWEEK(A.timestamp) NOT IN (1, 7)
              AND B.id IS NOT NULL
        ) src
        GROUP BY DATE(src.timestamp), src.user_id
        HAVING TIME(MIN(src.timestamp)) > %s
        ORDER BY MIN(src.timestamp) ASC
    """

    conn = await get_husky_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            params = (*direct_params, start_date, end_date, *chain_params, start_date, end_date, threshold_time)
            await cur.execute(query, params)
            return await cur.fetchall()
    finally:
        conn.close()
