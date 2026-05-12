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
        clean = str(code or "").strip().upper()
        if clean and clean not in seen:
            seen.add(clean)
            codes.append(clean)
    return codes or [""]


def _plantel_like_clause(alias: str, db_codes: Iterable[str] | str) -> Tuple[str, List[str]]:
    codes = _normalize_codes(db_codes)
    clause = " OR ".join([f"{alias}.plantel LIKE %s" for _ in codes])
    params = [f"{code}%" for code in codes]
    return f"({clause})", params


async def get_daily_scans(db_codes: Iterable[str] | str, start_date: date, end_date: date) -> list:
    """
    Executes the raw SQL query to obtain scan statistics from the Husky Pass DB.
    Some plantels can have more than one Husky storage code, e.g. PREET + CT.
    """
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
        GROUP BY DATE(A.timestamp), A.type
        ORDER BY DATE(A.timestamp) ASC
    """

    conn = await get_husky_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, (*plantel_params, start_date, end_date))
            results = await cur.fetchall()
            return results
    finally:
        conn.close()


async def fetch_plantel_retardos(db_codes: Iterable[str] | str, start_date: date, end_date: date, threshold_time: str) -> list:
    """
    Fetches tardies globally for the requested plantel. Uses the same Husky
    plantel storage codes as scan activity.
    """
    plantel_clause, plantel_params = _plantel_like_clause("B", db_codes)
    query = f"""
        SELECT
            A.id,
            CONCAT(IFNULL(ap.nombreA,''), ' ', IFNULL(ap.paternoA,''), ' ', IFNULL(ap.maternoA,'')) as student_fullname,
            B.username as matricula,
            DATE(A.timestamp) as date,
            TIME(A.timestamp) as time
        FROM acceso A
        JOIN alumno_pa ap ON ap.user_id = A.ss_id
        JOIN users B ON ap.user_id = B.id
        WHERE
            {plantel_clause}
            AND DATE(A.timestamp) BETWEEN %s AND %s
            AND A.timestamp IS NOT NULL
            AND A.type = 'entrada'
            AND A.suspension_efectiva = 0
            AND DAYOFWEEK(A.timestamp) NOT IN (1, 7)
            AND TIME(A.timestamp) > %s
        ORDER BY A.timestamp ASC
    """

    conn = await get_husky_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, (*plantel_params, start_date, end_date, threshold_time))
            results = await cur.fetchall()
            return results
    finally:
        conn.close()
