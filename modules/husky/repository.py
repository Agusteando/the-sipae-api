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
    Fetches student tardies from Husky Pass using the same access identity chain
    as the scan-rate query: acceso.ss_id -> personas_autorizadas.id -> users.id.

    Important: count only the first valid entrada per student per day. A student
    can scan more than once; additional entrance scans after the threshold should
    not inflate tardies.
    """
    plantel_clause, plantel_params = _plantel_like_clause("B", db_codes)
    query = f"""
        SELECT
            MIN(A.id) AS id,
            COALESCE(
                NULLIF(TRIM(CONCAT_WS(' ', ap.nombreA, ap.paternoA, ap.maternoA)), ''),
                NULLIF(TRIM(B.username), ''),
                'Desconocido'
            ) AS student_fullname,
            COALESCE(NULLIF(TRIM(B.username), ''), 'N/A') AS matricula,
            DATE(MIN(A.timestamp)) AS date,
            TIME(MIN(A.timestamp)) AS time
        FROM acceso A
        LEFT JOIN personas_autorizadas pa ON pa.id = A.ss_id
        LEFT JOIN users B ON pa.user_id = B.id
        LEFT JOIN alumno_pa ap ON ap.user_id = B.id
        WHERE
            {plantel_clause}
            AND DATE(A.timestamp) BETWEEN %s AND %s
            AND A.timestamp IS NOT NULL
            AND LOWER(A.type) = 'entrada'
            AND COALESCE(A.suspension_efectiva, 0) = 0
            AND DAYOFWEEK(A.timestamp) NOT IN (1, 7)
            AND B.id IS NOT NULL
        GROUP BY DATE(A.timestamp), B.id, B.username, ap.nombreA, ap.paternoA, ap.maternoA
        HAVING TIME(MIN(A.timestamp)) > %s
        ORDER BY MIN(A.timestamp) ASC
    """

    conn = await get_husky_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, (*plantel_params, start_date, end_date, threshold_time))
            results = await cur.fetchall()
            return results
    finally:
        conn.close()
