import aiomysql
from datetime import date
from typing import Dict, Iterable, List, Tuple

from core.database import get_attendance_db_connection, get_husky_db_connection
from modules.academic.repository import _build_academic_where


async def fetch_attendance_daily_activity(db_code: str, start_date: date, end_date: date) -> List[Dict]:
    """Daily student-attendance activity used for historical baselines."""
    query = """
        SELECT
            DATE(fecha) AS date_val,
            COUNT(name) AS total_students,
            SUM(CASE WHEN attendance = 1 THEN 1 ELSE 0 END) AS asistencia,
            SUM(CASE WHEN attendance = 0 THEN 1 ELSE 0 END) AS ausencia
        FROM asistencia
        WHERE plantel = %s
          AND DATE(fecha) BETWEEN %s AND %s
        GROUP BY DATE(fecha)
        ORDER BY date_val ASC
    """
    conn = await get_attendance_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, (db_code, start_date, end_date))
            return await cur.fetchall()
    finally:
        conn.close()


def _normalize_husky_codes(db_codes: Iterable[str] | str) -> List[str]:
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


def _husky_plantel_clause(alias: str, db_codes: Iterable[str] | str) -> Tuple[str, List[str]]:
    codes = _normalize_husky_codes(db_codes)
    clause = " OR ".join([f"{alias}.plantel LIKE %s" for _ in codes])
    return f"({clause})", [f"{code}%" for code in codes]


async def fetch_husky_daily_activity(db_codes: Iterable[str] | str, start_date: date, end_date: date) -> List[Dict]:
    """Daily Husky Pass scan activity used for historical baselines."""
    plantel_clause, plantel_params = _husky_plantel_clause("B", db_codes)
    query = f"""
        SELECT
            DATE(A.timestamp) AS date_val,
            A.type AS tipo_accion,
            COUNT(DISTINCT B.id) AS total_scans
        FROM acceso A
        LEFT JOIN personas_autorizadas pa ON pa.id = A.ss_id
        LEFT JOIN users B ON pa.user_id = B.id
        WHERE {plantel_clause}
          AND DATE(A.timestamp) BETWEEN %s AND %s
        GROUP BY DATE(A.timestamp), A.type
        ORDER BY date_val ASC
    """
    conn = await get_husky_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, (*plantel_params, start_date, end_date))
            return await cur.fetchall()
    finally:
        conn.close()


SAPF_DAILY_ACTIVITY_SQL = """
    SELECT DATE(src.fecha) AS date_val, COUNT(*) AS conteo
    FROM (
        SELECT fa.fecha
        FROM fichas_atencion fa
        WHERE fa.school_code = %s
          AND fa.fecha >= %s
          AND fa.fecha < DATE_ADD(%s, INTERVAL 1 DAY)
          AND fa.fecha <= NOW()

        UNION ALL

        SELECT se.fecha
        FROM seguimiento se
        WHERE se.school_code = %s
          AND se.fecha >= %s
          AND se.fecha < DATE_ADD(%s, INTERVAL 1 DAY)
          AND se.fecha <= NOW()
    ) src
    GROUP BY DATE(src.fecha)
    ORDER BY date_val ASC
"""


async def fetch_sapf_daily_activity(data_campuses: List[str], start_date: date, end_date: date) -> List[Dict]:
    """Daily SAPF activity with the same physical-campus semantics as the SAPF service."""
    totals: Dict[str, int] = {}
    conn = await get_attendance_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            for data_campus in data_campuses:
                await cur.execute(
                    SAPF_DAILY_ACTIVITY_SQL,
                    (data_campus, start_date, end_date, data_campus, start_date, end_date),
                )
                for row in await cur.fetchall():
                    key = str(row.get("date_val"))
                    totals[key] = totals.get(key, 0) + int(row.get("conteo") or 0)
    finally:
        conn.close()

    return [
        {"date_val": day, "conteo": count}
        for day, count in sorted(totals.items())
    ]


async def fetch_observaciones_daily_activity(academic_filters: List[Dict[str, str]], start_date: date, end_date: date) -> List[Dict]:
    """Daily observaciones activity. Uses submission_date by design."""
    where_clause, where_params = _build_academic_where(academic_filters)
    query = f"""
        SELECT
            DATE(submission_date) AS date_val,
            COUNT(*) AS total,
            SUM(CASE WHEN CHAR_LENGTH(IFNULL(comentarios_estrategias,'')) > 0 THEN 1 ELSE 0 END) AS quality
        FROM observaciones_form_submissions
        WHERE {where_clause}
          AND submission_date >= %s
          AND submission_date < DATE_ADD(%s, INTERVAL 1 DAY)
        GROUP BY DATE(submission_date)
        ORDER BY date_val ASC
    """
    conn = await get_attendance_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, (*where_params, start_date, end_date))
            return await cur.fetchall()
    finally:
        conn.close()


async def fetch_planeaciones_daily_activity(academic_filters: List[Dict[str, str]], start_date: date, end_date: date) -> List[Dict]:
    """Daily planeaciones activity. Uses created_at by design."""
    where_clause, where_params = _build_academic_where(academic_filters)
    query = f"""
        SELECT
            DATE(created_at) AS date_val,
            COUNT(*) AS total,
            SUM(CASE WHEN CHAR_LENGTH(IFNULL(feedback,'')) > 0
                       OR CHAR_LENGTH(IFNULL(feedback2,'')) > 0
                       OR CHAR_LENGTH(IFNULL(feedback3,'')) > 0
                 THEN 1 ELSE 0 END) AS quality
        FROM planeaciones
        WHERE {where_clause}
          AND created_at >= %s
          AND created_at < DATE_ADD(%s, INTERVAL 1 DAY)
        GROUP BY DATE(created_at)
        ORDER BY date_val ASC
    """
    conn = await get_attendance_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, (*where_params, start_date, end_date))
            return await cur.fetchall()
    finally:
        conn.close()
