import aiomysql
from datetime import date
from typing import Dict, List, Tuple
from core.database import get_attendance_db_connection


def _build_academic_where(filters: List[Dict[str, str]]) -> Tuple[str, List[str]]:
    """
    Observaciones and planeaciones store plantel as campus+nivel rather than
    the operational plantel code. Example: PM = campus Metepec + nivel Primaria.
    """
    clean_filters = []
    for item in filters or []:
        campus = str(item.get("campus") or "").strip()
        nivel = str(item.get("nivel") or "").strip()
        if campus and nivel:
            clean_filters.append((campus, nivel))

    if not clean_filters:
        return "1 = 0", []

    clauses = []
    params: List[str] = []
    for campus, nivel in clean_filters:
        clauses.append("(LOWER(TRIM(campus)) = LOWER(%s) AND LOWER(TRIM(nivel)) = LOWER(%s))")
        params.extend([campus, nivel])

    return "(" + " OR ".join(clauses) + ")", params


# ==========================================
# OBSERVACIONES DE CLASE
# ==========================================
async def get_observaciones_stats(academic_filters: List[Dict[str, str]], start_date: date, end_date: date) -> list:
    where_clause, where_params = _build_academic_where(academic_filters)
    query = f"""
        SELECT
            DATE(submission_date) AS date_val,
            COUNT(*) AS total_obs,
            SUM(CASE WHEN CHAR_LENGTH(IFNULL(comentarios_estrategias,'')) > 0 THEN 1 ELSE 0 END) AS obs_with_comment
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


async def get_observaciones_comments(academic_filters: List[Dict[str, str]], start_date: date, end_date: date) -> list:
    where_clause, where_params = _build_academic_where(academic_filters)
    query = f"""
        SELECT docente, comentarios_estrategias AS comment, submission_date
        FROM observaciones_form_submissions
        WHERE {where_clause}
          AND submission_date >= %s
          AND submission_date < DATE_ADD(%s, INTERVAL 1 DAY)
          AND CHAR_LENGTH(IFNULL(comentarios_estrategias,'')) > 0
        ORDER BY submission_date DESC
    """
    conn = await get_attendance_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, (*where_params, start_date, end_date))
            return await cur.fetchall()
    finally:
        conn.close()


# ==========================================
# PLANEACIONES
# ==========================================
async def get_planeaciones_stats(academic_filters: List[Dict[str, str]], start_date: date, end_date: date) -> list:
    where_clause, where_params = _build_academic_where(academic_filters)
    query = f"""
        SELECT
            DATE(created_at) AS date_val,
            COUNT(*) AS total_plans,
            SUM(CASE WHEN CHAR_LENGTH(IFNULL(feedback,'')) > 0
                       OR CHAR_LENGTH(IFNULL(feedback2,'')) > 0
                       OR CHAR_LENGTH(IFNULL(feedback3,'')) > 0
                 THEN 1 ELSE 0 END) AS plans_with_feedback
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


async def get_planeaciones_comments(academic_filters: List[Dict[str, str]], start_date: date, end_date: date) -> list:
    where_clause, where_params = _build_academic_where(academic_filters)
    query = f"""
        SELECT docente, feedback, feedback2, feedback3, created_at
        FROM planeaciones
        WHERE {where_clause}
          AND created_at >= %s
          AND created_at < DATE_ADD(%s, INTERVAL 1 DAY)
          AND (CHAR_LENGTH(IFNULL(feedback,'')) > 0
               OR CHAR_LENGTH(IFNULL(feedback2,'')) > 0
               OR CHAR_LENGTH(IFNULL(feedback3,'')) > 0)
        ORDER BY created_at DESC
    """
    conn = await get_attendance_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, (*where_params, start_date, end_date))
            return await cur.fetchall()
    finally:
        conn.close()
