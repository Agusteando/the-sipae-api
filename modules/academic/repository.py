import aiomysql
from datetime import date
from typing import Dict, List, Optional, Tuple
from core.database import get_attendance_db_connection


def _build_academic_where(filters: List[Dict[str, str]], table_alias: Optional[str] = None) -> Tuple[str, List[str]]:
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
    prefix = f"{table_alias}." if table_alias else ""

    for campus, nivel in clean_filters:
        clauses.append(
            f"(LOWER(TRIM({prefix}campus)) = LOWER(%s) "
            f"AND LOWER(TRIM({prefix}nivel)) = LOWER(%s))"
        )
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


# ==========================================
# OBSERVACIONES - ESTATUS POR DOCENTE
# ==========================================
async def get_observaciones_teacher_status(
    academic_filters: List[Dict[str, str]],
    start_date: date,
    end_date: date,
) -> list:
    where_clause, where_params = _build_academic_where(academic_filters, "obs")
    query = f"""
        SELECT
            latest.docente,
            latest.campus,
            latest.nivel,
            latest.last_submission_date,
            latest.last_observation_date,
            latest.total_observaciones,
            u.username,
            u.email,
            GROUP_CONCAT(
                DISTINCT NULLIF(TRIM(obs_latest.user_name), '')
                ORDER BY obs_latest.user_name
                SEPARATOR '|||'
            ) AS latest_observers
        FROM (
            SELECT
                obs.docente,
                obs.campus,
                obs.nivel,
                MAX(obs.submission_date) AS last_submission_date,
                DATE(MAX(obs.submission_date)) AS last_observation_date,
                COUNT(*) AS total_observaciones
            FROM observaciones_form_submissions obs
            WHERE {where_clause}
              AND obs.submission_date >= %s
              AND obs.submission_date < DATE_ADD(%s, INTERVAL 1 DAY)
            GROUP BY obs.docente, obs.campus, obs.nivel
        ) latest
        LEFT JOIN usuarios u
               ON u.username = latest.docente
        LEFT JOIN observaciones_form_submissions obs_latest
               ON obs_latest.docente = latest.docente
              AND obs_latest.campus <=> latest.campus
              AND obs_latest.nivel <=> latest.nivel
              AND DATE(obs_latest.submission_date) = latest.last_observation_date
        GROUP BY
            latest.docente,
            latest.campus,
            latest.nivel,
            latest.last_submission_date,
            latest.last_observation_date,
            latest.total_observaciones,
            u.username,
            u.email
        ORDER BY latest.last_submission_date DESC, latest.docente ASC
    """
    conn = await get_attendance_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, (*where_params, start_date, end_date))
            return await cur.fetchall()
    finally:
        conn.close()


async def get_least_observed_teacher(
    academic_filters: List[Dict[str, str]],
    school_year_start: date,
    school_year_end: date,
) -> Optional[Dict]:
    user_where_clause, user_where_params = _build_academic_where(academic_filters, "u")
    obs_where_clause, obs_where_params = _build_academic_where(academic_filters, "obs")
    query = f"""
        SELECT
            u.username AS docente,
            u.username,
            u.email,
            u.campus,
            u.nivel,
            COUNT(obs.id) AS total_observaciones
        FROM usuarios u
        LEFT JOIN observaciones_form_submissions obs
               ON obs.docente = u.username
              AND {obs_where_clause}
              AND obs.submission_date >= %s
              AND obs.submission_date < DATE_ADD(%s, INTERVAL 1 DAY)
        WHERE {user_where_clause}
          AND u.username IS NOT NULL
          AND TRIM(u.username) <> ''
          AND COALESCE(u.coordinador, 0) = 0
          AND COALESCE(u.banned, 0) = 0
          AND (u.ISSSTE IS NULL OR u.ISSSTE = 0)
        GROUP BY u.username, u.email, u.campus, u.nivel
        ORDER BY total_observaciones ASC, u.username ASC
        LIMIT 1
    """
    conn = await get_attendance_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                query,
                (
                    *obs_where_params,
                    school_year_start,
                    school_year_end,
                    *user_where_params,
                ),
            )
            return await cur.fetchone()
    finally:
        conn.close()


# ==========================================
# PLANEACIONES - PENDIENTES DE REVISIÓN
# ==========================================
async def count_recent_active_planeacion_docentes(
    academic_filters: List[Dict[str, str]],
    recent_start_date: date,
    recent_end_date: date,
) -> int:
    user_where_clause, user_where_params = _build_academic_where(academic_filters, "u")
    query = f"""
        SELECT COUNT(DISTINCT p.docente) AS total
        FROM planeaciones p
        JOIN usuarios u
          ON u.username = p.docente
        WHERE {user_where_clause}
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
            await cur.execute(query, (*user_where_params, recent_start_date, recent_end_date))
            row = await cur.fetchone()
            return int((row or {}).get("total") or 0)
    finally:
        conn.close()


async def get_pending_review_planeaciones(
    academic_filters: List[Dict[str, str]],
    start_date: date,
    end_date: date,
    recent_start_date: date,
    recent_end_date: date,
) -> list:
    active_user_where_clause, active_user_where_params = _build_academic_where(academic_filters, "recent_u")
    outer_user_where_clause, outer_user_where_params = _build_academic_where(academic_filters, "u")
    query = f"""
        SELECT
            p.id,
            p.docente,
            p.week,
            p.ciclo,
            p.created_at,
            p.weekEnd,
            p.nivel,
            p.campus,
            u.username,
            u.email,
            p.revisa,
            p.revisa2,
            p.revisa3,
            p.feedback,
            p.feedback2,
            p.feedback3
        FROM planeaciones p
        JOIN usuarios u
          ON u.username = p.docente
        JOIN (
            SELECT DISTINCT recent.docente
            FROM planeaciones recent
            JOIN usuarios recent_u
              ON recent_u.username = recent.docente
            WHERE {active_user_where_clause}
              AND recent.created_at >= %s
              AND recent.created_at < DATE_ADD(%s, INTERVAL 1 DAY)
              AND recent.flagged = 0
              AND recent.week IS NOT NULL
              AND recent.docente IS NOT NULL
              AND TRIM(recent.docente) <> ''
              AND COALESCE(recent_u.coordinador, 0) = 0
              AND COALESCE(recent_u.banned, 0) = 0
              AND (recent_u.ISSSTE IS NULL OR recent_u.ISSSTE = 0)
        ) active_docentes
          ON active_docentes.docente = p.docente
        WHERE {outer_user_where_clause}
          AND p.created_at >= %s
          AND p.created_at < DATE_ADD(%s, INTERVAL 1 DAY)
          AND p.flagged = 0
          AND p.week IS NOT NULL
          AND p.docente IS NOT NULL
          AND TRIM(p.docente) <> ''
          AND COALESCE(u.coordinador, 0) = 0
          AND COALESCE(u.banned, 0) = 0
          AND (u.ISSSTE IS NULL OR u.ISSSTE = 0)
          AND CHAR_LENGTH(IFNULL(TRIM(p.revisa), '')) = 0
          AND CHAR_LENGTH(IFNULL(TRIM(p.revisa2), '')) = 0
          AND CHAR_LENGTH(IFNULL(TRIM(p.revisa3), '')) = 0
          AND CHAR_LENGTH(IFNULL(TRIM(p.feedback), '')) = 0
          AND CHAR_LENGTH(IFNULL(TRIM(p.feedback2), '')) = 0
          AND CHAR_LENGTH(IFNULL(TRIM(p.feedback3), '')) = 0
        ORDER BY p.week ASC, u.username ASC, p.created_at ASC
    """
    conn = await get_attendance_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                query,
                (
                    *active_user_where_params,
                    recent_start_date,
                    recent_end_date,
                    *outer_user_where_params,
                    start_date,
                    end_date,
                ),
            )
            return await cur.fetchall()
    finally:
        conn.close()
