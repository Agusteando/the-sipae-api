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
    plan_where, plan_params = _build_academic_where(academic_filters, "p")
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
          AND {plan_where}
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
            await cur.execute(query, (*user_params, *plan_params, start_date, end_date))
            return await cur.fetchone() or {}
    finally:
        conn.close()


async def fetch_observation_teacher_totals(
    academic_filters: List[Dict[str, str]],
    observation_start: date,
    observation_end: date,
) -> Dict[str, Any]:
    user_where, user_params = _build_academic_where(academic_filters, "u")
    obs_where, obs_params = _build_academic_where(academic_filters, "obs")
    query = f"""
        SELECT
            COUNT(DISTINCT u.username) AS active_teachers,
            COUNT(DISTINCT obs.docente) AS observed_teachers,
            COUNT(obs.id) AS total_observations
        FROM usuarios u
        LEFT JOIN observaciones_form_submissions obs
               ON obs.docente = u.username
              AND {obs_where}
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
            await cur.execute(query, (*obs_params, observation_start, observation_end, *user_params))
            return await cur.fetchone() or {}
    finally:
        conn.close()
