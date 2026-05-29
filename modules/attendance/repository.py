import aiomysql
from datetime import date
from typing import Iterable, Tuple, List, Dict
from core.database import get_attendance_db_connection
from core.logger import get_logger

logger = get_logger("repository.attendance")


def _normalize_values(values: Iterable[str] | str) -> List[str]:
    if isinstance(values, str):
        raw_values = [values]
    else:
        raw_values = list(values or [])
    seen = set()
    normalized: List[str] = []
    for value in raw_values:
        clean = " ".join(str(value or "").strip().upper().split())
        if clean and clean not in seen:
            seen.add(clean)
            normalized.append(clean)
    return normalized or [""]


def _plantel_clause(alias: str, plantel_values: Iterable[str] | str) -> Tuple[str, List[str]]:
    """Build a tolerant campus clause for attendance records.

    The attendance database has used short codes, legacy aliases, and long
    campus labels across different periods. Exact normalized matches are tried
    first, while short-code LIKE matching catches values such as "1 - PT".
    """
    values = _normalize_values(plantel_values)
    prefix = f"{alias}." if alias else ""
    expr = f"TRIM(UPPER({prefix}plantel))"
    exact = ", ".join(["%s" for _ in values])
    like_values = [value for value in values if len(value) <= 8]
    like_clause = " OR ".join([f"{expr} LIKE %s" for _ in like_values])
    clause = f"({expr} IN ({exact})"
    params: List[str] = list(values)
    if like_clause:
        clause += f" OR {like_clause}"
        params.extend([f"%{value}%" for value in like_values])
    clause += ")"
    return clause, params


async def fetch_attendance_data(plantel_values: Iterable[str] | str, start_date: date, end_date: date) -> Tuple[List[Dict], List[Dict]]:
    """Executes robust extraction for attendance.

    Returns (stats_results, absents_results). The plantel filter accepts all
    known aliases for a campus instead of one rigid code.
    """
    stats_results: List[Dict] = []
    absents_results: List[Dict] = []
    plantel_clause, plantel_params = _plantel_clause("A", plantel_values)

    query_stats_with_join = f"""
        SELECT
            DATE(A.fecha) as d_fecha,
            A.grado,
            A.grupo,
            COUNT(A.name) as total_students_per_group,
            SUM(CASE WHEN A.attendance = 1 THEN 1 ELSE 0 END) as asistencia,
            SUM(CASE WHEN A.attendance = 0 THEN 1 ELSE 0 END) as ausencia,
            SUM(CASE WHEN A.modalidad = 0 OR A.modalidad IS NULL THEN 1 ELSE 0 END) as ausencia2,
            SUM(CASE WHEN A.modalidad = 1 THEN 1 ELSE 0 END) as presencial,
            SUM(CASE WHEN A.modalidad = 2 THEN 1 ELSE 0 END) as virt,
            SUM(CASE WHEN B.gender IN ('F', 'FEMENINO', 'Mujer') THEN 1 ELSE 0 END) as girls,
            SUM(CASE WHEN B.gender IN ('M', 'MASCULINO', 'Hombre') THEN 1 ELSE 0 END) as boys
        FROM asistencia A
        LEFT JOIN gender_list B ON A.name = B.student
        WHERE {plantel_clause} AND DATE(A.fecha) BETWEEN %s AND %s
        GROUP BY DATE(A.fecha), A.grado, A.grupo
    """

    query_stats_fallback = f"""
        SELECT
            DATE(A.fecha) as d_fecha,
            A.grado,
            A.grupo,
            COUNT(A.name) as total_students_per_group,
            SUM(CASE WHEN A.attendance = 1 THEN 1 ELSE 0 END) as asistencia,
            SUM(CASE WHEN A.attendance = 0 THEN 1 ELSE 0 END) as ausencia,
            SUM(CASE WHEN A.modalidad = 0 OR A.modalidad IS NULL THEN 1 ELSE 0 END) as ausencia2,
            SUM(CASE WHEN A.modalidad = 1 THEN 1 ELSE 0 END) as presencial,
            SUM(CASE WHEN A.modalidad = 2 THEN 1 ELSE 0 END) as virt,
            0 as girls,
            0 as boys
        FROM asistencia A
        WHERE {plantel_clause} AND DATE(A.fecha) BETWEEN %s AND %s
        GROUP BY DATE(A.fecha), A.grado, A.grupo
    """

    query_absents = f"""
        SELECT DATE(A.fecha) as d_fecha, A.id, A.name, A.grado, A.grupo, A.plantel, A.motivo
        FROM asistencia A
        WHERE A.attendance = 0 AND {plantel_clause} AND DATE(A.fecha) BETWEEN %s AND %s
        ORDER BY d_fecha ASC, A.grado ASC, A.grupo ASC
    """

    conn = await get_attendance_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            try:
                logger.info("Executing attendance primary join query for aliases=%s", plantel_values)
                await cur.execute(query_stats_with_join, (*plantel_params, start_date, end_date))
                stats_results = await cur.fetchall()
            except Exception as e:
                logger.warning("Attendance join failed (%s), using fallback query", e)
                await cur.execute(query_stats_fallback, (*plantel_params, start_date, end_date))
                stats_results = await cur.fetchall()

            logger.info("Executing attendance absents query for aliases=%s", plantel_values)
            await cur.execute(query_absents, (*plantel_params, start_date, end_date))
            absents_results = await cur.fetchall()
            return stats_results, absents_results
    finally:
        conn.close()
