import aiomysql
from datetime import date
from typing import Tuple, List, Dict
from core.database import get_attendance_db_connection
from core.logger import get_logger

logger = get_logger("repository.attendance")

# ==========================================
# SQL QUERIES
# ==========================================
QUERY_STATS_WITH_JOIN = """
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
    WHERE A.plantel = %s AND DATE(A.fecha) BETWEEN %s AND %s
    GROUP BY DATE(A.fecha), A.grado, A.grupo
"""

QUERY_STATS_FALLBACK = """
    SELECT
        DATE(fecha) as d_fecha,
        grado,
        grupo,
        COUNT(name) as total_students_per_group,
        SUM(CASE WHEN attendance = 1 THEN 1 ELSE 0 END) as asistencia,
        SUM(CASE WHEN attendance = 0 THEN 1 ELSE 0 END) as ausencia,
        SUM(CASE WHEN modalidad = 0 OR modalidad IS NULL THEN 1 ELSE 0 END) as ausencia2,
        SUM(CASE WHEN modalidad = 1 THEN 1 ELSE 0 END) as presencial,
        SUM(CASE WHEN modalidad = 2 THEN 1 ELSE 0 END) as virt,
        0 as girls,
        0 as boys
    FROM asistencia
    WHERE plantel = %s AND DATE(fecha) BETWEEN %s AND %s
    GROUP BY DATE(fecha), grado, grupo
"""

QUERY_ABSENTS = """
    SELECT DATE(fecha) as d_fecha, id, name, grado, grupo, plantel, motivo
    FROM asistencia
    WHERE attendance = 0 AND plantel = %s AND DATE(fecha) BETWEEN %s AND %s
    ORDER BY d_fecha ASC, grado ASC, grupo ASC
"""

async def fetch_attendance_data(db_code: str, start_date: date, end_date: date) -> Tuple[List[Dict], List[Dict]]:
    """
    Executes multi-query robust extraction for attendance.
    Returns (stats_results, absents_results)
    """
    stats_results = []
    absents_results = []
    
    conn = await get_attendance_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            # Try Join first
            try:
                logger.info("Executing Primary Join Query...")
                await cur.execute(QUERY_STATS_WITH_JOIN, (db_code, start_date, end_date))
                stats_results = await cur.fetchall()
            except Exception as e:
                logger.warning(f"Join failed ({e}), using Fallback Query...")
                await cur.execute(QUERY_STATS_FALLBACK, (db_code, start_date, end_date))
                stats_results = await cur.fetchall()

            # Execute Absents
            logger.info("Executing Absents Query...")
            await cur.execute(QUERY_ABSENTS, (db_code, start_date, end_date))
            absents_results = await cur.fetchall()
            
            return stats_results, absents_results
    finally:
        conn.close()