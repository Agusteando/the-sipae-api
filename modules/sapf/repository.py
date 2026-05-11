import aiomysql
from datetime import date
from core.database import get_attendance_db_connection

async def get_sapf_monthly_stats(db_code: str, start_date: date, end_date: date) -> list:
    """
    Recupera y agrupa los conteos mensuales de las fichas de atención por área.
    """
    query = """
        SELECT
            fa.school_code AS plantel0,
            fa.area,
            YEAR(fa.fecha) AS year,
            MONTH(fa.fecha) AS month,
            DATE_FORMAT(fa.fecha, '%Y-%m') AS period,
            COUNT(*) AS conteo
        FROM fichas_atencion fa
        WHERE fa.school_code IS NOT NULL
          AND fa.school_code LIKE %s
          AND DATE(fa.fecha) BETWEEN %s AND %s
          AND fa.fecha <= NOW()
        GROUP BY fa.school_code, fa.area, year, month, period
        ORDER BY fa.school_code, fa.area, year, month
    """
    conn = await get_attendance_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, (f"{db_code}%", start_date, end_date))
            return await cur.fetchall()
    finally:
        conn.close()

async def get_sapf_motivos_stats(db_code: str, start_date: date, end_date: date) -> list:
    """
    Extrae la sumatoria global de incidencias agrupadas por área y motivo.
    """
    query = """
        SELECT
            fa.school_code AS plantel0,
            fa.area,
            fa.motivo,
            COUNT(*) AS conteo
        FROM fichas_atencion fa
        WHERE fa.school_code IS NOT NULL
          AND fa.school_code LIKE %s
          AND DATE(fa.fecha) BETWEEN %s AND %s
          AND fa.fecha <= NOW()
        GROUP BY fa.school_code, fa.area, fa.motivo
        ORDER BY conteo DESC
    """
    conn = await get_attendance_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, (f"{db_code}%", start_date, end_date))
            return await cur.fetchall()
    finally:
        conn.close()