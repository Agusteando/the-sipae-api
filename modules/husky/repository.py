import aiomysql
from datetime import date
from core.database import get_husky_db_connection

async def get_daily_scans(db_code: str, start_date: date, end_date: date) -> list:
    """
    Executes the raw SQL query to obtain scan statistics from the Husky Pass DB.
    """
    query = """
        SELECT 
            DATE(A.timestamp) as fecha,
            A.type as tipo_accion,
            COUNT(DISTINCT B.id) as total_scans
        FROM acceso A
        LEFT JOIN personas_autorizadas pa ON pa.id = A.ss_id
        LEFT JOIN users B ON pa.user_id = B.id
        WHERE B.plantel LIKE %s
          AND DATE(A.timestamp) BETWEEN %s AND %s
        GROUP BY DATE(A.timestamp), A.type
        ORDER BY DATE(A.timestamp) ASC
    """
    
    conn = await get_husky_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, (f"{db_code}%", start_date, end_date))
            results = await cur.fetchall()
            return results
    finally:
        conn.close()


async def fetch_plantel_retardos(db_code: str, start_date: date, end_date: date, threshold_time: str) -> list:
    """
    Fetches the tardies (retardos) globally for a specific plantel applying a strict 
    timing threshold parameter driven by the normalized plantel code.
    """
    query = """
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
            B.plantel LIKE %s
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
            await cur.execute(query, (f"{db_code}%", start_date, end_date, threshold_time))
            results = await cur.fetchall()
            return results
    finally:
        conn.close()