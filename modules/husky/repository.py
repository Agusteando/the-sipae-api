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


async def fetch_student_retardos_by_matricula(matricula: str, start_date: date, end_date: date) -> list:
    """
    Fetches the tardies (retardos) for a specific student applying the strict timing rules
    based on the school level associated with the student profile.
    """
    query = """
        SELECT
            A.id,
            CONCAT(ap.nombreA, ' ', ap.paternoA, ' ', ap.maternoA) as student_fullname,
            B.username as matricula,
            DATE(A.timestamp) as date,
            TIME(A.timestamp) as time
        FROM acceso A
        JOIN alumno_pa ap ON ap.user_id = A.ss_id
        JOIN users B ON ap.user_id = B.id 
        WHERE 
            B.username = %s
            AND A.timestamp >= %s 
            AND A.timestamp < %s
            AND A.type = 'entrada'
            AND A.suspension_efectiva = 0
            AND DAYOFWEEK(A.timestamp) NOT IN (1, 7)
            AND (
                (ap.nivelEdu = 'Secundaria' AND TIME(A.timestamp) > '07:01:00')
                OR
                (ap.nivelEdu = 'Primaria' AND TIME(A.timestamp) > '08:01:00')
                OR
                (ap.nivelEdu NOT IN ('Secundaria', 'Primaria') AND TIME(A.timestamp) > '09:01:00')
            )
        ORDER BY A.timestamp ASC
    """
    
    conn = await get_husky_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, (matricula, start_date, end_date))
            results = await cur.fetchall()
            return results
    finally:
        conn.close()