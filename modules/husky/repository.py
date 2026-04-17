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