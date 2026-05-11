import aiomysql
from datetime import date
from core.database import get_attendance_db_connection

# ==========================================
# OBSERVACIONES DE CLASE
# ==========================================
async def get_observaciones_stats(db_code: str, start_date: date, end_date: date) -> list:
    query = """
        SELECT
            DATE(created_at) as date_val,
            COUNT(*) AS total_obs,
            SUM(CASE WHEN CHAR_LENGTH(IFNULL(comentarios_estrategias,'')) > 0 THEN 1 ELSE 0 END) AS obs_with_comment
        FROM observaciones_form_submissions
        WHERE campus LIKE %s
          AND DATE(created_at) BETWEEN %s AND %s
        GROUP BY DATE(created_at)
        ORDER BY date_val ASC
    """
    conn = await get_attendance_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, (f"{db_code}%", start_date, end_date))
            return await cur.fetchall()
    finally:
        conn.close()

async def get_observaciones_comments(db_code: str, start_date: date, end_date: date) -> list:
    query = """
        SELECT docente, comentarios_estrategias AS comment, created_at
        FROM observaciones_form_submissions
        WHERE campus LIKE %s
          AND DATE(created_at) BETWEEN %s AND %s
          AND CHAR_LENGTH(IFNULL(comentarios_estrategias,'')) > 0
        ORDER BY created_at DESC
    """
    conn = await get_attendance_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, (f"{db_code}%", start_date, end_date))
            return await cur.fetchall()
    finally:
        conn.close()

# ==========================================
# PLANEACIONES
# ==========================================
async def get_planeaciones_stats(db_code: str, start_date: date, end_date: date) -> list:
    query = """
        SELECT
            DATE(created_at) as date_val,
            COUNT(*) AS total_plans,
            SUM(CASE WHEN CHAR_LENGTH(IFNULL(feedback,'')) > 0 
                       OR CHAR_LENGTH(IFNULL(feedback2,'')) > 0 
                       OR CHAR_LENGTH(IFNULL(feedback3,'')) > 0 
                 THEN 1 ELSE 0 END) AS plans_with_feedback
        FROM planeaciones
        WHERE campus LIKE %s
          AND DATE(created_at) BETWEEN %s AND %s
        GROUP BY DATE(created_at)
        ORDER BY date_val ASC
    """
    conn = await get_attendance_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, (f"{db_code}%", start_date, end_date))
            return await cur.fetchall()
    finally:
        conn.close()

async def get_planeaciones_comments(db_code: str, start_date: date, end_date: date) -> list:
    query = """
        SELECT docente, feedback, feedback2, feedback3, created_at
        FROM planeaciones
        WHERE campus LIKE %s
          AND DATE(created_at) BETWEEN %s AND %s
          AND (CHAR_LENGTH(IFNULL(feedback,'')) > 0 
               OR CHAR_LENGTH(IFNULL(feedback2,'')) > 0 
               OR CHAR_LENGTH(IFNULL(feedback3,'')) > 0)
        ORDER BY created_at DESC
    """
    conn = await get_attendance_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, (f"{db_code}%", start_date, end_date))
            return await cur.fetchall()
    finally:
        conn.close()