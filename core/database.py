import aiomysql
from core.config import settings

async def get_husky_db_connection():
    """
    Returns an async connection to the Husky Pass database (casitaiedis).
    Used for scanning, access control, and student pass tracking.
    """
    return await aiomysql.connect(
        host=settings.db_husky_host,
        port=settings.db_husky_port,
        user=settings.db_husky_user,
        password=settings.db_husky_password,
        db=settings.db_husky_name,
        autocommit=True
    )

async def get_attendance_db_connection():
    """
    Returns an async connection to the Attendance database (control_coordinaciones).
    Used for checking attendance, teacher population logs, and messaging schedules.
    """
    return await aiomysql.connect(
        host=settings.db_attendance_host,
        port=settings.db_attendance_port,
        user=settings.db_attendance_user,
        password=settings.db_attendance_password,
        db=settings.db_attendance_name,
        autocommit=True
    )