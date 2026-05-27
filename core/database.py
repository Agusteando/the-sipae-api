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
async def get_sipae_db_connection():
    """
    Returns an async connection to the SIPAE app database.
    Used for principal/manager resolution and health-report audit logs.
    """
    if not all([settings.db_sipae_host, settings.db_sipae_user, settings.db_sipae_name]):
        raise RuntimeError("SIPAE DB settings are not configured. Fill DB_SIPAE_* values in .env.")

    return await aiomysql.connect(
        host=settings.db_sipae_host,
        port=settings.db_sipae_port,
        user=settings.db_sipae_user,
        password=settings.db_sipae_password,
        db=settings.db_sipae_name,
        autocommit=True
    )
