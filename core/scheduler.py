import asyncio
from datetime import date, datetime
from zoneinfo import ZoneInfo
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from core.logger import get_logger
from core.constants import ACTIVE_PLANTEL_CODES
from core.cache import set_cache
from core.config import settings

# Importación de Servicios de Agregación Pesada
from modules.husky.service import calculate_husky_daily_rate, get_plantel_retardos
from modules.attendance.service import get_attendance_detail_report
from modules.employee_attendance.service import get_kardex_attendance_report
from modules.sapf.service import get_sapf_monthly_report, get_sapf_motivos_report
from modules.academic.service import get_observaciones_report, get_planeaciones_report
from modules.baselines.service import get_global_baseline_report
from modules.health_reports.service import run_daily_health_reports
from modules.health_reports.schedule_config import get_schedule_config, save_schedule_config, cron_day_of_week

logger = get_logger("scheduler")
scheduler = AsyncIOScheduler()

async def refresh_today_metrics():
    """
    Tarea en segundo plano que pre-calcula métricas del día para todos los planteles.
    Alimenta la memoria caché para evitar bloqueos síncronos durante la petición de los usuarios.
    """
    logger.info("Iniciando actualización en segundo plano de métricas operativas (Caché).")
    
    # Se garantiza que el cronograma trabaje con la zona horaria correcta
    tz_mx = ZoneInfo("America/Mexico_City")
    today = datetime.now(tz_mx).date()
    
    for plantel in ACTIVE_PLANTEL_CODES:
        try:
            # 1. Husky Pass - Tasa de Captura
            husky_rate = await calculate_husky_daily_rate(plantel, today, today, "today")
            set_cache(f"husky_rate_{plantel}", husky_rate)
            
            # 2. Husky Pass - Retardos Escolares
            husky_ret = await get_plantel_retardos(plantel, today, today, "today")
            set_cache(f"husky_retardos_{plantel}", husky_ret)
            
            # 3. Asistencia Estudiantil - Pases de Lista
            att = await get_attendance_detail_report(plantel, today, today, "today")
            set_cache(f"attendance_{plantel}", att)
            
            # 4. Asistencia Empleados - Kardex (Descanso para no saturar API externa)
            await asyncio.sleep(0.5) 
            kardex = await get_kardex_attendance_report(plantel, today, today, "today")
            set_cache(f"kardex_{plantel}", kardex)
            
            # 5. SAPF - Reportes Mensuales y Motivos
            sapf_monthly = await get_sapf_monthly_report(plantel, today, today, "today")
            set_cache(f"sapf_monthly_{plantel}_today", sapf_monthly)
            
            sapf_motivos = await get_sapf_motivos_report(plantel, today, today, "today")
            set_cache(f"sapf_motivos_{plantel}_today", sapf_motivos)

            # 6. Monitoreo Académico - Observaciones y Planeaciones
            obs_report = await get_observaciones_report(plantel, today, today, "today")
            set_cache(f"academic_obs_{plantel}_today", obs_report)

            plan_report = await get_planeaciones_report(plantel, today, today, "today")
            set_cache(f"academic_plan_{plantel}_today", plan_report)
            
        except Exception as e:
            logger.error(f"Fallo al pre-calcular métricas para el plantel {plantel}: {str(e)}")
        
        # Pausa breve entre planteles para estabilizar los hilos de bases de datos
        await asyncio.sleep(0.5)
        
    logger.info("Ciclo de actualización de caché completado con éxito.")

async def refresh_global_baselines():
    """Pre-calcula el baseline histórico global sin bloquear las cargas del dashboard."""
    logger.info("Iniciando actualización en segundo plano de baselines históricos globales.")
    try:
        data = await get_global_baseline_report()
        today_key = datetime.now(ZoneInfo("America/Mexico_City")).date().isoformat()
        set_cache(f"baseline_global_ACTIVE_months:3_{today_key}_3_9", data)
        logger.info("Baselines históricos globales actualizados con éxito.")
    except Exception as e:
        logger.error(f"Fallo al pre-calcular baselines históricos globales: {str(e)}")

async def send_scheduled_health_reports():
    """Sends the weekday end-of-day SIPAE health report with managers copied."""
    logger.info("Iniciando envío programado de reportes de cierre SIPAE.")
    try:
        result = await run_daily_health_reports(send=True)
        logger.info("Reportes de cierre SIPAE enviados: %s", result)
    except Exception as e:
        logger.error(f"Fallo al enviar reportes de cierre SIPAE: {str(e)}")


def configure_health_reports_schedule(config=None):
    """Create, update or remove the scheduled health-report job at runtime."""
    config = config or get_schedule_config()
    job_id = 'send_health_reports_job'
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)
    if config.get('enabled'):
        scheduler.add_job(
            send_scheduled_health_reports,
            'cron',
            day_of_week=cron_day_of_week(config.get('days') or []),
            hour=int(config.get('hour', 15)),
            minute=int(config.get('minute', 55)),
            timezone=config.get('timezone') or 'America/Mexico_City',
            id=job_id,
            replace_existing=True,
        )
    return scheduler_status()


def update_health_reports_schedule(payload):
    config = save_schedule_config(payload or {})
    return configure_health_reports_schedule(config)


def scheduler_status():
    config = get_schedule_config()
    job = scheduler.get_job('send_health_reports_job')
    return {
        'config': config,
        'active': bool(job),
        'next_run_time': job.next_run_time.isoformat() if job and job.next_run_time else None,
    }

def start_scheduler():
    """Registra y arranca el cronograma de ejecución de trabajos automáticos."""
    # Se ejecuta con una cadencia conservadora para no saturar integraciones externas.
    scheduler.add_job(
        refresh_today_metrics, 
        'interval', 
        minutes=10, 
        id='refresh_metrics_job', 
        replace_existing=True
    )
    scheduler.add_job(
        refresh_global_baselines,
        'interval',
        minutes=60,
        id='refresh_global_baselines_job',
        replace_existing=True
    )
    configure_health_reports_schedule(get_schedule_config())
    scheduler.start()
    
    # Detona una primera ejecución asíncrona al arrancar el servidor (Cold Boot pre-warm)
    asyncio.create_task(refresh_today_metrics())
    asyncio.create_task(refresh_global_baselines())
