import asyncio
from datetime import date
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from core.logger import get_logger
from core.constants import PLANTEL_MAP
from core.cache import set_cache

# Importación de Servicios de Agregación Pesada
from modules.husky.service import calculate_husky_daily_rate, get_plantel_retardos
from modules.attendance.service import get_attendance_detail_report
from modules.employee_attendance.service import get_kardex_attendance_report

logger = get_logger("scheduler")
scheduler = AsyncIOScheduler()

async def refresh_today_metrics():
    """
    Tarea en segundo plano que pre-calcula métricas del día para todos los planteles.
    Alimenta la memoria caché para evitar bloqueos síncronos durante la petición de los usuarios.
    """
    logger.info("Iniciando actualización en segundo plano de métricas operativas (Caché).")
    today = date.today()
    
    for plantel in PLANTEL_MAP.keys():
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
            
        except Exception as e:
            logger.error(f"Fallo al pre-calcular métricas para el plantel {plantel}: {str(e)}")
        
        # Pausa breve entre planteles para estabilizar los hilos de bases de datos
        await asyncio.sleep(0.5)
        
    logger.info("Ciclo de actualización de caché completado con éxito.")

def start_scheduler():
    """Registra y arranca el cronograma de ejecución de trabajos automáticos."""
    # Se ejecuta cada 4 minutos (Balance ideal entre frescura y rendimiento)
    scheduler.add_job(
        refresh_today_metrics, 
        'interval', 
        minutes=4, 
        id='refresh_metrics_job', 
        replace_existing=True
    )
    scheduler.start()
    
    # Detona una primera ejecución asíncrona al arrancar el servidor (Cold Boot pre-warm)
    asyncio.create_task(refresh_today_metrics())