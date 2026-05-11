from datetime import date
from core.utils import resolve_plantel
from core.logger import get_logger
from .repository import get_sapf_monthly_stats, get_sapf_motivos_stats

logger = get_logger("service.sapf")

async def get_sapf_monthly_report(plantel: str, start_date: date, end_date: date, scope: str) -> dict:
    """
    Lógica de negocio para consolidar y mapear los reportes mensuales de Fichas de Atención (SAPF)
    por Área Departamental.
    """
    plantel_info = resolve_plantel(plantel)
    db_code = plantel_info["db_code"]
    
    logger.info(f"Procesando SAPF Mensual para {plantel_info['resolved_name']} ({start_date} -> {end_date})")
    
    results = await get_sapf_monthly_stats(db_code, start_date, end_date)
    
    areas_map = {}
    for row in results:
        area = row.get("area") or "Sin Área"
        if area not in areas_map:
            areas_map[area] = {"area": str(area), "monthly_data": [], "total_conteo": 0}
        
        conteo = row.get("conteo", 0)
        areas_map[area]["monthly_data"].append({
            "period": row.get("period", ""),
            "year": row.get("year", 0),
            "month": row.get("month", 0),
            "conteo": conteo
        })
        areas_map[area]["total_conteo"] += conteo

    data_list = list(areas_map.values())
    
    return {
        "plantel_requested": plantel_info["plantel_requested"],
        "resolved_name": plantel_info["resolved_name"],
        "scope": scope,
        "date_range": {"start": start_date, "end": end_date},
        "data": data_list
    }

async def get_sapf_motivos_report(plantel: str, start_date: date, end_date: date, scope: str) -> dict:
    """
    Lógica de negocio para agrupar e identificar las incidencias y motivos más comunes 
    registrados en el sistema SAPF.
    """
    plantel_info = resolve_plantel(plantel)
    db_code = plantel_info["db_code"]
    
    logger.info(f"Procesando SAPF Motivos para {plantel_info['resolved_name']} ({start_date} -> {end_date})")
    
    results = await get_sapf_motivos_stats(db_code, start_date, end_date)
    
    motivos_list = []
    for row in results:
        motivos_list.append({
            "area": str(row.get("area") or "Sin Área"),
            "motivo": str(row.get("motivo") or "Sin Motivo"),
            "conteo": row.get("conteo", 0)
        })

    return {
        "plantel_requested": plantel_info["plantel_requested"],
        "resolved_name": plantel_info["resolved_name"],
        "scope": scope,
        "date_range": {"start": start_date, "end": end_date},
        "motivos": motivos_list
    }