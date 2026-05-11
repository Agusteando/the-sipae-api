from datetime import date
from core.utils import resolve_plantel
from core.logger import get_logger
from .repository import get_sapf_monthly_stats, get_sapf_motivos_stats

logger = get_logger("service.sapf")


async def get_sapf_monthly_report(plantel: str, start_date: date, end_date: date, scope: str) -> dict:
    """
    Consolida SAPF con la semántica legacy: fichas_atencion + seguimiento,
    agrupado por departamento mapeado en deptos_map y por mes.
    """
    plantel_info = resolve_plantel(plantel)
    map_campus = plantel_info["sapf_map_campus"]
    data_campuses = plantel_info["sapf_data_campuses"]

    logger.info(
        "Procesando SAPF para %s (%s -> %s). map_campus=%s data_campuses=%s",
        plantel_info["resolved_name"],
        start_date,
        end_date,
        map_campus,
        data_campuses,
    )

    results = await get_sapf_monthly_stats(map_campus, data_campuses, start_date, end_date)

    areas_map = {}
    for row in results:
        area = str(row.get("department_name") or "Sin Departamento").strip() or "Sin Departamento"
        period = str(row.get("period") or "")
        key = (area, period)
        conteo = int(row.get("conteo") or 0)

        if area not in areas_map:
            areas_map[area] = {"area": area, "monthly_data": {}, "total_conteo": 0}

        if key not in areas_map[area]["monthly_data"]:
            areas_map[area]["monthly_data"][key] = {
                "period": period,
                "year": int(row.get("year") or 0),
                "month": int(row.get("month") or 0),
                "conteo": 0,
            }

        areas_map[area]["monthly_data"][key]["conteo"] += conteo
        areas_map[area]["total_conteo"] += conteo

    data_list = []
    for area_obj in areas_map.values():
        monthly_data = list(area_obj["monthly_data"].values())
        monthly_data.sort(key=lambda item: (item["year"], item["month"], item["period"]))
        data_list.append(
            {
                "area": area_obj["area"],
                "monthly_data": monthly_data,
                "total_conteo": area_obj["total_conteo"],
            }
        )

    data_list.sort(key=lambda item: (-item["total_conteo"], item["area"]))

    return {
        "plantel_requested": plantel_info["plantel_requested"],
        "resolved_name": plantel_info["resolved_name"],
        "scope": scope,
        "date_range": {"start": start_date, "end": end_date},
        "data": data_list,
    }


async def get_sapf_motivos_report(plantel: str, start_date: date, end_date: date, scope: str) -> dict:
    """
    Agrupa motivos SAPF con school_code exacto y deptos_map. PM/PREEM aliases
    se consolidan en la capa de servicio para evitar duplicados.
    """
    plantel_info = resolve_plantel(plantel)
    map_campus = plantel_info["sapf_map_campus"]
    data_campuses = plantel_info["sapf_data_campuses"]

    logger.info(
        "Procesando SAPF Motivos para %s (%s -> %s). map_campus=%s data_campuses=%s",
        plantel_info["resolved_name"],
        start_date,
        end_date,
        map_campus,
        data_campuses,
    )

    results = await get_sapf_motivos_stats(map_campus, data_campuses, start_date, end_date)

    motivo_map = {}
    for row in results:
        area = str(row.get("department_name") or "Sin Departamento").strip() or "Sin Departamento"
        motivo = str(row.get("motivo") or "Sin Motivo").strip() or "Sin Motivo"
        key = (area, motivo)
        motivo_map[key] = motivo_map.get(key, 0) + int(row.get("conteo") or 0)

    motivos_list = [
        {"area": area, "motivo": motivo, "conteo": conteo}
        for (area, motivo), conteo in motivo_map.items()
    ]
    motivos_list.sort(key=lambda item: (-item["conteo"], item["area"], item["motivo"]))

    return {
        "plantel_requested": plantel_info["plantel_requested"],
        "resolved_name": plantel_info["resolved_name"],
        "scope": scope,
        "date_range": {"start": start_date, "end": end_date},
        "motivos": motivos_list,
    }
