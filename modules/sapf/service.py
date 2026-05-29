from datetime import date
from decimal import Decimal
from typing import Any

from core.utils import resolve_plantel
from core.logger import get_logger
from .repository import get_sapf_monthly_stats, get_sapf_motivos_stats, get_sapf_overview_stats

logger = get_logger("service.sapf")


def _int(value: Any) -> int:
    try:
        if value is None or value == "":
            return 0
        return int(value)
    except Exception:
        return 0


def _float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        if isinstance(value, Decimal):
            return float(value)
        return float(value)
    except Exception:
        return None


def _sapf_identity(plantel: str) -> dict:
    plantel_info = resolve_plantel(plantel)
    return {
        "plantel_info": plantel_info,
        "map_campus": plantel_info["sapf_map_campus"],
        "data_campuses": plantel_info["sapf_data_campuses"],
    }


async def get_sapf_monthly_report(plantel: str, start_date: date, end_date: date, scope: str) -> dict:
    """
    Consolida SAPF con la estructura real del sistema Next.js: fichas_atencion +
    seguimiento, filtrando plantel de forma normalizada contra school_code,
    campus, plantel o escuela según existan en la base.
    """
    identity = _sapf_identity(plantel)
    plantel_info = identity["plantel_info"]
    map_campus = identity["map_campus"]
    data_campuses = identity["data_campuses"]

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
    source_breakdown = {"ficha": 0, "seguimiento": 0}
    for row in results:
        area = str(row.get("department_name") or "Sin Departamento").strip() or "Sin Departamento"
        period = str(row.get("period") or "")
        source = str(row.get("source") or "ficha")
        key = (area, period)
        conteo = _int(row.get("conteo"))
        source_breakdown[source] = source_breakdown.get(source, 0) + conteo

        if area not in areas_map:
            areas_map[area] = {"area": area, "monthly_data": {}, "total_conteo": 0, "sources": {}}

        if key not in areas_map[area]["monthly_data"]:
            areas_map[area]["monthly_data"][key] = {
                "period": period,
                "year": _int(row.get("year")),
                "month": _int(row.get("month")),
                "conteo": 0,
            }

        areas_map[area]["monthly_data"][key]["conteo"] += conteo
        areas_map[area]["total_conteo"] += conteo
        areas_map[area]["sources"][source] = areas_map[area]["sources"].get(source, 0) + conteo

    data_list = []
    for area_obj in areas_map.values():
        monthly_data = list(area_obj["monthly_data"].values())
        monthly_data.sort(key=lambda item: (item["year"], item["month"], item["period"]))
        data_list.append(
            {
                "area": area_obj["area"],
                "monthly_data": monthly_data,
                "total_conteo": area_obj["total_conteo"],
                "sources": area_obj["sources"],
            }
        )

    data_list.sort(key=lambda item: (-item["total_conteo"], item["area"]))

    return {
        "plantel_requested": plantel_info["plantel_requested"],
        "resolved_name": plantel_info["resolved_name"],
        "scope": scope,
        "date_range": {"start": start_date, "end": end_date},
        "map_campus": map_campus,
        "data_campuses": data_campuses,
        "source_breakdown": source_breakdown,
        "data": data_list,
    }


async def get_sapf_motivos_report(plantel: str, start_date: date, end_date: date, scope: str) -> dict:
    """
    Agrupa motivos SAPF con la estructura real del repo SAPF. El motivo primario
    viene de fichas_atencion.reason, no de una columna fija motivo.
    """
    identity = _sapf_identity(plantel)
    plantel_info = identity["plantel_info"]
    map_campus = identity["map_campus"]
    data_campuses = identity["data_campuses"]

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
        source = str(row.get("source") or "ficha")
        key = (area, motivo, source)
        motivo_map[key] = motivo_map.get(key, 0) + _int(row.get("conteo"))

    motivos_list = [
        {"area": area, "motivo": motivo, "source": source, "conteo": conteo}
        for (area, motivo, source), conteo in motivo_map.items()
    ]
    motivos_list.sort(key=lambda item: (-item["conteo"], item["area"], item["motivo"], item["source"]))

    return {
        "plantel_requested": plantel_info["plantel_requested"],
        "resolved_name": plantel_info["resolved_name"],
        "scope": scope,
        "date_range": {"start": start_date, "end": end_date},
        "map_campus": map_campus,
        "data_campuses": data_campuses,
        "motivos": motivos_list,
    }


async def get_sapf_overview_report(plantel: str, start_date: date, end_date: date, scope: str) -> dict:
    """Resumen de trazabilidad SAPF para tablero ejecutivo y endpoint diagnóstico."""
    identity = _sapf_identity(plantel)
    plantel_info = identity["plantel_info"]
    map_campus = identity["map_campus"]
    data_campuses = identity["data_campuses"]

    stats = await get_sapf_overview_stats(map_campus, data_campuses, start_date, end_date)
    total_fichas = _int(stats.get("total_fichas"))
    total_followups = _int(stats.get("total_followups"))
    total_interactions = total_fichas + total_followups
    open_cases = _int(stats.get("open_cases"))
    closed_cases = _int(stats.get("closed_cases"))
    complaints = _int(stats.get("complaints"))
    parent_origin_cases = _int(stats.get("parent_origin_cases"))
    avg_resolution_hours = _float(stats.get("avg_resolution_hours"))
    followup_ratio = round((total_followups / total_fichas) * 100, 2) if total_fichas > 0 else 0.0
    open_case_rate = round((open_cases / total_fichas) * 100, 2) if total_fichas > 0 else 0.0

    return {
        "plantel_requested": plantel_info["plantel_requested"],
        "resolved_name": plantel_info["resolved_name"],
        "scope": scope,
        "date_range": {"start": start_date, "end": end_date},
        "map_campus": map_campus,
        "data_campuses": data_campuses,
        "matched_campus_values": stats.get("matched_campus_values") or [],
        "total_fichas": total_fichas,
        "total_followups": total_followups,
        "total_interactions": total_interactions,
        "open_cases": open_cases,
        "closed_cases": closed_cases,
        "complaints": complaints,
        "parent_origin_cases": parent_origin_cases,
        "avg_resolution_hours": avg_resolution_hours,
        "followup_ratio_percent": followup_ratio,
        "open_case_rate_percent": open_case_rate,
        "areas_from_fichas": stats.get("areas_from_fichas") or [],
    }
