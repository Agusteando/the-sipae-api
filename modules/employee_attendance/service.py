from datetime import date
from typing import List, Dict, Any
from core.logger import get_logger
from core.utils import resolve_plantel
from integrations.kardex import (
    fetch_crossover_records,
    fetch_kardex_schema,
    fetch_kardex_unique_areas,
    fetch_kardex_records
)

logger = get_logger("service.employee_attendance")

def _extract_employee_name(record: Dict[str, Any]) -> str:
    for key in ["nombre", "empleado", "name", "employee", "trabajador"]:
        if key in record and record[key]:
            return str(record[key])
    return "Desconocido"

def _extract_employee_id(record: Dict[str, Any]) -> str:
    for key in ["numero", "id", "matricula", "empleado_id", "clave"]:
        if key in record and record[key]:
            return str(record[key])
    return "N/A"

def _extract_date(record: Dict[str, Any]) -> str:
    for key in ["fecha", "date", "timestamp", "dia"]:
        if key in record and record[key]:
            return str(record[key])
    return ""

async def get_kardex_attendance_report(plantel: str, start_date: date, end_date: date, scope: str) -> dict:
    """
    Fetches and processes external records replacing legacy fallback heuristics.
    Prioritizes Crossover payload, defaults to strict date constraints, and filters 
    tardies accurately using dynamic field detection instead of blind catch-alls.
    """
    plantel_info = resolve_plantel(plantel)
    norm_plantel = plantel_info["db_code"].upper()
    resolved_name = plantel_info["resolved_name"]

    logger.info(f"Processing Employee Crossover for Plantel: {norm_plantel} ({start_date} -> {end_date})")
    
    # 1. Primary Source: Exact Crossover Endpoint
    raw_records = await fetch_crossover_records(start_date, end_date, plantel=norm_plantel)
    logger.info(f"Exact crossover URL/params used: /api/crossover/plantel/{norm_plantel}?fecha_inicio={start_date.isoformat()}&fecha_fin={end_date.isoformat()}")
    logger.info(f"Raw Crossover response row count: {len(raw_records)}")
    
    used_fallback = False
    unmapped_areas = []

    # 2. Fallback execution only if Crossover is empty/unsupported for this day
    if not raw_records:
        logger.warning("Crossover payload empty or unavailable. Initiating strict Kardex fallback.")
        used_fallback = True
        
        schema = await fetch_kardex_schema()
        logger.info(f"Fetched Kardex schema for fallback mapping (Total columns: {len(schema.keys()) if schema else 0})")
        
        areas = await fetch_kardex_unique_areas()
        
        # Rigorous Plantel Area Mapping
        target_areas = []
        for a in areas:
            a_lower = str(a).lower()
            if norm_plantel.lower() in a_lower or resolved_name.lower() in a_lower:
                target_areas.append(a)
            else:
                unmapped_areas.append(a)

        logger.info(f"Fallback detected and mapped areas for {norm_plantel} ({resolved_name}): {target_areas}")

        for area in target_areas:
            area_records = await fetch_kardex_records(start_date, end_date, area=area)
            raw_records.extend(area_records)

        logger.info(f"Raw Fallback Kardex response row count: {len(raw_records)}")

    # 3. Dynamic Status Field Discovery (Prevents inventing schemas)
    status_field = "unknown"
    if raw_records:
        sample = raw_records[0]
        for key in ["estatus", "incidencia", "concepto", "estado", "tipo", "status", "descripcion"]:
            if key in sample:
                status_field = key
                break
    
    logger.info(f"Detected status/result field used to identify retardos: '{status_field}'")

    retardos_list = []
    ausencias_list = []
    sample_retardos = []

    # 4. Strict Filtering Logic
    for rec in raw_records:
        emp_name = _extract_employee_name(rec)
        emp_id = _extract_employee_id(rec)
        rec_date = _extract_date(rec)
        
        raw_status = str(rec.get(status_field, "")).strip().lower()
        
        # Fallback if the detected field is empty for this specific row
        if not raw_status:
            for key in ["estatus", "incidencia", "concepto", "estado", "tipo", "status", "descripcion"]:
                if key in rec and rec[key]:
                    raw_status = str(rec[key]).strip().lower()
                    break

        detail_obj = {
            "employee_name": emp_name,
            "employee_id": emp_id,
            "area_raw": str(rec.get("area", "") or rec.get("departamento", "") or norm_plantel),
            "plantel_normalized": norm_plantel,
            "date": rec_date,
            "raw_status": raw_status.title() if raw_status else "Registro Normal",
            "raw_record": rec
        }

        # Validate retardos intentionally, preventing regular attendances from counting as retardos
        if any(kw in raw_status for kw in ["retardo", "retraso", "minuto", "tarde", "demora"]):
            retardos_list.append(detail_obj)
            if len(sample_retardos) < 3:
                sample_retardos.append(detail_obj)
        elif any(kw in raw_status for kw in ["falta", "ausencia", "injustificada", "no checo", "omision"]):
            ausencias_list.append(detail_obj)

    logger.info(f"Filtered retardo count: {len(retardos_list)}")
    logger.info(f"Sample retardo rows: {sample_retardos}")

    return {
        "plantel": norm_plantel,
        "source_plantel_requested": plantel_info["plantel_requested"],
        "scope": scope,
        "date_range": {
            "start": start_date,
            "end": end_date
        },
        "summary": {
            "retardos_count": len(retardos_list),
            "ausencias_count": len(ausencias_list)
        },
        "retardos": retardos_list,
        "ausencias": ausencias_list,
        "debug": {
            "unmapped_areas": unmapped_areas if used_fallback else [],
            "source_filters": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "requested_plantel": norm_plantel,
                "used_fallback": used_fallback,
                "status_field_detected": status_field
            }
        }
    }