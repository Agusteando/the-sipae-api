from datetime import date
from typing import List, Dict, Any
from core.logger import get_logger
from integrations.kardex import fetch_crossover_records

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
    Fetches and processes external Crossover records natively replacing old legacy Kardex fallback heuristics.
    Strictly follows explicit dates and uses exact Crossover payload returns.
    """
    logger.info(f"Processing Employee Crossover for Plantel: {plantel} ({start_date} -> {end_date})")

    norm_plantel = plantel.upper()
    logger.info(f"Resolved norm_plantel: {norm_plantel} for crossover API request.")
    
    raw_records = await fetch_crossover_records(start_date, end_date, plantel=norm_plantel)
    logger.info(f"Crossover API returned {len(raw_records)} records.")
    
    retardos_list = []
    ausencias_list = []

    for rec in raw_records:
        emp_name = _extract_employee_name(rec)
        emp_id = _extract_employee_id(rec)
        rec_date = _extract_date(rec)
        
        # Explicit status mapping trusting the Crossover payload structure directly
        raw_status = ""
        for key in ["estatus", "incidencia", "concepto", "estado", "tipo"]:
            if key in rec and isinstance(rec[key], str):
                raw_status = rec[key]
                break

        detail_obj = {
            "employee_name": emp_name,
            "employee_id": emp_id,
            "area_raw": str(rec.get("area", "") or rec.get("departamento", "") or norm_plantel),
            "plantel_normalized": norm_plantel,
            "date": rec_date,
            "raw_status": raw_status or "Registro Crossover",
            "raw_record": rec
        }

        # Segregate ausencias vs standard crossover entries (assumed primarily retardos natively)
        if raw_status and ("falta" in raw_status.lower() or "ausencia" in raw_status.lower()):
            ausencias_list.append(detail_obj)
        else:
            retardos_list.append(detail_obj)

    logger.info(f"Final filtered logic applied: {len(retardos_list)} retardos, {len(ausencias_list)} ausencias.")

    return {
        "plantel": norm_plantel,
        "source_plantel_requested": norm_plantel,
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
            "unmapped_areas": [],
            "source_filters": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "requested_plantel": norm_plantel
            }
        }
    }