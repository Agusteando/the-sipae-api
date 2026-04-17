from datetime import date
from typing import List, Dict, Any, Tuple
from core.logger import get_logger
from integrations.kardex import fetch_kardex_records, fetch_kardex_unique_areas

logger = get_logger("service.employee_attendance")

# Centralized Normalization Mapping
KARDEX_AREA_MAPPING = {
    "4 - PM": "PM",
    "14 - PT": "PT",
    "5 - SM": "SM",
    "2 - ST": "ST",
    "6 - CM": "CM",
    "3 - CT": "CT"
}

REVERSE_AREA_MAPPING = {v: k for k, v in KARDEX_AREA_MAPPING.items()}

def _normalize_area(raw_area: str) -> Tuple[str, bool]:
    """Returns (normalized_plantel_code, is_known_mapping)"""
    if not raw_area:
        return "UNKNOWN", False
    if raw_area in KARDEX_AREA_MAPPING:
        return KARDEX_AREA_MAPPING[raw_area], True
    
    # Fallback to loose text matching just in case Kardex format shifts slightly
    norm_raw = raw_area.lower()
    for k, v in KARDEX_AREA_MAPPING.items():
        if v.lower() in norm_raw:
            return v, True
            
    return "UNKNOWN", False

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

def _identify_status(record: Dict[str, Any]) -> Tuple[bool, bool, str]:
    """
    Heuristically evaluates the record to determine if it's a retardo or ausencia.
    Returns (is_retardo, is_ausencia, raw_status_text)
    """
    is_retardo = False
    is_ausencia = False
    status_text = ""

    # Look into typical status columns
    for key in ["estatus", "incidencia", "concepto", "estado", "tipo"]:
        if key in record and isinstance(record[key], str):
            val = record[key].lower()
            if "retardo" in val:
                is_retardo = True
                status_text = record[key]
            if "falta" in val or "ausencia" in val or "injustificada" in val:
                is_ausencia = True
                status_text = record[key]

    # If standard columns didn't catch it, scan all string fields just in case
    if not is_retardo and not is_ausencia:
        for k, v in record.items():
            if isinstance(v, str):
                val = v.lower()
                if "retardo" in val:
                    is_retardo = True
                    status_text = v
                    break
                if "falta" in val or "ausencia" in val:
                    is_ausencia = True
                    status_text = v
                    break

    return is_retardo, is_ausencia, status_text

async def get_kardex_attendance_report(plantel: str, start_date: date, end_date: date, scope: str) -> dict:
    """
    Fetches and processes external Kardex records, filtering strictly by the defined date scope
    and requested Plantel through the explicit normalization layer.
    """
    logger.info(f"Processing Employee Kardex for Plantel: {plantel} ({start_date} -> {end_date})")

    # 1. Resolve Kardex specific Area Code
    kardex_area = REVERSE_AREA_MAPPING.get(plantel.upper())
    
    # 2. Fetch remote records
    raw_records = await fetch_kardex_records(start_date, end_date, area=kardex_area)
    
    # Run a debug check for background logging
    try:
        unique_areas_remote = await fetch_kardex_unique_areas()
        logger.info(f"Available Kardex Areas remotely: {unique_areas_remote}")
    except Exception:
        pass

    retardos_list = []
    ausencias_list = []
    unmapped_areas_found = set()

    for rec in raw_records:
        # Standardize area
        rec_area = str(rec.get("area", "") or rec.get("Area", ""))
        norm_plantel, is_known = _normalize_area(rec_area)
        
        if not is_known and rec_area:
            unmapped_areas_found.add(rec_area)

        # Enforce Plantel filter locally to prevent Kardex API bleeding data
        if norm_plantel != plantel.upper():
            continue
            
        # Parse logic
        emp_name = _extract_employee_name(rec)
        emp_id = _extract_employee_id(rec)
        rec_date = _extract_date(rec)
        is_retardo, is_ausencia, raw_status = _identify_status(rec)

        detail_obj = {
            "employee_name": emp_name,
            "employee_id": emp_id,
            "area_raw": rec_area,
            "plantel_normalized": norm_plantel,
            "date": rec_date,
            "raw_status": raw_status,
            "raw_record": rec
        }

        if is_retardo:
            retardos_list.append(detail_obj)
        elif is_ausencia:
            ausencias_list.append(detail_obj)

    return {
        "plantel": plantel.upper(),
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
            "unmapped_areas": list(unmapped_areas_found),
            "source_filters": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "requested_area": kardex_area
            }
        }
    }