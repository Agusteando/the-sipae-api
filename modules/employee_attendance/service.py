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

# Mapeo interno riguroso de códigos estándar a áreas específicas dentro de Kardex
DB_CODE_TO_KARDEX_AREAS = {
    "PT": ["14 - PT"],
    "PM": ["4 - PM", "9 - Preesco Met", "7 - DM"],
    "ST": ["2 - ST"],
    "SM": ["5 - SM"],
    "CT": ["3 - CT", "13 - IST"],
    "CM": ["6 - CM", "8 - ISM"]
}

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
    crossover_data = await fetch_crossover_records(start_date, end_date, plantel=norm_plantel)
    logger.info(f"Exact crossover URL/params used: /api/crossover/plantel/{norm_plantel}?fecha_inicio={start_date.isoformat()}&fecha_fin={end_date.isoformat()}")
    
    used_fallback = False
    unmapped_areas = []
    
    retardos_list = []
    ausencias_list = []
    sample_retardos = []
    retardos_count = 0
    ausencias_count = 0
    status_field = "incidencia"

    if crossover_data and "empleados" in crossover_data:
        logger.info("Crossover API responded properly with structured payload.")
        empleados = crossover_data.get("empleados", [])
        kpis = crossover_data.get("kpis_agregados", {})
        
        retardos_count = kpis.get("rawRetardos", 0)
        ausencias_count = kpis.get("rawFaltas", 0)
        
        for emp in empleados:
            identidad = emp.get("identidad", {})
            emp_name = identidad.get("nombre", "Desconocido")
            emp_id = identidad.get("ingressioId", "N/A")
            area_raw = identidad.get("plantel", norm_plantel)
            
            for day in emp.get("enrichedKardex", []):
                t_date = day.get("target_date")
                has_retardo = day.get("hasRetardo", False)
                
                rec = day.get("rec", {})
                incidencia = str(rec.get("incidencia", "")).strip().lower()
                
                detail_obj = {
                    "employee_name": emp_name,
                    "employee_id": emp_id,
                    "area_raw": area_raw,
                    "plantel_normalized": norm_plantel,
                    "date": t_date,
                    "raw_status": incidencia.title() if incidencia else ("Retardo" if has_retardo else "Registro Normal"),
                    "raw_record": day
                }

                if has_retardo:
                    retardos_list.append(detail_obj)
                    if len(sample_retardos) < 3:
                        sample_retardos.append(detail_obj)
                elif any(kw in incidencia for kw in ["falta", "ausencia", "injustificada", "no checo", "omision"]):
                    ausencias_list.append(detail_obj)
                    
        # Update ausencias count manually if the endpoint didn't provide rawFaltas
        if ausencias_count == 0 and len(ausencias_list) > 0:
            ausencias_count = len(ausencias_list)

    else:
        # 2. Fallback execution applying exact area mappings explicitly mapped to Kardex Values
        logger.warning("Crossover payload empty or unavailable. Initiating strict Kardex fallback.")
        used_fallback = True
        raw_records = []
        
        schema = await fetch_kardex_schema()
        logger.info(f"Fetched Kardex schema for fallback mapping (Total columns: {len(schema.keys()) if schema else 0})")
        
        areas = await fetch_kardex_unique_areas()
        
        target_areas = []
        mapped_areas = DB_CODE_TO_KARDEX_AREAS.get(norm_plantel, [])
        
        # Rigorous Plantel Area Mapping assignment
        if areas:
            for a in areas:
                if a in mapped_areas:
                    target_areas.append(a)
                else:
                    unmapped_areas.append(a)
        else:
            # If unique areas fetch fails, bypass dynamic check and fallback to directly querying mapped expected areas
            target_areas = mapped_areas

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

        # 4. Strict Filtering Logic for Fallback
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

            if any(kw in raw_status for kw in ["retardo", "retraso", "minuto", "tarde", "demora"]):
                retardos_list.append(detail_obj)
                if len(sample_retardos) < 3:
                    sample_retardos.append(detail_obj)
            elif any(kw in raw_status for kw in ["falta", "ausencia", "injustificada", "no checo", "omision"]):
                ausencias_list.append(detail_obj)

        retardos_count = len(retardos_list)
        ausencias_count = len(ausencias_list)

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
            "retardos_count": retardos_count,
            "ausencias_count": ausencias_count
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
                "status_field_detected": status_field,
                "mapped_kardex_areas": DB_CODE_TO_KARDEX_AREAS.get(norm_plantel, [])
            }
        }
    }