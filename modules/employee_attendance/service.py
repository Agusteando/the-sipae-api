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

def _get_val_case_insensitive(record: Dict[str, Any], possible_keys: List[str]) -> Any:
    lower_record = {str(k).lower(): v for k, v in record.items()}
    for pk in possible_keys:
        pk_lower = pk.lower()
        if pk_lower in lower_record and lower_record[pk_lower]:
            return lower_record[pk_lower]
    return None

def _extract_employee_name(record: Dict[str, Any]) -> str:
    val = _get_val_case_insensitive(record, ["nombre", "empleado", "name", "employee", "trabajador", "colaborador"])
    return str(val).strip() if val else "Desconocido"

def _extract_employee_id(record: Dict[str, Any]) -> str:
    val = _get_val_case_insensitive(record, ["numero", "id", "matricula", "empleado_id", "clave", "num_empleado", "ingressioid"])
    return str(val).strip() if val else "N/A"

def _extract_date(record: Dict[str, Any]) -> str:
    val = _get_val_case_insensitive(record, ["fecha", "date", "timestamp", "dia", "fecha_registro"])
    return str(val).strip() if val else ""

async def get_kardex_attendance_report(plantel: str, start_date: date, end_date: date, scope: str) -> dict:
    plantel_info = resolve_plantel(plantel)
    norm_plantel = plantel_info["db_code"].upper()
    
    logger.info(f"Procesando asistencia de empleados para Plantel: {norm_plantel} ({start_date} -> {end_date})")
    
    mapped_areas = DB_CODE_TO_KARDEX_AREAS.get(norm_plantel, [norm_plantel])
    logger.info(f"Sub-áreas internas a consultar en Crossover API: {mapped_areas}")

    crossover_empleados = []
    used_fallback = False
    unmapped_areas = []
    
    # 1. Intento Principal: Consultar Endpoint Crossover iterando sobre las áreas mapeadas
    for area_target in mapped_areas:
        c_data = await fetch_crossover_records(start_date, end_date, plantel=area_target)
        if c_data and "empleados" in c_data:
            crossover_empleados.extend(c_data["empleados"])

    retardos_list = []
    ausencias_list = []
    status_field = "incidencia"

    if len(crossover_empleados) > 0:
        logger.info(f"Procesamiento Post-Crossover: Evaluando {len(crossover_empleados)} empleados consolidados.")
        
        for emp in crossover_empleados:
            identidad = emp.get("identidad", {})
            kpis = emp.get("kpis", {})
            
            # Aplicación de reglas de negocio exactas (Ignorando faltas/retardos justificados)
            unj_mins = kpis.get("unjMins", 0)
            unj_retardos = kpis.get("unjRetardos", 0)
            unj_faltas = kpis.get("unjFaltas", 0)
            raw_faltas = kpis.get("rawFaltas", 0)
            
            enriched = emp.get("enrichedKardex", [])
            detalle_hoy = enriched[0].get("rec", {}) if enriched else {}
            
            hora_entrada = detalle_hoy.get("registro_de_entrada", "Sin registro")
            incidencia = str(detalle_hoy.get("incidencia", "")).strip().lower()
            horario_asignado = identidad.get("kardex_raw", {}).get("horario", "N/A")
            
            is_retardo = (unj_mins > 0 or unj_retardos > 0)
            is_falta = (unj_faltas > 0 or raw_faltas > 0 or any(kw in incidencia for kw in ["falta", "ausencia", "injustificada", "no checo", "omision"]))
            
            # Prevención de falsos positivos en ausencias
            is_justified = ("pase" in incidencia or "justificad" in incidencia or "permiso" in incidencia)
            if is_justified and not (unj_faltas > 0):
                is_falta = False

            if is_retardo or is_falta:
                detail_obj = {
                    "employee_name": identidad.get("nombre", "Desconocido"),
                    "employee_id": identidad.get("ingressioId", "N/A"),
                    "area_raw": identidad.get("plantel", norm_plantel),
                    "plantel_normalized": norm_plantel,
                    "date": enriched[0].get("target_date", str(start_date)) if enriched else str(start_date),
                    "minutos_descontar": unj_mins,
                    "hora_entrada_real": hora_entrada,
                    "horario_asignado": horario_asignado,
                    "raw_status": "",
                    "raw_record": emp
                }
                
                if is_retardo:
                    detail_obj["raw_status"] = f"Retardo Injustificado ({unj_mins} min)"
                    retardos_list.append(detail_obj)
                elif is_falta:
                    detail_obj["raw_status"] = incidencia.title() if incidencia else "Falta No Justificada"
                    ausencias_list.append(detail_obj)

    else:
        # 2. Intento de Respaldo (Fallback Puro de Kardex)
        logger.warning("Carga de Crossover vacía. Iniciando extracción cruda de Kardex (Fallback).")
        used_fallback = True
        raw_records = []
        
        areas = await fetch_kardex_unique_areas()
        target_areas = []
        
        if areas:
            for a in areas:
                if a in mapped_areas:
                    target_areas.append(a)
                else:
                    unmapped_areas.append(a)
        else:
            target_areas = mapped_areas

        for area in target_areas:
            area_records = await fetch_kardex_records(start_date, end_date, area=area)
            if area_records:
                raw_records.extend(area_records)

        for rec in raw_records:
            emp_name = _extract_employee_name(rec)
            emp_id = _extract_employee_id(rec)
            rec_date = _extract_date(rec)
            
            raw_status_val = _get_val_case_insensitive(rec, ["estatus", "incidencia", "concepto", "estado", "tipo", "status", "descripcion", "observaciones"])
            raw_status = str(raw_status_val).strip().lower() if raw_status_val else ""

            hora_entrada = str(_get_val_case_insensitive(rec, ["registro_de_entrada", "hora_entrada", "entrada"]) or "Sin registro")

            detail_obj = {
                "employee_name": emp_name,
                "employee_id": emp_id,
                "area_raw": str(_get_val_case_insensitive(rec, ["area", "departamento", "seccion"]) or norm_plantel),
                "plantel_normalized": norm_plantel,
                "date": rec_date,
                "minutos_descontar": 0,
                "hora_entrada_real": hora_entrada,
                "horario_asignado": "N/A",
                "raw_status": raw_status.title() if raw_status else "Registro Normal",
                "raw_record": rec
            }

            if any(kw in raw_status for kw in ["retardo", "retraso", "minuto", "tarde", "demora"]):
                retardos_list.append(detail_obj)
            elif any(kw in raw_status for kw in ["falta", "ausencia", "injustificada", "no checo", "omision"]):
                ausencias_list.append(detail_obj)

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
                "mapped_kardex_areas": mapped_areas
            }
        }
    }