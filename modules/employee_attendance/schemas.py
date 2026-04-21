from pydantic import BaseModel
from typing import Dict, List, Any, Optional
from datetime import date

class EmployeeRecordDetail(BaseModel):
    employee_name: str
    employee_id: str
    area_raw: str
    plantel_normalized: str
    date: str
    raw_status: str
    minutos_descontar: int = 0
    hora_entrada_real: str = "Sin registro"
    horario_asignado: str = "N/A"
    raw_record: Dict[str, Any]

class SummaryCounts(BaseModel):
    retardos_count: int
    ausencias_count: int

class DebugInfo(BaseModel):
    unmapped_areas: List[str]
    source_filters: Dict[str, Any]

class EmployeeAttendanceResponse(BaseModel):
    plantel: str
    source_plantel_requested: str
    scope: str
    date_range: Dict[str, date]
    summary: SummaryCounts
    retardos: List[EmployeeRecordDetail]
    ausencias: List[EmployeeRecordDetail]
    debug: DebugInfo
    meta: Optional[Dict[str, Any]] = None