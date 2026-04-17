from pydantic import BaseModel
from typing import Dict, List, Any
from datetime import date

class EmployeeRecordDetail(BaseModel):
    employee_name: str
    employee_id: str
    area_raw: str
    plantel_normalized: str
    date: str
    raw_status: str
    raw_record: Dict[str, Any]

class SummaryCounts(BaseModel):
    retardos_count: int
    ausencias_count: int

class DebugInfo(BaseModel):
    unmapped_areas: List[str]
    source_filters: Dict[str, Any]

class EmployeeAttendanceResponse(BaseModel):
    plantel: str
    scope: str
    date_range: Dict[str, date]
    summary: SummaryCounts
    retardos: List[EmployeeRecordDetail]
    ausencias: List[EmployeeRecordDetail]
    debug: DebugInfo