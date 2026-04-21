from pydantic import BaseModel
from typing import Dict, List, Optional, Any
from datetime import date

class DateRangeModel(BaseModel):
    start: date
    end: date

class DailyDatapointModel(BaseModel):
    entrada: int
    salida: int
    rate_entrada_percent: float

class HuskyDailyRateResponse(BaseModel):
    plantel_requested: str
    resolved_name: str
    expected_population: int
    scope: str
    date_range: DateRangeModel
    daily_datapoints: Dict[str, DailyDatapointModel]
    meta: Optional[Dict[str, Any]] = None

# ==========================================
# RETARDOS (TARDIES) SCHEMAS
# ==========================================
class RetardoDetailModel(BaseModel):
    id: int
    student_fullname: str
    matricula: str
    date: date
    time: str

class PlantelRetardosResponse(BaseModel):
    plantel_requested: str
    resolved_name: str
    scope: str
    date_range: DateRangeModel
    total_retardos: int
    retardos: List[RetardoDetailModel]
    meta: Optional[Dict[str, Any]] = None