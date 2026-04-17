from pydantic import BaseModel
from typing import Dict, List
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

# ==========================================
# RETARDOS (TARDIES) SCHEMAS
# ==========================================
class RetardoDetailModel(BaseModel):
    id: int
    student_fullname: str
    date: date
    time: str

class StudentRetardosResponse(BaseModel):
    matricula: str
    scope: str
    date_range: DateRangeModel
    total_retardos: int
    retardos: List[RetardoDetailModel]