from pydantic import BaseModel
from typing import List, Dict, Optional, Any
from datetime import date

class DateRangeModel(BaseModel):
    start: date
    end: date

class SapfMonthlyRecord(BaseModel):
    period: str
    year: int
    month: int
    conteo: int

class SapfAreaMonthlyData(BaseModel):
    area: str
    monthly_data: List[SapfMonthlyRecord]
    total_conteo: int
    sources: Optional[Dict[str, int]] = None

class SapfMonthlyResponse(BaseModel):
    plantel_requested: str
    resolved_name: str
    scope: str
    date_range: DateRangeModel
    data: List[SapfAreaMonthlyData]
    meta: Optional[Dict[str, Any]] = None

class SapfMotivoRecord(BaseModel):
    area: str
    motivo: str
    conteo: int
    source: Optional[str] = None

class SapfMotivosResponse(BaseModel):
    plantel_requested: str
    resolved_name: str
    scope: str
    date_range: DateRangeModel
    motivos: List[SapfMotivoRecord]
    meta: Optional[Dict[str, Any]] = None