from pydantic import BaseModel
from typing import Dict
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
    date_range: DateRangeModel
    daily_datapoints: Dict[str, DailyDatapointModel]