from pydantic import BaseModel
from typing import List, Dict, Optional, Any
from datetime import date

class DateRangeModel(BaseModel):
    start: date
    end: date

class ObservacionesSummary(BaseModel):
    total_observaciones: int
    observaciones_con_comentarios: int

class ObservacionDailyTrend(BaseModel):
    date: str
    total: int
    with_comments: int

class FeedbackItem(BaseModel):
    docente: str
    date: str
    comment: str

class ObservacionesResponse(BaseModel):
    plantel_requested: str
    resolved_name: str
    scope: str
    date_range: DateRangeModel
    summary: ObservacionesSummary
    daily_trend: List[ObservacionDailyTrend]
    feedback_list: List[FeedbackItem]
    meta: Optional[Dict[str, Any]] = None

class PlaneacionesSummary(BaseModel):
    total_planeaciones: int
    planeaciones_con_feedback: int

class PlaneacionDailyTrend(BaseModel):
    date: str
    total: int
    with_feedback: int

class PlaneacionesResponse(BaseModel):
    plantel_requested: str
    resolved_name: str
    scope: str
    date_range: DateRangeModel
    summary: PlaneacionesSummary
    daily_trend: List[PlaneacionDailyTrend]
    feedback_list: List[FeedbackItem]
    meta: Optional[Dict[str, Any]] = None