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


class ObservacionDocenteStatusItem(BaseModel):
    docente: str
    username: Optional[str] = None
    email: Optional[str] = None
    campus: Optional[str] = None
    nivel: Optional[str] = None
    last_observed_at: str
    latest_observers: List[str]
    total_observaciones_ultimos_30_dias: int


class DocenteMenosObservadoItem(BaseModel):
    docente: str
    username: Optional[str] = None
    email: Optional[str] = None
    campus: Optional[str] = None
    nivel: Optional[str] = None
    total_observaciones_ciclo: int


class CicloEscolarModel(BaseModel):
    label: str
    date_range: DateRangeModel


class DocenteSinObservacionItem(BaseModel):
    docente: str
    username: Optional[str] = None
    email: Optional[str] = None
    campus: Optional[str] = None
    nivel: Optional[str] = None
    last_observed_at: Optional[str] = None
    days_since_last_observation: Optional[int] = None
    total_observaciones_ciclo: int
    status: str


class ObservacionesDocentesSummary(BaseModel):
    total_docentes_observados: int
    total_docentes_activos: int
    total_docentes_sin_observacion_30_dias: int
    total_docentes_nunca_observados_ciclo: int
    window_days: int
    active_window_days: int


class ObservacionesDocentesResponse(BaseModel):
    plantel_requested: str
    resolved_name: str
    scope: str
    date_range: DateRangeModel
    ciclo_escolar: CicloEscolarModel
    summary: ObservacionesDocentesSummary
    docentes: List[ObservacionDocenteStatusItem]
    docentes_sin_observacion: List[DocenteSinObservacionItem]
    docente_menos_observado: Optional[DocenteMenosObservadoItem] = None
    meta: Optional[Dict[str, Any]] = None


class ActiveWindowModel(BaseModel):
    start: date
    end: date
    days: int


class PlaneacionesPendientesSummary(BaseModel):
    total_planeaciones_pendientes: int
    docentes_activos: int
    docentes_con_planeaciones_pendientes: int


class PlaneacionPendienteItem(BaseModel):
    id: int
    docente: str
    username: Optional[str] = None
    email: Optional[str] = None
    week: Optional[str] = None
    ciclo: Optional[str] = None
    created_at: str
    weekEnd: Optional[str] = None
    nivel: Optional[str] = None
    campus: Optional[str] = None
    revisa: Optional[str] = None
    revisa2: Optional[str] = None
    revisa3: Optional[str] = None
    feedback: Optional[str] = None
    feedback2: Optional[str] = None
    feedback3: Optional[str] = None


class PlaneacionesPendientesResponse(BaseModel):
    plantel_requested: str
    resolved_name: str
    scope: str
    date_range: DateRangeModel
    active_window: ActiveWindowModel
    summary: PlaneacionesPendientesSummary
    planeaciones_pendientes: List[PlaneacionPendienteItem]
    meta: Optional[Dict[str, Any]] = None
