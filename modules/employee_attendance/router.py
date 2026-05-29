from fastapi import APIRouter, Query, HTTPException, Depends
from datetime import datetime
from zoneinfo import ZoneInfo
from core.dependencies import DateScopeParams
from core.cache import get_cache, set_cache
from .service import get_kardex_attendance_report
from .schemas import EmployeeAttendanceResponse

router = APIRouter(prefix="/api/v1/employee-attendance", tags=["Employee Labor Attendance"])

@router.get("/labor-attendance-report", response_model=EmployeeAttendanceResponse)
@router.get("/kardex-report", response_model=EmployeeAttendanceResponse, include_in_schema=False)
async def get_kardex_report(
    plantel: str = Query(..., description="Código de Sede (Ej: PT, SM) para control de asistencia laboral"),
    scope_params: DateScopeParams = Depends()
):
    try:
        cache_key = f"kardex_{plantel}"
        tz_mx = ZoneInfo("America/Mexico_City")
        
        # Caché rápido en memoria para aliviar el servicio externo de asistencia laboral
        if scope_params.scope == "today" and not scope_params.force_refresh:
            cached = get_cache(cache_key)
            if cached:
                data = dict(cached["data"])
                data["meta"] = {"is_cached": True, "cached_at": cached["timestamp"].isoformat()}
                return data

        # Llamada integradora
        data = await get_kardex_attendance_report(
            plantel=plantel,
            start_date=scope_params.start_date,
            end_date=scope_params.end_date,
            scope=scope_params.scope
        )
        
        if scope_params.scope == "today":
            set_cache(cache_key, data)
            
        data["meta"] = {"is_cached": False, "cached_at": datetime.now(tz_mx).isoformat()}
        return data

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al integrar con control de asistencia laboral: {str(e)}")