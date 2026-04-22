from fastapi import APIRouter, Query, HTTPException, Depends
from datetime import datetime
from zoneinfo import ZoneInfo
from core.dependencies import DateScopeParams
from core.cache import get_cache, set_cache
from .service import get_attendance_detail_report
from .schemas import AttendanceDetailResponse

router = APIRouter(prefix="/api/v1/attendance", tags=["Attendance"])

@router.get("/detail", response_model=AttendanceDetailResponse)
async def get_attendance_detail(
    plantel: str = Query(..., description="Código de Sede (Ej: PT, SM)"),
    scope_params: DateScopeParams = Depends()
):
    try:
        cache_key = f"attendance_{plantel}"
        tz_mx = ZoneInfo("America/Mexico_City")
        
        # Estrategia SWR para lecturas del panel hoy
        if scope_params.scope == "today" and not scope_params.force_refresh:
            cached = get_cache(cache_key)
            if cached:
                data = dict(cached["data"])
                data["meta"] = {"is_cached": True, "cached_at": cached["timestamp"].isoformat()}
                return data

        # Extracción y cálculo síncrono nativo
        data = await get_attendance_detail_report(
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
        raise HTTPException(status_code=500, detail=f"Error en la ejecución de base de datos: {str(e)}")