from fastapi import APIRouter, Query, HTTPException, Depends
from datetime import datetime
from zoneinfo import ZoneInfo
from core.dependencies import DateScopeParams
from core.cache import get_cache, set_cache
from .service import get_sapf_monthly_report, get_sapf_motivos_report
from .schemas import SapfMonthlyResponse, SapfMotivosResponse

router = APIRouter(prefix="/api/v1/sapf", tags=["SAPF"])

@router.get("/monthly-summary", response_model=SapfMonthlyResponse)
async def get_sapf_monthly(
    plantel: str = Query(..., description="Código de Sede (Ej: PT, SM)"),
    scope_params: DateScopeParams = Depends()
):
    try:
        cache_key = f"sapf_monthly_{plantel}_{scope_params.scope}"
        tz_mx = ZoneInfo("America/Mexico_City")
        
        # Estrategia SWR para optimización en lecturas concurrentes
        if scope_params.scope == "today" and not scope_params.force_refresh:
            cached = get_cache(cache_key)
            if cached:
                data = dict(cached["data"])
                data["meta"] = {"is_cached": True, "cached_at": cached["timestamp"].isoformat()}
                return data
                
        data = await get_sapf_monthly_report(
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
        raise HTTPException(status_code=500, detail=f"Error al procesar métricas SAPF: {str(e)}")


@router.get("/motivos-summary", response_model=SapfMotivosResponse)
async def get_sapf_motivos(
    plantel: str = Query(..., description="Código de Sede (Ej: PT, SM)"),
    scope_params: DateScopeParams = Depends()
):
    try:
        cache_key = f"sapf_motivos_{plantel}_{scope_params.scope}"
        tz_mx = ZoneInfo("America/Mexico_City")
        
        # Estrategia SWR para optimización en lecturas concurrentes
        if scope_params.scope == "today" and not scope_params.force_refresh:
            cached = get_cache(cache_key)
            if cached:
                data = dict(cached["data"])
                data["meta"] = {"is_cached": True, "cached_at": cached["timestamp"].isoformat()}
                return data

        data = await get_sapf_motivos_report(
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
        raise HTTPException(status_code=500, detail=f"Error al procesar motivos SAPF: {str(e)}")