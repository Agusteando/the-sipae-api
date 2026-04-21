from fastapi import APIRouter, Query, HTTPException, Depends
from datetime import datetime
from core.dependencies import DateScopeParams
from core.cache import get_cache, set_cache
from .service import calculate_husky_daily_rate, get_plantel_retardos
from .schemas import HuskyDailyRateResponse, PlantelRetardosResponse

router = APIRouter(prefix="/api/v1/husky", tags=["Husky Pass"])

@router.get("/scans/daily-rate", response_model=HuskyDailyRateResponse)
async def get_husky_daily_rate(
    plantel: str = Query(..., description="Código de Sede (Ej: PT, SM)"),
    scope_params: DateScopeParams = Depends()
):
    try:
        cache_key = f"husky_rate_{plantel}"
        
        # Estrategia de Caché SWR para métricas de hoy
        if scope_params.scope == "today" and not scope_params.force_refresh:
            cached = get_cache(cache_key)
            if cached:
                data = dict(cached["data"])
                data["meta"] = {"is_cached": True, "cached_at": cached["timestamp"].isoformat()}
                return data
                
        # Procesamiento síncrono profundo
        data = await calculate_husky_daily_rate(
            plantel=plantel, 
            start_date=scope_params.start_date, 
            end_date=scope_params.end_date,
            scope=scope_params.scope
        )
        
        if scope_params.scope == "today":
            set_cache(cache_key, data)
            
        data["meta"] = {"is_cached": False, "cached_at": datetime.now().isoformat()}
        return data

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error de base de datos: {str(e)}")


@router.get("/retardos", response_model=PlantelRetardosResponse)
async def fetch_plantel_retardos_endpoint(
    plantel: str = Query(..., description="Código de Sede (Ej: PT, SM)"),
    scope_params: DateScopeParams = Depends()
):
    try:
        cache_key = f"husky_retardos_{plantel}"
        
        if scope_params.scope == "today" and not scope_params.force_refresh:
            cached = get_cache(cache_key)
            if cached:
                data = dict(cached["data"])
                data["meta"] = {"is_cached": True, "cached_at": cached["timestamp"].isoformat()}
                return data

        data = await get_plantel_retardos(
            plantel=plantel,
            start_date=scope_params.start_date,
            end_date=scope_params.end_date,
            scope=scope_params.scope
        )
        
        if scope_params.scope == "today":
            set_cache(cache_key, data)
            
        data["meta"] = {"is_cached": False, "cached_at": datetime.now().isoformat()}
        return data

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error de base de datos: {str(e)}")