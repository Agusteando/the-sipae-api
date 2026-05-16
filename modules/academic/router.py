from fastapi import APIRouter, Query, HTTPException, Depends
from datetime import datetime
from zoneinfo import ZoneInfo
from core.dependencies import DateScopeParams
from core.cache import get_cache, set_cache
from .service import (
    get_observaciones_report,
    get_planeaciones_report,
    get_observaciones_docentes_report,
    get_planeaciones_pendientes_report,
)
from .schemas import (
    ObservacionesResponse,
    PlaneacionesResponse,
    ObservacionesDocentesResponse,
    PlaneacionesPendientesResponse,
)

router = APIRouter(prefix="/api/v1/academic", tags=["Academic Tracking"])

@router.get("/observaciones", response_model=ObservacionesResponse)
async def get_academic_observaciones(
    plantel: str = Query(..., description="Código de Sede (Ej: PT, SM)"),
    scope_params: DateScopeParams = Depends()
):
    try:
        cache_key = f"academic_obs_{plantel}_{scope_params.scope}"
        tz_mx = ZoneInfo("America/Mexico_City")
        
        # Estrategia SWR para optimización
        if scope_params.scope == "today" and not scope_params.force_refresh:
            cached = get_cache(cache_key)
            if cached:
                data = dict(cached["data"])
                data["meta"] = {"is_cached": True, "cached_at": cached["timestamp"].isoformat()}
                return data
                
        data = await get_observaciones_report(
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
        raise HTTPException(status_code=500, detail=f"Error al procesar observaciones académicas: {str(e)}")


@router.get("/planeaciones", response_model=PlaneacionesResponse)
async def get_academic_planeaciones(
    plantel: str = Query(..., description="Código de Sede (Ej: PT, SM)"),
    scope_params: DateScopeParams = Depends()
):
    try:
        cache_key = f"academic_plan_{plantel}_{scope_params.scope}"
        tz_mx = ZoneInfo("America/Mexico_City")
        
        if scope_params.scope == "today" and not scope_params.force_refresh:
            cached = get_cache(cache_key)
            if cached:
                data = dict(cached["data"])
                data["meta"] = {"is_cached": True, "cached_at": cached["timestamp"].isoformat()}
                return data

        data = await get_planeaciones_report(
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
        raise HTTPException(status_code=500, detail=f"Error al procesar planeaciones: {str(e)}")


@router.get("/observaciones/docentes", response_model=ObservacionesDocentesResponse)
async def get_academic_observaciones_docentes(
    plantel: str = Query(..., description="Código de Sede (Ej: PT, SM)"),
):
    try:
        tz_mx = ZoneInfo("America/Mexico_City")
        data = await get_observaciones_docentes_report(plantel=plantel)
        data["meta"] = {"is_cached": False, "cached_at": datetime.now(tz_mx).isoformat()}
        return data

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al procesar estatus de observaciones: {str(e)}")


@router.get("/planeaciones/pendientes-revision", response_model=PlaneacionesPendientesResponse)
async def get_academic_planeaciones_pendientes_revision(
    plantel: str = Query(..., description="Código de Sede (Ej: PT, SM)"),
    scope_params: DateScopeParams = Depends(),
):
    try:
        cache_key = f"academic_plan_pending_{plantel}_{scope_params.scope}"
        tz_mx = ZoneInfo("America/Mexico_City")

        if scope_params.scope == "today" and not scope_params.force_refresh:
            cached = get_cache(cache_key)
            if cached:
                data = dict(cached["data"])
                data["meta"] = {"is_cached": True, "cached_at": cached["timestamp"].isoformat()}
                return data

        data = await get_planeaciones_pendientes_report(
            plantel=plantel,
            start_date=scope_params.start_date,
            end_date=scope_params.end_date,
            scope=scope_params.scope,
        )

        if scope_params.scope == "today":
            set_cache(cache_key, data)

        data["meta"] = {"is_cached": False, "cached_at": datetime.now(tz_mx).isoformat()}
        return data

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al procesar planeaciones pendientes: {str(e)}")
