from fastapi import APIRouter, Query, HTTPException
from datetime import date, datetime
from typing import Optional
from zoneinfo import ZoneInfo
from core.cache import get_cache, set_cache
from .service import get_sapf_monthly_report, get_sapf_motivos_report, get_sapf_overview_report
from .schemas import SapfMonthlyResponse, SapfMotivosResponse

router = APIRouter(prefix="/api/v1/sapf", tags=["SAPF"])


def _resolve_sapf_dates(scope: Optional[str], start_date: Optional[date], end_date: Optional[date]) -> tuple[str, date, date]:
    tz_mx = ZoneInfo("America/Mexico_City")
    today = datetime.now(tz_mx).date()
    requested_scope = (scope or "month").lower()

    if requested_scope == "range":
        resolved_start = start_date or today.replace(day=1)
        resolved_end = end_date or today
        if resolved_end < resolved_start:
            resolved_start, resolved_end = resolved_end, resolved_start
        return "range", resolved_start, resolved_end

    if requested_scope == "today":
        return "today", today, today

    if requested_scope == "ciclo_escolar":
        start_year = today.year - 1 if today.month < 8 else today.year
        return "ciclo_escolar", date(start_year, 8, 1), today

    return "month", today.replace(day=1), today


def _cache_age_seconds(cache_entry) -> float:
    try:
        return (datetime.now(ZoneInfo("America/Mexico_City")) - cache_entry["timestamp"]).total_seconds()
    except Exception:
        return 999999.0


@router.get("/monthly-summary", response_model=SapfMonthlyResponse)
async def get_sapf_monthly(
    plantel: str = Query(..., description="Código de Sede (Ej: PT, SM)"),
    scope: Optional[str] = Query("month", description="Alcance SAPF: month por defecto; también today, range, ciclo_escolar"),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    force_refresh: bool = Query(False),
):
    try:
        resolved_scope, resolved_start, resolved_end = _resolve_sapf_dates(scope, start_date, end_date)
        cache_key = f"sapf_monthly_{plantel}_{resolved_scope}_{resolved_start}_{resolved_end}"
        tz_mx = ZoneInfo("America/Mexico_City")
        
        # Estrategia SWR para optimización en lecturas concurrentes
        if not force_refresh:
            cached = get_cache(cache_key)
            if cached and _cache_age_seconds(cached) < 120:
                data = dict(cached["data"])
                data["meta"] = {"is_cached": True, "cached_at": cached["timestamp"].isoformat()}
                return data
                
        data = await get_sapf_monthly_report(
            plantel=plantel, 
            start_date=resolved_start, 
            end_date=resolved_end,
            scope=resolved_scope
        )
        
        set_cache(cache_key, data)
            
        data["meta"] = {"is_cached": False, "cached_at": datetime.now(tz_mx).isoformat()}
        return data

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al procesar métricas SAPF: {str(e)}")


@router.get("/motivos-summary", response_model=SapfMotivosResponse)
async def get_sapf_motivos(
    plantel: str = Query(..., description="Código de Sede (Ej: PT, SM)"),
    scope: Optional[str] = Query("month", description="Alcance SAPF: month por defecto; también today, range, ciclo_escolar"),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    force_refresh: bool = Query(False),
):
    try:
        resolved_scope, resolved_start, resolved_end = _resolve_sapf_dates(scope, start_date, end_date)
        cache_key = f"sapf_motivos_{plantel}_{resolved_scope}_{resolved_start}_{resolved_end}"
        tz_mx = ZoneInfo("America/Mexico_City")
        
        # Estrategia SWR para optimización en lecturas concurrentes
        if not force_refresh:
            cached = get_cache(cache_key)
            if cached and _cache_age_seconds(cached) < 120:
                data = dict(cached["data"])
                data["meta"] = {"is_cached": True, "cached_at": cached["timestamp"].isoformat()}
                return data

        data = await get_sapf_motivos_report(
            plantel=plantel,
            start_date=resolved_start,
            end_date=resolved_end,
            scope=resolved_scope
        )
        
        set_cache(cache_key, data)
            
        data["meta"] = {"is_cached": False, "cached_at": datetime.now(tz_mx).isoformat()}
        return data

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al procesar motivos SAPF: {str(e)}")

@router.get("/overview-summary")
async def get_sapf_overview(
    plantel: str = Query(..., description="Código de Sede (Ej: PT, SM)"),
    scope: Optional[str] = Query("month", description="Alcance SAPF: month por defecto; también today, range, ciclo_escolar"),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    force_refresh: bool = Query(False),
):
    try:
        resolved_scope, resolved_start, resolved_end = _resolve_sapf_dates(scope, start_date, end_date)
        cache_key = f"sapf_overview_{plantel}_{resolved_scope}_{resolved_start}_{resolved_end}"
        tz_mx = ZoneInfo("America/Mexico_City")

        if not force_refresh:
            cached = get_cache(cache_key)
            if cached and _cache_age_seconds(cached) < 120:
                data = dict(cached["data"])
                data["meta"] = {"is_cached": True, "cached_at": cached["timestamp"].isoformat()}
                return data

        data = await get_sapf_overview_report(
            plantel=plantel,
            start_date=resolved_start,
            end_date=resolved_end,
            scope=resolved_scope
        )

        set_cache(cache_key, data)

        data["meta"] = {"is_cached": False, "cached_at": datetime.now(tz_mx).isoformat()}
        return data

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al procesar overview SAPF: {str(e)}")
