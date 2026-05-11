from datetime import date, datetime
from typing import Optional
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, Query

from core.cache import get_cache, set_cache
from .service import DEFAULT_COMPARISON_MONTHS, DEFAULT_HISTORY_MONTHS, get_global_baseline_report

router = APIRouter(prefix="/api/v1/baselines", tags=["Baselines"])


@router.get("/plantel-performance")
async def get_plantel_performance_baselines(
    planteles: Optional[str] = Query(None, description="Lista separada por comas. Ej: PM,PT,SM,ST,PREET,PREEM"),
    start_date: Optional[date] = Query(None, description="Inicio de la ventana a comparar. Default: últimos 3 meses."),
    end_date: Optional[date] = Query(None, description="Fin de la ventana a comparar. Default: hoy."),
    months: int = Query(DEFAULT_COMPARISON_MONTHS, ge=1, le=6, description="Meses de comparación cuando no se envía start_date."),
    history_months: int = Query(DEFAULT_HISTORY_MONTHS, ge=1, le=9, description="Histórico máximo usado para baseline."),
    force_refresh: bool = Query(False, description="Omitir caché y recalcular síncronamente."),
):
    """
    Baselines históricos por plantel y métrica para dashboards globales.

    El endpoint compara la ventana solicitada contra hasta 9 meses previos de
    actividad real por plantel/métrica y devuelve expected values, bandas
    percentiles, score, status y severity para cada semana y para HOY.
    """
    try:
        tz_mx = ZoneInfo("America/Mexico_City")
        cache_end = end_date.isoformat() if end_date else datetime.now(tz_mx).date().isoformat()
        cache_key = "baseline_global_%s_%s_%s_%s_%s" % (
            planteles or "ACTIVE",
            start_date.isoformat() if start_date else f"months:{months}",
            cache_end,
            months,
            history_months,
        )

        if not force_refresh:
            cached = get_cache(cache_key)
            if cached:
                data = dict(cached["data"])
                data["meta"] = {"is_cached": True, "cached_at": cached["timestamp"].isoformat()}
                return data

        data = await get_global_baseline_report(
            planteles=planteles,
            start_date=start_date,
            end_date=end_date,
            comparison_months=months,
            history_months=history_months,
        )
        set_cache(cache_key, data)
        data["meta"] = {"is_cached": False, "cached_at": datetime.now(tz_mx).isoformat()}
        return data
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Error al calcular baselines históricos: {str(exc)}")
