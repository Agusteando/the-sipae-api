import asyncio
from datetime import date, datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, Query

from core.cache import get_cache, set_cache
from .service import DEFAULT_COMPARISON_MONTHS, DEFAULT_HISTORY_MONTHS, get_global_baseline_report

router = APIRouter(prefix="/api/v1/baselines", tags=["Baselines"])

_CACHE_TTL = timedelta(minutes=45)
_STALE_TTL = timedelta(hours=6)
_PENDING: dict[str, asyncio.Task] = {}
_PENDING_LOCK = asyncio.Lock()


def _now_mx() -> datetime:
    return datetime.now(ZoneInfo("America/Mexico_City"))


def _cache_state(cache_key: str):
    cached = get_cache(cache_key)
    if not cached:
        return None, False
    age = _now_mx() - cached["timestamp"]
    if age <= _CACHE_TTL:
        return cached, False
    if age <= _STALE_TTL:
        return cached, True
    return None, False


def _with_meta(cached: dict, is_cached: bool, stale: bool = False):
    data = dict(cached["data"])
    data["meta"] = {
        "is_cached": is_cached,
        "is_stale": stale,
        "cached_at": cached["timestamp"].isoformat(),
    }
    return data


@router.get("/plantel-performance")
async def get_plantel_performance_baselines(
    planteles: Optional[str] = Query(None, description="Lista separada por comas. Ej: PM,PT,SM,ST,PREET,PREEM"),
    start_date: Optional[date] = Query(None, description="Inicio de la ventana a comparar. Default: últimos 3 meses."),
    end_date: Optional[date] = Query(None, description="Fin de la ventana a comparar. Default: hoy."),
    months: int = Query(DEFAULT_COMPARISON_MONTHS, ge=1, le=6, description="Meses de comparación cuando no se envía start_date."),
    history_months: int = Query(DEFAULT_HISTORY_MONTHS, ge=1, le=9, description="Histórico máximo usado para baseline."),
    force_refresh: bool = Query(False, description="Omitir caché y recalcular síncronamente."),
):
    """Baselines históricos por plantel y métrica para dashboards globales."""
    tz_mx = ZoneInfo("America/Mexico_City")
    cache_end = end_date.isoformat() if end_date else datetime.now(tz_mx).date().isoformat()
    cache_key = "baseline_global_%s_%s_%s_%s_%s" % (
        planteles or "ACTIVE",
        start_date.isoformat() if start_date else f"months:{months}",
        cache_end,
        months,
        history_months,
    )

    cached, stale = _cache_state(cache_key)
    if cached and not force_refresh and not stale:
        return _with_meta(cached, True, False)

    async with _PENDING_LOCK:
        pending = _PENDING.get(cache_key)
        if pending is None:
            pending = asyncio.create_task(get_global_baseline_report(
                planteles=planteles,
                start_date=start_date,
                end_date=end_date,
                comparison_months=months,
                history_months=history_months,
            ))
            _PENDING[cache_key] = pending

    try:
        data = await pending
        set_cache(cache_key, data)
        data["meta"] = {"is_cached": False, "is_stale": False, "cached_at": _now_mx().isoformat()}
        return data
    except Exception as exc:
        if cached and not force_refresh:
            return _with_meta(cached, True, True)
        raise HTTPException(status_code=500, detail=f"Error al calcular baselines históricos: {str(exc)}")
    finally:
        async with _PENDING_LOCK:
            if _PENDING.get(cache_key) is pending:
                _PENDING.pop(cache_key, None)
