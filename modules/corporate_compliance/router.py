from datetime import date, datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Query
from fastapi.responses import HTMLResponse

from core.cache import get_cache, set_cache
from .service import get_corporate_compliance_index
from .templates import CORPORATE_COMPLIANCE_HTML

router = APIRouter(tags=["Cumplimiento operativo"])
DASHBOARD_DATA_VERSION = "2026-06-22-executive-1-100-v4"


@router.get("/corporate-compliance-risk-index", response_class=HTMLResponse, include_in_schema=False)
async def serve_corporate_compliance_dashboard():
    return HTMLResponse(CORPORATE_COMPLIANCE_HTML)


@router.get("/indice-corporativo-cumplimiento", response_class=HTMLResponse, include_in_schema=False)
async def serve_indice_corporativo_cumplimiento():
    return HTMLResponse(CORPORATE_COMPLIANCE_HTML)


@router.get("/cumplimiento-operativo", response_class=HTMLResponse, include_in_schema=False)
async def serve_cumplimiento_operativo():
    return HTMLResponse(CORPORATE_COMPLIANCE_HTML)


@router.get("/tablero-operativo", response_class=HTMLResponse, include_in_schema=False)
async def serve_tablero_operativo():
    return HTMLResponse(CORPORATE_COMPLIANCE_HTML)


def _last_completed_operational_day(now: datetime) -> date:
    """Return the latest school day that should be considered closed.

    The operational dashboard should not penalize the current day while lists,
    access scans, SAPF notes, and academic reviews are still being captured.
    Health reports can still call `today`; the global dashboard defaults to the
    last completed weekday before the afternoon cut-off.
    """
    candidate = now.date()
    if now.weekday() >= 5 or now.hour < 15:
        candidate = candidate - timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate = candidate - timedelta(days=1)
    return candidate


def _resolve_corporate_dates(scope: Optional[str], start_date: Optional[date], end_date: Optional[date]) -> tuple[str, date, date]:
    """Month-first defaults, using the last completed school day for safety."""
    tz_mx = ZoneInfo("America/Mexico_City")
    now = datetime.now(tz_mx)
    today = now.date()
    completed_day = _last_completed_operational_day(now)
    requested_scope = (scope or "month").lower()

    if requested_scope == "range":
        resolved_start = start_date or today.replace(day=1)
        resolved_end = end_date or completed_day
        if resolved_end < resolved_start:
            resolved_start, resolved_end = resolved_end, resolved_start
        return "range", resolved_start, resolved_end

    if requested_scope == "today":
        return "today", today, today

    if requested_scope == "ciclo_escolar":
        start_year = completed_day.year - 1 if completed_day.month < 8 else completed_day.year
        return "ciclo_escolar", date(start_year, 8, 1), completed_day

    month_start = completed_day.replace(day=1)
    if completed_day < month_start:
        completed_day = today
        month_start = today.replace(day=1)
    return "month", month_start, completed_day


def _cache_age_seconds(cache_entry) -> float:
    try:
        tz_mx = ZoneInfo("America/Mexico_City")
        return (datetime.now(tz_mx) - cache_entry["timestamp"]).total_seconds()
    except Exception:
        return 999999.0


@router.get("/api/v1/corporate-compliance-risk-index")
async def get_corporate_compliance_dashboard_data(
    planteles: Optional[str] = Query(None, description="Lista separada por comas en orden fijo: PT,PM,ST,SM,PREET,PREEM"),
    scope: Optional[str] = Query("month", description="Alcance: month por defecto; también acepta today, range, ciclo_escolar."),
    start_date: Optional[date] = Query(None, description="Fecha de inicio si scope=range"),
    end_date: Optional[date] = Query(None, description="Fecha de fin si scope=range"),
    include_baselines: bool = Query(False, description="Incluye comparación histórica. Desactivado por defecto para evitar consultas pesadas."),
    force_refresh: bool = Query(False, description="Omitir caché del tablero."),
):
    resolved_scope, resolved_start, resolved_end = _resolve_corporate_dates(scope, start_date, end_date)
    cache_key = "corp_index:{}:{}:{}:{}:{}:{}".format(
        resolved_scope,
        resolved_start.isoformat(),
        resolved_end.isoformat(),
        planteles or "ALL",
        "baseline" if include_baselines else "no-baseline",
        DASHBOARD_DATA_VERSION,
    )

    if not force_refresh:
        cached = get_cache(cache_key)
        if cached and _cache_age_seconds(cached) < 120:
            data = dict(cached["data"])
            meta = dict(data.get("meta") or {})
            meta.update({"is_cached": True, "cached_at": cached["timestamp"].isoformat()})
            data["meta"] = meta
            return data

    data = await get_corporate_compliance_index(
        planteles=planteles,
        start_date=resolved_start,
        end_date=resolved_end,
        scope=resolved_scope,
        include_baselines=include_baselines,
    )
    meta = dict(data.get("meta") or {})
    meta.update({"is_cached": False, "cached_at": datetime.now(ZoneInfo("America/Mexico_City")).isoformat()})
    data["meta"] = meta
    set_cache(cache_key, data)
    return data


@router.get("/api/v1/corporate-compliance-risk-index/debug")
async def get_corporate_compliance_source_debug(
    planteles: Optional[str] = Query(None, description="Lista separada por comas en orden fijo: PT,PM,ST,SM,PREET,PREEM"),
    scope: Optional[str] = Query("month", description="Alcance: month por defecto; también acepta today, range, ciclo_escolar."),
    start_date: Optional[date] = Query(None, description="Fecha de inicio si scope=range"),
    end_date: Optional[date] = Query(None, description="Fecha de fin si scope=range"),
):
    resolved_scope, resolved_start, resolved_end = _resolve_corporate_dates(scope, start_date, end_date)
    data = await get_corporate_compliance_index(
        planteles=planteles,
        start_date=resolved_start,
        end_date=resolved_end,
        scope=resolved_scope,
        include_baselines=False,
    )
    return {
        "scope": data.get("scope"),
        "window": (data.get("aggregate") or {}).get("window"),
        "selected_planteles": data.get("selected_planteles"),
        "source_audit": data.get("source_audit") or (data.get("aggregate") or {}).get("source_audit"),
        "planteles": [
            {
                "plantel": item.get("plantel"),
                "resolved_name": item.get("resolved_name"),
                "source_audit": item.get("source_audit"),
                "source_errors": item.get("source_errors"),
            }
            for item in data.get("planteles") or []
        ],
    }
