from datetime import date, datetime
from typing import Optional
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Query
from fastapi.responses import HTMLResponse

from core.cache import get_cache, set_cache
from .service import get_corporate_compliance_index
from .templates import CORPORATE_COMPLIANCE_HTML

router = APIRouter(tags=["Cumplimiento operativo"])


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


def _resolve_corporate_dates(scope: Optional[str], start_date: Optional[date], end_date: Optional[date]) -> tuple[str, date, date]:
    """
    This dashboard is month-first. Other API modules keep their existing
    defaults, but this page should open on the current month because daily
    SAPF/attendance reads are often too sparse to be useful.
    """
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

    # Month-to-date avoids treating future school days as missing attendance.
    return "month", today.replace(day=1), today


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
    cache_key = "corp_index:{}:{}:{}:{}:{}".format(
        resolved_scope,
        resolved_start.isoformat(),
        resolved_end.isoformat(),
        planteles or "ALL",
        "baseline" if include_baselines else "no-baseline",
    )

    if not force_refresh:
        cached = get_cache(cache_key)
        if cached and _cache_age_seconds(cached) < 120:
            data = dict(cached["data"])
            data["meta"] = {"is_cached": True, "cached_at": cached["timestamp"].isoformat()}
            return data

    data = await get_corporate_compliance_index(
        planteles=planteles,
        start_date=resolved_start,
        end_date=resolved_end,
        scope=resolved_scope,
        include_baselines=include_baselines,
    )
    data["meta"] = {"is_cached": False, "cached_at": datetime.now(ZoneInfo("America/Mexico_City")).isoformat()}
    set_cache(cache_key, data)
    return data
