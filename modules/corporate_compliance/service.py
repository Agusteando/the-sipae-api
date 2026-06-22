from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional
from zoneinfo import ZoneInfo

from core.logger import get_logger
from core.utils import resolve_plantel
from modules.employee_attendance.service import get_kardex_attendance_report
from modules.sapf.service import get_sapf_overview_report
from integrations.external_bot import fetch_expected_groups, fetch_expected_population

from .report_repository import (
    count_active_academic_teachers,
    fetch_attendance_rollup,
    fetch_husky_daily_scan_counts,
    fetch_husky_tardy_daily_counts,
    fetch_observation_monthly_totals,
    fetch_planning_review_totals,
)
from .scoring import (
    METRIC_WEIGHTS,
    average_score,
    bounded_inverse_rate,
    clamp_score,
    pct,
    safe_float,
    safe_int,
    traffic_for_score,
    weighted_score,
)

logger = get_logger("service.corporate_compliance")

FIXED_PLANTEL_ORDER = ["PT", "PM", "ST", "SM", "PREET", "PREEM"]
DISPLAY_METRICS = [
    "general",
    "roll_call",
    "student_attendance",
    "scans",
    "scan_balance",
    "student_punctuality",
    "staff_attendance",
    "planning",
    "observations",
    "observation_coverage",
    "sapf",
]
TREND_METRICS = ["general", "roll_call", "student_attendance", "scans", "scan_balance", "student_punctuality"]
SOURCE_TIMEOUTS = {
    "attendance": 26.0,
    "husky": 18.0,
    "retardos": 18.0,
    "staff_attendance": 18.0,
    "academic": 18.0,
    "sapf": 12.0,
}


def _mx_now() -> datetime:
    return datetime.now(ZoneInfo("America/Mexico_City"))


def _normalize_planteles(planteles: Optional[str]) -> List[str]:
    if not planteles:
        return list(FIXED_PLANTEL_ORDER)
    requested = {item.strip().upper() for item in str(planteles).split(",") if item.strip()}
    selected = [code for code in FIXED_PLANTEL_ORDER if code in requested]
    return selected or list(FIXED_PLANTEL_ORDER)


def _business_days(start_date: date, end_date: date) -> List[date]:
    days: List[date] = []
    cursor = start_date
    while cursor <= end_date:
        if cursor.weekday() < 5:
            days.append(cursor)
        cursor += timedelta(days=1)
    return days


def _weeks_touched(days: Iterable[date]) -> int:
    weeks = {(day.isocalendar().year, day.isocalendar().week) for day in days}
    return len(weeks)


def _week_window_for_period(start_date: date, end_date: date) -> tuple[date, date]:
    """Full planning-week window touched by the selected report period."""
    start_week = start_date - timedelta(days=start_date.weekday())
    end_week = end_date + timedelta(days=6 - end_date.weekday())
    return start_week, end_week


def _month_window_for(value: date) -> tuple[date, date]:
    return value.replace(day=1), value


def _date_key(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, date):
        return value.isoformat()
    raw = str(value)
    if len(raw) >= 10:
        return raw[:10]
    return raw or None


def _as_list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


async def _safe_call(
    name: str,
    fn: Callable[[], Awaitable[Dict[str, Any]]],
    timeout_seconds: float,
) -> Dict[str, Any]:
    started = time.perf_counter()
    try:
        payload = await asyncio.wait_for(fn(), timeout=timeout_seconds)
        if not isinstance(payload, dict):
            payload = {"value": payload}
        payload["_ok"] = True
        payload["_duration_ms"] = round((time.perf_counter() - started) * 1000, 1)
        return payload
    except asyncio.TimeoutError:
        message = f"{name} no respondió en {timeout_seconds:.0f}s"
        logger.error("Corporate report source timed out: %s", message)
        return {"_ok": False, "error": message, "timeout": True, "_duration_ms": round((time.perf_counter() - started) * 1000, 1)}
    except Exception as exc:
        logger.error("Corporate report source failed: %s: %s", name, exc)
        return {"_ok": False, "error": str(exc), "_duration_ms": round((time.perf_counter() - started) * 1000, 1)}


def _metric(score: Optional[float], label: str, detail: str, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    normalized = clamp_score(score)
    traffic = traffic_for_score(normalized)
    payload: Dict[str, Any] = {
        "score": normalized,
        "status": traffic["status"],
        "label": label,
        "detail": detail,
        "color": traffic["color"],
        "traffic_label": traffic["label"],
    }
    if extra:
        payload.update(extra)
    return payload


def _unavailable(label: str = "—", detail: str = "Sin denominador real") -> Dict[str, Any]:
    return _metric(None, label, detail)


def _daily_attendance_points(payload: Dict[str, Any], business_days: Iterable[date]) -> List[Dict[str, Any]]:
    allowed = {d.isoformat() for d in business_days}
    points: List[Dict[str, Any]] = []

    if payload.get("mode") == "daily" and payload.get("summary") is not None:
        day = _date_key((payload.get("date_range") or {}).get("start"))
        if day and day in allowed:
            summary = payload.get("summary") or {}
            missing = payload.get("missing_groups_data") or {}
            points.append({
                "date": day,
                "records": safe_int(summary.get("total_students")),
                "present": safe_int(summary.get("asistencia")),
                "absent": safe_int(summary.get("ausencia")),
                "expected_lists": safe_int(missing.get("expected_groups_count")),
                "completed_lists": safe_int(missing.get("completed_groups_count")),
                "missing_lists": safe_int(missing.get("missing_groups_count")),
            })
        return points

    for raw_day, day_payload in (payload.get("daily_points") or {}).items():
        day = _date_key(raw_day)
        if not day or day not in allowed:
            continue
        summary = day_payload.get("summary") or {}
        missing = day_payload.get("missing_groups_data") or {}
        points.append({
            "date": day,
            "records": safe_int(summary.get("total_students")),
            "present": safe_int(summary.get("asistencia")),
            "absent": safe_int(summary.get("ausencia")),
            "expected_lists": safe_int(missing.get("expected_groups_count")),
            "completed_lists": safe_int(missing.get("completed_groups_count")),
            "missing_lists": safe_int(missing.get("missing_groups_count")),
        })
    points.sort(key=lambda item: item["date"])
    return points


def _attendance_metrics(payload: Dict[str, Any], business_days: List[date]) -> tuple[Dict[str, Any], Dict[str, Any], List[Dict[str, Any]]]:
    if not payload.get("_ok", True) or payload.get("error"):
        return _unavailable("—", "Asistencia no respondió"), _unavailable("—", "Pase de lista no respondió"), []

    points = _daily_attendance_points(payload, business_days)
    total_records = sum(safe_int(p.get("records")) for p in points)
    present = sum(safe_int(p.get("present")) for p in points)
    absent = sum(safe_int(p.get("absent")) for p in points)
    expected_lists = sum(safe_int(p.get("expected_lists")) for p in points)
    completed_lists = sum(safe_int(p.get("completed_lists")) for p in points)
    missing_lists = sum(safe_int(p.get("missing_lists")) for p in points)

    student_attendance_score = pct(present, total_records)
    roll_call_score = pct(completed_lists, expected_lists)

    student_attendance = _metric(
        student_attendance_score,
        f"{present:,} presentes de {total_records:,} registros" if total_records > 0 else "—",
        f"{absent:,} ausencias registradas" if total_records > 0 else "Sin registros de asistencia",
        {"present": present, "records": total_records, "absent": absent},
    )
    roll_call = _metric(
        roll_call_score,
        f"{completed_lists:,} de {expected_lists:,} grupos capturados" if expected_lists > 0 else "—",
        f"{missing_lists:,} grupos/días faltantes" if expected_lists > 0 else "Sin grupos esperados",
        {"expected": expected_lists, "completed": completed_lists, "missing": missing_lists},
    )
    return roll_call, student_attendance, points


def _husky_daily_points(payload: Dict[str, Any], business_days: Iterable[date]) -> Dict[str, Dict[str, Any]]:
    allowed = {d.isoformat() for d in business_days}
    output: Dict[str, Dict[str, Any]] = {}
    expected_population = safe_int(payload.get("expected_population"))
    for raw_day, day_payload in (payload.get("daily_datapoints") or {}).items():
        day = _date_key(raw_day)
        if not day or day not in allowed:
            continue
        output[day] = {
            "entrada": safe_int(day_payload.get("entrada")),
            "salida": safe_int(day_payload.get("salida")),
            "expected_population": expected_population,
        }
    return output


def _scans_metric(payload: Dict[str, Any], business_days: List[date]) -> tuple[Dict[str, Any], List[Dict[str, Any]]]:
    if not payload.get("_ok", True) or payload.get("error"):
        return _unavailable("—", "Escaneos no respondió"), []
    expected_population = safe_int(payload.get("expected_population"))
    if expected_population <= 0 or not business_days:
        return _unavailable("—", "Sin población esperada"), []

    daily_map = _husky_daily_points(payload, business_days)
    expected = expected_population * len(business_days)
    entries = sum(safe_int((daily_map.get(day.isoformat()) or {}).get("entrada")) for day in business_days)
    score = pct(min(entries, expected), expected)
    daily = []
    for day in business_days:
        key = day.isoformat()
        day_entries = safe_int((daily_map.get(key) or {}).get("entrada"))
        daily.append({"date": key, "entries": day_entries, "expected": expected_population, "score": pct(min(day_entries, expected_population), expected_population)})
    return _metric(
        score,
        f"{entries:,} entradas de {expected:,} esperadas",
        f"Población esperada: {expected_population:,}",
        {"entries": entries, "expected": expected, "expected_population": expected_population},
    ), daily


def _tardy_counts(payload: Dict[str, Any], business_days: Iterable[date]) -> Dict[str, int]:
    allowed = {d.isoformat() for d in business_days}
    counts: Dict[str, int] = defaultdict(int)
    for row in _as_list(payload.get("retardos")):
        day = _date_key(row.get("date"))
        if day and day in allowed:
            counts[day] += 1
    return dict(counts)


def _student_punctuality_metric(
    husky_payload: Dict[str, Any],
    tardies_payload: Dict[str, Any],
    attendance_daily: List[Dict[str, Any]],
    business_days: List[date],
) -> tuple[Dict[str, Any], List[Dict[str, Any]]]:
    if not tardies_payload.get("_ok", True) or tardies_payload.get("error"):
        return _unavailable("—", "Retardos no respondió"), []

    expected_population = safe_int(husky_payload.get("expected_population")) if husky_payload.get("_ok", True) else 0
    attendance_by_day = {item.get("date"): item for item in attendance_daily}
    tardy_by_day = _tardy_counts(tardies_payload, business_days)
    total_records = sum(safe_int(item.get("records")) for item in attendance_daily)
    total_tardies = sum(safe_int(v) for v in tardy_by_day.values())

    opportunities = expected_population * len(business_days) if expected_population > 0 else total_records
    score = bounded_inverse_rate(total_tardies, opportunities)
    daily: List[Dict[str, Any]] = []
    for day in business_days:
        key = day.isoformat()
        day_opportunities = expected_population if expected_population > 0 else safe_int((attendance_by_day.get(key) or {}).get("records"))
        day_tardies = safe_int(tardy_by_day.get(key))
        daily.append({"date": key, "tardies": day_tardies, "opportunities": day_opportunities, "score": bounded_inverse_rate(day_tardies, day_opportunities)})

    return _metric(
        score,
        f"{total_tardies:,} retardos" if opportunities > 0 else "—",
        f"{opportunities:,} oportunidades alumno/día" if opportunities > 0 else "Sin población ni registros para comparar",
        {"tardies": total_tardies, "opportunities": opportunities},
    ), daily




def _attendance_aliases(plantel_info: Dict[str, Any]) -> List[str]:
    values = (
        list(plantel_info.get("db_codes") or [])
        + list(plantel_info.get("sapf_data_campuses") or [])
        + [plantel_info.get("resolved_name", ""), plantel_info.get("short_name", "")]
    )
    out: List[str] = []
    seen = set()
    for value in values:
        clean = str(value or "").strip()
        key = clean.upper()
        if clean and key not in seen:
            seen.add(key)
            out.append(clean)
    return out or [str(plantel_info.get("db_code") or "")]


def _husky_values(plantel_info: Dict[str, Any]) -> List[str]:
    values = (
        list(plantel_info.get("husky_db_codes") or [])
        + list(plantel_info.get("db_codes") or [])
        + list(plantel_info.get("sapf_data_campuses") or [])
        + [plantel_info.get("resolved_name", ""), plantel_info.get("short_name", "")]
    )
    out: List[str] = []
    seen = set()
    for value in values:
        clean = str(value or "").strip()
        key = clean.upper()
        if clean and key not in seen:
            seen.add(key)
            out.append(clean)
    return out or [str(plantel_info.get("db_code") or "")]


def _tardy_threshold(plantel_info: Dict[str, Any]) -> str:
    db_code = str(plantel_info.get("db_code") or "").upper()
    if db_code in {"PM", "PT"}:
        return "08:01:00"
    if db_code in {"SM", "ST"}:
        return "07:01:00"
    return "09:01:00"


async def _safe_value_call(name: str, fn: Callable[[], Awaitable[Any]], timeout_seconds: float) -> Dict[str, Any]:
    started = time.perf_counter()
    try:
        value = await asyncio.wait_for(fn(), timeout=timeout_seconds)
        return {"_ok": True, "value": value, "_duration_ms": round((time.perf_counter() - started) * 1000, 1)}
    except asyncio.TimeoutError:
        message = f"{name} no respondió en {timeout_seconds:.0f}s"
        logger.error("Corporate report source timed out: %s", message)
        return {"_ok": False, "value": None, "error": message, "timeout": True, "_duration_ms": round((time.perf_counter() - started) * 1000, 1)}
    except Exception as exc:
        logger.error("Corporate report source failed: %s: %s", name, exc)
        return {"_ok": False, "value": None, "error": str(exc), "_duration_ms": round((time.perf_counter() - started) * 1000, 1)}


def _attendance_points_from_rollup(rows: List[Dict[str, Any]], business_days: List[date], expected_group_count: Optional[int]) -> List[Dict[str, Any]]:
    allowed = {day.isoformat() for day in business_days}
    by_day: Dict[str, Dict[str, Any]] = {}
    groups_by_day: Dict[str, set[tuple[str, str]]] = defaultdict(set)
    precomputed_groups: Dict[str, int] = {}

    for row in rows or []:
        day = _date_key(row.get("d_fecha"))
        if not day or day not in allowed:
            continue
        item = by_day.setdefault(day, {"date": day, "records": 0, "present": 0, "absent": 0, "completed_lists": 0, "expected_lists": 0})
        item["records"] += safe_int(row.get("records"))
        item["present"] += safe_int(row.get("present"))
        item["absent"] += safe_int(row.get("absent"))

        if row.get("completed_lists") is not None:
            precomputed_groups[day] = max(precomputed_groups.get(day, 0), safe_int(row.get("completed_lists")))
        else:
            grado = str(row.get("grado") or "").strip()
            grupo = str(row.get("grupo") or "").strip()
            if grado and grupo:
                groups_by_day[day].add((grado, grupo))

    daily_expected = expected_group_count if expected_group_count and expected_group_count > 0 else None
    for day in business_days:
        key = day.isoformat()
        item = by_day.setdefault(key, {"date": key, "records": 0, "present": 0, "absent": 0, "completed_lists": 0, "expected_lists": 0})
        completed = precomputed_groups.get(key, len(groups_by_day.get(key) or set()))
        item["completed_lists"] = completed
        item["expected_lists"] = daily_expected if daily_expected is not None else 0
    return [by_day[day.isoformat()] for day in business_days]


def _attendance_metrics_from_rollup(
    rows: List[Dict[str, Any]],
    expected_groups_result: Dict[str, Any],
    business_days: List[date],
) -> tuple[Dict[str, Any], Dict[str, Any], List[Dict[str, Any]]]:
    expected_groups = expected_groups_result.get("value") if expected_groups_result.get("_ok") else None
    bot_expected_count = len(expected_groups) if isinstance(expected_groups, list) else 0
    points = _attendance_points_from_rollup(rows, business_days, bot_expected_count or None)
    observed_max_groups = max((safe_int(p.get("completed_lists")) for p in points), default=0)
    if bot_expected_count > 0:
        basis = "bot_expected_groups"
        daily_expected = bot_expected_count
    elif observed_max_groups > 0:
        basis = "observed_max_groups"
        daily_expected = observed_max_groups
    else:
        basis = "none"
        daily_expected = 0

    if daily_expected > 0:
        for item in points:
            item["expected_lists"] = daily_expected

    total_records = sum(safe_int(p.get("records")) for p in points)
    present = sum(safe_int(p.get("present")) for p in points)
    absent = sum(safe_int(p.get("absent")) for p in points)
    expected_lists = daily_expected * len(business_days) if daily_expected > 0 else 0
    completed_lists = sum(min(safe_int(p.get("completed_lists")), daily_expected or safe_int(p.get("completed_lists"))) for p in points)
    missing_lists = max(expected_lists - completed_lists, 0) if expected_lists > 0 else 0

    attendance_daily_scores = [pct(p.get("present"), p.get("records")) for p in points if safe_int(p.get("records")) > 0]
    roll_call_daily_scores = [
        pct(min(safe_int(p.get("completed_lists")), safe_int(p.get("expected_lists"))), p.get("expected_lists"))
        for p in points
        if safe_int(p.get("expected_lists")) > 0
    ]

    student_attendance = _metric(
        average_score(attendance_daily_scores),
        f"{present:,} presentes de {total_records:,} registros" if total_records > 0 else "—",
        f"Promedio diario del periodo · {absent:,} ausencias" if total_records > 0 else "Sin registros de asistencia",
        {"present": present, "records": total_records, "absent": absent, "basis": "daily_attendance_average", "days_with_records": len(attendance_daily_scores)},
    )
    roll_call_score = average_score(roll_call_daily_scores)
    roll_call = _metric(
        roll_call_score,
        f"{completed_lists:,} de {expected_lists:,} grupos/día capturados" if expected_lists > 0 else "—",
        f"Promedio diario del periodo · {missing_lists:,} grupos/día faltantes" if expected_lists > 0 else "Sin grupos esperados",
        {
            "expected": expected_lists,
            "completed": completed_lists,
            "missing": missing_lists,
            "basis": basis,
            "bot_groups": bot_expected_count,
            "observed_max_groups": observed_max_groups,
            "days_with_records": len(roll_call_daily_scores),
        },
    )
    return roll_call, student_attendance, points


def _scans_metric_from_rows(
    rows: List[Dict[str, Any]],
    population_result: Dict[str, Any],
    attendance_daily: List[Dict[str, Any]],
    business_days: List[date],
) -> tuple[Dict[str, Any], Dict[str, Any], List[Dict[str, Any]]]:
    expected_population = safe_int(population_result.get("value")) if population_result.get("_ok") else 0
    by_day: Dict[str, Dict[str, int]] = defaultdict(lambda: {"entrada": 0, "salida": 0})
    for row in rows or []:
        day = _date_key(row.get("fecha"))
        tipo = str(row.get("tipo_accion") or "").strip().lower()
        if day and tipo in {"entrada", "salida"}:
            by_day[day][tipo] += safe_int(row.get("total_scans"))

    entries_by_business_day = [safe_int((by_day.get(day.isoformat()) or {}).get("entrada")) for day in business_days]
    observed_max_entries = max(entries_by_business_day, default=0)
    if expected_population > 0:
        daily_expected_base = expected_population
        basis = "daily_population_reference"
    elif observed_max_entries > 0:
        daily_expected_base = observed_max_entries
        basis = "observed_max_daily_entries"
    else:
        daily_expected_base = 0
        basis = "none"

    expected = daily_expected_base * len(business_days) if daily_expected_base > 0 else 0
    entries = 0
    exits = 0
    gap_total = 0
    daily_scan_scores: List[Optional[float]] = []
    daily_balance_scores: List[Optional[float]] = []
    days_with_scan_records = 0
    daily: List[Dict[str, Any]] = []
    for day in business_days:
        key = day.isoformat()
        day_entries = safe_int((by_day.get(key) or {}).get("entrada"))
        day_exits = safe_int((by_day.get(key) or {}).get("salida"))
        day_expected = daily_expected_base
        day_scan_score = pct(min(day_entries, day_expected), day_expected) if day_expected > 0 else None
        balance_base = max(day_entries, day_exits)
        day_balance_score = pct(min(day_entries, day_exits), balance_base) if balance_base > 0 else None
        if day_scan_score is not None:
            daily_scan_scores.append(day_scan_score)
        if day_balance_score is not None:
            daily_balance_scores.append(day_balance_score)
            days_with_scan_records += 1
        entries += day_entries
        exits += day_exits
        gap_total += abs(day_entries - day_exits)
        daily.append({
            "date": key,
            "entries": day_entries,
            "exits": day_exits,
            "expected": day_expected,
            "score": day_scan_score,
            "scan_balance": day_balance_score,
            "gap": abs(day_entries - day_exits),
        })

    scan_metric = _metric(
        average_score(daily_scan_scores),
        f"{entries:,} entradas de {expected:,} esperadas" if expected > 0 else "—",
        f"Promedio diario del periodo · población base {daily_expected_base:,}" if expected > 0 else "Sin escaneos para comparar",
        {
            "entries": entries,
            "exits": exits,
            "expected": expected,
            "expected_population": expected_population,
            "observed_max_entries": observed_max_entries,
            "basis": basis,
            "days_with_records": len(daily_scan_scores),
        },
    )
    balance_metric = _metric(
        average_score(daily_balance_scores),
        f"{entries:,} entradas · {exits:,} salidas" if days_with_scan_records > 0 else "—",
        f"Brecha acumulada {gap_total:,} · promedio diario del periodo" if days_with_scan_records > 0 else "Sin días con entradas/salidas para comparar",
        {
            "entries": entries,
            "exits": exits,
            "gap_total": gap_total,
            "days_with_records": days_with_scan_records,
            "basis": "daily_entry_exit_balance",
        },
    )
    return scan_metric, balance_metric, daily


def _student_punctuality_metric_from_counts(
    rows: List[Dict[str, Any]],
    population_result: Dict[str, Any],
    attendance_daily: List[Dict[str, Any]],
    scans_daily: List[Dict[str, Any]],
    business_days: List[date],
) -> tuple[Dict[str, Any], List[Dict[str, Any]]]:
    allowed_days = {day.isoformat() for day in business_days}
    tardies_by_day = {
        day: safe_int(row.get("tardies"))
        for row in rows or []
        for day in [_date_key(row.get("date"))]
        if day and day in allowed_days
    }
    expected_population = safe_int(population_result.get("value")) if population_result.get("_ok") else 0
    total_attendance_records = sum(safe_int(item.get("records")) for item in attendance_daily)
    total_scan_entries = sum(safe_int(item.get("entries")) for item in scans_daily)
    if expected_population > 0:
        opportunities = expected_population * len(business_days)
        basis = "bot_expected_population"
    elif total_attendance_records > 0:
        opportunities = total_attendance_records
        basis = "attendance_records"
    elif total_scan_entries > 0:
        opportunities = total_scan_entries
        basis = "scan_entries"
    else:
        opportunities = 0
        basis = "none"
    total_tardies = sum(tardies_by_day.values())
    attendance_by_day = {item.get("date"): item for item in attendance_daily}
    scans_by_day = {item.get("date"): item for item in scans_daily}
    daily: List[Dict[str, Any]] = []
    daily_scores: List[Optional[float]] = []
    days_with_records = 0
    for day in business_days:
        key = day.isoformat()
        if expected_population > 0:
            day_opportunities = expected_population
        else:
            day_opportunities = safe_int((attendance_by_day.get(key) or {}).get("records")) or safe_int((scans_by_day.get(key) or {}).get("entries"))
        day_tardies = safe_int(tardies_by_day.get(key))
        day_score = bounded_inverse_rate(day_tardies, day_opportunities)
        if day_score is not None:
            daily_scores.append(day_score)
            days_with_records += 1
        daily.append({"date": key, "tardies": day_tardies, "opportunities": day_opportunities, "score": day_score})
    return _metric(
        average_score(daily_scores),
        f"{total_tardies:,} retardos" if opportunities > 0 else "—",
        f"Promedio diario del periodo · {opportunities:,} oportunidades base {basis.replace('_', ' ')}" if opportunities > 0 else "Sin oportunidades para comparar",
        {"tardies": total_tardies, "opportunities": opportunities, "basis": basis, "days_with_records": days_with_records},
    ), daily


def _staff_attendance_metric(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not payload.get("_ok", True) or payload.get("error"):
        return _unavailable("—", "Asistencia personal no respondió")
    summary = payload.get("summary") or {}
    records = safe_int(summary.get("records_processed"))
    tardies = safe_int(summary.get("retardos_count"))
    absences = safe_int(summary.get("ausencias_count"))
    incidents = tardies + absences
    score = pct(max(records - incidents, 0), records)
    return _metric(
        score,
        f"{max(records - incidents, 0):,} de {records:,} registros sin incidencia" if records > 0 else "—",
        f"{absences:,} faltas · {tardies:,} retardos" if records > 0 else "Sin registros de personal",
        {"records": records, "absences": absences, "tardies": tardies, "incidents": incidents},
    )


async def _planning_metric(plantel_info: Dict[str, Any], start_date: date, end_date: date, business_days: List[date]) -> Dict[str, Any]:
    academic_filters = plantel_info.get("academic_filters") or []
    if not academic_filters:
        return _unavailable("—", "Plantel sin filtro académico")
    active_teachers = await count_active_academic_teachers(academic_filters)
    period_week_start, period_week_end = _week_window_for_period(start_date, end_date)
    weeks = _weeks_touched(_business_days(period_week_start, period_week_end))
    expected_by_teacher_week = active_teachers * weeks
    totals = await fetch_planning_review_totals(
        academic_filters,
        start_date,
        end_date,
        period_week_start,
        period_week_end,
    )
    reviewed = safe_int(totals.get("reviewed_units"))
    submitted = safe_int(totals.get("submitted_units"))
    pending_submitted = max(submitted - reviewed, 0)
    submitted_teachers = safe_int(totals.get("docentes_con_planeacion"))
    created_at_fallback_rows = safe_int(totals.get("created_at_fallback_rows"))

    # Planeaciones measures review completion for plans whose planning week
    # belongs to the selected period, including the current week. It does not
    # count future-week plans just because they were created during the period.
    score = pct(min(reviewed, submitted), submitted)
    return _metric(
        score,
        f"{reviewed:,} revisadas de {submitted:,} del periodo" if submitted > 0 else "—",
        f"{pending_submitted:,} del periodo sin revisión" if submitted > 0 else "Sin planeaciones del periodo",
        {
            "active_teachers": active_teachers,
            "weeks": weeks,
            "expected": expected_by_teacher_week,
            "reviewed": reviewed,
            "submitted": submitted,
            "pending": pending_submitted,
            "submitted_teachers": submitted_teachers,
            "period_week_start": period_week_start.isoformat(),
            "period_week_end": period_week_end.isoformat(),
            "created_at_fallback_rows": created_at_fallback_rows,
            "basis": "reviewed_period_plans",
        },
    )


async def _observation_metrics(plantel_info: Dict[str, Any], end_date: date) -> Dict[str, Dict[str, Any]]:
    academic_filters = plantel_info.get("academic_filters") or []
    if not academic_filters:
        return {
            "observations": _unavailable("—", "Plantel sin filtro académico"),
            "observation_coverage": _unavailable("—", "Plantel sin filtro académico"),
        }
    active_start = end_date - timedelta(days=20)
    month_start, month_end = _month_window_for(end_date)
    monthly_goal = 40
    totals = await fetch_observation_monthly_totals(
        academic_filters,
        active_start,
        end_date,
        month_start,
        month_end,
    )
    active_teachers = safe_int(totals.get("active_teachers"))
    observed_teachers = safe_int(totals.get("observed_teachers"))
    teachers_with_2plus = safe_int(totals.get("teachers_with_2plus"))
    total_observations = safe_int(totals.get("total_observations"))
    without_observation = max(active_teachers - observed_teachers, 0)

    observation_score = pct(min(total_observations, monthly_goal), monthly_goal) if active_teachers > 0 else None
    observations = _metric(
        observation_score,
        f"{total_observations:,} de {monthly_goal:,} observaciones" if active_teachers > 0 else "—",
        f"Meta mensual · {active_teachers:,} docentes activos" if active_teachers > 0 else "Sin docentes activos recientes",
        {
            "active_teachers": active_teachers,
            "observed_teachers": observed_teachers,
            "without_observation": without_observation,
            "teachers_with_2plus": teachers_with_2plus,
            "total_observations": total_observations,
            "monthly_goal": monthly_goal,
            "active_window_start": active_start.isoformat(),
            "active_window_end": end_date.isoformat(),
            "window_start": month_start.isoformat(),
            "window_end": month_end.isoformat(),
            "basis": "monthly_observation_goal_active_teachers",
        },
    )

    coverage_score = pct(min(teachers_with_2plus, active_teachers), active_teachers)
    coverage = _metric(
        coverage_score,
        f"{teachers_with_2plus:,} de {active_teachers:,} docentes con 2+" if active_teachers > 0 else "—",
        f"{without_observation:,} sin observación mensual" if active_teachers > 0 else "Sin docentes activos recientes",
        {
            "active_teachers": active_teachers,
            "observed_teachers": observed_teachers,
            "teachers_with_2plus": teachers_with_2plus,
            "without_observation": without_observation,
            "total_observations": total_observations,
            "window_start": month_start.isoformat(),
            "window_end": month_end.isoformat(),
            "basis": "teachers_observed_twice_monthly",
        },
    )
    return {"observations": observations, "observation_coverage": coverage}


async def _academic_metrics(plantel_info: Dict[str, Any], start_date: date, end_date: date, business_days: List[date]) -> Dict[str, Dict[str, Any]]:
    try:
        planning, observation_payload = await asyncio.gather(
            _planning_metric(plantel_info, start_date, end_date, business_days),
            _observation_metrics(plantel_info, end_date),
        )
        return {"planning": planning, **observation_payload, "_ok": True}
    except Exception as exc:
        logger.error("Corporate academic metrics failed for %s: %s", plantel_info.get("db_code"), exc)
        return {
            "planning": _unavailable("—", f"Error en planeaciones: {exc}"),
            "observations": _unavailable("—", f"Error en observaciones: {exc}"),
            "observation_coverage": _unavailable("—", f"Error en cobertura docente: {exc}"),
            "shape": {},
            "_ok": False,
            "error": str(exc),
        }


def _sapf_metric(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not payload.get("_ok", True) or payload.get("error"):
        return _unavailable("—", "SAPF no respondió")
    open_cases = safe_int(payload.get("open_cases"))
    closed_cases = safe_int(payload.get("closed_cases"))
    complaints = safe_int(payload.get("complaints"))
    total_cases = open_cases + closed_cases
    score = pct(closed_cases, total_cases)
    return _metric(
        score,
        f"{closed_cases:,} cerrados de {total_cases:,} casos" if total_cases > 0 else "—",
        f"{open_cases:,} abiertos · {complaints:,} quejas" if total_cases > 0 else "Sin casos abiertos/cerrados para calcular",
        {"open_cases": open_cases, "closed_cases": closed_cases, "complaints": complaints, "total_cases": total_cases, "total_fichas": safe_int(payload.get("total_fichas"))},
    )


def _general_metric(metrics: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    raw_scores = {key: (metrics.get(key) or {}).get("score") for key in METRIC_WEIGHTS}
    usable_keys = [key for key in METRIC_WEIGHTS if clamp_score(raw_scores.get(key)) is not None]
    used_weight = sum(METRIC_WEIGHTS[key] for key in usable_keys)

    # Do not publish a plantel General when the evidence is too thin. A single
    # SAPF score must never make a campus look like 100% corporate compliance.
    minimum_weight = 50
    minimum_metrics = 3
    has_operational_base = any(key in usable_keys for key in ("roll_call", "student_attendance", "scans", "scan_balance", "student_punctuality"))
    has_followup_base = any(key in usable_keys for key in ("planning", "observations", "observation_coverage", "staff_attendance", "sapf"))
    has_enough_evidence = (
        used_weight >= minimum_weight
        and len(usable_keys) >= minimum_metrics
        and has_operational_base
        and has_followup_base
    )

    score = weighted_score(raw_scores, METRIC_WEIGHTS) if has_enough_evidence else None
    return _metric(
        score,
        "Promedio ponderado" if score is not None else "—",
        "Solo métricas calculables" if score is not None else "Evidencia insuficiente",
        {
            "weights_used": used_weight,
            "weights_total": sum(METRIC_WEIGHTS.values()),
            "metrics_used": usable_keys,
            "minimum_weight": minimum_weight,
            "minimum_metrics": minimum_metrics,
        },
    )


def _bucket_label(day: date, start_date: date, end_date: date) -> str:
    span = max((end_date - start_date).days + 1, 1)
    if span <= 45:
        return day.strftime("%d/%m")
    if span <= 130:
        return f"Sem {((day - start_date).days // 7) + 1}"
    return day.strftime("%Y-%m")


def _daily_general_score(point: Dict[str, Any]) -> Optional[float]:
    weights = {"roll_call": 18, "student_attendance": 18, "scans": 14, "scan_balance": 10, "student_punctuality": 10}
    scores = {key: point.get(key) for key in weights}
    return weighted_score(scores, weights)


def _merge_daily_points(
    attendance_daily: List[Dict[str, Any]],
    scans_daily: List[Dict[str, Any]],
    punctuality_daily: List[Dict[str, Any]],
    business_days: List[date],
) -> List[Dict[str, Any]]:
    attendance_by_day = {item["date"]: item for item in attendance_daily}
    scans_by_day = {item["date"]: item for item in scans_daily}
    punctuality_by_day = {item["date"]: item for item in punctuality_daily}
    output: List[Dict[str, Any]] = []
    for day in business_days:
        key = day.isoformat()
        att = attendance_by_day.get(key) or {}
        scan = scans_by_day.get(key) or {}
        punctuality = punctuality_by_day.get(key) or {}
        point = {
            "date": key,
            "roll_call": pct(att.get("completed_lists"), att.get("expected_lists")),
            "student_attendance": pct(att.get("present"), att.get("records")),
            "scans": scan.get("score"),
            "scan_balance": scan.get("scan_balance"),
            "student_punctuality": punctuality.get("score"),
            "attendance_records": safe_int(att.get("records")),
            "present": safe_int(att.get("present")),
            "expected_lists": safe_int(att.get("expected_lists")),
            "completed_lists": safe_int(att.get("completed_lists")),
            "scan_entries": safe_int(scan.get("entries")),
            "scan_exits": safe_int(scan.get("exits")),
            "scan_expected": safe_int(scan.get("expected")),
            "scan_gap": safe_int(scan.get("gap")),
            "tardies": safe_int(punctuality.get("tardies")),
            "punctuality_opportunities": safe_int(punctuality.get("opportunities")),
        }
        point["general"] = _daily_general_score(point)
        output.append(point)
    return output


def _bucketed_trend(planteles: List[Dict[str, Any]], start_date: date, end_date: date) -> Dict[str, Any]:
    business_days = _business_days(start_date, end_date)
    labels: List[str] = []
    day_to_label: Dict[str, str] = {}
    for day in business_days:
        label = _bucket_label(day, start_date, end_date)
        day_to_label[day.isoformat()] = label
        if label not in labels:
            labels.append(label)

    def series(metric_key: str) -> List[Dict[str, Any]]:
        output: List[Dict[str, Any]] = []
        for plantel in sorted(planteles, key=lambda item: item.get("order", 999)):
            buckets: Dict[str, List[Optional[float]]] = {label: [] for label in labels}
            for point in plantel.get("daily") or []:
                label = day_to_label.get(point.get("date"))
                if not label:
                    continue
                buckets[label].append(point.get(metric_key))
            values = [average_score(buckets[label]) for label in labels]
            output.append({"plantel": plantel.get("plantel"), "name": plantel.get("short_name"), "values": values})
        return output

    return {
        "labels": labels,
        "metrics": {
            key: {"label": _metric_label(key), "unit": "%", "series": series(key)}
            for key in TREND_METRICS
        },
    }


def _clean_sources(sources: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    clean: Dict[str, Any] = {}
    for key, payload in sources.items():
        clean[key] = {
            "ok": bool(payload.get("_ok", True)) and not bool(payload.get("error")),
            "duration_ms": payload.get("_duration_ms"),
            "error": payload.get("error"),
            "timeout": bool(payload.get("timeout")),
        }
    return clean


async def _collect_plantel(code: str, start_date: date, end_date: date, scope: str, business_days: List[date]) -> Dict[str, Any]:
    plantel_info = resolve_plantel(code)
    attendance_aliases = _attendance_aliases(plantel_info)
    husky_values = _husky_values(plantel_info)
    sheets_code = plantel_info.get("sheets_code") or code
    threshold_time = _tardy_threshold(plantel_info)

    sources = await asyncio.gather(
        _safe_value_call("expected_groups", lambda: fetch_expected_groups(sheets_code), 12.0),
        _safe_value_call("expected_population", lambda: fetch_expected_population(sheets_code), 12.0),
        _safe_value_call("attendance_db", lambda: fetch_attendance_rollup(attendance_aliases, start_date, end_date), 20.0),
        _safe_value_call("husky_scans_db", lambda: fetch_husky_daily_scan_counts(husky_values, start_date, end_date), 20.0),
        _safe_value_call("retardos_db", lambda: fetch_husky_tardy_daily_counts(husky_values, start_date, end_date, threshold_time), 20.0),
        _safe_call("staff_attendance", lambda: get_kardex_attendance_report(code, start_date, end_date, scope), SOURCE_TIMEOUTS["staff_attendance"]),
        _safe_call("academic", lambda: _academic_metrics(plantel_info, start_date, end_date, business_days), SOURCE_TIMEOUTS["academic"]),
        _safe_call("sapf", lambda: get_sapf_overview_report(code, start_date, end_date, scope), SOURCE_TIMEOUTS["sapf"]),
    )
    source_map = {
        "expected_groups": sources[0],
        "expected_population": sources[1],
        "attendance": sources[2],
        "husky": sources[3],
        "retardos": sources[4],
        "staff_attendance": sources[5],
        "academic": sources[6],
        "sapf": sources[7],
    }

    attendance_rows = source_map["attendance"].get("value") if source_map["attendance"].get("_ok") else []
    husky_rows = source_map["husky"].get("value") if source_map["husky"].get("_ok") else []
    tardy_rows = source_map["retardos"].get("value") if source_map["retardos"].get("_ok") else []

    if source_map["attendance"].get("_ok"):
        roll_call, student_attendance, attendance_daily = _attendance_metrics_from_rollup(attendance_rows, source_map["expected_groups"], business_days)
    else:
        roll_call = _unavailable("—", "Asistencia no respondió")
        student_attendance = _unavailable("—", "Asistencia no respondió")
        attendance_daily = []

    if source_map["husky"].get("_ok"):
        scans, scan_balance, scans_daily = _scans_metric_from_rows(husky_rows, source_map["expected_population"], attendance_daily, business_days)
    else:
        scans = _unavailable("—", "Escaneos no respondió")
        scan_balance = _unavailable("—", "Escaneos no respondió")
        scans_daily = []

    if source_map["retardos"].get("_ok"):
        student_punctuality, punctuality_daily = _student_punctuality_metric_from_counts(tardy_rows, source_map["expected_population"], attendance_daily, scans_daily, business_days)
    else:
        student_punctuality = _unavailable("—", "Retardos no respondió")
        punctuality_daily = []
    staff_attendance = _staff_attendance_metric(source_map["staff_attendance"])
    academic_payload = source_map["academic"] if isinstance(source_map["academic"], dict) else {}
    planning = academic_payload.get("planning") if isinstance(academic_payload.get("planning"), dict) else _unavailable("—", "Sin cálculo de planeaciones")
    observations = academic_payload.get("observations") if isinstance(academic_payload.get("observations"), dict) else _unavailable("—", "Sin cálculo de observaciones")
    observation_coverage = academic_payload.get("observation_coverage") if isinstance(academic_payload.get("observation_coverage"), dict) else _unavailable("—", "Sin cálculo de cobertura docente")
    sapf = _sapf_metric(source_map["sapf"])

    metrics = {
        "roll_call": roll_call,
        "student_attendance": student_attendance,
        "scans": scans,
        "scan_balance": scan_balance,
        "student_punctuality": student_punctuality,
        "staff_attendance": staff_attendance,
        "planning": planning,
        "observations": observations,
        "observation_coverage": observation_coverage,
        "sapf": sapf,
    }
    general = _general_metric(metrics)
    metrics = {"general": general, **metrics}

    order = FIXED_PLANTEL_ORDER.index(code) if code in FIXED_PLANTEL_ORDER else 999
    return {
        "plantel": code,
        "order": order,
        "resolved_name": plantel_info.get("resolved_name"),
        "short_name": plantel_info.get("short_name"),
        "metrics": metrics,
        "index": general,
        "domains": metrics,
        "daily": _merge_daily_points(attendance_daily, scans_daily, punctuality_daily, business_days),
        "source_audit": _clean_sources(source_map),
        "academic_shape": academic_payload.get("shape") if isinstance(academic_payload, dict) else {},
    }




def _metric_evidence(metric: Dict[str, Any], keys: Iterable[str]) -> Dict[str, Any]:
    output: Dict[str, Any] = {"score": metric.get("score")}
    for key in keys:
        if key in metric:
            output[key] = metric.get(key)
    return output


def _diagnostic(planteles: List[Dict[str, Any]], start_date: date, end_date: date, business_days: List[date]) -> Dict[str, Any]:
    rows: Dict[str, Any] = {}
    for plantel in sorted(planteles, key=lambda item: item.get("order", 999)):
        metrics = plantel.get("metrics") or {}
        rows[str(plantel.get("plantel"))] = {
            "name": plantel.get("resolved_name"),
            "sources": plantel.get("source_audit") or {},
            "metrics": {
                "pase_lista": _metric_evidence(metrics.get("roll_call") or {}, ["completed", "expected", "missing", "basis", "bot_groups", "observed_max_groups"]),
                "asistencia_alumnos": _metric_evidence(metrics.get("student_attendance") or {}, ["present", "records", "absent", "basis"]),
                "escaneos": _metric_evidence(metrics.get("scans") or {}, ["entries", "exits", "expected", "expected_population", "observed_max_entries", "days_with_records", "basis"]),
                "balance_escaneos": _metric_evidence(metrics.get("scan_balance") or {}, ["entries", "exits", "gap_total", "days_with_records", "basis"]),
                "puntualidad_alumnos": _metric_evidence(metrics.get("student_punctuality") or {}, ["tardies", "opportunities", "basis"]),
                "asistencia_personal": _metric_evidence(metrics.get("staff_attendance") or {}, ["records", "absences", "tardies", "incidents"]),
                "planeaciones": _metric_evidence(metrics.get("planning") or {}, ["submitted", "reviewed", "pending", "active_teachers", "submitted_teachers", "weeks", "expected", "period_week_start", "period_week_end", "created_at_fallback_rows", "basis"]),
                "observaciones": _metric_evidence(metrics.get("observations") or {}, ["total_observations", "monthly_goal", "active_teachers", "window_start", "window_end", "active_window_start", "active_window_end", "basis"]),
                "cobertura_observaciones": _metric_evidence(metrics.get("observation_coverage") or {}, ["active_teachers", "observed_teachers", "teachers_with_2plus", "without_observation", "window_start", "window_end", "basis"]),
                "sapf": _metric_evidence(metrics.get("sapf") or {}, ["open_cases", "closed_cases", "total_cases", "total_fichas", "complaints"]),
            },
            "general": _metric_evidence(metrics.get("general") or {}, ["weights_used", "weights_total", "metrics_used", "minimum_weight", "minimum_metrics"]),
        }
    return {
        "v": "corp-diagnostic-v6",
        "range": {"start": start_date.isoformat(), "end": end_date.isoformat(), "business_days": len(business_days)},
        "planteles": rows,
    }


def _domain_averages(planteles: List[Dict[str, Any]]) -> Dict[str, Any]:
    output: Dict[str, Any] = {}
    for key in DISPLAY_METRICS:
        values = [(p.get("metrics") or {}).get(key, {}).get("score") for p in planteles]
        score = average_score(values)
        output[key] = _metric(score, "Promedio", "Promedio de planteles con cálculo")
    return output


def _plantel_summary(plantel: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not plantel:
        return None
    index = plantel.get("index") or {}
    return {
        "plantel": plantel.get("plantel"),
        "resolved_name": plantel.get("resolved_name"),
        "score": index.get("score"),
        "status": index.get("status"),
        "color": index.get("color"),
    }


def _aggregate(planteles: List[Dict[str, Any]], start_date: date, end_date: date) -> Dict[str, Any]:
    scored = [p for p in planteles if (p.get("index") or {}).get("score") is not None]
    corporate_score = average_score([(p.get("index") or {}).get("score") for p in scored])
    best = max(scored, key=lambda item: safe_float((item.get("index") or {}).get("score")) or -1) if scored else None
    worst = min(scored, key=lambda item: safe_float((item.get("index") or {}).get("score")) or 101) if scored else None
    return {
        "corporate_index": _metric(
            corporate_score,
            "Cumplimiento general" if corporate_score is not None else "—",
            f"{len(scored)} de {len(planteles)} planteles con cálculo" if planteles else "Sin planteles",
        ),
        "best_plantel": _plantel_summary(best),
        "worst_plantel": _plantel_summary(worst),
        "domain_scores": _domain_averages(planteles),
        "window": {
            "start": start_date.isoformat(),
            "end": end_date.isoformat(),
            "calendar_days": max((end_date - start_date).days + 1, 1),
            "business_days": len(_business_days(start_date, end_date)),
        },
        "status_counts": {
            "fulfilled": sum(1 for p in scored if (p.get("index") or {}).get("status") == "fulfilled"),
            "warning": sum(1 for p in scored if (p.get("index") or {}).get("status") == "warning"),
            "critical": sum(1 for p in scored if (p.get("index") or {}).get("status") == "critical"),
            "unavailable": len(planteles) - len(scored),
        },
    }


def _matrix(planteles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for plantel in sorted(planteles, key=lambda item: item.get("order", 999)):
        metrics = plantel.get("metrics") or {}
        rows.append({
            "plantel": plantel.get("plantel"),
            "name": plantel.get("resolved_name"),
            "cells": {key: metrics.get(key) for key in DISPLAY_METRICS},
        })
    return rows


def _rankings(planteles: List[Dict[str, Any]], aggregate: Dict[str, Any]) -> Dict[str, Any]:
    plantel_rows = []
    for plantel in planteles:
        index = plantel.get("index") or {}
        plantel_rows.append({
            "plantel": plantel.get("plantel"),
            "name": plantel.get("resolved_name"),
            "score": index.get("score"),
            "status": index.get("status"),
            "color": index.get("color"),
        })
    plantel_rows.sort(key=lambda item: item.get("score") if item.get("score") is not None else -1, reverse=True)

    metric_rows = []
    for key, metric in (aggregate.get("domain_scores") or {}).items():
        if key == "general":
            continue
        metric_rows.append({
            "key": key,
            "label": _metric_label(key),
            "score": metric.get("score"),
            "status": metric.get("status"),
            "color": metric.get("color"),
        })
    metric_rows.sort(key=lambda item: item.get("score") if item.get("score") is not None else -1, reverse=True)
    return {"planteles": plantel_rows, "metrics": metric_rows}


def _metric_label(key: str) -> str:
    return {
        "general": "General",
        "roll_call": "Pase de lista",
        "student_attendance": "Asistencia alumnos",
        "scans": "Escaneos",
        "scan_balance": "Balance accesos",
        "student_punctuality": "Puntualidad alumnos",
        "staff_attendance": "Asistencia personal",
        "planning": "Planeaciones",
        "observations": "Observaciones",
        "observation_coverage": "Cobertura obs.",
        "sapf": "SAPF",
    }.get(key, key)


async def get_corporate_compliance_index(
    planteles: Optional[str],
    start_date: date,
    end_date: date,
    scope: str,
    include_baselines: bool = False,
) -> Dict[str, Any]:
    del include_baselines
    selected = _normalize_planteles(planteles)
    business_days = _business_days(start_date, end_date)
    semaphore = asyncio.Semaphore(1)

    async def collect(code: str) -> Dict[str, Any]:
        async with semaphore:
            return await _collect_plantel(code, start_date, end_date, scope, business_days)

    plantel_payloads = await asyncio.gather(*(collect(code) for code in selected))
    plantel_payloads.sort(key=lambda item: item.get("order", 999))
    aggregate = _aggregate(plantel_payloads, start_date, end_date)

    return {
        "title": "Reporte SIPAE",
        "generated_at": _mx_now().isoformat(),
        "timezone": "America/Mexico_City",
        "scope": scope,
        "selected_planteles": selected,
        "plantel_order": FIXED_PLANTEL_ORDER,
        "weights": METRIC_WEIGHTS,
        "traffic_light": {
            "green": {"min": 85, "max": 100, "label": "Bien"},
            "yellow": {"min": 70, "max": 84, "label": "Atención"},
            "red": {"min": 1, "max": 69, "label": "Bajo"},
        },
        "metrics": {
            key: {"label": _metric_label(key), "weight": METRIC_WEIGHTS.get(key)}
            for key in DISPLAY_METRICS
        },
        "aggregate": aggregate,
        "matrix": _matrix(plantel_payloads),
        "rankings": _rankings(plantel_payloads, aggregate),
        "trend": _bucketed_trend(plantel_payloads, start_date, end_date),
        "planteles": plantel_payloads,
        "source_audit": {p["plantel"]: p.get("source_audit") for p in plantel_payloads},
        "diagnostic": _diagnostic(plantel_payloads, start_date, end_date, business_days),
        "meta": {
            "logic_version": "2026-06-22-reporte-sipae-v7",
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "business_days": len(business_days),
        },
    }
