from __future__ import annotations

import asyncio
import calendar
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional
from zoneinfo import ZoneInfo

from core.logger import get_logger
from core.utils import resolve_plantel
from modules.sapf.service import get_sapf_overview_report
from integrations.external_bot import fetch_expected_groups, fetch_expected_population

from .report_repository import (
    count_active_academic_teachers,
    fetch_attendance_rollup,
    fetch_husky_access_user_rollup,
    fetch_husky_daily_scan_counts,
    fetch_husky_tardy_daily_counts,
    fetch_observation_monthly_totals,
    fetch_observation_period_monthly_totals,
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
PLANTEL_LEVELS = {
    "PT": "Primaria",
    "PM": "Primaria",
    "ST": "Secundaria",
    "SM": "Secundaria",
    "PREET": "Preescolar",
    "PREEM": "Preescolar",
}
LEVEL_ORDER = ["Preescolar", "Primaria", "Secundaria"]
DISPLAY_METRICS = [
    "general",
    "roll_call",
    "student_attendance",
    "scans",
    "scan_balance",
    "student_punctuality",
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


def _date_chunks(start_date: date, end_date: date, max_days: int = 45) -> List[tuple[date, date]]:
    """Split long report ranges so large source tables do not time out.

    The cycle-school default can span many months; chunking keeps each SQL read
    bounded while preserving the same aggregate formulas.
    """
    chunks: List[tuple[date, date]] = []
    cursor = start_date
    while cursor <= end_date:
        chunk_end = min(end_date, cursor + timedelta(days=max_days - 1))
        chunks.append((cursor, chunk_end))
        cursor = chunk_end + timedelta(days=1)
    return chunks


def _source_timeout_for_range(start_date: date, end_date: date, base_seconds: float = 24.0) -> float:
    """Bound long-range source waits so ciclo escolar never holds the HTTP request indefinitely.

    Long ranges are chunked before querying. The timeout only needs to cover the
    bounded chunk loop, not a monolithic year-sized SQL scan. Ciclo Escolar
    Husky reads can legitimately take more than a minute under load, so keep
    enough budget to finish instead of publishing failed heatmap cells.
    """
    chunks = max(len(_date_chunks(start_date, end_date)), 1)
    return max(base_seconds, min(95.0, chunks * 12.0))


async def _chunked_rows(
    fetcher: Callable[..., Awaitable[List[Dict[str, Any]]]],
    *prefix_args: Any,
    start_date: date,
    end_date: date,
    max_days: int = 45,
    **kwargs: Any,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for chunk_start, chunk_end in _date_chunks(start_date, end_date, max_days=max_days):
        chunk_rows = await fetcher(*prefix_args, chunk_start, chunk_end, **kwargs)
        rows.extend(_as_list(chunk_rows))
    return rows


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


def _month_keys_between(start_date: date, end_date: date) -> List[str]:
    keys: List[str] = []
    cursor = start_date.replace(day=1)
    end_month = end_date.replace(day=1)
    while cursor <= end_month:
        keys.append(f"{cursor.year:04d}-{cursor.month:02d}")
        if cursor.month == 12:
            cursor = date(cursor.year + 1, 1, 1)
        else:
            cursor = date(cursor.year, cursor.month + 1, 1)
    return keys


def _is_long_observation_period(start_date: date, end_date: date) -> bool:
    return (end_date - start_date).days > 45


def _monthly_target_per_100_for_period(start_date: date, end_date: date, monthly_target_per_100: float = 5.0) -> float:
    """Prorate a monthly per-100 target over the evaluated calendar range."""
    if end_date < start_date:
        return 0.0
    cursor = start_date
    prorated = 0.0
    while cursor <= end_date:
        days_in_month = calendar.monthrange(cursor.year, cursor.month)[1]
        month_end = date(cursor.year, cursor.month, days_in_month)
        segment_end = min(month_end, end_date)
        segment_days = (segment_end - cursor).days + 1
        prorated += monthly_target_per_100 * (segment_days / days_in_month)
        cursor = segment_end + timedelta(days=1)
    return round(prorated, 2)


def _month_bounds(month_key: str, report_start: date, report_end: date) -> tuple[date, date]:
    year, month = [int(part) for part in str(month_key).split("-")[:2]]
    month_start = date(year, month, 1)
    month_end = date(year, month, calendar.monthrange(year, month)[1])
    return max(month_start, report_start), min(month_end, report_end)


def _month_label(month_key: str) -> str:
    try:
        year, month = [int(part) for part in str(month_key).split("-")[:2]]
        month_names = ["", "ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic"]
        return f"{month_names[month]} {year}"
    except Exception:
        return str(month_key)


def _with_breakdown(metric: Dict[str, Any], breakdown: Dict[str, Any]) -> Dict[str, Any]:
    metric["breakdown"] = breakdown
    return metric


def _plain_unavailable_reason(metric: Dict[str, Any]) -> str:
    detail = str(metric.get("detail") or "").strip()
    label = str(metric.get("label") or "").strip()
    if detail and detail != "—":
        return detail
    if label and label != "—":
        return label
    return "No hay datos suficientes para calcular esta métrica."


def _daily_monthly_breakdown(
    daily: List[Dict[str, Any]],
    business_days: List[date],
    score_key: str,
    numerator_key: str,
    denominator_key: str,
    numerator_label: str,
    denominator_label: str,
) -> List[Dict[str, Any]]:
    by_day = {str(item.get("date")): item for item in daily or []}
    months: List[Dict[str, Any]] = []
    month_keys = _month_keys_between(business_days[0], business_days[-1]) if business_days else []
    for month_key in month_keys:
        month_days = [day for day in business_days if day.isoformat().startswith(month_key)]
        scores: List[Optional[float]] = []
        numerator = 0
        denominator = 0
        available_days = 0
        for day in month_days:
            item = by_day.get(day.isoformat()) or {}
            score = item.get(score_key)
            if clamp_score(score) is not None:
                scores.append(score)
                available_days += 1
            numerator += safe_int(item.get(numerator_key))
            denominator += safe_int(item.get(denominator_key))
        months.append({
            "period": month_key,
            "label": _month_label(month_key),
            "start": month_days[0].isoformat() if month_days else None,
            "end": month_days[-1].isoformat() if month_days else None,
            "numerator": numerator,
            "denominator": denominator,
            "numerator_label": numerator_label,
            "denominator_label": denominator_label,
            "score": average_score(scores),
            "available_days": available_days,
            "business_days": len(month_days),
            "unavailable_reason": None if available_days else "Sin días calculables en el mes",
        })
    return months


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


def _clock_seconds(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return float(value.hour * 3600 + value.minute * 60 + value.second) + (value.microsecond / 1_000_000)
    if isinstance(value, timedelta):
        return float(value.total_seconds() % 86400)
    if hasattr(value, "hour") and hasattr(value, "minute") and hasattr(value, "second"):
        return float(value.hour * 3600 + value.minute * 60 + value.second)
    text = str(value).strip()
    if not text:
        return None
    if "T" in text:
        text = text.split("T")[-1]
    if " " in text:
        text = text.split(" ")[-1]
    text = text.split(".")[0]
    parts = text.split(":")
    if len(parts) < 2:
        return safe_float(text)
    try:
        hours = int(parts[0])
        minutes = int(parts[1])
        seconds = int(parts[2]) if len(parts) > 2 else 0
        return float(((hours * 3600) + (minutes * 60) + seconds) % 86400)
    except Exception:
        return None


def _summarize_husky_access_rollup(
    rows: List[Dict[str, Any]],
    threshold_time: str,
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    scan_stats: Dict[tuple[str, str], Dict[str, Any]] = defaultdict(
        lambda: {
            "total_scans": 0,
            "samples": 0,
            "seconds_sum": 0.0,
            "first_seconds": None,
            "last_seconds": None,
        }
    )
    tardies_by_day: Dict[str, int] = defaultdict(int)
    threshold_seconds = _clock_seconds(threshold_time)

    for row in rows or []:
        day = _date_key(row.get("fecha"))
        tipo = str(row.get("tipo_accion") or "").strip().lower()
        if not day or tipo not in {"entrada", "salida"}:
            continue

        samples = safe_int(row.get("samples"))
        seconds_sum = safe_float(row.get("seconds_sum")) or 0.0
        first_seconds = _clock_seconds(row.get("first_timestamp"))
        last_seconds = _clock_seconds(row.get("last_timestamp"))

        stats = scan_stats[(day, tipo)]
        stats["total_scans"] += 1
        stats["samples"] += samples
        stats["seconds_sum"] += seconds_sum
        if first_seconds is not None and (stats["first_seconds"] is None or first_seconds < stats["first_seconds"]):
            stats["first_seconds"] = first_seconds
        if last_seconds is not None and (stats["last_seconds"] is None or last_seconds > stats["last_seconds"]):
            stats["last_seconds"] = last_seconds

        if tipo == "entrada" and threshold_seconds is not None and first_seconds is not None and first_seconds > threshold_seconds:
            tardies_by_day[day] += 1

    scan_rows: List[Dict[str, Any]] = []
    for (day, tipo), stats in sorted(scan_stats.items()):
        samples = safe_int(stats.get("samples"))
        scan_rows.append({
            "fecha": day,
            "tipo_accion": tipo,
            "total_scans": safe_int(stats.get("total_scans")),
            "samples": samples,
            "avg_entry_seconds": (safe_float(stats.get("seconds_sum")) or 0.0) / samples if samples > 0 else None,
            "first_entry_time": _format_hhmm(stats.get("first_seconds")),
            "last_entry_time": _format_hhmm(stats.get("last_seconds")),
        })

    tardy_rows = [
        {"date": day, "tardies": count}
        for day, count in sorted(tardies_by_day.items())
    ]
    return scan_rows, tardy_rows


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
    observed_max_attendance_records = max((safe_int(item.get("records")) for item in attendance_daily or []), default=0)
    population_candidates = {
        "expected_population": expected_population,
        "observed_max_daily_attendance_records": observed_max_attendance_records,
        "observed_max_daily_entries": observed_max_entries,
    }
    daily_expected_base = max(population_candidates.values())
    if daily_expected_base > 0:
        basis = next(
            (
                key
                for key in ("expected_population", "observed_max_daily_attendance_records", "observed_max_daily_entries")
                if population_candidates[key] == daily_expected_base
            ),
            "max_available_population_base",
        )
    else:
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
            "observed_max_attendance_records": observed_max_attendance_records,
            "population_candidates": population_candidates,
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
    observed_max_attendance_records = max((safe_int(item.get("records")) for item in attendance_daily or []), default=0)
    observed_max_entries = max((safe_int(item.get("entries")) for item in scans_daily or []), default=0)
    population_candidates = {
        "expected_population": expected_population,
        "observed_max_daily_attendance_records": observed_max_attendance_records,
        "observed_max_daily_entries": observed_max_entries,
    }
    daily_population_base = max(population_candidates.values())
    if daily_population_base > 0:
        opportunities = daily_population_base * len(business_days)
        basis = next(
            (
                key
                for key in ("expected_population", "observed_max_daily_attendance_records", "observed_max_daily_entries")
                if population_candidates[key] == daily_population_base
            ),
            "max_available_population_base",
        )
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
        if daily_population_base > 0:
            day_opportunities = daily_population_base
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
        {
            "tardies": total_tardies,
            "opportunities": opportunities,
            "expected_population": expected_population,
            "observed_max_attendance_records": observed_max_attendance_records,
            "observed_max_entries": observed_max_entries,
            "population_candidates": population_candidates,
            "basis": basis,
            "days_with_records": days_with_records,
        },
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
    totals = await fetch_planning_review_totals(
        academic_filters,
        start_date,
        end_date,
        period_week_start,
        period_week_end,
    )
    submitted = safe_int(totals.get("submitted_units"))
    reviewed = safe_int(totals.get("reviewed_units"))
    pending_submitted = safe_int(totals.get("pending_units")) or max(submitted - reviewed, 0)
    submitted_teachers = safe_int(totals.get("docentes_con_planeacion"))
    raw_rows = safe_int(totals.get("raw_rows"))
    monthly_rows = []
    for row in totals.get("monthly") or []:
        submitted_month = safe_int(row.get("submitted_units"))
        reviewed_month = safe_int(row.get("reviewed_units"))
        pending_month = safe_int(row.get("pending_units")) or max(submitted_month - reviewed_month, 0)
        monthly_rows.append({
            "period": str(row.get("month_key") or ""),
            "label": _month_label(str(row.get("month_key") or "")),
            "numerator": reviewed_month,
            "denominator": submitted_month,
            "numerator_label": "Planeaciones revisadas",
            "denominator_label": "Planeaciones creadas",
            "pending": pending_month,
            "score": pct(min(reviewed_month, submitted_month), submitted_month),
            "unavailable_reason": None if submitted_month > 0 else "Sin planeaciones creadas en el mes",
        })

    score = pct(min(reviewed, submitted), submitted)
    return _metric(
        score,
        f"{reviewed:,} revisadas de {submitted:,} creadas" if submitted > 0 else "—",
        f"{pending_submitted:,} creadas sin revisión" if submitted > 0 else "Sin planeaciones creadas en el periodo",
        {
            "active_teachers": active_teachers,
            "weeks": weeks,
            "expected": active_teachers * weeks,
            "reviewed": reviewed,
            "submitted": submitted,
            "pending": pending_submitted,
            "submitted_teachers": submitted_teachers,
            "raw_rows": raw_rows,
            "created_at_start": start_date.isoformat(),
            "created_at_end": end_date.isoformat(),
            "min_created_at": str(totals.get("min_created_at") or ""),
            "max_created_at": str(totals.get("max_created_at") or ""),
            "basis": "created_at_period_plans",
            "breakdown": {
                "method": "Razón del periodo",
                "formula": "planeaciones revisadas / planeaciones creadas en el periodo",
                "numerator": reviewed,
                "denominator": submitted,
                "numerator_label": "Planeaciones revisadas",
                "denominator_label": "Planeaciones creadas",
                "aggregation": "El valor final usa el total del periodo, no el promedio simple de meses.",
                "excluded": ["Planeaciones flagged", "Docentes coordinadores, banned o ISSSTE"],
                "monthly": monthly_rows,
            },
        },
    )


async def _observation_metrics(plantel_info: Dict[str, Any], start_date: date, end_date: date) -> Dict[str, Dict[str, Any]]:
    academic_filters = plantel_info.get("academic_filters") or []
    if not academic_filters:
        return {
            "observations": _unavailable("—", "Plantel sin filtro académico"),
            "observation_coverage": _unavailable("—", "Plantel sin filtro académico"),
        }

    monthly_goal = 40

    if _is_long_observation_period(start_date, end_date):
        month_keys = _month_keys_between(start_date, end_date)
        month_count = max(len(month_keys), 1)
        totals = await fetch_observation_period_monthly_totals(academic_filters, start_date, end_date)
        active_by_month = {
            str(row.get("month_key") or ""): safe_int(row.get("active_teachers"))
            for row in (totals.get("active_by_month") or [])
        }
        observations_by_month: Dict[str, Dict[str, int]] = defaultdict(dict)
        total_observations = 0
        for row in totals.get("observations_by_month") or []:
            month_key = str(row.get("month_key") or "")
            docente = str(row.get("docente") or "").strip()
            count = safe_int(row.get("total_observations"))
            if not month_key or not docente:
                continue
            observations_by_month[month_key][docente] = count
            total_observations += count

        monthly_observation_counts: List[int] = []
        monthly_coverage_scores: List[Optional[float]] = []
        monthly_active_counts: List[int] = []
        monthly_two_plus_counts: List[int] = []
        months_with_active = 0
        observation_monthly_rows: List[Dict[str, Any]] = []
        coverage_monthly_rows: List[Dict[str, Any]] = []
        for month_key in month_keys:
            teacher_counts = observations_by_month.get(month_key) or {}
            month_total = sum(teacher_counts.values())
            active_teachers = safe_int(active_by_month.get(month_key))
            teachers_with_2plus = sum(1 for value in teacher_counts.values() if safe_int(value) >= 2)
            monthly_observation_counts.append(month_total)
            monthly_active_counts.append(active_teachers)
            monthly_two_plus_counts.append(teachers_with_2plus)
            if active_teachers > 0:
                months_with_active += 1
                monthly_coverage_scores.append(pct(min(teachers_with_2plus, active_teachers), active_teachers))
            month_start, month_end = _month_bounds(month_key, start_date, end_date)
            observation_monthly_rows.append({
                "period": month_key,
                "label": _month_label(month_key),
                "start": month_start.isoformat(),
                "end": month_end.isoformat(),
                "numerator": month_total,
                "denominator": monthly_goal,
                "numerator_label": "Observaciones realizadas",
                "denominator_label": "Meta mensual",
                "score": pct(min(month_total, monthly_goal), monthly_goal),
                "unavailable_reason": None,
            })
            coverage_monthly_rows.append({
                "period": month_key,
                "label": _month_label(month_key),
                "start": month_start.isoformat(),
                "end": month_end.isoformat(),
                "numerator": teachers_with_2plus,
                "denominator": active_teachers,
                "numerator_label": "Docentes con 2+ observaciones",
                "denominator_label": "Docentes activos",
                "score": pct(min(teachers_with_2plus, active_teachers), active_teachers),
                "unavailable_reason": None if active_teachers > 0 else "Sin docentes activos en el mes",
            })

        avg_monthly_observations = total_observations / month_count if month_count else 0.0
        observation_score = pct(min(avg_monthly_observations, monthly_goal), monthly_goal)
        avg_active_teachers = (sum(monthly_active_counts) / months_with_active) if months_with_active else 0.0
        avg_two_plus = (sum(monthly_two_plus_counts) / months_with_active) if months_with_active else 0.0
        coverage_score = average_score(monthly_coverage_scores)
        observations = _metric(
            observation_score,
            f"{avg_monthly_observations:.1f} prom/mes de {monthly_goal:,}",
            f"{total_observations:,} observaciones en {month_count:,} meses del periodo",
            {
                "total_observations": total_observations,
                "avg_monthly_observations": round(avg_monthly_observations, 1),
                "monthly_goal": monthly_goal,
                "months_count": month_count,
                "window_start": start_date.isoformat(),
                "window_end": end_date.isoformat(),
                "basis": "period_monthly_average_observation_goal",
                "breakdown": {
                    "method": "Promedio mensual contra meta",
                    "formula": "promedio mensual de observaciones / meta mensual",
                    "numerator": round(avg_monthly_observations, 1),
                    "denominator": monthly_goal,
                    "numerator_label": "Promedio mensual de observaciones",
                    "denominator_label": "Meta mensual",
                    "aggregation": "Se suman las observaciones del ciclo, se dividen entre meses evaluados y luego se compara contra la meta mensual.",
                    "excluded": ["Meses fuera del periodo seleccionado", "Observaciones sin docente"],
                    "monthly": observation_monthly_rows,
                },
            },
        )
        coverage = _metric(
            coverage_score,
            f"{avg_two_plus:.1f} prom/mes con 2+" if months_with_active else "—",
            f"Promedio mensual sobre {avg_active_teachers:.1f} docentes activos" if months_with_active else "Sin docentes activos por mes",
            {
                "avg_active_teachers": round(avg_active_teachers, 1),
                "avg_teachers_with_2plus": round(avg_two_plus, 1),
                "months_with_active": months_with_active,
                "months_count": month_count,
                "window_start": start_date.isoformat(),
                "window_end": end_date.isoformat(),
                "basis": "period_monthly_average_teachers_observed_twice",
                "breakdown": {
                    "method": "Promedio mensual de cobertura",
                    "formula": "promedio de (docentes con 2+ observaciones / docentes activos)",
                    "numerator": round(avg_two_plus, 1),
                    "denominator": round(avg_active_teachers, 1),
                    "numerator_label": "Promedio mensual docentes con 2+",
                    "denominator_label": "Promedio mensual docentes activos",
                    "aggregation": "El valor final es el promedio de los porcentajes mensuales calculables.",
                    "excluded": ["Meses sin docentes activos", "Usuarios coordinadores, banned o ISSSTE"],
                    "monthly": coverage_monthly_rows,
                },
            },
        )
        return {"observations": observations, "observation_coverage": coverage}

    active_start = end_date - timedelta(days=20)
    month_start, month_end = _month_window_for(end_date)
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
            "breakdown": {
                "method": "Meta mensual",
                "formula": "observaciones realizadas / meta mensual",
                "numerator": total_observations,
                "denominator": monthly_goal,
                "numerator_label": "Observaciones realizadas",
                "denominator_label": "Meta mensual",
                "aggregation": "El valor final usa el mes seleccionado.",
                "excluded": ["Observaciones fuera del mes seleccionado", "Observaciones sin docente"],
                "monthly": [{
                    "period": f"{month_end.year:04d}-{month_end.month:02d}",
                    "label": _month_label(f"{month_end.year:04d}-{month_end.month:02d}"),
                    "start": month_start.isoformat(),
                    "end": month_end.isoformat(),
                    "numerator": total_observations,
                    "denominator": monthly_goal,
                    "numerator_label": "Observaciones realizadas",
                    "denominator_label": "Meta mensual",
                    "score": observation_score,
                    "unavailable_reason": None if active_teachers > 0 else "Sin docentes activos recientes",
                }],
            },
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
            "breakdown": {
                "method": "Cobertura mensual",
                "formula": "docentes con 2+ observaciones / docentes activos",
                "numerator": teachers_with_2plus,
                "denominator": active_teachers,
                "numerator_label": "Docentes con 2+ observaciones",
                "denominator_label": "Docentes activos",
                "aggregation": "El valor final usa el mes seleccionado.",
                "excluded": ["Docentes no activos en la ventana reciente", "Usuarios coordinadores, banned o ISSSTE"],
                "monthly": [{
                    "period": f"{month_end.year:04d}-{month_end.month:02d}",
                    "label": _month_label(f"{month_end.year:04d}-{month_end.month:02d}"),
                    "start": month_start.isoformat(),
                    "end": month_end.isoformat(),
                    "numerator": teachers_with_2plus,
                    "denominator": active_teachers,
                    "numerator_label": "Docentes con 2+ observaciones",
                    "denominator_label": "Docentes activos",
                    "score": coverage_score,
                    "unavailable_reason": None if active_teachers > 0 else "Sin docentes activos recientes",
                }],
            },
        },
    )
    return {"observations": observations, "observation_coverage": coverage}


async def _academic_metrics(plantel_info: Dict[str, Any], start_date: date, end_date: date, business_days: List[date]) -> Dict[str, Dict[str, Any]]:
    planning_result, observation_result = await asyncio.gather(
        _planning_metric(plantel_info, start_date, end_date, business_days),
        _observation_metrics(plantel_info, start_date, end_date),
        return_exceptions=True,
    )
    errors: List[str] = []
    if isinstance(planning_result, Exception):
        errors.append(f"planeaciones: {planning_result}")
        planning = _unavailable("—", f"Error en planeaciones: {planning_result}")
    else:
        planning = planning_result

    if isinstance(observation_result, Exception):
        errors.append(f"observaciones: {observation_result}")
        observation_payload = {
            "observations": _unavailable("—", f"Error en observaciones: {observation_result}"),
            "observation_coverage": _unavailable("—", f"Error en cobertura docente: {observation_result}"),
        }
    else:
        observation_payload = observation_result

    payload: Dict[str, Dict[str, Any]] = {"planning": planning, **observation_payload, "_ok": not errors}
    if errors:
        payload["error"] = " | ".join(errors)
    return payload


def _sapf_population_base(
    population_result: Dict[str, Any],
    attendance_daily: List[Dict[str, Any]],
    scans_daily: List[Dict[str, Any]],
) -> tuple[int, str, Dict[str, int]]:
    expected_population = safe_int(population_result.get("value")) if population_result.get("_ok") else 0
    observed_scan_population = max((safe_int(item.get("entries")) for item in scans_daily or []), default=0)
    observed_attendance_population = max((safe_int(item.get("records")) for item in attendance_daily or []), default=0)
    candidates = {
        "expected_population": expected_population,
        "observed_max_daily_attendance_records": observed_attendance_population,
        "observed_max_daily_entries": observed_scan_population,
    }
    population = max(candidates.values())
    if population <= 0:
        return 0, "none", candidates
    for basis in ("expected_population", "observed_max_daily_attendance_records", "observed_max_daily_entries"):
        if candidates[basis] == population:
            return population, basis, candidates
    return population, "max_available_population_base", candidates


def _sapf_monthly_breakdown(
    payload: Dict[str, Any],
    population: int,
    start_date: date,
    end_date: date,
    monthly_target_per_100: float,
) -> List[Dict[str, Any]]:
    by_month: Dict[str, Dict[str, int]] = defaultdict(lambda: {"fichas": 0, "followups": 0})
    for row in payload.get("monthly_activity") or []:
        month_key = str(row.get("month_key") or "")
        source = str(row.get("source") or "").strip().lower()
        total = safe_int(row.get("total"))
        if not month_key:
            continue
        if source == "seguimiento":
            by_month[month_key]["followups"] += total
        else:
            by_month[month_key]["fichas"] += total

    rows: List[Dict[str, Any]] = []
    for month_key in _month_keys_between(start_date, end_date):
        month_start, month_end = _month_bounds(month_key, start_date, end_date)
        target_per_100 = _monthly_target_per_100_for_period(month_start, month_end, monthly_target_per_100)
        denominator = round((population * target_per_100) / 100.0, 1) if population > 0 else 0.0
        followups = by_month[month_key]["followups"]
        fichas = by_month[month_key]["fichas"]
        rows.append({
            "period": month_key,
            "label": _month_label(month_key),
            "start": month_start.isoformat(),
            "end": month_end.isoformat(),
            "numerator": followups,
            "denominator": denominator,
            "numerator_label": "Seguimientos registrados",
            "denominator_label": "Seguimientos esperados",
            "score": pct(followups, denominator),
            "fichas": fichas,
            "followups": followups,
            "unavailable_reason": None if denominator > 0 else "Sin población base para meta mensual",
        })
    return rows


def _sapf_metric(
    payload: Dict[str, Any],
    population_result: Dict[str, Any],
    attendance_daily: List[Dict[str, Any]],
    scans_daily: List[Dict[str, Any]],
    start_date: date,
    end_date: date,
) -> Dict[str, Any]:
    """Seguimientos scored as positive activity against a prorated target.

    A higher score means the plantel registered enough follow-up activity for
    its population during the evaluated period. Zero follow-ups against a real
    population is a true 0, not a perfect score.
    """
    if not payload.get("_ok", True) or payload.get("error"):
        return _unavailable("—", "Seguimientos no respondió")

    population, population_basis, population_candidates = _sapf_population_base(population_result, attendance_daily, scans_daily)
    open_cases = safe_int(payload.get("open_cases"))
    closed_cases = safe_int(payload.get("closed_cases"))
    total_fichas = safe_int(payload.get("total_fichas"))
    total_followups = safe_int(payload.get("total_followups"))
    total_interactions = safe_int(payload.get("total_interactions")) or (total_fichas + total_followups)
    monthly_target_per_100 = 6.0
    target_per_100 = _monthly_target_per_100_for_period(start_date, end_date, monthly_target_per_100)
    target_followups = round((population * target_per_100) / 100.0, 1) if population > 0 and target_per_100 > 0 else 0.0
    followups_per_100 = round((total_followups / population) * 100.0, 1) if population > 0 else None
    activity_score = pct(total_followups, target_followups) if target_followups > 0 else None
    closure_score = pct(closed_cases, total_fichas) if total_fichas > 0 else None
    followup_density_score = pct(total_followups, total_fichas) if total_fichas > 0 else None
    if target_followups > 0:
        score = weighted_score(
            {
                "activity": activity_score,
                "closure": closure_score,
                "density": followup_density_score,
            },
            {"activity": 60, "closure": 25, "density": 15},
        )
    else:
        score = None
    monthly_rows = _sapf_monthly_breakdown(payload, population, start_date, end_date, monthly_target_per_100)

    return _metric(
        score,
        f"{total_followups:,} de {target_followups:.1f} seguimientos esperados" if target_followups > 0 else "—",
        (
            f"{followups_per_100:.1f} seguimientos por 100 alumnos · {closed_cases:,}/{total_fichas:,} fichas cerradas"
            if population > 0
            else f"{open_cases:,} abiertos · {closed_cases:,} cerrados · sin población"
        ),
        {
            "total_followups": total_followups,
            "target_followups": target_followups,
            "population": population,
            "population_basis": population_basis,
            "population_candidates": population_candidates,
            "followups_per_100_students": followups_per_100,
            "target_per_100_students": target_per_100,
            "monthly_target_per_100_students": monthly_target_per_100,
            "activity_score": activity_score,
            "closure_score": closure_score,
            "followup_density_score": followup_density_score,
            "open_cases": open_cases,
            "closed_cases": closed_cases,
            "total_fichas": total_fichas,
            "total_interactions": total_interactions,
            "total_followups_raw": total_followups,
            "complaints": safe_int(payload.get("complaints")),
            "basis": "blended_followups_population_closure_density",
            "breakdown": {
                "method": "Puntaje combinado de seguimiento",
                "formula": "60% volumen de seguimientos vs meta poblacional + 25% fichas cerradas + 15% densidad de seguimiento por ficha",
                "numerator": total_followups,
                "denominator": target_followups,
                "numerator_label": "Seguimientos registrados",
                "denominator_label": "Seguimientos esperados",
                "aggregation": "La meta se prorratea por población y periodo. El componente principal usa seguimientos reales, no solo fichas creadas.",
                "excluded": ["Actividad fuera del periodo", "Registros sin campus que coincida con el plantel"],
                "components": [
                    {"label": "Volumen vs meta poblacional", "score": activity_score, "weight": 60, "numerator": total_followups, "denominator": target_followups},
                    {"label": "Fichas cerradas", "score": closure_score, "weight": 25, "numerator": closed_cases, "denominator": total_fichas},
                    {"label": "Seguimientos por ficha", "score": followup_density_score, "weight": 15, "numerator": total_followups, "denominator": total_fichas},
                ],
                "monthly": monthly_rows,
            },
        },
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
    has_followup_base = any(key in usable_keys for key in ("planning", "observations", "observation_coverage"))
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


def _balance_monthly_breakdown(daily: List[Dict[str, Any]], business_days: List[date]) -> List[Dict[str, Any]]:
    by_day = {str(item.get("date")): item for item in daily or []}
    rows: List[Dict[str, Any]] = []
    month_keys = _month_keys_between(business_days[0], business_days[-1]) if business_days else []
    for month_key in month_keys:
        month_days = [day for day in business_days if day.isoformat().startswith(month_key)]
        numerator = 0
        denominator = 0
        scores: List[Optional[float]] = []
        available_days = 0
        for day in month_days:
            item = by_day.get(day.isoformat()) or {}
            entries = safe_int(item.get("scan_entries"))
            exits = safe_int(item.get("scan_exits"))
            if max(entries, exits) > 0:
                numerator += min(entries, exits)
                denominator += max(entries, exits)
                scores.append(item.get("scan_balance"))
                available_days += 1
        rows.append({
            "period": month_key,
            "label": _month_label(month_key),
            "numerator": numerator,
            "denominator": denominator,
            "numerator_label": "Accesos balanceados",
            "denominator_label": "Mayor total entradas/salidas",
            "score": average_score(scores),
            "available_days": available_days,
            "business_days": len(month_days),
            "unavailable_reason": None if available_days else "Sin días con entradas o salidas",
        })
    return rows


def _punctuality_monthly_breakdown(daily: List[Dict[str, Any]], business_days: List[date]) -> List[Dict[str, Any]]:
    by_day = {str(item.get("date")): item for item in daily or []}
    rows: List[Dict[str, Any]] = []
    month_keys = _month_keys_between(business_days[0], business_days[-1]) if business_days else []
    for month_key in month_keys:
        month_days = [day for day in business_days if day.isoformat().startswith(month_key)]
        numerator = 0
        denominator = 0
        scores: List[Optional[float]] = []
        available_days = 0
        tardies = 0
        for day in month_days:
            item = by_day.get(day.isoformat()) or {}
            opportunities = safe_int(item.get("punctuality_opportunities"))
            day_tardies = safe_int(item.get("tardies"))
            if opportunities > 0:
                numerator += max(opportunities - day_tardies, 0)
                denominator += opportunities
                tardies += day_tardies
                scores.append(item.get("student_punctuality"))
                available_days += 1
        rows.append({
            "period": month_key,
            "label": _month_label(month_key),
            "numerator": numerator,
            "denominator": denominator,
            "numerator_label": "Oportunidades sin retardo",
            "denominator_label": "Oportunidades alumno/día",
            "tardies": tardies,
            "score": average_score(scores),
            "available_days": available_days,
            "business_days": len(month_days),
            "unavailable_reason": None if available_days else "Sin oportunidades calculables",
        })
    return rows


def _general_monthly_breakdown(daily: List[Dict[str, Any]], business_days: List[date]) -> List[Dict[str, Any]]:
    by_day = {str(item.get("date")): item for item in daily or []}
    rows: List[Dict[str, Any]] = []
    month_keys = _month_keys_between(business_days[0], business_days[-1]) if business_days else []
    for month_key in month_keys:
        month_days = [day for day in business_days if day.isoformat().startswith(month_key)]
        scores: List[Optional[float]] = []
        for day in month_days:
            item = by_day.get(day.isoformat()) or {}
            if clamp_score(item.get("general")) is not None:
                scores.append(item.get("general"))
        rows.append({
            "period": month_key,
            "label": _month_label(month_key),
            "numerator": average_score(scores),
            "denominator": 100,
            "numerator_label": "Promedio diario general",
            "denominator_label": "Escala máxima",
            "score": average_score(scores),
            "available_days": len(scores),
            "business_days": len(month_days),
            "unavailable_reason": None if scores else "Sin días con suficientes métricas operativas",
        })
    return rows


def _ensure_breakdown(metric: Dict[str, Any], metric_key: str, reason: Optional[str] = None) -> Dict[str, Any]:
    if metric.get("breakdown"):
        return metric
    label = _metric_label(metric_key)
    unavailable = clamp_score(metric.get("score")) is None
    metric["breakdown"] = {
        "method": "Sin cálculo" if unavailable else "Valor del periodo",
        "formula": "No disponible" if unavailable else "Ver numerador y denominador del periodo",
        "numerator": None,
        "denominator": None,
        "numerator_label": label,
        "denominator_label": "Base esperada",
        "aggregation": (reason or _plain_unavailable_reason(metric)) if unavailable else "Valor calculado para el periodo seleccionado.",
        "excluded": [],
        "monthly": [],
    }
    return metric


def _attach_drilldowns(
    metrics: Dict[str, Dict[str, Any]],
    daily: List[Dict[str, Any]],
    business_days: List[date],
) -> Dict[str, Dict[str, Any]]:
    if metrics.get("roll_call"):
        metrics["roll_call"] = _with_breakdown(metrics["roll_call"], {
            "method": "Promedio diario",
            "formula": "promedio de (grupos capturados / grupos esperados) por día",
            "numerator": metrics["roll_call"].get("completed"),
            "denominator": metrics["roll_call"].get("expected"),
            "numerator_label": "Grupos/día capturados",
            "denominator_label": "Grupos/día esperados",
            "aggregation": "El valor final promedia los días hábiles con denominador real.",
            "excluded": ["Días no hábiles", "Días sin grupos esperados"],
            "monthly": _daily_monthly_breakdown(daily, business_days, "roll_call", "completed_lists", "expected_lists", "Grupos/día capturados", "Grupos/día esperados"),
        })
    if metrics.get("student_attendance"):
        metrics["student_attendance"] = _with_breakdown(metrics["student_attendance"], {
            "method": "Promedio diario",
            "formula": "promedio de (alumnos presentes / registros de asistencia) por día",
            "numerator": metrics["student_attendance"].get("present"),
            "denominator": metrics["student_attendance"].get("records"),
            "numerator_label": "Alumnos presentes",
            "denominator_label": "Registros de asistencia",
            "aggregation": "El valor final promedia la asistencia diaria capturada.",
            "excluded": ["Días no hábiles", "Días sin listas capturadas"],
            "monthly": _daily_monthly_breakdown(daily, business_days, "student_attendance", "present", "attendance_records", "Alumnos presentes", "Registros de asistencia"),
        })
    if metrics.get("scans"):
        metrics["scans"] = _with_breakdown(metrics["scans"], {
            "method": "Promedio diario",
            "formula": "promedio de (entradas registradas / población esperada o base observada) por día",
            "numerator": metrics["scans"].get("entries"),
            "denominator": metrics["scans"].get("expected"),
            "numerator_label": "Entradas registradas",
            "denominator_label": "Entradas esperadas",
            "aggregation": "El valor final promedia los porcentajes diarios.",
            "excluded": ["Días no hábiles", "Días sin base poblacional ni accesos observados"],
            "monthly": _daily_monthly_breakdown(daily, business_days, "scans", "scan_entries", "scan_expected", "Entradas registradas", "Entradas esperadas"),
        })
    if metrics.get("scan_balance"):
        metrics["scan_balance"] = _with_breakdown(metrics["scan_balance"], {
            "method": "Promedio diario",
            "formula": "promedio de (menor entre entradas/salidas / mayor entre entradas/salidas) por día",
            "numerator": min(safe_int(metrics["scan_balance"].get("entries")), safe_int(metrics["scan_balance"].get("exits"))),
            "denominator": max(safe_int(metrics["scan_balance"].get("entries")), safe_int(metrics["scan_balance"].get("exits"))),
            "numerator_label": "Accesos balanceados",
            "denominator_label": "Mayor total entradas/salidas",
            "aggregation": "El valor final promedia solo días con movimiento de acceso.",
            "excluded": ["Días sin entradas ni salidas"],
            "monthly": _balance_monthly_breakdown(daily, business_days),
        })
    if metrics.get("student_punctuality"):
        opportunities = safe_int(metrics["student_punctuality"].get("opportunities"))
        tardies = safe_int(metrics["student_punctuality"].get("tardies"))
        metrics["student_punctuality"] = _with_breakdown(metrics["student_punctuality"], {
            "method": "Promedio diario inverso",
            "formula": "promedio de 100 - (retardos / oportunidades alumno-día)",
            "numerator": max(opportunities - tardies, 0),
            "denominator": opportunities,
            "numerator_label": "Oportunidades sin retardo",
            "denominator_label": "Oportunidades alumno/día",
            "aggregation": "El valor final promedia los días con oportunidades calculables.",
            "excluded": ["Días sin población, asistencia o accesos para estimar oportunidades"],
            "monthly": _punctuality_monthly_breakdown(daily, business_days),
        })
    for key in DISPLAY_METRICS:
        if key in metrics:
            _ensure_breakdown(metrics[key], key)
    if metrics.get("general"):
        metrics["general"] = _with_breakdown(metrics["general"], {
            "method": "Promedio ponderado",
            "formula": "suma(score de cada métrica calculable x peso) / suma(pesos calculables)",
            "numerator": metrics["general"].get("weights_used"),
            "denominator": metrics["general"].get("weights_total"),
            "numerator_label": "Peso calculable usado",
            "denominator_label": "Peso total aprobado",
            "aggregation": "Solo entran métricas con denominador real y suficiente evidencia operativa.",
            "excluded": [key for key in METRIC_WEIGHTS if key not in (metrics["general"].get("metrics_used") or [])],
            "components": [
                {"key": key, "label": _metric_label(key), "score": (metrics.get(key) or {}).get("score"), "weight": METRIC_WEIGHTS.get(key)}
                for key in METRIC_WEIGHTS
            ],
            "monthly": _general_monthly_breakdown(daily, business_days),
        })
    return metrics


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


async def _fetch_husky_cycle_bundle(
    husky_values: List[str],
    start_date: date,
    end_date: date,
    threshold_time: str,
) -> Dict[str, Any]:
    """Fetch Husky access evidence with small aggregate queries.

    The Ciclo Escolar heatmap must not publish failed access cells just because
    a per-user year-sized rollup is too heavy.  This bundle reads the exact
    daily aggregates required by Escaneos/Balance and the daily tardy counts
    required by Puntualidad, chunked across the requested period.
    """
    scan_rows = await _chunked_rows(
        fetch_husky_daily_scan_counts,
        husky_values,
        start_date=start_date,
        end_date=end_date,
        max_days=14,
    )
    tardy_rows = await _chunked_rows(
        fetch_husky_tardy_daily_counts,
        husky_values,
        start_date=start_date,
        end_date=end_date,
        max_days=14,
        threshold_time=threshold_time,
    )
    return {
        "scans": scan_rows,
        "retardos": tardy_rows,
        "scan_rows": len(scan_rows or []),
        "tardy_rows": len(tardy_rows or []),
        "strategy": "chunked_daily_aggregate_husky_queries",
    }


def _clean_sources(sources: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    clean: Dict[str, Any] = {}
    for key, payload in sources.items():
        clean[key] = {
            "ok": bool(payload.get("_ok", True)) and not bool(payload.get("error")),
            "duration_ms": payload.get("_duration_ms"),
            "error": payload.get("error"),
            "timeout": bool(payload.get("timeout")),
            "strategy": payload.get("strategy") or payload.get("derived_from"),
            "rows": payload.get("raw_rows"),
        }
    return clean


async def _collect_plantel(code: str, start_date: date, end_date: date, scope: str, business_days: List[date]) -> Dict[str, Any]:
    plantel_info = resolve_plantel(code)
    attendance_aliases = _attendance_aliases(plantel_info)
    husky_values = _husky_values(plantel_info)
    sheets_code = plantel_info.get("sheets_code") or code
    threshold_time = _tardy_threshold(plantel_info)

    heavy_timeout = _source_timeout_for_range(start_date, end_date, 24.0)
    roster_timeout = 32.0
    sources = await asyncio.gather(
        _safe_value_call("expected_groups", lambda: fetch_expected_groups(sheets_code), roster_timeout),
        _safe_value_call("expected_population", lambda: fetch_expected_population(sheets_code), roster_timeout),
        _safe_value_call(
            "attendance_db",
            lambda: _chunked_rows(fetch_attendance_rollup, attendance_aliases, start_date=start_date, end_date=end_date),
            heavy_timeout,
        ),
        _safe_value_call(
            "husky_access_db",
            lambda: _fetch_husky_cycle_bundle(husky_values, start_date, end_date, threshold_time),
            heavy_timeout,
        ),
        _safe_call("academic", lambda: _academic_metrics(plantel_info, start_date, end_date, business_days), max(SOURCE_TIMEOUTS["academic"], heavy_timeout)),
        _safe_call("sapf", lambda: get_sapf_overview_report(code, start_date, end_date, scope), max(SOURCE_TIMEOUTS["sapf"], heavy_timeout)),
    )
    husky_bundle = dict(sources[3])
    if husky_bundle.get("_ok"):
        bundle_value = husky_bundle.get("value") or {}
        husky_source = {
            "_ok": True,
            "value": bundle_value.get("scans") or [],
            "_duration_ms": husky_bundle.get("_duration_ms"),
            "strategy": bundle_value.get("strategy"),
            "raw_rows": bundle_value.get("scan_rows"),
        }
        retardos_source = {
            "_ok": True,
            "value": bundle_value.get("retardos") or [],
            "_duration_ms": husky_bundle.get("_duration_ms"),
            "derived_from": "husky_access_db",
            "strategy": bundle_value.get("strategy"),
            "raw_rows": bundle_value.get("tardy_rows"),
        }
    else:
        husky_source = dict(husky_bundle)
        husky_source["value"] = []
        retardos_source = {
            "_ok": False,
            "value": [],
            "_duration_ms": husky_bundle.get("_duration_ms"),
            "error": husky_bundle.get("error"),
            "timeout": husky_bundle.get("timeout"),
            "derived_from": "husky_access_db",
        }
    source_map = {
        "expected_groups": sources[0],
        "expected_population": sources[1],
        "attendance": sources[2],
        "husky": husky_source,
        "retardos": retardos_source,
        "academic": sources[4],
        "seguimientos": sources[5],
    }

    attendance_rows = source_map["attendance"].get("value") if source_map["attendance"].get("_ok") else []
    husky_rows = source_map["husky"].get("value") if source_map["husky"].get("_ok") else []
    tardy_rows = source_map["retardos"].get("value") if source_map["retardos"].get("_ok") else []
    entry_time_daily = _entry_time_daily_points(husky_rows, business_days) if source_map["husky"].get("_ok") else []

    if source_map["attendance"].get("_ok"):
        roll_call, student_attendance, attendance_daily = _attendance_metrics_from_rollup(attendance_rows, source_map["expected_groups"], business_days)
    else:
        roll_call = _unavailable("—", "Asistencia no respondió")
        student_attendance = _unavailable("—", "Asistencia no respondió")
        attendance_daily = []

    if source_map["husky"].get("_ok"):
        scans, scan_balance, scans_daily = _scans_metric_from_rows(husky_rows, source_map["expected_population"], attendance_daily, business_days)
    else:
        scans = _unavailable("—", "Escaneos en reintento")
        scans.update({"source_failure": True, "retryable": True, "source_key": "husky", "failure_reason": source_map["husky"].get("error")})
        scan_balance = _unavailable("—", "Balance en reintento")
        scan_balance.update({"source_failure": True, "retryable": True, "source_key": "husky", "failure_reason": source_map["husky"].get("error")})
        scans_daily = []

    if source_map["retardos"].get("_ok"):
        student_punctuality, punctuality_daily = _student_punctuality_metric_from_counts(tardy_rows, source_map["expected_population"], attendance_daily, scans_daily, business_days)
    else:
        student_punctuality = _unavailable("—", "Puntualidad en reintento")
        student_punctuality.update({"source_failure": True, "retryable": True, "source_key": "retardos", "failure_reason": source_map["retardos"].get("error")})
        punctuality_daily = []
    academic_payload = source_map["academic"] if isinstance(source_map["academic"], dict) else {}
    planning = academic_payload.get("planning") if isinstance(academic_payload.get("planning"), dict) else _unavailable("—", "Sin cálculo de planeaciones")
    observations = academic_payload.get("observations") if isinstance(academic_payload.get("observations"), dict) else _unavailable("—", "Sin cálculo de observaciones")
    observation_coverage = academic_payload.get("observation_coverage") if isinstance(academic_payload.get("observation_coverage"), dict) else _unavailable("—", "Sin cálculo de cobertura docente")
    sapf = _sapf_metric(source_map["seguimientos"], source_map["expected_population"], attendance_daily, scans_daily, start_date, end_date)

    metrics = {
        "roll_call": roll_call,
        "student_attendance": student_attendance,
        "scans": scans,
        "scan_balance": scan_balance,
        "student_punctuality": student_punctuality,
        "planning": planning,
        "observations": observations,
        "observation_coverage": observation_coverage,
        "sapf": sapf,
    }
    general = _general_metric(metrics)
    metrics = {"general": general, **metrics}
    daily = _merge_daily_points(attendance_daily, scans_daily, punctuality_daily, business_days)
    metrics = _attach_drilldowns(metrics, daily, business_days)

    order = FIXED_PLANTEL_ORDER.index(code) if code in FIXED_PLANTEL_ORDER else 999
    return {
        "plantel": code,
        "order": order,
        "resolved_name": plantel_info.get("resolved_name"),
        "short_name": plantel_info.get("short_name"),
        "metrics": metrics,
        "index": general,
        "domains": metrics,
        "daily": daily,
        "entry_time_daily": entry_time_daily,
        "source_audit": _clean_sources(source_map),
        "academic_shape": academic_payload.get("shape") if isinstance(academic_payload, dict) else {},
    }




def _metric_evidence(metric: Dict[str, Any], keys: Iterable[str]) -> Dict[str, Any]:
    output: Dict[str, Any] = {"score": metric.get("score")}
    for key in keys:
        if key in metric:
            output[key] = metric.get(key)
    return output


def _followups_evidence(metric: Dict[str, Any]) -> Dict[str, Any]:
    """Compact diagnostic for Seguimientos target scoring."""
    return {
        "score": metric.get("score"),
        "total": safe_int(metric.get("total_followups")),
        "seguimientos": safe_int(metric.get("total_followups_raw")),
        "fichas": safe_int(metric.get("total_fichas")),
        "esperados": metric.get("target_followups"),
        "poblacion": safe_int(metric.get("population")),
        "population_basis": metric.get("population_basis"),
        "population_candidates": metric.get("population_candidates"),
        "por_100": metric.get("followups_per_100_students"),
        "meta_por_100_periodo": metric.get("target_per_100_students"),
        "activity_score": metric.get("activity_score"),
        "closure_score": metric.get("closure_score"),
        "followup_density_score": metric.get("followup_density_score"),
        "basis": metric.get("basis"),
    }


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
                "escaneos": _metric_evidence(metrics.get("scans") or {}, ["entries", "exits", "expected", "expected_population", "observed_max_entries", "observed_max_attendance_records", "population_candidates", "days_with_records", "basis"]),
                "balance_escaneos": _metric_evidence(metrics.get("scan_balance") or {}, ["entries", "exits", "gap_total", "days_with_records", "basis"]),
                "puntualidad_alumnos": _metric_evidence(metrics.get("student_punctuality") or {}, ["tardies", "opportunities", "expected_population", "observed_max_entries", "observed_max_attendance_records", "population_candidates", "days_with_records", "basis"]),
                "planeaciones": _metric_evidence(metrics.get("planning") or {}, ["submitted", "reviewed", "pending", "raw_rows", "active_teachers", "submitted_teachers", "created_at_start", "created_at_end", "min_created_at", "max_created_at", "basis"]),
                "observaciones": _metric_evidence(metrics.get("observations") or {}, ["total_observations", "avg_monthly_observations", "monthly_goal", "months_count", "active_teachers", "window_start", "window_end", "active_window_start", "active_window_end", "basis"]),
                "cobertura_observaciones": _metric_evidence(metrics.get("observation_coverage") or {}, ["active_teachers", "avg_active_teachers", "avg_teachers_with_2plus", "observed_teachers", "teachers_with_2plus", "without_observation", "months_count", "window_start", "window_end", "basis"]),
                "seguimientos": _followups_evidence(metrics.get("sapf") or {}),
            },
            "general": _metric_evidence(metrics.get("general") or {}, ["weights_used", "weights_total", "metrics_used", "minimum_weight", "minimum_metrics"]),
        }
    return {
        "v": "corp-diagnostic-v20",
        "range": {"start": start_date.isoformat(), "end": end_date.isoformat(), "business_days": len(business_days)},
        "planteles": rows,
        "hora_promedio_entrada_nivel": _level_access_summary(planteles, business_days),
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


def _format_hhmm(seconds: Any) -> Optional[str]:
    parsed = safe_float(seconds)
    if parsed is None:
        return None
    total = int(round(parsed)) % 86400
    hour = total // 3600
    minute = (total % 3600) // 60
    return f"{hour:02d}:{minute:02d}"


def _entry_time_daily_points(rows: List[Dict[str, Any]], business_days: List[date]) -> List[Dict[str, Any]]:
    """Average hora of entrada access rows from the lightweight Husky rollup."""
    allowed = {day.isoformat() for day in business_days}
    points: List[Dict[str, Any]] = []
    for row in rows or []:
        day = _date_key(row.get("date") or row.get("fecha"))
        tipo = str(row.get("tipo_accion") or "").strip().lower()
        if tipo and tipo != "entrada":
            continue
        if not day or day not in allowed:
            continue
        avg_seconds = safe_float(row.get("avg_entry_seconds"))
        samples = safe_int(row.get("samples")) or safe_int(row.get("total_scans"))
        if avg_seconds is None or samples <= 0:
            continue
        first_entry = str(row.get("first_entry_time") or "")[:5] or None
        last_entry = str(row.get("last_entry_time") or "")[:5] or None
        points.append({
            "date": day,
            "avg_entry_seconds": avg_seconds,
            "avg_entry_time": _format_hhmm(avg_seconds),
            "samples": samples,
            "first_entry_time": first_entry,
            "last_entry_time": last_entry,
        })
    points.sort(key=lambda item: item["date"])
    return points


def _level_access_summary(planteles: List[Dict[str, Any]], business_days: List[date]) -> Dict[str, Any]:
    """Average entrada access time by derived school level.

    Husky does not expose a `nivel` field. The level is derived from the plantel
    code: PT/PM=Primaria, ST/SM=Secundaria, PREET/PREEM=Preescolar.
    The average is weighted by entrada samples returned by the Husky rollup.
    """
    level_stats: Dict[str, Dict[str, Any]] = {
        level: {"weighted_seconds": 0.0, "samples": 0, "days": set(), "planteles": set(), "first_times": [], "last_times": []}
        for level in LEVEL_ORDER
    }

    for plantel in planteles:
        code = str(plantel.get("plantel") or "")
        level = PLANTEL_LEVELS.get(code)
        if not level:
            continue
        stats = level_stats.setdefault(level, {"weighted_seconds": 0.0, "samples": 0, "days": set(), "planteles": set(), "first_times": [], "last_times": []})
        stats["planteles"].add(code)
        for point in plantel.get("entry_time_daily") or []:
            samples = safe_int(point.get("samples"))
            avg_seconds = safe_float(point.get("avg_entry_seconds"))
            if samples <= 0 or avg_seconds is None:
                continue
            stats["weighted_seconds"] += avg_seconds * samples
            stats["samples"] += samples
            if point.get("date"):
                stats["days"].add(str(point.get("date")))
            if point.get("first_entry_time"):
                stats["first_times"].append(str(point.get("first_entry_time"))[:5])
            if point.get("last_entry_time"):
                stats["last_times"].append(str(point.get("last_entry_time"))[:5])

    rows: List[Dict[str, Any]] = []
    for level in LEVEL_ORDER:
        stats = level_stats.get(level) or {}
        samples = safe_int(stats.get("samples"))
        avg_seconds = (float(stats.get("weighted_seconds") or 0.0) / samples) if samples > 0 else None
        rows.append({
            "nivel": level,
            "planteles": sorted(stats.get("planteles") or []),
            "avg_entry_seconds": round(avg_seconds, 1) if avg_seconds is not None else None,
            "avg_entry_time": _format_hhmm(avg_seconds),
            "sample_count": samples,
            "days_with_samples": len(stats.get("days") or set()),
            "business_days": len(business_days),
            "first_entry_time": min(stats.get("first_times") or []) if stats.get("first_times") else None,
            "last_entry_time": max(stats.get("last_times") or []) if stats.get("last_times") else None,
            "basis": "entrada_access_time_derived_level",
        })
    return {
        "basis": "nivel_derivado_por_plantel_no_campo_db",
        "rows": rows,
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
        "planning": "Planeaciones",
        "observations": "Observaciones",
        "observation_coverage": "Cobertura obs.",
        "sapf": "Seguimientos",
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

    # Ciclo escolar is the default and must not serialize six campuses through
    # several large source reads. The source queries are already bounded and
    # chunked, so collecting the selected planteles concurrently prevents proxy
    # 502s caused by request time, while each source can still fail closed as —.
    concurrency = min(len(selected), 3 if scope == "ciclo_escolar" else 3) or 1
    semaphore = asyncio.Semaphore(concurrency)

    async def collect(code: str) -> Dict[str, Any]:
        async with semaphore:
            try:
                return await _collect_plantel(code, start_date, end_date, scope, business_days)
            except Exception as exc:
                logger.error("Corporate report plantel collection failed for %s: %s", code, exc)
                plantel_info = resolve_plantel(code)
                unavailable = _unavailable("—", f"No se pudo calcular plantel: {exc}")
                order = FIXED_PLANTEL_ORDER.index(code) if code in FIXED_PLANTEL_ORDER else 999
                return {
                    "plantel": code,
                    "order": order,
                    "resolved_name": plantel_info.get("resolved_name"),
                    "short_name": plantel_info.get("short_name"),
                    "metrics": {key: dict(unavailable) for key in ["general", *DISPLAY_METRICS]},
                    "index": unavailable,
                    "domains": {},
                    "daily": [],
                    "entry_time_daily": [],
                    "source_audit": {"plantel": {"ok": False, "error": str(exc), "timeout": False}},
                    "academic_shape": {},
                }

    plantel_payloads = await asyncio.gather(*(collect(code) for code in selected))
    plantel_payloads.sort(key=lambda item: item.get("order", 999))
    aggregate = _aggregate(plantel_payloads, start_date, end_date)
    access_by_level = _level_access_summary(plantel_payloads, business_days)

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
        "access_by_level": access_by_level,
        "planteles": plantel_payloads,
        "source_audit": {p["plantel"]: p.get("source_audit") for p in plantel_payloads},
        "diagnostic": _diagnostic(plantel_payloads, start_date, end_date, business_days),
        "meta": {
            "logic_version": "2026-06-23-heatmap-data-retry-v20",
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "business_days": len(business_days),
        },
    }
