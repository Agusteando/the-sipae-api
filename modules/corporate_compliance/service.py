from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional
from zoneinfo import ZoneInfo

from core.logger import get_logger
from core.utils import resolve_plantel
from modules.academic.service import (
    get_observaciones_docentes_report,
    get_planeaciones_pendientes_report,
)
from modules.attendance.service import get_attendance_detail_report
from modules.husky.service import calculate_husky_daily_rate, get_plantel_retardos
from modules.sapf.service import get_sapf_overview_report

from .scoring import (
    METRIC_WEIGHTS,
    average_score,
    clamp_score,
    pct,
    safe_float,
    safe_int,
    score_from_penalty,
    status_for_score,
    traffic_for_score,
)

logger = get_logger("service.corporate_compliance")

FIXED_PLANTEL_ORDER = ["PT", "PM", "ST", "SM", "PREET", "PREEM"]
DISPLAY_METRICS = ["general", "attendance", "lists", "tardies", "academic", "sapf"]
SOURCE_TIMEOUTS = {
    "attendance": 26.0,
    "husky": 18.0,
    "retardos": 18.0,
    "academic_observations": 14.0,
    "academic_plans": 14.0,
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


def _attendance_and_lists_metrics(payload: Dict[str, Any], business_days: List[date]) -> tuple[Dict[str, Any], Dict[str, Any], List[Dict[str, Any]]]:
    if not payload.get("_ok", True) or payload.get("error"):
        empty = _metric(None, "Sin datos", "No respondió.")
        return empty, empty, []

    points = _daily_attendance_points(payload, business_days)
    total_records = sum(safe_int(p.get("records")) for p in points)
    present = sum(safe_int(p.get("present")) for p in points)
    absent = sum(safe_int(p.get("absent")) for p in points)
    expected_lists = sum(safe_int(p.get("expected_lists")) for p in points)
    completed_lists = sum(safe_int(p.get("completed_lists")) for p in points)
    missing_lists = sum(safe_int(p.get("missing_lists")) for p in points)

    attendance_score = pct(present, total_records)
    lists_score = pct(completed_lists, expected_lists)

    attendance = _metric(
        attendance_score,
        f"{present:,} presentes de {total_records:,} registros" if total_records > 0 else "Sin registros",
        f"{absent:,} ausencias registradas" if total_records > 0 else "Sin denominador de asistencia",
        {
            "present": present,
            "records": total_records,
            "absent": absent,
            "absence_rate": pct(absent, total_records),
        },
    )
    lists = _metric(
        lists_score,
        f"{completed_lists:,} de {expected_lists:,} listas" if expected_lists > 0 else "Sin listas esperadas",
        f"{missing_lists:,} listas faltantes" if expected_lists > 0 else "Sin denominador de listas",
        {
            "expected": expected_lists,
            "completed": completed_lists,
            "missing": missing_lists,
        },
    )
    return attendance, lists, points


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


def _tardy_counts(payload: Dict[str, Any], business_days: Iterable[date]) -> Dict[str, int]:
    allowed = {d.isoformat() for d in business_days}
    counts: Dict[str, int] = defaultdict(int)
    for row in _as_list(payload.get("retardos")):
        day = _date_key(row.get("date"))
        if day and day in allowed:
            counts[day] += 1
    return dict(counts)


def _tardies_metric(husky_payload: Dict[str, Any], tardies_payload: Dict[str, Any], business_days: List[date]) -> tuple[Dict[str, Any], List[Dict[str, Any]]]:
    if not tardies_payload.get("_ok", True) or tardies_payload.get("error"):
        return _metric(None, "Sin datos", "No respondió."), []

    husky_points = _husky_daily_points(husky_payload, business_days)
    tardy_by_day = _tardy_counts(tardies_payload, business_days)
    daily: List[Dict[str, Any]] = []
    total_entries = 0
    total_tardies = 0

    for day in [d.isoformat() for d in business_days]:
        entries = safe_int((husky_points.get(day) or {}).get("entrada"))
        tardies = safe_int(tardy_by_day.get(day))
        total_entries += entries
        total_tardies += tardies
        rate = pct(tardies, entries)
        daily.append({
            "date": day,
            "entries": entries,
            "tardies": tardies,
            "tardies_per_100": rate,
            "score": score_from_penalty(rate, 6.0),
        })

    rate = pct(total_tardies, total_entries)
    score = score_from_penalty(rate, 6.0)
    metric = _metric(
        score,
        f"{total_tardies:,} retardos" if total_entries > 0 else "Sin entradas registradas",
        f"{rate:.1f} retardos por 100 entradas" if rate is not None else "Sin denominador de entradas",
        {
            "tardies": total_tardies,
            "entries": total_entries,
            "tardies_per_100": rate,
        },
    )
    return metric, daily


def _academic_metric(observations_payload: Dict[str, Any], plans_payload: Dict[str, Any]) -> Dict[str, Any]:
    observation_summary = observations_payload.get("summary") or {}
    plan_summary = plans_payload.get("summary") or {}

    active_teachers = max(
        safe_int(observation_summary.get("total_docentes_activos")),
        safe_int(plan_summary.get("docentes_activos")),
    )
    teachers_without_observation = safe_int(observation_summary.get("total_docentes_sin_observacion_30_dias"))
    pending_plans = safe_int(plan_summary.get("total_planeaciones_pendientes"))

    observation_score = None
    if active_teachers > 0:
        observation_score = pct(max(active_teachers - teachers_without_observation, 0), active_teachers)

    plan_score = None
    if active_teachers > 0:
        tolerated_pending_capacity = max(active_teachers * 2, 1)
        plan_score = clamp_score(100.0 - min((pending_plans / tolerated_pending_capacity) * 100.0, 100.0))

    score = average_score([observation_score, plan_score])
    return _metric(
        score,
        f"{pending_plans:,} pendientes" if active_teachers > 0 else "Sin docentes activos",
        f"{teachers_without_observation:,} docentes sin observación reciente" if active_teachers > 0 else "Sin denominador académico",
        {
            "active_teachers": active_teachers,
            "teachers_without_observation": teachers_without_observation,
            "pending_plans": pending_plans,
            "observation_score": observation_score,
            "planning_score": plan_score,
        },
    )


def _sapf_metric(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not payload.get("_ok", True) or payload.get("error"):
        return _metric(None, "Sin datos", "No respondió.")

    total_fichas = safe_int(payload.get("total_fichas"))
    total_followups = safe_int(payload.get("total_followups"))
    open_cases = safe_int(payload.get("open_cases"))
    closed_cases = safe_int(payload.get("closed_cases"))
    complaints = safe_int(payload.get("complaints"))
    total_cases = open_cases + closed_cases

    if total_cases > 0:
        score = pct(closed_cases, total_cases)
    elif total_fichas > 0 or total_followups > 0:
        score = 100.0
    else:
        score = None

    return _metric(
        score,
        f"{closed_cases:,} cerrados de {total_cases:,} casos" if total_cases > 0 else (f"{total_fichas:,} fichas" if total_fichas > 0 else "Sin fichas"),
        f"{open_cases:,} abiertos · {complaints:,} quejas" if total_cases > 0 else "Sin casos abiertos registrados",
        {
            "total_fichas": total_fichas,
            "followups": total_followups,
            "open_cases": open_cases,
            "closed_cases": closed_cases,
            "complaints": complaints,
        },
    )


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


def _general_metric(metrics: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    raw_scores = {key: (metrics.get(key) or {}).get("score") for key in METRIC_WEIGHTS}
    score = None
    weighted_numerator = 0.0
    used_weight = 0.0
    for key, weight in METRIC_WEIGHTS.items():
        metric_score = clamp_score(raw_scores.get(key))
        if metric_score is None:
            continue
        weighted_numerator += metric_score * weight
        used_weight += weight
    if used_weight > 0:
        score = clamp_score(weighted_numerator / used_weight)
    return _metric(
        score,
        "Promedio ponderado" if score is not None else "Sin datos",
        "Asistencia, listas, retardos, académico y SAPF",
        {"weights_used": used_weight, "weights_total": sum(METRIC_WEIGHTS.values())},
    )


def _bucket_label(day: date, start_date: date, end_date: date) -> str:
    span = max((end_date - start_date).days + 1, 1)
    if span <= 45:
        return day.strftime("%d/%m")
    if span <= 130:
        return f"Sem {((day - start_date).days // 7) + 1}"
    return day.strftime("%Y-%m")


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
                if metric_key == "general":
                    value = _daily_general_score(point)
                else:
                    value = point.get(metric_key)
                buckets[label].append(clamp_score(value))
            values = [average_score(buckets[label]) for label in labels]
            output.append({"plantel": plantel.get("plantel"), "name": plantel.get("short_name"), "values": values})
        return output

    return {
        "labels": labels,
        "metrics": {
            "general": {"label": "General", "unit": "%", "series": series("general")},
            "attendance": {"label": "Asistencia", "unit": "%", "series": series("attendance")},
            "lists": {"label": "Listas", "unit": "%", "series": series("lists")},
            "tardies": {"label": "Retardos", "unit": "%", "series": series("tardies")},
        },
    }


def _daily_general_score(point: Dict[str, Any]) -> Optional[float]:
    weighted_numerator = 0.0
    used_weight = 0.0
    for key, weight in (("attendance", 30), ("lists", 25), ("tardies", 15)):
        score = clamp_score(point.get(key))
        if score is None:
            continue
        weighted_numerator += score * weight
        used_weight += weight
    if used_weight <= 0:
        return None
    return clamp_score(weighted_numerator / used_weight)


def _merge_daily_points(attendance_daily: List[Dict[str, Any]], tardy_daily: List[Dict[str, Any]], business_days: List[date]) -> List[Dict[str, Any]]:
    attendance_by_day = {item["date"]: item for item in attendance_daily}
    tardy_by_day = {item["date"]: item for item in tardy_daily}
    output: List[Dict[str, Any]] = []
    for day in business_days:
        key = day.isoformat()
        att = attendance_by_day.get(key) or {}
        tardy = tardy_by_day.get(key) or {}
        attendance_score = pct(att.get("present"), att.get("records"))
        lists_score = pct(att.get("completed_lists"), att.get("expected_lists"))
        output.append({
            "date": key,
            "attendance": attendance_score,
            "lists": lists_score,
            "tardies": tardy.get("score"),
            "attendance_records": safe_int(att.get("records")),
            "present": safe_int(att.get("present")),
            "expected_lists": safe_int(att.get("expected_lists")),
            "completed_lists": safe_int(att.get("completed_lists")),
            "tardies_count": safe_int(tardy.get("tardies")),
            "entries": safe_int(tardy.get("entries")),
        })
    return output


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

    sources = await asyncio.gather(
        _safe_call("attendance", lambda: get_attendance_detail_report(code, start_date, end_date, scope), SOURCE_TIMEOUTS["attendance"]),
        _safe_call("husky", lambda: calculate_husky_daily_rate(code, start_date, end_date, scope), SOURCE_TIMEOUTS["husky"]),
        _safe_call("retardos", lambda: get_plantel_retardos(code, start_date, end_date, scope), SOURCE_TIMEOUTS["retardos"]),
        _safe_call("academic_observations", lambda: get_observaciones_docentes_report(code), SOURCE_TIMEOUTS["academic_observations"]),
        _safe_call("academic_plans", lambda: get_planeaciones_pendientes_report(code, start_date, end_date, scope), SOURCE_TIMEOUTS["academic_plans"]),
        _safe_call("sapf", lambda: get_sapf_overview_report(code, start_date, end_date, scope), SOURCE_TIMEOUTS["sapf"]),
    )
    source_map = {
        "attendance": sources[0],
        "husky": sources[1],
        "retardos": sources[2],
        "academic_observations": sources[3],
        "academic_plans": sources[4],
        "sapf": sources[5],
    }

    attendance_metric, lists_metric, attendance_daily = _attendance_and_lists_metrics(source_map["attendance"], business_days)
    tardies_metric, tardy_daily = _tardies_metric(source_map["husky"], source_map["retardos"], business_days)
    academic_metric = _academic_metric(source_map["academic_observations"], source_map["academic_plans"])
    sapf_metric = _sapf_metric(source_map["sapf"])

    metrics = {
        "attendance": attendance_metric,
        "lists": lists_metric,
        "tardies": tardies_metric,
        "academic": academic_metric,
        "sapf": sapf_metric,
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
        "daily": _merge_daily_points(attendance_daily, tardy_daily, business_days),
        "source_audit": _clean_sources(source_map),
    }


def _domain_averages(planteles: List[Dict[str, Any]]) -> Dict[str, Any]:
    output: Dict[str, Any] = {}
    for key in DISPLAY_METRICS:
        values = [(p.get("metrics") or {}).get(key, {}).get("score") for p in planteles]
        score = average_score(values)
        output[key] = _metric(score, "Promedio", "Promedio de planteles con dato")
    return output


def _aggregate(planteles: List[Dict[str, Any]], start_date: date, end_date: date) -> Dict[str, Any]:
    scored = [p for p in planteles if (p.get("index") or {}).get("score") is not None]
    corporate_score = average_score([(p.get("index") or {}).get("score") for p in scored])
    best = max(scored, key=lambda item: safe_float((item.get("index") or {}).get("score")) or -1) if scored else None
    worst = min(scored, key=lambda item: safe_float((item.get("index") or {}).get("score")) or 101) if scored else None
    domain_scores = _domain_averages(planteles)

    return {
        "corporate_index": _metric(
            corporate_score,
            "Cumplimiento general" if corporate_score is not None else "Sin datos",
            f"{len(scored)} de {len(planteles)} planteles con cálculo" if planteles else "Sin planteles",
        ),
        "best_plantel": _plantel_summary(best),
        "worst_plantel": _plantel_summary(worst),
        "domain_scores": domain_scores,
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

    domain_rows = []
    for key, metric in (aggregate.get("domain_scores") or {}).items():
        if key == "general":
            continue
        domain_rows.append({
            "key": key,
            "label": _metric_label(key),
            "score": metric.get("score"),
            "status": metric.get("status"),
            "color": metric.get("color"),
        })
    domain_rows.sort(key=lambda item: item.get("score") if item.get("score") is not None else -1, reverse=True)
    return {"planteles": plantel_rows, "metrics": domain_rows}


def _metric_label(key: str) -> str:
    return {
        "general": "General",
        "attendance": "Asistencia",
        "lists": "Listas",
        "tardies": "Retardos",
        "academic": "Académico",
        "sapf": "SAPF",
    }.get(key, key)


async def get_corporate_compliance_index(
    planteles: Optional[str],
    start_date: date,
    end_date: date,
    scope: str,
    include_baselines: bool = False,
) -> Dict[str, Any]:
    del include_baselines  # Nueva lógica: no usa baselines históricas para calcular score.
    selected = _normalize_planteles(planteles)
    business_days = _business_days(start_date, end_date)
    semaphore = asyncio.Semaphore(3)

    async def collect(code: str) -> Dict[str, Any]:
        async with semaphore:
            return await _collect_plantel(code, start_date, end_date, scope, business_days)

    plantel_payloads = await asyncio.gather(*(collect(code) for code in selected))
    plantel_payloads.sort(key=lambda item: item.get("order", 999))
    aggregate = _aggregate(plantel_payloads, start_date, end_date)

    return {
        "title": "Índice Corporativo de Cumplimiento",
        "generated_at": _mx_now().isoformat(),
        "timezone": "America/Mexico_City",
        "scope": scope,
        "selected_planteles": selected,
        "plantel_order": FIXED_PLANTEL_ORDER,
        "weights": METRIC_WEIGHTS,
        "traffic_light": {
            "green": {"min": 85, "max": 100, "label": "Bien"},
            "yellow": {"min": 70, "max": 84, "label": "Atención"},
            "red": {"min": 0, "max": 69, "label": "Bajo"},
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
        "meta": {
            "logic_version": "2026-06-22-executive-0-100-v1",
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "business_days": len(business_days),
        },
    }
