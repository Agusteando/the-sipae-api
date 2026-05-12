import asyncio
import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

from core.constants import ACTIVE_PLANTEL_CODES
from core.logger import get_logger
from core.utils import resolve_plantel
from integrations.external_bot import fetch_expected_population
from .repository import (
    fetch_attendance_daily_activity,
    fetch_husky_daily_activity,
    fetch_observaciones_daily_activity,
    fetch_planeaciones_daily_activity,
    fetch_sapf_daily_activity,
)

logger = get_logger("service.baselines")

DEFAULT_COMPARISON_MONTHS = 3
DEFAULT_HISTORY_MONTHS = 9
MAX_HISTORY_MONTHS = 9
MIN_BASELINE_SAMPLES = 4
SCHOOL_YEAR_START_MONTH = 8
SCHOOL_YEAR_START_DAY = 1
MIN_ACTIVITY_BASELINE_SAMPLES = 1

METRIC_DEFINITIONS: Dict[str, Dict[str, str]] = {
    "attendance": {
        "label": "Student attendance",
        "unit": "percent",
        "value_field": "attendance_rate_percent",
        "source_date_field": "asistencia.fecha",
        "activity_value_field": "captured_attendance_records",
    },
    "husky": {
        "label": "Husky Pass scans",
        "unit": "percent",
        "value_field": "entrada_rate_percent",
        "source_date_field": "acceso.timestamp",
        "activity_value_field": "entrada_scan_records",
    },
    "sapf": {
        "label": "SAPF activity",
        "unit": "count",
        "value_field": "activity_count",
        "source_date_field": "fichas_atencion.fecha + seguimiento.fecha",
        "activity_value_field": "sapf_records",
    },
    "observaciones": {
        "label": "Observaciones activity",
        "unit": "count",
        "value_field": "submission_count",
        "source_date_field": "observaciones_form_submissions.submission_date",
        "activity_value_field": "observaciones_records",
    },
    "planeaciones": {
        "label": "Planeaciones activity",
        "unit": "count",
        "value_field": "submission_count",
        "source_date_field": "planeaciones.created_at",
        "activity_value_field": "planeaciones_records",
    },
}


@dataclass(frozen=True)
class WeekBucket:
    key: str
    label: str
    start: date
    end: date
    weekday_count: int


@dataclass
class DailyPoint:
    value: float
    raw_value: float
    denominator: float = 0.0
    quality: float = 0.0
    has_signal: bool = True


@dataclass
class AggregatePoint:
    value: float
    raw_value: float
    denominator: float
    quality: float
    quality_rate: Optional[float]
    days_with_signal: int
    days_considered: int
    has_signal: bool


def _mx_today() -> date:
    return datetime.now(ZoneInfo("America/Mexico_City")).date()


def _school_year_start(reference: date) -> date:
    """Mexico school-year activity window.

    Operational dashboards should not let years of inactivity define a plantel as normal.
    Use the current school year as the activity baseline window, starting August 1.
    """
    year = reference.year if reference.month >= SCHOOL_YEAR_START_MONTH else reference.year - 1
    return date(year, SCHOOL_YEAR_START_MONTH, SCHOOL_YEAR_START_DAY)


def _add_months(base: date, months: int) -> date:
    year = base.year + ((base.month - 1 + months) // 12)
    month = ((base.month - 1 + months) % 12) + 1
    last_day = _last_day_of_month(year, month)
    return date(year, month, min(base.day, last_day))


def _last_day_of_month(year: int, month: int) -> int:
    if month == 12:
        nxt = date(year + 1, 1, 1)
    else:
        nxt = date(year, month + 1, 1)
    return (nxt - timedelta(days=1)).day


def _date_range(start: date, end: date) -> Iterable[date]:
    cursor = start
    while cursor <= end:
        yield cursor
        cursor += timedelta(days=1)


def _date_key(value: Any) -> str:
    return str(value)[:10]


def _parse_date_key(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(_date_key(value))


def _start_of_iso_week(value: date) -> date:
    return value - timedelta(days=value.weekday())


def _format_week_label(start: date, end: date) -> str:
    return f"{start.day:02d}/{start.month:02d}–{end.day:02d}/{end.month:02d}"


def _is_weekday(value: date) -> bool:
    return value.weekday() < 5


def _create_weeks(start: date, end: date) -> List[WeekBucket]:
    weeks: List[WeekBucket] = []
    cursor = _start_of_iso_week(start)
    while cursor <= end:
        raw_end = cursor + timedelta(days=6)
        week_start = max(cursor, start)
        week_end = min(raw_end, end)
        weekday_count = sum(1 for day in _date_range(week_start, week_end) if _is_weekday(day))
        weeks.append(
            WeekBucket(
                key=week_start.isoformat(),
                label=_format_week_label(week_start, week_end),
                start=week_start,
                end=week_end,
                weekday_count=weekday_count,
            )
        )
        cursor += timedelta(days=7)
    return weeks


def _round(value: Optional[float], digits: int = 2) -> Optional[float]:
    if value is None:
        return None
    if not isinstance(value, (int, float)) or not math.isfinite(value):
        return None
    factor = 10 ** digits
    return round(value * factor) / factor


def _percentile(values: List[float], pct: float) -> Optional[float]:
    clean = sorted(v for v in values if isinstance(v, (int, float)) and math.isfinite(v))
    if not clean:
        return None
    if len(clean) == 1:
        return float(clean[0])
    rank = (len(clean) - 1) * pct
    low = math.floor(rank)
    high = math.ceil(rank)
    if low == high:
        return float(clean[low])
    weight = rank - low
    return float(clean[low] * (1 - weight) + clean[high] * weight)


def _std_dev(values: List[float], mean: float) -> float:
    if len(values) < 2:
        return 0.0
    variance = sum((value - mean) ** 2 for value in values) / (len(values) - 1)
    return math.sqrt(variance)


def _stats(values: List[float]) -> Dict[str, Any]:
    clean = [float(v) for v in values if isinstance(v, (int, float)) and math.isfinite(v)]
    if not clean:
        return {
            "samples": 0,
            "mean": None,
            "median": None,
            "std_dev": None,
            "min": None,
            "max": None,
            "p10": None,
            "p20": None,
            "p80": None,
            "p90": None,
        }

    mean = sum(clean) / len(clean)
    return {
        "samples": len(clean),
        "mean": _round(mean),
        "median": _round(_percentile(clean, 0.5)),
        "std_dev": _round(_std_dev(clean, mean)),
        "min": _round(min(clean)),
        "max": _round(max(clean)),
        "p10": _round(_percentile(clean, 0.10)),
        "p20": _round(_percentile(clean, 0.20)),
        "p80": _round(_percentile(clean, 0.80)),
        "p90": _round(_percentile(clean, 0.90)),
    }


def _score_against_baseline(actual: float, stats: Dict[str, Any]) -> Dict[str, Any]:
    samples = int(stats.get("samples") or 0)
    expected = stats.get("expected", stats.get("median"))
    warning_floor = stats.get("warning_floor", stats.get("p20"))
    critical_floor = stats.get("critical_floor", stats.get("p10"))
    max_value = stats.get("max")

    if samples < MIN_BASELINE_SAMPLES or expected is None or warning_floor is None or critical_floor is None:
        return {
            "score": None,
            "status": "unavailable",
            "severity": None,
            "ratio_to_expected": None,
            "delta_to_expected": None,
            "reason": "insufficient_historical_samples",
        }

    expected = float(expected)
    warning_floor = float(warning_floor)
    critical_floor = float(critical_floor)
    max_value = float(max_value or 0)

    if max_value <= 0:
        if actual > 0:
            return {
                "score": 100.0,
                "status": "healthy",
                "severity": 0.0,
                "ratio_to_expected": None,
                "delta_to_expected": _round(actual),
                "reason": "new_activity_above_empty_history",
            }
        return {
            "score": None,
            "status": "unavailable",
            "severity": None,
            "ratio_to_expected": None,
            "delta_to_expected": 0.0,
            "reason": "no_historical_activity",
        }

    if actual < critical_floor:
        status = "critical"
    elif actual < warning_floor:
        status = "warning"
    else:
        status = "healthy"

    if expected <= 0:
        score = 100.0 if actual > 0 else 0.0
    elif actual >= expected:
        score = 100.0
    elif expected == warning_floor:
        score = max(0.0, min(80.0, 80.0 * (actual / expected)))
    elif actual >= warning_floor:
        score = 80.0 + (20.0 * ((actual - warning_floor) / (expected - warning_floor)))
    elif warning_floor == critical_floor:
        score = max(0.0, min(55.0, 55.0 * (actual / critical_floor))) if critical_floor > 0 else 0.0
    elif actual >= critical_floor:
        score = 55.0 + (25.0 * ((actual - critical_floor) / (warning_floor - critical_floor)))
    elif critical_floor > 0:
        score = max(0.0, min(55.0, 55.0 * (actual / critical_floor)))
    else:
        score = 0.0

    delta = actual - expected
    ratio = actual / expected if expected > 0 else None
    severity = 0.0
    if status == "warning":
        denom = warning_floor - critical_floor
        severity = 0.35 if denom <= 0 else 0.35 + (0.3 * ((warning_floor - actual) / denom))
    elif status == "critical":
        denom = critical_floor if critical_floor > 0 else max(expected, 1.0)
        severity = min(1.0, 0.65 + (0.35 * ((critical_floor - actual) / denom)))

    return {
        "score": _round(score, 1),
        "status": status,
        "severity": _round(severity, 3),
        "ratio_to_expected": _round(ratio, 3),
        "delta_to_expected": _round(delta),
        "reason": "historical_percentile_comparison",
    }


def _baseline_block(samples: List[float]) -> Dict[str, Any]:
    stats = _stats(samples)
    return {
        **stats,
        "expected": stats.get("median"),
        "warning_floor": stats.get("p20"),
        "critical_floor": stats.get("p10"),
        "basis": "plantel_metric_history",
        "minimum_samples": MIN_BASELINE_SAMPLES,
    }


def _activity_max_baseline(
    samples: List[float],
    scope: str,
    baseline_start: Optional[date] = None,
    baseline_end: Optional[date] = None,
) -> Dict[str, Any]:
    """Activity standard for one plantel/metric based on useful activity volume.

    The baseline is plantel-local and metric-local: the maximum useful activity
    observed during the current Mexico school year. This prevents a plantel with
    years of no activity from looking normal while avoiding cross-plantel standards
    that unfairly punish smaller planteles.
    """
    clean = [float(v) for v in samples if isinstance(v, (int, float)) and math.isfinite(v)]
    positive = [v for v in clean if v > 0]
    max_value = max(positive) if positive else 0.0
    return {
        "samples": len(clean),
        "positive_samples": len(positive),
        "max": _round(max_value),
        "expected": _round(max_value),
        "warning_floor": None,
        "critical_floor": None,
        "basis": "plantel_metric_current_school_year_max_useful_activity",
        "scope": scope,
        "unit": "activity_units",
        "minimum_samples": MIN_ACTIVITY_BASELINE_SAMPLES,
        "baseline_start": baseline_start.isoformat() if baseline_start else None,
        "baseline_end": baseline_end.isoformat() if baseline_end else None,
        "school_year_start_month": SCHOOL_YEAR_START_MONTH,
        "score_model": "zero_activity_0_positive_activity_1_100_against_school_year_max",
    }


def _score_activity_against_max(actual: Optional[float], baseline: Dict[str, Any]) -> Dict[str, Any]:
    if actual is None or not isinstance(actual, (int, float)) or not math.isfinite(actual):
        return {
            "score": None,
            "status": "unavailable",
            "severity": None,
            "ratio_to_expected": None,
            "delta_to_expected": None,
            "reason": "activity_value_unavailable",
        }

    maximum = float(baseline.get("max") or baseline.get("expected") or 0)
    actual_value = max(0.0, float(actual))
    if maximum <= 0:
        score = 100.0 if actual_value > 0 else 0.0
        return {
            "score": _round(score, 1),
            "status": "healthy" if actual_value > 0 else "critical",
            "severity": 0.0 if actual_value > 0 else 1.0,
            "ratio_to_expected": None,
            "delta_to_expected": _round(actual_value),
            "reason": "no_school_year_activity_max",
        }

    score = max(0.0, min(100.0, (actual_value / maximum) * 100.0))
    if actual_value > 0 and score < 1.0:
        score = 1.0
    if score <= 0:
        status = "critical"
    elif score < 50:
        status = "critical"
    elif score < 80:
        status = "warning"
    else:
        status = "healthy"

    severity = 0.0
    if status == "warning":
        severity = (80.0 - score) / 30.0 * 0.35
    elif status == "critical":
        severity = 0.55 + ((50.0 - min(score, 50.0)) / 50.0 * 0.45)

    return {
        "score": _round(score, 1),
        "status": status,
        "severity": _round(severity, 3),
        "ratio_to_expected": _round(actual_value / maximum, 3),
        "delta_to_expected": _round(actual_value - maximum),
        "reason": "useful_activity_vs_plantel_metric_school_year_max",
    }



def _activity_week_value(metric_key: str, aggregate: AggregatePoint, expected_population: int, weekday_count: int) -> Optional[float]:
    # Activity is only record existence/count. No population normalization.
    if metric_key == "attendance":
        return _round(aggregate.denominator)
    return _round(aggregate.raw_value)


def _activity_day_value(metric_key: str, aggregate: AggregatePoint, expected_population: int) -> Optional[float]:
    # Activity is only record existence/count. No population normalization.
    if metric_key == "attendance":
        return _round(aggregate.denominator)
    return _round(aggregate.raw_value)


def _activity_unit(metric_key: str, expected_population: int) -> str:
    return "records"


def _attach_activity_payload(point: Dict[str, Any], actual_value: Optional[float], baseline: Dict[str, Any], unit: str) -> None:
    comparison = _score_activity_against_max(actual_value, baseline)
    point["activity"] = {
        "actual": actual_value,
        "unit": unit,
        "baseline": baseline,
        "expected": baseline.get("expected"),
        "historical_max": baseline.get("max"),
        "score": comparison["score"],
        "status": comparison["status"],
        "severity": comparison["severity"],
        "ratio_to_expected": comparison["ratio_to_expected"],
        "delta_to_expected": comparison["delta_to_expected"],
        "reason": comparison["reason"],
        "comparison_model": "plantel_metric_current_school_year_max_useful_activity",
        "score_range": "0 for no activity, 1-100 for positive activity",
    }



def _aggregate_week(daily: Dict[date, DailyPoint], week: WeekBucket, kind: str) -> AggregatePoint:
    values: List[float] = []
    raw_value = 0.0
    denominator = 0.0
    quality = 0.0
    days_considered = 0
    days_with_signal = 0

    for day in _date_range(week.start, week.end):
        point = daily.get(day)
        if kind == "count":
            days_considered += 1
            raw_value += point.raw_value if point else 0.0
            quality += point.quality if point else 0.0
            if point and point.has_signal:
                days_with_signal += 1
        else:
            if not point or not point.has_signal:
                continue
            values.append(point.value)
            raw_value += point.raw_value
            denominator += point.denominator
            quality += point.quality
            days_considered += 1
            days_with_signal += 1

    if kind == "count":
        value = raw_value
        has_signal = True
    else:
        value = sum(values) / len(values) if values else 0.0
        has_signal = len(values) > 0

    quality_rate = (quality / raw_value * 100) if raw_value > 0 else None
    return AggregatePoint(
        value=value,
        raw_value=raw_value,
        denominator=denominator,
        quality=quality,
        quality_rate=quality_rate,
        days_with_signal=days_with_signal,
        days_considered=days_considered,
        has_signal=has_signal,
    )


def _aggregate_today(daily: Dict[date, DailyPoint], target: date, kind: str) -> AggregatePoint:
    point = daily.get(target)
    if kind == "count":
        raw_value = point.raw_value if point else 0.0
        quality = point.quality if point else 0.0
        return AggregatePoint(
            value=raw_value,
            raw_value=raw_value,
            denominator=0.0,
            quality=quality,
            quality_rate=(quality / raw_value * 100) if raw_value > 0 else None,
            days_with_signal=1 if point and point.has_signal else 0,
            days_considered=1,
            has_signal=True,
        )

    if not point or not point.has_signal:
        return AggregatePoint(0.0, 0.0, 0.0, 0.0, None, 0, 1, False)
    return AggregatePoint(
        value=point.value,
        raw_value=point.raw_value,
        denominator=point.denominator,
        quality=point.quality,
        quality_rate=(point.quality / point.raw_value * 100) if point.raw_value > 0 else None,
        days_with_signal=1,
        days_considered=1,
        has_signal=True,
    )


def _point_payload(
    actual: AggregatePoint,
    baseline: Dict[str, Any],
    unit: str,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    comparison = _score_against_baseline(actual.value, baseline)
    expected = baseline.get("expected")
    payload = {
        "actual": _round(actual.value),
        "raw_value": _round(actual.raw_value),
        "denominator": _round(actual.denominator),
        "quality_count": _round(actual.quality),
        "quality_rate": _round(actual.quality_rate),
        "days_with_signal": actual.days_with_signal,
        "days_considered": actual.days_considered,
        "baseline": baseline,
        "expected": expected,
        "score": comparison["score"],
        "status": comparison["status"],
        "severity": comparison["severity"],
        "ratio_to_expected": comparison["ratio_to_expected"],
        "delta_to_expected": comparison["delta_to_expected"],
        "reason": comparison["reason"],
        "unit": unit,
    }
    if extra:
        payload.update(extra)
    return payload


def _history_daily_samples(
    history_daily: Dict[date, DailyPoint],
    target_weekday: int,
    kind: str,
    history_start: date,
    history_end: date,
) -> List[float]:
    samples: List[float] = []
    for day in _date_range(history_start, history_end):
        if day.weekday() != target_weekday:
            continue
        point = history_daily.get(day)
        if kind == "count":
            samples.append(point.raw_value if point else 0.0)
        elif point and point.has_signal:
            samples.append(point.value)
    return samples


def _week_samples(
    history_daily: Dict[date, DailyPoint],
    history_weeks: List[WeekBucket],
    kind: str,
    target_weekday_count: Optional[int] = None,
) -> List[float]:
    samples: List[float] = []
    for week in history_weeks:
        point = _aggregate_week(history_daily, week, kind)
        if kind == "count":
            value = point.value
            if target_weekday_count is not None and week.weekday_count > 0:
                value = value * (target_weekday_count / week.weekday_count)
            samples.append(value)
        elif point.has_signal:
            samples.append(point.value)
    return samples


def _merge_daily_points(*sources: Dict[date, DailyPoint]) -> Dict[date, DailyPoint]:
    merged: Dict[date, DailyPoint] = {}
    for source in sources:
        for day, point in source.items():
            merged[day] = point
    return merged


def _activity_week_samples(
    metric_key: str,
    activity_daily: Dict[date, DailyPoint],
    activity_weeks: List[WeekBucket],
    kind: str,
    expected_population: int,
    target_weekday_count: Optional[int] = None,
) -> List[float]:
    samples: List[float] = []
    for week in activity_weeks:
        point = _aggregate_week(activity_daily, week, kind)
        value = _activity_week_value(metric_key, point, expected_population, week.weekday_count)
        if value is None:
            continue
        if target_weekday_count is not None and week.weekday_count > 0:
            value = float(value) * (target_weekday_count / week.weekday_count)
        samples.append(float(value))
    return samples


def _activity_today_samples(
    metric_key: str,
    activity_daily: Dict[date, DailyPoint],
    kind: str,
    expected_population: int,
    target_weekday: int,
    activity_start: date,
    activity_end: date,
) -> List[float]:
    samples: List[float] = []
    for day in _date_range(activity_start, activity_end):
        if day.weekday() != target_weekday:
            continue
        point = _aggregate_today(activity_daily, day, kind)
        value = _activity_day_value(metric_key, point, expected_population)
        if value is not None:
            samples.append(float(value))
    return samples


def _activity_status_from_score(score: Optional[float]) -> str:
    if score is None:
        return "unavailable"
    if score <= 0:
        return "critical"
    if score < 50:
        return "critical"
    if score < 80:
        return "warning"
    return "healthy"


def _activity_severity_from_score(score: Optional[float]) -> Optional[float]:
    if score is None:
        return None
    if score >= 80:
        return 0.0
    if score >= 50:
        return _round((80.0 - score) / 30.0 * 0.35, 3)
    return _round(0.55 + ((50.0 - max(0.0, score)) / 50.0 * 0.45), 3)


def _build_metric_payload(
    metric_key: str,
    kind: str,
    history_daily: Dict[date, DailyPoint],
    current_daily: Dict[date, DailyPoint],
    comparison_weeks: List[WeekBucket],
    history_weeks: List[WeekBucket],
    history_start: date,
    history_end: date,
    today: date,
    expected_population: int = 0,
    activity_start: Optional[date] = None,
    activity_end: Optional[date] = None,
) -> Dict[str, Any]:
    definition = METRIC_DEFINITIONS[metric_key]
    activity_start = activity_start or _school_year_start(today)
    activity_end = activity_end or max(today - timedelta(days=1), activity_start)
    activity_daily = {
        day: point
        for day, point in _merge_daily_points(history_daily, current_daily).items()
        if activity_start <= day <= activity_end
    }
    activity_weeks = _create_weeks(activity_start, activity_end) if activity_start <= activity_end else []

    weekly_samples = _week_samples(history_daily, history_weeks, kind)
    weekly_baseline = _baseline_block(weekly_samples)

    quality_weekly_samples = [
        point.quality_rate
        for point in (_aggregate_week(history_daily, week, kind) for week in history_weeks)
        if point.quality_rate is not None
    ]
    quality_weekly_baseline = _baseline_block(quality_weekly_samples) if quality_weekly_samples else None

    today_samples = _history_daily_samples(history_daily, today.weekday(), kind, history_start, history_end)
    today_baseline = _baseline_block(today_samples)

    quality_today_samples: List[float] = []
    for day in _date_range(history_start, history_end):
        if day.weekday() != today.weekday():
            continue
        point = history_daily.get(day)
        if point and point.raw_value > 0:
            quality_today_samples.append(point.quality / point.raw_value * 100)
    quality_today_baseline = _baseline_block(quality_today_samples) if quality_today_samples else None

    weeks_payload = []
    for week in comparison_weeks:
        actual = _aggregate_week(current_daily, week, kind)
        point_baseline = weekly_baseline
        if kind == "count":
            point_baseline = _baseline_block(_week_samples(history_daily, history_weeks, kind, week.weekday_count))
        weeks_payload.append(
            _point_payload(
                actual,
                point_baseline,
                definition["unit"],
                {
                    "key": week.key,
                    "label": week.label,
                    "start": week.start.isoformat(),
                    "end": week.end.isoformat(),
                    "weekday_count": week.weekday_count,
                },
            )
        )

    today_actual = _aggregate_today(current_daily, today, kind)
    today_payload = _point_payload(
        today_actual,
        today_baseline,
        definition["unit"],
        {
            "date": today.isoformat(),
            "weekday": today.strftime("%A"),
            "comparison_pattern": "same_weekday_history",
        },
    )

    activity_history_weekly = _activity_week_samples(
        metric_key,
        activity_daily,
        activity_weeks,
        kind,
        expected_population,
    )

    today_activity_end = min(activity_end, today - timedelta(days=1))
    activity_history_today = _activity_today_samples(
        metric_key,
        activity_daily,
        kind,
        expected_population,
        today.weekday(),
        activity_start,
        today_activity_end,
    ) if activity_start <= today_activity_end else []

    for week, point in zip(comparison_weeks, weeks_payload):
        activity_value = _activity_week_value(
            metric_key,
            _aggregate_week(current_daily, week, kind),
            expected_population,
            week.weekday_count,
        )
        point["activity"] = {
            "actual": activity_value,
            "unit": _activity_unit(metric_key, expected_population),
            "comparison_model": "plantel_metric_current_school_year_max_useful_activity",
        }

    today_payload["activity"] = {
        "actual": _activity_day_value(metric_key, today_actual, expected_population),
        "unit": _activity_unit(metric_key, expected_population),
        "comparison_model": "plantel_metric_current_school_year_max_useful_activity",
    }

    available_week_scores = [point["score"] for point in weeks_payload if point.get("score") is not None]
    latest_available = next((point for point in reversed(weeks_payload) if point.get("score") is not None), None)
    current_score = today_payload.get("score") if today_payload.get("score") is not None else (latest_available or {}).get("score")
    current_status = today_payload.get("status") if today_payload.get("status") != "unavailable" else (latest_available or {}).get("status", "unavailable")

    payload = {
        "key": metric_key,
        "label": definition["label"],
        "unit": definition["unit"],
        "direction": "higher_is_better",
        "value_field": definition["value_field"],
        "source_date_field": definition["source_date_field"],
        "comparison_model": "historical_percentile_bands",
        "activity_model": "plantel_metric_current_school_year_max_useful_activity",
        "activity_unit": _activity_unit(metric_key, expected_population),
        "activity_window": {
            "start": activity_start.isoformat(),
            "end": activity_end.isoformat(),
            "basis": "current_mexico_school_year",
        },
        "weekly_baseline": weekly_baseline,
        "today_baseline": today_baseline,
        "weekly": weeks_payload,
        "today": today_payload,
        "score": _round(current_score, 1),
        "status": current_status,
        "average_weekly_score": _round(sum(available_week_scores) / len(available_week_scores), 1) if available_week_scores else None,
        "_activity_history_weekly": activity_history_weekly,
        "_activity_history_today": activity_history_today,
    }
    if quality_weekly_baseline:
        payload["quality_rate_baseline"] = quality_weekly_baseline
    if quality_today_baseline:
        payload["today_quality_rate_baseline"] = quality_today_baseline
    return payload


def _attendance_daily(rows: List[Dict]) -> Dict[date, DailyPoint]:
    daily: Dict[date, DailyPoint] = {}
    for row in rows:
        day = _parse_date_key(row.get("date_val"))
        total = float(row.get("total_students") or 0)
        asistencia = float(row.get("asistencia") or 0)
        rate = (asistencia / total * 100) if total > 0 else 0.0
        daily[day] = DailyPoint(
            value=rate,
            raw_value=asistencia,
            denominator=total,
            has_signal=total > 0,
        )
    return daily


def _husky_daily(rows: List[Dict], expected_population: int) -> Dict[date, DailyPoint]:
    grouped: Dict[date, Dict[str, float]] = {}
    for row in rows:
        day = _parse_date_key(row.get("date_val"))
        tipo = str(row.get("tipo_accion") or "").strip().lower()
        if tipo not in {"entrada", "salida"}:
            continue
        if day not in grouped:
            grouped[day] = {"entrada": 0.0, "salida": 0.0}
        grouped[day][tipo] += float(row.get("total_scans") or 0)

    daily: Dict[date, DailyPoint] = {}
    for day, values in grouped.items():
        entrada = values.get("entrada", 0.0)
        rate = (entrada / expected_population * 100) if expected_population > 0 else 0.0
        daily[day] = DailyPoint(
            value=rate,
            raw_value=entrada,
            denominator=float(expected_population),
            quality=values.get("salida", 0.0),
            has_signal=expected_population > 0 or entrada > 0,
        )
    return daily


def _count_daily(rows: List[Dict], value_key: str = "conteo", quality_key: Optional[str] = None) -> Dict[date, DailyPoint]:
    daily: Dict[date, DailyPoint] = {}
    for row in rows:
        day = _parse_date_key(row.get("date_val"))
        total = float(row.get(value_key) or 0)
        quality = float(row.get(quality_key) or 0) if quality_key else 0.0
        current = daily.get(day)
        if current:
            current.raw_value += total
            current.value += total
            current.quality += quality
            current.has_signal = current.has_signal or total > 0
        else:
            daily[day] = DailyPoint(
                value=total,
                raw_value=total,
                quality=quality,
                has_signal=total > 0,
            )
    return daily


async def _safe_expected_population(sheets_code: str) -> int:
    try:
        return await fetch_expected_population(sheets_code)
    except Exception as exc:  # Defensive only; fetch_expected_population already catches.
        logger.warning("No se pudo obtener población esperada para %s: %s", sheets_code, exc)
        return 0


async def _build_plantel_baselines(
    plantel_code: str,
    comparison_start: date,
    comparison_end: date,
    history_start: date,
    history_end: date,
    today: date,
    activity_start: date,
    activity_end: date,
) -> Dict[str, Any]:
    plantel_info = resolve_plantel(plantel_code)
    comparison_weeks = _create_weeks(comparison_start, comparison_end)
    history_weeks = _create_weeks(history_start, history_end)

    expected_population = await _safe_expected_population(plantel_info["sheets_code"])

    (
        attendance_history_rows,
        attendance_current_rows,
        husky_history_rows,
        husky_current_rows,
        sapf_history_rows,
        sapf_current_rows,
        obs_history_rows,
        obs_current_rows,
        plan_history_rows,
        plan_current_rows,
    ) = await asyncio.gather(
        fetch_attendance_daily_activity(plantel_info["db_code"], history_start, history_end),
        fetch_attendance_daily_activity(plantel_info["db_code"], comparison_start, comparison_end),
        fetch_husky_daily_activity(plantel_info["husky_db_codes"], history_start, history_end),
        fetch_husky_daily_activity(plantel_info["husky_db_codes"], comparison_start, comparison_end),
        fetch_sapf_daily_activity(plantel_info["sapf_data_campuses"], history_start, history_end),
        fetch_sapf_daily_activity(plantel_info["sapf_data_campuses"], comparison_start, comparison_end),
        fetch_observaciones_daily_activity(plantel_info["academic_filters"], history_start, history_end),
        fetch_observaciones_daily_activity(plantel_info["academic_filters"], comparison_start, comparison_end),
        fetch_planeaciones_daily_activity(plantel_info["academic_filters"], history_start, history_end),
        fetch_planeaciones_daily_activity(plantel_info["academic_filters"], comparison_start, comparison_end),
    )

    metrics = {
        "attendance": _build_metric_payload(
            "attendance",
            "rate",
            _attendance_daily(attendance_history_rows),
            _attendance_daily(attendance_current_rows),
            comparison_weeks,
            history_weeks,
            history_start,
            history_end,
            today,
            expected_population,
            activity_start=activity_start,
            activity_end=activity_end,
        ),
        "husky": _build_metric_payload(
            "husky",
            "rate",
            _husky_daily(husky_history_rows, expected_population),
            _husky_daily(husky_current_rows, expected_population),
            comparison_weeks,
            history_weeks,
            history_start,
            history_end,
            today,
            expected_population,
            activity_start=activity_start,
            activity_end=activity_end,
        ),
        "sapf": _build_metric_payload(
            "sapf",
            "count",
            _count_daily(sapf_history_rows),
            _count_daily(sapf_current_rows),
            comparison_weeks,
            history_weeks,
            history_start,
            history_end,
            today,
            expected_population,
            activity_start=activity_start,
            activity_end=activity_end,
        ),
        "observaciones": _build_metric_payload(
            "observaciones",
            "count",
            _count_daily(obs_history_rows, "total", "quality"),
            _count_daily(obs_current_rows, "total", "quality"),
            comparison_weeks,
            history_weeks,
            history_start,
            history_end,
            today,
            expected_population,
            activity_start=activity_start,
            activity_end=activity_end,
        ),
        "planeaciones": _build_metric_payload(
            "planeaciones",
            "count",
            _count_daily(plan_history_rows, "total", "quality"),
            _count_daily(plan_current_rows, "total", "quality"),
            comparison_weeks,
            history_weeks,
            history_start,
            history_end,
            today,
            expected_population,
            activity_start=activity_start,
            activity_end=activity_end,
        ),
    }

    available_scores = [metric.get("score") for metric in metrics.values() if metric.get("score") is not None]
    overall_score = sum(available_scores) / len(available_scores) if available_scores else None
    metric_statuses = [metric.get("status") for metric in metrics.values()]
    if "critical" in metric_statuses:
        overall_status = "critical"
    elif "warning" in metric_statuses:
        overall_status = "warning"
    elif available_scores:
        overall_status = "healthy"
    else:
        overall_status = "unavailable"

    return {
        "code": plantel_info["plantel_code"],
        "canonical_code": plantel_info["canonical_code"],
        "requested_code": plantel_info["plantel_requested"],
        "resolved_name": plantel_info["resolved_name"],
        "identifiers": {
            "db_code": plantel_info["db_code"],
            "sheets_code": plantel_info["sheets_code"],
            "sapf_data_campuses": plantel_info["sapf_data_campuses"],
            "academic_filters": plantel_info["academic_filters"],
        },
        "expected_population": expected_population,
        "score": _round(overall_score, 1),
        "status": overall_status,
        "metrics": metrics,
    }


async def _map_with_limit(items: List[str], limit: int, worker):
    semaphore = asyncio.Semaphore(max(1, limit))

    async def run(item: str):
        async with semaphore:
            return await worker(item)

    return await asyncio.gather(*(run(item) for item in items))


def _apply_cross_plantel_activity_standards(plantel_payloads: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Attach the activity sub-metric without mixing it with performance.

    Despite the historical function name, the current model is plantel-local:
    each metric's activity score is useful activity volume / that plantel metric's
    current-school-year maximum. Period activity is kept separate from today activity.
    """
    standards: Dict[str, Any] = {}

    for plantel in plantel_payloads:
        plantel_code = str(plantel.get("code") or plantel.get("requested_code") or "")
        standards[plantel_code] = {}
        metrics = plantel.get("metrics") or {}

        for metric_key, metric in metrics.items():
            activity_window = metric.get("activity_window") or {}
            baseline_start = date.fromisoformat(activity_window["start"]) if activity_window.get("start") else None
            baseline_end = date.fromisoformat(activity_window["end"]) if activity_window.get("end") else None
            weekly_baseline = _activity_max_baseline(metric.get("_activity_history_weekly") or [], "weekly", baseline_start, baseline_end)
            today_baseline = _activity_max_baseline(metric.get("_activity_history_today") or [], "today_same_weekday", baseline_start, baseline_end)
            unit = "records"

            standards[plantel_code][metric_key] = {
                "weekly": weekly_baseline,
                "today": today_baseline,
            }

            for point in metric.get("weekly", []):
                actual = (point.get("activity") or {}).get("actual")
                _attach_activity_payload(point, actual, weekly_baseline, unit)

            today = metric.get("today")
            if today:
                actual = (today.get("activity") or {}).get("actual")
                _attach_activity_payload(today, actual, today_baseline, unit)

            weekly_activity_scores = [
                (point.get("activity") or {}).get("score")
                for point in metric.get("weekly", [])
                if (point.get("activity") or {}).get("score") is not None
            ]
            activity_today = (today or {}).get("activity") or {}
            period_activity_score = _round(sum(weekly_activity_scores) / len(weekly_activity_scores), 1) if weekly_activity_scores else None
            period_activity_status = _activity_status_from_score(period_activity_score)
            metric["activity"] = {
                "score": period_activity_score,
                "status": period_activity_status,
                "severity": _activity_severity_from_score(period_activity_score),
                "period_score": period_activity_score,
                "period_status": period_activity_status,
                "period_severity": _activity_severity_from_score(period_activity_score),
                "today_score": activity_today.get("score"),
                "today_status": activity_today.get("status"),
                "average_weekly_score": period_activity_score,
                "weekly_standard": weekly_baseline,
                "today_standard": today_baseline,
                "unit": unit,
                "comparison_model": "plantel_metric_current_school_year_max_useful_activity",
                "score_range": "0 for no activity, 1-100 for positive activity",
                "role": "activity_sub_metric",
                "period_basis": "average of weekly activity levels in selected period",
                "today_basis": "same-weekday activity level for today",
            }
            metric.pop("_activity_history_weekly", None)
            metric.pop("_activity_history_today", None)

        activity_scores = [
            metric.get("activity", {}).get("period_score")
            for metric in metrics.values()
            if metric.get("activity", {}).get("period_score") is not None
        ]
        today_scores = [
            metric.get("activity", {}).get("today_score")
            for metric in metrics.values()
            if metric.get("activity", {}).get("today_score") is not None
        ]
        period_activity_score = _round(sum(activity_scores) / len(activity_scores), 1) if activity_scores else None
        today_activity_score = _round(sum(today_scores) / len(today_scores), 1) if today_scores else None
        activity_status = _activity_status_from_score(period_activity_score)
        today_activity_status = _activity_status_from_score(today_activity_score)
        plantel["activity"] = {
            "score": period_activity_score,
            "status": activity_status,
            "period_score": period_activity_score,
            "period_status": activity_status,
            "period_severity": _activity_severity_from_score(period_activity_score),
            "today_score": today_activity_score,
            "today_status": today_activity_status,
            "today_severity": _activity_severity_from_score(today_activity_score),
            "role": "record_activity_sub_metric",
            "comparison_model": "plantel_metric_current_school_year_max_useful_activity",
            "score_range": "0 for no activity, 1-100 for positive activity",
            "period_basis": "average of metric period activity levels",
            "today_basis": "average of metric today activity levels",
        }

    return standards



def normalize_plantel_list(planteles: Optional[str]) -> List[str]:
    if not planteles:
        return list(ACTIVE_PLANTEL_CODES)
    seen = set()
    out: List[str] = []
    for item in str(planteles).split(","):
        code = item.strip().upper()
        if not code or code in seen:
            continue
        seen.add(code)
        out.append(code)
    return out or list(ACTIVE_PLANTEL_CODES)


async def get_global_baseline_report(
    planteles: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    comparison_months: int = DEFAULT_COMPARISON_MONTHS,
    history_months: int = DEFAULT_HISTORY_MONTHS,
) -> Dict[str, Any]:
    today = _mx_today()
    comparison_end = end_date or today
    safe_comparison_months = min(6, max(1, int(comparison_months or DEFAULT_COMPARISON_MONTHS)))
    comparison_start = start_date or _add_months(comparison_end, -safe_comparison_months)
    safe_history_months = min(MAX_HISTORY_MONTHS, max(1, int(history_months or DEFAULT_HISTORY_MONTHS)))
    history_end = comparison_start - timedelta(days=1)
    history_start = _add_months(history_end, -safe_history_months)
    activity_start = _school_year_start(comparison_end)
    activity_end = comparison_end

    plantel_codes = normalize_plantel_list(planteles)
    comparison_weeks = _create_weeks(comparison_start, comparison_end)
    history_weeks = _create_weeks(history_start, history_end) if history_start <= history_end else []

    async def build_one(code: str):
        try:
            return await _build_plantel_baselines(code, comparison_start, comparison_end, history_start, history_end, today, activity_start, activity_end)
        except Exception as exc:
            logger.error("Fallo al generar baseline para %s: %s", code, exc)
            info = resolve_plantel(code)
            return {
                "code": info["plantel_code"],
                "canonical_code": info["canonical_code"],
                "requested_code": info["plantel_requested"],
                "resolved_name": info["resolved_name"],
                "score": None,
                "status": "unavailable",
                "error": str(exc),
                "metrics": {},
            }

    plantel_payloads = await _map_with_limit(plantel_codes, 2, build_one)
    activity_standards = _apply_cross_plantel_activity_standards(plantel_payloads)

    return {
        "generated_at": datetime.now(ZoneInfo("America/Mexico_City")).isoformat(),
        "aggregation": "weekly",
        "comparison_window": {
            "start": comparison_start.isoformat(),
            "end": comparison_end.isoformat(),
            "months": safe_comparison_months,
            "weeks": [
                {
                    "key": week.key,
                    "label": week.label,
                    "start": week.start.isoformat(),
                    "end": week.end.isoformat(),
                    "weekday_count": week.weekday_count,
                }
                for week in comparison_weeks
            ],
        },
        "history_window": {
            "start": history_start.isoformat(),
            "end": history_end.isoformat(),
            "months": safe_history_months,
            "max_months": MAX_HISTORY_MONTHS,
            "weeks": len(history_weeks),
        },
        "activity_window": {
            "start": activity_start.isoformat(),
            "end": activity_end.isoformat(),
            "basis": "current_mexico_school_year",
            "school_year_start_month": SCHOOL_YEAR_START_MONTH,
        },
        "today": today.isoformat(),
        "status_model": {
            "basis": "historical_percentile_bands_per_plantel_metric",
            "direction": "higher_is_better",
            "healthy": "actual >= historical p20",
            "warning": "historical p10 <= actual < historical p20",
            "critical": "actual < historical p10",
            "unavailable": "insufficient baseline samples or no historical activity",
            "minimum_samples": MIN_BASELINE_SAMPLES,
            "fixed_performance_thresholds": False,
            "metric_performance_is_preserved": True,
            "activity_sub_metric": {
                "basis": "useful activity volume scored 0-100 against each plantel metric current-school-year maximum",
                "purpose": "keeps activity separate from metric performance; period activity and today activity are reported separately",
                "role": "sub_metric_per_metric_not_replacement",
                "baseline_window": "Mexico school year starting August 1",
                "normalization": {
                    "attendance": "captured attendance rows / school-year max captured attendance rows",
                    "husky": "entrada scan activity / school-year max entrada scan activity",
                    "sapf": "SAPF useful activity / school-year max SAPF useful activity",
                    "observaciones": "observaciones useful activity / school-year max observaciones useful activity",
                    "planeaciones": "planeaciones useful activity / school-year max planeaciones useful activity"
                }
            },
        },
        "activity_standards": activity_standards,
        "metrics": METRIC_DEFINITIONS,
        "planteles": plantel_payloads,
    }
