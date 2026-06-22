from __future__ import annotations

import math
from typing import Any, Iterable, Mapping, Optional

STATUS_GREEN = "fulfilled"
STATUS_YELLOW = "warning"
STATUS_RED = "critical"
STATUS_EMPTY = "unavailable"

TRAFFIC_LABELS = {
    STATUS_GREEN: "Bien",
    STATUS_YELLOW: "Atención",
    STATUS_RED: "Bajo",
    STATUS_EMPTY: "Sin cálculo",
}

TRAFFIC_COLORS = {
    STATUS_GREEN: "green",
    STATUS_YELLOW: "yellow",
    STATUS_RED: "red",
    STATUS_EMPTY: "gray",
}

METRIC_WEIGHTS = {
    "roll_call": 20,
    "student_attendance": 15,
    "scans": 10,
    "student_punctuality": 10,
    "staff_attendance": 10,
    "planning": 15,
    "observations": 15,
    "sapf": 5,
}


def safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        parsed = float(value)
        if not math.isfinite(parsed):
            return None
        return parsed
    except Exception:
        return None


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default


def clamp_score(value: Any) -> Optional[float]:
    """Executive score: calculable values are constrained to 1-100.

    `None` means no real denominator/source. It must remain unavailable and never
    become 0. Values at or below 0 are only possible when the denominator was real
    and the result was completely failed; those display as 1 by policy.
    """
    parsed = safe_float(value)
    if parsed is None:
        return None
    return round(max(1.0, min(100.0, parsed)), 1)


def pct(numerator: Any, denominator: Any) -> Optional[float]:
    den = safe_float(denominator)
    num = safe_float(numerator)
    if den is None or num is None or den <= 0:
        return None
    return clamp_score((num / den) * 100.0)


def bounded_inverse_rate(events: Any, opportunities: Any) -> Optional[float]:
    den = safe_float(opportunities)
    ev = safe_float(events)
    if den is None or ev is None or den <= 0:
        return None
    return clamp_score(100.0 - ((max(ev, 0.0) / den) * 100.0))


def status_for_score(score: Optional[float]) -> str:
    if score is None:
        return STATUS_EMPTY
    if score >= 85:
        return STATUS_GREEN
    if score >= 70:
        return STATUS_YELLOW
    return STATUS_RED


def traffic_for_score(score: Optional[float]) -> dict[str, str]:
    status = status_for_score(score)
    return {
        "status": status,
        "label": TRAFFIC_LABELS[status],
        "color": TRAFFIC_COLORS[status],
    }


def weighted_score(scores: Mapping[str, Optional[float]], weights: Mapping[str, int] | None = None) -> Optional[float]:
    weights = weights or METRIC_WEIGHTS
    numerator = 0.0
    denominator = 0.0
    for key, weight in weights.items():
        score = clamp_score(scores.get(key))
        if score is None:
            continue
        numerator += score * float(weight)
        denominator += float(weight)
    if denominator <= 0:
        return None
    return clamp_score(numerator / denominator)


def average_score(values: Iterable[Optional[float]]) -> Optional[float]:
    usable = []
    for value in values:
        score = clamp_score(value)
        if score is not None:
            usable.append(score)
    if not usable:
        return None
    return clamp_score(sum(usable) / len(usable))
