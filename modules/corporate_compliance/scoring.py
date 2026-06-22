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
    STATUS_EMPTY: "Sin datos",
}

TRAFFIC_COLORS = {
    STATUS_GREEN: "green",
    STATUS_YELLOW: "yellow",
    STATUS_RED: "red",
    STATUS_EMPTY: "gray",
}

METRIC_WEIGHTS = {
    "attendance": 30,
    "lists": 25,
    "tardies": 15,
    "academic": 20,
    "sapf": 10,
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
    parsed = safe_float(value)
    if parsed is None:
        return None
    return round(max(0.0, min(100.0, parsed)), 1)


def pct(numerator: Any, denominator: Any) -> Optional[float]:
    den = safe_float(denominator)
    num = safe_float(numerator)
    if den is None or num is None or den <= 0:
        return None
    return clamp_score((num / den) * 100.0)


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
    parsed = [clamp_score(v) for v in values]
    usable = [v for v in parsed if v is not None]
    if not usable:
        return None
    return clamp_score(sum(usable) / len(usable))


def score_from_penalty(rate: Optional[float], multiplier: float) -> Optional[float]:
    parsed = safe_float(rate)
    if parsed is None:
        return None
    return clamp_score(100.0 - (parsed * multiplier))
