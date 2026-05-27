import json
import os
from typing import Any, Dict, List

from core.config import ROOT_DIR, settings

SCHEDULE_CONFIG_PATH = os.path.join(ROOT_DIR, ".health_reports_schedule.json")
VALID_DAYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]


def _default_days() -> List[str]:
    return ["mon", "tue", "wed", "thu", "fri"]


def normalize_schedule(payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
    payload = payload or {}
    enabled = bool(payload.get("enabled", settings.health_reports_enabled))
    hour = int(payload.get("hour", settings.health_reports_send_hour))
    minute = int(payload.get("minute", settings.health_reports_send_minute))
    timezone = str(payload.get("timezone", settings.health_reports_timezone or "America/Mexico_City")).strip() or "America/Mexico_City"
    days_raw = payload.get("days") or _default_days()
    if isinstance(days_raw, str):
        days_raw = [item.strip().lower() for item in days_raw.replace(";", ",").split(",") if item.strip()]
    days = [str(day).strip().lower()[:3] for day in days_raw if str(day).strip().lower()[:3] in VALID_DAYS]
    if not days:
        days = _default_days()
    return {
        "enabled": enabled,
        "hour": max(0, min(23, hour)),
        "minute": max(0, min(59, minute)),
        "days": days,
        "timezone": timezone,
    }


def get_schedule_config() -> Dict[str, Any]:
    if os.path.exists(SCHEDULE_CONFIG_PATH):
        try:
            with open(SCHEDULE_CONFIG_PATH, "r", encoding="utf-8") as fh:
                return normalize_schedule(json.load(fh))
        except Exception:
            pass
    return normalize_schedule()


def save_schedule_config(payload: Dict[str, Any]) -> Dict[str, Any]:
    config = normalize_schedule(payload)
    with open(SCHEDULE_CONFIG_PATH, "w", encoding="utf-8") as fh:
        json.dump(config, fh, ensure_ascii=False, indent=2)
    return config


def cron_day_of_week(days: List[str]) -> str:
    ordered = [day for day in VALID_DAYS if day in days]
    if ordered == ["mon", "tue", "wed", "thu", "fri"]:
        return "mon-fri"
    return ",".join(ordered or _default_days())
