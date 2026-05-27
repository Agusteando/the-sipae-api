import asyncio
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import httpx

from core.config import settings
from core.logger import get_logger

logger = get_logger("integration.bot")

_CACHE_TTL = timedelta(minutes=30)
_STALE_TTL = timedelta(hours=6)
_CODE_ALIASES = {
    "CT": "PREET",
    "CM": "PREEM",
    "PMA": "PM",
    "PMB": "PM",
}
_BASE_CACHE: Dict[str, Dict[str, Any]] = {}
_PENDING: Dict[str, asyncio.Task] = {}
_LOCK = asyncio.Lock()


def _now() -> datetime:
    return datetime.now(ZoneInfo("America/Mexico_City"))


def _normalize_sheets_code(sheets_code: str) -> str:
    code = str(sheets_code or "").strip().upper()
    return _CODE_ALIASES.get(code, code)


def _fresh_entry(code: str) -> Optional[List[Dict[str, Any]]]:
    entry = _BASE_CACHE.get(code)
    if not entry:
        return None
    if _now() - entry["timestamp"] <= _CACHE_TTL:
        return entry["data"]
    return None


def _stale_entry(code: str) -> Optional[List[Dict[str, Any]]]:
    entry = _BASE_CACHE.get(code)
    if not entry:
        return None
    if _now() - entry["timestamp"] <= _STALE_TTL:
        return entry["data"]
    return None


async def _fetch_base_simple_uncached(code: str) -> List[Dict[str, Any]]:
    if not settings.external_bot_api_url:
        logger.warning("External Bot API URL is not configured; returning empty population for %s", code)
        return []

    logger.info("Fetching base-simple for sheets_code: %s", code)
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                settings.external_bot_api_url,
                json={"data": {"plantel": code}},
                timeout=10.0,
            )

        if resp.status_code != 200:
            detail = (resp.text or "").strip().replace("\n", " ")[:240]
            logger.warning("Base-simple returned %s for %s: %s", resp.status_code, code, detail)
            return []

        data = resp.json()
        if not isinstance(data, list):
            logger.warning("Base-simple returned non-list payload for %s", code)
            return []

        return data
    except Exception as exc:
        logger.warning("Error fetching base-simple for %s: %s", code, exc)
        return []


async def _fetch_base_simple(code: str) -> List[Dict[str, Any]]:
    normalized = _normalize_sheets_code(code)
    cached = _fresh_entry(normalized)
    if cached is not None:
        return cached

    async with _LOCK:
        cached = _fresh_entry(normalized)
        if cached is not None:
            return cached
        pending = _PENDING.get(normalized)
        if pending is None:
            pending = asyncio.create_task(_fetch_base_simple_uncached(normalized))
            _PENDING[normalized] = pending

    try:
        data = await pending
    finally:
        async with _LOCK:
            if _PENDING.get(normalized) is pending:
                _PENDING.pop(normalized, None)

    if data:
        _BASE_CACHE[normalized] = {"timestamp": _now(), "data": data}
        return data

    stale = _stale_entry(normalized)
    if stale is not None:
        logger.warning("Serving stale base-simple cache for %s after upstream failure", normalized)
        return stale

    return []


async def fetch_expected_population(sheets_code: str) -> int:
    """Fetch total active students per plantel from the external Bot API.

    The external Bot API uses formal preescolar labels (PREET/PREEM), not the
    legacy CT/CM storage codes. Calls are cached and coalesced to avoid repeated
    POST bursts when the global dashboard warms multiple metrics at once.
    """
    code = _normalize_sheets_code(sheets_code)
    data = await _fetch_base_simple(code)
    valid_students = [item for item in data if item.get("Grado") and item.get("Grupo")]
    return len(valid_students)


async def fetch_expected_groups(sheets_code: str) -> List[Dict[str, Any]]:
    """Fetch expected grade-group combinations with active student counts.

    Attendance gap insights need the number of students affected when a group
    does not submit attendance. The upstream base-simple payload is student-level,
    so the count can be derived without another external dependency.
    """
    code = _normalize_sheets_code(sheets_code)
    data = await _fetch_base_simple(code)

    group_counts: Dict[Tuple[str, str], int] = {}
    for item in data:
        grade = item.get("Grado")
        group = item.get("Grupo")
        if grade and group:
            key = (str(grade).strip(), str(group).strip())
            group_counts[key] = group_counts.get(key, 0) + 1

    return [
        {"grado": grade, "grupo": group, "expected_students": count}
        for (grade, group), count in sorted(group_counts.items())
    ]
