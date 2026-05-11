from copy import deepcopy
from typing import Any, Dict, List
from core.constants import PLANTEL_MAP


def _normalize_code(plantel_code: str) -> str:
    return str(plantel_code or "").strip().upper()


def _dedupe(values: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for value in values:
        value = str(value).strip()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def resolve_plantel(plantel_code: str) -> Dict[str, Any]:
    """
    Centralized utility to resolve UI/internal codes into database, spreadsheet,
    SAPF and academic identifiers.

    The legacy contract is preserved: callers can still read db_code,
    sheets_code and resolved_name. Newer modules can use db_codes,
    sapf_data_campuses and academic_filters when a subsystem uses a different
    encoding.
    """
    requested = str(plantel_code or "").strip()
    code = _normalize_code(requested)
    mapping = deepcopy(PLANTEL_MAP.get(code) or {})

    canonical_code = mapping.get("alias_of", code)
    db_code = mapping.get("db_code", canonical_code)
    sheets_code = mapping.get("sheets_code", db_code)

    db_codes = _dedupe(mapping.get("db_codes") or [db_code])
    sheets_codes = _dedupe(mapping.get("sheets_codes") or [sheets_code])
    sapf_data_campuses = _dedupe(mapping.get("sapf_data_campuses") or [db_code])

    return {
        "plantel_requested": requested,
        "plantel_code": code,
        "canonical_code": canonical_code,
        "db_code": db_code,
        "db_codes": db_codes,
        "sheets_code": sheets_code,
        "sheets_codes": sheets_codes,
        "resolved_name": mapping.get("display_name") or mapping.get("name") or "Unknown",
        "short_name": mapping.get("name") or mapping.get("display_name") or "Unknown",
        "academic_filters": mapping.get("academic_filters", []),
        "sapf_map_campus": mapping.get("sapf_map_campus", db_code),
        "sapf_data_campuses": sapf_data_campuses,
    }
