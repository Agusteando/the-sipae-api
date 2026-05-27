from typing import Any, Dict, List, Optional

from core.constants import ACTIVE_PLANTEL_CODES


def get_plantel_acronym(row: Dict[str, Any]) -> str:
    label = str(row.get("label") or "").strip().upper()
    dir_name = str(row.get("dir") or "").strip().lower()

    if label == "CT":
        return "PREET"
    if label == "CM":
        return "PREEM"
    if label in {"PMA", "PMB"}:
        return "PM"

    if "preescolar" in dir_name and "metepec" in dir_name:
        return "PREEM"
    if "preescolar" in dir_name and "toluca" in dir_name:
        return "PREET"
    if "primaria" in dir_name and "metepec" in dir_name:
        return "PM"
    if "primaria" in dir_name and "toluca" in dir_name:
        return "PT"
    if "secundaria" in dir_name and "metepec" in dir_name:
        return "SM"
    if "secundaria" in dir_name and "toluca" in dir_name:
        return "ST"

    return label


def normalize_email(value: Optional[str]) -> Optional[str]:
    email = str(value or "").strip().lower()
    return email or None


def build_principal_records(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    seen = set()

    for row in rows:
        plantel_code = get_plantel_acronym(row)
        principal_email = normalize_email(row.get("email"))
        if plantel_code not in ACTIVE_PLANTEL_CODES or not principal_email:
            continue

        manager_email = normalize_email(row.get("manager_email"))
        key = (plantel_code, principal_email)
        if key in seen:
            continue
        seen.add(key)

        cc_emails = []
        if manager_email and manager_email != principal_email:
            cc_emails.append(manager_email)

        records.append({
            "dir_id": row.get("id"),
            "plantel_code": plantel_code,
            "plantel_label": row.get("label"),
            "plantel_name": row.get("dir") or row.get("label") or plantel_code,
            "principal_email": principal_email,
            "manager_email": manager_email,
            "cc_emails": cc_emails,
            "coord_id": row.get("coord"),
            "coord_name": row.get("coord_name"),
        })

    return sorted(records, key=lambda item: (item["plantel_code"], item["principal_email"]))
