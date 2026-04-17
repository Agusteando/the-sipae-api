from typing import Dict
from core.constants import PLANTEL_MAP

def resolve_plantel(plantel_code: str) -> Dict[str, str]:
    """
    Centralized utility to resolve UI/Internal codes into Database and Integration values.
    """
    plantel_code_upper = plantel_code.upper()
    mapping = PLANTEL_MAP.get(plantel_code_upper)
    
    return {
        "plantel_requested": plantel_code,
        "db_code": mapping["db_code"] if mapping else plantel_code_upper,
        "sheets_code": mapping["sheets_code"] if mapping else plantel_code_upper,
        "resolved_name": mapping["name"] if mapping else "Unknown"
    }