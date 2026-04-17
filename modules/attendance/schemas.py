from pydantic import BaseModel
from typing import Dict, List, Any

# Dynamic schemas omitted to preserve exact dictionary output structure natively.
# Defining a strict top-level dictionary guarantees no regressions for nested unstructured endpoints.
class AttendanceDetailResponse(BaseModel):
    plantel_requested: str
    resolved_name: str
    mode: str
    date_range: Dict[str, Any]
    
    # Extra fields allowed due to dynamic 'daily' vs 'range' structure variations
    model_config = {
        "extra": "allow"
    }