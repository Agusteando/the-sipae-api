from pydantic import BaseModel
from typing import Dict, Any, Optional

class AttendanceDetailResponse(BaseModel):
    plantel_requested: str
    resolved_name: str
    mode: str
    date_range: Dict[str, Any]
    meta: Optional[Dict[str, Any]] = None
    
    model_config = {
        "extra": "allow"
    }