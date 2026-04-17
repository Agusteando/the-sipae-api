from fastapi import Query
from typing import Optional
from datetime import date

class DateRangeParams:
    """
    Shared FastAPI dependency to strictly manage and fallback date ranges.
    Eliminates duplicated date-checking logic across endpoints.
    """
    def __init__(
        self,
        start_date: Optional[date] = Query(None, description="Fecha de inicio"),
        end_date: Optional[date] = Query(None, description="Fecha de fin")
    ):
        self.start_date = start_date or date.today()
        self.end_date = end_date or date.today()