from fastapi import Query
from typing import Optional
from datetime import date
import calendar

class DateScopeParams:
    """
    Global Shared Dependency enforcing the platform-wide Date Contract.
    Rule: All endpoints default to returning data for TODAY only.
    Other scopes (range, month, ciclo_escolar) are ONLY activated when explicitly requested.
    """
    def __init__(
        self,
        scope: Optional[str] = Query(None, description="Alcance de los datos: today, range, month, ciclo_escolar"),
        start_date: Optional[date] = Query(None, description="Fecha de inicio (solo si scope=range)"),
        end_date: Optional[date] = Query(None, description="Fecha de fin (solo si scope=range)")
    ):
        # Zero-regression fallback: If legacy clients send explicit dates without a scope, infer 'range'
        if not scope and (start_date or end_date):
            self.scope = "range"
        else:
            self.scope = (scope or "today").lower()

        today_dt = date.today()

        if self.scope == "today":
            self.start_date = today_dt
            self.end_date = today_dt
            
        elif self.scope == "range":
            self.start_date = start_date or today_dt
            self.end_date = end_date or today_dt
            
        elif self.scope == "month":
            self.start_date = today_dt.replace(day=1)
            last_day = calendar.monthrange(today_dt.year, today_dt.month)[1]
            self.end_date = today_dt.replace(day=last_day)
            
        elif self.scope == "ciclo_escolar":
            if today_dt.month < 8:
                start_year = today_dt.year - 1
            else:
                start_year = today_dt.year
            self.start_date = date(start_year, 8, 1)
            self.end_date = date(start_year + 1, 8, 1)
            
        else:
            # Fallback for invalid scope inputs to guarantee safety
            self.scope = "today"
            self.start_date = today_dt
            self.end_date = today_dt