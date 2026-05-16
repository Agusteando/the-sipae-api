from fastapi import Query
from typing import Optional
from datetime import date, datetime
from zoneinfo import ZoneInfo
import calendar

def get_current_school_year_range(reference_date: Optional[date] = None) -> tuple[date, date]:
    """
    Returns the current ciclo escolar boundaries using the same August-based
    convention already used by the API's date scopes.
    """
    if reference_date is None:
        tz_mx = ZoneInfo("America/Mexico_City")
        reference_date = datetime.now(tz_mx).date()

    start_year = reference_date.year - 1 if reference_date.month < 8 else reference_date.year
    return date(start_year, 8, 1), date(start_year + 1, 8, 1)


def get_school_year_label(reference_date: Optional[date] = None) -> str:
    start_date, end_date = get_current_school_year_range(reference_date)
    return f"{start_date.year}-{end_date.year}"


class DateScopeParams:
    """
    Dependencia global que estandariza los parámetros de alcance de fechas y caché.
    Todos los endpoints asumen 'today' por defecto para maximizar el rendimiento.
    """
    def __init__(
        self,
        scope: Optional[str] = Query(None, description="Alcance de los datos: today, range, month, ciclo_escolar"),
        start_date: Optional[date] = Query(None, description="Fecha de inicio (solo si scope=range)"),
        end_date: Optional[date] = Query(None, description="Fecha de fin (solo si scope=range)"),
        force_refresh: bool = Query(False, description="Omitir el almacenamiento en caché y forzar una actualización síncrona.")
    ):
        # Compatibilidad hacia atrás: Si el cliente envía fechas explícitas sin alcance, inferir 'range'
        if not scope and (start_date or end_date):
            self.scope = "range"
        else:
            self.scope = (scope or "today").lower()

        self.force_refresh = force_refresh
        
        # Resolución de fecha explícita en zona horaria local para evitar problemas de UTC
        tz_mx = ZoneInfo("America/Mexico_City")
        today_dt = datetime.now(tz_mx).date()

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
            self.start_date, self.end_date = get_current_school_year_range(today_dt)
            
        else:
            # Prevención de fallos ante alcances inválidos
            self.scope = "today"
            self.start_date = today_dt
            self.end_date = today_dt
