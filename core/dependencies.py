from fastapi import Query
from typing import Optional
from datetime import date
import calendar

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
            # Prevención de fallos ante alcances inválidos
            self.scope = "today"
            self.start_date = today_dt
            self.end_date = today_dt