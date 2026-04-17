from datetime import date
from core.utils import resolve_plantel
from integrations.external_bot import fetch_expected_population
from .repository import get_daily_scans, fetch_student_retardos_by_matricula

async def calculate_husky_daily_rate(plantel: str, start_date: date, end_date: date, scope: str) -> dict:
    """
    Orchestrates integration and repository data to compute daily metrics.
    Strictly applies the requested scope constraint globally.
    """
    plantel_info = resolve_plantel(plantel)
    total_population = await fetch_expected_population(plantel_info["sheets_code"])
    
    results = await get_daily_scans(plantel_info["db_code"], start_date, end_date)

    daily_data = {}
    for row in results:
        f_date = str(row['fecha'])
        if f_date not in daily_data:
            daily_data[f_date] = {"entrada": 0, "salida": 0, "rate_entrada_percent": 0.0}
        
        tipo = row['tipo_accion'].lower()
        if tipo in ['entrada', 'salida']:
            daily_data[f_date][tipo] = row['total_scans']
            if tipo == 'entrada' and total_population > 0:
                rate = (row['total_scans'] / total_population) * 100
                daily_data[f_date]["rate_entrada_percent"] = round(rate, 2)

    return {
        "plantel_requested": plantel_info["plantel_requested"],
        "resolved_name": plantel_info["resolved_name"],
        "expected_population": total_population,
        "scope": scope,
        "date_range": {"start": start_date, "end": end_date},
        "daily_datapoints": daily_data
    }


async def get_student_retardos(matricula: str, start_date: date, end_date: date, scope: str) -> dict:
    """
    Extracts all tardies for the student precisely within the provided scoped date range.
    No hardcoded date logic exists here; the global contract determines the boundaries.
    """
    records = await fetch_student_retardos_by_matricula(matricula, start_date, end_date)
    
    formatted_retardos = []
    for r in records:
        formatted_retardos.append({
            "id": r["id"],
            "student_fullname": r["student_fullname"],
            "date": r["date"],
            "time": str(r["time"]) 
        })
        
    return {
        "matricula": matricula,
        "scope": scope,
        "date_range": {"start": start_date, "end": end_date},
        "total_retardos": len(formatted_retardos),
        "retardos": formatted_retardos
    }