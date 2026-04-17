from datetime import date
from core.utils import resolve_plantel
from integrations.external_bot import fetch_expected_population
from .repository import get_daily_scans, fetch_student_retardos_by_matricula

async def calculate_husky_daily_rate(plantel: str, start_date: date, end_date: date) -> dict:
    """
    Orchestrates integration and repository data to compute daily metrics.
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
        "date_range": {"start": start_date, "end": end_date},
        "daily_datapoints": daily_data
    }


async def get_student_retardos(matricula: str) -> dict:
    """
    Calculates the current active school year properly, and extracts all tardies.
    """
    today = date.today()
    
    # Define school year logical mapping safely
    if today.month < 8:
        start_year = today.year - 1
    else:
        start_year = today.year
        
    school_year_start = date(start_year, 8, 1)
    school_year_end = date(start_year + 1, 8, 1)
    
    records = await fetch_student_retardos_by_matricula(matricula, school_year_start, school_year_end)
    
    # aiomysql natively maps TIME types to datetime.timedelta which we convert to robust strings
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
        "school_year": f"{start_year}-{start_year + 1}",
        "total_retardos": len(formatted_retardos),
        "retardos": formatted_retardos
    }