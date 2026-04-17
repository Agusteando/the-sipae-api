from datetime import date
from core.utils import resolve_plantel
from core.logger import get_logger
from integrations.external_bot import fetch_expected_population
from .repository import get_daily_scans, fetch_plantel_retardos

logger = get_logger("service.husky")

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


async def get_plantel_retardos(plantel: str, start_date: date, end_date: date, scope: str) -> dict:
    """
    Extracts all tardies globally for the requested plantel within the provided date range.
    Applies the exact tardiness threshold driven by the resolved normalized plantel code.
    """
    plantel_info = resolve_plantel(plantel)
    db_code = plantel_info["db_code"]
    
    # Apply strict rules for threshold mappings based on normalized plantel
    if db_code in ['PM', 'PT']:
        threshold_time = '08:01:00'
    elif db_code in ['SM', 'ST']:
        threshold_time = '07:01:00'
    else:
        threshold_time = '09:01:00'

    logger.info(f"Requested Plantel: {plantel}")
    logger.info(f"Normalized Plantel (db_code): {db_code}")
    logger.info(f"Tardiness Threshold Applied: > {threshold_time}")
    logger.info(f"SQL Parameters: db_code={db_code}%, start_date={start_date}, end_date={end_date}, threshold_time={threshold_time}")

    records = await fetch_plantel_retardos(db_code, start_date, end_date, threshold_time)
    
    logger.info(f"Total rows returned after threshold filtering: {len(records)}")
    
    formatted_retardos = []
    for r in records:
        formatted_retardos.append({
            "id": r["id"],
            "student_fullname": str(r["student_fullname"]).strip() if r["student_fullname"] else "Desconocido",
            "matricula": r.get("matricula") or "N/A",
            "date": r["date"],
            "time": str(r["time"]) 
        })
        
    return {
        "plantel_requested": plantel_info["plantel_requested"],
        "resolved_name": plantel_info["resolved_name"],
        "scope": scope,
        "date_range": {"start": start_date, "end": end_date},
        "total_retardos": len(formatted_retardos),
        "retardos": formatted_retardos
    }