from fastapi import APIRouter, Query, Path, HTTPException, Depends
from core.dependencies import DateScopeParams
from .service import calculate_husky_daily_rate, get_student_retardos
from .schemas import HuskyDailyRateResponse, StudentRetardosResponse

router = APIRouter(prefix="/api/v1/husky", tags=["Husky Pass"])

@router.get("/scans/daily-rate", response_model=HuskyDailyRateResponse)
async def get_husky_daily_rate(
    plantel: str = Query(..., description="Código de Sede (Ej: PT, SM)"),
    scope_params: DateScopeParams = Depends()
):
    """
    Recupera el promedio de entradas por escáner. 
    Por defecto devuelve información únicamente del día de hoy.
    """
    try:
        return await calculate_husky_daily_rate(
            plantel=plantel, 
            start_date=scope_params.start_date, 
            end_date=scope_params.end_date,
            scope=scope_params.scope
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error de base de datos: {str(e)}")

@router.get("/students/{matricula}/retardos", response_model=StudentRetardosResponse)
async def fetch_student_retardos_endpoint(
    matricula: str = Path(..., description="Matrícula oficial del alumno a inspeccionar"),
    scope_params: DateScopeParams = Depends()
):
    """
    Obtiene el historial de retardos. Por defecto devuelve solo los del día de hoy.
    Para el análisis escolar completo, pasar ?scope=ciclo_escolar.
    """
    try:
        return await get_student_retardos(
            matricula=matricula,
            start_date=scope_params.start_date,
            end_date=scope_params.end_date,
            scope=scope_params.scope
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error de base de datos: {str(e)}")