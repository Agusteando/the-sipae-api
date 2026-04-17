from fastapi import APIRouter, Query, Path, HTTPException, Depends
from core.dependencies import DateRangeParams
from .service import calculate_husky_daily_rate, get_student_retardos
from .schemas import HuskyDailyRateResponse, StudentRetardosResponse

router = APIRouter(prefix="/api/v1/husky", tags=["Husky Pass"])

@router.get("/scans/daily-rate", response_model=HuskyDailyRateResponse)
async def get_husky_daily_rate(
    plantel: str = Query(..., description="Código de Sede (Ej: PT, SM)"),
    date_params: DateRangeParams = Depends()
):
    """
    Recupera el promedio diario de entradas procesadas por los escáneres Husky.
    """
    try:
        return await calculate_husky_daily_rate(
            plantel=plantel, 
            start_date=date_params.start_date, 
            end_date=date_params.end_date
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error de base de datos: {str(e)}")

@router.get("/students/{matricula}/retardos", response_model=StudentRetardosResponse)
async def fetch_student_retardos_endpoint(
    matricula: str = Path(..., description="Matrícula oficial del alumno a inspeccionar")
):
    """
    Obtiene el historial de retardos del alumno en el ciclo escolar actual, calculando
    las incidencias basado en la tolerancia horaria y su nivel educativo asociado.
    """
    try:
        return await get_student_retardos(matricula=matricula)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error de base de datos: {str(e)}")