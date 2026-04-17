from fastapi import APIRouter, Query, HTTPException, Depends
from core.dependencies import DateRangeParams
from .service import calculate_husky_daily_rate
from .schemas import HuskyDailyRateResponse

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