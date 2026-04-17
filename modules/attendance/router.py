from fastapi import APIRouter, Query, HTTPException, Depends
from core.dependencies import DateRangeParams
from .service import get_attendance_detail_report
from .schemas import AttendanceDetailResponse

router = APIRouter(prefix="/api/v1/attendance", tags=["Attendance"])

@router.get("/detail", response_model=AttendanceDetailResponse)
async def get_attendance_detail(
    plantel: str = Query(..., description="Código de Sede (Ej: PT, SM)"),
    date_params: DateRangeParams = Depends()
):
    """
    Punto de enlace central para evaluar la cobertura de inasistencias por grupo y alumno.
    Soporta formato diario y rango cronológico.
    """
    try:
        return await get_attendance_detail_report(
            plantel=plantel,
            start_date=date_params.start_date,
            end_date=date_params.end_date
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en la ejecución de base de datos: {str(e)}")