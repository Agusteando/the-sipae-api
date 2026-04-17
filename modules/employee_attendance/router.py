from fastapi import APIRouter, Query, HTTPException, Depends
from core.dependencies import DateScopeParams
from .service import get_kardex_attendance_report
from .schemas import EmployeeAttendanceResponse

router = APIRouter(prefix="/api/v1/employee-attendance", tags=["Employee Kardex"])

@router.get("/kardex-report", response_model=EmployeeAttendanceResponse)
async def get_kardex_report(
    plantel: str = Query(..., description="Código de Sede (Ej: PT, SM) para mapeo a Kardex"),
    scope_params: DateScopeParams = Depends()
):
    """
    Recupera el conteo y listado detallado de retardos y ausencias de empleados
    consumiendo el servicio externo de Kardex. 
    Por defecto asume únicamente el día en curso (TODAY).
    """
    try:
        return await get_kardex_attendance_report(
            plantel=plantel,
            start_date=scope_params.start_date,
            end_date=scope_params.end_date,
            scope=scope_params.scope
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al integrar con servicio Kardex: {str(e)}")