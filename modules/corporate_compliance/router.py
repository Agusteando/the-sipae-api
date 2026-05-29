from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, Query
from fastapi.responses import HTMLResponse

from core.dependencies import DateScopeParams
from .service import get_corporate_compliance_index
from .templates import CORPORATE_COMPLIANCE_HTML

router = APIRouter(tags=["Corporate Compliance Dashboard"])


@router.get("/corporate-compliance-risk-index", response_class=HTMLResponse, include_in_schema=False)
async def serve_corporate_compliance_dashboard():
    return HTMLResponse(CORPORATE_COMPLIANCE_HTML)


@router.get("/indice-corporativo-cumplimiento", response_class=HTMLResponse, include_in_schema=False)
async def serve_indice_corporativo_cumplimiento():
    return HTMLResponse(CORPORATE_COMPLIANCE_HTML)


@router.get("/api/v1/corporate-compliance-risk-index")
async def get_corporate_compliance_dashboard_data(
    planteles: Optional[str] = Query(None, description="Lista separada por comas en orden fijo: PT,PM,ST,SM,PREET,PREEM"),
    include_baselines: bool = Query(True, description="Incluye comparación histórica disponible en /api/v1/baselines."),
    scope_params: DateScopeParams = Depends(),
):
    return await get_corporate_compliance_index(
        planteles=planteles,
        start_date=scope_params.start_date,
        end_date=scope_params.end_date,
        scope=scope_params.scope,
        include_baselines=include_baselines,
    )
