from fastapi import APIRouter
from fastapi.responses import HTMLResponse
from .templates import TEST_HUB_HTML

router = APIRouter(tags=["UI Dashboards"])

@router.get("/test-hub", response_class=HTMLResponse, include_in_schema=False)
async def serve_test_hub():
    """
    Serves the exclusive internal Testing Dashboard GUI.
    """
    return TEST_HUB_HTML