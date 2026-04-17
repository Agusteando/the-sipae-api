import httpx
from datetime import date
from typing import List, Dict, Any
from core.config import settings
from core.logger import get_logger

logger = get_logger("integration.kardex")

async def fetch_kardex_schema() -> Dict[str, Any]:
    """Inspects the external Kardex schema to understand dynamically available columns."""
    url = f"{settings.kardex_api_url}/api/kardex/esquema"
    logger.info(f"Fetching Kardex Schema from {url}")
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, timeout=10.0)
            if resp.status_code == 200:
                return resp.json()
    except Exception as e:
        logger.error(f"Error fetching Kardex schema: {e}")
    return {}

async def fetch_kardex_unique_areas() -> List[str]:
    """Fetches dynamically registered unique areas in Kardex."""
    url = f"{settings.kardex_api_url}/api/kardex/valores-unicos/area"
    logger.info(f"Fetching Kardex Unique Areas from {url}")
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, timeout=10.0)
            if resp.status_code == 200:
                return resp.json()
    except Exception as e:
        logger.error(f"Error fetching Kardex unique areas: {e}")
    return []

async def fetch_kardex_records(start_date: date, end_date: date, area: str = None) -> List[Dict[str, Any]]:
    """
    Fetches raw Kardex records for the specified date range and optional area.
    Queries both `fecha_inicio`/`fecha_fin` and `start_date`/`end_date` as standard parameters.
    """
    url = f"{settings.kardex_api_url}/api/kardex"
    
    params = {
        "fecha_inicio": start_date.isoformat(),
        "fecha_fin": end_date.isoformat(),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }
    if area:
        params["area"] = area

    logger.info(f"Fetching Kardex records from {url} with params {params}")
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, params=params, timeout=20.0)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list):
                    return data
                elif isinstance(data, dict) and "data" in data:
                    return data["data"]
    except Exception as e:
        logger.error(f"Error fetching Kardex records: {e}")
    return []