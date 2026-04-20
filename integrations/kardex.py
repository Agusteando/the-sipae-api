import httpx
import urllib.parse
from datetime import date
from typing import List, Dict, Any
from core.config import settings
from core.logger import get_logger

logger = get_logger("integration.kardex")

async def fetch_kardex_schema() -> Dict[str, Any]:
    url = f"{settings.kardex_api_url}/api/kardex/esquema"
    logger.info(f"Solicitando Esquema Kardex en {url}")
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, timeout=10.0)
            if resp.status_code == 200:
                return resp.json()
            else:
                logger.warning(f"Respuesta inesperada al obtener esquema. HTTP {resp.status_code}: {resp.text}")
    except Exception as e:
        logger.error(f"Error en comunicación con API Kardex (esquema): {e}")
    return {}

async def fetch_kardex_unique_areas() -> List[str]:
    url = f"{settings.kardex_api_url}/api/kardex/valores-unicos/area"
    logger.info(f"Solicitando Áreas Únicas Kardex en {url}")
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, timeout=10.0)
            if resp.status_code == 200:
                return resp.json()
            else:
                logger.warning(f"Respuesta inesperada al obtener áreas. HTTP {resp.status_code}: {resp.text}")
    except Exception as e:
        logger.error(f"Error en comunicación con API Kardex (áreas únicas): {e}")
    return []

async def fetch_kardex_records(start_date: date, end_date: date, area: str = None) -> List[Dict[str, Any]]:
    url = f"{settings.kardex_api_url}/api/kardex"
    params = {
        "fecha_inicio": start_date.isoformat(),
        "fecha_fin": end_date.isoformat(),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }
    if area:
        params["area"] = area

    logger.info(f"Solicitando registros crudos a {url} con parámetros: {params}")
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, params=params, timeout=20.0)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list):
                    return data
                elif isinstance(data, dict) and "data" in data:
                    return data["data"]
                else:
                    logger.warning(f"La respuesta de registros tiene formato desconocido: {type(data)}")
            else:
                logger.warning(f"Fallo al obtener registros Kardex. HTTP {resp.status_code}: {resp.text}")
    except Exception as e:
        logger.error(f"Error en comunicación con API Kardex (registros crudos): {e}")
    return []

async def fetch_crossover_records(start_date: date, end_date: date, plantel: str) -> Dict[str, Any]:
    # Codificamos la variable plantel para evitar errores HTTP 400 si contiene espacios o guiones (ej. "4 - PM")
    safe_plantel = urllib.parse.quote(plantel)
    url = f"{settings.kardex_api_url}/api/crossover/plantel/{safe_plantel}"
    
    params = {
        "fecha_inicio": start_date.isoformat(),
        "fecha_fin": end_date.isoformat()
    }
    
    logger.info(f"Solicitando Crossover API a {url} con parámetros: {params}")
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, params=params, timeout=25.0)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, dict):
                    return data
            else:
                logger.warning(f"Endpoint Crossover no disponible o falló para el área {plantel}. HTTP {resp.status_code}: {resp.text}")
    except Exception as e:
        logger.error(f"Error en comunicación con Crossover API ({plantel}): {e}")
    return {}