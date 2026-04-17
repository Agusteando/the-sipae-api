import httpx
from typing import List, Dict
from core.config import settings
from core.logger import get_logger

logger = get_logger("integration.bot")

async def fetch_expected_population(sheets_code: str) -> int:
    """Fetches total active students per plantel from external API."""
    logger.info(f"Fetching population for sheets_code: {sheets_code}")
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                settings.external_bot_api_url, 
                json={"data": {"plantel": sheets_code}}, 
                timeout=15.0
            )
            if resp.status_code != 200: 
                return 0
            
            data = resp.json()
            if not isinstance(data, list): 
                return 0
            
            valid_students = [item for item in data if item.get('Grado') and item.get('Grupo')]
            return len(valid_students)
    except Exception as e:
        logger.error(f"Error fetching population: {e}")
        return 0


async def fetch_expected_groups(sheets_code: str) -> List[Dict[str, str]]:
    """Fetches unique expected grade-group combinations from external API."""
    logger.info(f"Fetching expected groups for sheets_code: {sheets_code}")
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                settings.external_bot_api_url, 
                json={"data": {"plantel": sheets_code}}, 
                timeout=15.0
            )
            if resp.status_code != 200:
                logger.warning(f"API responded with {resp.status_code}")
                return []
            
            data = resp.json()
            if not isinstance(data, list):
                return []
            
            unique_groups = set()
            for item in data:
                g = item.get('Grado')
                gr = item.get('Grupo')
                if g and gr:
                    unique_groups.add((str(g).strip(), str(gr).strip()))
            
            return [{"grado": g, "grupo": gr} for g, gr in sorted(list(unique_groups))]
            
    except Exception as e:
        logger.error(f"Error fetching expected groups: {e}")
        return []