from datetime import datetime
from typing import Any, Dict, Optional

# ==========================================
# GESTOR DE CACHÉ EN MEMORIA
# ==========================================
# Almacena el resultado de las operaciones pesadas de agregación.
# Permite servir respuestas instantáneas (<15ms) a los paneles de control.

_GLOBAL_CACHE: Dict[str, Dict[str, Any]] = {}

def set_cache(key: str, data: Any) -> None:
    """Guarda un conjunto de datos en caché con su marca de tiempo exacta."""
    _GLOBAL_CACHE[key] = {
        "timestamp": datetime.now(),
        "data": data
    }

def get_cache(key: str) -> Optional[Dict[str, Any]]:
    """Recupera los datos en caché si existen, de lo contrario devuelve None."""
    return _GLOBAL_CACHE.get(key)