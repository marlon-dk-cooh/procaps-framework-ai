"""
Hora en zona America/Bogota (Colombia), como datetime naive local.

Usar `current_colombian_time()` / `current_colombian_time_str()` para timestamps
de pipeline (StepLogger, manifiestos, reportes)
"""

from datetime import datetime
from zoneinfo import ZoneInfo

_COLOMBIA_TZ = ZoneInfo("America/Bogota")


def current_colombian_time_str() -> str:
    """Cadena legible: YYYY-MM-DD HH:MM:SS en hora de Bogotá (sin offset)."""
    return (
        datetime.now(_COLOMBIA_TZ)
        .replace(tzinfo=None)
        .strftime("%Y-%m-%d %H:%M:%S")
    )


def current_colombian_time() -> datetime:
    """Datetime naive con la hora civil actual en Bogotá (misma convención que time_str)."""
    return datetime.now(_COLOMBIA_TZ).replace(tzinfo=None)
