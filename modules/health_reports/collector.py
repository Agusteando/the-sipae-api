from datetime import date, timedelta
from typing import Any, Awaitable, Callable, Dict, Optional

from modules.attendance.service import get_attendance_detail_report
from modules.husky.service import calculate_husky_daily_rate, get_plantel_retardos
from modules.employee_attendance.service import get_kardex_attendance_report
from modules.academic.service import get_observaciones_docentes_report, get_planeaciones_pendientes_report


async def _safe_call(name: str, fn: Callable[[], Awaitable[Dict[str, Any]]]) -> Dict[str, Any]:
    try:
        return await fn()
    except Exception as exc:
        return {"error": str(exc), "source": name}


def week_start_for(target_date: date) -> date:
    return target_date - timedelta(days=target_date.weekday())


async def collect_plantel_health(plantel: str, report_date: date) -> Dict[str, Any]:
    week_start = week_start_for(report_date)
    return {
        "attendance": await _safe_call(
            "attendance",
            lambda: get_attendance_detail_report(plantel, report_date, report_date, "today"),
        ),
        "husky": await _safe_call(
            "husky",
            lambda: calculate_husky_daily_rate(plantel, report_date, report_date, "today"),
        ),
        "retardos": await _safe_call(
            "retardos",
            lambda: get_plantel_retardos(plantel, report_date, report_date, "today"),
        ),
        "kardex": await _safe_call(
            "kardex",
            lambda: get_kardex_attendance_report(plantel, report_date, report_date, "today"),
        ),
        "observaciones": await _safe_call(
            "observaciones",
            lambda: get_observaciones_docentes_report(plantel),
        ),
        "planeaciones": await _safe_call(
            "planeaciones",
            lambda: get_planeaciones_pendientes_report(plantel, week_start, report_date, "range"),
        ),
    }
