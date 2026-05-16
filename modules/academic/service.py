from datetime import date, timedelta
from zoneinfo import ZoneInfo

from datetime import datetime
from core.dependencies import get_current_school_year_range, get_school_year_label
from core.utils import resolve_plantel
from core.logger import get_logger
from .repository import (
    get_observaciones_stats, get_observaciones_comments,
    get_planeaciones_stats, get_planeaciones_comments,
    get_observaciones_teacher_status, get_least_observed_teacher,
    count_recent_active_planeacion_docentes, get_pending_review_planeaciones,
)

logger = get_logger("service.academic")


async def get_observaciones_report(plantel: str, start_date: date, end_date: date, scope: str) -> dict:
    plantel_info = resolve_plantel(plantel)
    academic_filters = plantel_info["academic_filters"]

    logger.info(
        "Procesando Observaciones para %s (%s -> %s). academic_filters=%s",
        plantel_info["resolved_name"],
        start_date,
        end_date,
        academic_filters,
    )

    stats_results = await get_observaciones_stats(academic_filters, start_date, end_date)
    comments_results = await get_observaciones_comments(academic_filters, start_date, end_date)

    total_obs = 0
    obs_with_comment = 0
    daily_trend = []

    for row in stats_results:
        dt_val = str(row.get("date_val"))
        tot = int(row.get("total_obs") or 0)
        wc = int(row.get("obs_with_comment") or 0)

        total_obs += tot
        obs_with_comment += wc
        daily_trend.append({
            "date": dt_val,
            "total": tot,
            "with_comments": wc,
        })

    feedback_list = []
    for row in comments_results:
        comment = str(row.get("comment") or "").strip()
        if not comment:
            continue
        feedback_list.append({
            "docente": str(row.get("docente") or "Desconocido").strip(),
            "date": str(row.get("submission_date")),
            "comment": comment,
        })

    return {
        "plantel_requested": plantel_info["plantel_requested"],
        "resolved_name": plantel_info["resolved_name"],
        "scope": scope,
        "date_range": {"start": start_date, "end": end_date},
        "summary": {
            "total_observaciones": total_obs,
            "observaciones_con_comentarios": obs_with_comment,
        },
        "daily_trend": daily_trend,
        "feedback_list": feedback_list,
    }


async def get_planeaciones_report(plantel: str, start_date: date, end_date: date, scope: str) -> dict:
    plantel_info = resolve_plantel(plantel)
    academic_filters = plantel_info["academic_filters"]

    logger.info(
        "Procesando Planeaciones para %s (%s -> %s). academic_filters=%s",
        plantel_info["resolved_name"],
        start_date,
        end_date,
        academic_filters,
    )

    stats_results = await get_planeaciones_stats(academic_filters, start_date, end_date)
    comments_results = await get_planeaciones_comments(academic_filters, start_date, end_date)

    total_plans = 0
    plans_with_feedback = 0
    daily_trend = []

    for row in stats_results:
        dt_val = str(row.get("date_val"))
        tot = int(row.get("total_plans") or 0)
        wf = int(row.get("plans_with_feedback") or 0)

        total_plans += tot
        plans_with_feedback += wf
        daily_trend.append({
            "date": dt_val,
            "total": tot,
            "with_feedback": wf,
        })

    feedback_list = []
    for row in comments_results:
        docente = str(row.get("docente") or "Desconocido").strip()
        created_at = str(row.get("created_at"))

        for f_key in ["feedback", "feedback2", "feedback3"]:
            val = row.get(f_key)
            if val and str(val).strip():
                feedback_list.append({
                    "docente": docente,
                    "date": created_at,
                    "comment": str(val).strip(),
                })

    return {
        "plantel_requested": plantel_info["plantel_requested"],
        "resolved_name": plantel_info["resolved_name"],
        "scope": scope,
        "date_range": {"start": start_date, "end": end_date},
        "summary": {
            "total_planeaciones": total_plans,
            "planeaciones_con_feedback": plans_with_feedback,
        },
        "daily_trend": daily_trend,
        "feedback_list": feedback_list,
    }


async def get_observaciones_docentes_report(plantel: str) -> dict:
    """
    Operational OBS view:
    - teacher rows are always a rolling 30-day window
    - least-observed teacher is evaluated against the current ciclo escolar
    """
    tz_mx = ZoneInfo("America/Mexico_City")
    today = datetime.now(tz_mx).date()
    lookback_start = today - timedelta(days=29)
    school_year_start, school_year_end = get_current_school_year_range(today)

    plantel_info = resolve_plantel(plantel)
    academic_filters = plantel_info["academic_filters"]

    logger.info(
        "Procesando estatus OBS para %s (%s -> %s). academic_filters=%s",
        plantel_info["resolved_name"],
        lookback_start,
        today,
        academic_filters,
    )

    teacher_rows = await get_observaciones_teacher_status(academic_filters, lookback_start, today)
    least_observed_row = await get_least_observed_teacher(
        academic_filters,
        school_year_start,
        school_year_end,
    )

    docentes = []
    for row in teacher_rows:
        latest_observers_raw = str(row.get("latest_observers") or "").strip()
        latest_observers = [
            observer.strip()
            for observer in latest_observers_raw.split("|||")
            if observer.strip()
        ]
        docentes.append({
            "docente": str(row.get("docente") or "").strip(),
            "username": str(row.get("username") or row.get("docente") or "").strip() or None,
            "email": row.get("email"),
            "campus": row.get("campus"),
            "nivel": row.get("nivel"),
            "last_observed_at": str(row.get("last_submission_date")),
            "latest_observers": latest_observers,
            "total_observaciones_ultimos_30_dias": int(row.get("total_observaciones") or 0),
        })

    docente_menos_observado = None
    if least_observed_row:
        docente_menos_observado = {
            "docente": str(least_observed_row.get("docente") or "").strip(),
            "username": str(least_observed_row.get("username") or least_observed_row.get("docente") or "").strip() or None,
            "email": least_observed_row.get("email"),
            "campus": least_observed_row.get("campus"),
            "nivel": least_observed_row.get("nivel"),
            "total_observaciones_ciclo": int(least_observed_row.get("total_observaciones") or 0),
        }

    return {
        "plantel_requested": plantel_info["plantel_requested"],
        "resolved_name": plantel_info["resolved_name"],
        "scope": "last_30_days",
        "date_range": {"start": lookback_start, "end": today},
        "ciclo_escolar": {
            "label": get_school_year_label(today),
            "date_range": {"start": school_year_start, "end": school_year_end},
        },
        "summary": {
            "total_docentes_observados": len(docentes),
            "window_days": 30,
        },
        "docentes": docentes,
        "docente_menos_observado": docente_menos_observado,
    }


async def get_planeaciones_pendientes_report(
    plantel: str,
    start_date: date,
    end_date: date,
    scope: str,
) -> dict:
    """
    Operational PLANS view for pending reviews in a selected period.
    Active docentes are inferred from recent planeaciones activity in the last
    three weeks, independently of the selected reporting period.
    """
    tz_mx = ZoneInfo("America/Mexico_City")
    today = datetime.now(tz_mx).date()
    recent_start_date = today - timedelta(days=20)

    plantel_info = resolve_plantel(plantel)
    academic_filters = plantel_info["academic_filters"]

    logger.info(
        "Procesando planeaciones pendientes para %s (%s -> %s). active_window=%s -> %s academic_filters=%s",
        plantel_info["resolved_name"],
        start_date,
        end_date,
        recent_start_date,
        today,
        academic_filters,
    )

    active_docentes_count = await count_recent_active_planeacion_docentes(
        academic_filters,
        recent_start_date,
        today,
    )
    rows = await get_pending_review_planeaciones(
        academic_filters,
        start_date,
        end_date,
        recent_start_date,
        today,
    )

    planeaciones = []
    for row in rows:
        planeaciones.append({
            "id": row.get("id"),
            "docente": str(row.get("docente") or "").strip(),
            "username": str(row.get("username") or row.get("docente") or "").strip() or None,
            "email": row.get("email"),
            "week": row.get("week"),
            "ciclo": row.get("ciclo"),
            "created_at": str(row.get("created_at")),
            "weekEnd": str(row.get("weekEnd")) if row.get("weekEnd") else None,
            "nivel": row.get("nivel"),
            "campus": row.get("campus"),
            "revisa": row.get("revisa"),
            "revisa2": row.get("revisa2"),
            "revisa3": row.get("revisa3"),
            "feedback": row.get("feedback"),
            "feedback2": row.get("feedback2"),
            "feedback3": row.get("feedback3"),
        })

    return {
        "plantel_requested": plantel_info["plantel_requested"],
        "resolved_name": plantel_info["resolved_name"],
        "scope": scope,
        "date_range": {"start": start_date, "end": end_date},
        "active_window": {
            "start": recent_start_date,
            "end": today,
            "days": 21,
        },
        "summary": {
            "total_planeaciones_pendientes": len(planeaciones),
            "docentes_activos": active_docentes_count,
        },
        "planeaciones_pendientes": planeaciones,
    }
