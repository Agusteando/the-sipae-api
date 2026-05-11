from datetime import date
from core.utils import resolve_plantel
from core.logger import get_logger
from .repository import (
    get_observaciones_stats, get_observaciones_comments,
    get_planeaciones_stats, get_planeaciones_comments
)

logger = get_logger("service.academic")

async def get_observaciones_report(plantel: str, start_date: date, end_date: date, scope: str) -> dict:
    plantel_info = resolve_plantel(plantel)
    db_code = plantel_info["db_code"]
    
    logger.info(f"Procesando Observaciones Académicas para {plantel_info['resolved_name']} ({start_date} -> {end_date})")
    
    stats_results = await get_observaciones_stats(db_code, start_date, end_date)
    comments_results = await get_observaciones_comments(db_code, start_date, end_date)
    
    total_obs = 0
    obs_with_comment = 0
    daily_trend = []
    
    for row in stats_results:
        dt_val = str(row.get("date_val"))
        tot = int(row.get("total_obs", 0))
        wc = int(row.get("obs_with_comment", 0))
        
        total_obs += tot
        obs_with_comment += wc
        daily_trend.append({
            "date": dt_val,
            "total": tot,
            "with_comments": wc
        })
        
    feedback_list = []
    for row in comments_results:
        feedback_list.append({
            "docente": str(row.get("docente") or "Desconocido").strip(),
            "date": str(row.get("created_at")),
            "comment": str(row.get("comment")).strip()
        })
        
    return {
        "plantel_requested": plantel_info["plantel_requested"],
        "resolved_name": plantel_info["resolved_name"],
        "scope": scope,
        "date_range": {"start": start_date, "end": end_date},
        "summary": {
            "total_observaciones": total_obs,
            "observaciones_con_comentarios": obs_with_comment
        },
        "daily_trend": daily_trend,
        "feedback_list": feedback_list
    }

async def get_planeaciones_report(plantel: str, start_date: date, end_date: date, scope: str) -> dict:
    plantel_info = resolve_plantel(plantel)
    db_code = plantel_info["db_code"]
    
    logger.info(f"Procesando Planeaciones para {plantel_info['resolved_name']} ({start_date} -> {end_date})")
    
    stats_results = await get_planeaciones_stats(db_code, start_date, end_date)
    comments_results = await get_planeaciones_comments(db_code, start_date, end_date)
    
    total_plans = 0
    plans_with_feedback = 0
    daily_trend = []
    
    for row in stats_results:
        dt_val = str(row.get("date_val"))
        tot = int(row.get("total_plans", 0))
        wf = int(row.get("plans_with_feedback", 0))
        
        total_plans += tot
        plans_with_feedback += wf
        daily_trend.append({
            "date": dt_val,
            "total": tot,
            "with_feedback": wf
        })
        
    feedback_list = []
    for row in comments_results:
        docente = str(row.get("docente") or "Desconocido").strip()
        created_at = str(row.get("created_at"))
        
        # Consolidar los posibles 3 feedbacks en una misma lista visual para el usuario
        for f_key in ['feedback', 'feedback2', 'feedback3']:
            val = row.get(f_key)
            if val and str(val).strip():
                feedback_list.append({
                    "docente": docente,
                    "date": created_at,
                    "comment": str(val).strip()
                })
        
    return {
        "plantel_requested": plantel_info["plantel_requested"],
        "resolved_name": plantel_info["resolved_name"],
        "scope": scope,
        "date_range": {"start": start_date, "end": end_date},
        "summary": {
            "total_planeaciones": total_plans,
            "planeaciones_con_feedback": plans_with_feedback
        },
        "daily_trend": daily_trend,
        "feedback_list": feedback_list
    }