import asyncio
import math
from datetime import date, datetime, timedelta
from typing import Any, Awaitable, Callable, Dict, List, Optional
from zoneinfo import ZoneInfo

from core.logger import get_logger
from core.utils import resolve_plantel
from modules.academic.service import (
    get_observaciones_docentes_report,
    get_observaciones_report,
    get_planeaciones_pendientes_report,
    get_planeaciones_report,
)
from modules.attendance.service import get_attendance_detail_report
from modules.baselines.service import get_global_baseline_report
from modules.employee_attendance.service import get_kardex_attendance_report
from modules.husky.service import calculate_husky_daily_rate, get_plantel_retardos
from modules.sapf.service import get_sapf_monthly_report, get_sapf_motivos_report

logger = get_logger("service.corporate_compliance")

FIXED_PLANTEL_ORDER = ["PT", "PM", "ST", "SM", "PREET", "PREEM"]
DOMAIN_WEIGHTS = {
    "attendance": 28,
    "academic": 25,
    "husky": 18,
    "employee": 18,
    "sapf": 11,
}


def _mx_now() -> datetime:
    return datetime.now(ZoneInfo("America/Mexico_City"))


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except Exception:
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        parsed = float(value)
        if not math.isfinite(parsed):
            return default
        return parsed
    except Exception:
        return default


def _round(value: Optional[float], digits: int = 2) -> Optional[float]:
    if value is None or not isinstance(value, (int, float)) or not math.isfinite(value):
        return None
    return round(float(value), digits)


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def _pct(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100, 2)


def _business_days(start_date: date, end_date: date) -> int:
    cursor = start_date
    days = 0
    while cursor <= end_date:
        if cursor.weekday() < 5:
            days += 1
        cursor += timedelta(days=1)
    return max(days, 1)


def _date_range_days(start_date: date, end_date: date) -> int:
    return max((end_date - start_date).days + 1, 1)


def _status_from_index(index: float, has_critical_flag: bool = False) -> str:
    if has_critical_flag or index < 70:
        return "critical"
    if index < 85:
        return "warning"
    return "fulfilled"


def _risk_label(status: str) -> str:
    return {
        "fulfilled": "Cumplimiento total / Seguro",
        "warning": "Riesgo operativo / Atención requerida",
        "critical": "Incumplimiento crítico / Riesgo legal o financiero",
        "unavailable": "Sin lectura completa",
    }.get(status, "Sin lectura completa")


def _normalize_planteles(planteles: Optional[str]) -> List[str]:
    if not planteles:
        return list(FIXED_PLANTEL_ORDER)
    requested = {item.strip().upper() for item in str(planteles).split(",") if item.strip()}
    selected = [code for code in FIXED_PLANTEL_ORDER if code in requested]
    return selected or list(FIXED_PLANTEL_ORDER)


async def _safe_call(name: str, fn: Callable[[], Awaitable[Dict[str, Any]]]) -> Dict[str, Any]:
    try:
        return await fn()
    except Exception as exc:
        logger.error("Corporate dashboard source failed: %s: %s", name, exc)
        return {"error": str(exc), "source": name}


def _sum_daily_attendance(attendance: Dict[str, Any], start_date: date, end_date: date) -> Dict[str, Any]:
    if attendance.get("error"):
        return {
            "status": "unavailable",
            "error": attendance.get("error"),
            "completion_percent": 0.0,
            "attendance_rate_percent": 0.0,
            "missing_groups_count": 0,
            "missing_expected_students": 0,
            "absent_students_count": 0,
            "expected_groups_count": 0,
            "completed_groups_count": 0,
            "legal_risk_units": 0,
            "missing_groups": [],
            "risk_score": 45.0,
        }

    daily_payloads: List[Dict[str, Any]] = []
    if attendance.get("mode") == "daily" or attendance.get("missing_groups_data"):
        daily_payloads.append(attendance)
    else:
        for day_key in sorted((attendance.get("daily_points") or {}).keys()):
            point = dict(attendance["daily_points"].get(day_key) or {})
            point["date"] = day_key
            daily_payloads.append(point)

    expected_groups = 0
    completed_groups = 0
    missing_groups_count = 0
    missing_expected_students = 0
    total_students = 0
    present_students = 0
    absent_students = 0
    missing_groups: List[Dict[str, Any]] = []

    for point in daily_payloads:
        missing_data = point.get("missing_groups_data") or {}
        summary = point.get("summary") or {}
        point_date = point.get("date") or point.get("date_range", {}).get("start") or start_date.isoformat()

        expected_groups += _safe_int(missing_data.get("expected_groups_count"))
        completed_groups += _safe_int(missing_data.get("completed_groups_count"))
        missing_groups_count += _safe_int(missing_data.get("missing_groups_count"))
        missing_expected_students += _safe_int(missing_data.get("expected_students_count"))
        total_students += _safe_int(summary.get("total_students"))
        present_students += _safe_int(summary.get("asistencia"))
        absent_students += _safe_int(summary.get("ausencia")) + _safe_int(summary.get("ausencia2"))

        for group in missing_data.get("missing_groups") or []:
            missing_groups.append({
                "date": str(point_date),
                "grado": str(group.get("grado") or "").strip(),
                "grupo": str(group.get("grupo") or "").strip(),
                "expected_students": _safe_int(group.get("expected_students")),
            })

    if expected_groups == 0 and not daily_payloads:
        completion = 0.0
    else:
        completion = _pct(completed_groups, expected_groups)
    attendance_rate = _pct(present_students, total_students)
    legal_risk_units = missing_expected_students

    risk_score = _clamp((100 - completion) * 1.65 + missing_groups_count * 6 + missing_expected_students * 0.08)
    critical = missing_groups_count > 0 or completion < 90
    status = _status_from_index(100 - risk_score, critical)

    return {
        "status": status,
        "completion_percent": completion,
        "attendance_rate_percent": attendance_rate,
        "expected_groups_count": expected_groups,
        "completed_groups_count": completed_groups,
        "missing_groups_count": missing_groups_count,
        "missing_expected_students": missing_expected_students,
        "absent_students_count": absent_students,
        "total_students_recorded": total_students,
        "legal_risk_units": legal_risk_units,
        "missing_groups": missing_groups[:80],
        "risk_score": _round(risk_score, 2),
        "risk_narrative": "Grupos sin pase de lista, rompiendo la continuidad del expediente legal y operativo del alumno, generando riesgo de compliance.",
    }


def _sum_husky(husky: Dict[str, Any], retardos: Dict[str, Any], start_date: date, end_date: date) -> Dict[str, Any]:
    if husky.get("error"):
        return {
            "status": "unavailable",
            "error": husky.get("error"),
            "scan_rate_percent": 0.0,
            "entrada_scans": 0,
            "salida_scans": 0,
            "scan_gap": 0,
            "student_tardies": _safe_int(retardos.get("total_retardos")),
            "risk_score": 45.0,
        }

    points = husky.get("daily_datapoints") or {}
    expected_population = _safe_int(husky.get("expected_population"))
    entrada = sum(_safe_int(point.get("entrada")) for point in points.values())
    salida = sum(_safe_int(point.get("salida")) for point in points.values())
    expected_ops = expected_population * _business_days(start_date, end_date)
    scan_rate = min(_pct(entrada, expected_ops), 100.0) if expected_ops else 0.0
    scan_gap = max(expected_ops - entrada, 0)
    student_tardies = _safe_int(retardos.get("total_retardos")) if not retardos.get("error") else 0

    risk_score = _clamp(max(0.0, 90 - scan_rate) * 1.75 + student_tardies * 0.8)
    status = _status_from_index(100 - risk_score, scan_rate < 60)
    return {
        "status": status,
        "expected_population": expected_population,
        "expected_scan_ops": expected_ops,
        "entrada_scans": entrada,
        "salida_scans": salida,
        "scan_rate_percent": _round(scan_rate, 2),
        "scan_gap": scan_gap,
        "student_tardies": student_tardies,
        "late_arrivals_sample": (retardos.get("retardos") or [])[:40] if not retardos.get("error") else [],
        "risk_score": _round(risk_score, 2),
        "risk_narrative": "Vulnerabilidad de seguridad y cadena de custodia en accesos.",
    }


def _sum_employee(kardex: Dict[str, Any]) -> Dict[str, Any]:
    if kardex.get("error"):
        return {
            "status": "unavailable",
            "error": kardex.get("error"),
            "employee_tardies": 0,
            "employee_absences": 0,
            "payroll_waste_minutes": 0,
            "risk_score": 45.0,
        }

    summary = kardex.get("summary") or {}
    employee_tardies = _safe_int(summary.get("retardos_count"))
    employee_absences = _safe_int(summary.get("ausencias_count"))
    tardies = kardex.get("retardos") or []
    absences = kardex.get("ausencias") or []
    payroll_minutes = sum(_safe_int(item.get("minutos_descontar")) for item in tardies)
    incidents = employee_tardies + employee_absences
    risk_score = _clamp(employee_absences * 24 + employee_tardies * 8 + payroll_minutes * 0.22)
    status = _status_from_index(100 - risk_score, employee_absences > 0)
    return {
        "status": status,
        "employee_tardies": employee_tardies,
        "employee_absences": employee_absences,
        "employee_incidents": incidents,
        "payroll_waste_minutes": payroll_minutes,
        "incident_sample": (absences + tardies)[:60],
        "risk_score": _round(risk_score, 2),
        "risk_narrative": "Fuga de capital humano y horas no laboradas.",
    }


def _sum_academic(
    observaciones: Dict[str, Any],
    planeaciones: Dict[str, Any],
    observaciones_docentes: Dict[str, Any],
    planeaciones_pendientes: Dict[str, Any],
) -> Dict[str, Any]:
    source_errors = [x.get("error") for x in [observaciones, planeaciones, observaciones_docentes, planeaciones_pendientes] if x.get("error")]

    obs_summary = observaciones.get("summary") or {}
    plan_summary = planeaciones.get("summary") or {}
    obs_doc_summary = observaciones_docentes.get("summary") or {}
    pending_summary = planeaciones_pendientes.get("summary") or {}

    total_observaciones = _safe_int(obs_summary.get("total_observaciones"))
    observaciones_con_comentarios = _safe_int(obs_summary.get("observaciones_con_comentarios"))
    total_planeaciones = _safe_int(plan_summary.get("total_planeaciones"))
    planeaciones_con_feedback = _safe_int(plan_summary.get("planeaciones_con_feedback"))
    docentes_activos = _safe_int(obs_doc_summary.get("total_docentes_activos"))
    docentes_sin_observacion = _safe_int(obs_doc_summary.get("total_docentes_sin_observacion_30_dias"))
    docentes_nunca_observados = _safe_int(obs_doc_summary.get("total_docentes_nunca_observados_ciclo"))
    planeaciones_pendientes_count = _safe_int(pending_summary.get("total_planeaciones_pendientes"))
    docentes_con_pendientes = _safe_int(pending_summary.get("docentes_con_planeaciones_pendientes"))
    docentes_activos_planeacion = _safe_int(pending_summary.get("docentes_activos"))

    obs_comment_rate = _pct(observaciones_con_comentarios, total_observaciones)
    plan_feedback_rate = _pct(planeaciones_con_feedback, total_planeaciones)
    observation_gap_rate = _pct(docentes_sin_observacion, docentes_activos)
    pending_teacher_rate = _pct(docentes_con_pendientes, docentes_activos_planeacion)
    supervision_backlog = docentes_sin_observacion + planeaciones_pendientes_count

    risk_score = _clamp(
        observation_gap_rate * 0.42
        + pending_teacher_rate * 0.32
        + planeaciones_pendientes_count * 2.4
        + docentes_nunca_observados * 4.0
        + max(0.0, 75 - plan_feedback_rate) * 0.10
    )
    status = "unavailable" if source_errors and not any([total_observaciones, total_planeaciones, docentes_activos, planeaciones_pendientes_count]) else _status_from_index(100 - risk_score, supervision_backlog > 0)

    return {
        "status": status,
        "errors": source_errors,
        "total_observaciones": total_observaciones,
        "observaciones_con_comentarios": observaciones_con_comentarios,
        "observaciones_comment_rate_percent": obs_comment_rate,
        "total_planeaciones": total_planeaciones,
        "planeaciones_con_feedback": planeaciones_con_feedback,
        "planeaciones_feedback_rate_percent": plan_feedback_rate,
        "docentes_activos": docentes_activos,
        "docentes_sin_observacion_30_dias": docentes_sin_observacion,
        "docentes_nunca_observados_ciclo": docentes_nunca_observados,
        "observation_gap_rate_percent": observation_gap_rate,
        "planeaciones_pendientes": planeaciones_pendientes_count,
        "docentes_con_planeaciones_pendientes": docentes_con_pendientes,
        "pending_teacher_rate_percent": pending_teacher_rate,
        "supervision_backlog": supervision_backlog,
        "docentes_sin_observacion_sample": (observaciones_docentes.get("docentes_sin_observacion") or [])[:60] if not observaciones_docentes.get("error") else [],
        "planeaciones_pendientes_sample": (planeaciones_pendientes.get("planeaciones_pendientes") or [])[:60] if not planeaciones_pendientes.get("error") else [],
        "risk_score": _round(risk_score, 2),
        "risk_narrative": "Negligencia de supervisión académica por parte de Dirección/Coordinación.",
    }


def _sum_sapf(monthly: Dict[str, Any], motivos: Dict[str, Any]) -> Dict[str, Any]:
    if monthly.get("error") and motivos.get("error"):
        return {
            "status": "unavailable",
            "error": monthly.get("error") or motivos.get("error"),
            "parent_interactions": 0,
            "risk_score": 35.0,
        }

    areas = monthly.get("data") or []
    parent_interactions = sum(_safe_int(area.get("total_conteo")) for area in areas)
    motive_rows = motivos.get("motivos") or [] if not motivos.get("error") else []
    top_motives = sorted(motive_rows, key=lambda item: _safe_int(item.get("conteo")), reverse=True)[:10]
    area_rows = sorted(
        [{"area": area.get("area"), "conteo": _safe_int(area.get("total_conteo"))} for area in areas],
        key=lambda item: item["conteo"],
        reverse=True,
    )[:12]
    risk_score = 42.0 if parent_interactions == 0 else 0.0
    status = _status_from_index(100 - risk_score, False)
    return {
        "status": status,
        "parent_interactions": parent_interactions,
        "areas": area_rows,
        "top_motives": top_motives,
        "risk_score": _round(risk_score, 2),
        "risk_narrative": "Trazabilidad de atención a padres y concentración de motivos de presión operativa.",
    }


def _build_index(domains: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    weighted_risk = 0.0
    total_weight = 0.0
    has_critical_flag = False

    for key, weight in DOMAIN_WEIGHTS.items():
        domain = domains.get(key) or {}
        risk = _safe_float(domain.get("risk_score"), 45.0)
        weighted_risk += risk * weight
        total_weight += weight
        if domain.get("status") == "critical":
            has_critical_flag = True

    index = 100.0 - (weighted_risk / total_weight if total_weight else 100.0)
    index = _clamp(index)
    status = _status_from_index(index, has_critical_flag)
    return {
        "score": _round(index, 1),
        "risk_score": _round(100.0 - index, 1),
        "status": status,
        "label": _risk_label(status),
    }


async def _collect_plantel(plantel: str, start_date: date, end_date: date, scope: str) -> Dict[str, Any]:
    week_start = start_date - timedelta(days=start_date.weekday())
    info = resolve_plantel(plantel)

    results = await asyncio.gather(
        _safe_call("attendance", lambda: get_attendance_detail_report(plantel, start_date, end_date, scope)),
        _safe_call("husky", lambda: calculate_husky_daily_rate(plantel, start_date, end_date, scope)),
        _safe_call("husky_retardos", lambda: get_plantel_retardos(plantel, start_date, end_date, scope)),
        _safe_call("kardex", lambda: get_kardex_attendance_report(plantel, start_date, end_date, scope)),
        _safe_call("sapf_monthly", lambda: get_sapf_monthly_report(plantel, start_date, end_date, scope)),
        _safe_call("sapf_motivos", lambda: get_sapf_motivos_report(plantel, start_date, end_date, scope)),
        _safe_call("observaciones", lambda: get_observaciones_report(plantel, start_date, end_date, scope)),
        _safe_call("planeaciones", lambda: get_planeaciones_report(plantel, start_date, end_date, scope)),
        _safe_call("observaciones_docentes", lambda: get_observaciones_docentes_report(plantel)),
        _safe_call("planeaciones_pendientes", lambda: get_planeaciones_pendientes_report(plantel, week_start, end_date, "range")),
    )

    attendance, husky, retardos, kardex, sapf_monthly, sapf_motivos, observaciones, planeaciones, obs_docentes, plan_pendientes = results
    domains = {
        "attendance": _sum_daily_attendance(attendance, start_date, end_date),
        "husky": _sum_husky(husky, retardos, start_date, end_date),
        "employee": _sum_employee(kardex),
        "academic": _sum_academic(observaciones, planeaciones, obs_docentes, plan_pendientes),
        "sapf": _sum_sapf(sapf_monthly, sapf_motivos),
    }
    index = _build_index(domains)

    return {
        "plantel": plantel,
        "plantel_order": FIXED_PLANTEL_ORDER.index(plantel),
        "resolved_name": info["resolved_name"],
        "index": index,
        "domain_scores": {
            key: {
                "compliance_score": _round(100.0 - _safe_float(domain.get("risk_score"), 45.0), 1),
                "risk_score": _round(_safe_float(domain.get("risk_score"), 45.0), 1),
                "status": domain.get("status") or "unavailable",
                "label": _risk_label(domain.get("status") or "unavailable"),
            }
            for key, domain in domains.items()
        },
        "domains": domains,
        "source_errors": [
            {"source": item.get("source"), "error": item.get("error")}
            for item in results
            if item.get("error")
        ],
    }


def _aggregate(planteles: List[Dict[str, Any]], start_date: date, end_date: date) -> Dict[str, Any]:
    count = max(len(planteles), 1)
    avg_index = sum(_safe_float(p.get("index", {}).get("score")) for p in planteles) / count
    critical_count = sum(1 for p in planteles if p.get("index", {}).get("status") == "critical")
    warning_count = sum(1 for p in planteles if p.get("index", {}).get("status") == "warning")
    green_count = sum(1 for p in planteles if p.get("index", {}).get("status") == "fulfilled")

    totals = {
        "missing_groups": sum(_safe_int(p["domains"]["attendance"].get("missing_groups_count")) for p in planteles),
        "students_without_legal_attendance_trace": sum(_safe_int(p["domains"]["attendance"].get("missing_expected_students")) for p in planteles),
        "employee_incidents": sum(_safe_int(p["domains"]["employee"].get("employee_incidents")) for p in planteles),
        "employee_absences": sum(_safe_int(p["domains"]["employee"].get("employee_absences")) for p in planteles),
        "employee_tardies": sum(_safe_int(p["domains"]["employee"].get("employee_tardies")) for p in planteles),
        "payroll_waste_minutes": sum(_safe_int(p["domains"]["employee"].get("payroll_waste_minutes")) for p in planteles),
        "security_scan_gap": sum(_safe_int(p["domains"]["husky"].get("scan_gap")) for p in planteles),
        "student_tardies": sum(_safe_int(p["domains"]["husky"].get("student_tardies")) for p in planteles),
        "academic_backlog": sum(_safe_int(p["domains"]["academic"].get("supervision_backlog")) for p in planteles),
        "pending_lesson_reviews": sum(_safe_int(p["domains"]["academic"].get("planeaciones_pendientes")) for p in planteles),
        "teachers_without_observation": sum(_safe_int(p["domains"]["academic"].get("docentes_sin_observacion_30_dias")) for p in planteles),
        "sapf_parent_interactions": sum(_safe_int(p["domains"]["sapf"].get("parent_interactions")) for p in planteles),
    }

    worst = None
    best = None
    if planteles:
        worst = min(planteles, key=lambda p: _safe_float(p.get("index", {}).get("score"), 0.0))
        best = max(planteles, key=lambda p: _safe_float(p.get("index", {}).get("score"), 0.0))

    status = _status_from_index(avg_index, critical_count > 0)
    return {
        "corporate_index": {
            "score": _round(avg_index, 1),
            "risk_score": _round(100 - avg_index, 1),
            "status": status,
            "label": _risk_label(status),
        },
        "status_counts": {
            "fulfilled": green_count,
            "warning": warning_count,
            "critical": critical_count,
        },
        "totals": totals,
        "worst_plantel": {
            "plantel": worst.get("plantel"),
            "resolved_name": worst.get("resolved_name"),
            "score": worst.get("index", {}).get("score"),
            "status": worst.get("index", {}).get("status"),
        } if worst else None,
        "best_plantel": {
            "plantel": best.get("plantel"),
            "resolved_name": best.get("resolved_name"),
            "score": best.get("index", {}).get("score"),
            "status": best.get("index", {}).get("status"),
        } if best else None,
        "window": {
            "start": start_date.isoformat(),
            "end": end_date.isoformat(),
            "calendar_days": _date_range_days(start_date, end_date),
            "business_days": _business_days(start_date, end_date),
        },
    }


def _attach_baselines(planteles: List[Dict[str, Any]], baseline_report: Dict[str, Any]) -> None:
    baseline_map = {
        str(item.get("code") or item.get("requested_code") or "").upper(): item
        for item in baseline_report.get("planteles") or []
    }
    for plantel in planteles:
        baseline = baseline_map.get(plantel["plantel"])
        if not baseline:
            continue
        plantel["baseline"] = {
            "score": baseline.get("score"),
            "status": baseline.get("status"),
            "activity": baseline.get("activity"),
            "metrics": {
                key: {
                    "score": value.get("score"),
                    "status": value.get("status"),
                    "today": value.get("today"),
                    "activity": value.get("activity"),
                }
                for key, value in (baseline.get("metrics") or {}).items()
            },
        }


async def get_corporate_compliance_index(
    planteles: Optional[str],
    start_date: date,
    end_date: date,
    scope: str,
    include_baselines: bool = True,
) -> Dict[str, Any]:
    selected_planteles = _normalize_planteles(planteles)
    semaphore = asyncio.Semaphore(2)

    async def collect(code: str) -> Dict[str, Any]:
        async with semaphore:
            return await _collect_plantel(code, start_date, end_date, scope)

    plantel_payloads = await asyncio.gather(*(collect(code) for code in selected_planteles))
    plantel_payloads.sort(key=lambda item: item.get("plantel_order", 999))

    baseline_payload: Optional[Dict[str, Any]] = None
    if include_baselines:
        baseline_payload = await _safe_call(
            "baselines",
            lambda: get_global_baseline_report(
                planteles=",".join(selected_planteles),
                start_date=start_date,
                end_date=end_date,
                comparison_months=3,
                history_months=9,
            ),
        )
        if baseline_payload and not baseline_payload.get("error"):
            _attach_baselines(plantel_payloads, baseline_payload)

    return {
        "title": "SIPAE Corporate Compliance & Risk Index",
        "subtitle": "Índice Corporativo de Cumplimiento SIPAE",
        "generated_at": _mx_now().isoformat(),
        "timezone": "America/Mexico_City",
        "scope": scope,
        "plantel_order": FIXED_PLANTEL_ORDER,
        "selected_planteles": selected_planteles,
        "weights": DOMAIN_WEIGHTS,
        "risk_language": {
            "fulfilled": _risk_label("fulfilled"),
            "warning": _risk_label("warning"),
            "critical": _risk_label("critical"),
            "attendance": "Grupos sin pase de lista, rompiendo la continuidad del expediente legal y operativo del alumno, generando riesgo de compliance.",
            "employee": "Fuga de capital humano y horas no laboradas.",
            "academic": "Negligencia de supervisión académica por parte de Dirección/Coordinación.",
            "husky": "Vulnerabilidad de seguridad y cadena de custodia en accesos.",
        },
        "aggregate": _aggregate(plantel_payloads, start_date, end_date),
        "planteles": plantel_payloads,
        "baselines": baseline_payload if include_baselines else None,
    }
