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
from modules.sapf.service import get_sapf_monthly_report, get_sapf_motivos_report, get_sapf_overview_report

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
            "absence_rate_percent": 0.0,
            "missing_groups_count": 0,
            "missing_expected_students": 0,
            "absent_students_count": 0,
            "expected_groups_count": 0,
            "completed_groups_count": 0,
            "legal_risk_units": 0,
            "no_modality_records": 0,
            "missing_groups": [],
            "repeated_missing_groups": [],
            "daily_attendance": [],
            "absence_motives": [],
            "group_absence_hotspots": [],
            "low_attendance_groups": [],
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
    no_modality_records = 0
    presencial_count = 0
    virtual_count = 0
    girls_count = 0
    boys_count = 0
    missing_groups: List[Dict[str, Any]] = []
    daily_attendance: List[Dict[str, Any]] = []
    motive_counts: Dict[str, int] = {}
    group_absence: Dict[str, Dict[str, Any]] = {}
    group_seen: Dict[str, Dict[str, int]] = {}
    missing_counter: Dict[str, Dict[str, Any]] = {}

    for point in daily_payloads:
        missing_data = point.get("missing_groups_data") or {}
        summary = point.get("summary") or {}
        point_date = point.get("date") or point.get("date_range", {}).get("start") or start_date.isoformat()
        point_date_str = str(point_date)

        expected_day = _safe_int(missing_data.get("expected_groups_count"))
        completed_day = _safe_int(missing_data.get("completed_groups_count"))
        missing_day = _safe_int(missing_data.get("missing_groups_count"))
        missing_students_day = _safe_int(missing_data.get("expected_students_count"))
        total_day = _safe_int(summary.get("total_students"))
        present_day = _safe_int(summary.get("asistencia"))
        absent_day = _safe_int(summary.get("ausencia"))
        no_modality_day = _safe_int(summary.get("ausencia2"))

        expected_groups += expected_day
        completed_groups += completed_day
        missing_groups_count += missing_day
        missing_expected_students += missing_students_day
        total_students += total_day
        present_students += present_day
        absent_students += absent_day
        no_modality_records += no_modality_day
        presencial_count += _safe_int(summary.get("presencial"))
        virtual_count += _safe_int(summary.get("virt"))
        girls_count += _safe_int(summary.get("girls"))
        boys_count += _safe_int(summary.get("boys"))

        daily_attendance.append({
            "date": point_date_str,
            "total_students": total_day,
            "present_students": present_day,
            "absent_students": absent_day,
            "attendance_rate_percent": _pct(present_day, total_day),
            "absence_rate_percent": _pct(absent_day, total_day),
            "completion_percent": _safe_float(missing_data.get("completion_percent")),
            "missing_groups_count": missing_day,
            "missing_expected_students": missing_students_day,
        })

        for group in point.get("groups") or []:
            grade = str(group.get("grado") or "").strip()
            classroom = str(group.get("grupo") or "").strip()
            key = f"{grade} {classroom}".strip() or "Sin grupo"
            g_total = _safe_int(group.get("total_students_per_group"))
            g_present = _safe_int(group.get("asistencia"))
            g_absent = _safe_int(group.get("ausencia"))
            if key not in group_seen:
                group_seen[key] = {"days": 0, "total": 0, "present": 0, "absent": 0}
            group_seen[key]["days"] += 1
            group_seen[key]["total"] += g_total
            group_seen[key]["present"] += g_present
            group_seen[key]["absent"] += g_absent

        for absent in point.get("absent_students") or []:
            motive = str(absent.get("motivo") or "Sin motivo capturado").strip() or "Sin motivo capturado"
            motive_counts[motive] = motive_counts.get(motive, 0) + 1
            grade = str(absent.get("grado") or "").strip()
            classroom = str(absent.get("grupo") or "").strip()
            key = f"{grade} {classroom}".strip() or "Sin grupo"
            if key not in group_absence:
                group_absence[key] = {"grupo": key, "absences": 0, "dates": set()}
            group_absence[key]["absences"] += 1
            group_absence[key]["dates"].add(point_date_str)

        for group in missing_data.get("missing_groups") or []:
            grade = str(group.get("grado") or "").strip()
            classroom = str(group.get("grupo") or "").strip()
            group_key = f"{grade} {classroom}".strip() or "Sin grupo"
            expected = _safe_int(group.get("expected_students"))
            missing_groups.append({
                "date": point_date_str,
                "grado": grade,
                "grupo": classroom,
                "expected_students": expected,
            })
            if group_key not in missing_counter:
                missing_counter[group_key] = {"grupo": group_key, "days_missing": 0, "expected_students": expected, "dates": []}
            missing_counter[group_key]["days_missing"] += 1
            missing_counter[group_key]["expected_students"] = max(missing_counter[group_key]["expected_students"], expected)
            missing_counter[group_key]["dates"].append(point_date_str)

    completion = _pct(completed_groups, expected_groups) if expected_groups else 0.0
    attendance_rate = _pct(present_students, total_students)
    absence_rate = _pct(absent_students, total_students)
    legal_risk_units = missing_expected_students

    absence_motives = [
        {"motivo": motive, "conteo": count}
        for motive, count in sorted(motive_counts.items(), key=lambda item: (-item[1], item[0]))[:12]
    ]
    group_absence_hotspots = [
        {"grupo": item["grupo"], "absences": item["absences"], "days_with_absences": len(item["dates"])}
        for item in sorted(group_absence.values(), key=lambda row: (-row["absences"], row["grupo"]))[:20]
    ]
    low_attendance_groups = []
    for group, data in group_seen.items():
        rate = _pct(data["present"], data["total"])
        if data["total"] > 0 and rate < 90:
            low_attendance_groups.append({
                "grupo": group,
                "attendance_rate_percent": rate,
                "absent_students": data["absent"],
                "total_students": data["total"],
                "days_recorded": data["days"],
            })
    low_attendance_groups.sort(key=lambda row: (row["attendance_rate_percent"], -row["absent_students"], row["grupo"]))
    repeated_missing_groups = [
        {**item, "dates": item["dates"][:10]}
        for item in sorted(missing_counter.values(), key=lambda row: (-row["days_missing"], row["grupo"]))
        if item["days_missing"] > 1
    ][:20]

    risk_score = _clamp(
        (100 - completion) * 1.65
        + missing_groups_count * 6
        + missing_expected_students * 0.08
        + max(0.0, absence_rate - 8.0) * 0.75
        + len(repeated_missing_groups) * 4
    )
    critical = missing_groups_count > 0 or completion < 90
    status = _status_from_index(100 - risk_score, critical)

    return {
        "status": status,
        "completion_percent": completion,
        "attendance_rate_percent": attendance_rate,
        "absence_rate_percent": absence_rate,
        "expected_groups_count": expected_groups,
        "completed_groups_count": completed_groups,
        "missing_groups_count": missing_groups_count,
        "missing_expected_students": missing_expected_students,
        "absent_students_count": absent_students,
        "total_students_recorded": total_students,
        "no_modality_records": no_modality_records,
        "presencial_count": presencial_count,
        "virtual_count": virtual_count,
        "girls_count": girls_count,
        "boys_count": boys_count,
        "legal_risk_units": legal_risk_units,
        "missing_groups": missing_groups[:120],
        "repeated_missing_groups": repeated_missing_groups,
        "daily_attendance": daily_attendance,
        "absence_motives": absence_motives,
        "group_absence_hotspots": group_absence_hotspots,
        "low_attendance_groups": low_attendance_groups[:20],
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
            "repeat_tardy_students": [],
            "daily_tardies": [],
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

    tardy_rows = retardos.get("retardos") or [] if not retardos.get("error") else []
    tardies_by_day: Dict[str, int] = {}
    student_map: Dict[str, Dict[str, Any]] = {}
    for item in tardy_rows:
        day = str(item.get("date") or "")
        if day:
            tardies_by_day[day] = tardies_by_day.get(day, 0) + 1
        matricula = str(item.get("matricula") or "N/A").strip() or "N/A"
        name = str(item.get("student_fullname") or "Desconocido").strip() or "Desconocido"
        key = matricula if matricula != "N/A" else name
        if key not in student_map:
            student_map[key] = {"matricula": matricula, "student_fullname": name, "tardies": 0, "dates": []}
        student_map[key]["tardies"] += 1
        if day:
            student_map[key]["dates"].append(day)

    daily_tardies = [
        {"date": day, "tardies": count}
        for day, count in sorted(tardies_by_day.items(), key=lambda item: item[0])
    ]
    repeat_tardy_students = [
        {**item, "dates": item["dates"][:10]}
        for item in sorted(student_map.values(), key=lambda row: (-row["tardies"], row["student_fullname"]))
        if item["tardies"] > 1
    ][:30]
    tardy_rate_per_population = _pct(student_tardies, expected_population * _business_days(start_date, end_date)) if expected_population else 0.0
    avg_tardies_per_business_day = _round(student_tardies / _business_days(start_date, end_date), 2)

    risk_score = _clamp(max(0.0, 90 - scan_rate) * 1.75 + student_tardies * 0.8 + len(repeat_tardy_students) * 1.5)
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
        "student_tardy_rate_percent": _round(tardy_rate_per_population, 2),
        "avg_tardies_per_business_day": avg_tardies_per_business_day,
        "daily_tardies": daily_tardies,
        "repeat_tardy_students": repeat_tardy_students,
        "late_arrivals_sample": tardy_rows[:60],
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


def _sum_sapf(monthly: Dict[str, Any], motivos: Dict[str, Any], overview: Dict[str, Any]) -> Dict[str, Any]:
    if monthly.get("error") and motivos.get("error") and overview.get("error"):
        return {
            "status": "unavailable",
            "error": monthly.get("error") or motivos.get("error") or overview.get("error"),
            "parent_interactions": 0,
            "risk_score": 35.0,
        }

    areas = monthly.get("data") or [] if not monthly.get("error") else []
    parent_interactions_from_monthly = sum(_safe_int(area.get("total_conteo")) for area in areas)
    total_fichas = _safe_int(overview.get("total_fichas")) if not overview.get("error") else 0
    total_followups = _safe_int(overview.get("total_followups")) if not overview.get("error") else 0
    parent_interactions = _safe_int(overview.get("total_interactions")) if not overview.get("error") else parent_interactions_from_monthly
    if parent_interactions == 0 and parent_interactions_from_monthly:
        parent_interactions = parent_interactions_from_monthly

    open_cases = _safe_int(overview.get("open_cases")) if not overview.get("error") else 0
    closed_cases = _safe_int(overview.get("closed_cases")) if not overview.get("error") else 0
    complaints = _safe_int(overview.get("complaints")) if not overview.get("error") else 0
    parent_origin_cases = _safe_int(overview.get("parent_origin_cases")) if not overview.get("error") else 0
    open_case_rate = _safe_float(overview.get("open_case_rate_percent"), 0.0) if not overview.get("error") else 0.0
    followup_ratio = _safe_float(overview.get("followup_ratio_percent"), 0.0) if not overview.get("error") else 0.0

    motive_rows = motivos.get("motivos") or [] if not motivos.get("error") else []
    top_motives = sorted(motive_rows, key=lambda item: _safe_int(item.get("conteo")), reverse=True)[:12]
    area_rows = sorted(
        [{"area": area.get("area"), "conteo": _safe_int(area.get("total_conteo")), "sources": area.get("sources") or {}} for area in areas],
        key=lambda item: item["conteo"],
        reverse=True,
    )[:14]

    zero_data_risk = parent_interactions == 0
    risk_score = _clamp(
        (45.0 if zero_data_risk else 0.0)
        + open_case_rate * 0.28
        + complaints * 1.4
        + max(0.0, followup_ratio - 120.0) * 0.06
    )
    status = _status_from_index(100 - risk_score, zero_data_risk)
    return {
        "status": status,
        "parent_interactions": parent_interactions,
        "tickets_created": total_fichas,
        "followups": total_followups,
        "open_cases": open_cases,
        "closed_cases": closed_cases,
        "complaints": complaints,
        "parent_origin_cases": parent_origin_cases,
        "open_case_rate_percent": _round(open_case_rate, 2),
        "followup_ratio_percent": _round(followup_ratio, 2),
        "avg_resolution_hours": _round(_safe_float(overview.get("avg_resolution_hours"), 0.0), 2) if overview.get("avg_resolution_hours") is not None else None,
        "areas": area_rows,
        "top_motives": top_motives,
        "matched_campus_values": overview.get("matched_campus_values") or monthly.get("data_campuses") or [],
        "source_breakdown": monthly.get("source_breakdown") or {},
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
        _safe_call("sapf_overview", lambda: get_sapf_overview_report(plantel, start_date, end_date, scope)),
        _safe_call("observaciones", lambda: get_observaciones_report(plantel, start_date, end_date, scope)),
        _safe_call("planeaciones", lambda: get_planeaciones_report(plantel, start_date, end_date, scope)),
        _safe_call("observaciones_docentes", lambda: get_observaciones_docentes_report(plantel)),
        _safe_call("planeaciones_pendientes", lambda: get_planeaciones_pendientes_report(plantel, week_start, end_date, "range")),
    )

    attendance, husky, retardos, kardex, sapf_monthly, sapf_motivos, sapf_overview, observaciones, planeaciones, obs_docentes, plan_pendientes = results
    domains = {
        "attendance": _sum_daily_attendance(attendance, start_date, end_date),
        "husky": _sum_husky(husky, retardos, start_date, end_date),
        "employee": _sum_employee(kardex),
        "academic": _sum_academic(observaciones, planeaciones, obs_docentes, plan_pendientes),
        "sapf": _sum_sapf(sapf_monthly, sapf_motivos, sapf_overview),
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
        "absent_students": sum(_safe_int(p["domains"]["attendance"].get("absent_students_count")) for p in planteles),
        "attendance_no_modality_records": sum(_safe_int(p["domains"]["attendance"].get("no_modality_records")) for p in planteles),
        "repeated_missing_groups": sum(len(p["domains"]["attendance"].get("repeated_missing_groups") or []) for p in planteles),
        "employee_incidents": sum(_safe_int(p["domains"]["employee"].get("employee_incidents")) for p in planteles),
        "employee_absences": sum(_safe_int(p["domains"]["employee"].get("employee_absences")) for p in planteles),
        "employee_tardies": sum(_safe_int(p["domains"]["employee"].get("employee_tardies")) for p in planteles),
        "payroll_waste_minutes": sum(_safe_int(p["domains"]["employee"].get("payroll_waste_minutes")) for p in planteles),
        "security_scan_gap": sum(_safe_int(p["domains"]["husky"].get("scan_gap")) for p in planteles),
        "student_tardies": sum(_safe_int(p["domains"]["husky"].get("student_tardies")) for p in planteles),
        "repeat_tardy_students": sum(len(p["domains"]["husky"].get("repeat_tardy_students") or []) for p in planteles),
        "academic_backlog": sum(_safe_int(p["domains"]["academic"].get("supervision_backlog")) for p in planteles),
        "pending_lesson_reviews": sum(_safe_int(p["domains"]["academic"].get("planeaciones_pendientes")) for p in planteles),
        "teachers_without_observation": sum(_safe_int(p["domains"]["academic"].get("docentes_sin_observacion_30_dias")) for p in planteles),
        "sapf_parent_interactions": sum(_safe_int(p["domains"]["sapf"].get("parent_interactions")) for p in planteles),
        "sapf_tickets_created": sum(_safe_int(p["domains"]["sapf"].get("tickets_created")) for p in planteles),
        "sapf_followups": sum(_safe_int(p["domains"]["sapf"].get("followups")) for p in planteles),
        "sapf_open_cases": sum(_safe_int(p["domains"]["sapf"].get("open_cases")) for p in planteles),
        "sapf_complaints": sum(_safe_int(p["domains"]["sapf"].get("complaints")) for p in planteles),
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
