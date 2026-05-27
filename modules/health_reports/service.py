import json
from datetime import date, datetime
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from core.config import settings
from core.constants import ACTIVE_PLANTEL_CODES
from core.logger import get_logger
from core.utils import resolve_plantel

from .collector import collect_plantel_health
from .gmail import lookup_recipient_read_status, send_html_email
from .repository import (
    create_run,
    fetch_principal_report_recipients,
    find_unread_prior_report,
    finish_run,
    mark_message_failed,
    mark_message_sent,
    recipients_for_read_sync,
    save_message,
    update_message_html,
    update_recipient_gmail_status,
)
from .templates import render_report_html

logger = get_logger("health_reports.service")

STATUS_ORDER = {"fulfilled": 0, "warning": 1, "critical": 2}
PRIORITY = {
    "attendance": 100,
    "observaciones": 90,
    "planeaciones": 80,
    "kardex": 70,
    "retardos": 60,
    "husky": 50,
    "read_status": 40,
}


def mx_now() -> datetime:
    return datetime.now(ZoneInfo(settings.health_reports_timezone or "America/Mexico_City"))


def today_mx() -> date:
    return mx_now().date()


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value or default)
    except Exception:
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value or default)
    except Exception:
        return default


def _status_from_count(count: int, critical_threshold: int = 1) -> str:
    if count >= critical_threshold:
        return "critical"
    if count > 0:
        return "warning"
    return "fulfilled"


def _dashboard_url(plantel_code: str) -> str:
    return f"https://sipae.casitaapps.com/?healthPanel=1&plantel={plantel_code}"


def _date_label(value: date) -> str:
    months = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"]
    return f"{value.day} de {months[value.month - 1]}"


def _top_card(cards: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not cards:
        return {"key": "none", "status": "fulfilled", "title": "Indicadores revisados", "summary": "Sin datos."}
    return sorted(cards, key=lambda c: (STATUS_ORDER.get(c.get("status"), 0), PRIORITY.get(c.get("key"), 0), c.get("count") or 0), reverse=True)[0]


def _build_attendance_card(data: Dict[str, Any]) -> Dict[str, Any]:
    if data.get("error"):
        return {"key": "attendance", "title": "Asistencia", "status": "warning", "headline": "Sin lectura completa", "summary": data.get("error"), "count": 0, "details": []}
    missing = data.get("missing_groups_data") or {}
    missing_count = _safe_int(missing.get("missing_groups_count"))
    affected = _safe_int(missing.get("expected_students_count"))
    expected = _safe_int(missing.get("expected_groups_count"))
    completed = _safe_int(missing.get("completed_groups_count"))
    status = "critical" if missing_count > 0 else "fulfilled"
    groups = []
    for group in missing.get("missing_groups") or []:
        groups.append({
            "label": f"{group.get('grado')} {group.get('grupo')}",
            "sub": "Grupo sin pase de lista al cierre",
            "value": f"{_safe_int(group.get('expected_students'))} alumnos",
        })
    if status == "critical":
        summary = f"{affected} estudiantes perdieron continuidad de expediente de asistencia hoy. Cobertura: {completed}/{expected} grupos."
        headline = f"{missing_count} grupos sin asistencia"
    else:
        summary = f"Todos los grupos esperados tienen pase de lista capturado. Cobertura: {completed}/{expected} grupos."
        headline = "Cobertura completa"
    return {"key": "attendance", "title": "Asistencia", "status": status, "headline": headline, "summary": summary, "count": affected if missing_count else completed, "details": groups}


def _build_husky_card(data: Dict[str, Any], report_date: date) -> Dict[str, Any]:
    if data.get("error"):
        return {"key": "husky", "title": "Escaneos", "status": "warning", "headline": "Sin lectura completa", "summary": data.get("error"), "count": 0, "details": []}
    point = (data.get("daily_datapoints") or {}).get(str(report_date), {})
    entrada = _safe_int(point.get("entrada"))
    salida = _safe_int(point.get("salida"))
    expected = _safe_int(data.get("expected_population"))
    rate = _safe_float(point.get("rate_entrada_percent"))
    status = "warning" if entrada > 0 and rate < 75 else "fulfilled"
    return {
        "key": "husky",
        "title": "Escaneos",
        "status": status,
        "headline": f"{rate:.1f}% de captura",
        "summary": f"Se registraron {entrada} entradas de {expected} alumnos esperados y {salida} salidas.",
        "count": entrada,
        "details": [{"label": "Entradas", "value": entrada}, {"label": "Salidas", "value": salida}],
    }


def _build_retardos_card(data: Dict[str, Any]) -> Dict[str, Any]:
    if data.get("error"):
        return {"key": "retardos", "title": "Retardos", "status": "warning", "headline": "Sin lectura completa", "summary": data.get("error"), "count": 0, "details": []}
    total = _safe_int(data.get("total_retardos"))
    status = "warning" if total > 10 else "fulfilled"
    details = [{"label": r.get("student_fullname"), "sub": r.get("matricula"), "value": r.get("time")} for r in (data.get("retardos") or [])[:5]]
    return {"key": "retardos", "title": "Retardos", "status": status, "headline": f"{total} alumnos", "summary": "Retardos registrados después del umbral operativo del plantel.", "count": total, "details": details}


def _build_kardex_card(data: Dict[str, Any]) -> Dict[str, Any]:
    if data.get("error"):
        return {"key": "kardex", "title": "Plantilla", "status": "warning", "headline": "Sin lectura completa", "summary": data.get("error"), "count": 0, "details": []}
    summary = data.get("summary") or {}
    faltas = _safe_int(summary.get("ausencias_count"))
    retardos = _safe_int(summary.get("retardos_count"))
    total = faltas + retardos
    status = "warning" if total > 0 else "fulfilled"
    details = []
    for item in (data.get("ausencias") or [])[:3]:
        details.append({"label": item.get("employee_name"), "sub": item.get("employee_id"), "value": item.get("raw_status") or "Falta"})
    for item in (data.get("retardos") or [])[:3]:
        details.append({"label": item.get("employee_name"), "sub": item.get("employee_id"), "value": item.get("raw_status") or "Retardo"})
    return {"key": "kardex", "title": "Plantilla", "status": status, "headline": f"{faltas} faltas · {retardos} retardos", "summary": "Incidencias de personal detectadas al cierre del día.", "count": total, "details": details[:6]}


def _build_observaciones_card(data: Dict[str, Any]) -> Dict[str, Any]:
    if data.get("error"):
        return {"key": "observaciones", "title": "Observaciones", "status": "warning", "headline": "Sin lectura completa", "summary": data.get("error"), "count": 0, "details": []}
    summary = data.get("summary") or {}
    total_active = _safe_int(summary.get("total_docentes_activos"))
    observed = _safe_int(summary.get("total_docentes_observados"))
    pending = _safe_int(summary.get("total_docentes_sin_observacion_30_dias"))
    status = "critical" if pending > 0 else "fulfilled"
    details = []
    for teacher in (data.get("docentes_sin_observacion") or [])[:6]:
        days = teacher.get("days_since_last_observation")
        details.append({
            "label": teacher.get("docente"),
            "sub": teacher.get("nivel") or teacher.get("campus") or "Docente activo",
            "value": "Sin ciclo" if days is None else f"{days} días",
        })
    return {"key": "observaciones", "title": "Observaciones", "status": status, "headline": f"{pending} docentes requieren observación", "summary": f"Docentes activos por planeaciones en últimos 21 días: {total_active}. Observados en 30 días: {observed}.", "count": pending, "details": details}


def _build_planeaciones_card(data: Dict[str, Any]) -> Dict[str, Any]:
    if data.get("error"):
        return {"key": "planeaciones", "title": "Planeaciones", "status": "warning", "headline": "Sin lectura completa", "summary": data.get("error"), "count": 0, "details": []}
    summary = data.get("summary") or {}
    total = _safe_int(summary.get("total_planeaciones_pendientes"))
    teachers = _safe_int(summary.get("docentes_con_planeaciones_pendientes"))
    active = _safe_int(summary.get("docentes_activos"))
    status = "critical" if total > 0 else "fulfilled"
    grouped: Dict[str, Dict[str, Any]] = {}
    for p in data.get("planeaciones_pendientes") or []:
        name = str(p.get("docente") or "").strip()
        if not name:
            continue
        row = grouped.setdefault(name, {"count": 0, "nivel": p.get("nivel") or p.get("campus")})
        row["count"] += 1
    details = [{"label": name, "sub": row.get("nivel") or "Docente activo", "value": f"{row['count']} sin revisar"} for name, row in sorted(grouped.items(), key=lambda x: (-x[1]["count"], x[0]))[:6]]
    return {"key": "planeaciones", "title": "Planeaciones", "status": status, "headline": f"{total} sin revisar", "summary": f"Esta semana {teachers} docentes activos tienen planeaciones sin firma ni feedback de revisión. Total docentes activos: {active}.", "count": total, "details": details}


def _build_read_status_card(prior: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not prior:
        return None
    report_date = prior.get("report_date")
    title_date = str(report_date)
    if hasattr(report_date, "strftime"):
        title_date = _date_label(report_date)
    return {
        "key": "read_status",
        "title": "Lectura de reportes",
        "status": "warning",
        "headline": f"No leíste el reporte del {title_date}",
        "summary": "No hay señal de apertura, click ni lectura confirmada del reporte anterior. Este indicador se actualiza con tracking del correo y verificación de Gmail cuando está disponible.",
        "count": 1,
        "details": [{"label": prior.get("subject"), "sub": f"Reporte {prior.get('report_date')}", "value": "Sin lectura"}],
    }


def _subject_for(plantel_code: str, card: Dict[str, Any]) -> str:
    key = card.get("key")
    count = card.get("count") or 0
    if key == "attendance" and card.get("status") == "critical":
        return f"{plantel_code}: {count} estudiantes perdieron continuidad de asistencia hoy"
    if key == "observaciones" and card.get("status") == "critical":
        return f"{plantel_code}: {count} docentes activos sin observación reciente"
    if key == "planeaciones" and card.get("status") == "critical":
        return f"{plantel_code}: {count} planeaciones sin revisar esta semana"
    if key == "kardex" and card.get("status") != "fulfilled":
        return f"{plantel_code}: incidencias de plantilla al cierre"
    if key == "retardos" and card.get("status") != "fulfilled":
        return f"{plantel_code}: {count} retardos detectados hoy"
    if key == "husky" and card.get("status") != "fulfilled":
        return f"{plantel_code}: tasa de escaneo baja al cierre"
    return f"{plantel_code}: cierre SIPAE en verde"


async def build_report_model(plantel_code: str, report_date: date, principal_email: str, manager_email: Optional[str] = None, cc_emails: Optional[List[str]] = None) -> Dict[str, Any]:
    plantel_info = resolve_plantel(plantel_code)
    collected = await collect_plantel_health(plantel_code, report_date)
    prior_unread = await find_unread_prior_report(principal_email, plantel_code, report_date)

    cards = [
        _build_attendance_card(collected["attendance"]),
        _build_husky_card(collected["husky"], report_date),
        _build_retardos_card(collected["retardos"]),
        _build_kardex_card(collected["kardex"]),
        _build_observaciones_card(collected["observaciones"]),
        _build_planeaciones_card(collected["planeaciones"]),
    ]
    read_card = _build_read_status_card(prior_unread)
    if read_card:
        cards.insert(0, read_card)

    top = _top_card(cards)
    overall = "critical" if any(c["status"] == "critical" for c in cards) else "warning" if any(c["status"] == "warning" for c in cards) else "fulfilled"
    subject = _subject_for(plantel_code, top)
    return {
        "plantel_code": plantel_code,
        "resolved_name": plantel_info["resolved_name"],
        "report_date": str(report_date),
        "generated_at": mx_now().strftime("%Y-%m-%d %H:%M"),
        "principal_email": principal_email,
        "manager_email": manager_email,
        "cc_emails": cc_emails or [],
        "subject": subject,
        "preheader": top.get("headline") or subject,
        "overall_status": overall,
        "top_insight": {"title": top.get("headline") or top.get("title"), "body": top.get("summary")},
        "cards": cards,
        "worst_metric": top.get("key"),
        "severity": overall,
        "dashboard_url": _dashboard_url(plantel_code),
    }


def _public_url(path: str) -> str:
    base = (settings.health_reports_public_base_url or "").rstrip("/")
    return f"{base}{path}" if base else ""


async def render_preview(plantel_code: str, report_date: date) -> Dict[str, Any]:
    recipients = await fetch_principal_report_recipients()
    record = next((r for r in recipients if r["plantel_code"] == plantel_code), None) or {
        "principal_email": settings.health_reports_test_recipient or "preview@casitaiedis.edu.mx",
        "manager_email": None,
        "cc_emails": [],
    }
    model = await build_report_model(plantel_code, report_date, record["principal_email"], record.get("manager_email"), record.get("cc_emails") or [])
    html = render_report_html(model)
    return {"model": model, "html": html, "to": record["principal_email"], "cc": record.get("cc_emails") or []}


async def create_and_optionally_send(
    *,
    run_id: Optional[int],
    report_date: date,
    plantel_code: str,
    principal_email: str,
    manager_email: Optional[str],
    cc_emails: List[str],
    send: bool,
    test_email: Optional[str] = None,
    include_cc: bool = True,
) -> Dict[str, Any]:
    actual_to = test_email or principal_email
    actual_cc = cc_emails if include_cc and not test_email else []
    model = await build_report_model(plantel_code, report_date, principal_email, manager_email, cc_emails)
    html_without_tracking = render_report_html(model)
    message = await save_message(
        run_id=run_id,
        report_date=report_date,
        plantel_code=plantel_code,
        resolved_name=model["resolved_name"],
        principal_email=actual_to,
        manager_email=manager_email if not test_email else None,
        cc_emails=actual_cc,
        subject=model["subject"],
        html_body=html_without_tracking,
        text_summary=model["top_insight"].get("body") or model["subject"],
        model=model,
        worst_metric=model["worst_metric"],
        severity=model["severity"],
    )
    open_url = _public_url(f"/api/v1/health-reports/events/open/{message['open_token']}.png")
    click_url = _public_url(f"/api/v1/health-reports/events/click/{message['click_token']}?url={_dashboard_url(plantel_code)}") or _dashboard_url(plantel_code)
    html = render_report_html(model, open_url=open_url, click_url=click_url)

    # Store the final tracked HTML body.
    await update_message_html(message["id"], html)
    message["html_body"] = html

    if send:
        try:
            result = send_html_email(
                to_email=actual_to,
                cc_emails=actual_cc,
                subject=model["subject"],
                html_body=html,
                text_body=model["top_insight"].get("body") or model["subject"],
                rfc_message_id=message["rfc_message_id"],
            )
            await mark_message_sent(message["id"], result.get("gmail_message_id"), result.get("gmail_thread_id"))
            message["status"] = "sent"
            message.update(result)
        except Exception as exc:
            await mark_message_failed(message["id"], str(exc))
            message["status"] = "failed"
            message["error"] = str(exc)
    return {"message": message, "model": model, "html": html}


async def send_test_report(plantel_code: str, report_date: date, test_email: str) -> Dict[str, Any]:
    recipients = await fetch_principal_report_recipients()
    record = next((r for r in recipients if r["plantel_code"] == plantel_code), None) or {}
    return await create_and_optionally_send(
        run_id=None,
        report_date=report_date,
        plantel_code=plantel_code,
        principal_email=record.get("principal_email") or test_email,
        manager_email=record.get("manager_email"),
        cc_emails=record.get("cc_emails") or [],
        send=True,
        test_email=test_email,
        include_cc=False,
    )


async def run_daily_health_reports(report_date: Optional[date] = None, send: bool = True, plantel: Optional[str] = None) -> Dict[str, Any]:
    target_date = report_date or today_mx()
    recipients = await fetch_principal_report_recipients()
    if plantel:
        recipients = [r for r in recipients if r["plantel_code"] == plantel]
    recipients = [r for r in recipients if r["plantel_code"] in ACTIVE_PLANTEL_CODES]
    run_id = await create_run("manual" if plantel else "scheduled", target_date, len(recipients))
    sent = failed = generated = 0
    try:
        for record in recipients:
            result = await create_and_optionally_send(
                run_id=run_id,
                report_date=target_date,
                plantel_code=record["plantel_code"],
                principal_email=record["principal_email"],
                manager_email=record.get("manager_email"),
                cc_emails=record.get("cc_emails") or [],
                send=send,
                include_cc=True,
            )
            generated += 1
            if result["message"].get("status") == "sent":
                sent += 1
            if result["message"].get("status") == "failed":
                failed += 1
        await finish_run(run_id, "completed" if failed == 0 else "completed_with_errors")
        return {"run_id": run_id, "generated": generated, "sent": sent, "failed": failed}
    except Exception as exc:
        await finish_run(run_id, "failed", str(exc))
        raise


async def sync_read_status(limit: int = 100) -> Dict[str, int]:
    rows = await recipients_for_read_sync(limit)
    checked = updated = 0
    for row in rows:
        checked += 1
        try:
            result = lookup_recipient_read_status(recipient_email=row["recipient_email"], rfc_message_id=row["rfc_message_id"])
            await update_recipient_gmail_status(row["id"], bool(result.get("found")), result.get("unread"))
            updated += 1
        except Exception as exc:
            logger.warning("No se pudo sincronizar lectura para %s: %s", row.get("recipient_email"), exc)
    return {"checked": checked, "updated": updated}
