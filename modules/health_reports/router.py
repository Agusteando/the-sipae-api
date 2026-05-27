import base64
import hashlib
import os
import secrets
from datetime import date
from typing import Optional
from urllib.parse import unquote

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from core.config import settings
from core.logger import get_logger
from core.constants import ACTIVE_PLANTEL_CODES

from .repository import fetch_principal_report_recipients, get_message, get_message_by_click_token, get_message_by_open_token, list_messages, list_runs, record_event
from .service import render_preview, run_daily_health_reports, send_test_report, sync_read_status, today_mx
from core.scheduler import scheduler_status, update_health_reports_schedule
from .templates import HEALTH_REPORTS_UI_HTML

router = APIRouter(tags=["Health Reports"])
logger = get_logger("health_reports.router")


def _clean_token(value: Optional[str]) -> str:
    token = str(value or "").strip()
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    return token.strip('"').strip("'").strip()


def _configured_admin_token() -> str:
    # Prefer the raw process environment so PM2/deployment secrets cannot be
    # shadowed by a stale parsed settings instance. Fall back to pydantic's
    # .env-loaded setting for local development.
    return _clean_token(os.getenv("HEALTH_REPORTS_ADMIN_TOKEN") or settings.health_reports_admin_token)


def _request_admin_token(request: Request, token: Optional[str] = None) -> str:
    header_value = token or request.headers.get("x-health-reports-admin-token") or request.headers.get("authorization")
    return _clean_token(header_value)


def _require_admin(request: Request, token: Optional[str] = None) -> None:
    # Health Reports admin token checks were intentionally disabled.
    # The dashboard runs as an internal operational console behind the API host.
    return None


def _parse_date(value: Optional[str]) -> date:
    if not value:
        return today_mx()
    try:
        return date.fromisoformat(value)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid date. Use YYYY-MM-DD.")


def _normalize_plantel(value: str) -> str:
    plantel = str(value or "").strip().upper()
    if plantel not in ACTIVE_PLANTEL_CODES:
        raise HTTPException(status_code=400, detail="Invalid plantel.")
    return plantel


def _ip_hash(request: Request) -> str:
    raw = request.headers.get("x-forwarded-for") or (request.client.host if request.client else "")
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:64]


def _safe_error_detail(action: str, exc: Exception) -> dict:
    return {
        "message": f"{action} failed after request validation.",
        "error": str(exc),
        "type": exc.__class__.__name__,
    }

def _raise_post_auth_error(action: str, exc: Exception) -> None:
    if isinstance(exc, HTTPException):
        raise exc
    logger.exception("%s failed after request validation", action)
    raise HTTPException(status_code=500, detail=_safe_error_detail(action, exc))


@router.get("/health-reports", response_class=HTMLResponse, include_in_schema=False)
async def health_reports_ui():
    return HTMLResponse(
        HEALTH_REPORTS_UI_HTML,
        headers={"Cache-Control": "no-store, no-cache, max-age=0, must-revalidate"},
    )


@router.get("/api/v1/health-reports/auth-status")
async def health_reports_auth_status(
    request: Request,
    x_health_reports_admin_token: Optional[str] = Header(None),
):
    expected = _configured_admin_token()
    received = _request_admin_token(request, x_health_reports_admin_token)
    return {
        "configured": bool(expected),
        "valid": True,
        "required": False,
        "received": bool(received),
        "source": "disabled",
        "expected_length": len(expected),
        "received_length": len(received),
    }


@router.get("/api/v1/health-reports/config-status")
async def health_reports_config_status(
    request: Request,
    x_health_reports_admin_token: Optional[str] = Header(None),
):
    return {
        "sipae_db_configured": bool(settings.db_sipae_host and settings.db_sipae_user and settings.db_sipae_name),
        "gmail_sender_configured": bool(settings.health_reports_gmail_sender),
        "google_service_account_configured": bool(settings.google_service_account_email and settings.google_private_key),
        "public_base_url_configured": bool(settings.health_reports_public_base_url),
        "test_recipient_configured": bool(settings.health_reports_test_recipient),
    }


@router.get("/api/v1/health-reports/recipients")
async def health_report_recipients(
    request: Request,
    plantel: Optional[str] = Query(None),
):
    try:
        plantel_code = _normalize_plantel(plantel) if plantel else None
        rows = await fetch_principal_report_recipients()
        rows = [row for row in rows if row.get("plantel_code") in ACTIVE_PLANTEL_CODES]
        if plantel_code:
            rows = [row for row in rows if row.get("plantel_code") == plantel_code]
        return {"recipients": rows}
    except Exception as exc:
        _raise_post_auth_error("recipients", exc)


@router.get("/api/v1/health-reports/schedule")
async def get_health_reports_schedule(request: Request):
    return scheduler_status()


@router.post("/api/v1/health-reports/schedule")
async def update_health_reports_schedule_endpoint(
    request: Request,
    payload: dict = Body(default={}),
):
    try:
        return update_health_reports_schedule(payload)
    except Exception as exc:
        _raise_post_auth_error("schedule", exc)


@router.get("/api/v1/health-reports/preview")
async def preview_health_report(
    request: Request,
    plantel: str = Query(...),
    date_value: Optional[str] = Query(None, alias="date"),
    x_health_reports_admin_token: Optional[str] = Header(None),
):
    try:
        plantel_code = _normalize_plantel(plantel)
        report_date = _parse_date(date_value)
        result = await render_preview(plantel_code, report_date)
        model = result["model"]
        return {
            "plantel": plantel_code,
            "date": str(report_date),
            "to": result["to"],
            "cc": result["cc"],
            "subject": model["subject"],
            "severity": model["severity"],
            "worst_metric": model["worst_metric"],
            "model": model,
            "html": result["html"],
            "resolver_error": result.get("resolver_error"),
        }
    except Exception as exc:
        _raise_post_auth_error("preview", exc)


@router.post("/api/v1/health-reports/send-test")
async def send_test_health_report(
    request: Request,
    payload: dict = Body(...),
    x_health_reports_admin_token: Optional[str] = Header(None),
):
    try:
        plantel_code = _normalize_plantel(payload.get("plantel"))
        report_date = _parse_date(payload.get("date"))
        test_email = str(payload.get("test_email") or settings.health_reports_test_recipient or "").strip().lower()
        if not test_email or "@" not in test_email:
            raise HTTPException(status_code=400, detail="Provide test_email or HEALTH_REPORTS_TEST_RECIPIENT.")
        result = await send_test_report(plantel_code, report_date, test_email)
        message = result["message"]
        return {
            "message_id": message.get("id"),
            "status": message.get("status"),
            "error": message.get("error"),
            "resolver_error": result.get("resolver_error"),
            "subject": result["model"]["subject"],
            "html": result["html"],
        }
    except Exception as exc:
        _raise_post_auth_error("send-test", exc)


@router.post("/api/v1/health-reports/send-now")
async def send_now_health_reports(
    request: Request,
    payload: dict = Body(default={}),
    x_health_reports_admin_token: Optional[str] = Header(None),
):
    try:
        report_date = _parse_date(payload.get("date"))
        plantel = payload.get("plantel")
        plantel_code = _normalize_plantel(plantel) if plantel else None
        send = bool(payload.get("send", True))
        return await run_daily_health_reports(report_date=report_date, send=send, plantel=plantel_code)
    except Exception as exc:
        _raise_post_auth_error("send-now", exc)


@router.post("/api/v1/health-reports/sync-read-status")
async def sync_health_report_read_status(
    request: Request,
    payload: dict = Body(default={}),
    x_health_reports_admin_token: Optional[str] = Header(None),
):
    try:
        limit = int(payload.get("limit") or 100)
        return await sync_read_status(limit=limit)
    except Exception as exc:
        _raise_post_auth_error("sync-read-status", exc)


@router.get("/api/v1/health-reports/runs")
async def health_report_runs(
    request: Request,
    limit: int = Query(30, ge=1, le=200),
    x_health_reports_admin_token: Optional[str] = Header(None),
):
    rows = await list_runs(limit=limit)
    return {"runs": rows}


@router.get("/api/v1/health-reports/messages")
async def health_report_messages(
    request: Request,
    limit: int = Query(100, ge=1, le=300),
    date_value: Optional[str] = Query(None, alias="date"),
    plantel: Optional[str] = Query(None),
    x_health_reports_admin_token: Optional[str] = Header(None),
):
    report_date = _parse_date(date_value) if date_value else None
    plantel_code = _normalize_plantel(plantel) if plantel else None
    rows = await list_messages(limit=limit, report_date=report_date, plantel=plantel_code)
    messages = []
    for row in rows:
        messages.append({
            "id": row.get("id"),
            "date": str(row.get("report_date")),
            "plantel": row.get("plantel_code"),
            "recipient": row.get("principal_email"),
            "manager": row.get("manager_email"),
            "subject": row.get("subject"),
            "status": row.get("status"),
            "severity": row.get("severity"),
            "worst_metric": row.get("worst_metric"),
            "open_count": row.get("open_count"),
            "click_count": row.get("click_count"),
            "sent_at": row.get("sent_at"),
            "first_opened_at": row.get("first_opened_at"),
            "first_clicked_at": row.get("first_clicked_at"),
            "recipient_statuses": row.get("recipient_statuses"),
        })
    return {"messages": messages}


@router.get("/api/v1/health-reports/messages/{message_id}/html")
async def health_report_message_html(
    request: Request,
    message_id: int,
    x_health_reports_admin_token: Optional[str] = Header(None),
):
    message = await get_message(message_id)
    if not message:
        raise HTTPException(status_code=404, detail="Message not found.")
    return {
        "id": message.get("id"),
        "subject": message.get("subject"),
        "html": message.get("html_body"),
        "meta": {
            "date": str(message.get("report_date")),
            "plantel": message.get("plantel_code"),
            "recipient": message.get("principal_email"),
            "manager": message.get("manager_email"),
            "cc": message.get("cc_emails"),
            "status": message.get("status"),
            "severity": message.get("severity"),
            "worst_metric": message.get("worst_metric"),
            "open_count": message.get("open_count"),
            "click_count": message.get("click_count"),
            "error": message.get("error"),
        },
    }


@router.get("/api/v1/health-reports/events/open/{token}.png", include_in_schema=False)
async def health_report_open_event(token: str, request: Request):
    message = await get_message_by_open_token(token)
    if message:
        await record_event(message["id"], "open", _ip_hash(request), request.headers.get("user-agent"))
    # 1x1 transparent PNG
    png = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/p9sAAAAASUVORK5CYII=")
    return Response(content=png, media_type="image/png", headers={"Cache-Control": "no-store, no-cache, max-age=0"})


@router.get("/api/v1/health-reports/events/click/{token}", include_in_schema=False)
async def health_report_click_event(token: str, request: Request, url: Optional[str] = Query(None)):
    message = await get_message_by_click_token(token)
    target = unquote(url or "https://sipae.casitaapps.com")
    if message:
        await record_event(message["id"], "click", _ip_hash(request), request.headers.get("user-agent"), target)
    return RedirectResponse(url=target)
