import json
import secrets
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import aiomysql

from core.database import get_sipae_db_connection
from .recipient_resolver import build_principal_records


async def ensure_health_report_tables() -> None:
    """Runtime schema creation is intentionally disabled.

    The SIPAE DB user may not have CREATE/ALTER permissions in production.
    Schema changes are provided outside the code and must be executed manually
    by the operator before enabling persistent logs/tracking.
    """
    return None


async def fetch_principal_report_recipients() -> List[Dict[str, Any]]:
    conn = await get_sipae_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("""
                SELECT
                    d.id,
                    d.email,
                    d.dir,
                    d.label,
                    d.coord,
                    c.coord AS coord_name,
                    c.manager_email
                FROM dirs d
                LEFT JOIN coords c ON d.coord = c.id
                WHERE d.email IS NOT NULL
                  AND TRIM(d.email) <> ''
                  AND d.label IS NOT NULL
                ORDER BY d.label ASC, d.email ASC
            """)
            rows = await cur.fetchall()
            return build_principal_records(rows)
    finally:
        conn.close()


async def create_run(run_type: str, report_date: date, plantels_total: int = 0) -> int:
    await ensure_health_report_tables()
    conn = await get_sipae_db_connection()
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO health_report_runs (run_type, report_date, started_at, status, plantels_total)
                VALUES (%s, %s, %s, 'running', %s)
                """,
                (run_type, report_date, datetime.utcnow(), plantels_total),
            )
            return int(cur.lastrowid)
    finally:
        conn.close()


async def finish_run(run_id: int, status: str, error: Optional[str] = None) -> None:
    conn = await get_sipae_db_connection()
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE health_report_runs r
                LEFT JOIN (
                    SELECT
                        run_id,
                        COUNT(*) AS generated,
                        SUM(CASE WHEN status = 'sent' THEN 1 ELSE 0 END) AS sent,
                        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed
                    FROM health_report_messages
                    WHERE run_id = %s
                    GROUP BY run_id
                ) m ON m.run_id = r.id
                SET r.finished_at = %s,
                    r.status = %s,
                    r.error = %s,
                    r.messages_generated = COALESCE(m.generated, 0),
                    r.messages_sent = COALESCE(m.sent, 0),
                    r.messages_failed = COALESCE(m.failed, 0)
                WHERE r.id = %s
                """,
                (run_id, datetime.utcnow(), status, error, run_id),
            )
    finally:
        conn.close()


async def save_message(
    *,
    run_id: Optional[int],
    report_date: date,
    plantel_code: str,
    resolved_name: str,
    principal_email: str,
    manager_email: Optional[str],
    cc_emails: List[str],
    subject: str,
    html_body: str,
    text_summary: str,
    model: Dict[str, Any],
    worst_metric: str,
    severity: str,
    status: str = "generated",
) -> Dict[str, Any]:
    await ensure_health_report_tables()
    rfc_message_id = f"<sipae-health-{secrets.token_hex(16)}@the-sipae-api.casitaapps.com>"
    open_token = secrets.token_urlsafe(32)
    click_token = secrets.token_urlsafe(32)
    conn = await get_sipae_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                """
                INSERT INTO health_report_messages (
                    run_id, report_date, plantel_code, resolved_name, principal_email,
                    manager_email, cc_emails, subject, rfc_message_id, html_body,
                    text_summary, model_json, worst_metric, severity, status, open_token, click_token
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    run_id,
                    report_date,
                    plantel_code,
                    resolved_name,
                    principal_email,
                    manager_email,
                    json.dumps(cc_emails, ensure_ascii=False),
                    subject,
                    rfc_message_id,
                    html_body,
                    text_summary,
                    json.dumps(model, ensure_ascii=False, default=str),
                    worst_metric,
                    severity,
                    status,
                    open_token,
                    click_token,
                ),
            )
            message_id = int(cur.lastrowid)
            recipients = [(principal_email, "principal")]
            for cc in cc_emails:
                recipients.append((cc, "manager" if cc == manager_email else "cc"))
            for email, role in recipients:
                await cur.execute(
                    """
                    INSERT IGNORE INTO health_report_recipient_statuses (message_id, recipient_email, recipient_role)
                    VALUES (%s, %s, %s)
                    """,
                    (message_id, email, role),
                )
            await cur.execute("SELECT * FROM health_report_messages WHERE id = %s", (message_id,))
            return await cur.fetchone()
    finally:
        conn.close()


async def mark_message_sent(message_id: int, gmail_message_id: Optional[str], gmail_thread_id: Optional[str]) -> None:
    conn = await get_sipae_db_connection()
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE health_report_messages
                SET status = 'sent', sent_at = %s, gmail_message_id = %s, gmail_thread_id = %s, error = NULL
                WHERE id = %s
                """,
                (datetime.utcnow(), gmail_message_id, gmail_thread_id, message_id),
            )
            await cur.execute(
                """
                UPDATE health_report_recipient_statuses
                SET delivery_status = 'sent'
                WHERE message_id = %s
                """,
                (message_id,),
            )
    finally:
        conn.close()


async def mark_message_failed(message_id: int, error: str) -> None:
    conn = await get_sipae_db_connection()
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE health_report_messages
                SET status = 'failed', failed_at = %s, error = %s
                WHERE id = %s
                """,
                (datetime.utcnow(), error[:5000], message_id),
            )
    finally:
        conn.close()


async def find_unread_prior_report(recipient_email: str, plantel_code: str, before_date: date) -> Optional[Dict[str, Any]]:
    await ensure_health_report_tables()
    conn = await get_sipae_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                """
                SELECT m.id, m.report_date, m.subject, m.plantel_code, r.gmail_unread,
                       r.gmail_read_at, r.opened_at, r.clicked_at
                FROM health_report_recipient_statuses r
                JOIN health_report_messages m ON m.id = r.message_id
                WHERE r.recipient_email = %s
                  AND m.plantel_code = %s
                  AND m.report_date < %s
                  AND m.status = 'sent'
                  AND r.clicked_at IS NULL
                  AND r.opened_at IS NULL
                  AND r.gmail_read_at IS NULL
                ORDER BY m.report_date DESC, m.id DESC
                LIMIT 1
                """,
                (recipient_email, plantel_code, before_date),
            )
            return await cur.fetchone()
    finally:
        conn.close()


async def list_runs(limit: int = 30) -> List[Dict[str, Any]]:
    await ensure_health_report_tables()
    conn = await get_sipae_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT * FROM health_report_runs ORDER BY id DESC LIMIT %s", (int(limit),))
            return await cur.fetchall()
    finally:
        conn.close()


async def list_messages(limit: int = 100, report_date: Optional[date] = None, plantel: Optional[str] = None) -> List[Dict[str, Any]]:
    await ensure_health_report_tables()
    clauses = []
    params: List[Any] = []
    if report_date:
        clauses.append("m.report_date = %s")
        params.append(report_date)
    if plantel:
        clauses.append("m.plantel_code = %s")
        params.append(plantel)
    where = "WHERE " + " AND ".join(clauses) if clauses else ""
    conn = await get_sipae_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                f"""
                SELECT m.*,
                       GROUP_CONCAT(CONCAT(r.recipient_email, '|', r.recipient_role, '|', COALESCE(r.delivery_status,''), '|', COALESCE(r.gmail_unread,''), '|', COALESCE(r.opened_at,''), '|', COALESCE(r.clicked_at,'')) SEPARATOR '|||') AS recipient_statuses
                FROM health_report_messages m
                LEFT JOIN health_report_recipient_statuses r ON r.message_id = m.id
                {where}
                GROUP BY m.id
                ORDER BY m.id DESC
                LIMIT %s
                """,
                (*params, int(limit)),
            )
            return await cur.fetchall()
    finally:
        conn.close()


async def get_message(message_id: int) -> Optional[Dict[str, Any]]:
    await ensure_health_report_tables()
    conn = await get_sipae_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT * FROM health_report_messages WHERE id = %s", (message_id,))
            return await cur.fetchone()
    finally:
        conn.close()


async def get_message_by_open_token(token: str) -> Optional[Dict[str, Any]]:
    await ensure_health_report_tables()
    conn = await get_sipae_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT * FROM health_report_messages WHERE open_token = %s", (token,))
            return await cur.fetchone()
    finally:
        conn.close()


async def get_message_by_click_token(token: str) -> Optional[Dict[str, Any]]:
    await ensure_health_report_tables()
    conn = await get_sipae_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT * FROM health_report_messages WHERE click_token = %s", (token,))
            return await cur.fetchone()
    finally:
        conn.close()


async def record_event(message_id: int, event_type: str, ip_hash: Optional[str] = None, user_agent: Optional[str] = None, target_url: Optional[str] = None) -> None:
    conn = await get_sipae_db_connection()
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO health_report_events (message_id, event_type, ip_hash, user_agent, target_url)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (message_id, event_type, ip_hash, user_agent, target_url),
            )
            if event_type == "open":
                await cur.execute(
                    """
                    UPDATE health_report_messages
                    SET open_count = open_count + 1,
                        first_opened_at = COALESCE(first_opened_at, CURRENT_TIMESTAMP)
                    WHERE id = %s
                    """,
                    (message_id,),
                )
                await cur.execute(
                    """
                    UPDATE health_report_recipient_statuses
                    SET opened_at = COALESCE(opened_at, CURRENT_TIMESTAMP)
                    WHERE message_id = %s AND recipient_role = 'principal'
                    """,
                    (message_id,),
                )
            if event_type == "click":
                await cur.execute(
                    """
                    UPDATE health_report_messages
                    SET click_count = click_count + 1,
                        first_clicked_at = COALESCE(first_clicked_at, CURRENT_TIMESTAMP)
                    WHERE id = %s
                    """,
                    (message_id,),
                )
                await cur.execute(
                    """
                    UPDATE health_report_recipient_statuses
                    SET clicked_at = COALESCE(clicked_at, CURRENT_TIMESTAMP)
                    WHERE message_id = %s AND recipient_role = 'principal'
                    """,
                    (message_id,),
                )
    finally:
        conn.close()


async def recipients_for_read_sync(limit: int = 100) -> List[Dict[str, Any]]:
    await ensure_health_report_tables()
    conn = await get_sipae_db_connection()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                """
                SELECT r.*, m.rfc_message_id, m.subject, m.report_date, m.plantel_code
                FROM health_report_recipient_statuses r
                JOIN health_report_messages m ON m.id = r.message_id
                WHERE m.status = 'sent'
                  AND r.delivery_status = 'sent'
                  AND (r.gmail_read_at IS NULL OR r.gmail_unread = 1)
                ORDER BY m.id DESC
                LIMIT %s
                """,
                (int(limit),),
            )
            return await cur.fetchall()
    finally:
        conn.close()


async def update_recipient_gmail_status(status_id: int, found: bool, unread: Optional[bool]) -> None:
    conn = await get_sipae_db_connection()
    try:
        async with conn.cursor() as cur:
            now = datetime.utcnow()
            read_at = now if found and unread is False else None
            await cur.execute(
                """
                UPDATE health_report_recipient_statuses
                SET gmail_found_at = CASE WHEN %s THEN COALESCE(gmail_found_at, %s) ELSE gmail_found_at END,
                    gmail_unread = %s,
                    gmail_read_at = COALESCE(gmail_read_at, %s),
                    last_checked_at = %s
                WHERE id = %s
                """,
                (found, now, unread, read_at, now, status_id),
            )
    finally:
        conn.close()


async def update_message_html(message_id: int, html_body: str) -> None:
    conn = await get_sipae_db_connection()
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE health_report_messages SET html_body = %s WHERE id = %s",
                (html_body, message_id),
            )
    finally:
        conn.close()
