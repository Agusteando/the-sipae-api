import base64
from email.message import EmailMessage
from typing import Dict, List, Optional

from core.config import settings


def _build_gmail_service(subject_email: Optional[str] = None):
    if not settings.google_service_account_email or not settings.google_private_key:
        raise RuntimeError("Google service account settings are not configured.")

    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    scopes = [
        "https://www.googleapis.com/auth/gmail.send",
        "https://www.googleapis.com/auth/gmail.readonly",
    ]
    credentials = service_account.Credentials.from_service_account_info(
        {
            "client_email": settings.google_service_account_email,
            "private_key": settings.google_private_key.replace("\\n", "\n"),
            "token_uri": "https://oauth2.googleapis.com/token",
        },
        scopes=scopes,
    )
    delegated_subject = subject_email or settings.health_reports_gmail_sender
    if delegated_subject:
        credentials = credentials.with_subject(delegated_subject)
    return build("gmail", "v1", credentials=credentials, cache_discovery=False)


def send_html_email(
    *,
    to_email: str,
    cc_emails: List[str],
    subject: str,
    html_body: str,
    text_body: str,
    rfc_message_id: str,
) -> Dict[str, Optional[str]]:
    sender = settings.health_reports_gmail_sender
    if not sender:
        raise RuntimeError("HEALTH_REPORTS_GMAIL_SENDER is not configured.")

    msg = EmailMessage()
    msg["To"] = to_email
    if cc_emails:
        msg["Cc"] = ", ".join(cc_emails)
    msg["From"] = sender
    msg["Subject"] = subject
    msg["Message-ID"] = rfc_message_id
    msg["X-SIPAE-Report-ID"] = rfc_message_id.strip("<>")
    msg.set_content(text_body or subject)
    msg.add_alternative(html_body, subtype="html")

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
    service = _build_gmail_service(sender)
    result = service.users().messages().send(userId="me", body={"raw": raw}).execute()
    return {
        "gmail_message_id": result.get("id"),
        "gmail_thread_id": result.get("threadId"),
    }


def lookup_recipient_read_status(*, recipient_email: str, rfc_message_id: str) -> Dict[str, Optional[bool]]:
    """
    Uses domain-wide delegation to inspect the recipient mailbox and infer read status.
    Gmail does not expose a formal read receipt. This checks whether the message exists
    in the recipient mailbox and whether Gmail still has the UNREAD label on it.
    """
    service = _build_gmail_service(recipient_email)
    query = f"rfc822msgid:{rfc_message_id.strip('<>')}"
    result = service.users().messages().list(userId="me", q=query, maxResults=1).execute()
    messages = result.get("messages") or []
    if not messages:
        return {"found": False, "unread": None}

    message = service.users().messages().get(
        userId="me",
        id=messages[0]["id"],
        format="metadata",
    ).execute()
    labels = set(message.get("labelIds") or [])
    return {"found": True, "unread": "UNREAD" in labels}
