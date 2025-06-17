"""Email notification helper.

Sends formatted scan reports using SMTP credentials provided via
environment variables.
"""

from __future__ import annotations

import ssl
import smtplib
from email.message import EmailMessage
from email.utils import formatdate, make_msgid
from datetime import datetime

from loguru import logger

__all__ = ["send_scan_report"]


def _build_message(subject: str, body: str, from_addr: str, to_addrs: list[str]) -> EmailMessage:
    """Build an email message with proper headers.

    Args:
        subject: Email subject line.
        body: Plain text email body content.
        from_addr: Sender email address.
        to_addrs: List of recipient email addresses.

    Returns:
        EmailMessage: Properly formatted email message object.
    """
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = ", ".join(to_addrs)
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = make_msgid()
    msg.set_content(body)
    return msg


def send_scan_report(*, smtp_host: str, smtp_port: int, username: str, password: str, from_addr: str, to_addrs: list[str], report_body: str, use_ssl: bool = True) -> None:
    """Send scan report via SMTP.

    All parameters except use_ssl are mandatory. If any error occurs during sending,
    it is logged but not raised, to avoid breaking the main process.

    Args:
        smtp_host: SMTP server hostname or IP address.
        smtp_port: SMTP server port number.
        username: SMTP authentication username.
        password: SMTP authentication password.
        from_addr: Sender email address.
        to_addrs: List of recipient email addresses.
        report_body: Email message content to send.
        use_ssl: Whether to use SSL/TLS connection (default True).
    """
    if not to_addrs:
        logger.warning("No e-mail recipients configured; skipping e-mail sending.")
        return

    subject = f"Deluge Orphaned Files Report – {datetime.now().date()}"
    msg = _build_message(subject, report_body, from_addr, to_addrs)

    try:
        if use_ssl:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(smtp_host, smtp_port, context=context) as server:
                server.login(username, password)
                server.send_message(msg)
        else:
            with smtplib.SMTP(smtp_host, smtp_port) as server:
                server.starttls(context=ssl.create_default_context())
                server.login(username, password)
                server.send_message(msg)
        logger.success("Scan report e-mail sent to {}", ", ".join(to_addrs))
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to send report e-mail: {}", exc)
