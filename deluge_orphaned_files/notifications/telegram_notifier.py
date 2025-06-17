"""Telegram notification helper.

Sends formatted scan reports via Telegram Bot API.
Requires the following environment variables (handled in settings):
    - TELEGRAM_BOT_TOKEN
    - TELEGRAM_CHAT_ID

Usage is similar to :pymod:`emailer`; errors are logged, not raised.
"""

from __future__ import annotations

import html
import requests
from typing import Dict, Any
from loguru import logger

__all__: list[str] = ["send_scan_report"]

API_BASE_URL = "https://api.telegram.org/bot{token}/{method}"


def _do_request(token: str, method: str, payload: Dict[str, Any]) -> bool:
    """Make a request to the Telegram API.

    Args:
        token: Telegram bot token.
        method: API method name to call.
        payload: Request payload to send as JSON.

    Returns:
        bool: True if the request was successful, False otherwise.
    """
    url = API_BASE_URL.format(token=token, method=method)
    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        data = response.json()
        if not data.get("ok"):
            logger.error("Telegram API responded with ok=false: {}", data)
            return False
        logger.info("Telegram message sent successfully (chat_id={})", payload.get("chat_id"))
        return True
    except requests.RequestException as exc:  # noqa: BLE001
        logger.error("Failed to send Telegram message: {}", exc)
        return False


def _send_in_chunks(*, bot_token: str, chat_id: str, title: str, content: str, chunk_size: int = 3800) -> bool:
    """Send a long message in multiple chunks to avoid Telegram's 4096 character limit.

    Args:
        bot_token: Bot token obtained from @BotFather.
        chat_id: Destination chat ID.
        title: Title to include in first message chunk.
        content: Content to split into multiple messages.
        chunk_size: Maximum size of each chunk (default 3800 to leave room for HTML tags).

    Returns:
        bool: True if all chunks were sent successfully, False otherwise.
    """
    lines = content.split("\n")
    chunks = []
    current_chunk = []
    current_length = 0

    # Process lines into chunks
    for line in lines:
        line_length = len(line) + 1  # +1 for newline

        # Handle lines longer than chunk_size
        if line_length > chunk_size:
            # If we have accumulated content already, save it as a chunk first
            if current_chunk:
                chunks.append("\n".join(current_chunk))
                current_chunk = []
                current_length = 0

            # Split long line into sub-chunks
            for i in range(0, len(line), chunk_size):
                sub_chunk = line[i:i + chunk_size]
                # Each sub-chunk becomes its own complete chunk
                chunks.append(sub_chunk)
        # Normal case - line fits within chunk_size
        else:
            # Start a new chunk if adding this line would exceed chunk_size
            if current_length + line_length > chunk_size:
                chunks.append("\n".join(current_chunk))
                current_chunk = []
                current_length = 0

            current_chunk.append(line)
            current_length += line_length

    if current_chunk:
        chunks.append("\n".join(current_chunk))

    if not chunks:
        logger.warning("No content to send via Telegram")
        return False

    # Send first chunk with title
    first_message = f"<b>{html.escape(title)}</b>\n\n<pre>{html.escape(chunks[0])}</pre>"
    first_payload = {
        "chat_id": chat_id,
        "text": first_message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    success = _do_request(bot_token, "sendMessage", first_payload)
    if not success:
        return False

    # Send remaining chunks
    for i, chunk in enumerate(chunks[1:], 1):
        cont_message = f"<pre>{html.escape(chunk)}</pre>"
        cont_payload = {
            "chat_id": chat_id,
            "text": cont_message,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if not _do_request(bot_token, "sendMessage", cont_payload):
            logger.error(f"Failed to send chunk {i+1}/{len(chunks)}")
            return False

    return True


def send_scan_report(*, bot_token: str, chat_id: str, report_body: str) -> None:
    """Send report body via Telegram.

    Args:
        bot_token: Bot token obtained from @BotFather.
        chat_id: Destination chat (user ID or channel/group ID).
        report_body: Text payload to send (will be split into multiple messages if needed).
    """
    if not bot_token or not chat_id:
        logger.warning("Telegram bot token or chat_id not configured; skipping Telegram notification.")
        return

    title = "Deluge Orphaned Files Scan Report"
    _send_in_chunks(bot_token=bot_token, chat_id=chat_id, title=title, content=report_body)
