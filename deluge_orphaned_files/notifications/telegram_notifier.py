"""Telegram notification helper.

Sends formatted scan reports via Telegram Bot API.
Requires the following environment variables (handled in settings):
    - TELEGRAM_BOT_TOKEN
    - TELEGRAM_CHAT_ID

Usage is similar to :pymod:`emailer`; errors are logged, not raised.
"""

from __future__ import annotations

import html
import time
import requests
from typing import Dict, Any
from loguru import logger

__all__: list[str] = ["send_scan_report"]

API_BASE_URL = "https://api.telegram.org/bot{token}/{method}"

# Telegram allows bots ~20 messages/minute to the same chat; long reports span
# dozens of chunks, so pace sends instead of relying on 429 retries alone.
SECONDS_BETWEEN_CHUNKS = 3.0
MAX_ATTEMPTS_PER_MESSAGE = 4


def _escape_and_chunk(content: str, chunk_size: int) -> list[str]:
    """Escape plain text into independently valid HTML chunks.

    Budget by escaped length while consuming one source character at a time. This
    prevents slicing inside entities such as ``&amp;`` while keeping every encoded
    chunk within Telegram's payload limit.
    """
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")

    chunks: list[str] = []
    current: list[str] = []
    current_length = 0

    for character in content:
        escaped = html.escape(character)
        if len(escaped) > chunk_size:
            raise ValueError("chunk_size is too small for an escaped character")
        if current and current_length + len(escaped) > chunk_size:
            chunks.append("".join(current))
            current = []
            current_length = 0
        current.append(escaped)
        current_length += len(escaped)

    if current:
        chunks.append("".join(current))
    return chunks


def _do_request(token: str, method: str, payload: Dict[str, Any]) -> bool:
    """Make a request to the Telegram API, honouring 429 rate-limit backoff.

    Args:
        token: Telegram bot token.
        method: API method name to call.
        payload: Request payload to send as JSON.

    Returns:
        bool: True if the request was successful, False otherwise.
    """
    url = API_BASE_URL.format(token=token, method=method)
    for attempt in range(1, MAX_ATTEMPTS_PER_MESSAGE + 1):
        try:
            response = requests.post(url, json=payload, timeout=10)
            if response.status_code == 429:
                # Telegram tells us exactly how long to wait; fall back to 30s if absent.
                try:
                    retry_after = int(response.json().get("parameters", {}).get("retry_after", 30))
                except (ValueError, requests.RequestException):
                    retry_after = 30
                if attempt < MAX_ATTEMPTS_PER_MESSAGE:
                    logger.warning("Telegram rate limit hit (429); waiting {}s before retry {}/{}", retry_after, attempt + 1, MAX_ATTEMPTS_PER_MESSAGE)
                    time.sleep(retry_after + 1)
                    continue
                logger.error("Telegram rate limit hit (429) and retries exhausted")
                return False
            response.raise_for_status()
            data = response.json()
            if not data.get("ok"):
                logger.error("Telegram API responded with ok=false: {}", data)
                return False
            logger.info("Telegram message sent successfully (chat_id={})", payload.get("chat_id"))
            return True
        except requests.RequestException as exc:  # noqa: BLE001
            # requests' exception text includes the request URL, which embeds the bot
            # token. Retain actionable diagnostics without writing credentials to logs.
            response = getattr(exc, "response", None)
            status_code = getattr(response, "status_code", None)
            diagnostic = type(exc).__name__
            if status_code is not None:
                diagnostic += f" (HTTP {status_code})"
            logger.error("Failed to send Telegram message via {}: {}", method, diagnostic)
            return False
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
    chunks = _escape_and_chunk(content, chunk_size)

    if not chunks:
        logger.warning("No content to send via Telegram")
        return False

    # Send first chunk with title (content is already escaped above)
    first_message = f"<b>{html.escape(title)}</b>\n\n<pre>{chunks[0]}</pre>"
    first_payload = {
        "chat_id": chat_id,
        "text": first_message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    success = _do_request(bot_token, "sendMessage", first_payload)
    if not success:
        return False

    # Send remaining chunks, paced to stay under Telegram's per-chat rate limit and
    # silent so a multi-chunk report triggers a single notification, not one per chunk.
    for i, chunk in enumerate(chunks[1:], 1):
        time.sleep(SECONDS_BETWEEN_CHUNKS)
        cont_message = f"<pre>{chunk}</pre>"
        cont_payload = {
            "chat_id": chat_id,
            "text": cont_message,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
            "disable_notification": True,
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
