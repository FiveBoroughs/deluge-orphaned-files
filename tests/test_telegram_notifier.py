"""Tests for the Telegram notifier's rate-limit handling and chunking.

Long scan reports span dozens of chunks; the notifier must honour Telegram's 429
retry_after, pace consecutive sends, and never let HTML entity expansion push a
chunk past the 4096-character message limit.
"""

from __future__ import annotations

import html

import pytest

from deluge_orphaned_files.notifications import telegram_notifier


class _FakeResponse:
    def __init__(self, status_code: int = 200, payload: dict | None = None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {"ok": True}

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise telegram_notifier.requests.HTTPError(f"{self.status_code} error")


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    """Collect sleeps instead of waiting; tests assert on pacing via this list."""
    slept: list[float] = []
    monkeypatch.setattr(telegram_notifier.time, "sleep", slept.append)
    return slept


def test_429_waits_retry_after_then_retries(monkeypatch, no_sleep):
    responses = iter(
        [
            _FakeResponse(429, {"ok": False, "parameters": {"retry_after": 7}}),
            _FakeResponse(200),
        ]
    )
    monkeypatch.setattr(telegram_notifier.requests, "post", lambda *a, **k: next(responses))

    assert telegram_notifier._do_request("tok", "sendMessage", {"chat_id": "1", "text": "x"}) is True
    assert 8 in no_sleep  # retry_after + 1


def test_429_gives_up_after_max_attempts(monkeypatch, no_sleep):
    monkeypatch.setattr(telegram_notifier.requests, "post", lambda *a, **k: _FakeResponse(429, {"ok": False, "parameters": {"retry_after": 1}}))

    assert telegram_notifier._do_request("tok", "sendMessage", {"chat_id": "1", "text": "x"}) is False
    assert len(no_sleep) == telegram_notifier.MAX_ATTEMPTS_PER_MESSAGE - 1


def test_chunks_are_paced_and_all_sent(monkeypatch, no_sleep):
    sent: list[dict] = []

    def fake_post(url, json=None, timeout=None):
        sent.append(json)
        return _FakeResponse(200)

    monkeypatch.setattr(telegram_notifier.requests, "post", fake_post)

    content = "\n".join(f"line {i} " + "x" * 100 for i in range(200))  # forces several chunks
    assert telegram_notifier._send_in_chunks(bot_token="tok", chat_id="1", title="T", content=content) is True
    assert len(sent) > 2
    # One pacing sleep per chunk after the first
    assert no_sleep.count(telegram_notifier.SECONDS_BETWEEN_CHUNKS) == len(sent) - 1
    # Only the first chunk notifies; the rest are silent
    assert "disable_notification" not in sent[0]
    assert all(p.get("disable_notification") is True for p in sent[1:])


def test_entity_expansion_cannot_exceed_telegram_limit(monkeypatch):
    sent: list[dict] = []

    def fake_post(url, json=None, timeout=None):
        sent.append(json)
        return _FakeResponse(200)

    monkeypatch.setattr(telegram_notifier.requests, "post", fake_post)
    monkeypatch.setattr(telegram_notifier.time, "sleep", lambda s: None)

    # Ampersand-heavy filenames ('Written & Read by…') expand 5x under html.escape
    content = "\n".join("File & Co & Sons & Friends & More " * 20 for _ in range(100))
    assert telegram_notifier._send_in_chunks(bot_token="tok", chat_id="1", title="T", content=content) is True
    for payload in sent:
        assert len(payload["text"]) <= 4096


def test_chunk_boundary_never_splits_an_html_entity(monkeypatch):
    sent: list[dict] = []

    def fake_post(url, json=None, timeout=None):
        sent.append(json)
        return _FakeResponse(200)

    monkeypatch.setattr(telegram_notifier.requests, "post", fake_post)
    monkeypatch.setattr(telegram_notifier.time, "sleep", lambda seconds: None)

    # Escaping appends "&amp;" after 3,799 characters, exactly across the old
    # 3,800-character slicing boundary.
    content = "x" * 3799 + "&"
    assert telegram_notifier._send_in_chunks(bot_token="tok", chat_id="1", title="T", content=content) is True

    bodies = [payload["text"].split("<pre>", 1)[1].rsplit("</pre>", 1)[0] for payload in sent]
    assert "".join(html.unescape(body) for body in bodies) == content


def test_request_errors_never_log_the_bot_token(monkeypatch):
    secret = "secret-bot-token"
    messages: list[str] = []

    def fail(*args, **kwargs):
        raise telegram_notifier.requests.ConnectionError(f"request failed for https://api.telegram.org/bot{secret}/sendMessage")

    monkeypatch.setattr(telegram_notifier.requests, "post", fail)
    sink = telegram_notifier.logger.add(messages.append, format="{message}")
    try:
        assert telegram_notifier._do_request(secret, "sendMessage", {"chat_id": "1", "text": "x"}) is False
    finally:
        telegram_notifier.logger.remove(sink)

    logged = "".join(messages)
    assert secret not in logged
    assert "ConnectionError" in logged
