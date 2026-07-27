"""Inbound email turns carry the full body, not the 200-char snippet.

``message.received`` ships the body alongside the snippet, self-describing
when it had to abbreviate. A whole body needs no round-trip; a truncated or
absent one falls back to a fetch, and the snippet is the last resort.
"""

import asyncio
import json
import types

import pytest

from inkbox_claude import gateway
from inkbox_claude.config import BridgeConfig


LONG_BODY = "Pricing details follow. " * 40
SNIPPET = LONG_BODY[:200]


@pytest.fixture(autouse=True)
def fake_web(monkeypatch):
    def json_response(payload):
        return types.SimpleNamespace(text=json.dumps(payload), payload=payload)

    monkeypatch.setattr(gateway, "web", types.SimpleNamespace(json_response=json_response))


class _FakeSession:
    def __init__(self):
        self.inbound = []

    async def handle_inbound(self, text, mode, meta):
        self.inbound.append((text, mode, meta))


class _FakeSessions:
    def __init__(self):
        self.by_id = {}

    def get(self, chat_id):
        return self.by_id.setdefault(chat_id, _FakeSession())


class _RecordingIdentity:
    """Stands in for the SDK identity; records full-body fetches."""

    def __init__(self, detail=None, fails=False):
        self.calls = []
        self._detail = detail
        self._fails = fails

    def get_message(self, message_id):
        self.calls.append(message_id)
        if self._fails:
            raise RuntimeError("unreachable")
        return self._detail


def _gw(identity=None):
    gw = gateway.InkboxGateway(BridgeConfig(require_signature=False, allow_all_users=True))
    gw.sessions = _FakeSessions()
    gw._identity = identity or _RecordingIdentity()
    return gw


def _envelope(**message_overrides):
    message = {
        "id": "mail-in-1",
        "thread_id": "thread-1",
        "from_address": "atlas@inkboxmail.com",
        "subject": "Coordinating",
        "snippet": SNIPPET,
        "direction": "inbound",
    }
    message.update(message_overrides)
    return {"data": {"message": message}}


# ── helper ───────────────────────────────────────────────────────────────


def test_complete_body_is_used_verbatim():
    message = {"body": LONG_BODY, "body_state": "complete", "snippet": SNIPPET}

    assert gateway._webhook_mail_body(message) == LONG_BODY


def test_truncated_body_appends_a_notice_with_the_message_id():
    message = {
        "id": "mail-in-9",
        "body": LONG_BODY,
        "body_state": "truncated",
        "body_truncated": True,
        "body_total_chars": 40_000,
        "body_included_chars": len(LONG_BODY),
        "snippet": SNIPPET,
    }

    result = gateway._webhook_mail_body(message)

    assert result.startswith(LONG_BODY)
    assert "too long to deliver in full" in result
    assert f"{len(LONG_BODY)} of 40000 characters" in result
    assert "mail-in-9" in result


def test_missing_body_falls_back_to_the_snippet():
    assert gateway._webhook_mail_body({"snippet": SNIPPET}) == SNIPPET


def test_unavailable_body_falls_back_to_the_snippet():
    message = {"body": "", "body_state": "unavailable", "snippet": SNIPPET}

    assert gateway._webhook_mail_body(message) == SNIPPET


# ── fetch policy ─────────────────────────────────────────────────────────


def test_complete_body_skips_the_fetch():
    identity = _RecordingIdentity()
    gw = _gw(identity)

    asyncio.run(gw._on_mail_received(_envelope(body=LONG_BODY, body_state="complete")))

    assert identity.calls == []
    body, mode, _ = gw.sessions.by_id[next(iter(gw.sessions.by_id))].inbound[0]
    assert mode == "email"
    assert LONG_BODY in body


def test_truncated_body_fetches_the_remainder():
    full = LONG_BODY + "and the rest of it."
    identity = _RecordingIdentity(detail=types.SimpleNamespace(body_text=full))
    gw = _gw(identity)

    asyncio.run(
        gw._on_mail_received(
            _envelope(
                body=LONG_BODY,
                body_state="truncated",
                body_truncated=True,
                body_total_chars=len(full),
                body_included_chars=len(LONG_BODY),
            )
        )
    )

    assert identity.calls == ["mail-in-1"]
    body, _, _ = gw.sessions.by_id[next(iter(gw.sessions.by_id))].inbound[0]
    assert body == full


def test_failed_fetch_keeps_the_truncated_body_and_notes_it():
    identity = _RecordingIdentity(fails=True)
    gw = _gw(identity)

    asyncio.run(
        gw._on_mail_received(
            _envelope(
                body=LONG_BODY,
                body_state="truncated",
                body_truncated=True,
                body_total_chars=40_000,
                body_included_chars=len(LONG_BODY),
            )
        )
    )

    body, _, _ = gw.sessions.by_id[next(iter(gw.sessions.by_id))].inbound[0]
    assert LONG_BODY in body
    assert "too long to deliver in full" in body


def test_absent_body_falls_back_to_the_fetch_then_the_snippet():
    identity = _RecordingIdentity(fails=True)
    gw = _gw(identity)

    asyncio.run(gw._on_mail_received(_envelope()))

    assert identity.calls == ["mail-in-1"]
    body, _, _ = gw.sessions.by_id[next(iter(gw.sessions.by_id))].inbound[0]
    assert body == SNIPPET
