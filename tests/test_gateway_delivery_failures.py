import asyncio
import json
import types

import pytest

from inkbox_claude import gateway
from inkbox_claude.config import BridgeConfig


@pytest.fixture(autouse=True)
def fake_web(monkeypatch):
    """aiohttp isn't installed in tests; stub the json_response the handlers use."""
    def json_response(payload):
        return types.SimpleNamespace(text=json.dumps(payload), payload=payload)
    monkeypatch.setattr(gateway, "web", types.SimpleNamespace(json_response=json_response))


class _FakeSession:
    """Captures the prompt run_consult was called with."""

    def __init__(self):
        self.consulted = []

    async def run_consult(self, prompt):
        self.consulted.append(prompt)
        return ""


class _FakeSessions:
    def __init__(self):
        self.by_id = {}

    def get(self, chat_id):
        return self.by_id.setdefault(chat_id, _FakeSession())


def _gw():
    gw = gateway.InkboxGateway(BridgeConfig(require_signature=False))
    gw.sessions = _FakeSessions()
    return gw


async def _drain():
    # Let the background _run_failure_turn task finish.
    await asyncio.sleep(0)
    await asyncio.sleep(0)


def _dispatch(gw, envelope, event_type):
    async def go():
        if event_type == "text.delivery_failed":
            r = await gw._on_text_delivery_failed(envelope, event_type)
        elif event_type == "imessage.delivery_failed":
            r = await gw._on_imessage_delivery_failed(envelope)
        else:
            r = await gw._on_mail_delivery_failed(envelope, event_type)
        await _drain()
        return r
    return asyncio.run(go())


class _FakeRequest:
    """Drives the real _handle_webhook (Inkbox-signed; verification off)."""

    def __init__(self, envelope, *, request_id="req-1"):
        self._body = json.dumps(envelope).encode()
        self.headers = {
            "X-Inkbox-Request-Id": request_id,
            "X-Inkbox-Signature": "sha256=test",
        }
        self.url = "https://agent.example/webhook"

    async def read(self):
        return self._body


def test_delivery_unconfirmed_does_not_wake_agent():
    # Fleet standard: text.delivery_unconfirmed is carrier uncertainty, not a
    # hard failure. It must NOT wake the agent - waking would resend a message
    # that likely landed. Driven through the real _handle_webhook dispatch.
    gw = _gw()
    envelope = {"event_type": "text.delivery_unconfirmed", "data": {"text_message": {
        "id": "u1", "direction": "outbound", "remote_phone_number": "+15551234567",
        "text": "your appointment is confirmed for 3pm",
    }, "contacts": [{"id": "contact-1"}]}}

    resp = asyncio.run(gw._handle_webhook(_FakeRequest(envelope)))

    # Logged and acked, but no session was woken.
    assert json.loads(resp.text)["ignored"] == "text.delivery_unconfirmed"
    assert gw.sessions.by_id == {}


def test_sms_delivery_failure_notifies_session():
    gw = _gw()
    envelope = {"data": {"text_message": {
        "id": "m1", "remote_phone_number": "+15551234567",
        "text": "build passed", "error_detail": "Message filtered by carrier",
    }, "contacts": [{"id": "contact-9"}]}}
    _dispatch(gw, envelope, "text.delivery_failed")

    # Keyed by resolved contact id; agent told via run_consult with the details.
    session = gw.sessions.by_id["contact-9"]
    assert len(session.consulted) == 1
    prompt = session.consulted[0]
    assert "SMS" in prompt and "+15551234567" in prompt
    assert "Message filtered by carrier" in prompt
    assert "build passed" in prompt


def test_imessage_delivery_failure_uses_error_reason():
    gw = _gw()
    envelope = {"data": {"message": {
        "id": "i1", "remote_number": "+15551112222",
        "content": "on it", "error_reason": "recipient_unavailable", "status": "error",
    }}}
    _dispatch(gw, envelope, "imessage.delivery_failed")
    # No contacts → falls back to the remote number as the session key.
    session = gw.sessions.by_id["+15551112222"]
    assert "iMessage" in session.consulted[0]
    assert "recipient_unavailable" in session.consulted[0]


def test_email_bounce_notifies_session():
    gw = _gw()
    envelope = {"data": {"message": {
        "id": "e1", "to_addresses": ["bob@example.com"], "subject": "Re: pricing",
    }}}
    _dispatch(gw, envelope, "message.bounced")
    session = gw.sessions.by_id["bob@example.com"]
    assert "email" in session.consulted[0]
    assert "bounced" in session.consulted[0]


def test_duplicate_failure_webhook_does_not_double_notify():
    gw = _gw()
    envelope = {"data": {"text_message": {
        "id": "dup1", "remote_phone_number": "+15550000000", "text": "x",
        "error_detail": "boom",
    }}}
    _dispatch(gw, envelope, "text.delivery_failed")
    second = _dispatch(gw, envelope, "text.delivery_failed")
    assert json.loads(second.text)["deduped"] is True
    # Only the first notification ran a turn.
    assert len(gw.sessions.by_id["+15550000000"].consulted) == 1
