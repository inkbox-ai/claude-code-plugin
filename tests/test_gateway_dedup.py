import asyncio
import json
import types

import pytest

from inkbox_claude import gateway
from inkbox_claude.config import BridgeConfig


class _FakeResponse:
    def __init__(self, *, status=200, text=""):
        self.status = status
        self.text = text


class _FakeRequest:
    def __init__(self, body, *, request_id="req-1"):
        self._body = body
        # Real Inkbox traffic always carries its signature header — routing
        # keys off the identified source even when verification is off.
        self.headers = {
            "X-Inkbox-Request-Id": request_id,
            "X-Inkbox-Signature": "sha256=test",
        }
        self.url = "https://agent.example/webhook"

    async def read(self):
        return self._body


@pytest.fixture(autouse=True)
def fake_web(monkeypatch):
    def json_response(payload):
        return _FakeResponse(status=200, text=json.dumps(payload))

    monkeypatch.setattr(
        gateway,
        "web",
        types.SimpleNamespace(Response=_FakeResponse, json_response=json_response),
    )


def test_request_id_commits_after_success():
    gw = gateway.InkboxGateway(BridgeConfig(require_signature=False, allow_all_users=True))
    body = json.dumps({"event_type": "unknown.event"}).encode()

    first = asyncio.run(gw._handle_webhook(_FakeRequest(body)))
    second = asyncio.run(gw._handle_webhook(_FakeRequest(body)))

    assert json.loads(first.text)["ignored"] == "unknown.event"
    assert json.loads(second.text)["deduped"] is True


def test_request_id_rolls_back_after_dispatch_failure(monkeypatch):
    gw = gateway.InkboxGateway(BridgeConfig(require_signature=False, allow_all_users=True))
    calls = {"count": 0}

    async def fail_once(_envelope):
        calls["count"] += 1
        raise RuntimeError("boom")

    monkeypatch.setattr(gw, "_on_text_received", fail_once)
    body = json.dumps({"event_type": "text.received", "data": {"text_message": {"id": "t1"}}}).encode()

    with pytest.raises(RuntimeError):
        asyncio.run(gw._handle_webhook(_FakeRequest(body)))
    with pytest.raises(RuntimeError):
        asyncio.run(gw._handle_webhook(_FakeRequest(body)))

    assert calls["count"] == 2


def test_email_replay_with_stable_event_id_queues_once(monkeypatch):
    gw = gateway.InkboxGateway(BridgeConfig(require_signature=False, allow_all_users=True))
    calls = []

    async def capture(envelope):
        calls.append(envelope)
        return _FakeResponse()

    monkeypatch.setattr(gw, "_on_mail_received_once", capture)
    envelope = {"id": "event-1", "data": {"message": {"id": "message-1"}}}

    first = asyncio.run(gw._on_mail_received(envelope))
    second = asyncio.run(gw._on_mail_received(envelope))

    assert first.status == 200 and json.loads(second.text)["deduped"] is True
    assert len(calls) == 1


def test_email_replay_with_different_request_ids_dispatches_once(monkeypatch):
    gw = gateway.InkboxGateway(BridgeConfig(require_signature=False, allow_all_users=True))
    calls = []

    async def capture(envelope):
        calls.append(envelope)
        return _FakeResponse()

    monkeypatch.setattr(gw, "_on_mail_received_once", capture)
    body = json.dumps({
        "id": "event-1",
        "event_type": "message.received",
        "data": {"message": {"id": "message-1"}},
    }).encode()

    asyncio.run(gw._handle_webhook(_FakeRequest(body, request_id="delivery-1")))
    second = asyncio.run(gw._handle_webhook(_FakeRequest(body, request_id="delivery-2")))

    assert json.loads(second.text)["deduped"] is True
    assert len(calls) == 1


def test_distinct_email_events_in_same_thread_queue_separately(monkeypatch):
    gw = gateway.InkboxGateway(BridgeConfig(require_signature=False, allow_all_users=True))
    calls = []

    async def capture(envelope):
        calls.append(envelope)
        return _FakeResponse()

    monkeypatch.setattr(gw, "_on_mail_received_once", capture)
    first = {"id": "event-1", "data": {"message": {"id": "message-1", "thread_id": "thread"}}}
    second = {"id": "event-2", "data": {"message": {"id": "message-2", "thread_id": "thread"}}}

    asyncio.run(gw._on_mail_received(first))
    asyncio.run(gw._on_mail_received(second))

    assert len(calls) == 2


def test_email_message_id_is_fallback_when_event_id_is_missing(monkeypatch):
    gw = gateway.InkboxGateway(BridgeConfig(require_signature=False, allow_all_users=True))
    calls = []

    async def capture(envelope):
        calls.append(envelope)
        return _FakeResponse()

    monkeypatch.setattr(gw, "_on_mail_received_once", capture)
    envelope = {"data": {"message": {"id": "message-1"}}}

    asyncio.run(gw._on_mail_received(envelope))
    asyncio.run(gw._on_mail_received(envelope))

    assert len(calls) == 1


def test_email_missing_stable_ids_preserves_current_behavior(monkeypatch):
    gw = gateway.InkboxGateway(BridgeConfig(require_signature=False, allow_all_users=True))
    calls = []

    async def capture(envelope):
        calls.append(envelope)
        return _FakeResponse()

    monkeypatch.setattr(gw, "_on_mail_received_once", capture)
    envelope = {"data": {"message": {"thread_id": "thread"}}}

    asyncio.run(gw._on_mail_received(envelope))
    asyncio.run(gw._on_mail_received(envelope))

    assert len(calls) == 2


def test_email_stable_dedup_rolls_back_after_handler_failure(monkeypatch):
    gw = gateway.InkboxGateway(BridgeConfig(require_signature=False, allow_all_users=True))
    calls = {"count": 0}

    async def fail_once(_envelope):
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError("boom")
        return _FakeResponse()

    monkeypatch.setattr(gw, "_on_mail_received_once", fail_once)
    envelope = {"id": "event-1", "data": {"message": {"id": "message-1"}}}

    with pytest.raises(RuntimeError):
        asyncio.run(gw._on_mail_received(envelope))
    response = asyncio.run(gw._on_mail_received(envelope))

    assert response.status == 200
    assert calls["count"] == 2
