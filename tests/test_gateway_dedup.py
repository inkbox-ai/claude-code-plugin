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


@pytest.mark.parametrize("fails_first", [False, True])
def test_event_replay_with_new_request_id(monkeypatch, fails_first):
    gw = gateway.InkboxGateway(BridgeConfig(require_signature=False, allow_all_users=True))
    calls = []

    async def receive(envelope):
        calls.append(envelope["id"])
        if fails_first and len(calls) == 1:
            raise RuntimeError("retry")
        return _FakeResponse(text="ok")

    monkeypatch.setattr(gw, "_on_mail_received", receive)
    body = json.dumps({"id": "evt-mail", "event_type": "message.received", "data": {"message": {"id": "mail-1"}}}).encode()
    if fails_first:
        with pytest.raises(RuntimeError):
            asyncio.run(gw._handle_webhook(_FakeRequest(body, request_id="failed")))
    asyncio.run(gw._handle_webhook(_FakeRequest(body, request_id="original")))
    replay = asyncio.run(gw._handle_webhook(_FakeRequest(body, request_id="replay")))
    assert json.loads(replay.text)["deduped"] is True
    assert len(calls) == (2 if fails_first else 1)


def test_external_object_id_does_not_deduplicate_distinct_deliveries(monkeypatch):
    gw = gateway.InkboxGateway(BridgeConfig(require_signature=False, allow_all_users=True, external_events_enabled=True))
    calls = []

    async def receive(envelope, request_id, **kwargs):
        calls.append(request_id)
        return _FakeResponse(text="ok")

    monkeypatch.setattr(gw, "_on_external_event", receive)
    body = b'{"id":"object-1","event_type":"external.updated"}'
    for request_id in ["update-1", "update-2"]:
        asyncio.run(gw._handle_webhook(_FakeRequest(body, request_id=request_id)))
    assert calls == ["update-1", "update-2"]
