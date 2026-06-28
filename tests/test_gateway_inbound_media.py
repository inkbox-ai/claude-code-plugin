import asyncio
import json
import types

import pytest

from inkbox_claude import gateway
from inkbox_claude.config import BridgeConfig


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


def _gw(monkeypatch, saved):
    async def fake_download(items, *, prefix):
        # Pretend each item downloaded; echo count so the prefix/threading works.
        return saved
    monkeypatch.setattr(gateway, "download_media", fake_download)
    gw = gateway.InkboxGateway(BridgeConfig(require_signature=False, allow_all_users=True))
    gw.sessions = _FakeSessions()
    return gw


def test_inbound_mms_media_only_wakes_agent_with_note(monkeypatch):
    gw = _gw(monkeypatch, [{"path": "/m/sms-0.jpg", "content_type": "image/jpeg"}])
    envelope = {"data": {"text_message": {
        "id": "t1", "direction": "inbound", "remote_phone_number": "+15551234567",
        "text": "", "media": [{"url": "https://s3/x.jpg", "content_type": "image/jpeg"}],
    }}}
    asyncio.run(gw._on_text_received(envelope))

    session = gw.sessions.by_id["+15551234567"]
    assert len(session.inbound) == 1
    body, mode, _ = session.inbound[0]
    assert mode == "sms"
    assert "/m/sms-0.jpg (image/jpeg)" in body  # media note present
    assert "Read tool" in body


def test_inbound_imessage_with_text_and_media_appends_note(monkeypatch):
    gw = _gw(monkeypatch, [{"path": "/m/imsg-0.png", "content_type": "image/png"}])
    envelope = {"data": {"message": {
        "id": "i1", "direction": "inbound", "remote_number": "+15551112222",
        "content": "check this out", "media": [{"url": "https://s3/y.png", "content_type": "image/png"}],
    }}}
    asyncio.run(gw._on_imessage_received(envelope))

    body, mode, _ = gw.sessions.by_id["+15551112222"].inbound[0]
    assert mode == "imessage"
    assert body.startswith("check this out")
    assert "/m/imsg-0.png (image/png)" in body


def test_inbound_text_without_media_is_unchanged(monkeypatch):
    gw = _gw(monkeypatch, [])
    envelope = {"data": {"text_message": {
        "id": "t2", "direction": "inbound", "remote_phone_number": "+15550000000",
        "text": "just text",
    }}}
    asyncio.run(gw._on_text_received(envelope))
    body, _, _ = gw.sessions.by_id["+15550000000"].inbound[0]
    assert body == "just text"


def test_group_sms_injects_silent_policy(monkeypatch):
    gw = _gw(monkeypatch, [])
    envelope = {"data": {
        "text_message": {
            "id": "t-group",
            "direction": "inbound",
            "remote_phone_number": "+15550000000",
            "local_phone_number": "+15550000001",
            "conversation_id": "conv-123",
            "participants": ["+15550000000", "+15550000002"],
            "text": "Dinner moved to 7.",
        },
    }}

    asyncio.run(gw._on_text_received(envelope))

    session = gw.sessions.by_id["sms:conv-123"]
    body, mode, meta = session.inbound[0]
    assert mode == "sms"
    assert body.startswith("[inkbox:group_sms conversation_id=conv-123")
    assert "participants=+15550000000,+15550000002" in body
    assert "Group SMS response policy" in body
    assert "return exactly [SILENT]" in body
    assert meta["conversation_id"] == "conv-123"
    assert meta["conversation_kind"] == "group"


def test_imessage_reaction_injects_silent_policy(monkeypatch):
    gw = _gw(monkeypatch, [])
    envelope = {"data": {
        "reaction": {
            "id": "react-1",
            "direction": "inbound",
            "remote_number": "+15551112222",
            "conversation_id": "imconv-123",
            "target_message_id": "im-target-9",
            "reaction": "question",
        },
        "contacts": [{"id": "contact-9"}],
    }}

    asyncio.run(gw._on_imessage_reaction_received(envelope))

    session = gw.sessions.by_id["contact-9"]
    body, mode, meta = session.inbound[0]
    assert mode == "imessage"
    assert body.startswith("[inkbox:imessage_reaction from=+15551112222 reaction=question")
    assert "conversation_id=imconv-123" in body
    assert "target_message_id=im-target-9" in body
    assert "return exactly [SILENT]" in body
    assert meta["conversation_id"] == "imconv-123"
    assert meta["typing"] is True


def test_outbound_imessage_reaction_echo_is_ignored(monkeypatch):
    gw = _gw(monkeypatch, [])
    envelope = {"data": {"reaction": {
        "id": "react-out",
        "direction": "outbound",
        "remote_number": "+15551112222",
        "reaction": "like",
    }}}

    resp = asyncio.run(gw._on_imessage_reaction_received(envelope))

    assert json.loads(resp.text)["ignored"] == "outbound-reaction"
    assert gw.sessions.by_id == {}


def test_imessage_reaction_subscribed():
    assert "imessage.reaction_received" in gateway.IMESSAGE_EVENTS


def test_empty_message_no_text_no_media_is_ignored(monkeypatch):
    gw = _gw(monkeypatch, [])
    envelope = {"data": {"text_message": {
        "id": "t3", "direction": "inbound", "remote_phone_number": "+15550000001", "text": "",
    }}}
    resp = asyncio.run(gw._on_text_received(envelope))
    assert json.loads(resp.text)["ignored"] == "empty"
    assert "+15550000001" not in gw.sessions.by_id
