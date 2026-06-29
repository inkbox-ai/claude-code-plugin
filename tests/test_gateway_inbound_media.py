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


class _FakeContacts:
    def lookup(self, **kwargs):
        if kwargs in (
            {"phone": "+15167251294"},
            {"email": "dima@inkbox.ai"},
        ):
            return [
                types.SimpleNamespace(
                    id="contact-dima",
                    preferred_name="Dima",
                    given_name="Dima",
                    family_name="",
                    company_name="Inkbox",
                    job_title="Cofounder",
                    notes="private note",
                    emails=[
                        types.SimpleNamespace(value="dima@inkbox.ai", is_primary=True),
                    ],
                    phones=[
                        types.SimpleNamespace(value="+15167251294", is_primary=True),
                    ],
                )
            ]
        return []


def _gw(monkeypatch, saved):
    async def fake_download(items, *, prefix):
        # Pretend each item downloaded; echo count so the prefix/threading works.
        return saved
    monkeypatch.setattr(gateway, "download_media", fake_download)
    gw = gateway.InkboxGateway(BridgeConfig(require_signature=False, allow_all_users=True))
    gw.sessions = _FakeSessions()
    return gw


def _attach_fake_contacts(gw):
    gw._inkbox = types.SimpleNamespace(contacts=_FakeContacts())


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


def test_duplicate_inbound_sms_event_id_does_not_double_enqueue(monkeypatch):
    gw = _gw(monkeypatch, [])
    envelope = {"data": {"text_message": {
        "id": "t1",
        "direction": "inbound",
        "remote_phone_number": "+15551234567",
        "text": "hello",
    }}}

    first = asyncio.run(gw._on_text_received(envelope))
    second = asyncio.run(gw._on_text_received(envelope))

    assert json.loads(first.text)["ok"] is True
    assert json.loads(second.text)["deduped"] is True
    assert len(gw.sessions.by_id["+15551234567"].inbound) == 1


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


def test_duplicate_inbound_imessage_event_id_does_not_double_enqueue(monkeypatch):
    gw = _gw(monkeypatch, [])
    envelope = {"data": {"message": {
        "id": "i1",
        "direction": "inbound",
        "remote_number": "+15551112222",
        "content": "hello",
    }}}

    first = asyncio.run(gw._on_imessage_received(envelope))
    second = asyncio.run(gw._on_imessage_received(envelope))

    assert json.loads(first.text)["ok"] is True
    assert json.loads(second.text)["deduped"] is True
    assert len(gw.sessions.by_id["+15551112222"].inbound) == 1


def test_unknown_inbound_email_uses_thread_session_key(monkeypatch):
    gw = _gw(monkeypatch, [])
    envelope = {"data": {"message": {
        "id": "m1",
        "from_address": "person@example.com",
        "thread_id": "thread-123",
        "subject": "Project",
        "snippet": "Can you check this?",
    }}}

    asyncio.run(gw._on_mail_received(envelope))

    body, mode, meta = gw.sessions.by_id["email:thread-123"].inbound[0]
    assert body == "Can you check this?"
    assert mode == "email"
    assert meta["to"] == "person@example.com"
    assert meta["thread_id"] == "thread-123"


def test_inbound_email_lookup_injects_contact_without_webhook_contact(monkeypatch):
    gw = _gw(monkeypatch, [])
    _attach_fake_contacts(gw)
    envelope = {"data": {"message": {
        "id": "m-dima",
        "from_address": "dima@inkbox.ai",
        "thread_id": "thread-dima",
        "subject": "Yo",
        "snippet": "Who am I?",
    }}}

    asyncio.run(gw._on_mail_received(envelope))

    body, mode, meta = gw.sessions.by_id["contact-dima"].inbound[0]
    assert body == "Who am I?"
    assert mode == "email"
    assert meta["contact"]["id"] == "contact-dima"
    assert meta["contact"]["name"] == "Dima"
    assert meta["contact"]["emails"] == ["dima@inkbox.ai"]


def test_unknown_direct_sms_uses_conversation_session_key(monkeypatch):
    gw = _gw(monkeypatch, [])
    envelope = {"data": {"text_message": {
        "id": "t-direct",
        "direction": "inbound",
        "remote_phone_number": "+15550000000",
        "conversation_id": "conv-direct",
        "text": "direct text",
    }}}

    asyncio.run(gw._on_text_received(envelope))

    body, mode, meta = gw.sessions.by_id["sms:conv-direct"].inbound[0]
    assert body == "direct text"
    assert mode == "sms"
    assert meta["conversation_id"] == "conv-direct"
    assert meta["conversation_kind"] == "direct"


def test_inbound_sms_lookup_injects_contact_without_webhook_contact(monkeypatch):
    gw = _gw(monkeypatch, [])
    _attach_fake_contacts(gw)
    envelope = {"data": {"text_message": {
        "id": "t-dima",
        "direction": "inbound",
        "remote_phone_number": "+15167251294",
        "conversation_id": "conv-dima",
        "text": "who am I?",
    }}}

    asyncio.run(gw._on_text_received(envelope))

    body, mode, meta = gw.sessions.by_id["contact-dima"].inbound[0]
    assert body == "who am I?"
    assert mode == "sms"
    assert meta["contact"]["id"] == "contact-dima"
    assert meta["contact"]["name"] == "Dima"
    assert meta["contact"]["phones"] == ["+15167251294"]


def test_unknown_inbound_imessage_uses_conversation_session_key(monkeypatch):
    gw = _gw(monkeypatch, [])
    envelope = {"data": {"message": {
        "id": "i2",
        "direction": "inbound",
        "remote_number": "+15551112222",
        "conversation_id": "imconv-123",
        "content": "hello",
    }}}

    asyncio.run(gw._on_imessage_received(envelope))

    body, mode, meta = gw.sessions.by_id["imessage:imconv-123"].inbound[0]
    assert body == "hello"
    assert mode == "imessage"
    assert meta["conversation_id"] == "imconv-123"


def test_inbound_imessage_lookup_injects_contact_without_webhook_contact(monkeypatch):
    gw = _gw(monkeypatch, [])
    _attach_fake_contacts(gw)
    envelope = {"data": {"message": {
        "id": "i-dima",
        "direction": "inbound",
        "remote_number": "+15167251294",
        "conversation_id": "imconv-dima",
        "content": "who am I?",
    }}}

    asyncio.run(gw._on_imessage_received(envelope))

    body, mode, meta = gw.sessions.by_id["contact-dima"].inbound[0]
    assert body == "who am I?"
    assert mode == "imessage"
    assert meta["contact"]["id"] == "contact-dima"
    assert meta["contact"]["name"] == "Dima"
    assert meta["contact"]["phones"] == ["+15167251294"]


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

    session = gw.sessions.by_id["imessage:imconv-123"]
    body, mode, meta = session.inbound[0]
    assert mode == "imessage"
    assert body.startswith("[inkbox:imessage_reaction from=+15551112222 reaction=question")
    assert "conversation_id=imconv-123" in body
    assert "target_message_id=im-target-9" in body
    assert "contact=unknown_in_inkbox" in body
    assert "return exactly [SILENT]" in body
    assert meta["conversation_id"] == "imconv-123"
    assert meta["typing"] is True


def test_imessage_reaction_without_contact_uses_conversation_session_key(monkeypatch):
    gw = _gw(monkeypatch, [])
    envelope = {"data": {
        "reaction": {
            "id": "react-2",
            "direction": "inbound",
            "remote_number": "+15551112222",
            "conversation_id": "imconv-456",
            "target_message_id": "im-target-10",
            "reaction": "like",
        },
    }}

    asyncio.run(gw._on_imessage_reaction_received(envelope))

    body, mode, meta = gw.sessions.by_id["imessage:imconv-456"].inbound[0]
    assert mode == "imessage"
    assert "reaction=like" in body
    assert meta["conversation_id"] == "imconv-456"


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
