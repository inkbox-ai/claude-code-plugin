"""Conversation routing and automatic email reply-all at the SDK boundary."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from inkbox_claude.config import BridgeConfig
from inkbox_claude.gateway import InkboxGateway
from test_companion import uid


class Sessions:
    def __init__(self):
        self.sessions = {}

    def get(self, key):
        if key not in self.sessions:
            self.sessions[key] = SimpleNamespace(handle_inbound=AsyncMock(), run_consult=AsyncMock())
        return self.sessions[key]


def gateway():
    gw = InkboxGateway(BridgeConfig(allow_all_users=True))
    gw.sessions = Sessions()
    gw._resolve_contact_full = AsyncMock(side_effect=lambda **kw: {"id": "contact-" + kw["value"]})
    gw._lookup_text_conversation_summary = AsyncMock(return_value=None)
    gw._lookup_imessage_conversation_summary = AsyncMock(return_value=None)
    return gw


@pytest.mark.parametrize("mode", ["sms", "imessage"])
def test_participants_share_group_without_merging_other_groups_or_dm(mode):
    async def scenario():
        gw = gateway()
        key = "text_message" if mode == "sms" else "message"
        handler = gw._on_text_received if mode == "sms" else gw._on_imessage_received
        for index, (sender, conversation, group) in enumerate([
            ("+15555550101", "group-one", True),
            ("+15555550102", "group-one", True),
            ("+15555550101", "group-two", True),
            ("+15555550101", "dm-one", False),
        ]):
            message = {"id": str(index), "conversation_id": conversation, "is_group": group,
                       "direction": "inbound", "sender_phone_number": sender,
                       "remote_number": sender, "text": "/stop", "content": "/stop"}
            await handler({"data": {key: message}})
        assert set(gw.sessions.sessions) == {f"{mode}:group-one", f"{mode}:group-two", "contact-+15555550101"}
        shared = gw.sessions.sessions[f"{mode}:group-one"].handle_inbound.call_args_list
        assert len(shared) == 2
        assert {call.args[2]["sender"] for call in shared} == {"+15555550101", "+15555550102"}
        assert all(call.args[2]["raw_text"] == "/stop" for call in shared)
    asyncio.run(scenario())


@pytest.mark.parametrize("mode", ["sms", "imessage"])
def test_group_without_conversation_never_falls_back_to_sender(mode):
    async def scenario():
        gw = gateway()
        key = "text_message" if mode == "sms" else "message"
        message = {"id": "no-conversation", "is_group": True, "sender_phone_number": "+15555550101",
                   "remote_number": "+15555550101", "text": "hello", "content": "hello"}
        handler = gw._on_text_received if mode == "sms" else gw._on_imessage_received
        response = await handler({"data": {key: message}})
        assert "group-without-conversation" in response.text
        assert not gw.sessions.sessions
    asyncio.run(scenario())


def test_reaction_and_delivery_recovery_stay_in_group():
    async def scenario():
        gw = gateway()
        gw._lookup_imessage_conversation_summary = AsyncMock(return_value={"is_group": True})
        await gw._on_imessage_reaction_received({"data": {"reaction": {
            "id": "reaction-one", "direction": "inbound", "remote_number": "+15555550101",
            "conversation_id": "group", "reaction": "like",
        }}})
        group = gw.sessions.sessions["imessage:group"]
        assert group.handle_inbound.call_args.args[2]["conversation_kind"] == "group"
        assert group.handle_inbound.call_args.args[2]["raw_text"] == ""
        await gw._on_imessage_delivery_failed({"data": {"message": {
            "id": "outbound-one", "direction": "outbound", "remote_number": "+15555550101",
            "conversation_id": "group", "status": "failed",
        }}})
        await asyncio.sleep(0)
        assert group.run_consult.call_args.kwargs == {
            "mode": "imessage", "reply_meta": {"to": "+15555550101", "sender": "+15555550101", "conversation_id": "group"},
        }
        assert list(gw.sessions.sessions) == ["imessage:group"]
    asyncio.run(scenario())


def test_automatic_email_uses_real_sdk_reply_all_endpoint_and_original_uuid():
    from inkbox.agent_identity import AgentIdentity
    from inkbox.mail.resources.messages import MessagesResource
    calls = []

    def post(path, **kwargs):
        calls.append((path, kwargs))
        return {"id": uid(100), "mailbox_id": uid(101), "message_id": "<reply@example.com>",
                "from_address": "helper@example.com", "to_addresses": ["sender@example.com"],
                "cc_addresses": ["other@example.com"], "direction": "outbound", "status": "sent",
                "is_read": True, "is_starred": False, "has_attachments": False,
                "created_at": "2026-01-01T00:00:00Z"}
    identity = AgentIdentity(SimpleNamespace(
        mailbox=SimpleNamespace(email_address="helper@example.com"), phone_number=None,
        imessage_number=None, tunnel=None,
    ), SimpleNamespace(_messages=MessagesResource(SimpleNamespace(post=post))))
    gw = gateway()
    gw._identity = identity
    asyncio.run(gw.send_to_contact("contact", "Reply", "email", {
        "message_id": uid(12), "to": "wrong@example.com", "subject": "Not a new email",
    }))
    assert calls == [(f"/mailboxes/helper@example.com/messages/{uid(12)}/reply-all", {"json": {"body_text": "Reply"}})]
    with pytest.raises(ValueError, match="stored inbound"):
        asyncio.run(gw.send_to_contact("contact", "Reply", "email", {"to": "sender@example.com"}))
    assert len(calls) == 1


def test_email_failure_recovery_keeps_original_inbound_reply_uuid():
    async def scenario():
        gw = gateway()
        original = {"sender": "person@example.com", "to": "person@example.com", "conversation_id": "thread", "message_id": "inbound-stored-uuid"}
        await gw._note_send_rejection("contact", "email", original, "failed answer", ValueError("rejected"))
        original["message_id"] = "later-message"
        await asyncio.sleep(0)
        route = gw.sessions.get("contact").run_consult.call_args.kwargs["reply_meta"]
        assert route["message_id"] == "inbound-stored-uuid"
        assert route["conversation_id"] == "thread"
    asyncio.run(scenario())


def test_missing_group_reaction_is_committed_to_dedup():
    async def scenario():
        gw = gateway()
        gw._lookup_imessage_conversation_summary = AsyncMock(return_value={"is_group": True})
        event = {"data": {"reaction": {"id": "reaction-missing-conversation", "remote_number": "+15555550101", "direction": "inbound", "reaction": "like", "is_group": True}}}
        first = await gw._on_imessage_reaction_received(event)
        assert "group-without-conversation" in first.text
        second = await gw._on_imessage_reaction_received(event)
        assert "deduped" in second.text
        assert not gw.sessions.sessions
    asyncio.run(scenario())
