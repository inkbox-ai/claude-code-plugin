"""Direct-only admission and saved-request safety boundaries."""

import asyncio
from copy import deepcopy
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest

from inkbox_claude.tools import CURRENT_SESSION
from inkbox_claude.whatsapp import ReplyPreflightRejected, send_reply
from inkbox_claude.whatsapp_v2 import receive_reaction
from tests.test_whatsapp_autoquote import admit, bind_tool, posts
from tests.test_whatsapp_v2 import channel as channel, CONVERSATION


@pytest.mark.parametrize("recipient", [[], ["+15550000002"], ["+15550000002", "+15550000003"],
                                       "+15550000002,+15550000003", "+1 (555) 000-0002", "+1١١١١١١١"])
def test_invalid_recipient_never_writes(channel, recipient):
    chat, _, meta = admit(channel)
    session, tool = bind_tool(channel, meta, chat)
    token = CURRENT_SESSION.set(session)
    try:
        with pytest.raises(ValueError):
            tool({"to": recipient, "text": "Requested message"}, lambda: None)
        assert posts(channel) == []
    finally:
        CURRENT_SESSION.reset(token)


@pytest.mark.parametrize("outcome", ["uncertain", "pending", "accepted", "rejected"])
@pytest.mark.parametrize("slot", ["whatsapp_plain_send", "whatsapp_recipient_sends"])
@pytest.mark.parametrize("recipients", [["+15550000002"], ["+15550000002", "+15550000003"]])
def test_saved_array_intent_cannot_be_converted_or_automatically_replied(channel, slot, recipients, outcome):
    chat, _, meta = admit(channel)
    prior = {"key": "saved-request", "intent": {"to": recipients, "text": "Original"}, "outcome": outcome}
    meta[slot] = prior if slot == "whatsapp_plain_send" else {prior["key"]: prior}
    saved = deepcopy(prior)
    session, tool = bind_tool(channel, meta, chat)
    token = CURRENT_SESSION.set(session)
    try:
        with pytest.raises(ReplyPreflightRejected):
            tool({"to": "+15550000002", "text": "Original", "idempotency_key": "saved-request"}, lambda: None)
        with pytest.raises(ReplyPreflightRejected):
            asyncio.run(send_reply(channel.gateway, chat, "Automatic replacement", meta))
        assert prior == saved
        assert posts(channel) == []
    finally:
        CURRENT_SESSION.reset(token)


@pytest.mark.parametrize("source", ["event", "target"])
@pytest.mark.parametrize("shape", [{"is_group": True}, {"group_name": "Unsupported"},
                                   {"participants": ["+15550000002", "+15550000003"]}])
def test_group_shaped_reactions_do_not_wake(channel, source, shape):
    event = deepcopy(channel.reaction)
    if source == "event":
        event.update(shape)
    else:
        channel.identity.get_whatsapp_message = Mock(return_value=NS(**{**channel.message, **shape}))
    asyncio.run(receive_reaction(channel.gateway, {"data": {"reaction": event}}))
    channel.inbound.assert_not_awaited()
    assert posts(channel) == []


def test_legacy_group_route_never_enters_host_queue(channel):
    from tests.test_sessions import make_session
    session = make_session([])
    session.send_fn = Mock()
    asyncio.run(session.handle_inbound("Question", "whatsapp", {
        "channel": "whatsapp", "conversation_kind": "group", "sender": "+15550000002",
        "conversation_id": CONVERSATION, "participants": ["+15550000002"],
    }))
    assert session._queue.empty() and session._worker is None
    session.send_fn.assert_not_called()


def test_removed_group_hint_in_saved_intent_is_not_ignored(channel):
    chat, _, meta = admit(channel)
    meta["whatsapp_plain_send"] = {"key": "saved-request", "outcome": "rejected",
        "intent": {"to": "+15550000002", "group_name": "Unsupported", "text": "Original"}}
    session, tool = bind_tool(channel, meta, chat)
    token = CURRENT_SESSION.set(session)
    try:
        with pytest.raises(ReplyPreflightRejected):
            tool({"to": "+15550000002", "text": "Original", "idempotency_key": "saved-request"}, lambda: None)
        assert posts(channel) == []
    finally:
        CURRENT_SESSION.reset(token)


@pytest.mark.parametrize("mode", ["imessage", "sms"])
def test_other_channel_group_turn_can_request_a_whatsapp_direct_message(channel, mode):
    chat, _, meta = admit(channel)
    meta.update(channel=mode, conversation_kind="group", participants=["+15550000002", "+15550000003"])
    session, tool = bind_tool(channel, meta, chat)
    token = CURRENT_SESSION.set(session)
    try:
        tool({"to": "+15550000004", "text": "Requested message"}, lambda: None)
        assert len(posts(channel)) == 1
    finally:
        CURRENT_SESSION.reset(token)


def test_send_schema_only_exposes_scalar_direct_selector(channel):
    from inkbox_claude.channel_tools import build_channel_tools
    tool = next(t for t in build_channel_tools(channel.client, "agent", channel.gateway.cfg)
                if t.name == "inkbox_send_whatsapp")
    props = tool.input_schema["properties"]
    assert props["to"]["type"] == "string"
    assert "group_name" not in props and "groupName" not in props


@pytest.mark.parametrize("selector", [{"to": "+15550000004"}, {"conversation_id": CONVERSATION}])
def test_saved_group_owner_cannot_send_to_any_destination(channel, selector):
    chat, _, meta = admit(channel)
    meta["conversation_kind"] = "group"
    session, tool = bind_tool(channel, meta, chat)
    token = CURRENT_SESSION.set(session)
    try:
        with pytest.raises(ReplyPreflightRejected):
            tool({**selector, "text": "Requested message"}, lambda: None)
        assert posts(channel) == []
    finally:
        CURRENT_SESSION.reset(token)
