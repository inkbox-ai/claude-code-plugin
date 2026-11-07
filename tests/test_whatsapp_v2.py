"""Direct-message, quote and reaction boundaries exercise the actual candidate SDK offline."""

import asyncio
import json
from copy import deepcopy
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import httpx
import pytest
from inkbox import Inkbox
from inkbox_claude.config import BridgeConfig
from inkbox_claude.gateway import InkboxGateway
from inkbox_claude.whatsapp import receive
from inkbox_claude.whatsapp_v2 import run_v2, send_selector, receive_reaction

IDENTITY, CONVERSATION, MESSAGE, REACTION, OUTBOUND = (str(UUID(int=i)) for i in range(1, 6))
NOW = "2026-01-01T00:00:00+00:00"


@pytest.fixture
def channel(monkeypatch, tmp_path):
    monkeypatch.setenv('INKBOX_CLAUDE_HOME', str(tmp_path))
    state = NS(requests=[], reaction_status=201)
    state.message = {"id": MESSAGE, "agent_identity_id": IDENTITY, "conversation_id": CONVERSATION,
        "direction": "inbound", "local_number": "+15550000001", "remote_number": "+15550000002",
        "sender_number": "+15550000002", "participants": ["+15550000002"],
        "content": "/stop do not replay this old text", "media": [], "status": "received", "delivery_state": None,
        "recipients": [{"remote_number": "+15550000002", "delivery_status": "received", "delivery_state": "unknown"}], "is_read": False, "is_blocked": False, "created_at": NOW, "updated_at": NOW}
    state.reaction = {"id": REACTION, "target_message_id": MESSAGE, "conversation_id": CONVERSATION,
        "agent_identity_id": IDENTITY, "direction": "inbound", "remote_number": "+15550000002",
        "reaction": "like", "custom_emoji": None, "emoji": "👍", "status": "applied", "revision": 2,
        "created_at": NOW, "updated_at": NOW, "removed_at": None}

    def handle(request):
        state.requests.append(request)
        path = request.url.path
        if path == "/api/v1/identities/agent":
            return httpx.Response(200, json={"id": IDENTITY, "organization_id": "org_example", "agent_handle": "agent", "created_at": NOW, "updated_at": NOW})
        assert request.url.params["agent_identity_id"] == IDENTITY
        if request.method == "GET" and path.startswith("/api/v1/whatsapp/conversations/") and "/threads/" not in path:
            return httpx.Response(200, json={"id": path.rsplit("/", 1)[-1], "agent_identity_id": IDENTITY,
                "local_number": "+15550000001", "remote_number": "+15550000002", "participants": ["+15550000002"],
                "created_at": NOW, "updated_at": NOW})
        if path == "/api/v1/whatsapp/conversations":
            return httpx.Response(200, json=[])
        if path == "/api/v1/whatsapp/messages" and request.method == "POST":
            return httpx.Response(201, json={"message": {**state.message, "id": OUTBOUND, "direction": "outbound", "status": "queued"}})
        if path.endswith("/thread") or "/threads/" in path:
            return httpx.Response(200, json={"conversation_id": CONVERSATION, "thread_id": MESSAGE,
                "thread_root_message_id": MESSAGE, "messages": [state.message], "next_cursor": "opaque-cursor"})
        if path == "/api/v1/whatsapp/reactions":
            return httpx.Response(state.reaction_status, json={"reaction": {**state.reaction, "status": "uncertain"}})
        if path == f"/api/v1/whatsapp/reactions/{state.reaction['id']}":
            return httpx.Response(200, json={"reaction": {**state.reaction, **({"status": "removed"} if request.method == "DELETE" else {})}})
        if path == f"/api/v1/whatsapp/messages/{MESSAGE}":
            return httpx.Response(200, json=state.message)
        raise AssertionError(f"Unexpected offline request: {request.method} {path}")

    monkeypatch.setattr(httpx, "HTTPTransport", lambda **kwargs: httpx.MockTransport(handle))
    state.client = Inkbox(api_key="ApiKey_synthetic_test_only", base_url="https://example.test")
    state.identity = state.client.get_identity("agent")
    state.gateway = InkboxGateway(BridgeConfig(identity="agent", base_url="https://example.test", allow_all_users=True))
    state.gateway._identity, state.gateway._inkbox = state.identity, state.client
    state.inbound = AsyncMock()
    state.gateway.sessions = NS(get=Mock(return_value=NS(handle_inbound=state.inbound)))
    state.gateway._resolve_contact_full = AsyncMock(return_value=None)
    state.gateway._with_media = AsyncMock(side_effect=lambda text, *_args, **_kwargs: text)
    yield state
    for store in state.gateway._channel_stores.values():
        store.close()
    state.client.close()


def test_direct_and_explicit_quote_use_bound_public_sdk(channel):
    from inkbox_claude.whatsapp import register_tools
    tools = {}
    register_tools(lambda name, description, properties, required, callback: tools.update({name: callback}), channel.client, "agent", channel.gateway.cfg)
    def send(args):
        return tools["inkbox_send_whatsapp"](args, lambda: None)
    send({"to": "+15550000002", "text": "Hello", "idempotency_key": "direct-one"})
    assert json.loads(channel.requests[-1].content) == {"to": "+15550000002", "text": "Hello"}
    send({"conversation_id": CONVERSATION, "reply_to_message_id": MESSAGE, "text": "Quoted", "idempotency_key": "quote-one"})
    request = channel.requests[-1]
    assert json.loads(request.content) == {"conversation_id": CONVERSATION, "reply_to_message_id": MESSAGE, "text": "Quoted"}
    assert request.headers["Idempotency-Key"] == "quote-one"


def test_thread_cursor_and_versioned_reaction_receipts_use_real_sdk(channel):
    authorize = Mock()
    page = run_v2(channel.identity, "get_whatsapp_thread", {"message_id": MESSAGE, "limit": 2, "cursor": "cursor"}, authorize=authorize)
    assert page.next_cursor == "opaque-cursor" and channel.requests[-1].url.params["cursor"] == "cursor"
    receipt = run_v2(channel.identity, "send_whatsapp_reaction", {"message_id": MESSAGE, "reaction": "like", "idempotency_key": "react-one"}, authorize=authorize)
    assert str(receipt.status) == "uncertain" and channel.requests[-1].headers["Idempotency-Key"] == "react-one"
    removed = run_v2(channel.identity, "remove_whatsapp_reaction", {"reaction_id": REACTION, "idempotency_key": "remove-one"}, authorize=authorize)
    assert str(removed.status) == "removed" and authorize.call_count == 2
    assert channel.requests[-1].method == "DELETE" and channel.requests[-1].headers["Idempotency-Key"] == "remove-one"


@pytest.mark.parametrize("args", [{}, {"to": []}, {"to": ["+15550000002"]}, {"to": "+15550000002,+15550000003"}, {"to": "+1 (555) 000-0002"}, {"to": "+1١١١١١١١"}, {"to": ["+15550000002", "+15550000002"]}, {"to": "+15550000002", "reply_to_message_id": MESSAGE}, {"conversation_id": CONVERSATION, "group_name": "bad"}, {"to": "+15550000002", "conversation_id": CONVERSATION}, {"to": ["+15550000002", "+15550000003"], "group_name": " "}])
def test_invalid_material_intent_is_rejected_before_write(args):
    with pytest.raises(ValueError):
        send_selector(args)


@pytest.mark.parametrize("change", [
    {"is_group": True}, {"group_id": "unsupported"}, {"groupName": "unsupported"},
    {"participants": ["+15550000002", "+15550000003"]},
    {"recipients": [{"phone_number": "+15550000002"}, {"phone_number": "+15550000003"}]},
    {"unresolved_participant_count": 1},
])
def test_unsupported_inbound_shapes_never_wake_or_reply(channel, change):
    channel.message.update(change)
    asyncio.run(receive(channel.gateway, {"id": "unsupported", "data": {"message": channel.message}}))
    channel.inbound.assert_not_awaited()
    assert not [r for r in channel.requests if r.method == "POST"]


@pytest.mark.parametrize("change", [{}, {"direction": "outbound"}, {"remote_number": None}, {"revision": 1}])
def test_peer_reaction_is_fresh_context_not_a_target_replay_or_self_echo(channel, change):
    event = {"event_type": "whatsapp.reaction_received", "data": {"reaction": {**deepcopy(channel.reaction), **change}}}
    asyncio.run(receive_reaction(channel.gateway, event))
    if change:
        channel.inbound.assert_not_awaited()
    else:
        body, mode, meta = channel.inbound.call_args.args
        assert "lightweight signal" in body and "/stop" not in body
        assert mode == "whatsapp" and meta["message_id"] == MESSAGE and meta["reaction_id"] == REACTION
        assert meta["raw_text"] == "" and meta["imessage_event_ids"] == [f"reaction:{REACTION}:2:applied"]
        asyncio.run(receive_reaction(channel.gateway, event))
        channel.inbound.assert_awaited_once()


def test_cancelled_turn_cannot_write_a_reaction(channel):
    def reject():
        raise PermissionError("Turn is no longer active")
    before = len(channel.requests)
    with pytest.raises(PermissionError):
        run_v2(channel.identity, "send_whatsapp_reaction", {"message_id": MESSAGE, "reaction": "like", "idempotency_key": "reaction-one"}, authorize=reject)
    assert len(channel.requests) == before
