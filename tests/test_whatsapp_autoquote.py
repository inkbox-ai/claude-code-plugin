"""Source-anchored replies retain intent across tool calls and bridge restarts."""

import asyncio
from copy import deepcopy
import hashlib
import json
from types import SimpleNamespace as NS
from unittest.mock import Mock
from uuid import UUID

import httpx
import pytest
from inkbox import InkboxAPIError

from inkbox_claude.imessage import IMessageState, SafeReplyPreflightError
from inkbox_claude.sessions import _Turn
from inkbox_claude.tools import CURRENT_SESSION
from inkbox_claude.whatsapp import receive, register_tools, send_reply, ReplyPreflightRejected
from inkbox_claude.whatsapp_v2 import receive_reaction
from tests.test_sessions import make_session
from tests.test_whatsapp_v2 import channel as channel, MESSAGE, CONVERSATION, IDENTITY


def admit(channel, **changes):
    message = {**channel.message, "content": "Question", **changes}
    channel.message = message
    asyncio.run(receive(channel.gateway, {"id": "source-event", "data": {"message": message}}))
    text, _, meta = channel.inbound.call_args.args
    chat_id = channel.gateway.sessions.get.call_args.args[0]
    return chat_id, text, meta


def posts(channel):
    return [r for r in channel.requests if r.method == "POST"]


def final(channel, chat_id, meta, text="Answer"):
    channel.gateway._channel_store("whatsapp").mark(meta, "reply_pending", reply=text)
    asyncio.run(send_reply(channel.gateway, chat_id, text, {**meta, "final_reply": True}))


@pytest.mark.parametrize("group", [False, True])
@pytest.mark.parametrize("inbound_quote", [False, True])
def test_new_final_quotes_trigger_not_parent_and_preserves_destination(channel, group, inbound_quote):
    changes = {"is_group": group, "reply_to_message_id": str(UUID(int=99)) if inbound_quote else None}
    if group:
        changes["participants"] = ["+15550000002", "+15550000003"]
        channel.identity.get_whatsapp_conversation = Mock(return_value=NS(
            id=CONVERSATION, agent_identity_id=IDENTITY, is_group=True,
            participants=changes["participants"], unresolved_participant_count=0))
    chat_id, _, meta = admit(channel, **changes)
    final(channel, chat_id, meta)
    request = posts(channel)[0]
    assert json.loads(request.content) == {"conversation_id": CONVERSATION, "text": "Answer", "reply_to_message_id": MESSAGE}
    expected = "claude-wa-" + hashlib.sha256(json.dumps([
        channel.gateway.cfg.base_url, IDENTITY, CONVERSATION, ["source-event"], "Answer",
        {"reply_to_message_id": MESSAGE}], sort_keys=True).encode()).hexdigest()
    assert request.headers["Idempotency-Key"] == expected


@pytest.mark.parametrize("old_target", [None, MESSAGE])
def test_legacy_saved_reply_restart_preserves_plain_body_and_hash(channel, old_target):
    meta = {"channel": "whatsapp", "sender": "+15550000002", "conversation_id": CONVERSATION,
            "message_id": MESSAGE, "source_event_id": "legacy", "imessage_event_ids": ["legacy"],
            "whatsapp_reply_target": old_target}
    store = channel.gateway._channel_store("whatsapp")
    store.admit("chat", "Question", meta)
    store.mark(meta, "reply_pending", reply="Answer")
    restarted = IMessageState(channel.gateway.cfg, channel="whatsapp")
    pending = restarted.pending_replies()[0]
    asyncio.run(send_reply(channel.gateway, pending["chat_id"], pending["reply"], {**pending["meta"], "final_reply": True}))
    request = posts(channel)[0]
    assert json.loads(request.content) == {"conversation_id": CONVERSATION, "text": "Answer"}
    expected = "claude-wa-" + hashlib.sha256(json.dumps([
        channel.gateway.cfg.base_url, IDENTITY, CONVERSATION, ["legacy"], "Answer"], sort_keys=True).encode()).hexdigest()
    assert request.headers["Idempotency-Key"] == expected


@pytest.mark.parametrize("fault", ["identity", "conversation", "target", "blocked", "gone", "pseudo"])
def test_invalid_source_never_sends_plain_replacement(channel, fault):
    chat_id, _, meta = admit(channel)
    if fault == "identity":
        channel.message["agent_identity_id"] = str(UUID(int=55))
    elif fault == "conversation":
        channel.message["conversation_id"] = str(UUID(int=55))
    elif fault == "target":
        meta["whatsapp_reply_target"] = str(UUID(int=55))
    elif fault == "blocked":
        channel.message["is_blocked"] = True
    elif fault == "gone":
        channel.identity.get_whatsapp_message = Mock(side_effect=InkboxAPIError(404, {}))
    else:
        meta["whatsapp_reply_target"] = "reaction:example:2:applied"
        meta["whatsapp_reply_source"]["message_id"] = meta["whatsapp_reply_target"]
    with pytest.raises(ReplyPreflightRejected):
        final(channel, chat_id, meta)
    assert posts(channel) == []


def test_source_read_outage_retries_same_quote_and_intent(channel):
    chat_id, _, meta = admit(channel)
    original = channel.identity.get_whatsapp_message
    channel.identity.get_whatsapp_message = Mock(side_effect=httpx.ReadTimeout("read unavailable"))
    with pytest.raises(SafeReplyPreflightError):
        final(channel, chat_id, meta)
    assert posts(channel) == []
    pending = IMessageState(channel.gateway.cfg, channel="whatsapp").pending_replies()[0]
    channel.identity.get_whatsapp_message = original
    asyncio.run(send_reply(channel.gateway, chat_id, pending["reply"], {**pending["meta"], "final_reply": True}))
    assert json.loads(posts(channel)[0].content)["reply_to_message_id"] == MESSAGE


def bind_tool(channel, meta, chat_id):
    session = make_session([])
    session.cfg = channel.gateway.cfg
    session.chat_id = chat_id
    session.mode, session.reply_meta = "whatsapp", meta
    session._turn_active = True
    session._current_turn = _Turn(text="Question", mode="whatsapp", reply_meta=meta)
    registry = {}
    register_tools(lambda name, description, properties, required, callback: registry.update({name: callback}),
                   channel.client, "agent", channel.gateway.cfg)
    return session, registry["inkbox_send_whatsapp"]


@pytest.mark.parametrize("selector", ["native", "other", "recipient", "explicit", "existing-key"])
def test_tool_source_defaults_respect_outreach_explicit_target_and_existing_keys(channel, selector):
    chat_id, _, meta = admit(channel)
    session, tool = bind_tool(channel, meta, chat_id)
    args = {"conversation_id": CONVERSATION, "text": "Answer"}
    if selector == "other":
        args["conversation_id"] = str(UUID(int=77))
    elif selector == "recipient":
        args = {"to": "+15550000002", "text": "Answer"}
    elif selector == "explicit":
        args["reply_to_message_id"] = str(UUID(int=88))
    elif selector == "existing-key":
        args["idempotency_key"] = "legacy-plain-key"
    token = CURRENT_SESSION.set(session)
    try:
        tool(args, lambda: None)
    finally:
        CURRENT_SESSION.reset(token)
    body = json.loads(posts(channel)[0].content)
    if selector == "native":
        assert body["reply_to_message_id"] == MESSAGE
        assert session._current_channel_tool_delivery
    elif selector == "explicit":
        assert body["reply_to_message_id"] == str(UUID(int=88))
    else:
        assert "reply_to_message_id" not in body
    if selector == "recipient":
        assert body["to"] == "+15550000002" and "conversation_id" not in body
    if selector == "other":
        assert body["conversation_id"] == str(UUID(int=77))


@pytest.mark.parametrize("failure", ["uncertain", "rejected"])
def test_tool_quote_fence_survives_failure_restart_and_blocks_replacements(channel, failure):
    chat_id, _, meta = admit(channel)
    session, tool = bind_tool(channel, meta, chat_id)
    error = httpx.ReadTimeout("lost response") if failure == "uncertain" else InkboxAPIError(422, {"error": "whatsapp_groups_not_supported"})
    channel.identity.send_whatsapp = Mock(side_effect=error)
    channel.client.get_identity = Mock(return_value=channel.identity)
    token = CURRENT_SESSION.set(session)
    try:
        args = {"conversation_id": CONVERSATION, "text": "Answer"}
        with pytest.raises(type(error)):
            tool(args, lambda: None)
        original = channel.identity.send_whatsapp.call_args
        assert original.kwargs["reply_to_message_id"] == MESSAGE
        # The same operation is retryable, but not a new plain/changed send.
        with pytest.raises(type(error)):
            tool(args, lambda: None)
        assert channel.identity.send_whatsapp.call_args == original
        # Callers may retain only the key; its durable quote is restored.
        args_without_quote = {key: value for key, value in args.items() if key != "reply_to_message_id"}
        with pytest.raises(type(error)):
            tool(args_without_quote, lambda: None)
        assert channel.identity.send_whatsapp.call_args == original
        for replacement in ({"conversation_id": CONVERSATION, "text": "Replacement", "idempotency_key": "new"},
                            {"to": "+15550000002", "text": "Replacement"},
                            {**args, "text": "Changed"}):
            with pytest.raises(ReplyPreflightRejected):
                tool(replacement, lambda: None)
        assert channel.identity.send_whatsapp.call_count == 3
        restarted = IMessageState(channel.gateway.cfg, channel="whatsapp")
        saved = restarted.interrupted()[0]["meta"]
        assert saved["whatsapp_quote_intent"]["reply_to_message_id"] == MESSAGE
        session._current_turn.reply_meta = saved
        with pytest.raises(ReplyPreflightRejected):
            tool({"conversation_id": CONVERSATION, "text": "After restart", "idempotency_key": "replacement"}, lambda: None)
        with pytest.raises(ReplyPreflightRejected):
            asyncio.run(send_reply(channel.gateway, chat_id, "Final", saved))
        assert session._current_channel_tool_delivery
    finally:
        CURRENT_SESSION.reset(token)


def test_new_reaction_quotes_verified_message_not_reaction_receipt(channel):
    asyncio.run(receive_reaction(channel.gateway, {"data": {"reaction": deepcopy(channel.reaction)}}))
    _, _, meta = channel.inbound.call_args.args
    chat_id = channel.gateway.sessions.get.call_args.args[0]
    assert meta["source_event_id"].startswith("reaction:")
    final(channel, chat_id, meta)
    assert json.loads(posts(channel)[0].content)["reply_to_message_id"] == MESSAGE


def test_burst_keeps_first_trigger_anchor_and_all_receipts(channel):
    async def run():
        channel.message["content"] = "First"
        await receive(channel.gateway, {"id": "source-event", "data": {"message": channel.message}})
        _, _, meta = channel.inbound.call_args.args
        session = make_session([])
        session.cfg = channel.gateway.cfg
        first = _Turn(text="First", mode="whatsapp", reply_meta=meta, queued_at=0)
        other = deepcopy(meta)
        other.update(message_id=str(UUID(int=22)), imessage_event_ids=["second"])
        other["whatsapp_reply_target"] = str(UUID(int=22))
        other["whatsapp_reply_source"]["message_id"] = str(UUID(int=22))
        second = _Turn(text="Second", mode="whatsapp", reply_meta=other, queued_at=0)
        session._batching_turn = first
        await session._queue.put(second)
        await session._batch(first)
        assert first.reply_meta["imessage_event_ids"] == ["source-event", "second"]
        assert first.reply_meta["whatsapp_reply_target"] == MESSAGE
    asyncio.run(run())


def test_new_tool_quote_suppresses_normal_host_final(channel):
    from tests.test_native_channel_sessions import Client
    async def run():
        await receive(channel.gateway, {"id": "source-event", "data": {"message": channel.message}})
        _, _, meta = channel.inbound.call_args.args
        chat_id = channel.gateway.sessions.get.call_args.args[0]
        session, tool = bind_tool(channel, meta, chat_id)
        session.receipt_fn = channel.gateway._channel_receipt
        session.send_fn = channel.gateway.send_to_contact
        channel.gateway.sessions = NS(get=lambda _: session)
        session._turn_active = False

        class Host(Client):
            async def query(self, text):
                token = CURRENT_SESSION.set(session)
                try:
                    tool({"conversation_id": CONVERSATION, "text": "Tool answer"}, lambda: None)
                finally:
                    CURRENT_SESSION.reset(token)

        session._client = Host()
        await session._run_turn(_Turn(text="Question", mode="whatsapp", reply_meta=meta))
        assert len(posts(channel)) == 1
        assert json.loads(posts(channel)[0].content)["reply_to_message_id"] == MESSAGE
        store = IMessageState(channel.gateway.cfg, channel="whatsapp")
        assert store.pending_replies() == []
        assert store.interrupted() == []
    asyncio.run(run())


def test_legacy_tool_restart_preserves_generated_key_without_quote(channel):
    chat_id, _, meta = admit(channel)
    meta.pop("whatsapp_reply_source")
    # Reconstruct a pre-upgrade pending receipt, including its old reply target.
    with channel.gateway._channel_store("whatsapp")._db() as db:
        db.execute("UPDATE receipts SET meta=?", (json.dumps(meta),))
    persisted = IMessageState(channel.gateway.cfg, channel="whatsapp").replay_pending()[0]["meta"]
    session, tool = bind_tool(channel, persisted, chat_id)
    token = CURRENT_SESSION.set(session)
    try:
        tool({"conversation_id": CONVERSATION, "text": "Answer"}, lambda: None)
    finally:
        CURRENT_SESSION.reset(token)
    request = posts(channel)[0]
    body = {"conversation_id": CONVERSATION, "text": "Answer"}
    assert json.loads(request.content) == body
    expected = "claude-wa-tool-" + hashlib.sha256(json.dumps(
        [IDENTITY, ["source-event"], body], sort_keys=True).encode()).hexdigest()
    assert request.headers["Idempotency-Key"] == expected


def test_uncertain_automatic_quote_is_not_replayed_after_restart(channel):
    chat_id, _, meta = admit(channel)
    channel.identity.send_whatsapp = Mock(side_effect=httpx.ReadTimeout("lost response"))
    with pytest.raises(httpx.ReadTimeout):
        final(channel, chat_id, meta)
    request = channel.identity.send_whatsapp.call_args.kwargs
    assert request["reply_to_message_id"] == MESSAGE
    restarted = IMessageState(channel.gateway.cfg, channel="whatsapp")
    assert restarted.replay_pending() == []
    assert restarted.pending_replies() == []
    saved = restarted.interrupted()[0]["meta"]
    assert saved["whatsapp_send_key"] == request["idempotency_key"]
    with pytest.raises(PermissionError):
        asyncio.run(send_reply(channel.gateway, chat_id, "Answer", {**saved, "final_reply": True}))
    assert channel.identity.send_whatsapp.call_count == 1


def test_confirmed_quote_allows_deliberate_distinct_tool_send(channel):
    chat_id, _, meta = admit(channel)
    session, tool = bind_tool(channel, meta, chat_id)
    token = CURRENT_SESSION.set(session)
    try:
        tool({"conversation_id": CONVERSATION, "text": "First answer"}, lambda: None)
        saved = IMessageState(channel.gateway.cfg, channel="whatsapp").interrupted()[0]["meta"]
        assert saved["whatsapp_quote_accepted"] is True
        session._current_turn.reply_meta = saved
        tool({"conversation_id": CONVERSATION, "text": "Additional requested message"}, lambda: None)
        assert len(posts(channel)) == 2
        assert {json.loads(r.content)["text"] for r in posts(channel)} == {"First answer", "Additional requested message"}
        assert all(json.loads(r.content)["reply_to_message_id"] == MESSAGE for r in posts(channel))
        assert session._current_channel_tool_delivery
        with pytest.raises(ReplyPreflightRejected):
            asyncio.run(send_reply(channel.gateway, chat_id, "Automatic final", saved))
    finally:
        CURRENT_SESSION.reset(token)


def test_tool_rechecks_ownership_after_persisting_quote_intent(channel, monkeypatch):
    chat_id, _, meta = admit(channel)
    session, tool = bind_tool(channel, meta, chat_id)
    original_mark = IMessageState.mark

    def mark_then_cancel(store, *args, **kwargs):
        original_mark(store, *args, **kwargs)
        session._turn_active = False

    def authorize():
        if not session._turn_active:
            raise ValueError("The originating turn has ended")

    monkeypatch.setattr(IMessageState, "mark", mark_then_cancel)
    token = CURRENT_SESSION.set(session)
    try:
        with pytest.raises(ValueError, match="originating turn"):
            tool({"conversation_id": CONVERSATION, "text": "Answer"}, authorize)
        assert posts(channel) == []
    finally:
        CURRENT_SESSION.reset(token)


@pytest.mark.parametrize("kind,answer", [("permission", "yes"), ("poll", "1")])
def test_accepted_quote_still_allows_separate_permission_and_poll(channel, kind, answer):
    chat_id, _, meta = admit(channel)

    async def run():
        session, tool = bind_tool(channel, meta, chat_id)
        session.receipt_fn = channel.gateway._channel_receipt
        sent = asyncio.Event()
        prompt_routes = []

        async def send(chat, text, mode, route):
            prompt_routes.append(deepcopy(route))
            await send_reply(channel.gateway, chat, text, route)
            sent.set()

        session.send_fn = send
        token = CURRENT_SESSION.set(session)
        try:
            tool({"conversation_id": CONVERSATION, "text": "Starting work"}, lambda: None)
        finally:
            CURRENT_SESSION.reset(token)
        assert meta["whatsapp_quote_accepted"]
        prompt = asyncio.create_task(session._escalate(kind, "Choose an option"))
        await asyncio.wait_for(sent.wait(), 2)
        await session.handle_inbound(answer, "whatsapp", {
            **meta, "message_id": str(UUID(int=98)), "source_event_id": "answer-event",
            "imessage_event_ids": ["answer-event"], "raw_text": answer})
        assert await prompt == answer
        assert len(posts(channel)) == 2
        tool_request, prompt_request = posts(channel)
        assert json.loads(prompt_request.content) == {
            "conversation_id": CONVERSATION, "text": "Choose an option", "reply_to_message_id": MESSAGE}
        assert prompt_request.headers["Idempotency-Key"] != tool_request.headers["Idempotency-Key"]
        operation_id = prompt_routes[0]["whatsapp_interaction_id"]
        expected = "claude-wa-" + hashlib.sha256(json.dumps([
            channel.gateway.cfg.base_url, IDENTITY, CONVERSATION, ["source-event"],
            "Choose an option", {"reply_to_message_id": MESSAGE}, operation_id], sort_keys=True).encode()).hexdigest()
        assert prompt_request.headers["Idempotency-Key"] == expected
        with pytest.raises(ReplyPreflightRejected):
            await send_reply(channel.gateway, chat_id, "Automatic final", {**meta, "final_reply": True})
        assert len(posts(channel)) == 2

    asyncio.run(run())


@pytest.mark.parametrize("include", [None, True])
def test_registered_list_tool_can_discover_existing_large_groups(channel, include):
    from inkbox_claude.channel_tools import build_channel_tools
    tool = next(t for t in build_channel_tools(channel.client, "agent", channel.gateway.cfg)
                if t.name == "inkbox_list_whatsapp_conversations")
    assert tool.input_schema["properties"]["include_groups"]["type"] == "boolean"
    result = asyncio.run(tool.handler({} if include is None else {"include_groups": include}))
    assert not result.get("isError"), result
    request = channel.requests[-1]
    assert request.url.params["include_groups"] == ("true" if include else "false")
    rows = json.loads(result["content"][0]["text"])
    assert len(rows) == (1 if include else 0)
    if include:
        assert rows[0]["is_group"] and len(rows[0]["participants"]) == 9


def test_interaction_marker_cannot_bypass_unconfirmed_quote_fence(channel):
    chat_id, _, meta = admit(channel)
    meta.update(whatsapp_explicit_quote_attempted=True, whatsapp_quote_accepted=False,
                whatsapp_interaction_id="distinct-permission")
    with pytest.raises(ReplyPreflightRejected):
        asyncio.run(send_reply(channel.gateway, chat_id, "Allow action?", meta))
    assert posts(channel) == []


def test_permission_prompt_rechecks_owner_after_source_read(channel):
    chat_id, _, meta = admit(channel)

    async def run():
        session, tool = bind_tool(channel, meta, chat_id)
        session.send_fn = channel.gateway.send_to_contact
        token = CURRENT_SESSION.set(session)
        try:
            tool({"conversation_id": CONVERSATION, "text": "Starting work"}, lambda: None)
        finally:
            CURRENT_SESSION.reset(token)
        original = channel.identity.get_whatsapp_message

        def revoked_source(message_id):
            value = original(message_id)
            session._current_turn.cancelled = True
            return value

        channel.identity.get_whatsapp_message = revoked_source
        with pytest.raises(PermissionError, match="ownership"):
            await session._escalate("permission", "Allow action?")
        assert session.pending is None
        assert len(posts(channel)) == 1

    asyncio.run(run())
