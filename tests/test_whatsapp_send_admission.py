"""Causal admission races and exact send recovery using offline HTTP."""

import asyncio
from copy import deepcopy
import json
import threading
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest
from inkbox import InkboxAPIError

from inkbox_claude.channel_tools import build_channel_tools
from inkbox_claude.imessage import IMessageState
from inkbox_claude.sessions import _Turn
from inkbox_claude.tools import CURRENT_SESSION
from inkbox_claude.whatsapp import ReplyPreflightRejected, send_reply
from inkbox_claude.whatsapp_v2 import receive_reaction
from tests.test_native_channel_sessions import Client
from tests.test_whatsapp_autoquote import admit, bind_tool, posts
from tests.test_whatsapp_v2 import channel as channel, CONVERSATION, IDENTITY


@pytest.fixture(autouse=True)
def bind_identity(channel):
    channel.client.get_identity = Mock(return_value=channel.identity)


def test_concurrent_removal_cannot_be_followed_by_older_applied_context(channel):
    async def run():
        entered, release = asyncio.Event(), asyncio.Event()
        calls = 0

        async def contact(**kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                entered.set()
                await release.wait()
            return None

        channel.gateway._resolve_contact_full = contact
        applied = deepcopy(channel.reaction)
        first = asyncio.create_task(receive_reaction(channel.gateway, {"data": {"reaction": applied}}))
        await asyncio.wait_for(entered.wait(), 2)
        channel.reaction.update(revision=3, status="removed")
        removed = deepcopy(channel.reaction)
        second = asyncio.create_task(receive_reaction(channel.gateway, {"data": {"reaction": removed}}))
        # Allow a concurrent removal to finish if it is not serialized.
        try:
            await asyncio.wait_for(asyncio.shield(second), 0.5)
        except TimeoutError:
            pass
        release.set()
        await asyncio.gather(first, second)
        revisions = [call.args[2]["reaction_revision"] for call in channel.inbound.call_args_list]
        assert revisions == sorted(revisions) and revisions[-1] == 3
        assert len(revisions) == 2
        await receive_reaction(channel.gateway, {"data": {"reaction": applied}})
        await receive_reaction(channel.gateway, {"data": {"reaction": removed}})
        assert channel.inbound.await_count == 2

    asyncio.run(run())


def corrupt_accepted_response(channel, error):
    original = channel.client.whatsapp._http.post

    def accepted(*args, **kwargs):
        result = original(*args, **kwargs)
        if error is KeyError:
            result.pop("message")
        else:
            result["message"]["id"] = "not-a-uuid"
        return result

    channel.client.whatsapp._http.post = accepted
    return original


@pytest.mark.parametrize("error", [KeyError, ValueError])
@pytest.mark.parametrize("legacy", [False, True])
def test_plain_decode_failure_retains_exact_intent_key_and_restart_fence(channel, error, legacy):
    chat, _, meta = admit(channel)
    if legacy:
        meta.pop("whatsapp_reply_source")
    session, tool = bind_tool(channel, meta, chat)
    args = {"conversation_id": CONVERSATION, "text": "Answer", "idempotency_key": "original-plain"}
    original = corrupt_accepted_response(channel, error)
    token = CURRENT_SESSION.set(session)
    try:
        with pytest.raises(error) as raised:
            tool(dict(args), lambda: None)
        assert raised.value.idempotency_key == "original-plain"
        assert session._current_channel_tool_delivery
        request = posts(channel)[0]
        assert json.loads(request.content) == {"conversation_id": CONVERSATION, "text": "Answer"}
        restarted = IMessageState(channel.gateway.cfg, channel="whatsapp")
        assert restarted.replay_pending() == [] and restarted.pending_replies() == []
        saved = restarted.interrupted()[0]["meta"]
        assert saved["whatsapp_plain_send"] == {
            "key": "original-plain", "intent": json.loads(request.content), "outcome": "uncertain"}
        session._current_turn.reply_meta = saved
        for change in ({"text": "Changed"}, {"idempotency_key": "replacement"},
                       {"reply_to_message_id": meta["message_id"]}):
            with pytest.raises(ReplyPreflightRejected):
                tool({**args, **change}, lambda: None)
        with pytest.raises(ReplyPreflightRejected):
            asyncio.run(send_reply(channel.gateway, chat, "Automatic final", {**saved, "final_reply": True}))
        assert len(posts(channel)) == 1
        channel.client.whatsapp._http.post = original
        tool(dict(args), lambda: None)
        assert len(posts(channel)) == 2
        assert posts(channel)[1].content == request.content
        assert posts(channel)[1].headers["Idempotency-Key"] == request.headers["Idempotency-Key"]
        assert saved["whatsapp_plain_send"]["outcome"] == "accepted"
    finally:
        CURRENT_SESSION.reset(token)


@pytest.mark.parametrize("error", [KeyError, ValueError])
def test_registered_plain_decode_error_reports_key_and_suppresses_host_final(channel, error):
    chat, _, meta = admit(channel)
    meta.pop("whatsapp_reply_source")
    session, _ = bind_tool(channel, meta, chat)
    session.send_fn = channel.gateway.send_to_contact
    session.receipt_fn = channel.gateway._channel_receipt
    channel.gateway.sessions = NS(get=lambda _: session)
    session._turn_active = False
    tool = next(t for t in build_channel_tools(channel.client, "agent", channel.gateway.cfg)
                if t.name == "inkbox_send_whatsapp")
    corrupt_accepted_response(channel, error)

    class Host(Client):
        async def query(self, text):
            token = CURRENT_SESSION.set(session)
            try:
                result = await tool.handler({"conversation_id": CONVERSATION, "text": "Answer"})
                payload = json.loads(result["content"][0]["text"])
                assert "error" in payload
                assert payload["idempotency_key"] == posts(channel)[0].headers["Idempotency-Key"]
                assert "do not send a new copy" in payload["recovery"]
            finally:
                CURRENT_SESSION.reset(token)

    session._client = Host()
    asyncio.run(session._run_turn(_Turn(text="Question", mode="whatsapp", reply_meta=meta)))
    assert len(posts(channel)) == 1
    store = IMessageState(channel.gateway.cfg, channel="whatsapp")
    assert store.pending_replies() == [] and store.interrupted() == []


def test_definitive_plain_rejection_allows_recovery_without_changing_original_key_payload(channel):
    chat, _, meta = admit(channel)
    meta.pop("whatsapp_reply_source")
    session, tool = bind_tool(channel, meta, chat)
    original = channel.identity.send_whatsapp
    channel.identity.send_whatsapp = Mock(side_effect=InkboxAPIError(429, {"error": "whatsapp_daily_limit_exceeded"}))
    token = CURRENT_SESSION.set(session)
    try:
        args = {"conversation_id": CONVERSATION, "text": "Answer", "idempotency_key": "plain-rejected"}
        with pytest.raises(InkboxAPIError):
            tool(dict(args), lambda: None)
        assert meta["whatsapp_plain_send"]["outcome"] == "rejected"
        assert not session._current_channel_tool_delivery
        with pytest.raises(ReplyPreflightRejected):
            tool({**args, "text": "Changed"}, lambda: None)
        channel.identity.send_whatsapp = original
        tool(dict(args), lambda: None)
        tool({**args, "text": "Additional requested answer", "idempotency_key": "distinct"}, lambda: None)
        assert len(posts(channel)) == 2
    finally:
        CURRENT_SESSION.reset(token)


@pytest.mark.parametrize("invalid", [{"text": "x" * 4001}, {"idempotency_key": "\n"}])
def test_plain_preflight_rejection_does_not_checkpoint_or_block_next_send(channel, invalid):
    chat, _, meta = admit(channel)
    meta.pop("whatsapp_reply_source")
    session, tool = bind_tool(channel, meta, chat)
    token = CURRENT_SESSION.set(session)
    try:
        args = {"conversation_id": CONVERSATION, "text": "Answer", "idempotency_key": "valid-key"}
        with pytest.raises(ValueError):
            tool({**args, **invalid}, lambda: None)
        assert posts(channel) == [] and "whatsapp_plain_send" not in meta
        assert not session._current_channel_tool_delivery
        tool(args, lambda: None)
        assert len(posts(channel)) == 1
    finally:
        CURRENT_SESSION.reset(token)


def test_definitive_retry_rejection_cannot_clear_prior_plain_uncertainty(channel):
    chat, _, meta = admit(channel)
    meta.pop("whatsapp_reply_source")
    session, tool = bind_tool(channel, meta, chat)
    corrupt_accepted_response(channel, KeyError)
    args = {"conversation_id": CONVERSATION, "text": "Answer", "idempotency_key": "original-plain"}
    token = CURRENT_SESSION.set(session)
    try:
        with pytest.raises(KeyError):
            tool(dict(args), lambda: None)
        channel.identity.send_whatsapp = Mock(side_effect=InkboxAPIError(429, {"error": "whatsapp_daily_limit_exceeded"}))
        with pytest.raises(InkboxAPIError):
            tool(dict(args), lambda: None)
        assert meta["whatsapp_plain_send"]["outcome"] == "uncertain"
        with pytest.raises(ReplyPreflightRejected):
            asyncio.run(send_reply(channel.gateway, chat, "Replacement", meta))
        assert len(posts(channel)) == 1
    finally:
        CURRENT_SESSION.reset(token)


def test_cancelled_reaction_waiter_releases_dedup_for_redelivery(channel):
    async def run():
        entered, release, waiter_started = asyncio.Event(), asyncio.Event(), asyncio.Event()
        original_begin = channel.gateway._dedup_begin

        def begin(receipt):
            if receipt.endswith(":3:removed"):
                waiter_started.set()
            return original_begin(receipt)

        channel.gateway._dedup_begin = begin

        async def contact(**kwargs):
            entered.set()
            await release.wait()
            return None

        channel.gateway._resolve_contact_full = contact
        first = asyncio.create_task(receive_reaction(channel.gateway, {"data": {"reaction": deepcopy(channel.reaction)}}))
        await asyncio.wait_for(entered.wait(), 2)
        channel.reaction.update(revision=3, status="removed")
        event = {"data": {"reaction": deepcopy(channel.reaction)}}
        waiter = asyncio.create_task(receive_reaction(channel.gateway, event))
        await asyncio.wait_for(waiter_started.wait(), 2)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        release.set()
        await first
        await receive_reaction(channel.gateway, event)
        assert [c.args[2]["reaction_revision"] for c in channel.inbound.call_args_list] == [2, 3]
        assert not channel.gateway._inflight_request_ids
        assert not channel.gateway._whatsapp_reaction_locks

    asyncio.run(run())


def test_plain_intent_is_durable_before_post(channel):
    chat, _, meta = admit(channel)
    meta.pop("whatsapp_reply_source")
    session, tool = bind_tool(channel, meta, chat)
    original = channel.client.whatsapp._http.post

    def post(*args, **kwargs):
        saved = IMessageState(channel.gateway.cfg, channel="whatsapp").interrupted()[0]["meta"]
        assert saved["whatsapp_plain_send"] == {
            "key": kwargs["headers"]["Idempotency-Key"], "intent": kwargs["json"], "outcome": "pending"}
        return original(*args, **kwargs)

    channel.client.whatsapp._http.post = post
    token = CURRENT_SESSION.set(session)
    try:
        tool({"conversation_id": CONVERSATION, "text": "Answer"}, lambda: None)
        assert len(posts(channel)) == 1
    finally:
        CURRENT_SESSION.reset(token)


@pytest.mark.parametrize("selector", ["implicit", "conversation", "sender"])
def test_source_owned_plain_rejection_fences_replacement_across_restart(channel, selector):
    chat, _, meta = admit(channel, legacy=False)
    session, tool = bind_tool(channel, meta, chat)
    original = channel.identity.send_whatsapp
    rejected = Mock(side_effect=InkboxAPIError(429, {"error": "whatsapp_daily_limit_exceeded"}))
    channel.identity.send_whatsapp = rejected
    args = {"text": "Answer", "idempotency_key": "source-rejected"}
    if selector == "conversation":
        args["conversation_id"] = CONVERSATION
    elif selector == "sender":
        args["to"] = meta["sender"]
    token = CURRENT_SESSION.set(session)
    try:
        with pytest.raises(InkboxAPIError):
            tool(dict(args), lambda: None)
        assert session._current_channel_tool_delivery
        saved = IMessageState(channel.gateway.cfg, channel="whatsapp").interrupted()[0]["meta"]
        assert saved["whatsapp_plain_send"]["outcome"] == "rejected"
        session._current_turn.reply_meta = saved
        for change in ({"idempotency_key": "replacement"}, {"text": "Changed"}):
            with pytest.raises(ReplyPreflightRejected):
                tool({**args, **change}, lambda: None)
        with pytest.raises(ReplyPreflightRejected):
            asyncio.run(send_reply(channel.gateway, chat, "Automatic final", {**saved, "final_reply": True}))
        assert rejected.call_count == 1
        channel.identity.send_whatsapp = original
        tool(dict(args), lambda: None)
        assert len(posts(channel)) == 1
        assert posts(channel)[0].headers["Idempotency-Key"] == "source-rejected"
        assert json.loads(posts(channel)[0].content) == saved["whatsapp_plain_send"]["intent"]
    finally:
        CURRENT_SESSION.reset(token)


@pytest.mark.parametrize("destination", [{"to": "+15550000099"}, {"conversation_id": "00000000-0000-0000-0000-000000000099"}])
def test_source_rejection_allows_explicit_outreach_without_replacing_source_intent(channel, destination):
    chat, _, meta = admit(channel, legacy=False)
    session, tool = bind_tool(channel, meta, chat)
    original = channel.identity.send_whatsapp
    channel.identity.send_whatsapp = Mock(side_effect=InkboxAPIError(429, {"error": "whatsapp_daily_limit_exceeded"}))
    token = CURRENT_SESSION.set(session)
    try:
        with pytest.raises(InkboxAPIError):
            tool({"text": "Source answer", "idempotency_key": "source-rejected"}, lambda: None)
        retained = deepcopy(meta["whatsapp_plain_send"])
        channel.identity.send_whatsapp = original
        tool({**destination, "text": "Requested outreach", "idempotency_key": "outreach"}, lambda: None)
        assert len(posts(channel)) == 1
        assert json.loads(posts(channel)[0].content) == {**destination, "text": "Requested outreach"}
        assert meta["whatsapp_plain_send"] == retained
        with pytest.raises(ReplyPreflightRejected):
            asyncio.run(send_reply(channel.gateway, chat, "Automatic final", {**meta, "final_reply": True}))
        assert len(posts(channel)) == 1
    finally:
        CURRENT_SESSION.reset(token)
