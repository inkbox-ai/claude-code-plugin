"""Recipient-selected groups do not own the originating conversation's reply."""

import asyncio
from copy import deepcopy
import hashlib
import json
from types import SimpleNamespace as NS
from unittest.mock import Mock
from uuid import UUID

import pytest
from inkbox import InkboxAPIError

from inkbox_claude.channel_tools import build_channel_tools
from inkbox_claude.imessage import IMessageState
from inkbox_claude.sessions import _Turn
from inkbox_claude.tools import CURRENT_SESSION
from inkbox_claude.whatsapp import ReplyPreflightRejected, delivery_target
from tests.test_native_channel_sessions import Client
from tests.test_whatsapp_autoquote import admit, bind_tool, final, posts
from tests.test_whatsapp_send_admission import corrupt_accepted_response
from tests.test_whatsapp_v2 import channel as channel, CONVERSATION, IDENTITY, MESSAGE

NUMBERS = ["+15550000002", "+15550000004"]
OTHER = str(UUID(int=77))


def setup_group(channel):
    chat, _, meta = admit(channel, is_group=True, participants=NUMBERS)
    channel.client.get_identity = Mock(return_value=channel.identity)
    channel.identity.get_whatsapp_conversation = Mock(return_value=NS(
        id=CONVERSATION, agent_identity_id=IDENTITY, is_group=True,
        unresolved_participant_count=0, participants=NUMBERS))
    original = channel.client.whatsapp._http.post

    def post(*args, **kwargs):
        result = original(*args, **kwargs)
        result["message"]["conversation_id"] = kwargs["json"].get("conversation_id", OTHER)
        return result

    channel.client.whatsapp._http.post = post
    session, tool = bind_tool(channel, meta, chat)
    return chat, meta, session, tool


@pytest.mark.parametrize("named", [False, True])
@pytest.mark.parametrize("selector", ["recipients", "source", "other"])
def test_same_members_are_not_conversation_identity(channel, named, selector):
    chat, meta, session, tool = setup_group(channel)
    args = {"to": list(reversed(NUMBERS)), "text": "Answer"}
    if named:
        args["group_name"] = " New project "
    if selector != "recipients":
        args = {"conversation_id": CONVERSATION if selector == "source" else OTHER, "text": "Answer"}
    token = CURRENT_SESSION.set(session)
    try:
        tool(args, lambda: None)
        body = json.loads(posts(channel)[0].content)
        if selector == "source":
            assert body == {"conversation_id": CONVERSATION, "text": "Answer", "reply_to_message_id": MESSAGE}
            assert session._current_channel_tool_delivery
            with pytest.raises(ReplyPreflightRejected):
                final(channel, chat, meta)
        else:
            expected = ({"to": NUMBERS, **({"group_name": "New project"} if named else {})}
                        if selector == "recipients" else {"conversation_id": OTHER})
            assert body == {**expected, "text": "Answer"}
            expected_key = "claude-wa-tool-" + hashlib.sha256(json.dumps(
                [IDENTITY, ["source-event"], body], sort_keys=True).encode()).hexdigest()
            assert posts(channel)[0].headers["Idempotency-Key"] == expected_key
            assert not session._current_channel_tool_delivery
            assert not meta.get("whatsapp_plain_send")
            assert not meta.get("whatsapp_explicit_quote_attempted")
            channel.identity.get_whatsapp_conversation.assert_not_called()
            final(channel, chat, meta, "Source final")
            assert json.loads(posts(channel)[1].content) == {
                "conversation_id": CONVERSATION, "text": "Source final", "reply_to_message_id": MESSAGE}
        channel.identity.get_whatsapp_conversation.assert_called_once_with(CONVERSATION)
    finally:
        CURRENT_SESSION.reset(token)


@pytest.mark.parametrize("allowed", [False, True])
def test_recipient_policy_does_not_use_source_roster(channel, allowed):
    chat, meta, session, tool = setup_group(channel)
    cfg = channel.gateway.cfg
    cfg.allow_all_users, cfg.allowed_users = False, NUMBERS.copy()
    chosen = NUMBERS if allowed else [NUMBERS[0], "+15550000003"]
    # The source roster changed, but it is not the selected destination.
    channel.identity.get_whatsapp_conversation.return_value.participants = [*NUMBERS, "+15550000005"]
    token = CURRENT_SESSION.set(session)
    try:
        if allowed:
            tool({"to": chosen, "text": "New group"}, lambda: None)
            assert json.loads(posts(channel)[0].content)["to"] == chosen
        else:
            with pytest.raises(ReplyPreflightRejected):
                tool({"to": chosen, "text": "New group"}, lambda: None)
            assert posts(channel) == []
        channel.identity.get_whatsapp_conversation.assert_not_called()
        assert not session._current_channel_tool_delivery
        with pytest.raises(ReplyPreflightRejected):
            final(channel, chat, meta)
        assert len(posts(channel)) == int(allowed)
    finally:
        CURRENT_SESSION.reset(token)


@pytest.mark.parametrize("legacy", [False, True])
def test_uncertain_group_recovery_preserves_bytes_key_and_fences_final(channel, legacy):
    chat, meta, session, tool = setup_group(channel)
    intent = {"to": NUMBERS, "group_name": "New project", "text": "Answer"}
    key = "original-group-key"
    if legacy:
        # A saved pre-upgrade attempt was mistakenly stored as a source reply.
        channel.identity.send_whatsapp(**intent, idempotency_key=key)
        old_request = posts(channel)[0]
        channel.requests.clear()
        meta["whatsapp_plain_send"] = {"key": key, "intent": deepcopy(intent), "outcome": "uncertain"}
        channel.gateway._channel_store("whatsapp").mark(meta, "running", chat_id=chat)
    original = corrupt_accepted_response(channel, KeyError)
    token = CURRENT_SESSION.set(session)
    try:
        with pytest.raises(KeyError):
            tool({**intent, "idempotency_key": key}, lambda: None)
        request = posts(channel)[0]
        assert json.loads(request.content) == intent
        assert request.headers["Idempotency-Key"] == key
        if legacy:
            assert request.content == old_request.content
            assert request.headers["Idempotency-Key"] == old_request.headers["Idempotency-Key"]
        assert not session._current_channel_tool_delivery
        restarted = IMessageState(channel.gateway.cfg, channel="whatsapp")
        assert restarted.replay_pending() == [] and restarted.pending_replies() == []
        saved = restarted.interrupted()[0]["meta"]
        session._current_turn.reply_meta = saved
        for change in ({"idempotency_key": "new-key"}, {"text": "Changed"}, {"group_name": "Changed"},
                       {"to": [NUMBERS[0], "+15550000003"]}):
            with pytest.raises(ReplyPreflightRejected):
                tool({**intent, "idempotency_key": key, **change}, lambda: None)
        assert len(posts(channel)) == 1
        channel.client.whatsapp._http.post = original
        with pytest.raises(ReplyPreflightRejected):
            final(channel, chat, saved, "Source final")
        assert len(posts(channel)) == 1
        tool({**intent, "idempotency_key": key}, lambda: None)
        assert posts(channel)[1].content == request.content
        assert posts(channel)[1].headers["Idempotency-Key"] == key
        saved = IMessageState(channel.gateway.cfg, channel="whatsapp").interrupted()[0]["meta"]
        session._current_turn.reply_meta = saved
        final(channel, chat, saved, "Source final")
        assert json.loads(posts(channel)[2].content)["conversation_id"] == CONVERSATION
        # A later intentional operation is not deduplicated by payload alone.
        tool({**intent, "idempotency_key": "later-intent"}, lambda: None)
        assert posts(channel)[3].content == request.content
        assert posts(channel)[3].headers["Idempotency-Key"] == "later-intent"
    finally:
        CURRENT_SESSION.reset(token)


@pytest.mark.parametrize("recipient", [NUMBERS[0], [NUMBERS[0]]])
def test_exact_direct_recipient_correlation_is_preserved(recipient):
    meta = {"channel": "whatsapp", "conversation_id": CONVERSATION,
            "conversation_kind": "direct", "sender": NUMBERS[0], "participants": [NUMBERS[0]]}
    assert delivery_target(meta, recipient=recipient) == CONVERSATION
    meta["conversation_kind"] = "group"
    assert delivery_target(meta, recipient=recipient) != CONVERSATION


@pytest.mark.parametrize("outcome", ["pending", "uncertain", "accepted", "rejected"])
def test_legacy_recipient_receipt_fences_only_unresolved_saved_source_final(channel, outcome):
    chat, meta, _, _ = setup_group(channel)
    meta["whatsapp_reply_source"] = None
    meta["whatsapp_plain_send"] = {"key": "legacy-group-key",
        "intent": {"to": NUMBERS, "text": "Answer"}, "outcome": outcome}
    store = channel.gateway._channel_store("whatsapp")
    store.mark(meta, "reply_pending", reply="Source final", chat_id=chat)
    pending = IMessageState(channel.gateway.cfg, channel="whatsapp").pending_replies()[0]
    if outcome in {"pending", "uncertain"}:
        with pytest.raises(ReplyPreflightRejected):
            final(channel, chat, pending["meta"], pending["reply"])
        assert posts(channel) == []
        return
    final(channel, chat, pending["meta"], pending["reply"])
    request = posts(channel)[0]
    assert json.loads(request.content) == {"conversation_id": CONVERSATION, "text": "Source final"}
    expected_key = "claude-wa-" + hashlib.sha256(json.dumps([
        channel.gateway.cfg.base_url, IDENTITY, CONVERSATION, ["source-event"], "Source final"
    ], sort_keys=True).encode()).hexdigest()
    assert request.headers["Idempotency-Key"] == expected_key
    assert meta["whatsapp_plain_send"]["outcome"] == outcome


@pytest.mark.parametrize("outcome", ["accepted", "rejected", "uncertain", "recovered"])
def test_native_host_final_waits_for_group_resolution_without_retrying_it(channel, outcome):
    chat, meta, session, _ = setup_group(channel)
    session.send_fn = channel.gateway.send_to_contact
    session.receipt_fn = channel.gateway._channel_receipt
    channel.gateway.sessions = NS(get=lambda _: session)
    session._turn_active = False
    tool = next(t for t in build_channel_tools(channel.client, "agent", channel.gateway.cfg)
                if t.name == "inkbox_send_whatsapp")
    uncertain = outcome in {"uncertain", "recovered"}
    if uncertain:
        original = corrupt_accepted_response(channel, KeyError)
    elif outcome == "rejected":
        original_send = channel.identity.send_whatsapp
        channel.identity.send_whatsapp = Mock(side_effect=InkboxAPIError(403, {"error": "whatsapp_access_required"}))

    class Host(Client):
        async def query(self, text):
            token = CURRENT_SESSION.set(session)
            try:
                result = await tool.handler({"to": NUMBERS, "text": "New group", "idempotency_key": "group-key"})
                if uncertain:
                    payload = json.loads(result["content"][0]["text"])
                    assert payload["idempotency_key"] == "group-key"
                    assert "do not send a new copy" in payload["recovery"]
                    channel.client.whatsapp._http.post = original
                    if outcome == "recovered":
                        result = await tool.handler({"to": NUMBERS, "text": "New group", "idempotency_key": "group-key"})
                        assert not result.get("isError")
                        assert posts(channel)[1].content == posts(channel)[0].content
                        assert posts(channel)[1].headers["Idempotency-Key"] == "group-key"
                elif outcome == "rejected":
                    assert json.loads(result["content"][0]["text"])["error"] == "whatsapp_access_required"
                    channel.identity.send_whatsapp = original_send
                else:
                    assert not result.get("isError")
            finally:
                CURRENT_SESSION.reset(token)

    session._client = Host()
    asyncio.run(session._run_turn(_Turn(text="Question", mode="whatsapp", reply_meta=meta)))
    assert len(posts(channel)) == {"accepted": 2, "rejected": 1, "uncertain": 1, "recovered": 3}[outcome]
    if outcome != "rejected":
        assert json.loads(posts(channel)[0].content) == {"to": NUMBERS, "text": "New group"}
    if outcome == "uncertain":
        assert meta["whatsapp_recipient_sends"]["group-key"]["outcome"] == "uncertain"
        assert not session._current_channel_tool_delivery
        return
    assert json.loads(posts(channel)[-1].content) == {
        "conversation_id": CONVERSATION, "text": "Answer", "reply_to_message_id": MESSAGE}
    assert posts(channel)[-1].headers["Idempotency-Key"] != "group-key"


@pytest.mark.parametrize("status,code", [(403, "whatsapp_access_required"), (429, "whatsapp_daily_limit_exceeded")])
@pytest.mark.parametrize("prior_uncertainty", [False, True])
def test_group_policy_rejection_does_not_clear_prior_uncertainty(channel, status, code, prior_uncertainty):
    chat, meta, session, tool = setup_group(channel)
    args = {"to": NUMBERS, "text": "New group", "idempotency_key": "group-key"}
    original = channel.identity.send_whatsapp
    token = CURRENT_SESSION.set(session)
    try:
        if prior_uncertainty:
            original_post = corrupt_accepted_response(channel, KeyError)
            with pytest.raises(KeyError):
                tool(dict(args), lambda: None)
            channel.client.whatsapp._http.post = original_post
        channel.identity.send_whatsapp = Mock(side_effect=InkboxAPIError(status, {"error": code}))
        with pytest.raises(InkboxAPIError):
            tool(dict(args), lambda: None)
        channel.identity.send_whatsapp.assert_called_once()
        saved = IMessageState(channel.gateway.cfg, channel="whatsapp").interrupted()[0]["meta"]
        assert saved["whatsapp_recipient_sends"]["group-key"]["outcome"] == (
            "uncertain" if prior_uncertainty else "rejected")
        if prior_uncertainty:
            with pytest.raises(ReplyPreflightRejected):
                tool({**args, "idempotency_key": "replacement"}, lambda: None)
        assert not session._current_channel_tool_delivery
        channel.identity.send_whatsapp = original
        if prior_uncertainty:
            with pytest.raises(ReplyPreflightRejected):
                final(channel, chat, saved, "Source final")
        else:
            final(channel, chat, saved, "Source final")
        assert len(posts(channel)) == 1
    finally:
        CURRENT_SESSION.reset(token)


def test_new_group_attempt_is_saved_before_post(channel):
    chat, meta, session, tool = setup_group(channel)
    original = channel.client.whatsapp._http.post

    def post(*args, **kwargs):
        saved = IMessageState(channel.gateway.cfg, channel="whatsapp").interrupted()[0]["meta"]
        key = kwargs["headers"]["Idempotency-Key"]
        assert saved["whatsapp_recipient_sends"][key] == {
            "key": key, "intent": kwargs["json"], "outcome": "pending"}
        assert not saved.get("whatsapp_plain_send")
        return original(*args, **kwargs)

    channel.client.whatsapp._http.post = post
    token = CURRENT_SESSION.set(session)
    try:
        tool({"to": NUMBERS, "text": "New group"}, lambda: None)
        assert len(posts(channel)) == 1
    finally:
        CURRENT_SESSION.reset(token)


@pytest.mark.parametrize("quoted", [False, True])
def test_independent_group_cannot_clear_source_attempt_fence(channel, quoted):
    chat, meta, session, tool = setup_group(channel)
    args = {"conversation_id": CONVERSATION, "text": "Source answer", "idempotency_key": "source-key"}
    if quoted:
        args["reply_to_message_id"] = MESSAGE
    original = corrupt_accepted_response(channel, KeyError)
    token = CURRENT_SESSION.set(session)
    try:
        with pytest.raises(KeyError):
            tool(dict(args), lambda: None)
        request = posts(channel)[0]
        channel.client.whatsapp._http.post = original
        with pytest.raises(ReplyPreflightRejected):
            tool({"to": NUMBERS, "text": "Independent group", "idempotency_key": "group-key"}, lambda: None)
        assert session._current_channel_tool_delivery
        with pytest.raises(ReplyPreflightRejected):
            final(channel, chat, meta)
        with pytest.raises(ReplyPreflightRejected):
            tool({**args, "idempotency_key": "replacement"}, lambda: None)
        assert len(posts(channel)) == 1
        tool(dict(args), lambda: None)
        assert posts(channel)[1].content == request.content
        assert posts(channel)[1].headers["Idempotency-Key"] == "source-key"
    finally:
        CURRENT_SESSION.reset(token)


@pytest.mark.parametrize("destination", ["group", "direct", "conversation"])
@pytest.mark.parametrize("pending", [False, True])
def test_unresolved_outreach_fences_every_send_before_overwriting_tracking(channel, destination, pending):
    chat, meta, session, tool = setup_group(channel)
    selector = {"to": NUMBERS}
    if destination == "direct":
        selector = {"to": "+15550000003"}
    elif destination == "conversation":
        selector = {"conversation_id": OTHER}
    args = {**selector, "text": "Original", "idempotency_key": "original-key"}
    original_post = channel.client.whatsapp._http.post

    def check_fence():
        snapshot = deepcopy(session._current_turn.reply_meta)
        for replacement in (
            {"conversation_id": CONVERSATION, "text": "Fresh plain", "idempotency_key": "plain-key"},
            {"conversation_id": CONVERSATION, "text": "Fresh autoquote"},
            {"conversation_id": CONVERSATION, "text": "Fresh quote", "reply_to_message_id": MESSAGE},
            {"conversation_id": OTHER, "text": "Other conversation"},
            {"to": NUMBERS, "text": "Fresh group", "idempotency_key": "group-key"},
            {"to": "+15550000003", "text": "Fresh direct"},
            {**args, "text": "Changed"},
            {**args, "idempotency_key": "replacement"},
        ):
            with pytest.raises(ReplyPreflightRejected):
                tool(replacement, lambda: None)
            assert session._current_turn.reply_meta == snapshot
        with pytest.raises(ReplyPreflightRejected):
            final(channel, chat, session._current_turn.reply_meta)
        assert not session._current_channel_tool_delivery

    def post(*post_args, **kwargs):
        if pending:
            saved = IMessageState(channel.gateway.cfg, channel="whatsapp").interrupted()[0]["meta"]
            assert saved["whatsapp_recipient_sends"]["original-key"]["outcome"] == "pending"
            check_fence()
        original_post(*post_args, **kwargs)
        raise KeyError("Accepted response could not be decoded")

    channel.client.whatsapp._http.post = post
    token = CURRENT_SESSION.set(session)
    try:
        with pytest.raises(KeyError):
            tool(dict(args), lambda: None)
        request = posts(channel)[0]
        assert len(posts(channel)) == 1
        restarted = IMessageState(channel.gateway.cfg, channel="whatsapp")
        assert restarted.replay_pending() == [] and restarted.pending_replies() == []
        saved = restarted.interrupted()[0]["meta"]
        session._current_turn.reply_meta = saved
        assert saved["whatsapp_recipient_sends"]["original-key"] == {
            "key": "original-key", "intent": {**selector, "text": "Original"}, "outcome": "uncertain"}
        check_fence()
        assert len(posts(channel)) == 1
        channel.client.whatsapp._http.post = original_post
        tool(dict(args), lambda: None)
        assert posts(channel)[1].content == request.content
        assert posts(channel)[1].headers["Idempotency-Key"] == request.headers["Idempotency-Key"]
        final(channel, chat, saved, "Resolved source final")
        assert len(posts(channel)) == 3
        assert json.loads(posts(channel)[2].content) == {
            "conversation_id": CONVERSATION, "text": "Resolved source final", "reply_to_message_id": MESSAGE}
    finally:
        CURRENT_SESSION.reset(token)


def test_uncertain_group_blocks_mutations_but_keeps_read_only_recovery(channel):
    from inkbox_claude.whatsapp import register_tools

    _, meta, session, tool = setup_group(channel)
    registry = {}
    register_tools(lambda name, description, properties, required, callback: registry.update({name: callback}),
                   channel.client, "agent", channel.gateway.cfg)
    original = corrupt_accepted_response(channel, KeyError)
    token = CURRENT_SESSION.set(session)
    try:
        with pytest.raises(KeyError):
            tool({"to": NUMBERS, "text": "Original", "idempotency_key": "group-key"}, lambda: None)
        channel.client.whatsapp._http.post = original
        snapshot = deepcopy(meta)
        for name, args in (
            ("inkbox_react_whatsapp", {"message_id": MESSAGE, "reaction": "like", "idempotency_key": "reaction-key"}),
            ("inkbox_unreact_whatsapp", {"reaction_id": MESSAGE, "idempotency_key": "remove-key"}),
            ("inkbox_upload_whatsapp_media", {"media_path": "missing-file"}),
        ):
            with pytest.raises(ReplyPreflightRejected):
                registry[name](args, lambda: None)
            assert meta == snapshot
        channel.client.message_sends.lookup = Mock(return_value=MESSAGE)
        assert registry["inkbox_lookup_whatsapp_send"]({"idempotency_key": "group-key"}, lambda: None) == {
            "message_id": MESSAGE}
        assert meta == snapshot  # Lookup alone cannot clear an unresolved operation.
        assert len(posts(channel)) == 1
    finally:
        CURRENT_SESSION.reset(token)


@pytest.mark.parametrize("quoted", [False, True])
def test_source_preflight_cannot_overwrite_group_uncertainty_created_during_roster_read(channel, quoted):
    chat, meta, session, tool = setup_group(channel)
    original_read = channel.identity.get_whatsapp_conversation
    original_post = corrupt_accepted_response(channel, KeyError)

    def read(conversation):
        with pytest.raises(KeyError):
            tool({"to": NUMBERS, "text": "New group", "idempotency_key": "group-key"}, lambda: None)
        channel.client.whatsapp._http.post = original_post
        return original_read(conversation)

    channel.identity.get_whatsapp_conversation = read
    args = {"conversation_id": CONVERSATION, "text": "Source answer", "idempotency_key": "source-key"}
    if quoted:
        args["reply_to_message_id"] = MESSAGE
    token = CURRENT_SESSION.set(session)
    try:
        with pytest.raises(ReplyPreflightRejected):
            tool(args, lambda: None)
        assert len(posts(channel)) == 1
        assert not meta.get("whatsapp_plain_send") and not meta.get("whatsapp_explicit_quote_attempted")
        assert meta["whatsapp_recipient_sends"]["group-key"]["outcome"] == "uncertain"
        with pytest.raises(ReplyPreflightRejected):
            final(channel, chat, meta)
        assert len(posts(channel)) == 1
    finally:
        CURRENT_SESSION.reset(token)


@pytest.mark.parametrize("prior_accepted", [False, True])
@pytest.mark.parametrize("pause_at", ["source", "dispatch"])
def test_native_final_rechecks_live_group_uncertainty_before_post(channel, monkeypatch, pause_at, prior_accepted):
    chat, meta, session, tool = setup_group(channel)
    session.send_fn = channel.gateway.send_to_contact
    session.receipt_fn = channel.gateway._channel_receipt
    channel.gateway.sessions = NS(get=lambda _: session)
    if prior_accepted:
        token = CURRENT_SESSION.set(session)
        try:
            tool({"to": NUMBERS, "text": "Prior accepted", "idempotency_key": "prior-key"}, lambda: None)
        finally:
            CURRENT_SESSION.reset(token)
    original_post = corrupt_accepted_response(channel, KeyError)

    def interrupt_final():
        assert "group-key" not in meta.get("whatsapp_recipient_sends", {})
        with pytest.raises(KeyError):
            tool({"to": NUMBERS, "text": "New group", "idempotency_key": "group-key"}, lambda: None)
        channel.client.whatsapp._http.post = original_post

    if pause_at == "source":
        original_read = channel.identity.get_whatsapp_message

        def read(message_id):
            interrupt_final()
            return original_read(message_id)

        channel.identity.get_whatsapp_message = read
    else:
        from inkbox_claude import tools
        original_dispatch = tools._to_thread_drained

        async def dispatch(function, *args, **kwargs):
            interrupt_final()
            return await original_dispatch(function, *args, **kwargs)

        monkeypatch.setattr(tools, "_to_thread_drained", dispatch)

    token = CURRENT_SESSION.set(session)
    try:
        channel.gateway._channel_store("whatsapp").mark(meta, "reply_pending", reply="Source final", chat_id=chat)
        asyncio.run(session._deliver_reply(session._current_turn, "Source final"))
        assert len(posts(channel)) == 1 + int(prior_accepted)
        assert json.loads(posts(channel)[-1].content) == {"to": NUMBERS, "text": "New group"}
        assert meta["whatsapp_recipient_sends"]["group-key"]["outcome"] == "uncertain"
        with IMessageState(channel.gateway.cfg, channel="whatsapp")._db() as db:
            row = db.execute("SELECT state,meta FROM receipts WHERE event_id=?", ("source-event",)).fetchone()
        assert row["state"] == "failed"
        assert json.loads(row["meta"])["whatsapp_recipient_sends"] == meta["whatsapp_recipient_sends"]
        assert not session._current_channel_tool_delivery
    finally:
        CURRENT_SESSION.reset(token)
