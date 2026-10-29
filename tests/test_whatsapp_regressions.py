"""Offline host, receipt and HTTP regressions for WhatsApp turn ownership."""

import asyncio
from contextlib import contextmanager
from copy import deepcopy
import hashlib
import json
import threading
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from inkbox_claude.imessage import IMessageState
from inkbox_claude.sessions import _Turn
from inkbox_claude.tools import CURRENT_SESSION
from inkbox_claude.whatsapp import record_status, register_tools, send_reply
from inkbox_claude.whatsapp_v2 import receive_reaction
from tests.test_native_channel_sessions import Client
from tests.test_sessions import make_session
from tests.test_whatsapp import setup as setup, CONV, MESSAGE, MESSAGE_WIRE, OWNER
from tests.test_whatsapp_v2 import channel as channel, MESSAGE as TARGET


def route(source="source", **changes):
    return {"channel": "whatsapp", "source_event_id": source,
            "imessage_event_ids": [source], "message_id": MESSAGE,
            "conversation_id": CONV, "sender": "+15555550101",
            "participants": ["+15555550101"], "conversation_kind": "direct", **changes}


def harness(setup):
    gw, _, client = setup
    session = make_session([])
    session.cfg = gw.cfg
    session.receipt_fn = gw._channel_receipt
    session.send_fn = gw.send_to_contact
    session._client = Client()
    gw.sessions = NS(get=lambda _: session, sessions={session.chat_id: session})
    registry = {}
    register_tools(lambda name, description, properties, required, callback: registry.update({name: callback}),
                   client, "agent", gw.cfg)
    return gw, session, client, registry, gw._channel_store("whatsapp")


def row(store, source="source"):
    with store._db() as db:
        result = dict(db.execute("SELECT * FROM receipts WHERE event_id=?", (source,)).fetchone())
    result["meta"] = json.loads(result["meta"])
    return result


@pytest.mark.parametrize("finish", [False, True])
def test_tool_acceptance_does_not_finish_host_receipt(setup, monkeypatch, finish):
    async def run():
        gw, session, client, registry, store = harness(setup)
        owner = {"pid": 123456, "created": 123.0}
        monkeypatch.setattr("inkbox_claude.runtime.process_identity", lambda _: owner)
        sent, release = asyncio.Event(), asyncio.Event()

        class Host(Client):
            async def query(self, text):
                token = CURRENT_SESSION.set(session)
                try:
                    registry["inkbox_send_whatsapp"]({"conversation_id": CONV, "text": "Answer"}, lambda: None)
                finally:
                    CURRENT_SESSION.reset(token)
                sent.set()
                await release.wait()  # Another tool is still executing.

        session._client = Host()
        meta = route()
        store.admit(session.chat_id, "Question", meta)
        task = asyncio.create_task(session._run_turn(_Turn(text="Question", mode="whatsapp", reply_meta=meta)))
        await asyncio.wait_for(sent.wait(), 2)
        try:
            restarted = IMessageState(gw.cfg, channel="whatsapp")
            assert restarted.interrupted()[0]["meta"]["host_owner"] == owner
            assert row(store)["state"] == "running"
            assert store.lookup_outbound(MESSAGE)["chat_id"] == session.chat_id
            if finish:
                release.set()
                await task
                assert row(store)["state"] == "done"
                assert row(store)["meta"]["host_terminal"] is True
                assert restarted.interrupted() == []
            else:
                fence = Mock(return_value=False)
                monkeypatch.setattr("inkbox_claude.runtime.fence_process", fence)
                await gw._recover_channel_inputs()
                fence.assert_called_once_with(owner)
                assert session._execution_blocked
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        assert client.whatsapp._http.post.call_count == 1
    asyncio.run(run())


@pytest.mark.parametrize("selector", ["uppercase", "singleton", "group", "other", "returned"])
@pytest.mark.parametrize("uncertain", [False, True])
def test_canonical_explicit_send_correlation(setup, monkeypatch, selector, uncertain):
    monkeypatch.setattr("inkbox._message_requests.time.sleep", lambda _: None)
    async def run():
        gw, session, client, registry, store = harness(setup)
        conversation = "abcdefab-cdef-4abc-8abc-abcdefabcdef"
        meta = route(conversation_id=conversation)
        args = {"text": "Answer", "idempotency_key": "original"}
        if selector == "uppercase":
            args.update(conversation_id=conversation.upper(), reply_to_message_id=MESSAGE)
        elif selector == "group":
            meta.update(conversation_kind="group", participants=[meta["sender"], "+15555550102"])
            args["to"] = ["+1 (555) 555-0102", meta["sender"]]
            client.whatsapp._http.get.return_value = {
                "id": conversation, "agent_identity_id": OWNER, "is_group": True,
                "local_number": MESSAGE_WIRE["local_number"], "remote_number": None,
                "participants": meta["participants"], "created_at": MESSAGE_WIRE["created_at"],
                "updated_at": MESSAGE_WIRE["updated_at"]}
        else:
            args["to"] = [meta["sender"]] if selector == "singleton" else "+15555550103"
        returned = CONV if selector == "other" else conversation
        client.whatsapp._http.post.return_value = {"message": {**MESSAGE_WIRE, "conversation_id": returned}}
        if uncertain:
            client.whatsapp._http.post.side_effect = httpx.ReadTimeout("lost response")

        class Host(Client):
            async def query(self, text):
                token = CURRENT_SESSION.set(session)
                try:
                    registry["inkbox_send_whatsapp"](args, lambda: None)
                except httpx.ReadTimeout:
                    assert uncertain
                finally:
                    CURRENT_SESSION.reset(token)

        finals = []
        async def final(chat, text, mode, reply_meta):
            finals.append(text)
        session.send_fn = final
        session._client = Host()
        store.admit(session.chat_id, "Question", meta)
        await session._run_turn(_Turn(text="Question", mode="whatsapp", reply_meta=meta))
        expect_final = selector in {"other", "group"} and not uncertain
        assert finals == (["Answer"] if expect_final else [])
        blocked_final = uncertain and selector in {"group", "other", "returned"}
        assert row(store)["state"] == ("failed" if blocked_final else "done")
        if blocked_final:
            receipt = row(store)["meta"]["whatsapp_recipient_sends"]["original"]
            assert receipt["outcome"] == "uncertain" and receipt["key"] == "original"
    asyncio.run(run())


@pytest.mark.parametrize("slot", ["queue", "_batching_turn", "_deferred_turn", "_delivery_turn", "_retrying_turn"])
def test_inbound_stop_cancels_own_snapshot_not_foreign_active(setup, slot):
    async def run():
        _, session, _, _, store = harness(setup)
        a, b = route("a"), route("b", sender="+15555550102")
        active = _Turn(text="A", mode="whatsapp", reply_meta=a)
        queued = _Turn(text="B", mode="whatsapp", reply_meta=b)
        session._current_turn, session._turn_active = active, True
        host = session._client
        session.mode, session.reply_meta = "whatsapp", a
        session.send_fn = AsyncMock()
        for meta in (a, b):
            store.admit(session.chat_id, "Work", meta)
        store.mark(a, "running")
        if slot == "queue":
            session._queue.put_nowait(queued)
        else:
            setattr(session, slot, queued)
        if slot == "_delivery_turn":
            store.mark(b, "reply_pending", reply="Saved")
        control = route("stop", sender=b["sender"], raw_text="/stop")
        store.admit(session.chat_id, "/stop", control)
        await session.handle_inbound("/stop", "whatsapp", control)
        assert queued.cancelled and not active.cancelled and host.interrupts == 0
        assert row(store, "b")["state"] == "cancelled"
        assert store.was_stopped(session.chat_id, "b")
        assert not store.was_stopped(session.chat_id, "a")
        assert session.send_fn.call_args.args[1] == "Stopped."
        fresh = _Turn(text="Fresh", mode="whatsapp", reply_meta=route("fresh", sender=b["sender"]))
        session._queue.put_nowait(fresh)
        await session.handle_inbound("/stop", "whatsapp", control)
        assert not fresh.cancelled
    asyncio.run(run())


@pytest.mark.parametrize("command", ["/clear", "/resume"])
@pytest.mark.parametrize("incoming,parked", [("whatsapp", "whatsapp"), ("sms", "whatsapp"), ("whatsapp", "sms")])
@pytest.mark.parametrize("slot", ["queue", "_batching_turn", "_deferred_turn", "_delivery_turn", "_retrying_turn"])
def test_foreign_parked_work_blocks_session_wide_controls(setup, command, incoming, parked, slot):
    async def run():
        _, session, _, _, _ = harness(setup)
        own = route("a")
        other = route("b", sender="+15555550102", channel=parked)
        turn = _Turn(text="Other", mode=parked, reply_meta=other)
        if slot == "queue":
            session._queue.put_nowait(turn)
        else:
            setattr(session, slot, turn)
        session._reset_session = AsyncMock()
        session._begin_resume = AsyncMock()
        await session.handle_inbound(command, incoming, {**own, "channel": incoming, "raw_text": command})
        session._reset_session.assert_not_awaited()
        session._begin_resume.assert_not_awaited()
    asyncio.run(run())


@pytest.mark.parametrize("status,code", [(422, "whatsapp_groups_not_supported"), (429, "whatsapp_conversation_rate_limited")])
def test_quote_rejection_fence_survives_lookup_and_restart(setup, monkeypatch, status, code):
    from inkbox import InkboxAPIError
    monkeypatch.setattr("inkbox._message_requests.time.sleep", lambda _: None)
    async def run():
        gw, session, client, registry, store = harness(setup)
        meta = route()
        store.admit(session.chat_id, "Question", meta)
        def rejected(*args, **kwargs):
            saved = row(store)["meta"]
            assert saved["whatsapp_explicit_quote_attempted"]
            assert saved["whatsapp_explicit_quote_key"] == "quote-key"
            assert kwargs["json"]["reply_to_message_id"] == MESSAGE
            assert kwargs["headers"]["Idempotency-Key"] == "quote-key"
            raise InkboxAPIError(status, {"error": code})
        client.whatsapp._http.post.side_effect = rejected
        client.message_sends._http = Mock()
        client.message_sends._http.get.return_value = {"message_id": MESSAGE}
        class Host(Client):
            async def query(self, text):
                token = CURRENT_SESSION.set(session)
                try:
                    with pytest.raises(InkboxAPIError):
                        registry["inkbox_send_whatsapp"]({"conversation_id": CONV, "reply_to_message_id": MESSAGE,
                            "text": "Quoted", "idempotency_key": "quote-key"}, lambda: None)
                    registry["inkbox_lookup_whatsapp_send"]({"idempotency_key": "quote-key"}, lambda: None)
                    assert row(store)["meta"]["whatsapp_explicit_quote_attempted"]
                    restarted = IMessageState(gw.cfg, channel="whatsapp")
                    assert restarted.interrupted()[0]["meta"]["whatsapp_explicit_quote_attempted"]
                finally:
                    CURRENT_SESSION.reset(token)
        session._client = Host()
        await session._run_turn(_Turn(text="Question", mode="whatsapp", reply_meta=meta))
        assert row(store)["state"] == "done"
        assert client.whatsapp._http.post.call_count == (3 if status == 429 else 1)
        with pytest.raises(ValueError, match="explicit quoted reply"):
            await send_reply(gw, session.chat_id, "Plain replacement", row(store)["meta"])
        assert client.whatsapp._http.post.call_count == (3 if status == 429 else 1)
    asyncio.run(run())


def test_host_eof_without_terminal_keeps_explicit_send_fence(setup):
    async def run():
        _, session, _, registry, store = harness(setup)
        meta = route()
        store.admit(session.chat_id, "Question", meta)
        class Host(Client):
            async def query(self, text):
                token = CURRENT_SESSION.set(session)
                try:
                    registry["inkbox_send_whatsapp"]({"conversation_id": CONV, "text": "Sent"}, lambda: None)
                finally:
                    CURRENT_SESSION.reset(token)
            async def receive_response(self):
                for message in []:
                    yield message
        session._client = Host()
        with pytest.raises(RuntimeError, match="host completion"):
            await session._run_turn(_Turn(text="Question", mode="whatsapp", reply_meta=meta))
        assert row(store)["state"] == "running" and len(store.interrupted()) == 1
    asyncio.run(run())


def test_repeated_permission_escalations_send_fresh_prompts(setup):
    async def run():
        gw, session, client, _, store = harness(setup)
        meta = route()
        store.admit(session.chat_id, "Question", meta)
        session._current_turn = _Turn(text="Question", mode="whatsapp", reply_meta=meta)
        session.mode, session.reply_meta = "whatsapp", meta
        sent = asyncio.Event()
        async def send(*args):
            await gw.send_to_contact(*args)
            sent.set()
        session.send_fn = send
        for index in range(2):
            sent.clear()
            prompt = asyncio.create_task(session._escalate("permission", "Allow command?"))
            await asyncio.wait_for(sent.wait(), 2)
            await session.handle_inbound("yes", "whatsapp", route(f"answer-{index}"))
            assert await prompt == "yes"
        calls = client.whatsapp._http.post.call_args_list
        assert len(calls) == 2 and calls[0].kwargs["json"] == calls[1].kwargs["json"]
        assert calls[0].kwargs["headers"]["Idempotency-Key"] != calls[1].kwargs["headers"]["Idempotency-Key"]
    asyncio.run(run())


def test_reaction_does_not_answer_pending_question_or_stop(channel):
    from inkbox_claude.escalation import PendingInteraction
    async def run():
        gw = channel.gateway
        session = make_session([])
        session.cfg, session.receipt_fn = gw.cfg, gw._channel_receipt
        session.cfg.group_reply_mode = "mention"
        gw.sessions = NS(get=lambda _: session)
        channel.message.update(is_group=True)
        meta = route(sender=channel.reaction["remote_number"], conversation_id=channel.reaction["conversation_id"])
        session.pending = PendingInteraction(kind="permission", sender=meta["sender"], route=meta,
            prompt_text="Allow?", future=asyncio.get_running_loop().create_future())
        session._stop_turn = AsyncMock()
        await receive_reaction(gw, {"data": {"reaction": deepcopy(channel.reaction)}})
        assert not session.pending.future.done()
        session._stop_turn.assert_not_awaited()
        assert len(session._context) == 1
    asyncio.run(run())


def test_stop_with_only_foreign_work_reports_idle(setup):
    async def run():
        _, session, _, _, _ = harness(setup)
        foreign = _Turn(text="Foreign", mode="whatsapp", reply_meta=route("other", sender="+15555550102"))
        session._queue.put_nowait(foreign)
        session.send_fn = AsyncMock()
        await session.handle_inbound("/stop", "whatsapp", route("stop", raw_text="/stop"))
        assert not foreign.cancelled
        assert "Nothing to stop" in session.send_fn.call_args.args[1]
    asyncio.run(run())


def test_recovered_consumed_stop_finishes_receipt_without_selecting_new_work(setup):
    async def run():
        _, session, _, _, store = harness(setup)
        control = route("stop", raw_text="/stop")
        store.admit(session.chat_id, "/stop", control)
        store.consume_control("stop", [{"chat_id": session.chat_id, "source_event_ids": ["old"]}])
        fresh = _Turn(text="Fresh", mode="whatsapp", reply_meta=route("fresh"))
        session._queue.put_nowait(fresh)
        session.send_fn = AsyncMock()
        await session.handle_inbound("/stop", "whatsapp", control)
        assert row(store, "stop")["state"] == "done"
        assert not fresh.cancelled and not store.was_stopped(session.chat_id, "fresh")
        session.send_fn.assert_not_awaited()
    asyncio.run(run())


def test_reaction_buffer_keeps_actor_revision_and_removal(channel):
    async def run():
        gw = channel.gateway
        session = make_session([])
        session.cfg = gw.cfg
        session.cfg.group_reply_mode = "mention"
        session.receipt_fn = gw._channel_receipt
        gw.sessions = NS(get=lambda _: session)
        channel.message.update(is_group=True, participants=["+15550000002", "+15550000003"])
        session.buffer_context("Original message", TARGET)
        for actor, revision, status in [("+15550000002", 2, "applied"), ("+15550000003", 2, "applied"), ("+15550000003", 3, "removed")]:
            channel.reaction.update(remote_number=actor, revision=revision, status=status)
            if actor.endswith("3"):
                channel.reaction["id"] = "00000000-0000-0000-0000-000000000006"
            await receive_reaction(gw, {"data": {"reaction": deepcopy(channel.reaction)}})
        assert len(session._context) == 4
        ids = [item["id"] for item in session._context]
        assert len(set(ids)) == 4 and ids[-1].endswith(":3:removed")
        assert session._worker is None
        with gw._channel_store("whatsapp")._db() as db:
            receipts = db.execute("SELECT event_id,meta FROM receipts").fetchall()
        assert len(receipts) == 3
        assert all(r["event_id"].startswith("reaction:") for r in receipts)
        assert all(json.loads(r["meta"])["message_id"] == TARGET for r in receipts)
    asyncio.run(run())


@pytest.mark.parametrize("phase", ["pending", "reply_pending"])
def test_recovery_rechecks_all_participants(setup, phase):
    gw, session, _, _, store = harness(setup)
    meta = route(conversation_kind="group", participants=["+15555550101", "+15555550102"])
    store.admit(session.chat_id, "Question", meta)
    store.mark(meta, phase, reply="Answer" if phase == "reply_pending" else None)
    gw._sender_allowed = lambda sender, *args: sender != "+15555550102"
    session.handle_inbound, session.recover_reply = AsyncMock(), AsyncMock()
    asyncio.run(gw._recover_channel_inputs())
    session.handle_inbound.assert_not_awaited()
    session.recover_reply.assert_not_awaited()
    assert row(store)["state"] == "cancelled"


@pytest.mark.parametrize("final", [False, True])
def test_current_group_membership_blocks_output_and_permissions(setup, final):
    gw, session, client, _, store = harness(setup)
    meta = route(conversation_kind="group", participants=["+15555550101"])
    store.admit(session.chat_id, "Question", meta)
    store.mark(meta, "reply_pending", reply="Answer")
    client.whatsapp._http.get.return_value = {"id": CONV, "agent_identity_id": OWNER,
        "local_number": "+15555550100", "remote_number": None, "is_group": True,
        "participants": ["+15555550101", "+15555550102"], "created_at": MESSAGE_WIRE["created_at"],
        "updated_at": MESSAGE_WIRE["updated_at"]}
    gw._sender_allowed = lambda sender, *args: sender != "+15555550102"
    with pytest.raises((ValueError, PermissionError)):
        asyncio.run(send_reply(gw, session.chat_id, "Allow command?", {**meta, "final_reply": final}))
    client.whatsapp._http.post.assert_not_called()


def test_identical_prompts_are_distinct_but_final_key_is_legacy_stable(setup):
    gw, session, client, _, store = harness(setup)
    meta = route()
    async def run():
        await send_reply(gw, session.chat_id, "Allow?", meta)
        await send_reply(gw, session.chat_id, "Allow?", meta)
        store.admit(session.chat_id, "Question", meta)
        store.mark(meta, "reply_pending", reply="Allow?")
        await send_reply(gw, session.chat_id, "Allow?", {**meta, "final_reply": True})
    asyncio.run(run())
    keys = [call.kwargs["headers"]["Idempotency-Key"] for call in client.whatsapp._http.post.call_args_list]
    assert len(set(keys)) == 3
    expected = "claude-wa-" + hashlib.sha256(json.dumps([gw.cfg.base_url, OWNER, CONV, ["source"], "Allow?"], sort_keys=True).encode()).hexdigest()
    assert keys[-1] == expected


def test_uncertain_final_retains_request_key_for_read_only_lookup(setup, monkeypatch):
    gw, session, client, registry, store = harness(setup)
    meta = route()
    store.admit(session.chat_id, "Question", meta)
    store.mark(meta, "reply_pending", reply="Answer")
    monkeypatch.setattr("inkbox._message_requests.time.sleep", lambda _: None)
    client.whatsapp._http.post.side_effect = httpx.ReadTimeout("response lost")
    with pytest.raises(httpx.ReadTimeout):
        asyncio.run(send_reply(gw, session.chat_id, "Answer", {**meta, "final_reply": True}))
    store.replay_pending()
    saved = row(store)["meta"]
    key = client.whatsapp._http.post.call_args.kwargs["headers"]["Idempotency-Key"]
    assert saved["whatsapp_send_key"] == key
    client.message_sends._http = Mock()
    client.message_sends._http.get.return_value = {"message_id": MESSAGE}
    assert registry["inkbox_lookup_whatsapp_send"]({"idempotency_key": saved["whatsapp_send_key"]}, lambda: None) == {"message_id": MESSAGE}
    assert row(store)["state"] == "uncertain" and store.pending_replies() == []


def test_overlong_final_is_definitively_failed_once(setup):
    async def run():
        gw, session, client, _, store = harness(setup)
        meta = route(media=True)  # Not a text burst.
        session._client.answer = "x" * 4001
        store.admit(session.chat_id, "Question", meta)
        await session.handle_inbound("Question", "whatsapp", meta)
        await session._worker
        assert row(store)["state"] == "failed"
        assert row(store)["meta"]["host_terminal"] is True
        assert store.pending_replies() == []
        assert client.whatsapp._http.post.call_count == 1
        body = client.whatsapp._http.post.call_args.kwargs["json"]
        assert "4000" in body["text"] and len(body["text"]) < 4000
        await gw._recover_channel_inputs()
        assert client.whatsapp._http.post.call_count == 1
    asyncio.run(run())


def test_status_and_accepted_response_race_preserves_route(setup, monkeypatch):
    gw, _, _ = setup
    store = gw._channel_store("whatsapp")
    original_db = store._db
    selected, writer_started, writer_done = threading.Event(), threading.Event(), threading.Event()
    errors = []

    @contextmanager
    def interleaved_db():
        with original_db() as db:
            class Connection:
                def execute(self, sql, parameters=()):
                    result = db.execute(sql, parameters)
                    if sql.startswith("SELECT route"):
                        snapshot = result.fetchone()
                        selected.set()
                        assert writer_started.wait(2)
                        if not db.in_transaction:
                            assert writer_done.wait(2)
                        return NS(fetchone=lambda: snapshot)
                    return result
            yield Connection()

    monkeypatch.setattr(store, "_db", interleaved_db)
    def write():
        try:
            assert selected.wait(2)
            writer_started.set()
            IMessageState(gw.cfg, channel="whatsapp").record_outbound(
                NS(id=MESSAGE, status="sent", delivery_state="pending"), route(), "chat")
        except BaseException as exc:
            errors.append(exc)
        finally:
            writer_done.set()
    writer = threading.Thread(target=write)
    writer.start()
    try:
        record_status(gw, {"event_type": "whatsapp.delivered", "data": {"message": {
            **MESSAGE_WIRE, "status": "delivered", "delivery_state": "delivered"}}})
    finally:
        writer.join(3)
    assert not writer.is_alive() and not errors
    monkeypatch.setattr(store, "_db", original_db)
    saved = store.lookup_outbound(MESSAGE)
    assert saved["chat_id"] == "chat" and saved["meta"]["source_event_id"] == "source"
    assert saved["status"] == "delivered" and saved["delivery_state"] == "delivered"
