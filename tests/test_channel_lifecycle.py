"""Exact native shutdown proof, approval ownership and terminal readiness."""
import asyncio
import json
import sqlite3
import subprocess
import sys
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, Mock

import pytest

from inkbox_claude.config import BridgeConfig
from inkbox_claude.escalation import PendingInteraction
from inkbox_claude.gateway import InkboxGateway
from inkbox_claude.imessage import IMessageState
from inkbox_claude.sessions import SessionManager, _Turn
from tests.test_native_channel_sessions import Client
from tests.test_sessions import make_session
from tests.test_slack import event, inbound_message, IDENTITY


@pytest.fixture(autouse=True)
def isolated(monkeypatch, tmp_path):
    monkeypatch.setenv("INKBOX_CLAUDE_HOME", str(tmp_path))


@pytest.mark.parametrize("mode", ["slack", "imessage"])
def test_shutdown_persists_positive_exact_owner_fence_and_fresh_input_progresses(monkeypatch, mode):
    async def run():
        child = subprocess.Popen([sys.executable, "-c", "import time;time.sleep(60)"])
        cfg = BridgeConfig(identity="test-agent", slack_enabled=True, imessage_threaded_replies=True)
        gw = InkboxGateway(cfg)
        sent = []
        session = make_session(sent)
        session.cfg, session.receipt_fn, session.fence_fn = cfg, gw._channel_receipt, gw._record_host_fence
        host = Client()
        host._transport = NS(_process=NS(pid=child.pid))
        host.gate = asyncio.Event()
        session._client = host
        gw.sessions = NS(get=lambda _chat: session, sessions={session.chat_id: session})
        meta = {"source_event_id": "old", "imessage_event_id": "old", "sender": "+15555550123", "media": True}
        store = gw._channel_store(mode)
        try:
            store.admit(session.chat_id, "Original request", meta)
            await session.handle_inbound("Original request", mode, meta)
            await asyncio.wait_for(host.started.wait(), 2)
            assert store.summary()["running"] == 1
            await session.stop_companion()
            assert child.poll() is not None
            assert store.summary()["uncertain"] == 1
            assert store.interrupted() == []
            for owned_store in gw._channel_stores.values():
                owned_store.close()
            restarted = InkboxGateway(cfg)
            fresh = make_session(sent)
            fresh.cfg, fresh.receipt_fn, fresh.fence_fn = cfg, restarted._channel_receipt, restarted._record_host_fence
            fresh._client = Client()
            restarted.sessions = NS(get=lambda _chat: fresh, sessions={fresh.chat_id: fresh})
            monkeypatch.setattr("inkbox_claude.runtime.fence_process", lambda _owner: pytest.fail("Persisted positive proof must survive the now-absent PID"))
            await restarted._recover_channel_inputs()
            assert not getattr(fresh, "_execution_blocked", False)
            assert fresh._client.queries == []
            next_meta = {**meta, "source_event_id": "fresh", "imessage_event_id": "fresh"}
            restarted._channel_store(mode).admit(fresh.chat_id, "Fresh request", next_meta)
            await fresh.handle_inbound("Fresh request", mode, next_meta)
            await asyncio.wait_for(fresh._worker, 2)
            assert len(fresh._client.queries) == 1 and "Fresh request" in fresh._client.queries[0]
            assert len(sent) == 1
            for owned_store in restarted._channel_stores.values():
                owned_store.close()
        finally:
            if child.poll() is None:
                child.kill()
                child.wait()
            for owned_store in gw._channel_stores.values():
                owned_store.close()
    asyncio.run(run())


def test_positive_fence_cannot_release_another_chat_or_reused_process():
    cfg = BridgeConfig(identity="test-agent")
    store = IMessageState(cfg)
    for source, chat, owner in [("owned", "one", {"pid": 10, "created": 20}),
                                ("other-chat", "two", {"pid": 10, "created": 20}),
                                ("reused", "one", {"pid": 10, "created": 21}),
                                ("unknown", "one", None)]:
        meta = {"imessage_event_id": source, "host_owner": owner}
        store.admit(chat, source, meta)
        store.mark(meta, "running")
    store.record_host_fence("one", {(10, 20)})
    assert {row["text"] for row in store.interrupted()} == {"other-chat", "reused", "unknown"}


@pytest.mark.parametrize("mode", ["slack", "imessage"])
def test_shutdown_fenced_outcome_context_survives_restart_once(mode, monkeypatch):
    async def run():
        cfg = BridgeConfig(identity="test-agent", slack_enabled=True, imessage_threaded_replies=True)
        gw = InkboxGateway(cfg)
        session = make_session([])
        session.cfg = cfg
        gw.sessions = NS(get=lambda _chat: session, sessions={session.chat_id: session})
        meta = {"source_event_id": "old", "imessage_event_id": "old", "host_fenced": True}
        store = gw._channel_store(mode)
        store.admit(session.chat_id, "Earlier request", meta)
        store.mark(meta, "uncertain")
        monkeypatch.setattr("inkbox_claude.runtime.fence_process", lambda _owner: pytest.fail("Do not refence a proven stopped owner"))
        await gw._recover_channel_inputs()
        assert len(session._context) == 1
        assert "unconfirmed outcome" in session._context[0]["text"]
        assert session._worker is None and session._queue.empty()
        session._context.clear()
        session._save_context()
        for owned in gw._channel_stores.values():
            owned.close()
        restarted = InkboxGateway(cfg)
        fresh = make_session([])
        restarted.sessions = NS(get=lambda _chat: fresh, sessions={fresh.chat_id: fresh})
        await restarted._recover_channel_inputs()
        assert fresh._context == []
        assert restarted._channel_store(mode).summary()["uncertain"] == 1
        for owned in restarted._channel_stores.values():
            owned.close()
    asyncio.run(run())


def test_uncertain_notice_failed_flush_is_not_acknowledged(monkeypatch):
    async def run():
        gw = InkboxGateway(BridgeConfig(identity="test-agent", slack_enabled=True))
        session = make_session([])
        gw.sessions = NS(get=lambda _chat: session, sessions={session.chat_id: session})
        store = gw._channel_store("slack")
        meta = {"source_event_id": "old", "host_fenced": True}
        store.admit(session.chat_id, "Earlier request", meta)
        store.mark(meta, "uncertain")
        save = session._save_context
        monkeypatch.setattr(session, "_save_context", lambda: (_ for _ in ()).throw(OSError("write unavailable")))
        with pytest.raises(OSError):
            await gw._recover_channel_inputs()
        assert len(store.pending_uncertain_notices()) == 1
        monkeypatch.setattr(session, "_save_context", save)
        await gw._recover_channel_inputs()
        assert len(json.loads(session._context_path.read_text())) == 1
        assert store.pending_uncertain_notices() == []
        store.close()
    asyncio.run(run())


@pytest.mark.parametrize("mode", ["slack", "imessage"])
@pytest.mark.parametrize("can_fence", [False, True])
def test_auth_retry_replaces_fence_proof_before_second_host_executes(monkeypatch, mode, can_fence):
    from claude_agent_sdk import ResultMessage
    from inkbox_claude import sessions as module
    from inkbox_claude.runtime import fence_process

    async def run():
        processes, hosts = [], []
        second_started = asyncio.Event()

        class Host(Client):
            def __init__(self, options):
                super().__init__()
                child = subprocess.Popen([sys.executable, "-c", "import time;time.sleep(60)"])
                processes.append(child)
                hosts.append(self)
                self._transport = NS(_process=NS(pid=child.pid))

            async def connect(self):
                pass

            async def query(self, text):
                self.queries.append(text)
                if len(hosts) == 2:
                    second_started.set()
                    await asyncio.Event().wait()

            async def receive_response(self):
                yield ResultMessage(subtype="error_during_execution", duration_ms=1,
                    duration_api_ms=1, is_error=True, num_turns=0, session_id="rejected",
                    result="Not logged in")

        monkeypatch.setattr(module, "ClaudeSDKClient", Host)
        cfg = BridgeConfig(identity="agent", slack_enabled=True, imessage_threaded_replies=True)
        gw = InkboxGateway(cfg)
        session = make_session([])
        session.cfg, session.receipt_fn, session.fence_fn = cfg, gw._channel_receipt, gw._record_host_fence
        gw.sessions = NS(get=lambda _chat: session, sessions={session.chat_id: session})
        meta = {"source_event_id": "retry", "imessage_event_id": "retry", "media": True}
        store = gw._channel_store(mode)
        try:
            store.admit(session.chat_id, "One request", meta)
            await session.handle_inbound("One request", mode, meta)
            await asyncio.wait_for(second_started.wait(), 3)
            assert processes[0].poll() is not None and processes[1].poll() is None
            rows = store.interrupted()
            assert len(rows) == 1 and rows[0]["meta"]["host_owner"]["pid"] == processes[1].pid
            assert rows[0]["meta"]["host_fenced"] is False
            # Simulate gateway loss without orderly native-host shutdown.
            session._worker.cancel()
            await asyncio.gather(session._worker, return_exceptions=True)
            for owned in gw._channel_stores.values():
                owned.close()
            restarted = InkboxGateway(cfg)
            fresh = make_session([])
            fresh.cfg = cfg
            restarted.sessions = NS(get=lambda _chat: fresh, sessions={fresh.chat_id: fresh})
            observed = []

            def fence(owner):
                observed.append(owner)
                return fence_process(owner) if can_fence else False

            monkeypatch.setattr("inkbox_claude.runtime.fence_process", fence)
            await restarted._recover_channel_inputs()
            assert [owner["pid"] for owner in observed] == [processes[1].pid]
            assert bool(getattr(fresh, "_execution_blocked", False)) is not can_fence
            assert (processes[1].poll() is not None) is can_fence
            assert sum(len(host.queries) for host in hosts) == 2
            assert fresh._worker is None
            for owned in restarted._channel_stores.values():
                owned.close()
        finally:
            for process in processes:
                if process.poll() is None:
                    process.kill()
                    process.wait()
            for owned in gw._channel_stores.values():
                owned.close()
    asyncio.run(run())


@pytest.mark.parametrize("unknown_owner", [False, True])
def test_positive_fence_write_failure_retries_exact_proof_before_new_host(monkeypatch, unknown_owner):
    from inkbox_claude.runtime import process_identity

    async def run():
        child = subprocess.Popen([sys.executable, "-c", "import time;time.sleep(60)"])
        cfg = BridgeConfig(identity="agent", slack_enabled=True)
        gw = InkboxGateway(cfg)
        session = make_session([])
        session.cfg = cfg
        host = Client()
        host._transport = NS(_process=NS(pid=child.pid))
        session._client = host
        owner = process_identity(host)
        store = gw._channel_store("slack")
        meta = {"source_event_id": "old", "host_owner": owner}
        store.admit(session.chat_id, "Original", meta)
        store.mark(meta, "running")
        writes = []

        def save(chat, proof):
            writes.append(set(proof))
            if len(writes) == 1:
                raise OSError("Synthetic persistence failure")
            gw._record_host_fence(chat, proof)

        session.fence_fn = save
        try:
            with pytest.raises(OSError):
                await session.close()
            assert child.poll() is not None
            assert session._pending_fenced_owners == {(owner["pid"], owner["created"])}
            assert len(store.interrupted()) == 1
            if unknown_owner:
                session._execution_blocked = True
                session._blocked_owner_keys = None
            monkeypatch.setattr("inkbox_claude.runtime.fence_process", lambda _owner: pytest.fail("Already-proven absent PID must not be fenced again"))
            # A fresh host request must retry proof persistence first, even if
            # host construction then cannot proceed in this synthetic fixture.
            monkeypatch.setattr("inkbox_claude.sessions.CLAUDE_SDK_AVAILABLE", False)
            with pytest.raises(PermissionError if unknown_owner else RuntimeError,
                               match="could not be fenced" if unknown_owner else "not installed"):
                await session._ensure_client()
            assert writes == [{(owner["pid"], owner["created"])}] * 2
            assert not session._pending_fenced_owners
            assert session._execution_blocked is unknown_owner
            assert store.interrupted() == [] and store.summary()["uncertain"] == 1
        finally:
            if child.poll() is None:
                child.kill()
                child.wait()
            store.close()
    asyncio.run(run())


@pytest.mark.parametrize("unknown_owner", [False, True])
def test_new_unresolved_owner_survives_failed_positive_fence_write(monkeypatch, unknown_owner):
    from inkbox_claude import runtime

    async def run():
        processes = [subprocess.Popen([sys.executable, "-c", "import time;time.sleep(60)"]) for _ in range(2)]
        session = make_session([])
        positive, unresolved = Client(), Client()
        positive._transport = NS(_process=NS(pid=processes[0].pid))
        if not unknown_owner:
            unresolved._transport = NS(_process=NS(pid=processes[1].pid))
        unresolved.disconnect = AsyncMock(side_effect=RuntimeError("disconnect unavailable"))
        session._client, session._connecting_client = positive, unresolved
        positive_owner = runtime.process_identity(positive)
        unresolved_owner = runtime.process_identity(unresolved)
        original_fence, writes = runtime.fence_process, []

        def fence(owner):
            return original_fence(owner) if owner == positive_owner else False

        def persist(_chat, proof):
            writes.append(set(proof))
            if len(writes) == 1:
                raise OSError("checkpoint unavailable")

        monkeypatch.setattr(runtime, "fence_process", fence)
        session.fence_fn = persist
        try:
            with pytest.raises(OSError):
                await session.close()
            assert processes[0].poll() is not None and processes[1].poll() is None
            assert session._execution_blocked
            expected = None if unknown_owner else {(unresolved_owner["pid"], unresolved_owner["created"])}
            assert session._blocked_owner_keys == expected
            with pytest.raises(PermissionError, match="could not be fenced"):
                await session._ensure_client()
            assert len(writes) == 2 and writes[0] == writes[1]
            assert not session._pending_fenced_owners and session._execution_blocked
            assert session._blocked_owner_keys == expected
        finally:
            for process in processes:
                if process.poll() is None:
                    process.kill()
                    process.wait()

    asyncio.run(run())


@pytest.mark.parametrize("start_fails", [False, True])
def test_fence_persistence_retry_preserves_fresh_host_generation(monkeypatch, start_fails):
    from inkbox_claude import sessions as module
    from inkbox_claude.runtime import process_identity

    async def run():
        child = subprocess.Popen([sys.executable, "-c", "import time;time.sleep(60)"])
        sent, writes, hosts = [], [], []
        session = make_session(sent)
        old = Client()
        old._transport = NS(_process=NS(pid=child.pid))
        session._client = old
        owner = process_identity(old)

        def persist(_chat, proof):
            writes.append(set(proof))
            if len(writes) == 1:
                raise OSError("checkpoint unavailable")

        class Fresh(Client):
            def __init__(self, **_kwargs):
                super().__init__()
                hosts.append(self)

            async def connect(self):
                if start_fails:
                    raise ConnectionError("fresh host unavailable")

        session.fence_fn = persist
        monkeypatch.setattr(module, "ClaudeSDKClient", Fresh)
        try:
            with pytest.raises(OSError):
                await session.close()
            generation = session._client_generation
            monkeypatch.setattr("inkbox_claude.runtime.fence_process", lambda _owner: pytest.fail("An already-proven absent owner must not be fenced again"))
            turn = _Turn("Fresh request", mode="email", reply_meta={})
            if start_fails:
                with pytest.raises(ConnectionError):
                    await session._run_turn(turn)
            else:
                await session._run_turn(turn)
                assert hosts[0].queries == ["Fresh request"]
                assert len(sent) == 1 and sent[0][1] == "Answer"
            assert session._client_generation == generation
            assert writes == [{(owner["pid"], owner["created"])}] * 2
            assert session._host_owner is None and not session._pending_fenced_owners
            await session.close()
            assert not session._execution_blocked
        finally:
            if child.poll() is None:
                child.kill()
                child.wait()

    asyncio.run(run())


def test_stop_during_fresh_connect_after_proof_flush_still_fences_generation(monkeypatch):
    from inkbox_claude import sessions as module

    async def run():
        session = make_session([])
        session._pending_fenced_owners = {(123, 456)}
        session.fence_fn = Mock()
        connecting, release, hosts = asyncio.Event(), asyncio.Event(), []

        class Fresh(Client):
            def __init__(self, **_kwargs):
                super().__init__()
                hosts.append(self)

            async def connect(self):
                connecting.set()
                await release.wait()

            async def disconnect(self):
                release.set()

        monkeypatch.setattr(module, "ClaudeSDKClient", Fresh)
        task = asyncio.create_task(session._run_turn(_Turn("Fresh request", mode="email", reply_meta={})))
        await asyncio.wait_for(connecting.wait(), 2)
        await session.close()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert hosts[0].queries == [] and session._client is None

    asyncio.run(run())


@pytest.mark.parametrize("control", ["new", "resume"])
def test_failed_control_fence_keeps_existing_conversation_and_reports_pause(control):
    async def run():
        sent = []
        session = make_session(sent)
        session.resume_session_id = "retained-session"
        session.always_allowed.add("retained-grant")
        session._context = [{"id": "retained", "text": "Earlier context"}]
        session._execution_blocked = True
        session._blocked_owner_keys = None
        session.on_clear, session.on_session_id = Mock(), Mock()
        if control == "new":
            await session.handle_inbound("/new", "sms", {"sender": "+15555550123"})
        else:
            session._escalate = AsyncMock(return_value="1")
            await session._run_resume_pick([{"id": "other", "summary": "Other conversation", "mtime": 1}])
        assert len(sent) == 1 and "remains paused" in sent[0][1]
        assert session.resume_session_id == "retained-session"
        assert session.always_allowed == {"retained-grant"} and len(session._context) == 1
        session.on_clear.assert_not_called()
        session.on_session_id.assert_not_called()
        assert session._execution_blocked and session._blocked_owner_keys is None

    asyncio.run(run())


def test_startup_retry_fence_write_failure_settles_worker_and_reports_error(monkeypatch):
    async def run():
        sent = []
        session = make_session(sent)
        session._pending_fenced_owners = {(123, 456)}
        session.fence_fn = Mock(side_effect=OSError("checkpoint unavailable"))
        session.receipt_fn = AsyncMock()
        session.activity_fn = AsyncMock()
        monkeypatch.setattr("inkbox_claude.sessions.ClaudeSDKClient", Mock(side_effect=AssertionError("No new host before durable proof")))
        await session.handle_inbound("A request", "slack", {"source_event_id": "request", "sender": "workspace:actor"})
        await asyncio.wait_for(session._worker, 2)
        assert session._worker.exception() is None and session._retrying_turn is None
        assert session.fence_fn.call_count >= 2
        assert session._pending_fenced_owners == {(123, 456)}
        assert len(sent) == 1 and "checkpoint unavailable" not in sent[0][1]
        assert any(call.args[3] == "failed" for call in session.receipt_fn.call_args_list)
        assert session.activity_fn.call_args.args[3] == "failed"

    asyncio.run(run())


def test_bystander_and_other_thread_cannot_interrupt_pending_slack_permission():
    async def run():
        session = make_session([])
        session._client = Client()
        _, _, route = inbound_message(event(), IDENTITY)
        turn = _Turn("task", mode="slack", reply_meta=route)
        session._current_turn, session._turn_active = turn, True
        pending = PendingInteraction("permission", "Allow?", asyncio.get_running_loop().create_future(),
                                     sender=route["sender"], route=route, owner=turn)
        session.pending = pending
        for changes in ({"actor_id": "U_OTHER", "sender": "T_TEST:U_OTHER"}, {"thread_ts": "another-thread"}):
            await session.handle_inbound("A new task", "slack", {**route, **changes, "source_event_id": str(changes), "raw_text": "A new task"})
        assert session.pending is pending and not pending.future.done()
        assert session._client.interrupts == 0 and session._queue.empty()
        assert len(session._context) == 2
    asyncio.run(run())


def test_native_slack_mention_answers_pending_permission_and_preserves_capture_queue():
    async def run():
        session = make_session([])
        session._client = Client()
        _, _, route = inbound_message(event(), IDENTITY)
        route["slack_bot_user_id"] = "UBOT"
        turn = _Turn("task", mode="slack", reply_meta=route)
        session._current_turn, session._turn_active = turn, True
        pending = PendingInteraction("permission", "Allow?", asyncio.get_running_loop().create_future(),
                                     sender=route["sender"], route=route, owner=turn)
        session.pending = pending
        await session.handle_inbound("<@UBOT> 1", "slack", {**route, "raw_text": "<@UBOT> 1"})
        assert pending.future.result() == "1" and session._client.interrupts == 0
        pending.future = asyncio.get_running_loop().create_future()
        capture = _Turn("Voice consultation", future=asyncio.get_running_loop().create_future())
        session._queue.put_nowait(capture)
        session._worker = asyncio.create_task(asyncio.Event().wait())
        await session.handle_inbound("Instead inspect", "slack", {**route, "raw_text": "Instead inspect"})
        assert pending.future.result() is None and session._client.interrupts == 1
        assert list(session._queue._queue)[0] is capture and not capture.future.done()
        assert session._queue.qsize() == 2
        session._worker.cancel()
        await asyncio.gather(session._worker, return_exceptions=True)
    asyncio.run(run())


def test_blocked_turn_reports_failure_and_all_sessions_still_close():
    async def run():
        sent = []
        session = make_session(sent)
        session._execution_blocked = True
        await session.handle_inbound("New request", "email", {})
        await asyncio.wait_for(session._worker, 2)
        assert session._worker.exception() is None and len(sent) == 1
        assert session._execution_blocked
        manager = SessionManager(session.cfg, session.send_fn, None, [], {})
        other = NS(stop_companion=AsyncMock())
        manager.sessions = {"blocked": session, "other": other}
        gw = InkboxGateway(session.cfg)
        gw.sessions = manager
        gw._slack_activity = NS(close=AsyncMock())
        gw._runner = NS(cleanup=AsyncMock())
        gw._tunnel = NS(close=Mock())
        await gw._cleanup()
        other.stop_companion.assert_awaited_once()
        gw._slack_activity.close.assert_awaited_once()
        gw._runner.cleanup.assert_awaited_once()
        gw._tunnel.close.assert_called_once()
        assert session._execution_blocked
    asyncio.run(run())


def test_stop_reports_unconfirmed_fence_and_settles_selected_queue():
    async def run():
        sent = []
        session = make_session(sent)
        session._connecting_client = NS(disconnect=AsyncMock(side_effect=RuntimeError("native disconnect unavailable")))
        captured = _Turn("Queued consultation", future=asyncio.get_running_loop().create_future())
        session._queue.put_nowait(captured)
        await session._stop_turn()
        assert session._execution_blocked
        assert captured.future.done() and session._queue.empty()
        assert len(sent) == 1 and "could not be confirmed" in sent[0][1]
    asyncio.run(run())


@pytest.mark.parametrize("native_stop", [False, True])
@pytest.mark.parametrize("write_error", [OSError, sqlite3.OperationalError])
def test_stop_fence_write_failure_still_settles_queue_and_reports_pause(native_stop, write_error):
    from tests.test_slack import connected_client, stop_event

    async def run():
        child = subprocess.Popen([sys.executable, "-c", "import time;time.sleep(60)"])
        sent = []
        session = make_session(sent)
        key, _, route = inbound_message(event(), IDENTITY)
        session.chat_id = key
        active = _Turn("Owned task", mode="slack", reply_meta=route)
        session._current_turn, session._turn_active = active, True
        host = Client()
        host._transport = NS(_process=NS(pid=child.pid))
        host.interrupt = AsyncMock(side_effect=ConnectionError("interrupt unavailable"))
        session._client = host
        session.fence_fn = Mock(side_effect=write_error("checkpoint unavailable"))
        session.receipt_fn, session.activity_fn = AsyncMock(), AsyncMock()
        queued = _Turn("Queued consultation", mode="slack", reply_meta={**route, "source_event_id": "queued"},
                       future=asyncio.get_running_loop().create_future())
        session._queue.put_nowait(queued)
        gw = InkboxGateway(BridgeConfig(identity="agent", slack_enabled=True))
        gw._identity, gw._inkbox = NS(id=IDENTITY), connected_client()
        gw.sessions = NS(sessions={key: session})
        try:
            if native_stop:
                response = await gw._on_slack_received(stop_event())
                assert response.status == 200 and json.loads(response.text)["paused"] is True
                assert json.loads((await gw._on_slack_received(stop_event())).text)["deduped"] is True
            else:
                await session.handle_inbound("/stop", "slack", {**route, "raw_text": "/stop"})
                assert len(sent) == 1 and "could not be confirmed" in sent[0][1]
            assert child.poll() is not None and session._pending_fenced_owners
            assert queued.future.done() and queued.future.result() == "" and session._queue.empty()
            assert any(call.args[2].get("source_event_id") == "queued" and call.args[3] == "cancelled"
                       for call in session.receipt_fn.call_args_list)
        finally:
            if child.poll() is None:
                child.kill()
                child.wait()
            for store in gw._channel_stores.values():
                store.close()

    asyncio.run(run())


def test_same_readonly_store_failure_cannot_strand_captured_waiters(monkeypatch):
    from contextlib import contextmanager
    from inkbox_claude.runtime import process_identity

    async def run():
        child = subprocess.Popen([sys.executable, "-c", "import time;time.sleep(60)"])
        gw = InkboxGateway(BridgeConfig(identity="agent", slack_enabled=True))
        session = make_session([])
        session.cfg = gw.cfg
        host = Client()
        host._transport = NS(_process=NS(pid=child.pid))
        host.interrupt = AsyncMock(side_effect=ConnectionError("interrupt unavailable"))
        session._client, session._turn_active = host, True
        meta = {"source_event_id": "active", "host_owner": process_identity(host)}
        session._current_turn = _Turn("Active", mode="slack", reply_meta=meta)
        store = gw._channel_store("slack")
        store.admit(session.chat_id, "Active", meta)
        store.mark(meta, "running")
        queued = [_Turn("Queued", mode="slack", reply_meta={"source_event_id": f"queued-{number}"},
                        future=asyncio.get_running_loop().create_future(),
                        completion=asyncio.get_running_loop().create_future()) for number in range(2)]
        for turn in queued:
            store.admit(session.chat_id, turn.text, turn.reply_meta)
            session._queue.put_nowait(turn)
        attempts, activity = [], []
        original_db = store._db

        @contextmanager
        def read_only():
            with original_db() as db:
                db.execute("PRAGMA query_only=ON")
                yield db

        async def receipt(chat, mode, route, state, text=None):
            attempts.append(route["source_event_id"])
            await gw._channel_receipt(chat, mode, route, state, text)

        async def notify(_chat, _mode, route, state):
            activity.append((route["source_event_id"], state))
            if len(activity) == 1:
                raise OSError("activity checkpoint unavailable")

        session.receipt_fn, session.activity_fn = receipt, notify
        session.fence_fn = gw._record_host_fence
        monkeypatch.setattr(store, "_db", read_only)
        try:
            with pytest.raises(sqlite3.OperationalError, match="readonly"):
                await session._abort_in_flight()
            assert child.poll() is not None and session._pending_fenced_owners
            assert all(turn.cancelled and turn.future.done() and turn.future.result() == ""
                       and turn.completion.cancelled() for turn in queued)
            assert attempts == ["queued-0", "queued-1"]
            assert activity == [("queued-0", "cancelled"), ("queued-1", "cancelled")]
            assert session._queue.empty()
            # Local settlement is not a claim that failed durable writes stuck.
            assert store.summary()["pending"] == 2 and store.summary()["running"] == 1
        finally:
            if child.poll() is None:
                child.kill()
                child.wait()
            store.close()

    asyncio.run(run())


@pytest.mark.parametrize("state,expected", [("failed", 200), ("paused", 503), ("initialized", 200)])
def test_readiness_distinguishes_terminal_tombstones_from_blocked_work(monkeypatch, state, expected):
    async def run():
        gw = InkboxGateway(BridgeConfig())
        gw._runtime_ready = True
        gw.sessions = NS(sessions={})
        gw._companion = NS(records={"scope": {"state": state, "revoked": state == "failed"}})
        monkeypatch.setattr("inkbox_claude.runtime.local_readiness", lambda: (True, "Ready"))
        response = await gw._handle_ready(None)
        assert response.status == expected
        assert json.loads(response.text)["ready"] is (expected == 200)
    asyncio.run(run())
