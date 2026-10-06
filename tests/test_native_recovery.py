"""Positive native recovery evidence, process fences, and durable ownership."""
import asyncio
import json
import subprocess
import sys
import threading
from types import SimpleNamespace as NS

import psutil
import pytest
from inkbox_claude.runtime import process_identity, fence_process, saved_answer, local_readiness
from inkbox_claude.imessage import IMessageState
from inkbox_claude.config import BridgeConfig
from inkbox_claude.tools import CURRENT_SESSION, _to_thread_drained
from tests.test_sessions import make_session


def test_native_history_requires_exact_input_and_terminal_answer(tmp_path):
    path=tmp_path/'session.jsonl'
    rows=[{"sessionId":"session","type":"user","message":{"content":"Logical input: exact\nTask"}},
          {"sessionId":"session","type":"assistant","message":{"content":[{"type":"text","text":"Completed answer"}],"stop_reason":"end_turn"}}]
    path.write_text('\n'.join(json.dumps(row) for row in rows))
    assert saved_answer(tmp_path,'session','exact')=='Completed answer'
    assert saved_answer(tmp_path,'session','another') is None
    assert saved_answer(tmp_path,'../session','exact') is None
    rows[-1]['message']['stop_reason']='tool_use'
    path.write_text('\n'.join(json.dumps(row) for row in rows))
    assert saved_answer(tmp_path,'session','exact') is None
    path.write_text(path.read_text()+'\nmalformed')
    assert saved_answer(tmp_path,'session','exact') is None


def test_fence_only_stops_owned_native_process():
    child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)'])
    try:
        client=NS(_transport=NS(_process=NS(pid=child.pid)))
        identity=process_identity(client)
        assert identity['pid']==child.pid
        assert not fence_process({**identity,'created':identity['created']-100})
        assert child.poll() is None
        assert fence_process(identity)
        child.wait(timeout=3)
        assert child.returncode is not None
        assert not fence_process(None)
    finally:
        if child.poll() is None:child.kill();child.wait()


def test_closing_session_drains_native_tool_side_effect(monkeypatch,tmp_path):
    monkeypatch.setenv('INKBOX_CLAUDE_HOME',str(tmp_path))
    async def run():
        session=make_session([])
        started=threading.Event();release=threading.Event();finished=[]
        def send():
            started.set();release.wait(5);finished.append(True)
        token=CURRENT_SESSION.set(session)
        task=asyncio.create_task(_to_thread_drained(send))
        await asyncio.to_thread(started.wait,3)
        task.cancel()
        close=asyncio.create_task(session.close())
        await asyncio.sleep(.01)
        assert not close.done()
        task.cancel()
        await asyncio.sleep(.01)
        assert not close.done() and not task.done()
        release.set()
        await asyncio.gather(task,return_exceptions=True)
        await close
        assert finished==[True] and not session._side_effects
        CURRENT_SESSION.reset(token)
    asyncio.run(run())


def test_channel_journal_has_exclusive_gateway_owner(monkeypatch,tmp_path):
    monkeypatch.setenv('INKBOX_CLAUDE_HOME',str(tmp_path))
    first=IMessageState(BridgeConfig(identity='agent'));second=IMessageState(BridgeConfig(identity='agent'))
    first.acquire_owner()
    with pytest.raises(RuntimeError,match='Another bridge'):second.acquire_owner()
    first.close();second.acquire_owner();second.close()


@pytest.mark.parametrize('logged_in,code,expected',[(True,0,True),(False,1,False),(False,0,False)])
def test_readiness_is_auth_not_model_completion(monkeypatch,logged_in,code,expected):
    monkeypatch.setattr('shutil.which',lambda _: '/native/claude')
    monkeypatch.setattr('subprocess.run',lambda *a,**kw:NS(returncode=code,stdout=json.dumps({'loggedIn':logged_in,'accessToken':'never-print'})))
    ready,detail=local_readiness()
    assert ready is expected and 'never-print' not in detail
    if ready:assert 'not yet verified' in detail


@pytest.mark.parametrize("initial_state", ["running", "sending", "uncertain"])
def test_unknown_channel_owner_stays_blocked_across_repeated_restarts(monkeypatch, tmp_path, initial_state):
    from inkbox_claude.gateway import InkboxGateway
    from inkbox_claude.sessions import SessionManager
    monkeypatch.setenv("INKBOX_CLAUDE_HOME", str(tmp_path))
    cfg = BridgeConfig(identity="agent", imessage_threaded_replies=True)
    meta = {"imessage_event_id": "interrupted", "sender": "+15550001111"}
    store = IMessageState(cfg)
    store.admit("chat", "do not repeat", meta)
    store.mark(meta, initial_state)
    pending = {"imessage_event_id": "fresh", "sender": "+15550001111"}
    store.admit("chat", "later request", pending)
    saved = {"imessage_event_id": "saved", "sender": "+15550001111"}
    store.admit("chat", "completed but not sent", saved)
    store.mark(saved, "reply_pending", reply="saved answer")
    calls = []
    monkeypatch.setattr("inkbox_claude.runtime.fence_process", lambda owner: calls.append(owner) or False)
    async def run():
        for _ in range(2):
            gw = InkboxGateway(cfg)
            session = make_session([])
            session.chat_id = "chat"
            gw.sessions = NS(get=lambda key: session, sessions={"chat": session})
            await gw._recover_channel_inputs()
            assert session._execution_blocked
            assert session._queue.empty()
            assert gw._channel_store("imessage").summary()["uncertain"] == 1
            assert gw._channel_store("imessage").summary()["pending"] == 1
            assert gw._channel_store("imessage").summary()["reply_pending"] == 1
            gw._channel_store("imessage").close()
        assert calls == [None, None]
    asyncio.run(run())


def test_confirmed_channel_fence_is_durable_without_replaying_old_work(monkeypatch, tmp_path):
    from inkbox_claude.gateway import InkboxGateway
    monkeypatch.setenv("INKBOX_CLAUDE_HOME", str(tmp_path))
    cfg = BridgeConfig(identity="agent", imessage_threaded_replies=True)
    meta = {"imessage_event_id": "interrupted", "sender": "+15550001111", "host_owner": {"pid": 1}}
    store = IMessageState(cfg)
    store.admit("chat", "do not repeat", meta)
    store.mark(meta, "running")
    calls = []
    monkeypatch.setattr("inkbox_claude.runtime.fence_process", lambda owner: calls.append(owner) or True)
    async def run():
        for _ in range(2):
            gw = InkboxGateway(cfg)
            session = make_session([])
            gw.sessions = NS(get=lambda key: session, sessions={"chat": session})
            await gw._recover_channel_inputs()
            assert not getattr(session, "_execution_blocked", False)
            assert session._queue.empty()
            assert gw._channel_store("imessage").summary()["uncertain"] == 1
            gw._channel_store("imessage").close()
        assert calls == [{"pid": 1}]
    asyncio.run(run())


def test_failed_disconnect_without_native_owner_blocks_later_execution(monkeypatch, tmp_path):
    monkeypatch.setenv("INKBOX_CLAUDE_HOME", str(tmp_path))
    async def run():
        session = make_session([])
        async def disconnect():
            raise RuntimeError("native transport lost")
        session._client = NS(disconnect=disconnect)
        with pytest.raises(RuntimeError, match="could not be stopped"):
            await session.close()
        assert session._execution_blocked
        with pytest.raises(RuntimeError, match="could not be stopped"):
            await session.close()
        with pytest.raises(PermissionError, match="could not be fenced"):
            await session._ensure_client()
    asyncio.run(run())


def test_companion_completion_settles_when_native_owner_cannot_be_fenced(monkeypatch, tmp_path):
    from inkbox_claude.sessions import _Turn
    monkeypatch.setenv("INKBOX_CLAUDE_HOME", str(tmp_path))
    async def run():
        session = make_session([])
        async def disconnect():
            raise RuntimeError("native transport lost")
        async def fail(turn):
            turn.submitted = True
            raise RuntimeError("unknown query outcome")
        session._client = NS(disconnect=disconnect)
        session._run_turn = fail
        completion = asyncio.get_running_loop().create_future()
        session._queue.put_nowait(_Turn("request", mode="slack", completion=completion))
        await session._drain()
        assert completion.done() and session._execution_blocked
        with pytest.raises(RuntimeError, match="could not be stopped"):
            await completion
    asyncio.run(run())


def test_missing_native_parent_cannot_prove_no_unrecorded_orphan_tools():
    child = subprocess.Popen([sys.executable, "-c", "pass"])
    created = psutil.Process(child.pid).create_time()
    child.wait(timeout=3)
    assert not fence_process({"pid": child.pid, "created": created, "children": []})


def test_close_fences_native_parent_before_disconnect_erases_ownership(monkeypatch, tmp_path):
    monkeypatch.setenv("INKBOX_CLAUDE_HOME", str(tmp_path))
    async def run():
        child = subprocess.Popen([sys.executable, "-c", "import time;time.sleep(60)"])
        try:
            session = make_session([])
            async def disconnect():
                assert child.poll() is not None
            session._client = NS(_transport=NS(_process=NS(pid=child.pid)), disconnect=disconnect)
            await session.close()
            assert child.poll() is not None
        finally:
            if child.poll() is None:
                child.kill()
                child.wait()
    asyncio.run(run())


def test_live_native_owner_discovers_late_tool_children_before_fencing(tmp_path):
    import time
    child_file = tmp_path / "tool.pid"
    code = "import subprocess,sys,time,pathlib; child=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)']);pathlib.Path(sys.argv[1]).write_text(str(child.pid));time.sleep(60)"
    parent = subprocess.Popen([sys.executable, "-c", code, str(child_file)])
    child = None
    try:
        owner = {"pid": parent.pid, "created": psutil.Process(parent.pid).create_time(), "children": []}
        deadline = time.monotonic() + 5
        while not child_file.exists() and time.monotonic() < deadline:
            time.sleep(.01)
        assert child_file.exists()
        child = psutil.Process(int(child_file.read_text()))
        assert fence_process(owner)
        parent.wait(timeout=3)
        assert not child.is_running() or child.status() in (psutil.STATUS_ZOMBIE, psutil.STATUS_DEAD)
    finally:
        if parent.poll() is None:
            parent.kill();parent.wait()
        if child is not None and child.is_running():
            child.kill()


def test_later_positive_fence_releases_only_the_same_known_blocked_owner(monkeypatch, tmp_path):
    monkeypatch.setenv("INKBOX_CLAUDE_HOME", str(tmp_path))
    async def run():
        child = subprocess.Popen([sys.executable, "-c", "import time;time.sleep(60)"])
        try:
            session = make_session([])
            async def disconnect():
                raise RuntimeError("temporary native disconnect failure")
            session._client = NS(_transport=NS(_process=NS(pid=child.pid)), disconnect=disconnect)
            monkeypatch.setattr("inkbox_claude.runtime.fence_process", lambda owner: False)
            with pytest.raises(RuntimeError, match="could not be stopped"):
                await session.close()
            assert session._execution_blocked and child.poll() is None
            expected = (child.pid, psutil.Process(child.pid).create_time())
            assert session._blocked_owner_keys == {expected}
            monkeypatch.setattr("inkbox_claude.runtime.fence_process", fence_process)
            await session.close()
            assert not session._execution_blocked
            assert child.poll() is not None
            assert not session._blocked_owner_keys
        finally:
            if child.poll() is None:
                child.kill();child.wait()
    asyncio.run(run())


def test_fencing_a_different_owner_does_not_release_old_quarantine(monkeypatch, tmp_path):
    monkeypatch.setenv("INKBOX_CLAUDE_HOME", str(tmp_path))
    async def run():
        session = make_session([])
        session._execution_blocked = True
        session._blocked_owner_keys = {(10, 1.0)}
        session._host_owner = {"pid": 11, "created": 2.0, "children": []}
        monkeypatch.setattr("inkbox_claude.runtime.fence_process", lambda owner: True)
        with pytest.raises(RuntimeError, match="could not be stopped"):
            await session.close()
        assert session._execution_blocked and session._blocked_owner_keys == {(10, 1.0)}
    asyncio.run(run())


def test_interrupt_failure_log_omits_private_route_and_provider_detail(monkeypatch, tmp_path, caplog):
    import logging
    from inkbox_claude.sessions import _Turn
    monkeypatch.setenv("INKBOX_CLAUDE_HOME", str(tmp_path))
    caplog.set_level(logging.DEBUG, logger="inkbox_claude.sessions")
    async def run():
        session=make_session([])
        session.chat_id="private-contact@example.com"
        session._current_turn=_Turn("private request",mode="sms")
        session._turn_active=True
        async def interrupt():raise RuntimeError("sensitive provider transcript")
        async def disconnect():pass
        session._client=NS(interrupt=interrupt,disconnect=disconnect)
        await session._abort_in_flight()
        assert "fencing the owned host" in caplog.text
        assert session.chat_id not in caplog.text
        assert "sensitive provider transcript" not in caplog.text
    asyncio.run(run())
