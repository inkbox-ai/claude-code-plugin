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
        assert fence_process({**identity,'created':identity['created']-100})
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
