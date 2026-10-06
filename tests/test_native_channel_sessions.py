"""Native Claude queue, approval, and control ownership regression tests."""
import asyncio
import json
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest
from claude_agent_sdk import ResultMessage
from inkbox_claude.sessions import _Turn
from inkbox_claude.imessage import source_metadata
from inkbox_claude.escalation import PendingInteraction
from tests.test_sessions import make_session
from tests.test_slack import event, inbound_message, IDENTITY


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv("INKBOX_CLAUDE_HOME", str(tmp_path))


class Client:
    def __init__(self):
        self.queries = []
        self.gate = None
        self.started = asyncio.Event()
        self.interrupts = 0
        self.answer = "Answer"
    async def query(self, text):
        self.queries.append(text)
        self.started.set()
        if self.gate:
            await self.gate.wait()
    async def receive_response(self):
        yield ResultMessage(subtype="success", duration_ms=1, duration_api_ms=1,
                            is_error=False, num_turns=1, session_id="native-session", result=self.answer)
    async def interrupt(self):
        self.interrupts += 1
        if self.gate:
            self.gate.set()
    async def disconnect(self):
        if self.gate:
            self.gate.set()


def source(number, **changes):
    return {"conversation_id": "conversation", "sender": "+15555550123", "conversation_kind": "direct",
            **source_metadata({"id": f"message-{number}", "thread_id": f"standalone-{number}", "content": str(number)}, f"event-{number}"),
            **changes}


def make_native(sent, **changes):
    session = make_session(sent)
    for key, value in changes.items():
        setattr(session.cfg, key, value)
    session._client = Client()
    return session


def test_native_imessage_burst_preserves_each_source_and_first_reply_target():
    async def run():
        sent=[]
        session=make_native(sent, imessage_threaded_replies=True)
        original=source(1)
        await session.handle_inbound("Find dinner", "imessage", original)
        await session.handle_inbound("Friday", "imessage", source(2))
        await session.handle_inbound("six people", "imessage", source(3))
        original["conversation_id"]="wrong"
        await session._worker
        assert len(session._client.queries)==1
        assert all(text in session._client.queries[0] for text in ("Find dinner","Friday","six people"))
        route=sent[0][3]
        assert route["conversation_id"]=="conversation"
        assert route["imessage_reply_target"]=="message-1"
        assert route["imessage_event_ids"]==["event-1","event-2","event-3"]
        assert [row["id"] for row in route["imessage_sources"]]==["message-1","message-2","message-3"]
    asyncio.run(run())


@pytest.mark.parametrize("different", [{"sender":"other"},{"conversation_id":"other"},
    {"sender_access":"sponsored"},{"media":True},{"reply_to_message_id":"parent"}])
def test_distinct_contexts_cannot_coalesce(different):
    async def run():
        sent=[]
        session=make_native(sent, imessage_threaded_replies=True)
        await session.handle_inbound("first", "imessage", source(1))
        await session.handle_inbound("second", "imessage", source(2, **different))
        await session._worker
        assert len(session._client.queries)==2
        assert [row[3]["imessage_reply_target"] for row in sent]==["message-1","message-2"]
    asyncio.run(run())


def test_queued_followup_does_not_interrupt_active_native_imessage():
    async def run():
        sent=[]
        session=make_native(sent, imessage_threaded_replies=True)
        client=session._client
        client.gate=asyncio.Event()
        await session.handle_inbound("first", "imessage", source(1))
        await client.started.wait()
        await session.handle_inbound("second", "imessage", source(2))
        assert client.interrupts==0
        assert session._reply_route()[1]["imessage_reply_target"]=="message-1"
        client.gate.set()
        await session._worker
        assert len(client.queries)==2
        assert [row[3]["imessage_reply_target"] for row in sent]==["message-1","message-2"]
    asyncio.run(run())


def test_stop_during_burst_cancels_unstarted_work():
    async def run():
        sent=[]
        session=make_native(sent, imessage_threaded_replies=True)
        states=[]
        async def receipt(_chat,_mode,meta,state,text=None):
            states.append((meta.get("message_id"),state))
        session.receipt_fn=receipt
        await session.handle_inbound("first", "imessage", source(1))
        await asyncio.sleep(0)
        await session.handle_inbound("/stop", "imessage", source(2))
        await session._worker
        assert session._client.queries==[]
        assert ("message-1","cancelled") in states
        assert len(sent)==1 and "stop" in sent[0][1].lower()
    asyncio.run(run())


def test_slack_mention_context_persists_until_next_native_query(tmp_path):
    async def run():
        sent=[]
        session=make_native(sent, group_reply_mode="mention")
        _,body,meta=inbound_message(event(message_kinds=["channel","thread"],thread_ts="1234567890.000001",event={"text":"quiet context"}),IDENTITY)
        await session.handle_inbound(body,"slack",meta)
        assert session._worker is None
        assert len(json.loads(session._context_path.read_text()))==1
        _,body,meta=inbound_message(event(),IDENTITY)
        await session.handle_inbound(body,"slack",meta)
        await session._worker
        assert len(session._client.queries)==1
        assert "quiet context" in session._client.queries[0]
        assert session._context==[]
        assert sent[0][3]["thread_ts"]==meta["thread_ts"]
    asyncio.run(run())


def test_slack_approval_exact_actor_thread_and_fresh_instruction_supersedes():
    async def run():
        session=make_native([])
        _,_,meta=inbound_message(event(),IDENTITY)
        session._current_turn=_Turn(text="task",mode="slack",reply_meta=meta)
        session._turn_active=True
        session._worker=asyncio.create_task(asyncio.Event().wait())
        future=asyncio.get_running_loop().create_future()
        session.pending=PendingInteraction(kind="permission",sender=meta["sender"],route=meta,prompt_text="Allow?",future=future)
        await session.handle_inbound("yes","slack",{**meta,"actor_id":"U_OTHER","sender":"T_TEST:U_OTHER","raw_text":"yes"})
        assert not future.done()
        await session.handle_inbound("yes","slack",{**meta,"thread_ts":"1234567890.000099","raw_text":"yes"})
        assert not future.done()
        await session.handle_inbound("instead inspect the file","slack",{**meta,"raw_text":"instead inspect the file"})
        assert future.done() and future.result() is None
        assert session.pending is None
        assert session._queue.qsize()==1
        assert "instead inspect" in session._queue.get_nowait().text
        session._worker.cancel()
        await asyncio.gather(session._worker,return_exceptions=True)
    asyncio.run(run())


def test_parallel_native_permission_prompts_are_serialized():
    async def run():
        sent=[]
        session=make_native(sent)
        first=asyncio.create_task(session._escalate("permission","first"))
        second=asyncio.create_task(session._escalate("permission","second"))
        await asyncio.sleep(0)
        assert [x[1] for x in sent]==["first"]
        session.pending.future.set_result("1")
        assert await first=="1"
        await asyncio.sleep(0)
        assert [x[1] for x in sent]==["first","second"]
        session.pending.future.set_result("2")
        assert await second=="2"
    asyncio.run(run())


def test_imessage_stop_preserves_unrelated_voice_consult():
    async def run():
        sent=[]
        session=make_native(sent,imessage_threaded_replies=True)
        voice_future=asyncio.get_running_loop().create_future()
        voice=_Turn(text="voice action",mode="voice",reply_meta={"call_id":"call"},future=voice_future)
        session._current_turn=voice
        session._turn_active=True
        session._worker=asyncio.create_task(asyncio.Event().wait())
        session._queue.put_nowait(_Turn(text="queued voice",mode="voice",reply_meta={"call_id":"next"}))
        session._queue.put_nowait(_Turn(text="queued iMessage",mode="imessage",reply_meta=source(1)))
        await session.handle_inbound("/stop","imessage",source(2))
        assert not voice_future.done()
        assert session._client.interrupts==0
        assert session._current_turn is voice
        assert session._queue.qsize()==1 and session._queue.get_nowait().mode=="voice"
        session._worker.cancel()
        await asyncio.gather(session._worker,return_exceptions=True)
    asyncio.run(run())


def test_structured_auth_failure_reconnects_once_without_persisting_error_session(monkeypatch):
    from inkbox_claude import sessions as module
    async def run():
        sent=[];clients=[];saved=[]
        session=make_session(sent)
        session.resume_session_id="selected-session"
        session.on_session_id=lambda chat,value:saved.append(value)
        class Host(Client):
            def __init__(self,options):
                super().__init__();self.options=options;clients.append(self)
            async def connect(self):pass
            async def receive_response(self):
                failed=clients.index(self)==0
                yield ResultMessage(subtype="error_during_execution" if failed else "success",
                    duration_ms=1,duration_api_ms=1,is_error=failed,num_turns=1,
                    session_id="error-session" if failed else "selected-session",
                    result="Not logged in" if failed else "Recovered answer")
        monkeypatch.setattr(module,"ClaudeSDKClient",Host)
        await session.handle_inbound("hello","sms",{"sender":"owner"})
        await session._worker
        assert len(clients)==2
        assert [c.options.resume for c in clients]==["selected-session","selected-session"]
        assert saved==["selected-session"]
        assert [row[1] for row in sent]==["Recovered answer"]
        await session.close()
    asyncio.run(run())
