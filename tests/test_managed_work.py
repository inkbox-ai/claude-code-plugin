import asyncio
from unittest.mock import AsyncMock

import pytest

from inkbox_claude import managed_work
from inkbox_claude.sessions import _Turn
from tests.test_sessions import make_session


def test_accepted_work_waits_for_turn_and_detached_children():
    async def run():
        session = make_session([])
        gate = asyncio.Event()
        work = managed_work.DeliveryWork()
        children = []

        async def turn(_):
            children.append(managed_work.spawn(gate.wait()))
            return "Finished"

        session._run_turn = turn
        with managed_work.scope((work,)):
            await session._queue.put(_Turn(text="synthetic", mode="sms"))
        work.seal()
        await session._drain()
        assert not work.done.done()
        gate.set()
        await children[0]
        assert await work.done is True

    asyncio.run(run())


def test_failed_work_does_not_claim_success_after_error_notice():
    async def run():
        session = make_session([])
        work = managed_work.DeliveryWork()
        session._run_turn = AsyncMock(side_effect=ValueError("synthetic"))
        session.close = AsyncMock()
        with managed_work.scope((work,)):
            await session._queue.put(_Turn(text="synthetic", mode="sms"))
        work.seal()
        await session._drain()
        assert await work.done is False

    asyncio.run(run())


def test_terminal_reply_settlement_preserves_uncertain_delivery(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import Mock

    from inkbox_claude import sessions

    class Result:
        result = "Automatic fallback"
        session_id = None

    class Client:
        async def query(self, _text):
            pass

        async def receive_response(self):
            yield Result()

    monkeypatch.setattr(sessions, "ResultMessage", Result)

    async def run(lookup, expected):
        sent = []
        session = make_session(sent)
        session._ensure_client = AsyncMock(return_value=Client())
        remote = SimpleNamespace(terminal_replies=lambda **_: SimpleNamespace(lookup=lookup))
        session._mcp_bridge = SimpleNamespace(root=tmp_path, remote=remote, calls={})
        session.notify_activity = AsyncMock()
        work = managed_work.DeliveryWork()
        with managed_work.scope((work,)):
            await session._queue.put(_Turn(text="synthetic", mode="sms", reply_meta={"message_id": "inbound"}))
        work.seal()
        await session._drain()
        assert await work.done is expected
        assert sent == []
        if not expected:
            with pytest.raises(sessions._TerminalReplyUncertain):
                await asyncio.wait_for(session.run_consult("Voice consult"), timeout=1)
            assert sent == []

    asyncio.run(run(Mock(return_value={"state": "accepted"}), True))
    asyncio.run(run(Mock(return_value={"state": "pending", "submitted": True}), False))
    asyncio.run(run(Mock(side_effect=OSError("unreadable journal")), False))
