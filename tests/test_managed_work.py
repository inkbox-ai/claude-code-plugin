import asyncio
from unittest.mock import AsyncMock

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
