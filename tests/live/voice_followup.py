"""Bounded clarification turns for the scripted live voice peer."""

import asyncio
import time


async def listen_with_followups(say, *, seconds, nudge, last_heard, interval=30.0,
                                max_nudges=2, clock=time.monotonic, sleep=asyncio.sleep):
    """Hold the call open; clarify only after a quiet period, at most twice."""
    deadline = clock() + seconds
    last_sent = clock()
    sent = 0
    while clock() < deadline:
        await sleep(min(1.0, deadline - clock()))
        now = clock()
        if now >= deadline:
            break
        if nudge and sent < max_nudges and now - max(last_sent, last_heard()) >= interval:
            await say(nudge)
            sent += 1
            last_sent = clock()
