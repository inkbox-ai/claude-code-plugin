"""Associate asynchronous turns with their durable delivery receipt."""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from contextvars import ContextVar

_current = ContextVar("managed_deliveries", default=())


class DeliveryWork:
    def __init__(self):
        self.parts = set()
        self.failed = False
        self.sealed = False
        self.done = asyncio.get_running_loop().create_future()

    def add(self, part):
        if self.done.done():
            raise RuntimeError("Delivery work was already completed")
        self.parts.add(part)

    def finish(self, part, *, success):
        self.failed |= not success
        self.parts.discard(part)
        self.check()

    def seal(self):
        self.sealed = True
        self.check()

    def check(self):
        if self.sealed and not self.parts and not self.done.done():
            self.done.set_result(not self.failed)


@contextmanager
def scope(work):
    token = _current.set(tuple(work))
    try:
        yield
    finally:
        _current.reset(token)


def attach(turn):
    for work in _current.get():
        if work not in turn.managed_work:
            work.add(id(turn))
            turn.managed_work.append(work)


def finish(turn, outcome):
    for work in turn.managed_work:
        work.finish(id(turn), success=outcome in {"completed", "cancelled"})
    turn.managed_work.clear()


class TurnQueue(asyncio.Queue):
    def put_nowait(self, turn):
        if not turn.managed_admitted:
            attach(turn)
            turn.managed_admitted = True
        super().put_nowait(turn)


def spawn(coroutine, **kwargs):
    task = asyncio.create_task(coroutine, **kwargs)
    owners = _current.get()
    for work in owners:
        work.add(task)

    def finished(done):
        success = not done.cancelled() and done.exception() is None
        for work in owners:
            work.finish(done, success=success)

    if owners:
        task.add_done_callback(finished)
    return task
