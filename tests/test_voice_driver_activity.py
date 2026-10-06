"""Execute the real media handler against deterministic speech/timer events."""

import asyncio
import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


def _driver(monkeypatch):
    class App:
        def get(self, _path):
            return lambda function: function

        websocket = get

    path = Path(__file__).parent / "live" / "voice_driver.py"
    with monkeypatch.context() as imports:
        imports.syspath_prepend(str(path.parent))
        imports.setenv("REMOTE_INKBOX_API_KEY", "synthetic-driver-key")
        imports.delenv("VOICE_DRIVER_LINE_FILE", raising=False)
        imports.setenv("VOICE_DRIVER_LINE", "synthetic requested task")
        imports.setenv("VOICE_DRIVER_ANSWER_CONTAINS", "synthetic expected answer")
        imports.setenv("VOICE_DRIVER_AUTO_STOP", "false")
        imports.setenv("VOICE_DRIVER_LISTEN", "180")
        for variable in ("VOICE_DRIVER_SPEAK_AFTER", "VOICE_DRIVER_REASK",
                         "VOICE_DRIVER_QUIET_GAP", "VOICE_DRIVER_MAX_REASKS"):
            imports.delenv(variable, raising=False)
        imports.setitem(sys.modules, "uvicorn", SimpleNamespace())
        imports.setitem(sys.modules, "fastapi", SimpleNamespace(FastAPI=App, WebSocket=object))
        imports.setitem(sys.modules, "starlette.websockets", SimpleNamespace(
            WebSocketState=SimpleNamespace(DISCONNECTED="disconnected"),
        ))
        imports.setitem(sys.modules, "inkbox", SimpleNamespace(Inkbox=object))
        imports.setitem(sys.modules, "inkbox.tunnels.client", SimpleNamespace(connect=None))
        spec = importlib.util.spec_from_file_location("voice_driver_activity_under_test", path)
        driver = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(driver)
    # These are the existing live-driver timing and retry policy, not faster
    # replacement constants. Only the clock is virtualized below.
    assert (driver.SPEAK_AFTER_S, driver.REASK_EVERY_S,
            driver.QUIET_GAP_S, driver.MAX_REASKS) == (5, 20, 6, 2)
    return driver


def _transcript(at, text, *, final):
    return at, {"event": "transcript", "text": text, "is_final": final}


async def _replay(monkeypatch, driver, frames, *, stop_at=75):
    """No network, SDK calls or copied handler logic; run actual coroutine."""
    clock = SimpleNamespace(now=0.0)
    real_sleep = asyncio.sleep
    create_task = asyncio.create_task
    incoming = asyncio.Queue()
    incoming.put_nowait({"event": "start"})
    pending = sorted(frames, key=lambda frame: frame[0]) + [
        (stop_at, {"event": "stop", "reason": "synthetic cleanup"}),
    ]
    sent = []
    turns = []

    class Socket:
        client_state = "connected"

        async def accept(self, **_kwargs):
            pass

        async def receive_text(self):
            return json.dumps(await incoming.get())

        async def send_text(self, raw):
            event = json.loads(raw)
            sent.append((clock.now, event))

        async def close(self):
            self.client_state = "disconnected"

    async def advance(delay):
        target = clock.now + delay
        while pending and pending[0][0] <= target:
            at, event = pending.pop(0)
            clock.now = at
            incoming.put_nowait(event)
            # Let the actual receive loop consume this frame before allowing
            # the timer task to evaluate its next scheduling decision.
            await real_sleep(0)
            await real_sleep(0)
        clock.now = target
        await real_sleep(0)

    def track_turn(coro, **kwargs):
        task = create_task(coro, **kwargs)
        turns.append(task)
        return task

    socket = Socket()
    with monkeypatch.context() as runtime:
        runtime.setattr(asyncio, "get_event_loop", lambda: SimpleNamespace(time=lambda: clock.now))
        runtime.setattr(asyncio, "sleep", advance)
        runtime.setattr(asyncio, "create_task", track_turn)
        handler = create_task(driver.phone_media_ws(socket))
        try:
            await asyncio.wait_for(handler, timeout=2)
        finally:
            handler.cancel()
            await asyncio.gather(handler, *turns, return_exceptions=True)
    assert socket.client_state == "disconnected"
    assert not any(event.get("event") == "stop" for _, event in sent)
    return [at for at, event in sent if event.get("delta") == driver.LINE]


def test_partial_speech_defers_reask_until_existing_quiet_gap(monkeypatch):
    driver = _driver(monkeypatch)
    frames = [_transcript(t, "ongoing synthetic speech", final=False) for t in range(20, 31)]
    asks = asyncio.run(_replay(monkeypatch, driver, frames))
    # A request used to be repeated at25 while a partial arrived at25. It must
    # now wait until30+6, then retain the existing20-second repeat interval.
    assert asks == [5, 36, 56]


@pytest.mark.parametrize("final", [False, True])
def test_whitespace_transcripts_do_not_postpone_reask(monkeypatch, final):
    driver = _driver(monkeypatch)
    frames = [_transcript(t, " \t\n", final=final) for t in range(20, 61)]
    assert asyncio.run(_replay(monkeypatch, driver, frames)) == [5, 25, 45]


def test_partial_exact_answer_does_not_latch_completion(monkeypatch):
    driver = _driver(monkeypatch)
    frames = [_transcript(20, driver.ANSWER_CONTAINS, final=False)]
    assert asyncio.run(_replay(monkeypatch, driver, frames)) == [5, 26, 46]


def test_nonmatching_final_speech_only_delays_reask(monkeypatch):
    driver = _driver(monkeypatch)
    frames = [_transcript(20, "unrelated final speech", final=True)]
    assert asyncio.run(_replay(monkeypatch, driver, frames)) == [5, 26, 46]


@pytest.mark.parametrize("frames", [
    [_transcript(10, "Synthetic, EXPECTED-answer!", final=True)],
    [_transcript(10, "synthetic expected", final=True), _transcript(12, "answer", final=True)],
])
def test_exact_final_answer_in_one_or_multiple_frames_stops_reasks(monkeypatch, frames):
    driver = _driver(monkeypatch)
    assert asyncio.run(_replay(monkeypatch, driver, frames)) == [5]


def test_intervening_final_words_do_not_create_an_exact_answer(monkeypatch):
    driver = _driver(monkeypatch)
    frames = [
        _transcript(10, "synthetic expected", final=True),
        _transcript(11, "unrelated words", final=True),
        _transcript(12, "answer", final=True),
    ]
    assert asyncio.run(_replay(monkeypatch, driver, frames)) == [5, 25, 45]


def test_final_answer_fragments_do_not_cross_websocket_calls(monkeypatch):
    driver = _driver(monkeypatch)

    async def run():
        first = await _replay(monkeypatch, driver, [_transcript(10, "synthetic expected", final=True)], stop_at=15)
        second = await _replay(monkeypatch, driver, [_transcript(12, "answer", final=True)])
        return first, second

    assert asyncio.run(run()) == ([5], [5, 25, 45])


def test_final_answer_matcher_retains_only_bounded_final_history(monkeypatch):
    driver = _driver(monkeypatch)
    matcher = driver._FinalAnswerMatcher("synthetic expected answer")
    assert not matcher.observe("x" * 100_000, final=True)
    assert len(matcher._tail) <= min(len(matcher.key) - 1, 4096)
    tail = matcher._tail
    assert not matcher.observe(matcher.key, final=False)
    assert not matcher.observe(" \n", final=True)
    assert matcher._tail == tail
    assert not matcher.observe("synthetic expected", final=True)
    assert matcher.observe("answer", final=True)

    oversized = driver._FinalAnswerMatcher("y" * 10_000)
    assert not oversized.observe("x" * 100_000, final=True)
    assert len(oversized._tail) <= 4096
