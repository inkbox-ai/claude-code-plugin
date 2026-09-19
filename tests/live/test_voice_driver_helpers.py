"""Offline regressions for the live peer's speech turn-taking."""

import asyncio
import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.fixture
def driver(monkeypatch):
    # The turn scheduler needs no HTTP server or API connection.
    class App:
        def get(self, _path):
            return lambda function: function

        websocket = get

    with monkeypatch.context() as imports:
        imports.setenv("REMOTE_INKBOX_API_KEY", "synthetic-driver-key")
        imports.delenv("VOICE_DRIVER_LINE_FILE", raising=False)
        imports.delenv("VOICE_DRIVER_STAGES_FILE", raising=False)
        imports.delenv("VOICE_DRIVER_WAIT_FOR_PEER", raising=False)
        imports.setitem(sys.modules, "uvicorn", SimpleNamespace())
        imports.setitem(sys.modules, "fastapi", SimpleNamespace(FastAPI=App, WebSocket=object))
        imports.setitem(sys.modules, "starlette.websockets", SimpleNamespace(
            WebSocketState=SimpleNamespace(DISCONNECTED="disconnected"),
        ))
        imports.setitem(sys.modules, "inkbox", SimpleNamespace(Inkbox=object))
        imports.setitem(sys.modules, "inkbox.tunnels.client", SimpleNamespace(connect=None))
        path = Path(__file__).with_name("voice_driver.py")
        spec = importlib.util.spec_from_file_location("live_voice_driver_under_test", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module


def _clock(driver, monkeypatch, state, utterances=(), continuous=False):
    now = 100.0
    events = iter(utterances)
    next_event = next(events, None)
    pauses = []

    async def sleep(delay):
        nonlocal now, next_event
        pauses.append(delay)
        now += delay
        while next_event is not None and next_event <= now:
            state["last_heard"] = next_event
            next_event = next(events, None)
        if continuous:
            state["last_heard"] = now

    monkeypatch.setattr(driver, "asyncio", SimpleNamespace(
        get_running_loop=lambda: SimpleNamespace(time=lambda: now), sleep=sleep,
    ))
    monkeypatch.setattr(driver, "SPEAK_AFTER_S", 5)
    monkeypatch.setattr(driver, "QUIET_GAP_S", 6)
    return lambda: now, pauses


def test_first_request_waits_for_multisentence_greeting(driver, monkeypatch):
    state = {"last_heard": 0.0}
    now, pauses = _clock(driver, monkeypatch, state, utterances=[104, 108, 112])

    assert asyncio.run(driver._wait_for_greeting(state))
    assert now() == 118
    assert pauses[0] == 5
    assert now() - state["last_heard"] == 6


def test_silent_peer_gets_request_without_extra_greeting_wait(driver, monkeypatch):
    state = {"last_heard": 0.0}
    now, pauses = _clock(driver, monkeypatch, state)

    assert asyncio.run(driver._wait_for_greeting(state))
    assert now() == 105
    assert pauses == [5]


def test_continuous_peer_speech_has_bounded_wait(driver, monkeypatch):
    state = {"last_heard": 0.0}
    now, _pauses = _clock(driver, monkeypatch, state, continuous=True)

    assert not asyncio.run(driver._wait_for_greeting(state))
    assert now() == 130


def test_partial_transcript_extends_quiet_gate_before_final_transcript(driver, monkeypatch):
    observed = {}

    async def run():
        greeting_started = asyncio.Event()
        partial_seen = asyncio.Event()
        state_seen = None

        async def wait_for_greeting(state):
            nonlocal state_seen
            state_seen = state
            greeting_started.set()
            await partial_seen.wait()
            return False

        monkeypatch.setattr(driver, "_wait_for_greeting", wait_for_greeting)

        class Socket:
            client_state = "disconnected"

            def __init__(self):
                self.received = 0
                self.sent = []
                self.stopped = asyncio.Event()

            async def accept(self, **_kwargs):
                pass

            async def send_text(self, raw):
                message = json.loads(raw)
                self.sent.append(message)
                if message.get("event") == "stop":
                    self.stopped.set()

            async def receive_text(self):
                self.received += 1
                if self.received == 1:
                    return json.dumps({"event": "start"})
                if self.received == 2:
                    await greeting_started.wait()
                    return json.dumps({"event": "transcript", "text": "Still speaking", "is_final": False})
                observed["partial_activity"] = state_seen["last_heard"]
                partial_seen.set()
                await self.stopped.wait()
                return json.dumps({"event": "stop"})

        socket = Socket()
        await driver.phone_media_ws(socket)
        observed["utterances"] = [message["delta"] for message in socket.sent if "delta" in message]
        observed["stopped"] = socket.stopped.is_set()

    asyncio.run(run())
    assert observed["partial_activity"] > 0
    assert observed["utterances"] == [driver.GREETING]
    assert observed["stopped"]


@pytest.mark.parametrize("peer_at, expected_retry", [(None, 159.0), (158.0, 164.0)])
def test_long_request_does_not_requeue_before_playback_and_peer_quiet(
    driver, monkeypatch, peer_at, expected_retry,
):
    now = 100.0
    observed_state = None
    original_wait = driver._wait_for_greeting

    async def wait_for_greeting(state):
        nonlocal observed_state
        observed_state = state
        return await original_wait(state)

    async def sleep(delay):
        nonlocal now
        now += delay
        if now == peer_at:
            observed_state["last_heard"] = now
        await asyncio.sleep(0)

    monkeypatch.setattr(driver, "_wait_for_greeting", wait_for_greeting)
    monkeypatch.setattr(driver, "LINE", " ".join(["word"] * 80))
    monkeypatch.setattr(driver, "SPEAK_AFTER_S", 5)
    monkeypatch.setattr(driver, "QUIET_GAP_S", 6)
    monkeypatch.setattr(driver, "REASK_EVERY_S", 20)
    monkeypatch.setattr(driver, "LISTEN_S", 70)
    monkeypatch.setattr(driver, "MAX_REASKS", 1)
    loop = SimpleNamespace(time=lambda: now)
    monkeypatch.setattr(driver, "asyncio", SimpleNamespace(
        get_event_loop=lambda: loop, get_running_loop=lambda: loop,
        sleep=sleep, Event=asyncio.Event,
        create_task=asyncio.create_task, TimeoutError=asyncio.TimeoutError,
        CancelledError=asyncio.CancelledError,
    ))

    async def run():
        class Socket:
            client_state = "disconnected"

            def __init__(self):
                self.started = False
                self.stopped = asyncio.Event()
                self.spoken = []

            async def accept(self, **_kwargs):
                pass

            async def send_text(self, raw):
                event = json.loads(raw)
                if "delta" in event:
                    self.spoken.append((now, event["delta"]))
                if event["event"] == "stop":
                    self.stopped.set()

            async def receive_text(self):
                if not self.started:
                    self.started = True
                    return json.dumps({"event": "start"})
                await self.stopped.wait()
                return json.dumps({"event": "stop"})

        socket = Socket()
        await driver.phone_media_ws(socket)
        return socket.spoken

    spoken = asyncio.run(run())
    assert spoken == [
        (100.0, driver.GREETING),
        (105.0, driver.LINE),
        (expected_retry, driver.LINE),
    ]


@pytest.mark.parametrize(
    "require_peer, speech, expected_request, expected_stop",
    [
        (False, False, 105.0, 107.0),
        (True, True, 116.0, 118.0),
        (True, False, None, 130.0),
    ],
)
@pytest.mark.parametrize("empty_text", ["", None, "   "])
def test_initial_peer_requirement_through_actual_websocket(
    driver, monkeypatch, require_peer, speech, expected_request, expected_stop, empty_text,
):
    """Delayed speech must precede the hosted request; blank frames aren't speech."""
    now = 100.0
    pending = [
        (104, {"event": "transcript", "text": empty_text, "is_final": False}),
        (115, {"event": "transcript", "text": empty_text, "is_final": True}),
    ]
    if speech:
        pending.extend([
            (107, {"event": "transcript", "text": "Hi", "is_final": False}),
            (110, {"event": "transcript", "text": "Hi, caller", "is_final": True}),
        ])
    pending.sort(key=lambda item: item[0])
    incoming = None

    async def advance(delay):
        nonlocal now
        target = now + delay
        while pending and pending[0][0] <= target:
            now, event = pending.pop(0)
            incoming.put_nowait(json.dumps(event))
            await asyncio.sleep(0)
        now = target
        await asyncio.sleep(0)

    loop = SimpleNamespace(time=lambda: now)
    monkeypatch.setattr(driver, "asyncio", SimpleNamespace(
        get_event_loop=lambda: loop, get_running_loop=lambda: loop,
        sleep=advance, Event=asyncio.Event, create_task=asyncio.create_task,
        CancelledError=asyncio.CancelledError,
    ))
    monkeypatch.setattr(driver, "WAIT_FOR_PEER", require_peer, raising=False)
    monkeypatch.setattr(driver, "SPEAK_AFTER_S", 5)
    monkeypatch.setattr(driver, "QUIET_GAP_S", 6)
    monkeypatch.setattr(driver, "LISTEN_S", 2)
    monkeypatch.setattr(driver, "REASK_EVERY_S", 0)

    async def run():
        nonlocal incoming
        incoming = asyncio.Queue()
        incoming.put_nowait(json.dumps({"event": "start"}))

        class Socket:
            client_state = "disconnected"

            def __init__(self):
                self.sent = []

            async def accept(self, **_kwargs):
                pass

            async def send_text(self, raw):
                event = json.loads(raw)
                self.sent.append((now, event))
                if event["event"] == "stop":
                    incoming.put_nowait(json.dumps({"event": "stop"}))

            async def receive_text(self):
                return await incoming.get()

        socket = Socket()
        await driver.phone_media_ws(socket)
        return socket.sent

    sent = asyncio.run(run())
    spoken = [(when, event["delta"]) for when, event in sent if "delta" in event]
    expected_spoken = [(100.0, driver.GREETING)]
    if expected_request is not None:
        expected_spoken.append((expected_request, driver.LINE))
    assert spoken == expected_spoken
    assert [when for when, event in sent if event["event"] == "stop"] == [expected_stop]


def _run_staged_socket(driver, monkeypatch, stages, events, *, listen=30, reasks=0):
    """Exercise the real handler with only peer frames and a virtual clock."""
    now = 100.0
    pending = sorted([(100.0 + offset, event) for offset, event in events])
    incoming = None

    async def advance(delay):
        nonlocal now
        target = now + delay
        while pending and pending[0][0] <= target:
            now, event = pending.pop(0)
            incoming.put_nowait(json.dumps(event))
            await asyncio.sleep(0)
        now = target
        await asyncio.sleep(0)

    loop = SimpleNamespace(time=lambda: now)
    monkeypatch.setattr(driver, "asyncio", SimpleNamespace(
        get_event_loop=lambda: loop, get_running_loop=lambda: loop,
        sleep=advance, Event=asyncio.Event, create_task=asyncio.create_task,
        CancelledError=asyncio.CancelledError,
    ))
    monkeypatch.setattr(driver, "STAGES", stages, raising=False)
    monkeypatch.setattr(driver, "SPEAK_AFTER_S", 0)
    monkeypatch.setattr(driver, "WAIT_FOR_PEER", False)
    monkeypatch.setattr(driver, "QUIET_GAP_S", 2)
    monkeypatch.setattr(driver, "LISTEN_S", listen)
    monkeypatch.setattr(driver, "REASK_EVERY_S", 10)
    monkeypatch.setattr(driver, "MAX_REASKS", reasks)

    async def run():
        nonlocal incoming
        incoming = asyncio.Queue()
        incoming.put_nowait(json.dumps({"event": "start"}))

        class Socket:
            client_state = "disconnected"

            def __init__(self):
                self.sent = []

            async def accept(self, **_kwargs):
                pass

            async def send_text(self, raw):
                event = json.loads(raw)
                self.sent.append((now, event))
                if event["event"] == "stop":
                    incoming.put_nowait(json.dumps({"event": "stop"}))

            async def receive_text(self):
                return await incoming.get()

        socket = Socket()
        await driver.phone_media_ws(socket)
        return socket.sent

    return asyncio.run(run())


def _peer(text, *, final=True):
    return {"event": "transcript", "text": text, "is_final": final}


def _scripted_speech(sent, driver):
    return [(when, event["delta"]) for when, event in sent
            if "delta" in event and event["delta"] != driver.GREETING]


def test_stages_advance_on_new_final_replies_and_accumulate_current_phase_fragments(driver, monkeypatch):
    stages = [{"text": "intro"}, {"text": "request", "expected_reply": "alpha beta"}, {"text": "confirm"}]
    sent = _run_staged_socket(driver, monkeypatch, stages, [
        (1, _peer("ready")), (4, _peer("alpha")),
        (5, _peer("beta", final=False)), (6, _peer("beta")), (9, _peer("saved")),
    ])
    assert _scripted_speech(sent, driver) == [(100.0, "intro"), (103.0, "request"), (108.0, "confirm")]
    assert [when for when, event in sent if event["event"] == "stop"] == [130.0]
    assert all(event["event"] in {"text", "stop"} for _when, event in sent)


def test_stages_do_not_reuse_previous_stage_reply(driver, monkeypatch):
    sent = _run_staged_socket(driver, monkeypatch, [
        {"text": "intro"}, {"text": "request", "expected_reply": "alpha beta"}, {"text": "confirm"},
    ], [(1, _peer("alpha beta"))])
    assert _scripted_speech(sent, driver) == [(100.0, "intro"), (103.0, "request")]


def test_stages_ignore_partial_blank_and_text_done_as_reply(driver, monkeypatch):
    sent = _run_staged_socket(driver, monkeypatch, [{"text": "intro"}, {"text": "request"}], [
        (1, _peer("")), (2, _peer("ready", final=False)), (3, _peer(None)),
        (4, _peer("  ")), (5, {"event": "text", "done": True}), (8, _peer("ready")),
    ], listen=15)
    assert _scripted_speech(sent, driver) == [(100.0, "intro"), (110.0, "request")]


def test_stages_share_retry_budget_and_retry_only_current_stage(driver, monkeypatch):
    sent = _run_staged_socket(driver, monkeypatch, [{"text": "intro"}, {"text": "request"}], [
        (15, _peer("ready")),
        (22, {"event": "barge_in", "tts_interrupted": True}),
    ], listen=40, reasks=2)
    assert _scripted_speech(sent, driver) == [
        (100.0, "intro"), (110.0, "intro"), (117.0, "request"), (127.0, "request"),
    ]
    assert [when for when, event in sent if event["event"] == "stop"] == [140.0]


def test_stages_respect_spoken_length_before_advancing(driver, monkeypatch):
    long_text = " ".join(["word"] * 20)
    sent = _run_staged_socket(driver, monkeypatch, [{"text": long_text}, {"text": "request"}], [
        (1, _peer("ready")),
    ])
    assert _scripted_speech(sent, driver) == [(100.0, long_text), (112.0, "request")]


def test_stages_do_not_reset_deadline_or_send_at_deadline(driver, monkeypatch):
    sent = _run_staged_socket(driver, monkeypatch, [{"text": "intro"}, {"text": "request"}], [
        (28, _peer("ready")),
    ])
    assert _scripted_speech(sent, driver) == [(100.0, "intro")]
    assert [when for when, event in sent if event["event"] == "stop"] == [130.0]


def test_stages_stop_cancels_future_speech(driver, monkeypatch):
    sent = _run_staged_socket(driver, monkeypatch, [{"text": "intro"}, {"text": "request"}], [
        (1, _peer("ready")), (5, {"event": "stop"}),
    ], reasks=2)
    assert _scripted_speech(sent, driver) == [(100.0, "intro"), (103.0, "request")]
    assert not any(event["event"] == "stop" for _when, event in sent)


def test_stage_loader_accepts_bounded_text_and_optional_reply(driver, tmp_path):
    path = tmp_path / "stages.json"
    path.write_text(json.dumps([
        {"text": "  intro  "}, {"text": "request", "expected_reply": "  alpha beta  "},
    ]))
    assert driver._load_stages(str(path)) == [
        {"text": "intro"}, {"text": "request", "expected_reply": "alpha beta"},
    ]


@pytest.mark.parametrize("value", [
    {}, [], [{"text": "x"}] * 4, [None], [{}], [{"text": "   "}],
    [{"text": 3}], [{"text": "x", "expected_reply": None}],
    [{"text": "x", "expected_reply": "..."}], [{"text": "x", "expected_repy": "yes"}],
])
def test_stage_loader_rejects_invalid_shape_without_echoing_content(driver, tmp_path, value):
    path = tmp_path / "stages.json"
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError):
        driver._load_stages(str(path))


def test_stage_loader_does_not_echo_malformed_json(driver, tmp_path):
    path = tmp_path / "stages.json"
    path.write_text('private-request-body {')
    with pytest.raises(ValueError, match="must contain valid JSON") as error:
        driver._load_stages(str(path))
    assert "private-request-body" not in str(error.value)
