import asyncio
import json
from types import SimpleNamespace

from inkbox_claude import realtime
from inkbox_claude.realtime import (
    CONSULT_TOOL_NAME,
    DELETE_POST_CALL_ACTION_TOOL_NAME,
    EDIT_POST_CALL_ACTION_TOOL_NAME,
    HANG_UP_CALL_TOOL_NAME,
    HANGUP_CLOSE_DELAY_S,
    POST_CALL_ACTION_TOOL_NAME,
    RealtimeCallMeta,
    RealtimeConfig,
    _BridgeState,
    _openai_to_inkbox_pump,
    _dispatch_post_call,
    _dispatch_tool_call,
    _send_session_update,
    build_realtime_greeting,
    build_realtime_instructions,
)


class _FakeWS:
    """Records every send_str payload (parsed) for assertions."""

    def __init__(self):
        self.sent = []

    async def send_str(self, data):
        self.sent.append(json.loads(data))

    def types(self):
        return [f.get("type") for f in self.sent]


class _FakeMsg:
    def __init__(self, data):
        self.type = realtime.aiohttp.WSMsgType.TEXT
        self.data = data


class _ScriptedOpenAIWS(_FakeWS):
    """Async-iterable fake yielding Realtime frames and recording writes."""

    def __init__(self, frames):
        super().__init__()
        self._frames = [_FakeMsg(json.dumps(frame)) for frame in frames]

    def __aiter__(self):
        self._iter = iter(self._frames)
        return self

    async def __anext__(self):
        try:
            return next(self._iter)
        except StopIteration:
            raise StopAsyncIteration


def _meta():
    return RealtimeCallMeta(
        call_id="c1",
        remote_phone_number="+15551234567",
        project_dir="/tmp/proj",
    )


def test_session_update_configures_telephony_audio_vad_and_all_tools():
    ws = _FakeWS()
    asyncio.run(_send_session_update(ws, RealtimeConfig(api_key="sk-x"), _meta()))
    assert len(ws.sent) == 1
    sess = ws.sent[0]["session"]
    assert ws.sent[0]["type"] == "session.update"
    assert sess["output_modalities"] == ["audio"]
    # μ-law telephony on both legs.
    assert sess["audio"]["input"]["format"] == {"type": "audio/pcmu"}
    assert sess["audio"]["output"]["format"] == {"type": "audio/pcmu"}
    # Server-side VAD drives turns + barge-in.
    assert sess["audio"]["input"]["turn_detection"]["type"] == "server_vad"
    assert sess["audio"]["input"]["turn_detection"]["interrupt_response"] is True
    # All five call tools are exposed.
    assert [t["name"] for t in sess["tools"]] == [
        CONSULT_TOOL_NAME,
        POST_CALL_ACTION_TOOL_NAME,
        EDIT_POST_CALL_ACTION_TOOL_NAME,
        DELETE_POST_CALL_ACTION_TOOL_NAME,
        HANG_UP_CALL_TOOL_NAME,
    ]


def test_instructions_name_the_consult_tool_and_project():
    meta = RealtimeCallMeta(
        call_id="c1",
        remote_phone_number="+15551234567",
        project_dir="/tmp/proj",
        agent_identity_handle="claude",
        agent_identity_email="claude@example.com",
        agent_identity_phone="+15550001111",
        contact_known=True,
        contact_id="contact-1",
        contact_name="Ada Lovelace",
        contact_emails=["ada@example.com"],
        contact_phones=["+15551234567"],
        contact_company="Inkbox",
        contact_job_title="Engineer",
        contact_notes="Prefers calls in the morning.",
    )
    text = build_realtime_instructions(meta)
    assert CONSULT_TOOL_NAME in text
    assert "/tmp/proj" in text
    assert "Your Inkbox identity handle: claude." in text
    assert "claude@example.com" in text
    assert "Ada Lovelace" in text
    assert "ada@example.com" in text
    assert "Do not perform a context lookup before greeting" in text
    assert "look up contacts" in text


def test_instructions_name_the_two_lines_when_imessage_enabled():
    meta = RealtimeCallMeta(
        call_id="c1",
        remote_phone_number="+15551234567",
        project_dir="/tmp/proj",
        agent_identity_phone="+15550001111",
        agent_imessage_enabled=True,
    )
    text = build_realtime_instructions(meta)
    assert "Your dedicated phone line (your own number, for SMS and voice calls): +15550001111." in text
    assert "shared Inkbox iMessage line" in text
    # The shared line's number must never be spoken or promised.
    assert "never state or promise a number for it" in text
    assert "calls follow the conversation's channel" in text


def test_instructions_omit_shared_line_when_imessage_disabled():
    meta = RealtimeCallMeta(
        call_id="c1",
        remote_phone_number="+15551234567",
        project_dir="/tmp/proj",
        agent_identity_phone="+15550001111",
    )
    text = build_realtime_instructions(meta)
    assert "shared Inkbox iMessage line" not in text


def test_outbound_call_context_shapes_realtime_prompt_and_greeting():
    meta = RealtimeCallMeta(
        call_id="c1",
        remote_phone_number="+15551234567",
        direction="outbound",
        project_dir="/tmp/proj",
        contact_known=True,
        contact_name="Ada Lovelace",
        outbound_purpose="tell them the deployment is fixed",
        outbound_opening="Hi, this is Claude Code calling with the deployment update.",
        outbound_context="Deployment failed twice before the final fix.",
    )

    text = build_realtime_instructions(meta)

    assert "outbound call" in text
    assert "tell them the deployment is fixed" in text
    assert "Deployment failed twice before the final fix." in text
    assert "Hi, this is Claude Code calling with the deployment update." in build_realtime_greeting(meta)


def test_dispatch_consult_runs_agent_and_speaks_answer():
    ws = _FakeWS()
    state = _BridgeState()

    async def fake_consult(query, transcript):
        assert query == "run the tests"
        return "tests pass, 42 green"

    asyncio.run(_dispatch_tool_call(
        openai_ws=ws,
        inkbox_ws=None,
        call_id="call-1",
        name=CONSULT_TOOL_NAME,
        arguments_json=json.dumps({"query": "run the tests"}),
        state=state,
        config=RealtimeConfig(api_key="sk-x"),
        on_agent_consult=fake_consult,
    ))

    # An interim "one moment" response.create, then the tool output + a
    # response.create so the model speaks the answer.
    assert "conversation.item.create" in ws.types()
    item = next(f for f in ws.sent if f.get("type") == "conversation.item.create")
    assert item["item"]["type"] == "function_call_output"
    assert item["item"]["call_id"] == "call-1"
    output = json.loads(item["item"]["output"])
    assert output["status"] == "ok"
    assert output["answer"] == "tests pass, 42 green"
    assert ws.types().count("response.create") >= 1


def test_dispatch_missing_query_returns_error():
    ws = _FakeWS()

    async def fake_consult(query, transcript):  # pragma: no cover - must not run
        raise AssertionError("consult should not be called without a query")

    asyncio.run(_dispatch_tool_call(
        openai_ws=ws,
        inkbox_ws=None,
        call_id="call-2",
        name=CONSULT_TOOL_NAME,
        arguments_json="{}",
        state=_BridgeState(),
        config=RealtimeConfig(api_key="sk-x"),
        on_agent_consult=fake_consult,
    ))
    item = next(f for f in ws.sent if f.get("type") == "conversation.item.create")
    assert "error" in json.loads(item["item"]["output"])


def test_dispatch_unknown_tool_refuses():
    ws = _FakeWS()

    async def fake_consult(query, transcript):  # pragma: no cover
        raise AssertionError("not the consult tool")

    asyncio.run(_dispatch_tool_call(
        openai_ws=ws,
        inkbox_ws=None,
        call_id="call-3",
        name="some_other_tool",
        arguments_json="{}",
        state=_BridgeState(),
        config=RealtimeConfig(api_key="sk-x"),
        on_agent_consult=fake_consult,
    ))
    item = next(f for f in ws.sent if f.get("type") == "conversation.item.create")
    assert "not available" in json.loads(item["item"]["output"])["error"]


def test_consult_timeout_reports_error_not_crash():
    ws = _FakeWS()

    async def slow_consult(query, transcript):
        await asyncio.sleep(1)
        return "too late"

    cfg = RealtimeConfig(api_key="sk-x", consult_timeout_s=0.01)
    asyncio.run(_dispatch_tool_call(
        openai_ws=ws,
        inkbox_ws=None,
        call_id="call-4",
        name=CONSULT_TOOL_NAME,
        arguments_json=json.dumps({"query": "x"}),
        state=_BridgeState(),
        config=cfg,
        on_agent_consult=slow_consult,
    ))
    item = next(f for f in ws.sent if f.get("type") == "conversation.item.create")
    assert "timed out" in json.loads(item["item"]["output"])["error"]


def test_realtime_transcripts_are_mirrored_into_inkbox(monkeypatch):
    monkeypatch.setattr(
        realtime,
        "aiohttp",
        SimpleNamespace(
            WSMsgType=SimpleNamespace(
                TEXT="text",
                CLOSE="close",
                CLOSED="closed",
                ERROR="error",
            ),
        ),
    )
    openai_ws = _ScriptedOpenAIWS([
        {
            "type": "conversation.item.input_audio_transcription.completed",
            "transcript": "who is Alex",
        },
        {
            "type": "response.output_audio_transcript.done",
            "transcript": "one moment",
        },
    ])
    inkbox_ws = _FakeWS()
    state = _BridgeState()

    async def fake_consult(query, transcript):  # pragma: no cover - must not run
        raise AssertionError("transcript mirroring should not consult")

    asyncio.run(_openai_to_inkbox_pump(
        openai_ws=openai_ws,
        inkbox_ws=inkbox_ws,
        state=state,
        config=RealtimeConfig(api_key="sk-x"),
        meta=_meta(),
        on_agent_consult=fake_consult,
    ))

    assert [frame for frame in inkbox_ws.sent if frame.get("event") == "transcript"] == [
        {
            "event": "transcript",
            "party": "remote",
            "text": "who is Alex",
            "is_final": True,
        },
        {
            "event": "transcript",
            "party": "local",
            "text": "one moment",
            "is_final": True,
        },
    ]
    assert state.transcript == [("caller", "who is Alex"), ("agent", "one moment")]


def test_openai_pump_dispatches_call_id_keyed_consult_events(monkeypatch):
    """GA Realtime may key argument events by call_id."""
    async def scenario():
        monkeypatch.setattr(
            realtime,
            "aiohttp",
            SimpleNamespace(
                WSMsgType=SimpleNamespace(
                    TEXT="text",
                    CLOSE="close",
                    CLOSED="closed",
                    ERROR="error",
                ),
            ),
        )
        openai_ws = _ScriptedOpenAIWS([
            {
                "type": "response.output_item.added",
                "item_id": "item-1",
                "item": {
                    "type": "function_call",
                    "call_id": "call-1",
                    "name": CONSULT_TOOL_NAME,
                },
            },
            {
                "type": "response.function_call_arguments.delta",
                "call_id": "call-1",
                "name": CONSULT_TOOL_NAME,
                "delta": '{"query":"who is Alex?"}',
            },
            {
                "type": "response.function_call_arguments.done",
                "call_id": "call-1",
                "name": CONSULT_TOOL_NAME,
            },
        ])
        state = _BridgeState()
        seen = {}

        async def fake_consult(query, transcript):
            seen["query"] = query
            seen["transcript"] = transcript
            return "Alex is in the contact book."

        await _openai_to_inkbox_pump(
            openai_ws=openai_ws,
            inkbox_ws=_FakeWS(),
            state=state,
            config=RealtimeConfig(api_key="sk-x"),
            meta=_meta(),
            on_agent_consult=fake_consult,
        )
        if state.consult_tasks:
            await asyncio.gather(*state.consult_tasks)

        assert seen["query"] == "who is Alex?"
        item = next(f for f in openai_ws.sent if f.get("type") == "conversation.item.create")
        output = json.loads(item["item"]["output"])
        assert output["status"] == "ok"
        assert output["answer"] == "Alex is in the contact book."

    asyncio.run(scenario())


def test_openai_pump_uses_frame_item_id_when_item_has_no_id(monkeypatch):
    """output_item.added sometimes carries item_id on the frame."""
    async def scenario():
        monkeypatch.setattr(
            realtime,
            "aiohttp",
            SimpleNamespace(
                WSMsgType=SimpleNamespace(
                    TEXT="text",
                    CLOSE="close",
                    CLOSED="closed",
                    ERROR="error",
                ),
            ),
        )
        openai_ws = _ScriptedOpenAIWS([
            {
                "type": "response.output_item.added",
                "item_id": "item-2",
                "item": {
                    "type": "function_call",
                    "call_id": "call-2",
                    "name": POST_CALL_ACTION_TOOL_NAME,
                },
            },
            {
                "type": "response.function_call_arguments.delta",
                "item_id": "item-2",
                "delta": '{"action":"email Dima the summary"}',
            },
            {
                "type": "response.function_call_arguments.done",
                "item_id": "item-2",
                "call_id": "call-2",
            },
        ])
        state = _BridgeState()

        async def fake_consult(query, transcript):  # pragma: no cover - must not run
            raise AssertionError("post-call action should not consult")

        await _openai_to_inkbox_pump(
            openai_ws=openai_ws,
            inkbox_ws=_FakeWS(),
            state=state,
            config=RealtimeConfig(api_key="sk-x"),
            meta=_meta(),
            on_agent_consult=fake_consult,
        )
        if state.consult_tasks:
            await asyncio.gather(*state.consult_tasks)

        assert state.post_call_actions == [{"action": "email Dima the summary", "details": ""}]

    asyncio.run(scenario())


# ----------------------------------------------------------------------
# Post-call action tools + hangup + post-call dispatch
# ----------------------------------------------------------------------


def _dispatch(ws, name, args, state, inkbox_ws=None):
    asyncio.run(_dispatch_tool_call(
        openai_ws=ws,
        inkbox_ws=inkbox_ws,
        call_id="t",
        name=name,
        arguments_json=json.dumps(args),
        state=state,
        config=RealtimeConfig(api_key="sk-x"),
        on_agent_consult=lambda q, t: (_ for _ in ()).throw(AssertionError("no consult")),
    ))


def _last_output(ws):
    item = next(f for f in reversed(ws.sent) if f.get("type") == "conversation.item.create")
    return json.loads(item["item"]["output"])


def test_register_edit_delete_post_call_actions():
    ws, state = _FakeWS(), _BridgeState()

    _dispatch(ws, POST_CALL_ACTION_TOOL_NAME, {"action": "email the summary"}, state)
    assert state.post_call_actions == [{"action": "email the summary", "details": ""}]
    assert _last_output(ws)["status"] == "queued"

    _dispatch(ws, EDIT_POST_CALL_ACTION_TOOL_NAME,
              {"action_index": 1, "details": "to dima@x.com"}, state)
    assert state.post_call_actions[0]["details"] == "to dima@x.com"
    assert _last_output(ws)["status"] == "updated"

    _dispatch(ws, DELETE_POST_CALL_ACTION_TOOL_NAME, {"action_index": 1}, state)
    assert state.post_call_actions == []
    assert _last_output(ws)["status"] == "deleted"


def test_edit_and_delete_reject_bad_index():
    ws, state = _FakeWS(), _BridgeState()
    _dispatch(ws, EDIT_POST_CALL_ACTION_TOOL_NAME, {"action_index": 5, "action": "x"}, state)
    assert "invalid action_index" in _last_output(ws)["error"]
    _dispatch(ws, DELETE_POST_CALL_ACTION_TOOL_NAME, {"action_index": 1}, state)
    assert "invalid action_index" in _last_output(ws)["error"]


class _FakeInkboxWS:
    def __init__(self):
        self.sent = []
        self.closed = False

    async def send_str(self, data):
        self.sent.append(json.loads(data))

    async def close(self):
        self.closed = True


def test_hangup_is_two_step(monkeypatch):
    # Don't actually wait out the close delay.
    monkeypatch.setattr(realtime, "HANGUP_CLOSE_DELAY_S", 0.0)
    ws, ink, state = _FakeWS(), _FakeInkboxWS(), _BridgeState()
    state.stream_id = "s1"

    # First call: arm + ask for goodbye, no stop frame yet.
    _dispatch(ws, HANG_UP_CALL_TOOL_NAME, {}, state, inkbox_ws=ink)
    assert _last_output(ws)["status"] == "confirm_goodbye"
    assert state.hangup_armed_at is not None
    assert not any(f.get("event") == "stop" for f in ink.sent)

    # Second call: real stop frame to Inkbox + sockets closed.
    _dispatch(ws, HANG_UP_CALL_TOOL_NAME, {"reason": "done"}, state, inkbox_ws=ink)
    stop = next(f for f in ink.sent if f.get("event") == "stop")
    assert stop["reason"] == "done" and stop["stream_id"] == "s1"
    assert ink.closed is True and state.closed is True


def test_post_call_dispatch_runs_actions_when_queued():
    state = _BridgeState()
    state.post_call_actions = [{"action": "open a PR", "details": ""}]
    state.transcript = [("caller", "open a pr please")]
    seen = {}

    async def on_actions(actions, transcript):
        seen["actions"] = actions
        seen["transcript"] = transcript

    async def on_ended(transcript):  # pragma: no cover - must not run
        raise AssertionError("should not reflect when actions are queued")

    asyncio.run(_dispatch_post_call(state, on_actions, on_ended))
    assert seen["actions"] == [{"action": "open a PR", "details": ""}]


def test_post_call_dispatch_reflects_when_no_actions():
    state = _BridgeState()
    state.transcript = [("agent", "bye")]
    seen = {}

    async def on_actions(actions, transcript):  # pragma: no cover - must not run
        raise AssertionError("no actions to run")

    async def on_ended(transcript):
        seen["transcript"] = transcript

    asyncio.run(_dispatch_post_call(state, on_actions, on_ended))
    assert seen["transcript"] == [("agent", "bye")]
