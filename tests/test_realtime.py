import asyncio
import json

from inkbox_claude import realtime
from inkbox_claude.realtime import (
    CONSULT_TOOL_NAME,
    RealtimeCallMeta,
    RealtimeConfig,
    _BridgeState,
    _dispatch_tool_call,
    _send_session_update,
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


def _meta():
    return RealtimeCallMeta(call_id="c1", remote_phone_number="+15551234567", project_dir="/tmp/proj")


def test_session_update_configures_telephony_audio_vad_and_consult_tool():
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
    # Exactly one tool — the Claude Code consult.
    assert [t["name"] for t in sess["tools"]] == [CONSULT_TOOL_NAME]


def test_instructions_name_the_consult_tool_and_project():
    text = build_realtime_instructions(_meta())
    assert CONSULT_TOOL_NAME in text
    assert "/tmp/proj" in text


def test_dispatch_consult_runs_agent_and_speaks_answer():
    ws = _FakeWS()
    state = _BridgeState()

    async def fake_consult(query, transcript):
        assert query == "run the tests"
        return "tests pass, 42 green"

    asyncio.run(_dispatch_tool_call(
        openai_ws=ws,
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
        call_id="call-4",
        name=CONSULT_TOOL_NAME,
        arguments_json=json.dumps({"query": "x"}),
        state=_BridgeState(),
        config=cfg,
        on_agent_consult=slow_consult,
    ))
    item = next(f for f in ws.sent if f.get("type") == "conversation.item.create")
    assert "timed out" in json.loads(item["item"]["output"])["error"]
