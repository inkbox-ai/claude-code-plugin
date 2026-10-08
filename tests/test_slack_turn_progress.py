"""Native host progress and Stop retain the original Slack request owner."""

import asyncio
from claude_agent_sdk import AssistantMessage, ResultMessage, ToolUseBlock

from inkbox_claude.slack_streams import tool_progress
from tests.test_sessions import make_session
from tests.test_slack_activity import route


def result(text="Finished"):
    return ResultMessage(subtype="success", duration_ms=1, duration_api_ms=1,
        is_error=False, num_turns=1, session_id="test-session", result=text)


def test_progress_observes_real_tool_blocks_without_inputs_or_latest_route():
    async def run():
        session = make_session([])
        events = []
        async def activity(chat, mode, meta, state):
            events.append((mode, dict(meta), state))
        session.activity_fn = activity
        source = route()
        class Host:
            async def query(self, _):
                session.reply_meta = {"conversation_id": "D_OTHER"}
            async def receive_response(self):
                yield AssistantMessage(model="test", parent_tool_use_id="older-parent", content=[ToolUseBlock(
                    id="child", name="Bash", input={"command": "private arguments"})])
                yield AssistantMessage(model="test", content=[ToolUseBlock(
                    id="private-worker-id", name="Read", input={"file_path": "/private/example"})])
                yield result()
        session._client = Host()
        await session.handle_inbound("Inspect the file", "slack", source)
        await session._worker
        tool_events = [event for event in events if event[2].startswith("tool:")]
        assert tool_events == [("slack", source, "tool:Read")]
        assert "/private" not in str(events) and "private-worker-id" not in str(events)
    asyncio.run(run())


def test_delegated_upload_cannot_borrow_a_new_parent_turn():
    async def run():
        session = make_session([])
        denied = await session._observe_a2a_tool_start({"tool_name": "mcp__inkbox__inkbox_slack_upload_file",
            "agent_id": "background-worker", "tool_input": {"file_path": "/private/example"}}, None, None)
        assert denied["hookSpecificOutput"]["permissionDecision"] == "deny"
        assert "/private" not in str(denied) and "background-worker" not in str(denied)
        assert await session._observe_a2a_tool_start({"tool_name": "mcp__inkbox__inkbox_slack_upload_file",
            "agent_type": "main-custom-agent"}, None, None) == {}
    asyncio.run(run())


def test_unknown_tool_names_never_become_public_status_text():
    assert tool_progress("mcp__inkbox__inkbox_slack_upload_file") == "Uploading an attachment…"
    assert tool_progress("mcp__inkbox__inkbox_a2a_call") == "Delegating work…"
    assert tool_progress("mcp__inkbox__inkbox_a2a_check") == "Checking delegated work…"
    assert tool_progress("untrusted-secret-worker-id") == "Using a tool…"


def test_native_slack_stop_then_new_request_runs_without_old_progress_or_reply():
    async def run():
        sent, events = [], []
        session = make_session(sent)
        async def activity(chat, mode, meta, state):
            events.append((meta["source_event_id"], state))
        session.activity_fn = activity
        started, interrupted = asyncio.Event(), asyncio.Event()
        class Host:
            def __init__(self):
                self.queries = 0
            async def query(self, _):
                self.queries += 1
            async def receive_response(self):
                if self.queries == 1:
                    yield AssistantMessage(model="test", content=[ToolUseBlock(id="first", name="Read", input={})])
                    started.set()
                    await interrupted.wait()
                    yield AssistantMessage(model="test", content=[ToolUseBlock(id="late", name="Bash", input={})])
                    yield result("Old answer")
                else:
                    yield AssistantMessage(model="test", content=[ToolUseBlock(id="new", name="Grep", input={})])
                    yield result("New answer")
            async def interrupt(self):
                interrupted.set()
        host = Host()
        session._client = host
        first = {**route(), "actor_id": "U_TEST", "workspace_id": "T_TEST"}
        await session.handle_inbound("First request", "slack", first)
        await asyncio.wait_for(started.wait(), 2)
        targets = session.slack_stop_targets(first)
        assert len(targets) == 1
        await session._abort_in_flight(only_turns=targets)
        await asyncio.wait_for(session._worker, 2)
        later = {**first, "source_event_id": "event-2", "message_ts": "1234567890.000003"}
        await session.handle_inbound("New request", "slack", later)
        await asyncio.wait_for(session._worker, 2)
        assert host.queries == 2
        assert [item[1] for item in sent] == ["New answer"]
        assert ("event-1", "cancelled") in events
        assert ("event-1", "tool:Bash") not in events
        assert ("event-2", "tool:Grep") in events
        assert ("event-2", "completed") in events
    asyncio.run(run())
