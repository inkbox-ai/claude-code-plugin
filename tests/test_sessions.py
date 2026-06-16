import asyncio
import json
import os
from pathlib import Path

from inkbox_claude.config import BridgeConfig
from inkbox_claude.sessions import (
    ContactSession,
    _parse_index,
    list_recent_sessions,
)


def make_session(sent, typing=None):
    async def send_fn(chat_id, text, mode, meta):
        sent.append((chat_id, text, mode, dict(meta)))

    typing_fn = None
    if typing is not None:
        async def typing_fn(chat_id, mode, meta):  # noqa: F811
            typing.append((chat_id, mode, dict(meta)))

    cfg = BridgeConfig(permission_timeout_s=2.0, project_dir="/tmp")
    return ContactSession(
        chat_id="contact-1",
        cfg=cfg,
        send_fn=send_fn,
        mcp_server=None,
        mcp_tool_names=[],
        identity_info={"handle": "t", "email": "", "phone": ""},
        typing_fn=typing_fn,
    )


def test_pending_escalation_consumes_next_inbound():
    async def scenario():
        sent = []
        session = make_session(sent)
        session.mode = "sms"

        task = asyncio.create_task(
            session._escalate("permission", "ok to run tests?", tool_name="Bash")
        )
        await asyncio.sleep(0.05)  # escalation text goes out, future is pending
        assert sent and sent[0][1] == "ok to run tests?"

        # The human's reply answers the escalation instead of queueing a turn.
        await session.handle_inbound("yes", "sms", {"conversation_id": "c1"})
        assert await task == "yes"
        assert session._queue.empty()

    asyncio.run(scenario())


def test_escalation_timeout_returns_none():
    async def scenario():
        sent = []
        session = make_session(sent)
        session.cfg.permission_timeout_s = 0.05
        result = await session._escalate("permission", "anyone there?")
        assert result is None
        assert session.pending is None

    asyncio.run(scenario())


def test_typing_loop_pings_imessage_only():
    async def scenario():
        typing = []
        session = make_session([], typing)
        session.mode = "imessage"
        session.reply_meta = {"conversation_id": "c1"}

        task = asyncio.create_task(session._typing_loop())
        await asyncio.sleep(0.05)  # first tick fires immediately
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        assert typing and typing[0] == ("contact-1", "imessage", {"conversation_id": "c1"})

    asyncio.run(scenario())


def test_typing_loop_skips_non_imessage():
    async def scenario():
        typing = []
        session = make_session([], typing)
        session.mode = "sms"  # SMS has no typing indicator

        task = asyncio.create_task(session._typing_loop())
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        assert typing == []

    asyncio.run(scenario())


def test_clear_command_starts_fresh_session():
    async def scenario():
        sent = []
        cleared = []
        session = make_session(sent)
        session.on_clear = lambda chat_id: cleared.append(chat_id)
        session.mode = "imessage"

        class FakeClient:
            def __init__(self):
                self.disconnects = 0

            async def disconnect(self):
                self.disconnects += 1

        fake = FakeClient()
        session._client = fake
        session.resume_session_id = "old-session"
        session.always_allowed.add("Bash")

        await session.handle_inbound("/clear", "imessage", {"conversation_id": "c1"})

        # Resume id forgotten, client torn down, persisted state cleared.
        assert session.resume_session_id is None
        assert session._client is None
        assert fake.disconnects == 1
        assert cleared == ["contact-1"]
        assert session.always_allowed == set()
        # The command is confirmed and never queued as a Claude turn.
        assert session._queue.empty()
        assert "fresh conversation" in sent[-1][1].lower()

    asyncio.run(scenario())


def test_stop_command_interrupts_turn_without_clearing():
    async def scenario():
        sent = []
        session = make_session(sent)

        class FakeClient:
            def __init__(self):
                self.interrupts = 0

            async def interrupt(self):
                self.interrupts += 1

        fake = FakeClient()
        session._client = fake
        session._turn_active = True
        session.resume_session_id = "keep-me"
        session._worker = asyncio.create_task(asyncio.sleep(10))

        await session.handle_inbound("/stop", "imessage", {"conversation_id": "c1"})

        assert fake.interrupts == 1
        assert session._interrupting is True
        # Context is preserved — /stop only halts the current work.
        assert session.resume_session_id == "keep-me"
        assert session._queue.empty()
        assert sent[-1][1] == "Stopped."

        session._worker.cancel()

    asyncio.run(scenario())


def test_stop_command_when_idle():
    async def scenario():
        sent = []
        session = make_session(sent)
        await session.handle_inbound("/stop", "sms", {"conversation_id": "c1"})
        assert sent[-1][1] == "Nothing to stop — I'm idle."
        assert session._queue.empty()

    asyncio.run(scenario())


def test_cancel_is_an_alias_for_stop():
    from inkbox_claude.sessions import _control_command

    assert _control_command("/cancel") == "stop"

    async def scenario():
        sent = []
        session = make_session(sent)
        await session.handle_inbound("/cancel", "sms", {"conversation_id": "c1"})
        assert sent[-1][1] == "Nothing to stop — I'm idle."  # same behavior as /stop
        assert session._queue.empty()

    asyncio.run(scenario())


def test_non_command_is_forwarded_as_a_turn():
    async def scenario():
        sent = []
        session = make_session(sent)
        # A message that merely mentions a slash word is a normal turn.
        await session.handle_inbound("please /clear the cache", "sms", {})
        assert not session._queue.empty()
        assert session._queue.get_nowait().endswith("please /clear the cache")
        session._worker.cancel()

    asyncio.run(scenario())


def test_status_command_reports_idle_without_queueing():
    async def scenario():
        sent = []
        session = make_session(sent)
        await session.handle_inbound("/status", "imessage", {"conversation_id": "c1"})
        # Reports state, starts no turn.
        assert "idle" in sent[-1][1].lower()
        assert session._queue.empty()

    asyncio.run(scenario())


def test_status_command_does_not_interrupt_a_running_turn():
    async def scenario():
        sent = []
        session = make_session(sent)

        class FakeClient:
            def __init__(self):
                self.interrupts = 0

            async def interrupt(self):
                self.interrupts += 1

        fake = FakeClient()
        session._client = fake
        session._turn_active = True
        session._worker = asyncio.create_task(asyncio.sleep(10))

        await session.handle_inbound("/status", "imessage", {"conversation_id": "c1"})

        # Read-only: it reports "working" and leaves the turn running.
        assert fake.interrupts == 0
        assert session._interrupting is False
        assert "working" in sent[-1][1].lower()

        session._worker.cancel()

    asyncio.run(scenario())


def test_health_command_reports_gateway_health():
    async def scenario():
        sent = []
        session = make_session(sent)

        async def fake_health():
            return "Inkbox: reachable as agent (iMessage)\nClaude: ready (subscription login)"

        session.health_fn = fake_health
        await session.handle_inbound("/health", "imessage", {"conversation_id": "c1"})
        assert "Inkbox: reachable" in sent[-1][1]
        assert "Claude: ready" in sent[-1][1]
        assert session._queue.empty()  # report only, no Claude turn

    asyncio.run(scenario())


def test_usage_command_reports_claude_usage(monkeypatch):
    # /usage delegates to claude_usage.usage_report (the real subscription fetch).
    import inkbox_claude.claude_usage as cu

    async def scenario():
        sent = []
        session = make_session(sent)
        monkeypatch.setattr(cu, "usage_report", lambda: "Claude usage:\n5-hour session: 12% used")
        await session.handle_inbound("/usage", "imessage", {"conversation_id": "c1"})
        assert "5-hour session: 12% used" in sent[-1][1]
        assert session._queue.empty()  # report only, no Claude turn

    asyncio.run(scenario())


def _make_transcripts(base, project, specs):
    """Write fake Claude Code transcripts.

    Each spec is (name, [user message contents], mtime) — one JSONL user line
    is written per content string.
    """
    slug = str(Path(project).resolve()).replace("/", "-")
    tdir = Path(base) / "projects" / slug
    tdir.mkdir(parents=True, exist_ok=True)
    for name, contents, mtime in specs:
        path = tdir / name
        body = "".join(
            json.dumps({"type": "user", "message": {"content": c}}) + "\n"
            for c in contents
        )
        path.write_text(body)
        os.utime(path, (mtime, mtime))
    return tdir


def test_list_recent_sessions_orders_excludes_and_summarizes(tmp_path, monkeypatch):
    project = str(tmp_path / "proj")
    base = tmp_path / "cfg"
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(base))

    _make_transcripts(
        base,
        project,
        [
            ("aaa.jsonl", ["[iMessage from +1] fix the auth bug"], 100),
            ("bbb.jsonl", ["exclude me"], 200),
            ("ccc.jsonl", ["older one"], 50),
            # First user line is an injected reminder — the digest should skip
            # it and use the next real message.
            ("ddd.jsonl", ["<system-reminder>x</system-reminder>", "the real message"], 300),
        ],
    )

    out = list_recent_sessions(project, exclude_id="bbb")
    # Newest first, excluded id dropped.
    assert [s["id"] for s in out] == ["ddd", "aaa", "ccc"]
    # Channel tag stripped, and the reminder line skipped.
    assert out[1]["summary"] == "fix the auth bug"
    assert out[0]["summary"] == "the real message"


def test_parse_index():
    assert _parse_index("2", 3) == 1
    assert _parse_index("#3 please", 3) == 2
    assert _parse_index("0", 3) is None
    assert _parse_index("9", 3) is None
    assert _parse_index("nope", 3) is None


def test_resume_command_with_no_sessions(tmp_path, monkeypatch):
    async def scenario():
        sent = []
        session = make_session(sent)
        session.cfg.project_dir = str(tmp_path / "empty-proj")
        monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "cfg"))
        await session.handle_inbound("/resume", "imessage", {"conversation_id": "c1"})
        assert sent[-1][1] == "No other recent conversations to resume."
        assert session._queue.empty()

    asyncio.run(scenario())


def test_resume_command_lists_then_swaps_on_pick(tmp_path, monkeypatch):
    async def scenario():
        project = str(tmp_path / "proj")
        base = tmp_path / "cfg"
        monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(base))
        _make_transcripts(
            base,
            project,
            [
                ("newer.jsonl", ["the newer conversation"], 200),
                ("older.jsonl", ["the older conversation"], 100),
            ],
        )

        sent = []
        persisted = []
        session = make_session(sent)
        session.cfg.project_dir = project
        session.on_session_id = lambda chat_id, sid: persisted.append((chat_id, sid))

        class FakeClient:
            async def disconnect(self):
                pass

        session._client = FakeClient()

        # /resume sends the numbered menu and parks waiting for a pick.
        await session.handle_inbound("/resume", "imessage", {"conversation_id": "c1"})
        await asyncio.sleep(0.05)
        assert "Recent conversations" in sent[-1][1]
        assert session.pending is not None

        # Picking #2 swaps in the older session and persists it.
        await session.handle_inbound("2", "imessage", {"conversation_id": "c1"})
        await asyncio.sleep(0.05)
        assert session.resume_session_id == "older"
        assert persisted == [("contact-1", "older")]
        assert session._client is None
        assert sent[-1][1] == "Resumed: the older conversation"

    asyncio.run(scenario())


def test_double_text_interrupts_running_turn():
    async def scenario():
        session = make_session([])

        class FakeClient:
            def __init__(self):
                self.interrupts = 0

            async def interrupt(self):
                self.interrupts += 1

        fake = FakeClient()
        session._client = fake
        session._turn_active = True
        # Pretend a turn worker is already draining so handle_inbound doesn't
        # spawn a real one (which would touch the fake client).
        session._worker = asyncio.create_task(asyncio.sleep(10))

        await session.handle_inbound("do this instead", "imessage", {"conversation_id": "c1"})

        assert fake.interrupts == 1
        assert session._interrupting is True
        # The new (channel-tagged) message is queued for the worker to pick up.
        assert session._queue.get_nowait().endswith("do this instead")

        session._worker.cancel()

    asyncio.run(scenario())
