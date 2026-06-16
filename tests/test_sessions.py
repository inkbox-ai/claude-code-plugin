import asyncio

from inkbox_claude.config import BridgeConfig
from inkbox_claude.sessions import ContactSession


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
