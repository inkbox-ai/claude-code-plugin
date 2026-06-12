import asyncio

from inkbox_claude.config import BridgeConfig
from inkbox_claude.sessions import ContactSession


def make_session(sent):
    async def send_fn(chat_id, text, mode, meta):
        sent.append((chat_id, text, mode, dict(meta)))

    cfg = BridgeConfig(permission_timeout_s=2.0, project_dir="/tmp")
    return ContactSession(
        chat_id="contact-1",
        cfg=cfg,
        send_fn=send_fn,
        mcp_server=None,
        mcp_tool_names=[],
        identity_info={"handle": "t", "email": "", "phone": ""},
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
