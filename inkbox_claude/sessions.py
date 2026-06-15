"""Contact-keyed Claude Code sessions.

One :class:`ContactSession` per remote party, spanning every channel
(email + SMS + iMessage + voice) — the same person texting and then
emailing lands in the same Claude Code conversation. Each session owns
one ``ClaudeSDKClient`` (a dedicated Claude Code subprocess) and a
serial turn queue; Claude session ids are persisted so conversations
survive bridge restarts.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Optional

try:
    from claude_agent_sdk import (
        AssistantMessage,
        ClaudeAgentOptions,
        ClaudeSDKClient,
        PermissionResultAllow,
        PermissionResultDeny,
        ResultMessage,
        TextBlock,
    )

    CLAUDE_SDK_AVAILABLE = True
except ImportError:  # pragma: no cover - doctor reports this cleanly
    AssistantMessage = ClaudeAgentOptions = ClaudeSDKClient = None  # type: ignore
    PermissionResultAllow = PermissionResultDeny = ResultMessage = TextBlock = None  # type: ignore
    CLAUDE_SDK_AVAILABLE = False

try:
    from .config import BridgeConfig
    from .escalation import (
        PendingInteraction,
        format_permission_request,
        format_poll,
        parse_permission_reply,
        parse_poll_reply,
    )
    from .prompts import build_channel_prompt, frame_inbound
except ImportError:  # pragma: no cover - direct local import/test fallback
    from config import BridgeConfig
    from escalation import (
        PendingInteraction,
        format_permission_request,
        format_poll,
        parse_permission_reply,
        parse_poll_reply,
    )
    from prompts import build_channel_prompt, frame_inbound

logger = logging.getLogger(__name__)

# gateway.send_to_contact(chat_id, text, mode, meta) signature.
SendFn = Callable[[str, str, str, Dict[str, Any]], Awaitable[Any]]
# gateway.send_typing(chat_id, mode, meta) signature.
TypingFn = Callable[[str, str, Dict[str, Any]], Awaitable[Any]]

# iMessage typing bubbles expire after a few seconds, so we refresh the
# indicator on this cadence for as long as a turn is running.
TYPING_REFRESH_SECONDS = 4.0


def _state_path() -> Path:
    root = Path(os.getenv("INKBOX_CLAUDE_HOME") or Path.home() / ".inkbox-claude")
    root.mkdir(parents=True, exist_ok=True)
    return root / "sessions.json"


class ContactSession:
    """One Claude Code conversation bound to one remote human."""

    def __init__(
        self,
        chat_id: str,
        cfg: BridgeConfig,
        send_fn: SendFn,
        mcp_server: Any,
        mcp_tool_names: list[str],
        identity_info: Dict[str, str],
        resume_session_id: Optional[str] = None,
        on_session_id: Optional[Callable[[str, str], None]] = None,
        typing_fn: Optional[TypingFn] = None,
    ):
        self.chat_id = chat_id
        self.cfg = cfg
        self.send_fn = send_fn
        self.typing_fn = typing_fn
        self.mcp_server = mcp_server
        self.mcp_tool_names = mcp_tool_names
        self.identity_info = identity_info
        self.resume_session_id = resume_session_id
        self.on_session_id = on_session_id

        self.mode = "email"  # last inbound modality; selects the reply channel
        self.reply_meta: Dict[str, Any] = {}
        self.pending: Optional[PendingInteraction] = None
        self.always_allowed: set[str] = set()

        self._client: Optional[ClaudeSDKClient] = None
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._worker: Optional[asyncio.Task] = None
        self._turn_active = False     # a Claude turn is mid-flight
        self._interrupting = False    # a new message asked us to abort it

    # ------------------------------------------------------------------
    # Inbound routing
    # ------------------------------------------------------------------

    async def handle_inbound(self, text: str, mode: str, meta: Dict[str, Any]) -> None:
        """Route one inbound message: answer a pending escalation, or queue a turn.

        Args:
            text (str): The human's message text.
            mode (str): Channel it arrived on (email/sms/imessage/voice).
            meta (dict): Reply-routing metadata (conversation ids, subject, ...).

        Returns:
            None
        """
        self.mode = mode
        self.reply_meta = dict(meta or {})

        # A reply while an escalation is outstanding answers the escalation —
        # it does not start a new agent turn.
        if self.pending is not None and not self.pending.future.done():
            logger.info("[session %s] reply consumed by pending %s", self.chat_id, self.pending.kind)
            self.pending.future.set_result(text)
            return

        # Tag the message with its channel + sender so Claude knows where it
        # is and who it's talking to (the static system prompt can't).
        await self._queue.put(frame_inbound(mode, meta, text))

        # Texting again while Claude is mid-turn behaves like hitting Esc and
        # typing a new message: interrupt the running turn so the worker drops
        # to this fresh message instead of making the human wait it out.
        if self._turn_active and self._client is not None:
            logger.info("[session %s] new message interrupts the running turn", self.chat_id)
            self._interrupting = True
            try:
                await self._client.interrupt()
            except Exception:
                logger.debug("[session %s] interrupt failed", self.chat_id, exc_info=True)

        if self._worker is None or self._worker.done():
            self._worker = asyncio.create_task(self._drain())

    async def _drain(self) -> None:
        while not self._queue.empty():
            text = await self._queue.get()
            try:
                await self._run_turn(text)
            except Exception:
                # An interrupt aborts the turn on purpose — the next queued
                # message takes over, so it is not an error to report.
                if self._interrupting:
                    logger.info("[session %s] turn interrupted by a new message", self.chat_id)
                    continue
                logger.exception("[session %s] turn failed", self.chat_id)
                try:
                    await self._reply(
                        "Sorry — I hit an error while working on that and had to stop. "
                        "Try sending it again."
                    )
                except Exception:
                    logger.exception("[session %s] could not send the error notice", self.chat_id)

    # ------------------------------------------------------------------
    # Claude Code turn
    # ------------------------------------------------------------------

    async def _run_turn(self, text: str) -> None:
        self._interrupting = False  # fresh turn starts un-interrupted
        client = await self._ensure_client()

        # Keep a typing indicator alive on the human's channel for the whole
        # turn, then always tear it down — even if the turn raises.
        self._turn_active = True
        typing_task = asyncio.create_task(self._typing_loop())
        try:
            await client.query(text)

            chunks: list[str] = []
            final: Optional[str] = None
            async for message in client.receive_response():
                if isinstance(message, AssistantMessage):
                    for block in message.content:
                        if isinstance(block, TextBlock):
                            chunks.append(block.text)
                elif isinstance(message, ResultMessage):
                    final = message.result
                    if message.session_id and self.on_session_id:
                        self.resume_session_id = message.session_id
                        self.on_session_id(self.chat_id, message.session_id)
        finally:
            self._turn_active = False
            typing_task.cancel()
            try:
                await typing_task
            except asyncio.CancelledError:
                pass

        # If a new message interrupted this turn, drop the partial answer —
        # the next queued message is what the human actually wants now.
        if self._interrupting:
            return
        reply = (final or "\n\n".join(chunks)).strip()
        if reply:
            await self._reply(reply)

    async def _typing_loop(self) -> None:
        """Refresh the channel's typing indicator until the turn ends.

        Returns:
            None: Runs until cancelled by :meth:`_run_turn`.
        """
        if self.typing_fn is None:
            return
        try:
            while True:
                # Only iMessage has a typing bubble; stay quiet while an
                # escalation is parked waiting on the human to reply.
                if self.mode == "imessage" and self.pending is None:
                    try:
                        await self.typing_fn(self.chat_id, self.mode, self.reply_meta)
                    except Exception:
                        logger.debug("[session %s] typing ping failed", self.chat_id, exc_info=True)
                await asyncio.sleep(TYPING_REFRESH_SECONDS)
        except asyncio.CancelledError:
            return

    async def _ensure_client(self) -> ClaudeSDKClient:
        if self._client is not None:
            return self._client
        if not CLAUDE_SDK_AVAILABLE:
            raise RuntimeError(
                "claude-agent-sdk is not installed; run: pip install claude-agent-sdk"
            )

        options = ClaudeAgentOptions(
            cwd=self.cfg.project_dir or None,
            model=self.cfg.claude_model or None,
            system_prompt={
                "type": "preset",
                "preset": "claude_code",
                "append": build_channel_prompt(
                    project_dir=self.cfg.project_dir,
                    identity_handle=self.identity_info.get("handle", ""),
                    email_address=self.identity_info.get("email", ""),
                    phone_number=self.identity_info.get("phone", ""),
                ),
            },
            setting_sources=["user", "project"],
            # Read-only tools and our own Inkbox tools run without a text;
            # everything else lands in _can_use_tool and escalates.
            allowed_tools=list(self.cfg.auto_allowed_tools) + list(self.mcp_tool_names),
            mcp_servers={"inkbox": self.mcp_server},
            can_use_tool=self._can_use_tool,
            resume=self.resume_session_id or None,
        )
        self._client = ClaudeSDKClient(options=options)
        await self._client.connect()
        logger.info(
            "[session %s] Claude Code session started (resume=%s)",
            self.chat_id, self.resume_session_id or "fresh",
        )
        return self._client

    # ------------------------------------------------------------------
    # Escalation (permission prompts + AskUserQuestion polls)
    # ------------------------------------------------------------------

    async def _can_use_tool(self, tool_name: str, input_data: Dict[str, Any], context: Any):
        # AskUserQuestion → numbered poll on the human's channel.
        if tool_name == "AskUserQuestion":
            questions = list(input_data.get("questions") or [])
            reply = await self._escalate("poll", format_poll(questions), questions=questions)
            if reply is None:
                return PermissionResultDeny(
                    message="The human did not answer the poll in time; proceed with your best judgment."
                )
            answers = parse_poll_reply(reply, questions)
            return PermissionResultAllow(updated_input={**input_data, "answers": answers})

        # Session-scoped "always" grants plus the configured read-only set.
        if tool_name in self.always_allowed:
            return PermissionResultAllow()

        reply = await self._escalate(
            "permission",
            format_permission_request(tool_name, input_data),
            tool_name=tool_name,
        )
        if reply is None:
            return PermissionResultDeny(
                message=(
                    f"No reply from the human within "
                    f"{int(self.cfg.permission_timeout_s)}s — not approved."
                )
            )

        decision = parse_permission_reply(reply)
        if decision == "always":
            self.always_allowed.add(tool_name)
            return PermissionResultAllow()
        if decision == "allow":
            return PermissionResultAllow()
        return PermissionResultDeny(
            message=(
                f'The human replied "{reply.strip()}" — treating that as not approved. '
                "If their reply contains new instructions, follow those instead."
            )
        )

    async def _escalate(
        self,
        kind: str,
        prompt_text: str,
        questions: Optional[list] = None,
        tool_name: str = "",
    ) -> Optional[str]:
        """Send an escalation text and wait for the next inbound reply.

        Args:
            kind (str): "permission" or "poll".
            prompt_text (str): Pre-formatted message for the human.
            questions (Optional[list]): AskUserQuestion questions, for polls.
            tool_name (str): Tool being gated, for permission requests.

        Returns:
            Optional[str]: The human's reply text, or None on timeout.
        """
        loop = asyncio.get_running_loop()
        self.pending = PendingInteraction(
            kind=kind,
            prompt_text=prompt_text,
            future=loop.create_future(),
            questions=list(questions or []),
            tool_name=tool_name,
        )
        await self._reply(prompt_text)
        try:
            return await asyncio.wait_for(
                self.pending.future, timeout=self.cfg.permission_timeout_s
            )
        except asyncio.TimeoutError:
            return None
        finally:
            self.pending = None

    async def _reply(self, text: str) -> None:
        await self.send_fn(self.chat_id, text, self.mode, self.reply_meta)

    async def close(self) -> None:
        if self._client is not None:
            try:
                await self._client.disconnect()
            except Exception:
                pass
            self._client = None


class SessionManager:
    """Owns every ContactSession and the chat_id → claude session_id map."""

    def __init__(
        self,
        cfg: BridgeConfig,
        send_fn: SendFn,
        mcp_server: Any,
        mcp_tool_names: list[str],
        identity_info: Dict[str, str],
        typing_fn: Optional[TypingFn] = None,
    ):
        self.cfg = cfg
        self.send_fn = send_fn
        self.typing_fn = typing_fn
        self.mcp_server = mcp_server
        self.mcp_tool_names = mcp_tool_names
        self.identity_info = identity_info
        self.sessions: Dict[str, ContactSession] = {}
        self._session_ids: Dict[str, str] = self._load_state()

    def _load_state(self) -> Dict[str, str]:
        try:
            return json.loads(_state_path().read_text())
        except Exception:
            return {}

    def _save_session_id(self, chat_id: str, session_id: str) -> None:
        self._session_ids[chat_id] = session_id
        try:
            path = _state_path()
            tmp = path.with_suffix(".tmp")
            tmp.write_text(json.dumps(self._session_ids, indent=2) + "\n")
            os.replace(tmp, path)
        except Exception:
            logger.exception("failed to persist session state")

    def get(self, chat_id: str) -> ContactSession:
        """Fetch or lazily create the session for one remote party.

        Args:
            chat_id (str): Contact id, or raw address/number fallback.

        Returns:
            ContactSession: The (possibly new) session for that contact.
        """
        session = self.sessions.get(chat_id)
        if session is None:
            session = ContactSession(
                chat_id=chat_id,
                cfg=self.cfg,
                send_fn=self.send_fn,
                mcp_server=self.mcp_server,
                mcp_tool_names=self.mcp_tool_names,
                identity_info=self.identity_info,
                resume_session_id=self._session_ids.get(chat_id),
                on_session_id=self._save_session_id,
                typing_fn=self.typing_fn,
            )
            self.sessions[chat_id] = session
        return session

    async def close_all(self) -> None:
        for session in self.sessions.values():
            await session.close()
