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
import re
from dataclasses import dataclass
from datetime import datetime
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
# gateway.health_report() signature.
HealthFn = Callable[[], Awaitable[str]]

TYPING_REFRESH_SECONDS = 40.0

# Cap on a single stream-json message from the `claude` subprocess. The SDK
# defaults to 1 MB and kills its message reader if one line exceeds it — a big
# file read, wide grep, or base64 media payload can blow past that and wedge the
# session. Give it generous headroom; the buffer is only allocated if actually hit.
MAX_BUFFER_SIZE = 64 * 1024 * 1024  # 64 MB


@dataclass
class _Turn:
    """One unit of work for a session's single Claude client.

    Everything that drives a turn — inbound messages and capture turns alike —
    goes through one queue and one worker, so two turns can never hit the
    subprocess at once. A normal turn (``future is None``) sends its reply on
    the channel the human last used. A capture turn (``future`` set) hands the
    reply text back to the awaiting caller instead and never auto-replies —
    used by voice consults, post-call actions, and delivery-failure notices.
    """

    text: str
    future: Optional["asyncio.Future[str]"] = None
    # True for a one-shot turn spawned to recover from a rejected reply send.
    # A recovery turn that itself fails to send is not recovered again (no loop).
    recovery: bool = False

# Leading slash-commands the human can text to steer the conversation itself.
# The bridge acts on these locally — they never reach Claude as a turn.
RESET_COMMANDS = frozenset({"/clear", "/new"})  # start a fresh conversation
STOP_COMMANDS = frozenset({"/stop", "/cancel"})  # abort whatever's in flight
RESUME_COMMANDS = frozenset({"/resume"})        # pick a past session to reopen
STATUS_COMMANDS = frozenset({"/status"})        # report what the bridge is doing
USAGE_COMMANDS = frozenset({"/usage"})          # report Claude usage this convo
HEALTH_COMMANDS = frozenset({"/health"})        # probe Inkbox + Claude reachability

# How many recent sessions to offer when the human texts /resume.
RESUME_LIST_LIMIT = 5


def _control_command(text: str) -> Optional[str]:
    """Classify a message as a bridge control command, if it is one.

    Args:
        text (str): The raw inbound message text.

    Returns:
        Optional[str]: "reset", "stop", "resume", "status", "usage", or "health"
            when the whole message is exactly that command, else None (forwarded).
    """
    token = text.strip().lower()
    if token in RESET_COMMANDS:
        return "reset"
    if token in STOP_COMMANDS:
        return "stop"
    if token in RESUME_COMMANDS:
        return "resume"
    if token in STATUS_COMMANDS:
        return "status"
    if token in USAGE_COMMANDS:
        return "usage"
    if token in HEALTH_COMMANDS:
        return "health"
    return None


def _send_error_reason(exc: Exception) -> str:
    """Pull a human reason out of a send exception.

    Inkbox API errors carry a ``detail`` dict whose ``message`` is already a
    clear, actionable sentence (e.g. the spam-filter rejection). Fall back to
    the string form for anything else.

    Args:
        exc (Exception): The exception raised by the send.

    Returns:
        str: A human-readable failure reason.
    """
    detail = getattr(exc, "detail", None)
    if isinstance(detail, dict):
        message = detail.get("message") or detail.get("error")
        if message:
            return str(message)
    return str(exc)


def _send_rejected_prompt(reply: str, reason: str) -> str:
    """Build the recovery prompt for a reply the provider rejected at send time.

    Args:
        reply (str): The text that was blocked.
        reason (str): Why the send was rejected.

    Returns:
        str: A prompt telling Claude to rephrase or switch channels.
    """
    return "\n".join([
        "[reply rejected] Your last reply was NOT delivered — the messaging "
        "provider rejected it before sending.",
        f"Reason: {reason}",
        "",
        f'Your blocked reply was:\n"{reply}"',
        "",
        "Recover now: rephrase to avoid whatever was flagged (e.g. drop the "
        "restricted content), or send it over a different channel with your "
        "Inkbox tools — iMessage isn't subject to carrier SMS content filtering. "
        "Send the recovered version now.",
    ])


def _transcript_dir(project_dir: Optional[str]) -> Optional[Path]:
    """Locate Claude Code's transcript folder for a project.

    Claude Code stores one JSONL transcript per session under
    ``<config>/projects/<slugified project path>``.

    Args:
        project_dir (Optional[str]): Project working directory.

    Returns:
        Optional[Path]: The transcript directory, or None without a project.
    """
    if not project_dir:
        return None
    base = Path(os.getenv("CLAUDE_CONFIG_DIR") or (Path.home() / ".claude"))
    # The slug is the absolute path with every separator turned into a dash.
    slug = str(Path(project_dir).resolve()).replace("/", "-")
    return base / "projects" / slug


def _clean_summary(text: str) -> str:
    # Drop the leading channel tag the bridge prepends ("[iMessage from ...]").
    text = text.strip()
    if text.startswith("["):
        end = text.find("]")
        if end != -1:
            text = text[end + 1:].strip()
    # Collapse whitespace and keep it short enough for a text message.
    return " ".join(text.split())[:80]


def _session_digest(path: Path) -> Optional[Dict[str, Any]]:
    """Summarize one transcript file into {id, summary, mtime}.

    Args:
        path (Path): Path to a ``<session id>.jsonl`` transcript.

    Returns:
        Optional[Dict[str, Any]]: Digest, or None if the file can't be read.
    """
    summary = ""
    try:
        with path.open() as fh:
            for raw in fh:
                try:
                    entry = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                # Prefer an explicit compaction summary when one exists.
                if entry.get("type") == "summary" and entry.get("summary"):
                    summary = str(entry["summary"])
                    break
                # Otherwise fall back to the first real user message.
                if entry.get("type") == "user":
                    content = (entry.get("message") or {}).get("content")
                    if isinstance(content, list):
                        content = " ".join(
                            b.get("text", "") for b in content
                            if isinstance(b, dict) and b.get("type") == "text"
                        )
                    text = _clean_summary(str(content or ""))
                    # Skip injected system reminders and other tool noise.
                    if text and not text.startswith("<"):
                        summary = text
                        break
    except OSError:
        return None
    return {"id": path.stem, "summary": summary or "(no summary)", "mtime": path.stat().st_mtime}


def list_recent_sessions(
    project_dir: Optional[str],
    limit: int = RESUME_LIST_LIMIT,
    exclude_id: Optional[str] = None,
) -> list[Dict[str, Any]]:
    """List a project's most recent Claude Code sessions, newest first.

    Args:
        project_dir (Optional[str]): Project working directory.
        limit (int): Max sessions to return.
        exclude_id (Optional[str]): Session id to omit (e.g. the live one).

    Returns:
        list[Dict[str, Any]]: Digests {id, summary, mtime}, newest first.
    """
    tdir = _transcript_dir(project_dir)
    if tdir is None or not tdir.is_dir():
        return []
    files = sorted(tdir.glob("*.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True)
    out: list[Dict[str, Any]] = []
    for path in files:
        if path.stem == exclude_id:
            continue
        digest = _session_digest(path)
        if digest is not None:
            out.append(digest)
        if len(out) >= limit:
            break
    return out


def _format_resume_list(sessions: list[Dict[str, Any]]) -> str:
    # A numbered, one-line-each menu sized for a text message.
    lines = ["Recent conversations — reply with a number to resume:"]
    for i, s in enumerate(sessions, 1):
        when = datetime.fromtimestamp(s["mtime"]).strftime("%b %d %H:%M")
        lines.append(f"{i}. ({when}) {s['summary']}")
    return "\n".join(lines)


def _parse_index(reply: str, count: int) -> Optional[int]:
    # Pull the first integer out of the reply ("2", "#2", "option 2").
    match = re.search(r"\d+", reply or "")
    if not match:
        return None
    choice = int(match.group())
    return choice - 1 if 1 <= choice <= count else None


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
        on_clear: Optional[Callable[[str], None]] = None,
        typing_fn: Optional[TypingFn] = None,
        health_fn: Optional[HealthFn] = None,
    ):
        self.chat_id = chat_id
        self.cfg = cfg
        self.send_fn = send_fn
        self.typing_fn = typing_fn
        self.health_fn = health_fn
        self.mcp_server = mcp_server
        self.mcp_tool_names = mcp_tool_names
        self.identity_info = identity_info
        self.resume_session_id = resume_session_id
        self.on_session_id = on_session_id
        self.on_clear = on_clear

        self.mode = "email"  # last inbound modality; selects the reply channel
        self.reply_meta: Dict[str, Any] = {}
        self.pending: Optional[PendingInteraction] = None
        self.always_allowed: set[str] = set()

        self._client: Optional[ClaudeSDKClient] = None
        self._queue: asyncio.Queue[_Turn] = asyncio.Queue()
        self._worker: Optional[asyncio.Task] = None
        self._resume_task: Optional[asyncio.Task] = None  # /resume pick in flight
        self._turn_active = False     # a Claude turn is mid-flight
        self._interrupting = False    # a new message asked us to abort it
        self._current_turn: Optional[_Turn] = None  # the turn the worker is running

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

        # Bridge control commands (/clear, /new, /stop) steer the conversation
        # itself — handle them here instead of forwarding them to Claude.
        command = _control_command(text)
        if command == "reset":
            await self._reset_session()
            return
        if command == "stop":
            await self._stop_turn()
            return
        if command == "resume":
            await self._begin_resume()
            return
        # /status and /usage just report back — they don't disturb a running turn.
        if command == "status":
            await self._report_status()
            return
        if command == "usage":
            await self._report_usage()
            return
        if command == "health":
            await self._report_health()
            return

        # A reply while an escalation is outstanding answers the escalation —
        # it does not start a new agent turn.
        if self.pending is not None and not self.pending.future.done():
            logger.info("[session %s] reply consumed by pending %s", self.chat_id, self.pending.kind)
            self.pending.future.set_result(text)
            return

        # Tag the message with its channel + sender so Claude knows where it
        # is and who it's talking to (the static system prompt can't).
        await self._queue.put(_Turn(text=frame_inbound(mode, meta, text)))

        # Texting again while Claude is mid-turn behaves like hitting Esc and
        # typing a new message: interrupt the running turn so the worker drops
        # to this fresh message instead of making the human wait it out. Only
        # interrupt a normal turn — a capture turn (voice consult, post-call,
        # delivery-failure recovery) runs to completion and this message just
        # queues behind it.
        running_normal = self._current_turn is not None and self._current_turn.future is None
        if self._turn_active and self._client is not None and running_normal:
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
            turn = await self._queue.get()
            try:
                await self._run_turn(turn)
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
    # Control commands (/clear, /new, /stop)
    # ------------------------------------------------------------------

    async def _reset_session(self) -> None:
        """Start a fresh conversation: drop the resumed Claude session id and
        tear down the client so the next turn opens a brand-new session.

        Returns:
            None
        """
        await self._abort_in_flight()
        # Forget the resumed conversation everywhere — in memory, the live
        # client, the persisted map, and any session-scoped tool grants.
        self.resume_session_id = None
        await self.close()
        if self.on_clear is not None:
            self.on_clear(self.chat_id)
        self.always_allowed.clear()
        await self._reply("Started a fresh conversation — previous context cleared.")

    async def _stop_turn(self) -> None:
        """Interrupt the running turn (if any) and drop anything queued,
        keeping the conversation context intact.

        Returns:
            None
        """
        had_work = (
            self._turn_active or self.pending is not None or not self._queue.empty()
        )
        await self._abort_in_flight()
        await self._reply("Stopped." if had_work else "Nothing to stop — I'm idle.")

    async def _abort_in_flight(self) -> None:
        """Cancel whatever the session is currently doing: a parked
        escalation, a running turn, and any queued-but-unstarted messages.

        Returns:
            None
        """
        # Unblock a parked permission/poll so its turn can unwind (None reads
        # as "no answer" — the same as a timeout).
        if self.pending is not None and not self.pending.future.done():
            self.pending.future.set_result(None)
            self.pending = None
        # Interrupt a turn that's actively running, like pressing Esc.
        if self._turn_active and self._client is not None:
            self._interrupting = True
            try:
                await self._client.interrupt()
            except Exception:
                logger.debug("[session %s] interrupt failed", self.chat_id, exc_info=True)
        # Discard messages queued but not yet started. Settle any capture-turn
        # futures (consult / post-call / failure recovery) so their awaiters
        # don't hang waiting on work we just dropped.
        while not self._queue.empty():
            try:
                turn = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if turn.future is not None and not turn.future.done():
                turn.future.set_result("")

    async def _begin_resume(self) -> None:
        """List recent sessions and let the human pick one to reopen.

        Returns:
            None
        """
        sessions = list_recent_sessions(
            self.cfg.project_dir, exclude_id=self.resume_session_id
        )
        if not sessions:
            await self._reply("No other recent conversations to resume.")
            return
        # Run the numbered pick in the background so the inbound webhook can
        # return promptly while we wait (up to the escalation timeout) for the
        # human's choice. Keep a reference so the task isn't GC'd.
        self._resume_task = asyncio.create_task(self._run_resume_pick(sessions))

    async def _run_resume_pick(self, sessions: list[Dict[str, Any]]) -> None:
        try:
            reply = await self._escalate("resume", _format_resume_list(sessions))
            if reply is None:
                await self._reply("No pick — staying in the current conversation.")
                return
            index = _parse_index(reply, len(sessions))
            if index is None:
                await self._reply(
                    f"Didn't catch a number from 1-{len(sessions)} — staying put. "
                    "Send /resume to try again."
                )
                return
            chosen = sessions[index]
            # Swap in the chosen session and tear down the client so the next
            # turn continues it; persist it so it survives bridge restarts.
            await self.close()
            self.resume_session_id = chosen["id"]
            if self.on_session_id is not None:
                self.on_session_id(self.chat_id, chosen["id"])
            self.always_allowed.clear()
            await self._reply(f"Resumed: {chosen['summary']}")
        except Exception:
            logger.exception("[session %s] resume pick failed", self.chat_id)

    # ------------------------------------------------------------------
    # Status / usage reports (/status, /usage)
    # ------------------------------------------------------------------

    async def _report_status(self) -> None:
        """Text back what the bridge is doing for this contact right now.

        Returns:
            None
        """
        if self._turn_active:
            state = "I'm working on your last message right now."
        elif self.pending is not None and not self.pending.future.done():
            state = f"I'm waiting on your reply to a {self.pending.kind}."
        elif not self._queue.empty():
            state = "I'm about to start on your message."
        else:
            state = "I'm idle and ready for your next message."
        convo = "an ongoing conversation" if self.resume_session_id else "a fresh conversation"
        await self._reply(f"{state} We're in {convo}.")

    async def _report_usage(self) -> None:
        """Text back Claude subscription usage, mirroring Claude Code's /usage.

        Returns:
            None
        """
        try:
            from .claude_usage import usage_report
        except ImportError:  # pragma: no cover - direct local import/test fallback
            from claude_usage import usage_report
        # The fetch is a blocking HTTP call — keep it off the event loop.
        await self._reply(await asyncio.to_thread(usage_report))

    async def _report_health(self) -> None:
        """Text back Inkbox + Claude reachability (the gateway probes it).

        Returns:
            None
        """
        if self.health_fn is None:
            await self._reply("Health check unavailable.")
            return
        await self._reply(await self.health_fn())

    # ------------------------------------------------------------------
    # Claude Code turn
    # ------------------------------------------------------------------

    async def _run_turn(self, turn: _Turn) -> None:
        self._interrupting = False  # fresh turn starts un-interrupted
        self._current_turn = turn
        typing_task: Optional[asyncio.Task] = None
        try:
            client = await self._ensure_client()
            # Keep a typing indicator alive on the human's channel for the whole
            # turn, then always tear it down — even if the turn raises.
            self._turn_active = True
            typing_task = asyncio.create_task(self._typing_loop())
            await client.query(turn.text)

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
            reply = (final or "\n\n".join(chunks)).strip()
        except Exception as exc:
            # A failed turn can leave the SDK client wedged — its message reader
            # dies on errors like an oversized stream-json line, so every later
            # turn would fail against the same dead client. Drop it (keeping the
            # resumed session id) so the next turn rebuilds a fresh subprocess
            # with context intact. An intentional interrupt leaves a healthy
            # client, so skip the reset there.
            if not self._interrupting:
                await self._reset_client()
            # A capture turn must always settle its waiter — surface the error
            # there. A normal turn re-raises so _drain shows the human a notice.
            if turn.future is not None and not turn.future.done():
                turn.future.set_exception(exc)
                return
            raise
        finally:
            self._turn_active = False
            self._current_turn = None
            if typing_task is not None:
                typing_task.cancel()
                try:
                    await typing_task
                except asyncio.CancelledError:
                    pass

        # Route the result. Capture turns hand the text back to their waiter and
        # never auto-reply (the caller speaks/queues/swallows it). Normal turns
        # reply on the channel the human last used — unless a new message
        # interrupted this one, in which case the partial answer is dropped.
        if turn.future is not None:
            if not turn.future.done():
                turn.future.set_result(reply or "I finished that, but didn't have anything to say back.")
            return
        if self._interrupting:
            return
        if reply:
            await self._deliver_reply(turn, reply)

    async def _deliver_reply(self, turn: _Turn, reply: str) -> None:
        """Send a normal turn's reply, recovering once if the send is rejected.

        A synchronous send rejection (carrier spam filter, opt-out, invalid
        recipient) comes back as an API error, not a webhook. Rather than
        surfacing a generic failure, hand the reason back to Claude once so it
        can rephrase or switch channels. A recovery turn that itself fails is
        re-raised (the worker logs it) — never retried, so it can't loop.

        Args:
            turn (_Turn): The turn whose reply is being sent.
            reply (str): Claude's reply text.

        Returns:
            None
        """
        try:
            await self._reply(reply)
        except Exception as exc:
            reason = _send_error_reason(exc)
            logger.warning("[session %s] reply send rejected: %s", self.chat_id, reason)
            if turn.recovery:
                raise  # already a recovery attempt — don't spawn another
            await self._queue.put(
                _Turn(text=_send_rejected_prompt(reply, reason), recovery=True)
            )

    async def run_consult(self, query: str) -> str:
        """Run one Claude Code turn and RETURN its text (don't send it).

        Used by the Realtime voice bridge, post-call actions, and delivery-
        failure recovery: the caller wants Claude to act, then to receive the
        reply text rather than have it auto-sent. Runs on the same resumed
        session as this contact's texts, so it shares context across channels.

        Goes through the session's single queue/worker like a normal turn, so it
        can never run concurrently with one — it just carries a future the worker
        resolves instead of replying on a channel.

        Args:
            query (str): Plain-English request for Claude.

        Returns:
            str: Claude's reply text, or a short fallback if it produced none.
        """
        loop = asyncio.get_running_loop()
        future: asyncio.Future[str] = loop.create_future()
        await self._queue.put(_Turn(text=query, future=future))
        if self._worker is None or self._worker.done():
            self._worker = asyncio.create_task(self._drain())
        return await future

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
            # Raise the SDK's stream-json line cap so an oversized tool result
            # doesn't kill the reader and wedge the session (see MAX_BUFFER_SIZE).
            max_buffer_size=MAX_BUFFER_SIZE,
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

    async def _reset_client(self) -> None:
        """Tear down the live Claude client so the next turn rebuilds it.

        Called after a turn fails: the SDK's message reader dies on errors like
        an oversized stream-json line, leaving the client wedged so every later
        turn fails against it. The resumed session id is kept, so the rebuilt
        client picks the conversation back up with context intact.

        Returns:
            None
        """
        if self._client is None:
            return
        logger.info("[session %s] resetting Claude client after a failed turn", self.chat_id)
        await self.close()

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
        health_fn: Optional[HealthFn] = None,
    ):
        self.cfg = cfg
        self.send_fn = send_fn
        self.typing_fn = typing_fn
        self.health_fn = health_fn
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

    def _persist(self) -> None:
        try:
            path = _state_path()
            tmp = path.with_suffix(".tmp")
            tmp.write_text(json.dumps(self._session_ids, indent=2) + "\n")
            os.replace(tmp, path)
        except Exception:
            logger.exception("failed to persist session state")

    def _save_session_id(self, chat_id: str, session_id: str) -> None:
        self._session_ids[chat_id] = session_id
        self._persist()

    def _clear_state(self, chat_id: str) -> None:
        """Forget a contact's persisted Claude session id (for /clear, /new)."""
        if self._session_ids.pop(chat_id, None) is not None:
            self._persist()

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
                on_clear=self._clear_state,
                typing_fn=self.typing_fn,
                health_fn=self.health_fn,
            )
            self.sessions[chat_id] = session
        return session

    async def close_all(self) -> None:
        for session in self.sessions.values():
            await session.close()
