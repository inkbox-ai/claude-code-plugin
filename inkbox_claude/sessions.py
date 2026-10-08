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
import hashlib
from contextvars import ContextVar
import json
import logging
import os
import re
import time
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Optional

try:
    from .delivery_policy import sms_tool_failure_kind
    from .hosted_sms_guard import (
        reserve_hosted_sms_attempt,
        settle_hosted_sms_attempt,
    )
except ImportError:  # pragma: no cover - direct local import/test fallback
    from delivery_policy import sms_tool_failure_kind
    from hosted_sms_guard import (
        reserve_hosted_sms_attempt,
        settle_hosted_sms_attempt,
    )

try:
    from claude_agent_sdk import (
        AssistantMessage,
        ClaudeAgentOptions,
        ClaudeSDKClient,
        HookMatcher,
        PermissionResultAllow,
        PermissionResultDeny,
        ResultMessage,
        TextBlock,
        ToolUseBlock,
    )

    CLAUDE_SDK_AVAILABLE = True
except ImportError:  # pragma: no cover - doctor reports this cleanly
    AssistantMessage = ClaudeAgentOptions = ClaudeSDKClient = HookMatcher = None  # type: ignore
    PermissionResultAllow = PermissionResultDeny = ResultMessage = TextBlock = ToolUseBlock = None  # type: ignore
    CLAUDE_SDK_AVAILABLE = False

try:
    from .a2a_progress import observe_a2a_tool_start
    from .config import BridgeConfig
    from .escalation import (
        PendingInteraction,
        format_permission_request,
        format_poll,
        parse_permission_reply,
        parse_poll_reply,
    )
    from .prompts import build_channel_prompt, frame_inbound, mentions_agent
except ImportError:  # pragma: no cover - direct local import/test fallback
    from a2a_progress import observe_a2a_tool_start
    from config import BridgeConfig
    from escalation import (
        PendingInteraction,
        format_permission_request,
        format_poll,
        parse_permission_reply,
        parse_poll_reply,
    )
    from prompts import build_channel_prompt, frame_inbound, mentions_agent

class ClaudeNotLoggedInError(RuntimeError):
    """The native host needs refreshed authentication, not a cleared conversation."""


logger = logging.getLogger(__name__)

# gateway.send_to_contact(chat_id, text, mode, meta) signature.
SendFn = Callable[[str, str, str, Dict[str, Any]], Awaitable[Any]]
# gateway.send_typing(chat_id, mode, meta) signature.
TypingFn = Callable[[str, str, Dict[str, Any]], Awaitable[Any]]
# gateway.health_report() signature.
HealthFn = Callable[[], Awaitable[str]]
# gateway._note_send_rejection(chat_id, mode, meta, content, exc) signature —
# hands a synchronously-rejected reply to the shared delivery-failure loop.
SendRejectedFn = Callable[[str, str, Dict[str, Any], str, Exception], Awaitable[None]]

TYPING_REFRESH_SECONDS = 40.0
TYPING_MAX_SECONDS = 600.0
CHANNEL_RETRY_INITIAL_DELAY = 1.0
CHANNEL_RETRY_MAX_DELAY = 30.0
CHANNEL_DELIVERY_GUARD: ContextVar[Any] = ContextVar("channel_delivery_guard", default=None)


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
    future: Optional["asyncio.Future[Any]"] = None
    a2a_context: Optional[Dict[str, Any]] = None
    capture_tools: bool = False
    hosted_sms_context: Optional[Dict[str, Any]] = None
    mode: Optional[str] = None
    reply_meta: Optional[Dict[str, Any]] = None
    completion: Optional["asyncio.Future[str]"] = None
    checkpoint: Optional[Callable[..., None]] = None
    context_ids: tuple[str, ...] = ()
    authorize: Optional[Callable[[], Awaitable[None]]] = None
    queued_at: float = 0
    submitted: bool = False
    execution_observed: bool = False
    attempts: int = 0
    burst_source_count: int = 1
    burst_text_chars: int = 0
    saved_reply: Optional[str] = None
    cancelled: bool = False
    retry_wake: asyncio.Event = field(default_factory=asyncio.Event)


@dataclass(frozen=True)
class ToolDeliveryResult:
    """Sanitized host-native outcome for one messaging tool attempt."""

    mode: str
    target: str
    sent: bool
    error_kind: str


@dataclass(frozen=True)
class CapturedTurnResult:
    """Capture-turn text plus host-native messaging-tool outcomes."""

    text: str
    tool_deliveries: tuple[ToolDeliveryResult, ...]
    aborted: bool = False

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


def _exception_text(exc: Exception) -> str:
    """Flatten an exception plus SDK stderr into text for classification."""
    parts = [str(exc)]
    stderr = getattr(exc, "stderr", None)
    if stderr:
        parts.append(str(stderr))
    return "\n".join(p for p in parts if p)


def _is_missing_resume_error(exc: Exception) -> bool:
    return "No conversation found with session ID" in _exception_text(exc)


def _turn_error_notice(exc: Exception) -> str:
    """Build the text the human sees when a Claude turn cannot start/finish."""
    if isinstance(exc, ClaudeNotLoggedInError):
        return "Claude login needs refreshing. Run /login in a terminal on the bridge machine; your next message will reconnect without clearing this conversation."
    if _is_missing_resume_error(exc):
        return (
            "Claude Code couldn't find the old conversation I tried to resume. "
            "I cleared that stale session; send your message again to start fresh, "
            "or text /health to check the bridge."
        )
    return (
        "Sorry — Claude Code hit an error while working on that. "
        "Text /health to check the bridge, or /clear to start fresh."
    )


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
        on_send_rejected: Optional[SendRejectedFn] = None,
        system_prompt_extra: str = "",
        activity_fn=None,
        receipt_fn=None,
        fence_fn=None,
    ):
        self.chat_id = chat_id
        self.activity_fn = activity_fn
        self.receipt_fn = receipt_fn
        self.fence_fn = fence_fn
        self._escalation_lock = asyncio.Lock()
        self._imessage_tool_outputs = []
        self._deferred_turn = None
        self._batching_turn = None
        self._retrying_turn = None
        self._delivery_turn = None
        self._shutting_down = False
        self._side_effects = set()
        self.cfg = cfg
        self.send_fn = send_fn
        self.typing_fn = typing_fn
        self.health_fn = health_fn
        self.on_send_rejected = on_send_rejected
        self.mcp_server = mcp_server
        self.mcp_tool_names = mcp_tool_names
        self.identity_info = identity_info
        self.resume_session_id = resume_session_id
        self.on_session_id = on_session_id
        self.on_clear = on_clear
        # Extra session-scoped system prompt (e.g. the external-event
        # directive) — appended after the channel prompt at client start.
        self.system_prompt_extra = system_prompt_extra

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
        # Set when an Inkbox tool successfully delivers to the same channel
        # and recipient as the inbound turn. The tool's message is the reply;
        # do not follow it with Claude's usually-redundant "sent it" result.
        self._current_channel_tool_delivery = False
        self._current_tool_deliveries: list[ToolDeliveryResult] = []
        self._current_hosted_sms_context: Optional[Dict[str, Any]] = None
        self.companion_approver = ""
        self._client_generation = 0
        self._connecting_client = None
        self._host_owner = None
        self._pending_fenced_owners: set[tuple] = set()
        self._close_lock = asyncio.Lock()
        self._control_route: ContextVar[Any] = ContextVar("control_reply_route", default=None)
        owner = [cfg.base_url, identity_info.get("id") or cfg.identity, chat_id]
        context_id = hashlib.sha256(json.dumps(owner).encode()).hexdigest()
        self._context_path = _state_path().parent / "group-context" / f"{context_id}.json"
        self._context: list[dict[str, Any]] = (
            json.loads(self._context_path.read_text()) if self._context_path.exists() else []
        )


    # ------------------------------------------------------------------
    # Inbound routing
    # ------------------------------------------------------------------

    async def handle_inbound(self, text: str, mode: str, meta: Dict[str, Any]) -> Optional[bool]:
        """Route one inbound message: answer a pending escalation, or queue a turn.

        Args:
            text (str): The human's message text.
            mode (str): Channel it arrived on (email/sms/imessage/voice).
            meta (dict): Reply-routing metadata (conversation ids, subject, ...).

        Returns:
            Whether a reset completed, for reset controls; otherwise None.
        """
        meta = deepcopy(meta or {})
        if mode == "imessage" and self.cfg.imessage_threaded_replies:
            meta["imessage_threaded_replies"] = True
        raw_text = str(meta.get("raw_text", text))
        bot = meta.get("slack_bot_user_id") if mode == "slack" else None
        if bot and raw_text.lstrip().startswith(f"<@{bot}>"):
            raw_text = raw_text.lstrip()[len(bot) + 3:].strip()
        command = None if meta.get("reaction") else _control_command(raw_text)
        is_group = mode in {"sms", "imessage", "slack"} and meta.get("conversation_kind") == "group"
        pending_reply = self.pending is not None and not self.pending.future.done()
        if pending_reply and mode == "slack":
            pending_reply = all(meta.get(k) == self.pending.route.get(k) for k in ("connection_id", "conversation_id", "thread_ts", "actor_id"))
        if pending_reply:
            expected = self.pending.sender
            sender = str(meta.get("sender") or "")
            from .companion import same_author
            pending_reply = (not meta.get("reaction") and
                             (not (is_group or meta.get("companion")) or (bool(expected) and same_author(mode, sender, expected))))
        if mode == "slack" and is_group and self.pending is not None and not self.pending.future.done() and not pending_reply:
            self.buffer_context(frame_inbound(mode, meta, text), str(meta.get("source_event_id") or meta.get("message_id") or ""))
            await self.receipt(mode, meta, "done")
            return
        if pending_reply and self.pending.kind == "permission" and not command and parse_permission_reply(raw_text) is None:
            await self._abort_in_flight(only_turns=[self.pending.owner or self._current_turn])
            pending_reply = False
        addressed = meta.get("slack_mentioned") if mode == "slack" else mentions_agent(raw_text, self.identity_info.get("handle") or self.cfg.identity)
        quiet = (is_group and self.cfg.group_reply_mode == "mention" and not command
                 and not pending_reply and not addressed)
        if quiet:
            self.buffer_context(frame_inbound(mode, meta, text), str(meta.get("message_id") or meta.get("source_event_id") or ""))
            await self.receipt(mode, meta, "done")
            return

        control_turn = self._current_turn or self._delivery_turn or self._retrying_turn
        if command and mode == "slack" and control_turn is not None:
            route = control_turn.reply_meta or {}
            if any(meta.get(k) != route.get(k) for k in ("connection_id", "conversation_id", "thread_ts", "actor_id")):
                self.buffer_context(frame_inbound(mode, meta, text), str(meta.get("source_event_id") or ""))
                await self.receipt(mode, meta, "done")
                return
        if command:
            await self.receipt(mode, meta, "done")
            token = self._control_route.set((mode, meta))
            try:
                handlers = {"reset": self._reset_session, "stop": self._stop_turn,
                            "resume": self._begin_resume, "status": self._report_status,
                            "usage": self._report_usage, "health": self._report_health}
                return await handlers[command]()
            finally:
                self._control_route.reset(token)
        if pending_reply:
            await self.receipt(mode, meta, "done")
            self.pending.future.set_result(raw_text)
            return
        if not self._current_turn:
            self.mode, self.reply_meta = mode, deepcopy(meta)
        await self.notify_activity(mode, meta, "accepted")
        await self._queue.put(_Turn(text=frame_inbound(mode, meta, text), mode=mode, reply_meta=meta, queued_at=time.monotonic(), burst_text_chars=len(text)))

        # Texting again while Claude is mid-turn behaves like hitting Esc and
        # typing a new message: interrupt the running turn so the worker drops
        # to this fresh message instead of making the human wait it out. Only
        # interrupt a normal turn — a capture turn (voice consult, post-call,
        # delivery-failure recovery) runs to completion and this message just
        # queues behind it.
        running_normal = self._current_turn is not None and self._current_turn.future is None
        if self._turn_active and self._client is not None and running_normal and not self._interrupting and not (mode == "imessage" and self.cfg.imessage_threaded_replies):
            logger.info("[session %s] new message interrupts the running turn", self.chat_id)
            self._interrupting = True
            try:
                await self._client.interrupt()
            except Exception:
                logger.debug("[session %s] interrupt failed", self.chat_id, exc_info=True)

        if self._worker is None or self._worker.done():
            self._worker = asyncio.create_task(self._drain())

    def _save_context(self) -> None:
        """Flush quiet input before acknowledging it to the webhook sender."""
        self._context_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        temporary = self._context_path.with_suffix(".tmp")
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as stream:
            json.dump(self._context, stream)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, self._context_path)
        directory = os.open(self._context_path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)

    def buffer_context(self, text: str, source_id: str = "") -> None:
        """Retain a quiet message without generation, tools, typing or interrupts."""
        source_id = source_id or hashlib.sha256(text.encode()).hexdigest()
        if any(item["id"] == source_id for item in self._context):
            self._save_context()
            return
        self._context.append({"id": source_id, "text": text})
        self._save_context()

    def buffer_delivery_notice(self, text: str, source_id: str) -> None:
        """Bound quiet delivery outcomes independently of ordinary conversation input."""
        if any(item["id"] == source_id for item in self._context):
            self._save_context()
            return
        notices = [item for item in self._context if item.get("delivery_notice")]
        expired = {item["id"] for item in notices[:-31]} if len(notices) >= 32 else set()
        self._context = [item for item in self._context if item["id"] not in expired]
        self._context.append({"id": source_id, "text": text[:1024], "delivery_notice": True})
        self._save_context()

    def _reply_route(self) -> tuple[str, Dict[str, Any]]:
        """Prefer the original turn route, with a task-local control override."""
        override = self._control_route.get()
        if override is not None:
            return override
        turn = self._current_turn
        if turn is not None and turn.reply_meta is not None:
            return turn.mode or self.mode, turn.reply_meta
        return self.mode, self.reply_meta

    async def notify_activity(self, mode, meta, state):
        if self.activity_fn is not None:
            await self.activity_fn(self.chat_id, mode, meta, state)

    async def receipt(self, mode, meta, state, text=None):
        if self.receipt_fn is not None:
            return await self.receipt_fn(self.chat_id, mode, meta, state, text)

    async def _batch(self, turn):
        meta = turn.reply_meta or {}
        if (turn.mode != "imessage" or not self.cfg.imessage_threaded_replies or turn.checkpoint or turn.saved_reply is not None
                or turn.future or meta.get("companion") or meta.get("media") or meta.get("reaction")):
            return turn
        deadline = turn.queued_at + 2.0
        quiet_until = turn.queued_at + .75
        while True:
            delay = min(deadline, quiet_until) - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            if self._batching_turn is None:
                return turn
            if self._queue.empty():
                return turn
            other = self._queue.get_nowait()
            ometa = other.reply_meta or {}
            from .imessage import compatible_key
            if (other.mode != "imessage" or other.future or other.checkpoint or other.saved_reply is not None or ometa.get("companion")
                    or ometa.get("media") or ometa.get("reaction") or other.queued_at > deadline
                    or turn.burst_source_count + other.burst_source_count > 8
                    or (turn.burst_text_chars or len(turn.text)) + (other.burst_text_chars or len(other.text)) + 2 > 4000
                    or compatible_key(meta) != compatible_key(ometa)):
                self._deferred_turn = other
                return turn
            turn.burst_source_count += other.burst_source_count
            turn.burst_text_chars = (turn.burst_text_chars or len(turn.text)) + (other.burst_text_chars or len(other.text)) + 2
            turn.text += "\n\n" + other.text
            for field in ("imessage_event_ids", "imessage_sources"):
                meta[field] = list(meta.get(field, [])) + list(ometa.get(field, []))
            quiet_until = other.queued_at + .75
            if time.monotonic() >= deadline:
                return turn

    async def _drain(self) -> None:
        while not self._shutting_down and (self._deferred_turn is not None or not self._queue.empty()):
            turn = self._deferred_turn or await self._queue.get()
            self._deferred_turn = None
            self._batching_turn = turn
            turn = await self._batch(turn)
            if self._batching_turn is None or turn.cancelled:
                continue
            self._batching_turn = None
            try:
                if turn.saved_reply is not None:
                    await self._deliver_reply(turn, turn.saved_reply)
                    result = turn.saved_reply
                else:
                    result = await self._run_turn(turn)
                if turn.checkpoint is None:
                    await self.notify_activity(turn.mode, turn.reply_meta or {}, "cancelled" if turn.cancelled or self._interrupting else "completed")
                if turn.completion is not None and not turn.completion.done():
                    turn.completion.set_result(result or "")
            except Exception as exc:
                if turn.cancelled or self._shutting_down:
                    continue
                from .companion import retryable_read
                from .imessage import SafeReplyPreflightError
                ordinary_channel = (turn.checkpoint is None and turn.future is None
                    and (turn.mode == "slack" or (turn.mode == "imessage" and self.cfg.imessage_threaded_replies)))
                safe_start = (ordinary_channel and turn.saved_reply is None and not turn.submitted and not turn.execution_observed
                              and retryable_read(exc))
                safe_reply = (ordinary_channel and turn.saved_reply is not None
                              and isinstance(exc, SafeReplyPreflightError))
                retry_start = (turn.saved_reply is None and not turn.submitted and turn.checkpoint is None
                               and turn.attempts < 3 and not isinstance(exc, (PermissionError, ValueError)))
                if safe_start or safe_reply or retry_start:
                    try:
                        await self._wait_channel_retry(turn, close_host=not safe_reply)
                    except Exception as retry_error:
                        exc = retry_error
                    else:
                        continue
                outcome = "uncertain" if turn.submitted or turn.execution_observed or turn.saved_reply is not None else "failed"
                await self.receipt(turn.mode, turn.reply_meta or {}, outcome)
                if turn.checkpoint is None:
                    await self.notify_activity(turn.mode, turn.reply_meta or {}, "failed")
                if turn.completion is not None:
                    try:
                        await self.close()
                    except Exception as fence_error:
                        exc = fence_error
                    finally:
                        if not turn.completion.done():
                            turn.completion.set_exception(exc)
                    if getattr(self, "_execution_blocked", False):
                        return
                    continue
                # An interrupt aborts the turn on purpose — the next queued
                # message takes over, so it is not an error to report.
                if self._interrupting:
                    logger.info("[session %s] turn interrupted by a new message", self.chat_id)
                    continue
                logger.exception("[session %s] turn failed", self.chat_id)
                try:
                    await self.close()
                except Exception as fence_error:
                    exc = fence_error
                if ordinary_channel and turn.saved_reply is not None:
                    continue  # Preserve the answer; do not start an untracked second send.
                try:
                    await self.send_fn(self.chat_id, _turn_error_notice(exc), turn.mode or self.mode, deepcopy(turn.reply_meta or self.reply_meta))
                except Exception:
                    logger.exception("[session %s] could not send the error notice", self.chat_id)
                if getattr(self, "_execution_blocked", False):
                    return

    async def _wait_channel_retry(self, turn: _Turn, *, close_host: bool) -> None:
        """Keep one cancellable queue owner while retrying proven pre-effect work."""
        self._retrying_turn = turn
        turn.attempts += 1
        try:
            if close_host:
                await self.close()
            generation = self._client_generation
            delay = min(CHANNEL_RETRY_INITIAL_DELAY * 2 ** min(turn.attempts - 1, 10), CHANNEL_RETRY_MAX_DELAY)
            try:
                await asyncio.wait_for(turn.retry_wake.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
            if (not turn.cancelled and not self._shutting_down and generation == self._client_generation
                    and not getattr(self, "_execution_blocked", False)):
                # Burst collection may already own the next incompatible input.
                # Put it ahead of later arrivals before reserving the retry slot.
                if self._deferred_turn is not None:
                    following = [self._deferred_turn]
                    while not self._queue.empty():
                        following.append(self._queue.get_nowait())
                    for pending in following:
                        self._queue.put_nowait(pending)
                self._deferred_turn = turn
        finally:
            if self._retrying_turn is turn:
                self._retrying_turn = None

    async def recover_reply(self, reply: str, mode: str, meta: Dict[str, Any]) -> None:
        """Queue a durable answer for delivery without submitting another model turn."""
        if self._shutting_down:
            return
        await self.notify_activity(mode, meta, "accepted")
        await self._queue.put(_Turn(text="", mode=mode, reply_meta=deepcopy(meta), saved_reply=reply))
        if self._worker is None or self._worker.done():
            self._worker = asyncio.create_task(self._drain())

    async def run_companion(
        self, text: str, mode: str, meta: Dict[str, Any], checkpoint: Callable[..., None],
        authorize: Callable[[], Awaitable[None]],
    ) -> None:
        """Queue one complete input without commands, approvals, or interruption."""
        completion = asyncio.get_running_loop().create_future()
        await self._queue.put(_Turn(
            text=text, mode=mode, reply_meta=deepcopy(meta),
            completion=completion, checkpoint=checkpoint, authorize=authorize,
        ))
        if self._worker is None or self._worker.done():
            self._worker = asyncio.create_task(self._drain())
        return await completion

    # ------------------------------------------------------------------
    # Control commands (/clear, /new, /stop)
    # ------------------------------------------------------------------

    async def _reset_session(self) -> bool:
        """Start a fresh conversation: drop the resumed Claude session id and
        tear down the client so the next turn opens a brand-new session.

        Returns:
            True only after the old host and context were cleared.
        """
        try:
            await self._abort_in_flight()
            await self.close()
        except Exception:
            await self._reply("Could not start a fresh conversation. This conversation remains paused.")
            return False
        # Forget the resumed conversation everywhere — in memory, the live
        # client, the persisted map, and any session-scoped tool grants.
        self.resume_session_id = None
        if self.on_clear is not None:
            self.on_clear(self.chat_id)
        self.always_allowed.clear()
        self._context.clear()
        self._save_context()
        await self._reply("Started a fresh conversation — previous context cleared.")
        return True

    async def _stop_turn(self) -> None:
        """Interrupt the running turn (if any) and drop anything queued,
        keeping the conversation context intact.

        Returns:
            None
        """
        had_work = (
            self._turn_active or self.pending is not None or not self._queue.empty()
            or self._batching_turn is not None or self._deferred_turn is not None
            or self._retrying_turn is not None or self._delivery_turn is not None
        )
        reply_mode, _ = self._reply_route()
        try:
            await self._abort_in_flight(only_mode="imessage" if reply_mode == "imessage" and self.cfg.imessage_threaded_replies else None)
        except Exception:
            if not getattr(self, "_execution_blocked", False) and not self._pending_fenced_owners:
                raise
            await self._reply("Stop could not be confirmed. This conversation remains paused.")
            return
        await self._reply("Stopped." if had_work else "Nothing to stop — I'm idle.")

    def slack_stop_targets(self, meta):
        """Snapshot exact actor/route-owned turns, never future requests on that route."""
        fields = ("connection_id", "workspace_id", "conversation_id", "thread_ts", "actor_id")
        candidates = [self._current_turn, self._batching_turn, self._deferred_turn,
                      self._retrying_turn, self._delivery_turn, *list(self._queue._queue)]
        return [turn for turn in candidates if turn is not None and turn.mode == "slack"
                and all((turn.reply_meta or {}).get(k) == meta.get(k) for k in fields)]

    async def _abort_in_flight(self, only_mode=None, only_turns=None) -> None:
        """Cancel a snapshot of owned work; requests arriving during abort survive."""
        def selected(turn):
            return (turn is not None and (only_mode is None or turn.mode == only_mode)
                    and (only_turns is None or any(turn is owned for owned in only_turns)))

        active = self._current_turn
        owns_active = selected(active) or (active is None and only_mode is None and only_turns is None)
        pending = self.pending
        dropped = []
        for turn in (self._retrying_turn, self._delivery_turn):
            if selected(turn) and not any(turn is item for item in dropped):
                turn.cancelled = True
                turn.retry_wake.set()
                dropped.append(turn)
        if selected(self._batching_turn):
            dropped.append(self._batching_turn)
            self._batching_turn = None
        if selected(self._deferred_turn):
            dropped.append(self._deferred_turn)
            self._deferred_turn = None
        retained = []
        while not self._queue.empty():
            turn = self._queue.get_nowait()
            (dropped if selected(turn) else retained).append(turn)
        for turn in retained:
            self._queue.put_nowait(turn)
        for turn in dropped:
            turn.cancelled = True
            turn.retry_wake.set()
            if turn.completion is not None and not turn.completion.done():
                turn.completion.cancel()
            if turn.future is not None and not turn.future.done():
                if turn.capture_tools:
                    turn.future.set_result(CapturedTurnResult(text="", tool_deliveries=(), aborted=True))
                else:
                    turn.future.set_result("")
        # Everything selected is removed before the first suspension. A fresh
        # request arriving during a slow native interrupt cannot join this Stop.
        if owns_active and pending is not None and self.pending is pending and not pending.future.done():
            pending.future.set_result(None)
            self.pending = None
        fence_error = None
        if self._connecting_client is not None and owns_active and self._current_turn is active:
            self._interrupting = True
            try:
                await self.close()
            except Exception as exc:
                fence_error = exc
        if owns_active and self._current_turn is active and self._turn_active and self._client is not None:
            self._interrupting = True
            try:
                await self._client.interrupt()
            except Exception:
                logger.debug("Native interrupt failed; fencing the owned host")
                try:
                    await self.close()
                except Exception as exc:
                    fence_error = exc
        for turn in dropped:
            try:
                await self.receipt(turn.mode, turn.reply_meta or {}, "cancelled")
            except Exception as exc:
                fence_error = fence_error or exc
            try:
                await self.notify_activity(turn.mode, turn.reply_meta or {}, "cancelled")
            except Exception as exc:
                fence_error = fence_error or exc
        if fence_error is not None:
            raise fence_error

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
            try:
                await self.close()
            except Exception:
                await self._reply("Could not resume that conversation. This conversation remains paused.")
                return
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
        elif self._retrying_turn is not None:
            state = "I'm waiting for a temporary connection issue to clear, then I'll continue your message."
        elif self._delivery_turn is not None:
            state = "I've prepared your reply and am checking delivery."
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

    @staticmethod
    def _phone_key(value: Any) -> str:
        """Normalize a phone-like target for same-recipient comparisons."""
        raw = str(value or "").strip()
        digits = re.sub(r"\D", "", raw)
        return digits or raw.lower()

    def mark_tool_delivery(self, mode: str, target: str) -> None:
        """Record a successful tool send that already answered this turn.

        Only a send on the inbound channel to the inbound counterparty counts.
        Cross-channel sends and sends to third parties still need the normal
        automatic reply so the human hears that the action completed.
        """
        if not self._turn_active:
            return

        normalized_mode = str(mode or "").strip().lower()
        target = str(target or "").strip()
        self._current_tool_deliveries.append(ToolDeliveryResult(
            mode=normalized_mode,
            target=target,
            sent=True,
            error_kind="none",
        ))
        if normalized_mode == "sms":
            self._settle_hosted_sms_attempt("success")
        if normalized_mode != self.mode:
            return
        matched = False
        if self.mode == "email":
            current = str(
                self.reply_meta.get("to") or self.reply_meta.get("sender") or ""
            ).strip()
            matched = bool(target and current and target.lower() == current.lower())
        elif self.mode == "sms":
            conversation_id = str(self.reply_meta.get("conversation_id") or "").strip()
            current = str(
                self.reply_meta.get("to") or self.reply_meta.get("sender") or ""
            ).strip()
            matched = bool(
                (conversation_id and target == conversation_id)
                or (target and current and self._phone_key(target) == self._phone_key(current))
            )
        elif self.mode == "imessage":
            conversation_id = str(self.reply_meta.get("conversation_id") or "").strip()
            matched = bool(conversation_id and target == conversation_id)

        if matched:
            self._current_channel_tool_delivery = True
            logger.info(
                "[session %s] current-channel %s tool delivered the reply",
                self.chat_id,
                self.mode,
            )

    async def _observe_a2a_tool_start(
        self,
        hook_input: Dict[str, Any],
        _tool_use_id: Optional[str],
        _context: Any,
    ) -> Dict[str, Any]:
        """Observe A2A activity without giving delegated uploads a later turn's owner."""
        if (str(hook_input.get("tool_name") or "").removeprefix("mcp__inkbox__") == "inkbox_slack_upload_file"
                and hook_input.get("agent_id")):
            return {"hookSpecificOutput": {"hookEventName": "PreToolUse", "permissionDecision": "deny",
                "permissionDecisionReason": "Return the file to the originating conversation for upload."}}
        turn = self._current_turn
        a2a_context = turn.a2a_context if turn is not None else None
        if isinstance(a2a_context, dict):
            observe_a2a_tool_start(
                str(a2a_context.get("task_id") or ""),
                str(hook_input.get("tool_name") or ""),
            )
        return {}

    def mark_tool_failure(self, mode: str, target: str, error: Any) -> None:
        """Record a failed host-native tool attempt without retaining its payload."""
        if not self._turn_active:
            return
        error_kind = sms_tool_failure_kind(error)
        self._current_tool_deliveries.append(ToolDeliveryResult(
            mode=str(mode or "").strip().lower(),
            target=str(target or "").strip(),
            sent=False,
            error_kind=error_kind,
        ))
        if str(mode or "").strip().lower() == "sms":
            self._settle_hosted_sms_attempt(error_kind)

    def preflight_hosted_sms(self, target: str) -> Optional[Dict[str, str]]:
        """Reserve a trusted hosted SMS attempt before the provider is called."""
        context = self._current_hosted_sms_context
        if not self._turn_active or not isinstance(context, dict):
            return None
        expected = str(context.get("remote_phone") or "").strip()
        if not expected or str(target or "").strip() != expected:
            return {
                "kind": "terminal",
                "message": (
                    "Hosted-call SMS target does not match the authoritative "
                    "caller; send blocked."
                ),
            }
        try:
            reserved = reserve_hosted_sms_attempt(
                str(context.get("call_id") or ""),
                int(context.get("attempt") or 1),
                expected,
            )
        except Exception:
            logger.exception(
                "[session %s] hosted SMS reservation failed; send blocked",
                self.chat_id,
            )
            return {
                "kind": "terminal",
                "message": "Hosted-call SMS safety state is unavailable; send blocked.",
            }
        if not reserved:
            return {
                "kind": "duplicate",
                "message": (
                    "This hosted-call SMS attempt was already used; duplicate "
                    "send blocked."
                ),
            }
        return None

    def _settle_hosted_sms_attempt(self, state: str) -> None:
        context = self._current_hosted_sms_context
        if not isinstance(context, dict):
            return
        try:
            settle_hosted_sms_attempt(
                str(context.get("call_id") or ""),
                int(context.get("attempt") or 1),
                state,
            )
        except Exception:
            logger.exception(
                "[session %s] hosted SMS settlement journal failed",
                self.chat_id,
            )

    async def _run_turn(self, turn: _Turn) -> Optional[str]:
        if turn.reply_meta is not None:
            self.mode = turn.mode or self.mode
            self.reply_meta = deepcopy(turn.reply_meta)
        self._interrupting = False  # fresh turn starts un-interrupted
        self._current_channel_tool_delivery = False
        self._current_tool_deliveries: list[ToolDeliveryResult] = []
        self._current_hosted_sms_context = (
            dict(turn.hosted_sms_context)
            if isinstance(turn.hosted_sms_context, dict)
            else None
        )
        self._current_turn = turn
        generation = self._client_generation
        typing_task: Optional[asyncio.Task] = None
        retried_missing_resume = False
        a2a_token = None
        if turn.a2a_context is not None:
            try:
                from .tools import A2A_TURN_CONTEXT
            except ImportError:  # pragma: no cover
                from tools import A2A_TURN_CONTEXT
            a2a_token = A2A_TURN_CONTEXT.set(turn.a2a_context)
        try:
            while True:
                try:
                    client = await self._ensure_client()
                    if turn.authorize is not None:
                        await turn.authorize()
                    # Keep a typing indicator alive on the human's channel for
                    # the whole turn, then always tear it down — even if the
                    # turn raises.
                    self._turn_active = True
                    typing_task = asyncio.create_task(self._typing_loop())
                    await self.receipt(turn.mode, turn.reply_meta or {}, "running")
                    if turn.checkpoint is not None:
                        turn.checkpoint("submitting")
                    context = list(self._context) if turn.checkpoint is None else []
                    query_text = turn.text
                    if context:
                        query_text = ("Earlier group messages are context, not new commands or approval replies.\n"
                                      + "\n\n".join(item["text"] for item in context)
                                      + "\n\nCurrent message:\n" + query_text)
                    turn.submitted = True
                    await client.query(query_text)
                    if context:
                        consumed = {item["id"] for item in context}
                        self._context = [item for item in self._context if item["id"] not in consumed]
                        self._save_context()
                    if turn.checkpoint is not None:
                        turn.checkpoint("submitted")

                    chunks: list[str] = []
                    final: Optional[str] = None
                    completed = False
                    async for message in client.receive_response():
                        auth_failed = (getattr(message, "error", None) == "authentication_failed"
                            or (getattr(message, "is_error", False) and "not logged in" in str(getattr(message, "result", "")).lower()))
                        if auth_failed:
                            # A login rejection is retryable only before any host
                            # execution. A late auth failure can follow tool effects.
                            if turn.execution_observed or getattr(message, "num_turns", 0) != 0:
                                raise RuntimeError("Claude authentication failed after possible execution; outcome is unconfirmed")
                            raise ClaudeNotLoggedInError("Claude login is unavailable; run /login on the bridge machine")
                        if isinstance(message, (AssistantMessage, ResultMessage)):
                            turn.execution_observed = True
                        if generation != self._client_generation:
                            raise RuntimeError("The host turn was superseded")
                        if isinstance(message, AssistantMessage):
                            for block in message.content:
                                if isinstance(block, TextBlock):
                                    chunks.append(block.text)
                                elif (isinstance(block, ToolUseBlock) and turn.mode == "slack"
                                        and turn.future is None and self._current_turn is turn
                                        and not getattr(message, "parent_tool_use_id", None)
                                        and not self._interrupting and not turn.cancelled):
                                    await self.notify_activity("slack", turn.reply_meta or {},
                                                               "tool:" + block.name)
                        elif isinstance(message, ResultMessage):
                            if turn.checkpoint is not None:
                                completed = not message.is_error and message.subtype == "success"
                            final = message.result
                            if message.session_id and not message.is_error and self.on_session_id:
                                self.resume_session_id = message.session_id
                                self.on_session_id(self.chat_id, message.session_id)
                                if turn.checkpoint is not None:
                                    turn.checkpoint("submitted")
                    if turn.checkpoint is not None and (not completed or not self.resume_session_id):
                        raise RuntimeError("Companion host completion could not be confirmed")
                    reply = (final or "\n\n".join(chunks)).strip()
                    break
                except Exception as exc:
                    if isinstance(exc, ClaudeNotLoggedInError) and not retried_missing_resume and not turn.execution_observed:
                        retried_missing_resume = True
                        if typing_task is not None:
                            typing_task.cancel()
                            await asyncio.gather(typing_task, return_exceptions=True)
                            typing_task = None
                        await self.close()
                        generation = self._client_generation
                        continue
                    if (
                        turn.checkpoint is not None
                        or turn.execution_observed
                        or retried_missing_resume
                        or not self.resume_session_id
                        or not _is_missing_resume_error(exc)
                    ):
                        raise
                    retried_missing_resume = True
                    if typing_task is not None:
                        typing_task.cancel()
                        try:
                            await typing_task
                        except asyncio.CancelledError:
                            pass
                        typing_task = None
                    self._turn_active = False
                    await self._clear_stale_resume()
                    generation = self._client_generation
        except Exception as exc:
            # A capture turn must always settle its waiter — surface the error
            # there. A normal turn re-raises so _drain shows the human a notice.
            if turn.future is not None and not turn.future.done():
                if turn.capture_tools and self._interrupting:
                    turn.future.set_result(CapturedTurnResult(
                        text="",
                        tool_deliveries=tuple(self._current_tool_deliveries),
                        aborted=True,
                    ))
                else:
                    turn.future.set_exception(exc)
                return
            raise
        finally:
            if a2a_token is not None:
                A2A_TURN_CONTEXT.reset(a2a_token)
            self._turn_active = False
            pending = self.pending
            if pending is not None and pending.owner is turn:
                if not pending.future.done():
                    pending.future.set_result(None)
                self.pending = None
            self._current_turn = None
            self._current_hosted_sms_context = None
            if typing_task is not None:
                typing_task.cancel()
                try:
                    await typing_task
                except asyncio.CancelledError:
                    pass

        if turn.mode == "imessage" and self.cfg.imessage_threaded_replies:
            duplicate = any(owner is turn and text == reply and target == (turn.reply_meta or {}).get("imessage_reply_target")
                            for owner, text, target in self._imessage_tool_outputs)
            self._imessage_tool_outputs = [item for item in self._imessage_tool_outputs if item[0] is not turn]
            if duplicate:
                reply = ""
        if turn.checkpoint is not None:
            reply = "" if self._current_channel_tool_delivery or self._interrupting else reply
            turn.checkpoint("generated", reply=reply, cancelled=self._interrupting)
            return reply

        # Route the result. Capture turns hand the text back to their waiter and
        # never auto-reply (the caller speaks/queues/swallows it). Normal turns
        # reply on the channel the human last used — unless a new message
        # interrupted this one, in which case the partial answer is dropped.
        if turn.future is not None:
            if not turn.future.done():
                if turn.capture_tools:
                    turn.future.set_result(CapturedTurnResult(
                        text=reply,
                        tool_deliveries=tuple(self._current_tool_deliveries),
                        aborted=self._interrupting,
                    ))
                else:
                    turn.future.set_result(
                        reply or "I finished that, but didn't have anything to say back."
                    )
            return
        if self._interrupting:
            await self.receipt(turn.mode, turn.reply_meta or {}, "cancelled")
            return
        if self._current_channel_tool_delivery:
            logger.info(
                "[session %s] suppressing automatic %s reply after tool delivery",
                self.chat_id,
                self.mode,
            )
            return
        if reply and reply.strip() != "[SILENT]":
            await self.receipt(turn.mode, turn.reply_meta or {}, "reply_pending", reply)
            turn.saved_reply = reply
            await self._deliver_reply(turn, reply)
        else:
            await self.receipt(turn.mode, turn.reply_meta or {}, "done")

    async def _clear_stale_resume(self) -> None:
        """Forget a stale Claude resume id and reset conversation-scoped state."""
        logger.warning(
            "[session %s] Claude resume id %s is stale; retrying fresh",
            self.chat_id,
            self.resume_session_id,
        )
        self.resume_session_id = None
        if self.on_clear is not None:
            self.on_clear(self.chat_id)
        self.always_allowed.clear()
        await self.close()

    async def _deliver_reply(self, turn: _Turn, reply: str) -> None:
        """Send a normal turn's reply, feeding a rejection to the failure loop.

        A synchronous send rejection (carrier spam filter, opt-out, invalid
        recipient, too-long) comes back as an API error, not a webhook. Hand
        it to the gateway's shared delivery-failure loop so it can wake the
        agent with the rule to fix — sharing one capped budget with the async
        delivery-failure webhooks, so a reply that keeps failing goes quiet
        after the cap instead of looping.

        Args:
            turn (_Turn): The turn whose reply is being sent.
            reply (str): Claude's reply text.

        Returns:
            None
        """
        mode, meta = turn.mode or self.mode, deepcopy(turn.reply_meta or self.reply_meta)
        generation = self._client_generation
        self._delivery_turn = turn

        def guard():
            if (turn.cancelled or self._shutting_down or self._delivery_turn is not turn
                    or generation != self._client_generation or getattr(self, "_execution_blocked", False)):
                raise PermissionError("Reply ownership is no longer active")

        token = CHANNEL_DELIVERY_GUARD.set(guard)
        try:
            guard()
            await self.send_fn(self.chat_id, reply, mode, {**meta, "final_reply": True})
            await self.receipt(mode, meta, "done")
        except Exception as exc:
            if mode == "slack" or (mode == "imessage" and self.cfg.imessage_threaded_replies):
                raise
            logger.warning("Reply send rejected (%s)", type(exc).__name__)
            if self.on_send_rejected is not None:
                await self.on_send_rejected(self.chat_id, mode, meta, reply, exc)
        finally:
            CHANNEL_DELIVERY_GUARD.reset(token)
            if self._delivery_turn is turn:
                self._delivery_turn = None

    async def run_consult(
        self,
        query: str,
        *,
        a2a_context: Optional[Dict[str, Any]] = None,
        mode: Optional[str] = None,
        reply_meta: Optional[Dict[str, Any]] = None,
    ) -> str:
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
        await self._queue.put(
            _Turn(text=query, future=future, a2a_context=a2a_context,
                  mode=mode or self._reply_route()[0],
                  reply_meta=deepcopy(reply_meta if reply_meta is not None else self._reply_route()[1]))
        )
        if self._worker is None or self._worker.done():
            self._worker = asyncio.create_task(self._drain())
        return await future

    async def run_consult_detailed(
        self,
        query: str,
        *,
        hosted_sms_context: Optional[Dict[str, Any]] = None,
    ) -> CapturedTurnResult:
        """Run a capture turn and return sanitized host-native tool outcomes."""
        loop = asyncio.get_running_loop()
        future: asyncio.Future[CapturedTurnResult] = loop.create_future()
        await self._queue.put(_Turn(
            text=query,
            future=future,
            capture_tools=True,
            hosted_sms_context=hosted_sms_context,
            mode=self._reply_route()[0], reply_meta=deepcopy(self._reply_route()[1]),
        ))
        if self._worker is None or self._worker.done():
            self._worker = asyncio.create_task(self._drain())
        return await future

    async def _typing_loop(self) -> None:
        """Refresh the channel's typing indicator until the turn ends.

        Returns:
            None: Runs until cancelled by :meth:`_run_turn` or the safety cap.
        """
        if self.typing_fn is None:
            return
        if self.reply_meta.get("typing") is False:
            return
        elapsed = 0.0
        try:
            while elapsed < TYPING_MAX_SECONDS:
                # Only iMessage has a typing bubble; stay quiet while an
                # escalation is parked waiting on the human to reply.
                if self.mode == "imessage" and self.pending is None:
                    try:
                        await self.typing_fn(self.chat_id, self.mode, self.reply_meta)
                    except Exception:
                        logger.debug("[session %s] typing ping failed", self.chat_id, exc_info=True)
                await asyncio.sleep(TYPING_REFRESH_SECONDS)
                elapsed += TYPING_REFRESH_SECONDS
        except asyncio.CancelledError:
            return

    async def _ensure_client(self) -> ClaudeSDKClient:
        if self._pending_fenced_owners:
            # Retry the durable proof before handing fresh work to another host.
            async with self._close_lock:
                self._persist_host_fences()
        if getattr(self, "_execution_blocked", False):
            raise PermissionError("Previous native host ownership could not be fenced; inspect the retained receipt before resuming")
        if self._client is not None:
            return self._client
        if not CLAUDE_SDK_AVAILABLE:
            raise RuntimeError(
                "claude-agent-sdk is not installed; run: pip install claude-agent-sdk"
            )

        try:
            from .tools import CURRENT_SESSION
        except ImportError:  # pragma: no cover - direct local import/test fallback
            from tools import CURRENT_SESSION
        # Bind this session before the client connects: the client's internal
        # tool-dispatch tasks inherit this context, so the Inkbox tools can
        # read the session's live mode (e.g. to pick an outbound call line).
        CURRENT_SESSION.set(self)

        # Session-scoped extras (e.g. the external-event directive) ride on
        # the system prompt so they carry harness authority for every turn.
        prompt_append = build_channel_prompt(
            project_dir=self.cfg.project_dir,
            identity_handle=self.identity_info.get("handle", ""),
            email_address=self.identity_info.get("email", ""),
            phone_number=self.identity_info.get("phone", ""),
            channels="email, SMS, iMessage, Slack, and voice calls" if self.cfg.slack_enabled else "email, SMS, iMessage, and voice calls",
        )
        if self.system_prompt_extra:
            prompt_append = f"{prompt_append}\n\n{self.system_prompt_extra}"

        options = ClaudeAgentOptions(
            cwd=self.cfg.project_dir or None,
            model=self.cfg.claude_model or None,
            system_prompt={
                "type": "preset",
                "preset": "claude_code",
                "append": prompt_append,
            },
            setting_sources=["user", "project"],
            # Read-only tools and our own Inkbox tools run without a text;
            # everything else lands in _can_use_tool and escalates.
            allowed_tools=list(self.cfg.auto_allowed_tools) + list(self.mcp_tool_names),
            mcp_servers={"inkbox": self.mcp_server},
            can_use_tool=self._can_use_tool,
            hooks={
                "PreToolUse": [
                    HookMatcher(hooks=[self._observe_a2a_tool_start]),
                ],
            },
            resume=self.resume_session_id or None,
        )
        generation = self._client_generation
        client = ClaudeSDKClient(options=options)
        self._connecting_client = client
        try:
            await client.connect()
            if generation != self._client_generation:
                raise asyncio.CancelledError
        except BaseException:
            try:
                await client.disconnect()
            except Exception:
                pass
            raise
        finally:
            if self._connecting_client is client:
                self._connecting_client = None
        self._client = client
        from .runtime import process_identity
        self._host_owner = process_identity(client)
        logger.info(
            "[session %s] Claude Code session started (resume=%s)",
            self.chat_id, self.resume_session_id or "fresh",
        )
        return self._client

    # ------------------------------------------------------------------
    # Escalation (permission prompts + AskUserQuestion polls)
    # ------------------------------------------------------------------

    async def _can_use_tool(self, tool_name: str, input_data: Dict[str, Any], context: Any):
        if self._current_turn is not None:
            self._current_turn.execution_observed = True
        if self.reply_meta.get("companion") and not self.companion_approver:
            return PermissionResultDeny(message="This conversation has no verified sender to approve tools.")
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

    async def _escalate(self, kind, prompt_text, questions=None, tool_name=""):
        generation = self._client_generation
        owner = self._current_turn
        async with self._escalation_lock:
            if generation != self._client_generation or owner is not self._current_turn or self._interrupting:
                return None
            return await self._escalate_once(kind, prompt_text, questions, tool_name)

    async def _escalate_once(
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
        reply_mode, route = self._reply_route()
        if route.get("companion") and self.cfg.group_reply_mode == "mention":
            prompt_text += ("\nMention the bot in your reply." if reply_mode == "slack" else
                            "\nInclude @agent in your reply" + (" or put my address in To." if reply_mode == "email" else "."))
        pending = PendingInteraction(
            owner=self._current_turn, generation=self._client_generation,
            route=deepcopy(route),
            kind=kind,
            sender=self.companion_approver if route.get("companion") else str(route.get("sender") or ""),
            prompt_text=prompt_text,
            future=loop.create_future(),
            questions=list(questions or []),
            tool_name=tool_name,
        )
        self.pending = pending
        try:
            await self.notify_activity(reply_mode, route, "waiting")
            await self._reply(prompt_text)
            return await asyncio.wait_for(
                pending.future, timeout=self.cfg.permission_timeout_s
            )
        except asyncio.TimeoutError:
            return None
        finally:
            if self.pending is pending:
                self.pending = None
            if not self._interrupting:
                await self.notify_activity(reply_mode, route, "resumed")

    async def _reply(self, text: str) -> None:
        mode, meta = self._reply_route()
        await self.send_fn(self.chat_id, text, mode, deepcopy(meta))

    async def stop_companion(self) -> None:
        """Stop the host worker before its journal owner can be released."""
        self._shutting_down = True
        if self._retrying_turn is not None:
            self._retrying_turn.retry_wake.set()
        if self._worker is not None:
            self._worker.cancel()
            await asyncio.gather(self._worker, return_exceptions=True)
        while not self._queue.empty():
            turn = self._queue.get_nowait()
            if turn.completion is not None and not turn.completion.done():
                turn.completion.cancel()
        if self.pending is not None and not self.pending.future.done():
            self.pending.future.cancel()
        await self.close()

    async def close(self) -> None:
        async with self._close_lock:
            await self._close_owned()

    async def _close_owned(self) -> None:
        pending = self.pending
        if pending is not None and not pending.future.done():
            pending.future.set_result(None)
        self.pending = None
        self._client_generation += 1
        from .runtime import process_identity
        current_owner = process_identity(self._client) if self._client is not None else None
        if current_owner:
            self._host_owner = current_owner
        connecting = self._connecting_client
        self._connecting_client = None
        clients = [client for client in (connecting, self._client) if client is not None]
        owners = [process_identity(client) for client in clients]
        if self._host_owner and self._host_owner not in owners:
            owners.append(self._host_owner)
        blocked = bool(getattr(self, "_execution_blocked", False))
        prior_blocked_owners = getattr(self, "_blocked_owner_keys", None) if blocked else set()
        # A known old owner may be released after a later positive fence. An
        # unknown owner has no such proof path and must remain quarantined.
        unknown_owner = blocked and not prior_blocked_owners
        failed_owners = set()
        fenced = set(self._pending_fenced_owners)
        from .runtime import fence_process
        for owner in owners:
            if owner is not None and (owner["pid"], owner["created"]) not in fenced:
                if await asyncio.to_thread(fence_process, owner):
                    fenced.add((owner["pid"], owner["created"]))
                    self._pending_fenced_owners.add((owner["pid"], owner["created"]))
                else:
                    failed_owners.add((owner["pid"], owner["created"]))
        for client, owner in zip(clients, owners):
            try:
                await client.disconnect()
            except Exception:
                if owner is None:
                    unknown_owner = True
                elif (owner["pid"], owner["created"]) not in fenced:
                    failed_owners.add((owner["pid"], owner["created"]))
        self._client = None
        failed_owners.update((prior_blocked_owners or set()) - fenced)
        # Retain every unresolved owner before persistence can raise: the
        # clients above have already been detached and cannot rediscover it.
        self._execution_blocked = bool(unknown_owner or failed_owners)
        self._blocked_owner_keys = None if unknown_owner else failed_owners
        self._pending_fenced_owners.update(fenced)
        if self._side_effects:
            await asyncio.gather(*list(self._side_effects), return_exceptions=True)
        self._persist_host_fences()
        if unknown_owner or failed_owners:
            raise RuntimeError("The previous host execution could not be stopped")
        self._execution_blocked = False
        self._blocked_owner_keys = set()
        self._host_owner = None

    def _persist_host_fences(self) -> None:
        """Persist exact proof without closing a new host or changing its generation."""
        fenced = set(self._pending_fenced_owners)
        if fenced and self.fence_fn is not None:
            self.fence_fn(self.chat_id, fenced)
        self._pending_fenced_owners.difference_update(fenced)
        owner = self._host_owner
        if (owner and (owner["pid"], owner["created"]) in fenced
                and self._client is None and self._connecting_client is None
                and not getattr(self, "_execution_blocked", False)):
            self._host_owner = None


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
        on_send_rejected: Optional[SendRejectedFn] = None,
        activity_fn=None,
        receipt_fn=None,
        fence_fn=None,
    ):
        self.cfg = cfg
        self.activity_fn = activity_fn
        self.receipt_fn = receipt_fn
        self.fence_fn = fence_fn
        self.send_fn = send_fn
        self.typing_fn = typing_fn
        self.health_fn = health_fn
        self.on_send_rejected = on_send_rejected
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

    def has_session(self, chat_id: str) -> bool:
        return chat_id in self.sessions or chat_id in self._session_ids

    def get(self, chat_id: str, system_prompt_extra: str = "") -> ContactSession:
        """Fetch or lazily create the session for one remote party.

        Args:
            chat_id (str): Contact id, or raw address/number fallback.
            system_prompt_extra (str): Optional extra system-prompt text bound
                to the session if this call creates it (existing sessions keep
                the prompt they started with).

        Returns:
            ContactSession: The (possibly new) session for that contact.
        """
        session = self.sessions.get(chat_id)
        if session is None:
            session = ContactSession(
                chat_id=chat_id,
                activity_fn=self.activity_fn,
                receipt_fn=self.receipt_fn,
                fence_fn=self.fence_fn,
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
                on_send_rejected=self.on_send_rejected,
                system_prompt_extra=system_prompt_extra,
            )
            self.sessions[chat_id] = session
        return session

    async def close_all(self) -> None:
        results = await asyncio.gather(*(session.stop_companion() for session in self.sessions.values()), return_exceptions=True)
        if any(isinstance(result, BaseException) for result in results):
            logger.warning("Some conversations remain paused because shutdown could not confirm their host stopped")
