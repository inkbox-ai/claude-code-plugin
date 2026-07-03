"""Inkbox ↔ OpenAI Realtime API voice bridge for live phone calls.

When Realtime is configured, the gateway pre-opens an OpenAI Realtime
WebSocket *before* accepting the Inkbox call in raw-media mode, then runs
two pumps for the call's duration:

* caller audio (Inkbox ``media`` frames, base64 μ-law) → OpenAI
  ``input_audio_buffer.append``; server-side VAD handles turn-taking.
* OpenAI ``response.output_audio.delta`` → Inkbox ``media`` frames, so the
  model's own voice is what the caller hears.

The Realtime model runs the spoken conversation itself. It only reaches
back to Claude Code through the ``consult_agent`` tool — and only when the
caller asks for real work or account/contact context. The consult runs in the caller's
shared :class:`~inkbox_claude.sessions.ContactSession` and its text answer
is handed back to the model, which speaks it. If OpenAI can't be reached
the gateway falls back to Inkbox STT/TTS (see ``_handle_call_ws``).
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import time
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set, Tuple
from urllib.parse import urlencode

try:
    import aiohttp
except ImportError:  # pragma: no cover - aiohttp is a runtime dep
    aiohttp = None  # type: ignore

logger = logging.getLogger("inkbox_claude.realtime")


# ----------------------------------------------------------------------
# Constants
# ----------------------------------------------------------------------

REALTIME_URL = "wss://api.openai.com/v1/realtime"
DEFAULT_MODEL = "gpt-realtime-2"
DEFAULT_VOICE = "cedar"
# μ-law telephony audio, matching the codec Inkbox bridges from the carrier.
AUDIO_FORMAT_TELEPHONY = {"type": "audio/pcmu"}
INPUT_TRANSCRIPTION_MODEL = "whisper-1"

CONSULT_TOOL_NAME = "consult_agent"
POST_CALL_ACTION_TOOL_NAME = "register_post_call_action"
EDIT_POST_CALL_ACTION_TOOL_NAME = "edit_post_call_action"
DELETE_POST_CALL_ACTION_TOOL_NAME = "delete_post_call_action"
HANG_UP_CALL_TOOL_NAME = "hang_up_call"

DEFAULT_CONSULT_TIMEOUT_S = 300.0
DEFAULT_CONNECT_TIMEOUT_S = 8.0
# hang_up_call is two-step: a second call within this window actually hangs up.
HANGUP_CONFIRM_WINDOW_S = 60.0
# Brief grace so the model's spoken goodbye reaches the caller before we drop.
HANGUP_CLOSE_DELAY_S = 2.0
# Never let a cancelled consult/task hold the call WebSocket cleanup forever.
TASK_CANCEL_TIMEOUT_S = 2.0


# A consult takes (query, recent_transcript) and returns Claude's spoken-
# friendly answer. The gateway wires this to the caller's ContactSession.
AgentConsultCallback = Callable[[str, List[Tuple[str, str]]], Awaitable[str]]
# After the call ends with queued actions: (actions, transcript) → run them.
PostCallActionsCallback = Callable[[List[Dict[str, str]], List[Tuple[str, str]]], Awaitable[None]]
# After a call with no queued actions: (transcript) → reflect / follow up.
CallEndedCallback = Callable[[List[Tuple[str, str]]], Awaitable[None]]


# ----------------------------------------------------------------------
# Config / per-call types
# ----------------------------------------------------------------------


@dataclass
class RealtimeConfig:
    """Realtime voice configuration, populated from the env in config.py."""

    enabled: bool = False
    api_key: str = ""
    model: str = DEFAULT_MODEL
    voice: str = DEFAULT_VOICE
    additional_instructions: str = ""
    consult_timeout_s: float = DEFAULT_CONSULT_TIMEOUT_S
    connect_timeout_s: float = DEFAULT_CONNECT_TIMEOUT_S
    fallback_to_inkbox_stt_tts: bool = True
    base_url: str = REALTIME_URL

    @property
    def has_credential(self) -> bool:
        return bool(self.api_key)


@dataclass
class RealtimeCallMeta:
    """Per-call metadata threaded to the greeting and instructions."""

    call_id: str
    remote_phone_number: Optional[str]
    direction: str = "inbound"
    agent_identity_handle: Optional[str] = None
    agent_identity_email: Optional[str] = None
    agent_identity_phone: Optional[str] = None
    # Whether the identity also has the shared Inkbox iMessage line enabled —
    # lets the spoken prompt draw the dedicated-vs-shared-line distinction.
    agent_imessage_enabled: bool = False
    project_dir: Optional[str] = None
    contact_known: bool = False
    contact_id: Optional[str] = None
    contact_name: Optional[str] = None
    contact_emails: List[str] = field(default_factory=list)
    contact_phones: List[str] = field(default_factory=list)
    contact_company: Optional[str] = None
    contact_job_title: Optional[str] = None
    contact_notes: Optional[str] = None
    # Outbound calls only: why this agent placed the call, threaded from
    # ``inkbox_place_call`` so the live session opens with context, not cold.
    outbound_purpose: Optional[str] = None
    outbound_opening: Optional[str] = None
    outbound_context: Optional[str] = None
    outbound_reason: Optional[str] = None
    outbound_scheduled_by: Optional[str] = None
    outbound_conversation_summary: Optional[str] = None


@dataclass
class _BridgeState:
    transcript: List[Tuple[str, str]] = field(default_factory=list)
    # Work the model asked to run after the call: [{"action", "details"}].
    post_call_actions: List[Dict[str, str]] = field(default_factory=list)
    closed: bool = False
    greeting_triggered: bool = False
    # Inkbox-assigned stream id from the `start` event; echoed on outbound
    # media / audio_done frames.
    stream_id: Optional[str] = None
    # Monotonic time the model first armed hang_up_call. A second call within
    # HANGUP_CONFIRM_WINDOW_S performs the real hangup. None = not armed.
    hangup_armed_at: Optional[float] = None
    # In-flight consult dispatches. The consult runs a full Claude Code turn
    # (seconds), so it is dispatched as a background task to keep the
    # OpenAI→Inkbox audio pump flowing; tracked here so call teardown can
    # cancel them.
    consult_tasks: Set["asyncio.Task[None]"] = field(default_factory=set)


# ----------------------------------------------------------------------
# Prompt builders
# ----------------------------------------------------------------------


def build_realtime_instructions(meta: RealtimeCallMeta, additional: str = "") -> str:
    """Compose the system prompt sent to the Realtime model.

    Args:
        meta (RealtimeCallMeta): Per-call context (caller, project).
        additional (str): Operator-supplied extra instructions.

    Returns:
        str: The instruction string for the ``session.update``.
    """
    lines = [
        "You are the configured Claude Code Inkbox agent speaking on a live Inkbox phone call.",
        "Use natural, concise spoken replies. Keep most answers to one or two short sentences.",
        "You are a voice; do not read out code, file paths, diffs, or logs verbatim.",
        "Do not mention implementation details unless the caller asks.",
    ]
    if meta.agent_identity_handle:
        lines.append(f"Your Inkbox identity handle: {meta.agent_identity_handle}.")
    if meta.agent_identity_email:
        lines.append(f"Your Inkbox agent email address: {meta.agent_identity_email}.")
    if meta.agent_identity_phone:
        lines.append(
            f"Your dedicated phone line (your own number, for SMS and voice calls): "
            f"{meta.agent_identity_phone}.",
        )
    if meta.agent_imessage_enabled:
        lines.append(
            "You also have a shared Inkbox iMessage line — voice calls and iMessage "
            "with people connected to you over iMessage. Its number is managed by "
            "Inkbox: never state or promise a number for it. The current call may be "
            "running over either line; calls follow the conversation's channel "
            "(iMessage contacts are called over the shared line, SMS/phone contacts "
            "over your dedicated number).",
        )
    if meta.remote_phone_number:
        lines.append(f"Remote phone number: {meta.remote_phone_number}.")
    if meta.contact_known:
        lines.append(
            "Known Inkbox contact info is already loaded; do not look them up or ask for details you already have.",
        )
        if meta.contact_name:
            lines.append(f"Contact name: {meta.contact_name}.")
        if meta.contact_id:
            lines.append(f"Inkbox contact id: {meta.contact_id}.")
        if meta.contact_company:
            lines.append(f"Contact company: {meta.contact_company}.")
        if meta.contact_job_title:
            lines.append(f"Contact title: {meta.contact_job_title}.")
        if meta.contact_emails:
            lines.append(f"Contact email(s): {', '.join(meta.contact_emails)}.")
        if meta.contact_phones:
            lines.append(f"Contact phone(s): {', '.join(meta.contact_phones)}.")
        if meta.contact_notes:
            lines.append(f"Contact notes: {meta.contact_notes}")
    else:
        lines.append(
            "No matching Inkbox contact record is loaded; use the phone number or a neutral greeting.",
        )
    if meta.direction == "outbound":
        if meta.outbound_purpose:
            lines.append(f"This is an outbound call you placed. Purpose: {meta.outbound_purpose}")
        if meta.outbound_reason:
            lines.append(f"Reason for the call: {meta.outbound_reason}")
        if meta.outbound_scheduled_by:
            lines.append(f"This call was scheduled by: {meta.outbound_scheduled_by}")
        if meta.outbound_conversation_summary:
            lines.append(
                f"Summary of the prior conversation that led to this call:\n{meta.outbound_conversation_summary}",
            )
        if meta.outbound_context:
            lines.append(f"Relevant outbound-call context:\n{meta.outbound_context}")
        if meta.outbound_opening:
            lines.append(
                f"Preferred opening message (say this naturally as your first turn): {meta.outbound_opening}",
            )
        lines.append(
            "For outbound calls, do not open with a generic offer to help. Start by explaining why you are calling, then ask the next specific question or give the requested update.",
        )
    lines.extend([
        "Do not perform a context lookup before greeting the caller. Do not say you are waiting on a lookup or checking context.",
        f"To do real work NOW in the project ({meta.project_dir or 'the working directory'}) "
        f"or Inkbox account - look up contacts, inspect texts/calls, use Inkbox tools, "
        f"read or edit files, run commands or tests, check git, or search the codebase - "
        f"call {CONSULT_TOOL_NAME} with a plain-English request. It runs the Claude Code "
        "agent in the caller's ongoing conversation and returns a spoken-friendly answer; read that answer back in your own voice.",
        f"If the caller wants work done AFTER the call (or accepts a deferral), call "
        f"{POST_CALL_ACTION_TOOL_NAME} to queue it. Tell them it's queued for after the "
        "call; do not claim it is already done.",
        f"If the caller changes or cancels queued after-call work, call "
        f"{EDIT_POST_CALL_ACTION_TOOL_NAME} or {DELETE_POST_CALL_ACTION_TOOL_NAME} with "
        f"the action_index returned when it was queued. If {CONSULT_TOOL_NAME} already "
        f"did the work a queued action describes, delete that action so it isn't repeated.",
        f"When the caller says goodbye or the conversation is clearly done, call "
        f"{HANG_UP_CALL_TOOL_NAME}: the first call arms hangup and asks you to say a short "
        "goodbye; after the goodbye, call it once more to actually end the call.",
        f"Do NOT call {CONSULT_TOOL_NAME} for greetings, small talk, or questions you "
        "can answer directly from the loaded call context. Use it whenever the caller wants "
        "something done in code, asks for contact/account context you do not already have, "
        "or needs an Inkbox tool lookup.",
        "While a tool runs you may say a brief 'one moment' so the caller isn't left in silence.",
    ])
    if additional.strip():
        lines += ["", additional.strip()]
    return "\n".join(lines)


def build_realtime_greeting(meta: RealtimeCallMeta) -> str:
    """Instructions for the proactive opening line spoken at pickup."""
    first_name = meta.contact_name.split()[0] if meta.contact_known and meta.contact_name else "there"
    if meta.direction == "outbound" and meta.outbound_opening:
        return (
            "Open the call by saying this naturally as the very first thing, with no greeting before it:\n"
            f"{meta.outbound_opening}"
        )
    if meta.direction == "outbound" and meta.outbound_purpose:
        return (
            f"Greet {first_name} briefly, then immediately explain that you are calling because: "
            f"{meta.outbound_purpose}. Do not ask a generic how-can-I-help question."
        )
    return (
        f"Greet the caller now as the very first thing you say. Say something like "
        f"'Hi {first_name}, this is your Claude Code Inkbox agent - how can I help?' "
        f"Keep it to one short sentence and then wait for them to respond."
    )


# ----------------------------------------------------------------------
# Tool schema
# ----------------------------------------------------------------------


def _consult_tool_schema() -> Dict[str, Any]:
    return {
        "type": "function",
        "name": CONSULT_TOOL_NAME,
        "description": (
            "Hand a request to the Claude Code agent working in the project, when "
            "the caller wants real work done - look up contacts, inspect Inkbox "
            "texts/calls/email, read/edit files, run commands or tests, check git "
            "status, search the codebase, etc. The request runs in the caller's "
            "ongoing conversation and you get back a spoken-friendly "
            "answer to read aloud. Do NOT use this for greetings or small talk."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": (
                        "What to ask Claude Code, in plain English. Include enough "
                        "context that it can act standalone."
                    ),
                },
            },
            "required": ["query"],
        },
    }


def _post_call_action_tool_schema() -> Dict[str, Any]:
    return {
        "type": "function",
        "name": POST_CALL_ACTION_TOOL_NAME,
        "description": (
            "Queue work for Claude Code to do AFTER this call ends — e.g. open a "
            "PR, run a long task, email/text the caller a summary. Tell the caller "
            "it's queued; do NOT claim it is already done."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "description": "Plain-English task for Claude Code. Include the outcome wanted.",
                },
                "details": {
                    "type": "string",
                    "description": "Optional extra context, constraints, or draft text.",
                },
            },
            "required": ["action"],
        },
    }


def _edit_post_call_action_tool_schema() -> Dict[str, Any]:
    return {
        "type": "function",
        "name": EDIT_POST_CALL_ACTION_TOOL_NAME,
        "description": (
            "Edit a queued after-call action by its one-based action_index "
            "(returned by register_post_call_action) when the caller changes it."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "action_index": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "One-based index of the queued action to edit.",
                },
                "action": {
                    "type": "string",
                    "description": "Replacement task. Omit to keep the current task.",
                },
                "details": {
                    "type": "string",
                    "description": "Replacement details. Empty string clears details.",
                },
            },
            "required": ["action_index"],
        },
    }


def _delete_post_call_action_tool_schema() -> Dict[str, Any]:
    return {
        "type": "function",
        "name": DELETE_POST_CALL_ACTION_TOOL_NAME,
        "description": (
            "Delete a queued after-call action by its one-based action_index "
            "when the caller cancels it or it's already been handled."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "action_index": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "One-based index of the queued action to delete.",
                },
            },
            "required": ["action_index"],
        },
    }


def _hang_up_call_tool_schema() -> Dict[str, Any]:
    return {
        "type": "function",
        "name": HANG_UP_CALL_TOOL_NAME,
        "description": (
            "End the live phone call. TWO-STEP: the first call does NOT hang up — "
            "it prompts you to say a short goodbye. After the goodbye, call "
            "hang_up_call again to actually end the call. Use only when the caller "
            "asks to hang up, says goodbye, or the conversation is clearly complete."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "reason": {
                    "type": "string",
                    "description": "Optional short reason for ending the call.",
                },
            },
            "required": [],
        },
    }


# ----------------------------------------------------------------------
# Bridge lifecycle
# ----------------------------------------------------------------------


class RealtimeBridgeConnectError(Exception):
    """Raised when OpenAI Realtime cannot be opened before Inkbox accept."""

    def __init__(self, cause: Any):
        self.cause = cause
        super().__init__(f"OpenAI Realtime connect failed: {cause}")


@dataclass
class OpenedRealtimeBridge:
    """A connected OpenAI Realtime session, ready to bridge to Inkbox."""

    session: Any
    openai_ws: Any
    state: _BridgeState
    config: RealtimeConfig
    meta: RealtimeCallMeta
    _closed: bool = False

    async def run(
        self,
        *,
        inkbox_ws: Any,
        on_agent_consult: AgentConsultCallback,
        on_post_call_actions: PostCallActionsCallback,
        on_call_ended: CallEndedCallback,
    ) -> None:
        """Bridge the open OpenAI session to ``inkbox_ws`` for the whole call.

        Args:
            inkbox_ws (Any): The accepted Inkbox call WebSocket (raw media).
            on_agent_consult (AgentConsultCallback): Runs a consult and returns text.
            on_post_call_actions (PostCallActionsCallback): Runs queued actions after hangup.
            on_call_ended (CallEndedCallback): Runs a follow-up reflection when no actions queued.

        Returns:
            None: Returns when either side closes the socket.
        """
        state = self.state
        openai_ws = self.openai_ws
        try:
            inkbox_task = asyncio.create_task(
                _inkbox_to_openai_pump(inkbox_ws, openai_ws, state, self.meta),
                name=f"realtime-inkbox-pump-{self.meta.call_id}",
            )
            openai_task = asyncio.create_task(
                _openai_to_inkbox_pump(
                    openai_ws=openai_ws,
                    inkbox_ws=inkbox_ws,
                    state=state,
                    config=self.config,
                    meta=self.meta,
                    on_agent_consult=on_agent_consult,
                ),
                name=f"realtime-openai-pump-{self.meta.call_id}",
            )
            done, _pending = await asyncio.wait(
                {inkbox_task, openai_task}, return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                if task.cancelled():
                    continue
                exc = task.exception()
                if exc:
                    logger.warning("[realtime] pump %s raised: %s", task.get_name(), exc)
        finally:
            state.closed = True
            tasks = [
                task for task in (
                    locals().get("inkbox_task"),
                    locals().get("openai_task"),
                )
                if task is not None
            ]
            for task in tasks:
                if not task.done():
                    task.cancel()
            await _maybe_close_ws(inkbox_ws)
            await self.close()
            await _settle_tasks(tasks, label="pump")
            await _cancel_consult_tasks(state)

        # After teardown: run queued after-call work, or a follow-up reflection.
        await _dispatch_post_call(state, on_post_call_actions, on_call_ended)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        with suppress(Exception):
            await self.openai_ws.close()
        with suppress(Exception):
            await self.session.close()


async def open_inkbox_realtime_bridge(
    *, config: RealtimeConfig, meta: RealtimeCallMeta
) -> OpenedRealtimeBridge:
    """Open OpenAI Realtime before the Inkbox WebSocket commits to media mode.

    Args:
        config (RealtimeConfig): Resolved realtime settings (key, model, voice).
        meta (RealtimeCallMeta): Per-call context for the session prompt.

    Returns:
        OpenedRealtimeBridge: A connected bridge ready to ``run()``.

    Raises:
        RealtimeBridgeConnectError: If aiohttp is missing, no key is set, or
            the WebSocket handshake fails / times out.
    """
    if aiohttp is None:
        raise RealtimeBridgeConnectError("aiohttp not available")
    if not config.has_credential:
        raise RealtimeBridgeConnectError("no OpenAI API key configured")

    session = aiohttp.ClientSession()
    openai_ws = None
    try:
        separator = "&" if "?" in config.base_url else "?"
        url = f"{config.base_url}{separator}{urlencode({'model': config.model})}"
        openai_ws = await asyncio.wait_for(
            session.ws_connect(
                url, headers={"Authorization": f"Bearer {config.api_key}"}, heartbeat=30
            ),
            timeout=config.connect_timeout_s,
        )
        await _send_session_update(openai_ws, config, meta)
        return OpenedRealtimeBridge(
            session=session,
            openai_ws=openai_ws,
            state=_BridgeState(),
            config=config,
            meta=meta,
        )
    except Exception as exc:
        if openai_ws is not None:
            with suppress(Exception):
                await openai_ws.close()
        with suppress(Exception):
            await session.close()
        if isinstance(exc, RealtimeBridgeConnectError):
            raise
        raise RealtimeBridgeConnectError(exc) from exc


async def _cancel_consult_tasks(state: _BridgeState) -> None:
    """Cancel in-flight consult tasks and let them settle."""
    tasks = list(state.consult_tasks)
    state.consult_tasks.clear()
    if not tasks:
        return
    for task in tasks:
        task.cancel()
    await _settle_tasks(tasks, label="consult")


async def _settle_tasks(tasks: List["asyncio.Task[Any]"], *, label: str) -> None:
    """Let cancelled background tasks drain, but never block call teardown."""
    if not tasks:
        return
    try:
        await asyncio.wait_for(
            asyncio.gather(*tasks, return_exceptions=True),
            timeout=TASK_CANCEL_TIMEOUT_S,
        )
    except asyncio.TimeoutError:
        names = ", ".join(task.get_name() for task in tasks)
        logger.warning("[realtime] timed out waiting for %s task cancellation: %s", label, names)


# ----------------------------------------------------------------------
# Session config + pumps
# ----------------------------------------------------------------------


async def _send_session_update(
    openai_ws: Any, config: RealtimeConfig, meta: RealtimeCallMeta
) -> None:
    """Send the initial ``session.update`` configuring audio, VAD, and tools."""
    payload = {
        "type": "session.update",
        "session": {
            "type": "realtime",
            "model": config.model,
            "instructions": build_realtime_instructions(meta, config.additional_instructions),
            "output_modalities": ["audio"],
            "audio": {
                "input": {
                    "format": AUDIO_FORMAT_TELEPHONY,
                    "noise_reduction": None,
                    "transcription": {"model": INPUT_TRANSCRIPTION_MODEL},
                    # Server-side VAD: the model auto-detects speech start/stop,
                    # auto-responds, and supports barge-in. The bridge never
                    # triggers response.create per turn itself.
                    "turn_detection": {
                        "type": "server_vad",
                        "threshold": 0.5,
                        "prefix_padding_ms": 300,
                        "silence_duration_ms": 500,
                        "create_response": True,
                        "interrupt_response": True,
                    },
                },
                "output": {
                    "format": AUDIO_FORMAT_TELEPHONY,
                    "voice": config.voice,
                },
            },
            "tools": [
                _consult_tool_schema(),
                _post_call_action_tool_schema(),
                _edit_post_call_action_tool_schema(),
                _delete_post_call_action_tool_schema(),
                _hang_up_call_tool_schema(),
            ],
            "tool_choice": "auto",
        },
    }
    await openai_ws.send_str(json.dumps(payload))


async def _maybe_send_greeting(
    openai_ws: Any, state: _BridgeState, meta: RealtimeCallMeta
) -> None:
    """Fire the proactive opening line once, so calls don't open with silence."""
    if state.greeting_triggered:
        return
    state.greeting_triggered = True
    try:
        await openai_ws.send_str(json.dumps({
            "type": "response.create",
            "response": {"instructions": build_realtime_greeting(meta)},
        }))
    except Exception as exc:
        logger.debug("[realtime] greeting send failed: %s", exc)


async def _inkbox_to_openai_pump(
    inkbox_ws: Any, openai_ws: Any, state: _BridgeState, meta: RealtimeCallMeta
) -> None:
    """Forward caller audio from Inkbox to OpenAI; fire the opening greeting.

    Inkbox sends ``{"event": "media", "media": {"payload": "<b64>"}}``; we
    re-emit as ``input_audio_buffer.append`` and let server-side VAD drive
    turns. The greeting fires on ``start`` (or first media if no start).
    """
    async for msg in inkbox_ws:
        if state.closed:
            return
        if msg.type == aiohttp.WSMsgType.TEXT:
            try:
                frame = json.loads(msg.data)
            except (TypeError, ValueError):
                continue
            event = (frame.get("event") or "").lower()
            if event == "start":
                state.stream_id = frame.get("stream_id") or state.stream_id
                await _maybe_send_greeting(openai_ws, state, meta)
            elif event == "media":
                if not state.greeting_triggered:
                    await _maybe_send_greeting(openai_ws, state, meta)
                payload_b64 = (frame.get("media") or {}).get("payload")
                if payload_b64:
                    await openai_ws.send_str(json.dumps({
                        "type": "input_audio_buffer.append",
                        "audio": payload_b64,
                    }))
            elif event in {"stop", "closed", "hangup"}:
                logger.info("[realtime] Inkbox WS signaled %s", event)
                return
        elif msg.type in {
            aiohttp.WSMsgType.CLOSE,
            aiohttp.WSMsgType.CLOSED,
            aiohttp.WSMsgType.ERROR,
        }:
            return


async def _openai_to_inkbox_pump(
    *,
    openai_ws: Any,
    inkbox_ws: Any,
    state: _BridgeState,
    config: RealtimeConfig,
    meta: RealtimeCallMeta,
    on_agent_consult: AgentConsultCallback,
) -> None:
    """Forward model audio to Inkbox and handle ``consult_agent`` calls."""
    # Function-call accumulation keyed by item_id. The name arrives on
    # output_item.added; args stream via ...arguments.delta and finalize on
    # ...arguments.done. Dedupe by call_id so a call dispatches at most once.
    fn_calls: Dict[str, Dict[str, str]] = {}
    dispatched: set = set()

    async def _finalize_fn_call(entry: Dict[str, str]) -> None:
        cid = (entry or {}).get("call_id") or ""
        if not cid or cid in dispatched:
            return
        dispatched.add(cid)
        logger.info(
            "[realtime] dispatching tool call name=%s call_id=%s",
            entry.get("name") or "",
            cid,
        )
        coro = _dispatch_tool_call(
            openai_ws=openai_ws,
            inkbox_ws=inkbox_ws,
            call_id=cid,
            name=entry.get("name") or "",
            arguments_json=entry.get("args") or "{}",
            state=state,
            config=config,
            on_agent_consult=on_agent_consult,
        )
        # The consult runs a full Claude Code turn (seconds). Awaiting it here
        # would freeze this read loop — no audio, no barge-in — so dispatch it
        # as a background task; it submits the tool result when it finishes,
        # which is exactly the async-tool flow gpt-realtime expects.
        task = asyncio.create_task(coro, name=f"realtime-consult-{cid}")
        state.consult_tasks.add(task)
        def _done(done_task: "asyncio.Task[None]") -> None:
            state.consult_tasks.discard(done_task)
            if done_task.cancelled():
                logger.info("[realtime] tool call cancelled call_id=%s", cid)
                return
            exc = done_task.exception()
            if exc:
                logger.warning("[realtime] tool call task failed call_id=%s: %s", cid, exc)

        task.add_done_callback(_done)

    async def _relay_transcript(party: str, text: str) -> None:
        # Realtime runs the WS in raw-media mode, so Inkbox does not create its
        # own STT transcript. Mirror finalized turns back into the call record.
        with suppress(Exception):
            await inkbox_ws.send_str(json.dumps({
                "event": "transcript",
                "party": party,
                "text": text,
                "is_final": True,
            }))

    async for msg in openai_ws:
        if state.closed:
            return
        if msg.type != aiohttp.WSMsgType.TEXT:
            if msg.type in {
                aiohttp.WSMsgType.CLOSE,
                aiohttp.WSMsgType.CLOSED,
                aiohttp.WSMsgType.ERROR,
            }:
                return
            continue
        try:
            frame = json.loads(msg.data)
        except (TypeError, ValueError):
            continue
        if not isinstance(frame, dict):
            continue
        ftype = frame.get("type", "")

        # GA: response.output_audio.delta; beta: response.audio.delta.
        if ftype in ("response.output_audio.delta", "response.audio.delta"):
            delta_b64 = frame.get("delta") or ""
            if delta_b64:
                out: Dict[str, Any] = {
                    "event": "media",
                    "media": {"payload": delta_b64, "track": "outbound"},
                }
                if state.stream_id:
                    out["stream_id"] = state.stream_id
                try:
                    await inkbox_ws.send_str(json.dumps(out))
                except Exception as exc:
                    logger.debug("[realtime] Inkbox WS send failed: %s", exc)
                    return

        # A response's audio finished — tell Inkbox to flush/play.
        elif ftype in ("response.output_audio.done", "response.audio.done"):
            done: Dict[str, Any] = {"event": "audio_done"}
            if state.stream_id:
                done["stream_id"] = state.stream_id
            with suppress(Exception):
                await inkbox_ws.send_str(json.dumps(done))

        # Caller started speaking (barge-in) — drop queued outbound audio.
        elif ftype == "input_audio_buffer.speech_started":
            with suppress(Exception):
                await inkbox_ws.send_str(json.dumps({"event": "clear"}))

        # Transcripts (for logging / consult context).
        elif ftype in (
            "response.output_audio_transcript.done",
            "response.audio_transcript.done",
        ):
            text = (frame.get("transcript") or "").strip()
            if text:
                state.transcript.append(("agent", text))
                await _relay_transcript("local", text)
        elif ftype == "conversation.item.input_audio_transcription.completed":
            text = (frame.get("transcript") or "").strip()
            if text:
                state.transcript.append(("caller", text))
                await _relay_transcript("remote", text)

        # Function-call lifecycle.
        elif ftype == "response.output_item.added":
            item = frame.get("item") or {}
            if item.get("type") == "function_call":
                item_id = item.get("id") or frame.get("item_id") or ""
                if item_id:
                    fn_calls[item_id] = {
                        "call_id": item.get("call_id") or "",
                        "name": item.get("name") or "",
                        "args": item.get("arguments") or "",
                    }
        elif ftype == "response.function_call_arguments.delta":
            key = frame.get("item_id") or frame.get("call_id") or ""
            if not key:
                continue
            entry = fn_calls.setdefault(key, {"call_id": "", "name": "", "args": ""})
            if not entry.get("call_id") and frame.get("call_id"):
                entry["call_id"] = frame["call_id"]
            if not entry.get("name") and frame.get("name"):
                entry["name"] = frame["name"]
            entry["args"] = (entry.get("args") or "") + (frame.get("delta") or "")
        elif ftype == "response.function_call_arguments.done":
            key = frame.get("item_id") or frame.get("call_id") or ""
            entry = fn_calls.get(key) or fn_calls.get(frame.get("call_id") or "") or {}
            if frame.get("call_id"):
                entry["call_id"] = frame["call_id"]
            if frame.get("name"):
                entry["name"] = frame["name"]
            if frame.get("arguments"):
                entry["args"] = frame["arguments"]
            await _finalize_fn_call(entry)
        # Fallback: a completed function_call item.
        elif ftype in ("response.output_item.done", "conversation.item.done"):
            item = frame.get("item") or {}
            if item.get("type") == "function_call":
                await _finalize_fn_call({
                    "call_id": item.get("call_id") or "",
                    "name": item.get("name") or "",
                    "args": item.get("arguments") or "{}",
                })
        elif ftype == "error":
            logger.warning("[realtime] OpenAI error frame: %s", frame.get("error"))


# ----------------------------------------------------------------------
# Tool dispatch
# ----------------------------------------------------------------------


async def _dispatch_tool_call(
    *,
    openai_ws: Any,
    inkbox_ws: Any,
    call_id: str,
    name: str,
    arguments_json: str,
    state: _BridgeState,
    config: RealtimeConfig,
    on_agent_consult: AgentConsultCallback,
) -> None:
    """Handle a function call from the Realtime model.

    Dispatches the five call tools: consult, register/edit/delete post-call
    action, and the two-step hang_up_call.
    """
    try:
        args = json.loads(arguments_json or "{}")
    except (TypeError, ValueError):
        args = {}

    if name == POST_CALL_ACTION_TOOL_NAME:
        await _handle_register_action(openai_ws, call_id, args, state)
        return
    if name == EDIT_POST_CALL_ACTION_TOOL_NAME:
        await _handle_edit_action(openai_ws, call_id, args, state)
        return
    if name == DELETE_POST_CALL_ACTION_TOOL_NAME:
        await _handle_delete_action(openai_ws, call_id, args, state)
        return
    if name == HANG_UP_CALL_TOOL_NAME:
        await _handle_hang_up(openai_ws, inkbox_ws, call_id, args, state)
        return
    if name != CONSULT_TOOL_NAME:
        await _submit_tool_result(
            openai_ws, call_id, {"error": f"Tool '{name}' is not available on calls."}
        )
        return

    query = (args.get("query") or "").strip()
    if not query:
        await _submit_tool_result(openai_ws, call_id, {"error": "missing query argument"})
        return

    # Best-effort interim cue so the caller hears something while Claude works.
    with suppress(Exception):
        await openai_ws.send_str(json.dumps({
            "type": "response.create",
            "response": {"instructions": "Say only 'One moment.'"},
        }))

    try:
        answer = await asyncio.wait_for(
            on_agent_consult(query, list(state.transcript)),
            timeout=config.consult_timeout_s,
        )
    except asyncio.TimeoutError:
        await _submit_tool_result(openai_ws, call_id, {
            "error": "consult timed out",
            "message": "Tell the caller you couldn't finish that right now; offer to follow up.",
        })
        return
    except Exception as exc:
        logger.warning("[realtime] consult failed: %s", exc)
        await _submit_tool_result(openai_ws, call_id, {
            "error": f"consult error: {exc}",
            "message": "Apologize briefly and ask if you can help another way.",
        })
        return

    await _submit_tool_result(openai_ws, call_id, {
        "status": "ok",
        "answer": answer,
        "instructions": "Read the answer back to the caller in your own voice. Keep it natural and concise.",
    })


async def _handle_register_action(
    openai_ws: Any, call_id: str, args: Dict[str, Any], state: _BridgeState
) -> None:
    """Queue an after-call action; the model is told it's queued, not done."""
    action = (args.get("action") or "").strip()
    if not action:
        await _submit_tool_result(openai_ws, call_id, {"error": "missing action argument"})
        return
    state.post_call_actions.append({"action": action, "details": (args.get("details") or "").strip()})
    await _submit_tool_result(openai_ws, call_id, {
        "status": "queued",
        "action_index": len(state.post_call_actions),
        "action_count": len(state.post_call_actions),
        "message": "Tell the caller the action is queued for after the call; do not claim it is done.",
    })


async def _handle_edit_action(
    openai_ws: Any, call_id: str, args: Dict[str, Any], state: _BridgeState
) -> None:
    """Edit a queued action in place by its one-based index."""
    index = _action_index(args)
    if index < 1 or index > len(state.post_call_actions):
        await _submit_tool_result(openai_ws, call_id, {
            "error": "invalid action_index", "action_count": len(state.post_call_actions),
        })
        return
    if "action" not in args and "details" not in args:
        await _submit_tool_result(openai_ws, call_id, {"error": "missing action or details argument"})
        return
    queued = state.post_call_actions[index - 1]
    if "action" in args:
        new_action = (args.get("action") or "").strip()
        if not new_action:
            await _submit_tool_result(openai_ws, call_id, {"error": "action cannot be empty"})
            return
        queued["action"] = new_action
    if "details" in args:
        queued["details"] = (args.get("details") or "").strip()
    await _submit_tool_result(openai_ws, call_id, {
        "status": "updated", "action_index": index, "action": queued,
        "message": "If the caller needs to know, confirm briefly the queued work was changed.",
    })


async def _handle_delete_action(
    openai_ws: Any, call_id: str, args: Dict[str, Any], state: _BridgeState
) -> None:
    """Remove a queued action by its one-based index."""
    index = _action_index(args)
    if index < 1 or index > len(state.post_call_actions):
        await _submit_tool_result(openai_ws, call_id, {
            "error": "invalid action_index", "action_count": len(state.post_call_actions),
        })
        return
    deleted = state.post_call_actions.pop(index - 1)
    await _submit_tool_result(openai_ws, call_id, {
        "status": "deleted", "deleted_action": deleted,
        "action_count": len(state.post_call_actions),
        "message": "If the caller needs to know, confirm briefly it was canceled.",
    })


async def _handle_hang_up(
    openai_ws: Any, inkbox_ws: Any, call_id: str, args: Dict[str, Any], state: _BridgeState
) -> None:
    """Two-step hangup: arm + goodbye, then drop the line on the second call."""
    if inkbox_ws is None:
        await _submit_tool_result(openai_ws, call_id, {"error": "hangup unavailable without Inkbox websocket"})
        return

    now = time.monotonic()
    armed = state.hangup_armed_at
    # First attempt (or a stale arm past the window) → arm and say goodbye
    # rather than dropping the caller mid-farewell.
    if armed is None or (now - armed) > HANGUP_CONFIRM_WINDOW_S:
        state.hangup_armed_at = now
        await _submit_tool_result(openai_ws, call_id, {
            "status": "confirm_goodbye",
            "message": (
                "Don't hang up yet. Say a brief, natural goodbye now, then call "
                "hang_up_call once more to actually end the call."
            ),
        })
        return

    # Second attempt within the window → perform the real hangup.
    reason = (args.get("reason") or "").strip()
    # Inkbox ends the call on a `stop` event; `hangup` is ignored server-side.
    stop_frame: Dict[str, Any] = {"event": "stop"}
    if reason:
        stop_frame["reason"] = reason
    if state.stream_id:
        stop_frame["stream_id"] = state.stream_id
    # Don't ask the model to speak again — we're ending the call.
    await _submit_tool_result(
        openai_ws, call_id,
        {"status": "hangup_requested", "reason": reason, "message": "The call is ending now."},
        create_response=False,
    )
    try:
        # Let the spoken goodbye land before we drop the carrier leg.
        await asyncio.sleep(HANGUP_CLOSE_DELAY_S)
        await inkbox_ws.send_str(json.dumps(stop_frame))
    except Exception as exc:
        logger.debug("[realtime] hangup frame send failed: %s", exc)
    state.closed = True
    await _maybe_close_ws(inkbox_ws)
    await _maybe_close_ws(openai_ws)


def _action_index(args: Dict[str, Any]) -> int:
    try:
        return int(args.get("action_index"))
    except (TypeError, ValueError):
        return 0


async def _dispatch_post_call(
    state: _BridgeState,
    on_post_call_actions: PostCallActionsCallback,
    on_call_ended: CallEndedCallback,
) -> None:
    """Run exactly one follow-up after the call: queued actions, else a reflection."""
    if state.post_call_actions:
        try:
            await on_post_call_actions(list(state.post_call_actions), list(state.transcript))
        except Exception as exc:
            logger.warning("[realtime] post-call action dispatch failed: %s", exc)
    else:
        try:
            await on_call_ended(list(state.transcript))
        except Exception as exc:
            logger.warning("[realtime] call-ended dispatch failed: %s", exc)


async def _maybe_close_ws(ws: Any) -> None:
    """Close a WS whether its close() is sync or a coroutine."""
    close = getattr(ws, "close", None)
    if not callable(close):
        return
    try:
        result = close()
        if inspect.isawaitable(result):
            await result
    except Exception:
        pass


async def _submit_tool_result(
    openai_ws: Any, call_id: str, output: Dict[str, Any], *, create_response: bool = True
) -> None:
    """Submit a function_call_output and (optionally) prompt the model to speak.

    Args:
        openai_ws (Any): The OpenAI Realtime WebSocket.
        call_id (str): The function call id being answered.
        output (dict): The tool result payload.
        create_response (bool): Whether to ask the model to respond afterward.
            False on hangup, where we don't want another spoken turn.
    """
    try:
        await openai_ws.send_str(json.dumps({
            "type": "conversation.item.create",
            "item": {
                "type": "function_call_output",
                "call_id": call_id,
                "output": json.dumps(output),
            },
        }))
        if not create_response:
            return
        # Bare response.create — let the session's audio settings apply (GA
        # rejects a modalities field here).
        await openai_ws.send_str(json.dumps({"type": "response.create"}))
    except Exception as exc:
        logger.debug("[realtime] submit_tool_result failed: %s", exc)
