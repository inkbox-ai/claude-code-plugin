"""Inkbox ↔ OpenAI Realtime API voice bridge for live phone calls.

Ported from hermes-agent-plugin's ``realtime.py``, trimmed to one tool.

When Realtime is configured, the gateway pre-opens an OpenAI Realtime
WebSocket *before* accepting the Inkbox call in raw-media mode, then runs
two pumps for the call's duration:

* caller audio (Inkbox ``media`` frames, base64 μ-law) → OpenAI
  ``input_audio_buffer.append``; server-side VAD handles turn-taking.
* OpenAI ``response.output_audio.delta`` → Inkbox ``media`` frames, so the
  model's own voice is what the caller hears.

The Realtime model runs the spoken conversation itself. It only reaches
back to Claude Code through the single ``consult_claude_code`` tool — and
only when the caller asks for real work. The consult runs in the caller's
shared :class:`~inkbox_claude.sessions.ContactSession` and its text answer
is handed back to the model, which speaks it. If OpenAI can't be reached
the gateway falls back to Inkbox STT/TTS (see ``_handle_call_ws``).
"""

from __future__ import annotations

import asyncio
import json
import logging
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
INPUT_TRANSCRIPTION_MODEL = "gpt-4o-mini-transcribe"

CONSULT_TOOL_NAME = "consult_claude_code"

DEFAULT_CONSULT_TIMEOUT_S = 120.0
DEFAULT_CONNECT_TIMEOUT_S = 8.0


# A consult takes (query, recent_transcript) and returns Claude's spoken-
# friendly answer. The gateway wires this to the caller's ContactSession.
AgentConsultCallback = Callable[[str, List[Tuple[str, str]]], Awaitable[str]]


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
    agent_identity_phone: Optional[str] = None
    project_dir: Optional[str] = None


@dataclass
class _BridgeState:
    transcript: List[Tuple[str, str]] = field(default_factory=list)
    closed: bool = False
    greeting_triggered: bool = False
    # Inkbox-assigned stream id from the `start` event; echoed on outbound
    # media / audio_done frames.
    stream_id: Optional[str] = None
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
        "You are a Claude Code agent speaking with your operator on a live phone call.",
        "Use natural, concise spoken replies — usually one or two short sentences.",
        "You are a voice; do not read out code, file paths, diffs, or logs verbatim.",
        "",
        f"To do any real work in the project ({meta.project_dir or 'the working directory'}) "
        f"— read or edit files, run commands or tests, check git, search the codebase — "
        f"call the {CONSULT_TOOL_NAME} tool with a plain-English request. It runs the "
        "Claude Code agent in the caller's ongoing conversation and returns a "
        "spoken-friendly answer; read that answer back in your own voice.",
        f"Do NOT call {CONSULT_TOOL_NAME} for greetings, small talk, or questions you "
        "can answer directly. Use it whenever the caller wants something done in the code.",
        "While the tool runs you may say a brief 'one moment' so the caller isn't left in silence.",
    ]
    if additional.strip():
        lines += ["", additional.strip()]
    return "\n".join(lines)


def build_realtime_greeting(meta: RealtimeCallMeta) -> str:
    """Instructions for the proactive opening line spoken at pickup."""
    return (
        "Greet the caller briefly and naturally, e.g. \"Hey, it's your Claude Code "
        "agent — what do you need?\" Keep it to one short sentence and then stop."
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
            "the caller wants real work done — read/edit files, run commands or "
            "tests, check git status, search the codebase, etc. The request runs "
            "in the caller's ongoing conversation and you get back a spoken-friendly "
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

    async def run(self, *, inkbox_ws: Any, on_agent_consult: AgentConsultCallback) -> None:
        """Bridge the open OpenAI session to ``inkbox_ws`` for the whole call.

        Args:
            inkbox_ws (Any): The accepted Inkbox call WebSocket (raw media).
            on_agent_consult (AgentConsultCallback): Runs a consult and returns text.

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
            _, pending = await asyncio.wait(
                {inkbox_task, openai_task}, return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()
        finally:
            state.closed = True
            await _cancel_consult_tasks(state)
            await self.close()

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
    for task in tasks:
        task.cancel()
    for task in tasks:
        with suppress(asyncio.CancelledError, Exception):
            await task


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
            "tools": [_consult_tool_schema()],
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
    """Forward model audio to Inkbox and handle ``consult_claude_code`` calls."""
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
        coro = _dispatch_tool_call(
            openai_ws=openai_ws,
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
        task.add_done_callback(state.consult_tasks.discard)

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
        elif ftype == "conversation.item.input_audio_transcription.completed":
            text = (frame.get("transcript") or "").strip()
            if text:
                state.transcript.append(("caller", text))

        # Function-call lifecycle.
        elif ftype == "response.output_item.added":
            item = frame.get("item") or {}
            if item.get("type") == "function_call":
                item_id = item.get("id") or ""
                fn_calls[item_id] = {
                    "call_id": item.get("call_id") or "",
                    "name": item.get("name") or "",
                    "args": item.get("arguments") or "",
                }
        elif ftype == "response.function_call_arguments.delta":
            item_id = frame.get("item_id") or ""
            if item_id in fn_calls:
                fn_calls[item_id]["args"] += frame.get("delta") or ""
        elif ftype == "response.function_call_arguments.done":
            item_id = frame.get("item_id") or ""
            entry = fn_calls.get(item_id)
            if entry is not None:
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
    call_id: str,
    name: str,
    arguments_json: str,
    state: _BridgeState,
    config: RealtimeConfig,
    on_agent_consult: AgentConsultCallback,
) -> None:
    """Handle a function call from the Realtime model (only the consult tool)."""
    try:
        args = json.loads(arguments_json or "{}")
    except (TypeError, ValueError):
        args = {}

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


async def _submit_tool_result(
    openai_ws: Any, call_id: str, output: Dict[str, Any]
) -> None:
    """Submit a function_call_output and trigger the model to speak the result."""
    try:
        await openai_ws.send_str(json.dumps({
            "type": "conversation.item.create",
            "item": {
                "type": "function_call_output",
                "call_id": call_id,
                "output": json.dumps(output),
            },
        }))
        # Bare response.create — let the session's audio settings apply (GA
        # rejects a modalities field here).
        await openai_ws.send_str(json.dumps({"type": "response.create"}))
    except Exception as exc:
        logger.debug("[realtime] submit_tool_result failed: %s", exc)
