"""Inkbox gateway for Claude Code.

The bridge's runtime core, modeled on the hermes-agent-plugin Inkbox
adapter:

1. On startup, bring up the identity's Inkbox tunnel (or use
   ``INKBOX_PUBLIC_URL``), reconcile webhook subscriptions for the
   identity's mailbox (``message.received`` plus the ``message.bounced`` /
   ``message.failed`` bounce events), phone number (``text.received`` plus
   ``text.delivery_failed``), and - when iMessage-enabled - the identity
   itself (``imessage.received``, ``imessage.reaction_received``, and
   ``imessage.delivery_failed``), and patch the phone number's
   incoming-call channel to auto-accept onto our call WebSocket.
2. Serve ``POST /webhook`` (HMAC-verified) and ``WS /phone/media/ws``.
3. Map every inbound event to a contact-keyed Claude Code session:
   one session per remote party across email + SMS + iMessage + voice.
4. Send Claude's replies back over the modality the human last used,
   stripping markdown for phone-bound channels.
5. When an outbound message fails to deliver (a hard failure webhook),
   correlate it to the original send and wake that session to recover —
   the recovery reply goes back on the same channel/thread by default.
"""

from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import json
import logging
import os
import shutil
import threading
import time
from collections import OrderedDict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from aiohttp import WSMsgType, web

    AIOHTTP_AVAILABLE = True
except ImportError:  # pragma: no cover
    web = WSMsgType = None  # type: ignore
    AIOHTTP_AVAILABLE = False

try:
    from inkbox import Inkbox, verify_webhook

    INKBOX_AVAILABLE = True
except ImportError:  # pragma: no cover
    Inkbox = verify_webhook = None  # type: ignore
    INKBOX_AVAILABLE = False

try:
    from inkbox.tunnels.client import connect as inkbox_tunnel_connect

    INKBOX_TUNNEL_AVAILABLE = True
except ImportError:  # pragma: no cover
    inkbox_tunnel_connect = None  # type: ignore
    INKBOX_TUNNEL_AVAILABLE = False

try:
    from .config import (
        DEFAULT_WEBHOOK_PATH,
        INKBOX_WS_PATH,
        BridgeConfig,
        call_contexts_dir,
        inkbox_client_kwargs,
    )
    from .media import download_media, inbound_media_note
    from .prompts import contact_marker, strip_markdown
    from .realtime import (
        RealtimeBridgeConnectError,
        RealtimeCallMeta,
        open_inkbox_realtime_bridge,
    )
    from .sessions import SessionManager
    from .tools import build_inkbox_mcp_server
except ImportError:  # pragma: no cover - direct local import/test fallback
    from config import DEFAULT_WEBHOOK_PATH, INKBOX_WS_PATH, BridgeConfig, call_contexts_dir, inkbox_client_kwargs
    from media import download_media, inbound_media_note
    from prompts import contact_marker, strip_markdown
    from realtime import (
        RealtimeBridgeConnectError,
        RealtimeCallMeta,
        open_inkbox_realtime_bridge,
    )
    from sessions import SessionManager
    from tools import build_inkbox_mcp_server

logger = logging.getLogger(__name__)


def _format_transcript(transcript: Any, limit: int = 30) -> str:
    """Render the last ``limit`` (role, text) turns as plain lines."""
    rows = list(transcript or [])[-limit:]
    return "\n".join(f"  {role}: {text}" for role, text in rows)


def _post_call_prompt(actions: List[Dict[str, str]], transcript: Any) -> str:
    """Build the Claude Code prompt that executes queued after-call work."""
    action_lines = "\n".join(
        f"  {i}. {a.get('action', '')}"
        + (f" — {a.get('details')}" if a.get("details") else "")
        for i, a in enumerate(actions or [], start=1)
    )
    convo = _format_transcript(transcript)
    parts = [
        "[voice call ended] You were just on a phone call with your operator and "
        "agreed to do this work after the call. Do the actions that are still needed:",
        action_lines or "  (none)",
        "",
        "Reconcile against the transcript first — skip anything already done or "
        "canceled on the call. Use your tools to actually perform the work; if you "
        "need to reach the operator, use the Inkbox messaging tools.",
    ]
    if convo:
        parts += ["", "Recent call transcript:", convo]
    return "\n".join(parts)


def _delivery_failure_prompt(channel: str, recipient: str, body: str, reason: str) -> str:
    """Build the Claude Code prompt for a failed outbound message.

    Args:
        channel (str): Channel that failed (SMS / iMessage / email).
        recipient (str): Intended recipient.
        body (str): The undelivered message text, if known.
        reason (str): Carrier/provider failure reason.

    Returns:
        str: A prompt instructing the agent to retry or switch channels.
    """
    quoted = f'\n\nThe message was:\n"{body}"' if body else ""
    return "\n".join([
        f"[delivery failed] Your {channel} message to {recipient} was NOT delivered.",
        f"Reason: {reason or 'unknown'}.{quoted}",
        "",
        "This matters — the person did not get what you sent. Recover now. Your reply "
        f"to THIS message is sent back to them on {channel} by default, so:",
        f"- To retry, just write the message again (shorten or rephrase it if the "
        "reason suggests that would help, e.g. drop a link or trim length).",
        f"- If {channel} looks broken for them, reach them another way instead: use "
        "your Inkbox tools to send on a different channel (SMS, iMessage, email) or, "
        "as a last resort, place a call — then reply exactly [SILENT] here so nothing "
        f"is re-sent on {channel}.",
        "- If no further action is warranted, reply exactly [SILENT] to do nothing.",
    ])


def _call_ended_prompt(transcript: Any) -> str:
    """Build the Claude Code prompt for a no-actions post-call reflection."""
    convo = _format_transcript(transcript)
    parts = [
        "[voice call ended] Your phone call with the operator just ended. If you "
        "committed to anything during it (open a PR, run a task, send a summary), "
        "do that now with your tools. First reconcile against the transcript: do "
        "not redo work that was already completed, queued, canceled, or superseded "
        "during the call. If there's nothing still needed, do nothing.",
    ]
    if convo:
        parts += ["", "Recent call transcript:", convo]
    return "\n".join(parts)


@dataclasses.dataclass
class _OutboundContext:
    """What we remember about an outbound send so we can recover it if it fails.

    Recorded when a bridge send queues successfully, keyed by the Inkbox
    message id. When a delivery-failure webhook arrives we correlate on that id
    to route the recovery turn back to the exact session and thread — even when
    the webhook payload can't resolve a contact on its own.
    """

    channel: str            # human label: "SMS" / "iMessage" / "email"
    mode: str               # session reply mode: "sms" / "imessage" / "email"
    session_key: str        # contact-keyed chat id the send belonged to
    recipient: str = ""
    body: str = ""          # snippet of what we sent, for the recovery prompt
    conversation_id: str = ""   # SMS / iMessage thread
    thread_id: str = ""         # email thread
    subject: str = ""           # email subject
    recovery: bool = False  # this send was itself a recovery attempt
    at: float = 0.0


OUTBOUND_CONTEXT_TTL_SECONDS = 3600
OUTBOUND_CONTEXT_MAX = 500
WEBHOOK_DEDUP_TTL_SECONDS = 300
CONTACT_CACHE_TTL_SECONDS = 300
SMS_MAX_LENGTH = 1600  # Inkbox SMS hard cap
IMESSAGE_MAX_LENGTH = 18995  # Sendblue-compatible iMessage text cap
# Inbound SMS carrier keywords handled entirely by the Inkbox server;
# never wake the agent for them.
SMS_CONTROL_WORDS = {"stop", "start", "help", "unstop", "unsubscribe", "cancel", "end", "quit"}
# Subscriptions must include the hard failed-delivery events, or the handlers
# below never fire. text.delivery_unconfirmed is deliberately NOT subscribed:
# it's carrier telemetry/uncertainty, not a hard failure to recover from.
TEXT_EVENTS = ["text.received", "text.delivery_failed"]
IMESSAGE_EVENTS = ["imessage.received", "imessage.reaction_received", "imessage.delivery_failed"]
MAIL_EVENTS = ["message.received", "message.bounced", "message.failed"]
# How many recovery turns we'll spin for one contact inside the dedup window
# before we stop, so a channel that keeps failing can't loop the agent forever.
MAX_RECOVERIES_PER_WINDOW = 3


def _message_too_long_reason(channel: str, content: str, max_chars: int) -> str:
    char_count = len(content or "")
    return (
        f"{channel} text is {char_count} characters; maximum is {max_chars}. "
        f"Shorten it or split it into smaller {channel} messages."
    )


def _claude_health() -> str:
    """Describe whether Claude Code can run: SDK present and auth available.

    Returns:
        str: A short readiness description (no token is spent).
    """
    try:
        import claude_agent_sdk  # noqa: F401
    except ImportError:
        return "agent SDK missing — can't run turns"
    if os.environ.get("ANTHROPIC_API_KEY"):
        return "ready (API key billing)"
    if (Path.home() / ".claude" / ".credentials.json").exists():
        return "ready (subscription login)"
    return "NOT authenticated — log in to Claude or set ANTHROPIC_API_KEY"


def _tunnel_state_dir() -> Path:
    root = Path.home() / ".inkbox-claude" / "tunnel"
    root.mkdir(parents=True, exist_ok=True)
    return root


class InkboxGateway:
    """Routes Inkbox webhooks into contact-keyed Claude Code sessions."""

    def __init__(self, cfg: BridgeConfig):
        self.cfg = cfg
        self._inkbox: Any = None
        self._identity: Any = None
        self._tunnel: Any = None
        self._public_url: str = ""
        self._public_host: str = ""
        self._runner: Any = None
        self.sessions: Optional[SessionManager] = None

        self._self_addresses: set[str] = set()
        self._recent_request_ids: Dict[str, float] = {}
        self._inflight_request_ids: Dict[str, float] = {}
        self._active_call_ws: Dict[str, Any] = {}
        self._call_meta_by_id: Dict[str, Dict[str, Any]] = {}
        # ((kind, value) -> (contact summary, expires_at)); mirrors Hermes'
        # per-inbound lookup cache for repeated remote phone/email events.
        self._contact_cache: Dict[Tuple[str, str], Tuple[Optional[Dict[str, Any]], float]] = {}
        # Failed outbound message ids we've already told the agent about, so a
        # webhook retry (or a second failure event for the same message) doesn't
        # re-notify and spin the agent in a loop.
        self._notified_failures: Dict[str, float] = {}
        # Outbound sends we might need to recover, keyed by Inkbox message id.
        self._outbound_by_id: "OrderedDict[str, _OutboundContext]" = OrderedDict()
        # Recovery turns spun per contact inside the dedup window (count, first_at),
        # so a channel that keeps failing can't loop the agent unbounded.
        self._recovery_counts: Dict[str, Tuple[int, float]] = {}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Connect to Inkbox, start the webhook server, and serve forever.

        Returns:
            None
        """
        if not AIOHTTP_AVAILABLE:
            raise RuntimeError("aiohttp is not installed; run: pip install aiohttp")
        if not INKBOX_AVAILABLE:
            raise RuntimeError("inkbox SDK is not installed; run: pip install 'inkbox>=0.4.10'")
        if not self.cfg.api_key or not self.cfg.identity:
            raise RuntimeError("INKBOX_API_KEY and INKBOX_IDENTITY must be set (see README)")

        self._inkbox = Inkbox(**inkbox_client_kwargs(self.cfg.api_key, self.cfg.base_url))
        self._identity = await asyncio.to_thread(self._inkbox.get_identity, self.cfg.identity)

        mailbox = getattr(self._identity, "mailbox", None)
        phone = getattr(self._identity, "phone_number", None)
        identity_info = {
            "handle": self._identity.agent_handle,
            "email": str(getattr(mailbox, "email_address", "") or ""),
            "phone": str(getattr(phone, "number", "") or ""),
        }
        if identity_info["email"]:
            self._self_addresses.add(identity_info["email"].lower())

        # Local webhook server first, so the tunnel has something to hit.
        await self._start_http_server()

        if self.cfg.public_url:
            self._public_url = self.cfg.public_url.rstrip("/")
            self._public_host = self._public_url.split("://", 1)[-1]
        else:
            await self._open_tunnel()

        await asyncio.to_thread(self._patch_identity_objects)

        # Sessions get the Inkbox tools so Claude can message proactively.
        server, tool_names = build_inkbox_mcp_server(self._inkbox, self.cfg.identity)
        self.sessions = SessionManager(
            cfg=self.cfg,
            send_fn=self.send_to_contact,
            mcp_server=server,
            mcp_tool_names=tool_names,
            identity_info=identity_info,
            typing_fn=self.send_typing,
            health_fn=self.health_report,
        )

        logger.info(
            "[bridge] ready — %s / %s / %s → Claude Code in %s",
            identity_info["handle"], identity_info["email"] or "(no mailbox)",
            identity_info["phone"] or "(no phone)", self.cfg.project_dir,
        )
        try:
            await asyncio.Event().wait()  # serve until cancelled
        finally:
            await self._cleanup()

    async def _start_http_server(self) -> None:
        app = web.Application()
        app.router.add_get("/health", self._handle_health)
        app.router.add_post(DEFAULT_WEBHOOK_PATH, self._handle_webhook)
        app.router.add_get(INKBOX_WS_PATH, self._handle_call_ws)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.cfg.host, self.cfg.port)
        await site.start()
        logger.info("[bridge] webhook server on %s:%d", self.cfg.host, self.cfg.port)

    async def _open_tunnel(self) -> None:
        if not INKBOX_TUNNEL_AVAILABLE:
            raise RuntimeError("inkbox SDK tunnel client unavailable; upgrade: pip install -U inkbox")
        state_dir = _tunnel_state_dir()
        # Wipe SDK tunnel state so a stale tunnel_id can't wedge reconnects.
        shutil.rmtree(state_dir, ignore_errors=True)
        state_dir.mkdir(parents=True, exist_ok=True)

        name = self.cfg.tunnel_name or self.cfg.identity
        self._tunnel = await asyncio.to_thread(
            inkbox_tunnel_connect,
            self._inkbox,
            name=name,
            forward_to=f"http://127.0.0.1:{self.cfg.port}",
            state_dir=state_dir,
        )

        # listener.wait() is what actually spawns the data-plane runtime
        # thread — without it inkboxwire returns 503 for every webhook.
        def _drive(listener):
            try:
                listener.wait()
            except Exception:
                logger.exception("[bridge] tunnel runtime exited")

        threading.Thread(target=_drive, args=(self._tunnel,), name="inkbox-tunnel-wait", daemon=True).start()
        self._public_url = self._tunnel.public_url.rstrip("/")
        self._public_host = self._tunnel.tunnel.public_host
        logger.info("[bridge] tunnel ready: %s → 127.0.0.1:%d", self._public_url, self.cfg.port)

    def _patch_identity_objects(self) -> None:
        """Point the identity's mailbox/phone/iMessage events at this server."""
        webhook_url = f"{self._public_url}{DEFAULT_WEBHOOK_PATH}"
        ws_url = f"wss://{self._public_host}{INKBOX_WS_PATH}"
        identity = self._inkbox.get_identity(self.cfg.identity)

        def _reconcile(owner_kw: Dict[str, Any], event_types: List[str]) -> None:
            existing = self._inkbox.webhooks.subscriptions.list(**owner_kw)
            for sub in existing:
                if sub.url == webhook_url and set(sub.event_types) == set(event_types):
                    return  # already wired
                if sub.url.endswith(DEFAULT_WEBHOOK_PATH):
                    # A previous bridge install — replace it.
                    self._inkbox.webhooks.subscriptions.delete(sub.id)
            self._inkbox.webhooks.subscriptions.create(
                url=webhook_url, event_types=event_types, **owner_kw
            )

        if identity.mailbox is not None:
            _reconcile({"mailbox_id": identity.mailbox.id}, MAIL_EVENTS)
            logger.info("[bridge] mailbox %s → %s", identity.mailbox.email_address, webhook_url)
        if identity.phone_number is not None:
            _reconcile({"phone_number_id": identity.phone_number.id}, TEXT_EVENTS)
            # auto_accept: Inkbox answers and opens the call WS directly.
            self._inkbox.phone_numbers.update(
                identity.phone_number.id,
                incoming_call_webhook_url=webhook_url,
                incoming_call_action="auto_accept",
                client_websocket_url=ws_url,
            )
            logger.info("[bridge] phone %s → %s + %s", identity.phone_number.number, webhook_url, ws_url)
        if getattr(identity, "imessage_enabled", False):
            _reconcile({"agent_identity_id": identity.id}, IMESSAGE_EVENTS)
            logger.info("[bridge] iMessage for %s → %s", self.cfg.identity, webhook_url)

    async def _cleanup(self) -> None:
        if self.sessions is not None:
            await self.sessions.close_all()
        if self._runner is not None:
            await self._runner.cleanup()
        if self._tunnel is not None:
            try:
                await asyncio.to_thread(self._tunnel.close)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Inbound: webhooks
    # ------------------------------------------------------------------

    async def _handle_health(self, request: "web.Request") -> "web.Response":
        return web.json_response({"ok": True, "identity": self.cfg.identity})

    def _prune_dedup_ids(self) -> None:
        now = time.time()
        for store in (self._recent_request_ids, self._inflight_request_ids):
            for key, seen_at in list(store.items()):
                if now - seen_at > WEBHOOK_DEDUP_TTL_SECONDS:
                    store.pop(key, None)
        if len(self._recent_request_ids) > 2000:
            oldest = sorted(self._recent_request_ids.items(), key=lambda item: item[1])
            for key, _seen_at in oldest[: len(self._recent_request_ids) - 2000]:
                self._recent_request_ids.pop(key, None)

    def _dedup_begin(self, request_id: str) -> bool:
        if not request_id:
            return False
        self._prune_dedup_ids()
        if request_id and request_id in self._recent_request_ids:
            return True
        if request_id and request_id in self._inflight_request_ids:
            return True
        self._inflight_request_ids[request_id] = time.time()
        return False

    def _dedup_commit(self, request_id: str) -> None:
        if not request_id:
            return
        self._prune_dedup_ids()
        self._inflight_request_ids.pop(request_id, None)
        self._recent_request_ids[request_id] = time.time()

    def _dedup_rollback(self, request_id: str) -> None:
        if request_id:
            self._inflight_request_ids.pop(request_id, None)

    def _is_duplicate(self, request_id: str) -> bool:
        if self._dedup_begin(request_id):
            return True
        self._dedup_commit(request_id)
        return False

    def _sender_allowed(self, *candidates: str) -> bool:
        if self.cfg.allow_all_users or not self.cfg.allowed_users:
            # Reachability is governed server-side by Inkbox contact rules.
            return True
        normalized = {c.lower() for c in candidates if c}
        return any(u.lower() in normalized for u in self.cfg.allowed_users)

    async def _handle_webhook(self, request: "web.Request") -> "web.Response":
        body = await request.read()
        if self.cfg.require_signature:
            if not self.cfg.signing_key:
                return web.Response(status=401, text="signing key not configured")
            ok = verify_webhook(
                payload=body, headers=dict(request.headers), secret=self.cfg.signing_key
            )
            if not ok:
                return web.Response(status=401, text="invalid signature")

        request_id = request.headers.get("X-Inkbox-Request-Id", "")
        if self._dedup_begin(request_id):
            return web.json_response({"ok": True, "deduped": True})

        try:
            envelope = json.loads(body)
        except json.JSONDecodeError:
            self._dedup_rollback(request_id)
            return web.Response(status=400, text="invalid json")

        try:
            event_type = str(envelope.get("event_type") or "")
            response = await self._dispatch_event(envelope, event_type)
        except Exception:
            self._dedup_rollback(request_id)
            raise
        self._dedup_commit(request_id)
        return response

    async def _dispatch_event(self, envelope: Dict[str, Any], event_type: str) -> "web.Response":
        """Route one parsed webhook event to its handler.

        Split out from transport/signature/dedup so the routing — in particular
        which events wake the agent — is unit-testable on its own.
        """
        if not event_type and (
            self._call_context_id(envelope)
            or (envelope.get("direction") == "inbound" and envelope.get("local_phone_number"))
        ):
            # Incoming-call payloads are flat (no envelope); with
            # auto_accept this is informational, but it can carry resolved
            # contact context before the WS starts.
            call_id = self._call_context_id(envelope)
            if call_id:
                self._call_meta_by_id[call_id] = envelope
                if len(self._call_meta_by_id) > 100:
                    self._call_meta_by_id.pop(next(iter(self._call_meta_by_id)), None)
            return web.json_response({"ok": True})
        if event_type == "message.received":
            return await self._on_mail_received(envelope)
        if event_type == "text.received":
            return await self._on_text_received(envelope)
        if event_type == "imessage.received":
            return await self._on_imessage_received(envelope)
        if event_type == "imessage.reaction_received":
            return await self._on_imessage_reaction_received(envelope)
        # Outbound delivery failures: tell the agent its message didn't land so
        # it can retry or reach the human another way. Only hard failures wake
        # the agent; text.delivery_unconfirmed is telemetry, not a failure.
        if event_type == "text.delivery_failed":
            return await self._on_text_delivery_failed(envelope)
        if event_type == "imessage.delivery_failed":
            return await self._on_imessage_delivery_failed(envelope)
        if event_type in ("message.bounced", "message.failed"):
            return await self._on_mail_delivery_failed(envelope, event_type)
        if event_type == "text.delivery_unconfirmed":
            logger.debug("[bridge] text.delivery_unconfirmed (telemetry) — not waking agent")
            return web.json_response({"ok": True, "ignored": event_type})
        # Other delivery lifecycle (text.sent/delivered, imessage.sent/...) is
        # logged without waking the agent, matching the hermes plugin.
        logger.debug("[bridge] lifecycle event %s", event_type)
        return web.json_response({"ok": True, "ignored": event_type})

    @staticmethod
    def _thread_key(prefix: str, value: Any) -> Optional[str]:
        raw = str(value or "").strip()
        return f"{prefix}:{raw}" if raw else None

    @staticmethod
    def _chat_key(
        data: Dict[str, Any],
        fallback: str,
        thread_key: Optional[str] = None,
        contact: Optional[Dict[str, Any]] = None,
        *,
        allow_webhook_contact: bool = True,
    ) -> str:
        # Webhook payloads carry resolved contacts — key the session by
        # contact id so email/SMS/iMessage/voice converge on one session. If
        # Inkbox cannot resolve a contact, keep channel conversations stable
        # before falling back to the raw address/number.
        if contact and contact.get("id"):
            return str(contact["id"])
        if allow_webhook_contact:
            contacts = data.get("contacts") or []
            if len(contacts) == 1:
                contact_id = (
                    contacts[0].get("id")
                    or contacts[0].get("contact_id")
                    or contacts[0].get("contactId")
                )
                if contact_id:
                    return str(contact_id)
        if thread_key:
            return thread_key
        return fallback

    @staticmethod
    def _field(obj: Any, *names: str) -> Any:
        """Read a field from either an SDK object or webhook dict."""
        if obj is None:
            return None
        for name in names:
            if isinstance(obj, dict):
                value = obj.get(name)
            else:
                value = getattr(obj, name, None)
            if value not in (None, ""):
                return value
        return None

    @classmethod
    def _webhook_list(cls, obj: Any, *names: str) -> List[Any]:
        if obj is None:
            return []
        for name in names:
            value = obj.get(name) if isinstance(obj, dict) else getattr(obj, name, None)
            if isinstance(value, (list, tuple)):
                return list(value)
        return []

    @classmethod
    def _string_list_field(cls, obj: Any, *names: str) -> List[str]:
        values = cls._webhook_list(obj, *names)
        return [str(value).strip() for value in values if str(value).strip()]

    @classmethod
    def _conversation_summary_is_group(cls, summary: Any) -> bool:
        return bool(cls._field(summary, "isGroup", "is_group", "is_group_conversation"))

    @classmethod
    def _call_context_id(cls, call_context: Dict[str, Any]) -> str:
        return str(cls._field(call_context, "id", "call_id", "callId") or "").strip()

    @classmethod
    def _merge_call_context(
        cls, primary: Dict[str, Any], fallback: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        merged = dict(fallback or {})
        for key, value in (primary or {}).items():
            if value not in (None, "", [], {}):
                merged[key] = value
        return merged

    @classmethod
    def _contact_values(cls, entries: Any) -> List[str]:
        if not entries:
            return []
        if isinstance(entries, str):
            rows = [entries]
        elif isinstance(entries, (list, tuple)):
            rows = list(entries)
        else:
            rows = [entries]
        rows.sort(
            key=lambda item: not bool(cls._field(item, "is_primary", "isPrimary")),
        )
        values: List[str] = []
        for item in rows:
            value = item if isinstance(item, str) else cls._field(item, "value", "address", "email", "phone")
            if value:
                values.append(str(value))
        return values

    @classmethod
    def _contact_summary(cls, contact: Any) -> Optional[Dict[str, Any]]:
        if not contact:
            return None
        given = cls._field(contact, "given_name", "givenName")
        family = cls._field(contact, "family_name", "familyName")
        full_name = " ".join(str(part) for part in (given, family) if part).strip()
        name = (
            cls._field(contact, "preferred_name", "preferredName")
            or cls._field(contact, "name", "display_name", "displayName")
            or full_name
            or None
        )
        summary = {
            "id": str(cls._field(contact, "id", "contact_id", "contactId") or ""),
            "name": str(name) if name else None,
            "emails": cls._contact_values(
                cls._field(
                    contact,
                    "emails",
                    "email_addresses",
                    "emailAddresses",
                    "email",
                    "email_address",
                    "emailAddress",
                )
            ),
            "phones": cls._contact_values(
                cls._field(
                    contact,
                    "phones",
                    "phone_numbers",
                    "phoneNumbers",
                    "phone",
                    "phone_number",
                    "phoneNumber",
                )
            ),
            "company": cls._field(contact, "company_name", "companyName", "company"),
            "job_title": cls._field(contact, "job_title", "jobTitle", "title"),
            "notes": ((str(cls._field(contact, "notes") or "")[:200]).strip() or None),
        }
        if any(summary.get(key) for key in ("id", "name", "emails", "phones")):
            return summary
        return None

    async def _hydrate_contact(self, contact: Any) -> Optional[Dict[str, Any]]:
        summary = self._contact_summary(contact)
        contact_id = (summary or {}).get("id")
        if not contact_id or self._inkbox is None:
            return summary
        try:
            return self._contact_summary(await asyncio.to_thread(self._inkbox.contacts.get, contact_id)) or summary
        except Exception:
            return summary

    async def _resolve_contact_full(
        self, *, kind: str, value: str
    ) -> Optional[Dict[str, Any]]:
        if not value:
            return None
        cache_key = (kind, value.lower())
        now = time.time()
        cached = self._contact_cache.get(cache_key)
        if cached and cached[1] > now:
            return cached[0]

        if self._inkbox is None:
            return None
        try:
            matches = await asyncio.to_thread(self._inkbox.contacts.lookup, **{kind: value})
        except Exception:
            logger.debug("[bridge] contacts.lookup(%s=%s) failed", kind, value, exc_info=True)
            self._contact_cache[cache_key] = (None, now + CONTACT_CACHE_TTL_SECONDS)
            return None
        if len(matches) != 1:
            self._contact_cache[cache_key] = (None, now + CONTACT_CACHE_TTL_SECONDS)
            return None
        contact = self._contact_summary(matches[0])
        self._contact_cache[cache_key] = (contact, now + CONTACT_CACHE_TTL_SECONDS)
        return contact

    async def _resolve_call_contact(
        self, call_context: Dict[str, Any], remote: str
    ) -> Optional[Dict[str, Any]]:
        """Resolve the call's remote party before Realtime greets."""
        direct = (
            call_context.get("contact")
            or call_context.get("remote_contact")
            or call_context.get("remoteContact")
        )
        if direct:
            return await self._hydrate_contact(direct)

        contact_id = self._field(
            call_context, "contact_id", "contactId", "remote_contact_id", "remoteContactId"
        )
        if contact_id:
            return await self._hydrate_contact({
                "id": contact_id,
                "name": self._field(
                    call_context, "contact_name", "contactName", "remote_name", "remoteName"
                ),
            })

        contacts = (
            call_context.get("contacts")
            or call_context.get("contact_list")
            or call_context.get("contactList")
            or []
        )
        if isinstance(contacts, dict):
            contacts = [contacts]
        if len(contacts) == 1:
            return await self._hydrate_contact(contacts[0])
        for entry in contacts:
            bucket = str(self._field(entry, "bucket", "role", "type") or "").lower()
            if bucket in {"from", "remote", "caller", "callee", "to"} and self._field(
                entry, "id", "contact_id", "contactId"
            ):
                return await self._hydrate_contact(entry)

        if not remote or self._inkbox is None:
            return None
        try:
            matches = await asyncio.to_thread(self._inkbox.contacts.lookup, phone=remote)
        except Exception:
            logger.debug("[bridge] contacts.lookup(phone=%s) failed for call", remote, exc_info=True)
            return None
        if len(matches) != 1:
            return None
        return self._contact_summary(matches[0])

    async def _on_mail_received(self, envelope: Dict[str, Any]) -> "web.Response":
        data = envelope.get("data") or {}
        message = data.get("message") or {}
        sender = str(message.get("from_address") or "").strip()
        if not sender or sender.lower() in self._self_addresses:
            return web.json_response({"ok": True, "ignored": "self"})
        if not self._sender_allowed(sender):
            return web.json_response({"ok": True, "ignored": "sender-not-allowed"})

        subject = str(message.get("subject") or "")
        body_text = await asyncio.to_thread(self._fetch_mail_body, message)
        if message.get("has_attachments"):
            saved = await self._fetch_mail_attachments(message)
            body_text = (body_text + inbound_media_note(saved)).strip()
        thread_key = self._thread_key("email", message.get("thread_id"))
        contact = await self._resolve_contact_full(kind="email", value=sender)
        chat_id = self._chat_key(
            data,
            sender,
            thread_key,
            contact=contact,
            allow_webhook_contact=False,
        )
        meta = {
            "to": sender,
            "sender": sender,
            "subject": subject,
            "thread_id": message.get("thread_id"),
            "contact": contact,
        }
        # The channel tag (Subject included) is added by frame_inbound.
        await self.sessions.get(chat_id).handle_inbound(body_text, "email", meta)
        return web.json_response({"ok": True})

    async def _fetch_mail_attachments(self, message: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Fetch + download an inbound email's attachments, best-effort.

        Email webhooks only carry ``has_attachments``; the file list and signed
        URLs come from the message detail + per-attachment endpoint.

        Args:
            message (dict): The inbound message object from the webhook.

        Returns:
            list[dict]: Saved attachments ({path, content_type, size}); empty on
            any failure.
        """
        msg_id = str(message.get("id") or "")
        email = getattr(self._identity, "email_address", None)
        if not msg_id or not email:
            return []
        try:
            detail = await asyncio.to_thread(self._identity.get_message, msg_id)
            metadata = list(getattr(detail, "attachment_metadata", None) or [])
        except Exception:
            logger.debug("[bridge] attachment metadata fetch failed", exc_info=True)
            return []

        items: List[Dict[str, Any]] = []
        for att in metadata:
            filename = att.get("filename") if isinstance(att, dict) else getattr(att, "filename", None)
            if not filename:
                continue
            try:
                # Mint a signed URL per attachment (mirrors identity.get_message).
                signed = await asyncio.to_thread(
                    self._inkbox._messages.get_attachment, email, msg_id, filename
                )
            except Exception:
                logger.debug("[bridge] attachment URL fetch failed for %s", filename, exc_info=True)
                continue
            url = signed.get("url") if isinstance(signed, dict) else None
            if url:
                ctype = att.get("content_type") if isinstance(att, dict) else None
                items.append({"url": url, "content_type": ctype, "size": None})
        return await download_media(items, prefix=f"mail-{msg_id}")

    def _fetch_mail_body(self, message: Dict[str, Any]) -> str:
        # The webhook only carries a snippet; pull the full body when we can.
        try:
            detail = self._identity.get_message(str(message.get("id")))
            for attr in ("body_text", "text_body", "body"):
                value = getattr(detail, attr, None)
                if value:
                    return str(value)
        except Exception:
            logger.debug("[bridge] full-body fetch failed; using snippet", exc_info=True)
        return str(message.get("snippet") or "")

    async def _lookup_text_conversation_summary(self, conversation_id: str) -> Any:
        if not conversation_id:
            return None

        def _lookup() -> Any:
            identity = self._identity
            if identity is None and self._inkbox is not None:
                identity = self._inkbox.get_identity(self.cfg.identity)
            if identity is None:
                return None
            method = getattr(identity, "list_text_conversations", None)
            if callable(method):
                try:
                    conversations = method(limit=200, offset=0, include_groups=True)
                except TypeError:
                    conversations = method({"limit": 200, "offset": 0, "includeGroups": True})
            else:
                method = getattr(identity, "listTextConversations", None)
                if not callable(method):
                    return None
                conversations = method({"limit": 200, "offset": 0, "includeGroups": True})
            for entry in conversations or []:
                if str(self._field(entry, "id", "conversation_id", "conversationId") or "") == conversation_id:
                    return entry
            return None

        try:
            return await asyncio.to_thread(_lookup)
        except Exception:
            logger.debug(
                "[bridge] text conversation summary lookup failed for %s",
                conversation_id,
                exc_info=True,
            )
            return None

    @classmethod
    def _group_sms_prompt(
        cls,
        body: str,
        *,
        sender: str,
        conversation_id: str,
        local_phone: str,
        participants: List[str],
        contact: Optional[Dict[str, Any]] = None,
    ) -> str:
        marker_parts = [
            f"[inkbox:group_sms conversation_id={conversation_id or 'unknown'}",
            f"from={sender}",
            f"local={local_phone}" if local_phone else None,
            f"participants={','.join(participants)}" if participants else None,
            "reply_mode=conversation_id",
            f"| {contact_marker(contact)}]",
        ]
        marker = " ".join(part for part in marker_parts if part)
        policy = "\n".join([
            "Group SMS response policy: you receive every message in this group so you can track context.",
            "Reply only when the latest message clearly addresses this Inkbox agent, asks it to act, or a visible answer would be expected from the agent.",
            "Treat ordinary group chatter as context only.",
            "If no visible reply is warranted, return exactly [SILENT].",
        ])
        return "\n".join(part for part in [marker, policy, body] if part)

    @classmethod
    def _imessage_reaction_prompt(
        cls,
        *,
        sender: str,
        conversation_id: str,
        target_message_id: str,
        reaction_label: str,
        contact: Optional[Dict[str, Any]] = None,
    ) -> str:
        conversation_part = f" conversation_id={conversation_id}" if conversation_id else ""
        target_part = f" target_message_id={target_message_id}" if target_message_id else ""
        marker = (
            f"[inkbox:imessage_reaction from={sender} reaction={reaction_label}"
            f"{conversation_part}{target_part} | {contact_marker(contact)}]"
        )
        policy = "\n".join([
            f"{sender} reacted with a '{reaction_label}' tapback to your message.",
            "A reaction is a lightweight signal, not always a request for a reply.",
            "Reply only when the reaction plausibly warrants one - e.g. a 'question' "
            "tapback usually asks for clarification or a follow-up, 'emphasize' may "
            "invite one, while 'love'/'like'/'laugh'/'dislike' are usually just "
            "acknowledgements that need no response.",
            "If no visible reply is warranted, return exactly [SILENT].",
        ])
        return f"{marker}\n{policy}"

    async def _on_text_received(self, envelope: Dict[str, Any]) -> "web.Response":
        data = envelope.get("data") or {}
        message = data.get("text_message") or {}
        message_id = str(message.get("id") or "").strip()
        event_key = f"text:{message_id}" if message_id else ""
        if self._dedup_begin(event_key):
            return web.json_response({"ok": True, "deduped": True})
        try:
            response = await self._on_text_received_once(envelope)
        except Exception:
            self._dedup_rollback(event_key)
            raise
        self._dedup_commit(event_key)
        return response

    async def _on_text_received_once(self, envelope: Dict[str, Any]) -> "web.Response":
        data = envelope.get("data") or {}
        message = data.get("text_message") or {}
        if message.get("direction") == "outbound":
            return web.json_response({"ok": True, "ignored": "outbound"})
        sender = str(
            message.get("sender_phone_number") or message.get("remote_phone_number") or ""
        ).strip()
        text = str(message.get("text") or "").strip()
        media = message.get("media") or []
        # An MMS can be media-only (no text) — still wake the agent for it.
        if not sender or (not text and not media):
            return web.json_response({"ok": True, "ignored": "empty"})
        if text.lower() in SMS_CONTROL_WORDS:
            # Carrier keywords (STOP/START/HELP/...) are acked by Inkbox.
            return web.json_response({"ok": True, "ignored": "control-word"})
        if not self._sender_allowed(sender):
            return web.json_response({"ok": True, "ignored": "sender-not-allowed"})

        body = await self._with_media(text, media, prefix=f"sms-{message.get('id', '')}")
        conversation_id = str(
            message.get("conversation_id") or message.get("conversationId") or ""
        ).strip()
        local_phone = str(
            message.get("local_phone_number") or message.get("localPhoneNumber") or ""
        ).strip()
        conversation_summary = await self._lookup_text_conversation_summary(conversation_id)
        participants: List[str] = []
        for entry in (
            self._string_list_field(conversation_summary, "participants")
            + self._string_list_field(message, "participants")
        ):
            if entry not in participants:
                participants.append(entry)
        contacts = self._webhook_list(data, "contacts", "contact_list")
        agent_identities = self._webhook_list(
            data,
            "agent_identities",
            "agentIdentities",
            "identity_agents",
        )
        is_group = (
            self._conversation_summary_is_group(conversation_summary)
            or bool(self._field(message, "isGroup", "is_group"))
            or len(participants) > 1
            or len(contacts) > 1
            or len(agent_identities) > 1
        )
        contact = await self._resolve_contact_full(kind="phone", value=sender)
        if is_group:
            body = self._group_sms_prompt(
                body,
                sender=sender,
                conversation_id=conversation_id,
                local_phone=local_phone,
                participants=participants,
                contact=contact,
            )
        thread_key = self._thread_key("sms", conversation_id)
        chat_id = self._chat_key(
            data,
            sender,
            thread_key,
            contact=contact,
            allow_webhook_contact=False,
        )
        meta = {
            "conversation_id": conversation_id or None,
            "to": sender,
            "sender": sender,
            "conversation_kind": "group" if is_group else "direct",
            "contact": contact,
        }
        await self.sessions.get(chat_id).handle_inbound(body, "sms", meta)
        return web.json_response({"ok": True})

    async def _on_imessage_received(self, envelope: Dict[str, Any]) -> "web.Response":
        data = envelope.get("data") or {}
        message = data.get("message") or {}
        message_id = str(message.get("id") or "").strip()
        event_key = f"imessage:{message_id}" if message_id else ""
        if self._dedup_begin(event_key):
            return web.json_response({"ok": True, "deduped": True})
        try:
            response = await self._on_imessage_received_once(envelope)
        except Exception:
            self._dedup_rollback(event_key)
            raise
        self._dedup_commit(event_key)
        return response

    async def _on_imessage_received_once(self, envelope: Dict[str, Any]) -> "web.Response":
        data = envelope.get("data") or {}
        message = data.get("message") or {}
        if not message or message.get("direction") == "outbound":
            return web.json_response({"ok": True, "ignored": "outbound-or-reaction"})
        sender = str(message.get("remote_number") or "").strip()
        text = str(message.get("content") or "").strip()
        media = message.get("media") or []
        if not sender or (not text and not media):
            return web.json_response({"ok": True, "ignored": "empty"})
        if not self._sender_allowed(sender):
            return web.json_response({"ok": True, "ignored": "sender-not-allowed"})

        body = await self._with_media(text, media, prefix=f"imsg-{message.get('id', '')}")
        conversation_id = str(message.get("conversation_id") or "").strip()
        contact = await self._resolve_contact_full(kind="phone", value=sender)
        chat_id = self._chat_key(
            data,
            sender,
            self._thread_key("imessage", conversation_id),
            contact=contact,
            allow_webhook_contact=False,
        )
        meta = {"conversation_id": conversation_id or None, "sender": sender, "contact": contact}
        await self.sessions.get(chat_id).handle_inbound(body, "imessage", meta)
        return web.json_response({"ok": True})

    async def _on_imessage_reaction_received(self, envelope: Dict[str, Any]) -> "web.Response":
        data = envelope.get("data") or {}
        reaction = data.get("reaction") or {}
        reaction_id = str(reaction.get("id") or "").strip()
        event_key = f"imessage_reaction:{reaction_id}" if reaction_id else ""
        if self._dedup_begin(event_key):
            return web.json_response({"ok": True, "deduped": True})
        try:
            direction = str(reaction.get("direction") or "").strip().lower()
            if direction and direction != "inbound":
                response = web.json_response({"ok": True, "ignored": "outbound-reaction"})
            else:
                sender = str(reaction.get("remote_number") or "").strip()
                if not sender:
                    response = web.json_response({"ok": True, "ignored": "empty"})
                elif not self._sender_allowed(sender):
                    response = web.json_response({"ok": True, "ignored": "sender-not-allowed"})
                else:
                    conversation_id = str(reaction.get("conversation_id") or "").strip()
                    target_message_id = str(reaction.get("target_message_id") or "").strip()
                    reaction_type = str(reaction.get("reaction") or "").strip().lower()
                    custom_emoji = str(reaction.get("custom_emoji") or "").strip()
                    reaction_label = (
                        f"{reaction_type}:{custom_emoji}"
                        if reaction_type == "custom" and custom_emoji
                        else reaction_type
                    ) or "unknown"
                    contact = await self._resolve_contact_full(kind="phone", value=sender)
                    body = self._imessage_reaction_prompt(
                        sender=sender,
                        conversation_id=conversation_id,
                        target_message_id=target_message_id,
                        reaction_label=reaction_label,
                        contact=contact,
                    )
                    chat_id = self._chat_key(
                        data,
                        sender,
                        self._thread_key("imessage", conversation_id),
                        contact=contact,
                        allow_webhook_contact=False,
                    )
                    meta = {
                        "conversation_id": conversation_id or None,
                        "sender": sender,
                        "message_id": reaction_id or target_message_id,
                        "reply_to_id": target_message_id or reaction_id,
                        "reaction": reaction_label,
                        "typing": reaction_label == "question",
                        "contact": contact,
                    }
                    await self.sessions.get(chat_id).handle_inbound(body, "imessage", meta)
                    response = web.json_response({"ok": True})
        except Exception:
            self._dedup_rollback(event_key)
            raise
        self._dedup_commit(event_key)
        return response

    async def _with_media(self, text: str, media: List[Dict[str, Any]], *, prefix: str) -> str:
        """Download inbound media and append a note pointing Claude at the files.

        Args:
            text (str): The message text (may be empty for media-only messages).
            media (list): The webhook's media items ({url, content_type, size}).
            prefix (str): Filename prefix for the saved files.

        Returns:
            str: The text with a saved-attachments note appended (or just the
            note when the message had no text).
        """
        if not media:
            return text
        saved = await download_media(media, prefix=prefix)
        return (text + inbound_media_note(saved)).strip()

    # ------------------------------------------------------------------
    # Outbound delivery failures
    # ------------------------------------------------------------------

    # --- Outbound context: what we sent, so we can recover it if it fails ----

    def _record_outbound(self, message_id: str, ctx: _OutboundContext) -> None:
        """Remember an outbound send, keyed by its Inkbox message id.

        Called after a bridge send queues successfully. A later delivery-failure
        webhook correlates on the id to route recovery back to this exact
        session/thread. Bounded LRU + TTL so it can't grow without limit.
        """
        if not message_id:
            return
        ctx.at = time.time()
        self._outbound_by_id[message_id] = ctx
        self._outbound_by_id.move_to_end(message_id)
        while len(self._outbound_by_id) > OUTBOUND_CONTEXT_MAX:
            self._outbound_by_id.popitem(last=False)

    def _lookup_outbound(self, message_id: str) -> Optional[_OutboundContext]:
        """Return the recorded context for a message id, expiring stale entries."""
        now = time.time()
        for key, ctx in list(self._outbound_by_id.items()):
            if now - ctx.at > OUTBOUND_CONTEXT_TTL_SECONDS:
                self._outbound_by_id.pop(key, None)
        return self._outbound_by_id.get(message_id) if message_id else None

    # --- Dedup + loop protection ---------------------------------------------

    @staticmethod
    def _payload_hash(payload: Any) -> str:
        """Stable short hash of a webhook payload, for dedup when it has no id."""
        try:
            blob = json.dumps(payload, sort_keys=True, default=str)
        except Exception:
            blob = str(payload)
        return hashlib.sha256(blob.encode("utf-8", "replace")).hexdigest()[:16]

    def _dedup_failure(self, channel: str, event_type: str, message_id: str, payload: Any) -> bool:
        """True if this failure was already handled recently.

        Keyed by channel + event type + message id so a webhook retry (or a
        second failure event for the same message) doesn't re-notify. When the
        payload carries no id, fall back to a hash of the payload itself.
        """
        now = time.time()
        for key, seen_at in list(self._notified_failures.items()):
            if now - seen_at > WEBHOOK_DEDUP_TTL_SECONDS:
                self._notified_failures.pop(key, None)
        if message_id:
            key = f"{channel}:{event_type}:{message_id}"
        else:
            key = f"{channel}:{event_type}:hash:{self._payload_hash(payload)}"
        if key in self._notified_failures:
            return True
        self._notified_failures[key] = now
        return False

    def _recovery_allowed(self, session_key: str) -> bool:
        """Cap recovery turns per contact inside the dedup window.

        Belt-and-suspenders on top of the per-message recovery flag: even if a
        channel produces a stream of distinct failing message ids, we stop
        waking the agent once we've spun MAX_RECOVERIES_PER_WINDOW for it.
        """
        now = time.time()
        count, first_at = self._recovery_counts.get(session_key, (0, now))
        if now - first_at > WEBHOOK_DEDUP_TTL_SECONDS:
            count, first_at = 0, now  # window elapsed → start fresh
        if count >= MAX_RECOVERIES_PER_WINDOW:
            return False
        self._recovery_counts[session_key] = (count + 1, first_at)
        return True

    @staticmethod
    def _recovery_meta(
        mode: str, recipient: str, conversation_id: str, thread_id: str, subject: str
    ) -> Dict[str, Any]:
        """Reply routing for a recovery turn, matching send_to_contact's keys.

        ``recovery`` is threaded through so the eventual send is tagged as a
        recovery attempt — a recovery send that itself fails won't spawn another.
        """
        meta: Dict[str, Any] = {"recovery": True}
        if mode == "email":
            if recipient:
                meta["to"] = recipient
            if subject:
                meta["subject"] = subject
            return meta
        # sms / imessage
        if conversation_id:
            meta["conversation_id"] = conversation_id
        if mode == "sms" and recipient:
            meta["to"] = recipient
        return meta

    # --- The recovery entry point --------------------------------------------

    async def _wake_for_failure(
        self,
        *,
        data: Dict[str, Any],
        message_id: str,
        event_type: str,
        channel: str,
        mode: str,
        recipient: str,
        body: str,
        reason: str,
        conversation_id: str = "",
        thread_id: str = "",
        subject: str = "",
    ) -> "web.Response":
        """Correlate a failed outbound send and wake its session to recover.

        The recovery turn's final reply is delivered on the SAME channel/thread
        by default, so the agent can retry by simply rewriting the message,
        switch channels with its Inkbox tools, or stay silent with ``[SILENT]``.
        """
        if self._dedup_failure(channel, event_type, message_id, data):
            return web.json_response({"ok": True, "deduped": True})

        # Correlate to the original send. Outbound context (keyed by the id we
        # sent) wins — it names the exact session + thread even when the webhook
        # can't resolve a contact — and enriches anything the webhook omitted.
        ctx = self._lookup_outbound(message_id)
        if ctx is not None:
            session_key = ctx.session_key
            recipient = recipient or ctx.recipient
            body = body or ctx.body
            conversation_id = conversation_id or ctx.conversation_id
            thread_id = thread_id or ctx.thread_id
            subject = subject or ctx.subject
            if ctx.recovery:
                logger.info(
                    "[bridge] %s recovery send failed again (%s) — not looping",
                    channel, recipient,
                )
                return web.json_response({"ok": True, "recovery_exhausted": True})
        else:
            thread_key = self._thread_key(mode, conversation_id or thread_id)
            session_key = self._chat_key(data, recipient, thread_key)

        if not session_key:
            logger.warning(
                "[bridge] %s delivery failed but no session/thread resolved (%s) — dropping",
                channel, recipient,
            )
            return web.json_response({"ok": True, "unresolved": True})

        if not self._recovery_allowed(session_key):
            logger.warning(
                "[bridge] recovery cap reached for %s — not waking again", session_key
            )
            return web.json_response({"ok": True, "recovery_capped": True})

        if self.sessions is None:
            return web.json_response({"ok": True, "ignored": "no-sessions"})

        meta = self._recovery_meta(mode, recipient, conversation_id, thread_id, subject)
        prompt = _delivery_failure_prompt(channel, recipient, body, reason)
        logger.info(
            "[bridge] %s delivery failed to %s: %s — waking %s",
            channel, recipient, reason, session_key,
        )
        # Run in the background so the webhook returns promptly; the turn can
        # take a while (the agent may rework the message or switch channels).
        asyncio.create_task(
            self._run_failure_turn(session_key, prompt, mode, meta, channel, recipient)
        )
        return web.json_response({"ok": True})

    async def _run_failure_turn(
        self, session_key: str, prompt: str, mode: str, meta: Dict[str, Any],
        channel: str, recipient: str,
    ) -> None:
        try:
            await self.sessions.get(session_key).run_recovery(prompt, mode, meta)
        except Exception:
            logger.exception(
                "[bridge] delivery-failure recovery turn failed: %s → %s", channel, recipient
            )

    async def _on_text_delivery_failed(self, envelope: Dict[str, Any]) -> "web.Response":
        data = envelope.get("data") or {}
        message = data.get("text_message") or {}
        # Prefer the human detail; fall back to the carrier code, then a default.
        reason = str(message.get("error_detail") or message.get("error_code") or "").strip()
        conversation_id = str(message.get("conversation_id") or message.get("conversationId") or "").strip()
        return await self._wake_for_failure(
            data=data,
            message_id=str(message.get("id") or ""),
            event_type="text.delivery_failed",
            channel="SMS",
            mode="sms",
            recipient=str(message.get("remote_phone_number") or "").strip(),
            body=str(message.get("text") or "").strip(),
            reason=reason or "the carrier rejected the message",
            conversation_id=conversation_id,
        )

    async def _on_imessage_delivery_failed(self, envelope: Dict[str, Any]) -> "web.Response":
        data = envelope.get("data") or {}
        message = data.get("message") or {}
        reason = str(
            message.get("error_detail")
            or message.get("error_reason")
            or message.get("error_message")
            or message.get("status")
            or ""
        ).strip()
        conversation_id = str(message.get("conversation_id") or message.get("conversationId") or "").strip()
        return await self._wake_for_failure(
            data=data,
            message_id=str(message.get("id") or ""),
            event_type="imessage.delivery_failed",
            channel="iMessage",
            mode="imessage",
            recipient=str(message.get("remote_number") or "").strip(),
            body=str(message.get("content") or "").strip(),
            reason=reason or "the iMessage could not be delivered",
            conversation_id=conversation_id,
        )

    async def _on_mail_delivery_failed(self, envelope: Dict[str, Any], event_type: str) -> "web.Response":
        data = envelope.get("data") or {}
        message = data.get("message") or {}
        to_addresses = message.get("to_addresses") or []
        subject = str(message.get("subject") or "").strip()
        reason = "bounced" if event_type == "message.bounced" else "permanent send failure"
        body = f"(email, subject: {subject})" if subject else ""
        return await self._wake_for_failure(
            data=data,
            message_id=str(message.get("id") or ""),
            event_type=event_type,
            channel="email",
            mode="email",
            recipient=str(to_addresses[0] if to_addresses else "").strip(),
            body=body,
            reason=reason,
            thread_id=str(message.get("thread_id") or "").strip(),
            subject=subject,
        )

    # ------------------------------------------------------------------
    # Inbound: live calls (Inkbox STT/TTS text-frame bridge)
    # ------------------------------------------------------------------

    async def _open_realtime_bridge(
        self,
        remote: str,
        call_id: str,
        outbound: Optional[Dict[str, Any]] = None,
        contact: Optional[Dict[str, Any]] = None,
        direction: str = "inbound",
    ) -> Any:
        """Preflight an OpenAI Realtime session for an incoming call.

        Args:
            remote (str): Caller phone number (may be empty).
            call_id (str): Inkbox call id, for logging.

        Returns:
            Any: An OpenedRealtimeBridge on success, or None if the connect
            failed (the caller then falls back to Inkbox STT/TTS).
        """
        identity = self._identity
        mailbox = getattr(identity, "mailbox", None)
        phone = getattr(identity, "phone_number", None)
        oc = outbound or {}
        contact = contact or {}
        meta = RealtimeCallMeta(
            call_id=call_id or "unknown",
            remote_phone_number=remote or None,
            direction=direction or "inbound",
            agent_identity_handle=(
                getattr(identity, "agent_handle", None)
                or getattr(identity, "handle", None)
                or self.cfg.identity
                or None
            ),
            agent_identity_email=(
                getattr(mailbox, "email_address", None)
                or getattr(identity, "email_address", None)
            ),
            agent_identity_phone=(
                getattr(phone, "number", None)
                if not isinstance(phone, str)
                else phone
            ),
            project_dir=self.cfg.project_dir,
            contact_known=bool(contact.get("id")),
            contact_id=contact.get("id"),
            contact_name=contact.get("name"),
            contact_emails=list(contact.get("emails") or []),
            contact_phones=list(contact.get("phones") or []),
            contact_company=contact.get("company"),
            contact_job_title=contact.get("job_title"),
            contact_notes=contact.get("notes"),
            outbound_purpose=(oc.get("purpose") or None),
            outbound_opening=(oc.get("opening_message") or None),
            outbound_context=(oc.get("context") or None),
            outbound_reason=(oc.get("reason") or None),
            outbound_scheduled_by=(oc.get("scheduled_by") or None),
            outbound_conversation_summary=(oc.get("conversation_summary") or None),
        )
        try:
            return await open_inkbox_realtime_bridge(config=self.cfg.realtime, meta=meta)
        except RealtimeBridgeConnectError as exc:
            logger.warning(
                "[bridge] realtime connect failed for call %s (%s); "
                "falling back to Inkbox STT/TTS unless disabled",
                call_id, exc.cause,
            )
            return None

    @staticmethod
    def _load_outbound_context(token: Optional[str]) -> Optional[Dict[str, Any]]:
        """Load the purpose/opening an outbound call was placed with."""
        token = (token or "").strip()
        # Token rides in off the URL; never let it escape the contexts dir.
        if not token or "/" in token or "\\" in token or token in {".", ".."}:
            return None
        path = call_contexts_dir() / f"{token}.json"
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            return None

    async def _handle_call_ws(self, request: "web.Request") -> Any:
        # The tunnel URL is internet-reachable; Inkbox signs the WS upgrade
        # with the webhook scheme over the X-Call-Context header body.
        call_context_raw = request.headers.get("X-Call-Context", "") or ""
        if self.cfg.require_signature:
            ok = verify_webhook(
                payload=call_context_raw.encode(),
                headers=dict(request.headers),
                secret=self.cfg.signing_key,
            )
            if not ok:
                return web.Response(status=401, text="invalid signature")

        try:
            call_context = json.loads(call_context_raw) if call_context_raw else {}
        except json.JSONDecodeError:
            call_context = {}
        call_id = self._call_context_id(call_context) or str(request.query.get("call_id") or "").strip()
        stored_call_context = self._call_meta_by_id.pop(call_id, None) if call_id else None
        if stored_call_context:
            call_context = self._merge_call_context(call_context, stored_call_context)
        if call_id and not self._call_context_id(call_context):
            call_context["id"] = call_id
        call_id = self._call_context_id(call_context) or call_id
        outbound = self._load_outbound_context(request.query.get("context_token"))
        remote = str(
            self._field(
                call_context,
                "remote_phone_number",
                "remotePhoneNumber",
                "from_number",
                "fromNumber",
                "to_number",
                "toNumber",
            )
            or (outbound or {}).get("to_number")
            or ""
        ).strip()
        direction = str(
            self._field(call_context, "direction") or ("outbound" if outbound else "inbound")
        ).strip().lower() or "inbound"
        contact = await self._resolve_call_contact(call_context, remote)
        chat_id = (contact or {}).get("id") or remote or f"call:{call_id}"

        ws = web.WebSocketResponse()

        # Realtime branch: when configured, pre-open OpenAI Realtime BEFORE we
        # commit the WS to a mode. If it connects, accept in raw-media mode and
        # bridge audio both ways; the model runs the call and consults Claude
        # Code via run_consult. If the preflight fails, fall through to Inkbox
        # STT/TTS below (unless fallback is disabled, then refuse the call).
        if self.cfg.realtime.enabled:
            bridge = await self._open_realtime_bridge(remote, call_id, outbound, contact, direction)
            if bridge is None and not self.cfg.realtime.fallback_to_inkbox_stt_tts:
                return web.Response(status=503, text="realtime bridge unavailable")
            if bridge is not None:
                # Raw-media mode: Inkbox must NOT run its own STT/TTS — the
                # OpenAI model handles both ends of the audio.
                ws.headers["x-use-inkbox-speech-to-text"] = "false"
                ws.headers["x-use-inkbox-text-to-speech"] = "false"
                await ws.prepare(request)
                self._active_call_ws[chat_id] = ws
                logger.info("[bridge] realtime call connected: %s", chat_id or call_id)

                async def _consult(query: str, _transcript: Any) -> str:
                    # Route the model's request into the caller's shared session.
                    return await self.sessions.get(chat_id).run_consult(query)

                async def _post_call(actions: List[Dict[str, str]], transcript: Any) -> None:
                    # Run the queued after-call work in the caller's session. The
                    # text reply is discarded; side effects (emails, edits, PRs)
                    # happen via Claude's tools during the turn.
                    prompt = _post_call_prompt(actions, transcript)
                    await self.sessions.get(chat_id).run_consult(prompt)

                async def _call_ended(transcript: Any) -> None:
                    # No queued actions: let Claude reflect and do any follow-up
                    # it committed to on the call. Stays silent if nothing to do.
                    prompt = _call_ended_prompt(transcript)
                    await self.sessions.get(chat_id).run_consult(prompt)

                try:
                    await bridge.run(
                        inkbox_ws=ws,
                        on_agent_consult=_consult,
                        on_post_call_actions=_post_call,
                        on_call_ended=_call_ended,
                    )
                except Exception:
                    logger.exception("[bridge] realtime call failed: %s", call_id)
                finally:
                    await bridge.close()
                    self._active_call_ws.pop(chat_id, None)
                    logger.info("[bridge] realtime call ended: %s", chat_id or call_id)
                return ws

        # Inkbox STT/TTS path. Tell Inkbox which side runs speech: STT on the
        # caller's audio (so we receive `transcript` events) and TTS on the
        # text frames we send back (so the caller hears the reply). These
        # headers must be set on the upgrade response BEFORE prepare();
        # without them Inkbox defaults to raw media and neither transcripts
        # nor spoken replies flow.
        ws.headers["x-use-inkbox-speech-to-text"] = "true"
        ws.headers["x-use-inkbox-text-to-speech"] = "true"
        await ws.prepare(request)
        self._active_call_ws[chat_id] = ws
        logger.info("[bridge] call connected: %s", chat_id or call_id)
        transcript: List[Tuple[str, str]] = []

        try:
            async for msg in ws:
                if msg.type != WSMsgType.TEXT:
                    continue
                try:
                    payload = json.loads(msg.data)
                except json.JSONDecodeError:
                    continue
                event = payload.get("event")
                if event == "start":
                    await self._speak(ws, "Hey, you've reached Claude. What do you need?", "greeting")
                elif event == "transcript" and payload.get("is_final"):
                    text = str(payload.get("text") or "").strip()
                    if not text:
                        continue
                    transcript.append(("user", text))
                    meta = {
                        "call_id": call_id,
                        "sender": remote,
                        "contact": contact,
                        "direction": direction,
                    }
                    session = self.sessions.get(chat_id)
                    await session.handle_inbound(text, "voice", meta)
                elif event == "stop":
                    break
        finally:
            self._active_call_ws.pop(chat_id, None)
            if transcript:
                prompt = _call_ended_prompt(transcript)
                await self.sessions.get(chat_id).run_consult(prompt)
            logger.info("[bridge] call ended: %s", chat_id or call_id)
        return ws

    @staticmethod
    async def _speak(ws: Any, text: str, turn_id: str) -> None:
        # Two-frame protocol: a delta with the text, then done — the done
        # frame flushes Inkbox's TTS and ends the agent's speaking turn.
        await ws.send_str(json.dumps({"event": "text", "delta": text, "turn_id": turn_id}))
        await ws.send_str(json.dumps({"event": "text", "done": True, "turn_id": turn_id}))

    # ------------------------------------------------------------------
    # Outbound
    # ------------------------------------------------------------------

    async def health_report(self) -> str:
        """Probe Inkbox + Claude readiness for the texted /health command.

        Returns:
            str: A short multi-line health summary for the human.
        """
        lines = []

        # Inkbox: a live identity fetch proves the API is reachable and the key
        # is valid; report which channels are provisioned.
        try:
            identity = await asyncio.to_thread(self._inkbox.get_identity, self.cfg.identity)
            channels = []
            if getattr(identity, "mailbox", None) is not None:
                channels.append("email")
            if getattr(identity, "phone_number", None) is not None:
                channels.append("phone")
            if getattr(identity, "imessage_enabled", False):
                channels.append("iMessage")
            lines.append(
                f"Inkbox: reachable as {identity.agent_handle} "
                f"({', '.join(channels) or 'no channels yet'})"
            )
        except Exception as exc:
            lines.append(f"Inkbox: NOT reachable — {exc}")

        # Inbound path: the tunnel + reconciled webhook subscriptions.
        if self._public_url:
            lines.append(f"Inbound: connected ({self._public_host or self._public_url})")
        else:
            lines.append("Inbound: not connected")

        lines.append(f"Claude: {_claude_health()}")
        return "\n".join(lines)

    async def send_typing(self, chat_id: str, mode: str, meta: Dict[str, Any]) -> None:
        """Show a typing indicator while Claude works on a turn.

        Args:
            chat_id (str): Contact-keyed session id.
            mode (str): Channel the human last used.
            meta (dict): Channel routing details captured on inbound.

        Returns:
            None: No-op for channels without a typing indicator (iMessage only).
        """
        if mode != "imessage":
            return
        conversation_id = (meta or {}).get("conversation_id")
        if not conversation_id:
            return
        try:
            # Reuse the identity fetched at startup — this fires every few
            # seconds, so we don't want a network round trip just to refresh it.
            await asyncio.to_thread(self._identity.send_imessage_typing, str(conversation_id))
        except Exception:
            logger.debug("[bridge] typing indicator failed", exc_info=True)

    async def send_to_contact(
        self, chat_id: str, content: str, mode: str, meta: Dict[str, Any]
    ) -> None:
        """Deliver agent output over the modality the human last used.

        Args:
            chat_id (str): Contact-keyed session id.
            content (str): Reply text from Claude.
            mode (str): email / sms / imessage / voice.
            meta (dict): Channel routing details captured on inbound.

        Returns:
            None
        """
        meta = meta or {}
        if content.strip() == "[SILENT]":
            logger.debug("[bridge] suppressing exact [SILENT] reply for %s", chat_id)
            return
        if mode == "voice":
            ws = self._active_call_ws.get(chat_id)
            if ws is not None:
                await self._speak(ws, strip_markdown(content), str(meta.get("call_id") or ""))
                return
            logger.info(
                "[bridge] dropped late voice reply after call ended: %s",
                chat_id,
            )
            return

        if mode == "sms":
            text = strip_markdown(content)
            if len(text) > SMS_MAX_LENGTH:
                raise ValueError(_message_too_long_reason("SMS", text, SMS_MAX_LENGTH))
            identity = await asyncio.to_thread(self._inkbox.get_identity, self.cfg.identity)
            kwargs: Dict[str, Any] = {"text": text}
            conversation_id = str(meta.get("conversation_id") or "").strip()
            if not conversation_id and str(chat_id).startswith("sms:"):
                conversation_id = str(chat_id).split(":", 1)[1]
            if conversation_id:
                kwargs["conversation_id"] = conversation_id
            else:
                kwargs["to"] = str(meta.get("to") or chat_id)
            msg = await asyncio.to_thread(identity.send_text, **kwargs)
            self._record_outbound(str(getattr(msg, "id", "") or ""), _OutboundContext(
                channel="SMS", mode="sms", session_key=chat_id,
                recipient=str(kwargs.get("to") or meta.get("to") or ""),
                body=text, conversation_id=conversation_id,
                recovery=bool(meta.get("recovery")),
            ))
        elif mode == "imessage":
            text = strip_markdown(content)
            if len(text) > IMESSAGE_MAX_LENGTH:
                raise ValueError(_message_too_long_reason("iMessage", text, IMESSAGE_MAX_LENGTH))
            identity = await asyncio.to_thread(self._inkbox.get_identity, self.cfg.identity)
            conversation_id = str(meta.get("conversation_id") or "").strip()
            if not conversation_id and str(chat_id).startswith("imessage:"):
                conversation_id = str(chat_id).split(":", 1)[1]
            if not conversation_id:
                raise ValueError(f"No iMessage conversation id for chat {chat_id}")
            msg = await asyncio.to_thread(
                identity.send_imessage,
                conversation_id=conversation_id,
                text=text,
            )
            self._record_outbound(str(getattr(msg, "id", "") or ""), _OutboundContext(
                channel="iMessage", mode="imessage", session_key=chat_id,
                recipient=str(meta.get("to") or ""), body=text,
                conversation_id=conversation_id,
                recovery=bool(meta.get("recovery")),
            ))
        else:  # email
            identity = await asyncio.to_thread(self._inkbox.get_identity, self.cfg.identity)
            subject = str(meta.get("subject") or "").strip()
            reply_subject = subject if subject.lower().startswith("re:") else f"Re: {subject}" if subject else "From your Claude Code agent"
            to_address = str(meta.get("to") or chat_id)
            msg = await asyncio.to_thread(
                identity.send_email,
                to=[to_address],
                subject=reply_subject,
                body_text=content,
            )
            self._record_outbound(str(getattr(msg, "id", "") or ""), _OutboundContext(
                channel="email", mode="email", session_key=chat_id,
                recipient=to_address, body=f"(email, subject: {reply_subject})",
                thread_id=str(meta.get("thread_id") or ""), subject=reply_subject,
                recovery=bool(meta.get("recovery")),
            ))
