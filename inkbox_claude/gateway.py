"""Inkbox gateway for Claude Code.

The bridge's runtime core, modeled on the hermes-agent-plugin Inkbox
adapter:

1. On startup, bring up the identity's Inkbox tunnel (or use
   ``INKBOX_PUBLIC_URL``), reconcile webhook subscriptions for the
   identity's mailbox (``message.received``), phone number
   (``text.received``), and — when iMessage-enabled — the identity
   itself (``imessage.received``), and patch the phone number's
   incoming-call channel to auto-accept onto our call WebSocket.
2. Serve ``POST /webhook`` (HMAC-verified) and ``WS /phone/media/ws``.
3. Map every inbound event to a contact-keyed Claude Code session:
   one session per remote party across email + SMS + iMessage + voice.
4. Send Claude's replies back over the modality the human last used,
   stripping markdown for phone-bound channels.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

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
    from .config import DEFAULT_WEBHOOK_PATH, INKBOX_WS_PATH, BridgeConfig, inkbox_client_kwargs
    from .media import download_media, inbound_media_note
    from .prompts import strip_markdown
    from .realtime import (
        RealtimeBridgeConnectError,
        RealtimeCallMeta,
        open_inkbox_realtime_bridge,
    )
    from .sessions import SessionManager
    from .tools import build_inkbox_mcp_server
except ImportError:  # pragma: no cover - direct local import/test fallback
    from config import DEFAULT_WEBHOOK_PATH, INKBOX_WS_PATH, BridgeConfig, inkbox_client_kwargs
    from media import download_media, inbound_media_note
    from prompts import strip_markdown
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
        "This matters — the person did not get what you sent. Decide how to recover:",
        f"- If it looks transient, retry once on {channel} using your Inkbox tools.",
        f"- If {channel} seems broken for them (or already failed on retry), reach "
        "them another way — try a different channel you have for them (SMS, iMessage, "
        "email), and only as a last resort place a call.",
        "Act now via your Inkbox messaging tools. Do not just acknowledge this; the "
        "original channel may be down, so a plain reply here may not reach them.",
    ])


def _call_ended_prompt(transcript: Any) -> str:
    """Build the Claude Code prompt for a no-actions post-call reflection."""
    convo = _format_transcript(transcript)
    parts = [
        "[voice call ended] Your phone call with the operator just ended. If you "
        "committed to anything during it (open a PR, run a task, send a summary), "
        "do that now with your tools. If there's nothing to do, do nothing.",
    ]
    if convo:
        parts += ["", "Recent call transcript:", convo]
    return "\n".join(parts)


WEBHOOK_DEDUP_TTL_SECONDS = 300
SMS_MAX_LENGTH = 1600  # Inkbox SMS hard cap
# Inbound SMS carrier keywords handled entirely by the Inkbox server;
# never wake the agent for them.
SMS_CONTROL_WORDS = {"stop", "start", "help", "unstop", "unsubscribe", "cancel", "end", "quit"}


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
        self._active_call_ws: Dict[str, Any] = {}
        self._call_meta_by_id: Dict[str, Dict[str, Any]] = {}
        # Failed outbound message ids we've already told the agent about, so a
        # webhook retry (or a second failure event for the same message) doesn't
        # re-notify and spin the agent in a loop.
        self._notified_failures: Dict[str, float] = {}

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
            _reconcile({"mailbox_id": identity.mailbox.id}, ["message.received"])
            logger.info("[bridge] mailbox %s → %s", identity.mailbox.email_address, webhook_url)
        if identity.phone_number is not None:
            _reconcile({"phone_number_id": identity.phone_number.id}, ["text.received"])
            # auto_accept: Inkbox answers and opens the call WS directly.
            self._inkbox.phone_numbers.update(
                identity.phone_number.id,
                incoming_call_webhook_url=webhook_url,
                incoming_call_action="auto_accept",
                client_websocket_url=ws_url,
            )
            logger.info("[bridge] phone %s → %s + %s", identity.phone_number.number, webhook_url, ws_url)
        if getattr(identity, "imessage_enabled", False):
            _reconcile({"agent_identity_id": identity.id}, ["imessage.received"])
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

    def _is_duplicate(self, request_id: str) -> bool:
        now = time.time()
        # Opportunistic TTL sweep keeps the dict bounded.
        for key, seen_at in list(self._recent_request_ids.items()):
            if now - seen_at > WEBHOOK_DEDUP_TTL_SECONDS:
                self._recent_request_ids.pop(key, None)
        if request_id and request_id in self._recent_request_ids:
            return True
        if request_id:
            self._recent_request_ids[request_id] = now
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

        if self._is_duplicate(request.headers.get("X-Inkbox-Request-Id", "")):
            return web.json_response({"ok": True, "deduped": True})

        try:
            envelope = json.loads(body)
        except json.JSONDecodeError:
            return web.Response(status=400, text="invalid json")

        event_type = str(envelope.get("event_type") or "")
        if not event_type and envelope.get("direction") == "inbound" and envelope.get("local_phone_number"):
            # Incoming-call payloads are flat (no envelope); with
            # auto_accept this is informational — the WS is the channel.
            return web.json_response({"ok": True})

        if event_type == "message.received":
            return await self._on_mail_received(envelope)
        if event_type == "text.received":
            return await self._on_text_received(envelope)
        if event_type == "imessage.received":
            return await self._on_imessage_received(envelope)
        # Outbound delivery failures: tell the agent its message didn't land so
        # it can retry or reach the human another way.
        if event_type in ("text.delivery_failed", "text.delivery_unconfirmed"):
            return await self._on_text_delivery_failed(envelope, event_type)
        if event_type == "imessage.delivery_failed":
            return await self._on_imessage_delivery_failed(envelope)
        if event_type in ("message.bounced", "message.failed"):
            return await self._on_mail_delivery_failed(envelope, event_type)
        # Other delivery lifecycle (text.sent/delivered, imessage.sent/...) is
        # logged without waking the agent, matching the hermes plugin.
        logger.debug("[bridge] lifecycle event %s", event_type)
        return web.json_response({"ok": True, "ignored": event_type})

    @staticmethod
    def _chat_key(data: Dict[str, Any], fallback: str) -> str:
        # Webhook payloads carry resolved contacts — key the session by
        # contact id so email/SMS/iMessage/voice converge on one session.
        contacts = data.get("contacts") or []
        if len(contacts) == 1 and contacts[0].get("id"):
            return str(contacts[0]["id"])
        return fallback

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
        chat_id = self._chat_key(data, sender)
        meta = {
            "to": sender,
            "sender": sender,
            "subject": subject,
            "thread_id": message.get("thread_id"),
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

    async def _on_text_received(self, envelope: Dict[str, Any]) -> "web.Response":
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
        chat_id = self._chat_key(data, sender)
        meta = {
            "conversation_id": message.get("conversation_id"),
            "to": sender,
            "sender": sender,
        }
        await self.sessions.get(chat_id).handle_inbound(body, "sms", meta)
        return web.json_response({"ok": True})

    async def _on_imessage_received(self, envelope: Dict[str, Any]) -> "web.Response":
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
        chat_id = self._chat_key(data, sender)
        meta = {"conversation_id": message.get("conversation_id"), "sender": sender}
        await self.sessions.get(chat_id).handle_inbound(body, "imessage", meta)
        return web.json_response({"ok": True})

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

    def _already_notified(self, message_id: str) -> bool:
        """True if we've recently told the agent about this failed message id."""
        now = time.time()
        for key, seen_at in list(self._notified_failures.items()):
            if now - seen_at > WEBHOOK_DEDUP_TTL_SECONDS:
                self._notified_failures.pop(key, None)
        if message_id and message_id in self._notified_failures:
            return True
        if message_id:
            self._notified_failures[message_id] = now
        return False

    async def _notify_delivery_failure(
        self, chat_id: str, channel: str, recipient: str, body: str, reason: str
    ) -> "web.Response":
        """Wake the agent's session to handle a failed outbound message.

        Runs as a side-effect turn (run_consult): the agent decides whether to
        retry or switch channels and acts via its Inkbox tools. We deliberately
        do NOT auto-reply on the original channel — it may be the dead one, and
        replying there would just fail again and loop.

        Args:
            chat_id (str): Session key for the affected contact.
            channel (str): Channel that failed (SMS / iMessage / email).
            recipient (str): Who the message was meant for.
            body (str): The undelivered message text (may be empty).
            reason (str): Carrier/provider failure reason.

        Returns:
            web.Response: 200 ack for the webhook.
        """
        if self.sessions is None:
            return web.json_response({"ok": True, "ignored": "no-sessions"})
        prompt = _delivery_failure_prompt(channel, recipient, body, reason)
        # Run in the background so the webhook returns promptly; the turn can
        # take a while (the agent may send on another channel).
        asyncio.create_task(self._run_failure_turn(chat_id, prompt, channel, recipient))
        return web.json_response({"ok": True})

    async def _run_failure_turn(self, chat_id: str, prompt: str, channel: str, recipient: str) -> None:
        try:
            await self.sessions.get(chat_id).run_consult(prompt)
        except Exception:
            logger.exception("[bridge] delivery-failure turn failed: %s → %s", channel, recipient)

    async def _on_text_delivery_failed(self, envelope: Dict[str, Any], event_type: str) -> "web.Response":
        data = envelope.get("data") or {}
        message = data.get("text_message") or {}
        message_id = str(message.get("id") or "")
        if self._already_notified(message_id):
            return web.json_response({"ok": True, "deduped": True})
        recipient = str(message.get("remote_phone_number") or "").strip()
        body = str(message.get("text") or "").strip()
        # Prefer the human detail; fall back to the carrier code, then event.
        reason = str(message.get("error_detail") or message.get("error_code") or "").strip()
        if event_type == "text.delivery_unconfirmed" and not reason:
            reason = "carrier could not confirm delivery"
        chat_id = self._chat_key(data, recipient)
        logger.info("[bridge] SMS delivery failed to %s: %s", recipient, reason or event_type)
        return await self._notify_delivery_failure(chat_id, "SMS", recipient, body, reason or event_type)

    async def _on_imessage_delivery_failed(self, envelope: Dict[str, Any]) -> "web.Response":
        data = envelope.get("data") or {}
        message = data.get("message") or {}
        message_id = str(message.get("id") or "")
        if self._already_notified(message_id):
            return web.json_response({"ok": True, "deduped": True})
        recipient = str(message.get("remote_number") or "").strip()
        body = str(message.get("content") or "").strip()
        reason = str(
            message.get("error_detail")
            or message.get("error_reason")
            or message.get("error_message")
            or message.get("status")
            or ""
        ).strip()
        chat_id = self._chat_key(data, recipient)
        logger.info("[bridge] iMessage delivery failed to %s: %s", recipient, reason)
        return await self._notify_delivery_failure(chat_id, "iMessage", recipient, body, reason)

    async def _on_mail_delivery_failed(self, envelope: Dict[str, Any], event_type: str) -> "web.Response":
        data = envelope.get("data") or {}
        message = data.get("message") or {}
        message_id = str(message.get("id") or "")
        if self._already_notified(message_id):
            return web.json_response({"ok": True, "deduped": True})
        to_addresses = message.get("to_addresses") or []
        recipient = str(to_addresses[0] if to_addresses else "").strip()
        subject = str(message.get("subject") or "").strip()
        reason = "bounced" if event_type == "message.bounced" else "permanent send failure"
        chat_id = self._chat_key(data, recipient)
        logger.info("[bridge] email %s to %s (subject: %s)", reason, recipient, subject)
        body = f"(email, subject: {subject})" if subject else ""
        return await self._notify_delivery_failure(chat_id, "email", recipient, body, reason)

    # ------------------------------------------------------------------
    # Inbound: live calls (Inkbox STT/TTS text-frame bridge)
    # ------------------------------------------------------------------

    async def _open_realtime_bridge(self, remote: str, call_id: str) -> Any:
        """Preflight an OpenAI Realtime session for an incoming call.

        Args:
            remote (str): Caller phone number (may be empty).
            call_id (str): Inkbox call id, for logging.

        Returns:
            Any: An OpenedRealtimeBridge on success, or None if the connect
            failed (the caller then falls back to Inkbox STT/TTS).
        """
        phone = getattr(self._identity, "phone_number", None)
        meta = RealtimeCallMeta(
            call_id=call_id or "unknown",
            remote_phone_number=remote or None,
            agent_identity_phone=getattr(phone, "number", None),
            project_dir=self.cfg.project_dir,
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
        remote = str(call_context.get("remote_phone_number") or "").strip()
        call_id = str(call_context.get("id") or call_context.get("call_id") or "")
        chat_id = remote or f"call:{call_id}"

        ws = web.WebSocketResponse()

        # Realtime branch: when configured, pre-open OpenAI Realtime BEFORE we
        # commit the WS to a mode. If it connects, accept in raw-media mode and
        # bridge audio both ways; the model runs the call and consults Claude
        # Code via run_consult. If the preflight fails, fall through to Inkbox
        # STT/TTS below (unless fallback is disabled, then refuse the call).
        if self.cfg.realtime.enabled:
            bridge = await self._open_realtime_bridge(remote, call_id)
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
                    meta = {"call_id": call_id, "sender": remote}
                    session = self.sessions.get(chat_id)
                    await session.handle_inbound(text, "voice", meta)
                elif event == "stop":
                    break
        finally:
            self._active_call_ws.pop(chat_id, None)
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
        if mode == "voice":
            ws = self._active_call_ws.get(chat_id)
            if ws is not None:
                await self._speak(ws, strip_markdown(content), str(meta.get("call_id") or ""))
                return
            # Call ended while Claude was thinking — fall back to SMS so
            # the answer isn't lost.
            mode = "sms" if str(meta.get("to") or chat_id).startswith("+") else "email"

        identity = await asyncio.to_thread(self._inkbox.get_identity, self.cfg.identity)

        if mode == "sms":
            text = strip_markdown(content)
            if len(text) > SMS_MAX_LENGTH:
                text = text[: SMS_MAX_LENGTH - 1] + "…"
            kwargs: Dict[str, Any] = {"text": text}
            if meta.get("conversation_id"):
                kwargs["conversation_id"] = str(meta["conversation_id"])
            else:
                kwargs["to"] = str(meta.get("to") or chat_id)
            await asyncio.to_thread(identity.send_text, **kwargs)
        elif mode == "imessage":
            await asyncio.to_thread(
                identity.send_imessage,
                conversation_id=str(meta.get("conversation_id") or ""),
                text=strip_markdown(content),
            )
        else:  # email
            subject = str(meta.get("subject") or "").strip()
            reply_subject = subject if subject.lower().startswith("re:") else f"Re: {subject}" if subject else "From your Claude Code agent"
            await asyncio.to_thread(
                identity.send_email,
                to=[str(meta.get("to") or chat_id)],
                subject=reply_subject,
                body_text=content,
            )
