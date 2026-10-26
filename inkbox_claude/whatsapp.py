"""Approved one-to-one WhatsApp routing; delivery notifications never request a reply."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from typing import Any

EVENTS = [
    "whatsapp.received",
    "whatsapp.sent",
    "whatsapp.delivered",
    "whatsapp.delivery_failed",
    "whatsapp.delivery_unconfirmed",
]
logger = logging.getLogger(__name__)


def field(value: Any, name: str, default=None):
    return (
        value.get(name, default)
        if isinstance(value, dict)
        else getattr(value, name, default)
    )


def unsupported(exc):
    """An older API may reject the new subscription family without blocking other channels."""
    detail = str(getattr(exc, "detail", exc)).lower()
    return (
        (isinstance(exc, ValueError) or getattr(exc, "status_code", None) in {400, 422})
        and "whatsapp" in detail
        and any(
            word in detail for word in ("event", "prefix", "unknown", "unsupported")
        )
    )


def require_method(identity, name):
    method = getattr(identity, name, None)
    if not callable(method):
        raise RuntimeError("WhatsApp requires Inkbox SDK 0.7.21 or newer")
    return method


async def receive(gateway, envelope):
    from aiohttp import web

    data = envelope.get("data") or {}
    message = data.get("message") or {}
    message_id = str(message.get("id") or "")
    event_id = str(envelope.get("id") or message_id)
    conversation = str(message.get("conversation_id") or "")
    sender = str(message.get("sender_number") or "")
    identity = gateway._identity
    participants = message.get("participants") or []
    if (
        not message_id
        or not event_id
        or not conversation
        or message.get("direction") != "inbound"
        or message.get("is_group")
        or len(participants) != 1
        or participants[0] != sender
        or sender != message.get("remote_number")
        or sender == message.get("local_number")
        or str(message.get("agent_identity_id") or "") != str(identity.id)
    ):
        return web.json_response({"ok": True, "ignored": "unsupported-message"})
    if not gateway._sender_allowed(sender):
        return web.json_response({"ok": True, "ignored": "sender-not-allowed"})
    text = str(message.get("content") or "")
    media = message.get("media") or []
    if len(media) > 1 or (not text.strip() and not media):
        return web.json_response({"ok": True, "ignored": "unsupported-message"})
    key = f"whatsapp:{message_id}"
    if gateway._dedup_begin(key):
        return web.json_response({"ok": True, "deduped": True})
    try:
        refreshed = []
        for item in media:
            if item.get("status") == "unavailable":
                text += "\n[WhatsApp attachment unavailable]"
                continue
            fresh = await asyncio.to_thread(
                require_method(identity, "get_whatsapp_media"), item["id"]
            )
            if field(fresh, "status") == "ready" and field(fresh, "url"):
                refreshed.append(
                    {
                        "url": field(fresh, "url"),
                        "content_type": field(fresh, "content_type"),
                        "size": field(fresh, "size"),
                    }
                )
            else:
                text += "\n[WhatsApp attachment unavailable]"
        body = await gateway._with_media(
            text, refreshed, prefix=f"whatsapp-{message_id}"
        )
        if refreshed and body == text:
            body += "\n[WhatsApp attachment could not be downloaded; its media ID remains available]"
        contact = await gateway._resolve_contact_full(kind="phone", value=sender)
        memories = (
            gateway._webhook_contact_memories(
                data,
                resolved_contact_id=(contact or {}).get("id", ""),
                allow_sole_fallback=True,
            )
            if gateway.cfg.contact_memories_enabled
            else []
        )
        chat_id = gateway._chat_key(
            data,
            sender,
            gateway._thread_key("whatsapp", conversation),
            contact=contact,
            allow_webhook_contact=False,
        )
        meta = {
            "channel": "whatsapp",
            "raw_text": str(message.get("content") or ""),
            "message_id": message_id,
            "source_event_id": event_id,
            "imessage_event_ids": [event_id],
            "conversation_id": conversation,
            "sender": sender,
            "contact": contact,
            "agent_identity": None
            if contact
            else gateway._single_agent_identity(
                gateway._webhook_list(
                    data, "agent_identities", "agentIdentities", "identity_agents"
                )
            ),
            "contact_memories": memories,
            "conversation_kind": "direct",
            "media": bool(media),
        }
        store = gateway._channel_store("whatsapp")
        if not store.admit(chat_id, body, meta):
            gateway._dedup_commit(key)
            return web.json_response({"ok": True, "deduped": True})
        try:
            await gateway.sessions.get(chat_id).handle_inbound(body, "whatsapp", meta)
        except BaseException:
            store.forget_pending(meta)
            raise
    except BaseException:
        gateway._dedup_rollback(key)
        raise
    gateway._dedup_commit(key)
    return web.json_response({"ok": True})


async def send_reply(gateway, chat_id, content, meta):
    from .sessions import CHANNEL_DELIVERY_GUARD
    from .tools import _to_thread_drained
    from .prompts import strip_markdown

    text = strip_markdown(content)
    if len(text) > 4000:
        raise ValueError("WhatsApp text must be at most 4000 characters")
    conversation = str(meta.get("conversation_id") or "")
    if not conversation or meta.get("companion"):
        raise ValueError("WhatsApp reply requires its original one-to-one conversation")
    guard = CHANNEL_DELIVERY_GUARD.get()
    if guard is not None:
        guard()
    identity = gateway._identity
    method = require_method(identity, "send_whatsapp")
    source = meta.get("imessage_event_ids") or [
        meta.get("source_event_id") or meta.get("message_id")
    ]
    key = (
        "claude-wa-"
        + hashlib.sha256(
            json.dumps(
                [gateway.cfg.base_url, str(identity.id), conversation, source, text],
                sort_keys=True,
            ).encode()
        ).hexdigest()
    )
    if meta.get("final_reply"):
        gateway._channel_store("whatsapp").begin_send(meta, chat_id)

    def checked():
        if guard is not None:
            guard()
        return method(conversation_id=conversation, text=text, idempotency_key=key)

    sent = await _to_thread_drained(checked)
    try:
        gateway._channel_store("whatsapp").record_outbound(sent, meta, chat_id)
    except Exception:
        logger.warning(
            "WhatsApp accepted; local tracking needs recovery. Do not resend."
        )


def register_tools(register, client, identity_handle, cfg):
    """Native tools keep the originating turn's authorization through blocking I/O."""
    from pathlib import Path
    import mimetypes
    import uuid
    from .tools import CURRENT_SESSION, _mark_tool_delivery

    def string(description):
        return {"type": "string", "description": description}

    integer = {"type": "integer", "minimum": 1, "maximum": 200}

    def run(args, authorize, operation):
        authorize()
        identity = client.get_identity(identity_handle)
        authorize()
        if operation == "lookup_whatsapp_send":
            return {
                "message_id": str(
                    client.message_sends.lookup(
                        sender_kind="whatsapp_identity",
                        sender_id=identity.id,
                        operation="whatsapp.send",
                        idempotency_key=args["idempotency_key"],
                    )
                )
            }
        if operation == "get_whatsapp_status":
            status = require_method(identity, operation)()
            limits = None
            try:
                limits = require_method(identity, "get_whatsapp_limits")()
            except Exception:
                pass  # An older API must not block status or historical reads.
            return {"status": status, "limits": limits}
        if operation == "send_whatsapp":
            selector = {
                key: args[key] for key in ("to", "conversation_id") if key in args
            }
            if len(selector) != 1 or not (
                str(args.get("text") or "").strip() or args.get("media_id")
            ):
                raise ValueError(
                    "Use one recipient or conversation and text or an owned media ID"
                )
            session = CURRENT_SESSION.get()
            turn = getattr(session, "_current_turn", None)
            meta = getattr(turn, "reply_meta", None) or {}
            source = (
                meta.get("imessage_event_ids")
                or meta.get("source_event_id")
                or meta.get("message_id")
                or uuid.uuid4().hex
            )
            kwargs = {
                **selector,
                **({"text": args["text"]} if "text" in args else {}),
                **({"media_ids": [args["media_id"]]} if args.get("media_id") else {}),
            }
            key = args.get("idempotency_key") or (
                "claude-wa-tool-"
                + hashlib.sha256(
                    json.dumps(
                        [str(identity.id), source, kwargs], sort_keys=True
                    ).encode()
                ).hexdigest()
            )
            authorize()
            target = str(args.get("conversation_id") or args.get("to") or "")
            try:
                sent = require_method(identity, operation)(
                    **kwargs, idempotency_key=key
                )
            except Exception as exc:
                exc.whatsapp_media_id = args.get("media_id")
                if getattr(exc, "idempotency_key", None) and not public_quota_error(exc):
                    # An unknown outcome must not be followed by the same final
                    # answer as a second ordinary send on this conversation.
                    _mark_tool_delivery("whatsapp", target)
                raise
            _mark_tool_delivery("whatsapp", target)
            if (
                session is not None
                and meta.get("channel") == "whatsapp"
                and target == meta.get("conversation_id")
            ):
                from .imessage import IMessageState

                try:
                    store = IMessageState(cfg, channel="whatsapp")
                    store.record_outbound(sent, meta, session.chat_id)
                    store.mark(meta, "done", chat_id=session.chat_id)
                except Exception:
                    logger.warning(
                        "WhatsApp accepted; local tracking needs recovery. Do not resend."
                    )
            return {
                "message": sent,
                "idempotency_key": key,
                "media_id": args.get("media_id"),
            }
        if operation == "upload_whatsapp_media":
            path = Path(args["media_path"]).expanduser().resolve()
            if path.stat().st_size > 20 * 1024 * 1024:
                raise ValueError("WhatsApp media must be at most 20 MiB")
            with path.open("rb") as stream:
                content = stream.read(20 * 1024 * 1024 + 1)
            if len(content) > 20 * 1024 * 1024:
                raise ValueError("WhatsApp media must be at most 20 MiB")
            authorize()
            return require_method(identity, operation)(
                content=content,
                filename=path.name,
                content_type=mimetypes.guess_type(path.name)[0]
                or "application/octet-stream",
            )
        authorize()
        return require_method(identity, operation)(**args)

    specs = [
        (
            "inkbox_lookup_whatsapp_send",
            "Recover an ambiguous WhatsApp send's original message ID using its preserved request key. Read-only: this never sends or retries delivery.",
            {"idempotency_key": string("Original logical-send key")},
            ["idempotency_key"],
            "lookup_whatsapp_send",
        ),
        (
            "inkbox_get_whatsapp_status",
            "Read fresh WhatsApp availability. Access is approved in the console, not enabled by this tool.",
            {},
            [],
            "get_whatsapp_status",
        ),
        (
            "inkbox_send_whatsapp",
            "Send a requested one-to-one WhatsApp message. Your normal final response already replies automatically. Use one recipient or conversation; text and one owned media ID are optional individually. Preserve the returned request key and media ID after uncertainty; never retry as a new message.",
            {
                "to": string("One E.164 recipient"),
                "conversation_id": string(
                    "Existing conversation; mutually exclusive with to"
                ),
                "text": {**string("Message text"), "maxLength": 4000},
                "media_id": string("Owned ID returned by upload"),
                "idempotency_key": string(
                    "Optional original key for the same logical send"
                ),
            },
            [],
            "send_whatsapp",
        ),
        (
            "inkbox_list_whatsapp_conversations",
            "List WhatsApp conversations with local unread counts.",
            {"limit": integer, "offset": {"type": "integer", "minimum": 0}},
            [],
            "list_whatsapp_conversations",
        ),
        (
            "inkbox_get_whatsapp_conversation",
            "Read metadata for one WhatsApp conversation.",
            {"conversation_id": string("Conversation ID")},
            ["conversation_id"],
            "get_whatsapp_conversation",
        ),
        (
            "inkbox_list_whatsapp_messages",
            "List WhatsApp messages in one conversation, newest first.",
            {
                "conversation_id": string("Conversation ID"),
                "limit": integer,
                "offset": {"type": "integer", "minimum": 0},
            },
            ["conversation_id"],
            "list_whatsapp_messages",
        ),
        (
            "inkbox_get_whatsapp_message",
            "Read one WhatsApp message and its current sending and delivery state.",
            {"message_id": string("Message ID")},
            ["message_id"],
            "get_whatsapp_message",
        ),
        (
            "inkbox_upload_whatsapp_media",
            "Upload one local attachment (at most 20 MiB) and retain its owned ID for sending. Do not upload again to recover an uncertain send.",
            {"media_path": string("Local file path")},
            ["media_path"],
            "upload_whatsapp_media",
        ),
        (
            "inkbox_get_whatsapp_media",
            "Renew temporary access to an owned WhatsApp attachment by ID; unavailable media retains metadata.",
            {"media_id": string("Owned attachment ID")},
            ["media_id"],
            "get_whatsapp_media",
        ),
        (
            "inkbox_mark_whatsapp_read",
            "Mark local WhatsApp history read through an exact message. This sends no remote read receipt.",
            {
                "conversation_id": string("Conversation ID"),
                "through_message_id": string("Last visible message ID"),
            },
            ["conversation_id", "through_message_id"],
            "mark_whatsapp_read",
        ),
    ]
    for name, description, properties, required, operation in specs:
        register(
            name,
            description,
            properties,
            required,
            lambda args, authorize, operation=operation: run(
                args, authorize, operation
            ),
        )


def record_status(gateway, envelope):
    """Correlate content-free delivery facts without starting or retrying model work."""
    message = (envelope.get("data") or {}).get("message") or {}
    if envelope.get("event_type") not in EVENTS[1:]:
        return
    if (
        not message.get("id")
        or str(message.get("agent_identity_id") or "") != str(gateway._identity.id)
        or message.get("direction") != "outbound"
    ):
        return
    store = gateway._channel_store("whatsapp")
    with store._db() as db:
        row = db.execute(
            "SELECT route FROM outbound WHERE message_id=?", (str(message["id"]),)
        ).fetchone()
        route = (
            json.loads(row["route"])
            if row
            else {
                "chat_id": None,
                "meta": {},
                "message_id": str(message["id"]),
                "unknown_route": True,
            }
        )
        from datetime import datetime, timezone

        def timestamp(value):
            try:
                parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
                return (
                    parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
                ).timestamp()
            except ValueError:
                return None

        incoming, previous = (
            timestamp(message.get("updated_at")),
            timestamp(route.get("updated_at")),
        )
        if incoming is None or (previous is not None and incoming < previous):
            return
        route.update(
            {
                "status": message.get("status"),
                "delivery_state": message.get("delivery_state"),
                "updated_at": message.get("updated_at"),
            }
        )
        db.execute(
            "INSERT OR REPLACE INTO outbound(message_id,route) VALUES(?,?)",
            (str(message["id"]), json.dumps(route)),
        )


def public_quota_error(exc):
    """Keep only the public WhatsApp policy envelope, never arbitrary error bodies."""
    import re
    detail = getattr(exc, "detail", None)
    if getattr(exc, "status_code", None) not in {402, 403, 413, 422, 429}:
        return {}
    if not isinstance(detail, dict) or not re.fullmatch(r"whatsapp_[a-z0-9_]{1,120}", str(detail.get("error", ""))):
        return {}
    result = {"error": detail["error"]}
    quota = detail.get("quota")
    if isinstance(quota, str) and re.fullmatch(r"whatsapp\.[a-z0-9_.]{1,120}", quota):
        result["quota"] = quota
    for key in ("limit", "used"):
        if isinstance(detail.get(key), int) and not isinstance(detail[key], bool):
            result[key] = detail[key]
    condition = detail.get("retry_condition")
    if isinstance(condition, str):
        result["retry_condition"] = condition[:1000]
    reset = detail.get("reset_at")
    if isinstance(reset, str) and re.fullmatch(r"[0-9T:.+Z-]{20,40}", reset):
        result["reset_at"] = reset
    retry = getattr(exc, "retry_after_seconds", None)
    if isinstance(retry, int) and not isinstance(retry, bool) and retry >= 0:
        result["retry_after_seconds"] = retry
    return result
