"""Dedicated WhatsApp routing; delivery notifications never request a reply."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import uuid
from contextlib import nullcontext
from typing import Any

from .whatsapp_v2 import V2_OPERATIONS, run_v2, specifications as v2_specifications, send_selector

EVENTS = [
    "whatsapp.received",
    "whatsapp.sent",
    "whatsapp.delivered",
    "whatsapp.delivery_failed",
    "whatsapp.delivery_unconfirmed",
    "whatsapp.reaction_received",
]

logger = logging.getLogger(__name__)
_REJECTED_SEND_CODES = {
    403: {"whatsapp_not_eligible", "whatsapp_access_required", "whatsapp_contact_not_allowed", "whatsapp_media_source_not_allowed"},
    413: {"whatsapp_media_too_large"},
    422: {"whatsapp_groups_not_supported", "whatsapp_invalid_recipient", "whatsapp_text_too_long", "whatsapp_unanswered_followup_limit", "whatsapp_opening_message_too_long", "whatsapp_reply_required_for_media", "whatsapp_reply_required_for_links", "whatsapp_reply_required_for_phone_numbers", "whatsapp_reply_required_for_email_addresses"},
    429: {"whatsapp_daily_limit_exceeded", "whatsapp_unanswered_message_limit", "whatsapp_conversation_rate_limited", "whatsapp_new_contact_rate_limited", "whatsapp_new_contact_spacing", "whatsapp_follow_up_rate_limited"},
}


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
        raise RuntimeError("WhatsApp requires Inkbox SDK 0.7.24 or newer")
    return method


class ReplyPreflightRejected(ValueError):
    """A definitive rejection before submitting an automatic reply."""

    notice = None


class SendRecoveryRequired(ReplyPreflightRejected):
    """Expose only the retained request identity and fixed recovery guidance."""

    def __init__(self, key, intent=None, *, quoted=False):
        message = ("An explicit quoted reply is unresolved; preserve its original request and key" if quoted
                   else "A WhatsApp send was already attempted; preserve its original request and key")
        super().__init__(message)
        if isinstance(key, str) and key.strip() and len(key) <= 255 and key.isascii() and key.isprintable():
            self.idempotency_key = key
        media = (intent or {}).get("media_ids") or []
        self.whatsapp_media_id = media[0] if media else None


class ReplyTooLong(ReplyPreflightRejected):
    notice = "The answer exceeded WhatsApp's 4000-character limit and was not sent. Please ask for a shorter answer."


def _local_route_allowed(sender_allowed, meta):
    if not sender_allowed(str(meta.get("sender") or "")):
        return False
    if meta.get("conversation_kind") != "group":
        return True
    participants = meta.get("participants")
    return (isinstance(participants, list) and bool(participants)
            and meta.get("sender") in participants
            and all(isinstance(number, str) and re.fullmatch(r"\+[1-9]\d{6,14}", number)
                    and sender_allowed(number) for number in participants))


def local_route_allowed(gateway, meta):
    return _local_route_allowed(gateway._sender_allowed, meta)


def _validate_reply_access(identity, meta, sender_allowed):
    if not _local_route_allowed(sender_allowed, meta):
        raise ReplyPreflightRejected("WhatsApp conversation is no longer locally allowed")
    if meta.get("conversation_kind") != "group":
        return
    from .companion import retryable_read
    from .imessage import SafeReplyPreflightError
    try:
        conversation = require_method(identity, "get_whatsapp_conversation")(meta["conversation_id"])
    except Exception as exc:
        if retryable_read(exc):
            raise SafeReplyPreflightError("WhatsApp membership is temporarily unavailable") from None
        raise ReplyPreflightRejected("WhatsApp conversation membership could not be confirmed") from None
    if (str(field(conversation, "id")) != meta["conversation_id"]
            or str(field(conversation, "agent_identity_id")) != str(identity.id)
            or field(conversation, "is_group") is not True
            or field(conversation, "unresolved_participant_count", 0) != 0
            or not _local_route_allowed(sender_allowed, {**meta, "participants": field(conversation, "participants")})):
        raise ReplyPreflightRejected("WhatsApp conversation membership is no longer locally allowed")


async def validate_reply_access(gateway, meta):
    await asyncio.to_thread(_validate_reply_access, gateway._identity, meta, gateway._sender_allowed)


def delivery_target(meta, conversation=None, recipient=None):
    """Correlate canonical selectors without attributing outreach to this turn."""
    current = str(meta.get("conversation_id") or "")
    try:
        canonical = str(uuid.UUID(current))
    except ValueError:
        canonical = current
    if conversation is not None:
        return current if str(conversation) == canonical else str(conversation)
    recipients = recipient if isinstance(recipient, list) else [recipient]
    if (meta.get("channel") == "whatsapp" and current
            and meta.get("conversation_kind") != "group"
            and recipients == [meta.get("sender")] and meta.get("sender")):
        return current
    return str(recipient or "")


def source_plain_send(meta):
    """Older recipient attempts retain their request, not source-reply ownership."""
    plain = meta.get("whatsapp_plain_send")
    if plain and "to" in plain.get("intent", {}):
        if delivery_target(meta, recipient=plain["intent"]["to"]) != meta.get("conversation_id"):
            return None
    return plain


def validate_send_uncertainty(meta, *, key=None, intent=None):
    """Unresolved sends fence the owning turn, not just their destination."""
    attempts = list(meta.get("whatsapp_recipient_sends", {}).values())
    if meta.get("whatsapp_plain_send"):
        attempts.append(meta["whatsapp_plain_send"])
    for previous in attempts:
        if previous["outcome"] in {"pending", "uncertain"} and (
                key != previous["key"] or intent != previous["intent"]):
            raise SendRecoveryRequired(previous["key"], previous["intent"])
    if (meta.get("whatsapp_explicit_quote_attempted")
            and not meta.get("whatsapp_quote_accepted")
            and (key is None or key != meta.get("whatsapp_explicit_quote_key")
                 or intent is None or intent.get("reply_to_message_id") is None
                 or delivery_target(meta, intent.get("conversation_id")) != meta.get("conversation_id")
                 or (meta.get("whatsapp_quote_intent") is not None
                     and intent != meta["whatsapp_quote_intent"]))):
        raise SendRecoveryRequired(meta.get("whatsapp_explicit_quote_key"), meta.get("whatsapp_quote_intent"), quoted=True)


def reply_source(identity, conversation, message_id):
    """Persist source ownership only when admitting a new input."""
    return {"identity_id": str(identity.id), "conversation_id": conversation,
            "message_id": message_id}


def auto_reply_kwargs(identity, meta):
    """Legacy receipts without a source snapshot keep their original plain intent."""
    source = meta.get("whatsapp_reply_source")
    if source is None:
        return {}
    target = meta.get("whatsapp_reply_target")
    if (not isinstance(source, dict) or source.get("identity_id") != str(identity.id)
            or source.get("conversation_id") != meta.get("conversation_id")
            or not target or source.get("message_id") != target):
        raise ReplyPreflightRejected("WhatsApp reply source ownership cannot be verified")
    try:
        uuid.UUID(target)
    except (ValueError, TypeError, AttributeError):
        raise ReplyPreflightRejected("WhatsApp reply source must be a message ID") from None
    from .companion import retryable_read
    from .imessage import SafeReplyPreflightError
    try:
        message = require_method(identity, "get_whatsapp_message")(target)
    except Exception as exc:
        if retryable_read(exc):
            raise SafeReplyPreflightError("WhatsApp reply source is temporarily unavailable") from None
        raise ReplyPreflightRejected("WhatsApp reply source is no longer available") from None
    if (str(field(message, "id")) != target
            or str(field(message, "conversation_id")) != source["conversation_id"]
            or str(field(message, "agent_identity_id")) != source["identity_id"]
            or field(message, "is_blocked") is True):
        raise ReplyPreflightRejected("WhatsApp reply source ownership cannot be verified")
    return {"reply_to_message_id": target}


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
    is_group = message.get("is_group") is True
    if (
        not message_id
        or not event_id
        or not conversation
        or message.get("direction") != "inbound"
        or not isinstance(participants, list)
        or not participants
        or any(not isinstance(number, str) or re.fullmatch(r"\+[1-9]\d{6,14}", number) is None for number in participants)
        or sender not in participants
        or (not is_group and (len(participants) != 1 or sender != message.get("remote_number")))
        or sender == message.get("local_number")
        or str(message.get("agent_identity_id") or "") != str(identity.id)
    ):
        return web.json_response({"ok": True, "ignored": "unsupported-message"})
    if not gateway._sender_allowed(sender) or (is_group and any(not gateway._sender_allowed(number) for number in participants)):
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
        chat_id = gateway._thread_key("whatsapp", conversation) if is_group else gateway._chat_key(
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
            if contact or is_group
            else gateway._single_agent_identity(
                gateway._webhook_list(
                    data, "agent_identities", "agentIdentities", "identity_agents"
                )
            ),
            "contact_memories": memories,
            "conversation_kind": "group" if is_group else "direct",
            "participants": participants,
            "reply_to_message_id": message.get("reply_to_message_id"),
            "thread_id": message.get("thread_id"),
            "thread_root_message_id": message.get("thread_root_message_id"),
            "whatsapp_reply_target": message_id,
            "whatsapp_reply_source": reply_source(identity, conversation, message_id),
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


def validate_reply_fence(meta, *, interaction=False):
    """Only confirmed explicit sends permit a separate human interaction."""
    validate_send_uncertainty(meta)
    plain = source_plain_send(meta)
    if plain and plain["outcome"] != "rejected" and not (
            plain["outcome"] == "accepted" and interaction):
        raise ReplyPreflightRejected("An explicit reply was already attempted; inspect its original receipt instead of sending an automatic replacement")
    if meta.get("whatsapp_explicit_quote_attempted") and not (
            interaction and meta.get("whatsapp_quote_accepted")):
        raise ReplyPreflightRejected("An explicit quoted reply was already attempted; inspect its original receipt instead of sending an automatic replacement")


async def send_reply(gateway, chat_id, content, meta):
    validate_reply_fence(meta, interaction=bool(meta.get("whatsapp_interaction_id") and not meta.get("final_reply")))
    from .sessions import CHANNEL_DELIVERY_GUARD
    from .tools import _to_thread_drained
    from .prompts import strip_markdown

    text = strip_markdown(content)
    if len(text) > 4000:
        raise ReplyTooLong("WhatsApp text must be at most 4000 characters")
    conversation = str(meta.get("conversation_id") or "")
    if not conversation or meta.get("companion"):
        raise ValueError("WhatsApp reply requires its original conversation")
    guard = CHANNEL_DELIVERY_GUARD.get()
    if guard is not None:
        guard()
    await validate_reply_access(gateway, meta)
    if guard is not None:
        guard()
    identity = gateway._identity
    method = require_method(identity, "send_whatsapp")
    quote = await asyncio.to_thread(auto_reply_kwargs, identity, meta)
    if guard is not None:
        guard()
    source = meta.get("imessage_event_ids") or [
        meta.get("source_event_id") or meta.get("message_id")
    ]
    key_parts = [gateway.cfg.base_url, str(identity.id), conversation, source, text]
    if quote:
        key_parts.append(quote)
    if not meta.get("final_reply"):
        key_parts.append(meta.get("whatsapp_interaction_id") or uuid.uuid4().hex)
    key = (
        "claude-wa-"
        + hashlib.sha256(
            json.dumps(
                key_parts,
                sort_keys=True,
            ).encode()
        ).hexdigest()
    )
    meta["whatsapp_send_key"] = key
    if meta.get("final_reply"):
        gateway._channel_store("whatsapp").begin_send(meta, chat_id)

    def checked():
        if guard is not None:
            guard()
        return method(conversation_id=conversation, text=text, idempotency_key=key, **quote)

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
        session = CURRENT_SESSION.get()
        turn = getattr(session, "_current_turn", None)
        meta = getattr(turn, "reply_meta", None) or {}

        def authorize_effect():
            authorize()
            validate_send_uncertainty(meta)

        if operation in {"upload_whatsapp_media", "send_whatsapp_reaction", "remove_whatsapp_reaction"}:
            authorize_effect()
        identity = client.get_identity(identity_handle)
        authorize()
        if operation in V2_OPERATIONS:
            return run_v2(identity, operation, args, authorize=authorize_effect)
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
            recipient, conversation, reply_target, group_name = send_selector(args)
            native = (meta.get("channel") == "whatsapp" and conversation is not None
                      and delivery_target(meta, conversation) == meta.get("conversation_id"))
            if native and meta.get("companion"):
                raise ValueError("Use the bound Companion reply tool for this turn")
            if (native and reply_target is None and args.get("idempotency_key")
                    and args["idempotency_key"] == meta.get("whatsapp_explicit_quote_key")
                    and meta.get("whatsapp_quote_intent")):
                reply_target = meta["whatsapp_quote_intent"].get("reply_to_message_id")
            if native and meta.get("whatsapp_reply_source") is not None:
                quote = auto_reply_kwargs(identity, meta)
                authorize()
                # A supplied key may belong to an older plain request. Only an
                # explicit target or a new key can introduce a quote.
                if reply_target is None and not args.get("idempotency_key"):
                    reply_target = quote.get("reply_to_message_id")
                    args["reply_to_message_id"] = reply_target
            selector = {"conversation_id": conversation} if conversation is not None else {"to": recipient}
            if reply_target is not None:
                selector["reply_to_message_id"] = reply_target
            if group_name is not None:
                selector["group_name"] = group_name
            if not (
                str(args.get("text") or "").strip() or args.get("media_id")
            ):
                raise ValueError(
                    "Use one recipient or conversation and text or an owned media ID"
                )
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
            if (not isinstance(key, str) or not key.strip() or len(key) > 255
                    or not key.isascii() or not key.isprintable()):
                raise ValueError("idempotency_key must contain 1–255 printable ASCII characters")
            if "text" in args and (not isinstance(args["text"], str) or len(args["text"]) > 4000):
                raise ValueError("WhatsApp text must be at most 4000 characters")
            args["idempotency_key"] = key
            authorize()
            target = delivery_target(meta, conversation, recipient)
            source_bound = meta.get("channel") == "whatsapp" and target == meta.get("conversation_id")
            def recipient_allowed(number):
                return (cfg.allow_all_users or not cfg.allowed_users
                        or number.lower() in {user.lower() for user in cfg.allowed_users})

            if recipient is not None and not all(recipient_allowed(number) for number in (
                    recipient if isinstance(recipient, list) else [recipient])):
                raise ReplyPreflightRejected("WhatsApp recipients are no longer locally allowed")
            if source_bound:
                _validate_reply_access(identity, meta, recipient_allowed)
            method = require_method(identity, operation)
            attempt = None
            prior_uncertainty = False
            recipient_attempt = (session is not None and meta.get("channel") == "whatsapp"
                                 and not source_bound)
            send_lock = turn.whatsapp_send_lock if turn is not None else nullcontext()
            # Network preflight stays outside the lock; admission uses the live journal.
            with send_lock:
                authorize()
                attempts = meta.get("whatsapp_recipient_sends", {})
                legacy = meta.get("whatsapp_plain_send")
                if legacy and source_plain_send(meta) is None:
                    attempts = meta.setdefault("whatsapp_recipient_sends", {})
                    attempts.setdefault(legacy["key"], legacy)
                    # Clear the old slot explicitly so persistence cannot restore it.
                    meta["whatsapp_plain_send"] = None
                validate_send_uncertainty(meta, key=key, intent=kwargs)
                for previous in attempts.values():
                    if ((key == previous["key"] and kwargs != previous["intent"])
                            or (previous["outcome"] in {"pending", "uncertain"}
                                and (key != previous["key"] or kwargs != previous["intent"]))):
                        raise SendRecoveryRequired(previous["key"], previous["intent"])
                if source_bound:
                    retained = source_plain_send(meta)
                    if retained and (
                            (key == retained["key"] and kwargs != retained["intent"])
                            or (retained["outcome"] in {"pending", "uncertain"}
                                and (key != retained["key"] or kwargs != retained["intent"]))):
                        raise SendRecoveryRequired(retained["key"], retained["intent"])
                    if meta.get("whatsapp_explicit_quote_attempted") and (
                            not meta.get("whatsapp_quote_accepted") or key == meta.get("whatsapp_explicit_quote_key")) and (
                            reply_target is None or key != meta.get("whatsapp_explicit_quote_key")
                            or (meta.get("whatsapp_quote_intent") is not None
                                and kwargs != meta["whatsapp_quote_intent"])):
                        raise SendRecoveryRequired(meta.get("whatsapp_explicit_quote_key"), meta.get("whatsapp_quote_intent"), quoted=True)
                if reply_target is not None and source_bound:
                    meta["whatsapp_quote_intent"] = kwargs.copy()
                    meta["whatsapp_quote_accepted"] = False
                    meta["whatsapp_explicit_quote_attempted"] = True
                    meta["whatsapp_explicit_quote_key"] = key
                    from .imessage import IMessageState
                    IMessageState(cfg, channel="whatsapp").mark(meta, "running", chat_id=session.chat_id)
                    _mark_tool_delivery("whatsapp", target)
                if (source_bound and reply_target is None) or recipient_attempt:
                    if recipient_attempt:
                        retained = attempts.get(key)
                    prior_uncertainty = bool(retained and retained["outcome"] in {"pending", "uncertain"})
                    attempt = {"key": key, "intent": kwargs.copy(), "outcome": "pending"}
                    if recipient_attempt:
                        meta.setdefault("whatsapp_recipient_sends", {})[key] = attempt
                    else:
                        meta["whatsapp_plain_send"] = attempt
                    from .imessage import IMessageState
                    IMessageState(cfg, channel="whatsapp").mark(meta, "running", chat_id=session.chat_id)
            authorize()
            try:
                sent = method(
                    **kwargs, idempotency_key=key
                )
            except Exception as exc:
                exc.idempotency_key = key
                exc.whatsapp_media_id = args.get("media_id")
                if prior_uncertainty or not public_quota_error(exc):
                    # An unknown outcome must not be followed by the same final
                    # answer as a second ordinary send on this conversation.
                    _mark_tool_delivery("whatsapp", target)
                with send_lock:
                    if attempt is not None:
                        attempt["outcome"] = "rejected" if public_quota_error(exc) and not prior_uncertainty else "uncertain"
                        try:
                            IMessageState(cfg, channel="whatsapp").mark(meta, "running", chat_id=session.chat_id)
                        except Exception:
                            logger.warning("WhatsApp send tracking needs recovery; preserve the original request key.")
                raise
            with send_lock:
                if attempt is not None:
                    attempt["outcome"] = "accepted"
                returned_conversation = field(sent, "conversation_id")
                quoted_current = (reply_target is not None and meta.get("channel") == "whatsapp"
                                  and target == meta.get("conversation_id"))
                if quoted_current:
                    meta["whatsapp_quote_accepted"] = True
                if returned_conversation is not None:
                    returned_target = delivery_target(meta, str(returned_conversation))
                    if not (isinstance(recipient, list) and len(recipient) > 1
                            and returned_target == meta.get("conversation_id")):
                        target = returned_target
                _mark_tool_delivery("whatsapp", target)
                if recipient_attempt:
                    try:
                        IMessageState(cfg, channel="whatsapp").mark(meta, "running", chat_id=session.chat_id)
                    except Exception:
                        logger.warning("WhatsApp accepted; local tracking needs recovery. Do not resend.")
                if (
                    session is not None
                    and meta.get("channel") == "whatsapp"
                    and target == meta.get("conversation_id")
                ):
                    from .imessage import IMessageState

                    try:
                        store = IMessageState(cfg, channel="whatsapp")
                        if quoted_current or attempt is not None:
                            store.mark(meta, "running", chat_id=session.chat_id)
                        store.record_outbound(sent, meta, session.chat_id)
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
            authorize_effect()
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
            "Send a requested direct or dedicated-line group WhatsApp message, quoting the active source by default in its exact conversation for new request keys. An explicit visible quote is also supported. Your normal final response already replies automatically. Use one recipient or conversation; text and one owned media ID are optional individually. Preserve the returned request key and media ID after uncertainty; never retry as a new message.",
            {
                "to": {"anyOf": [string("One E.164 recipient"), {"type": "array", "items": string("E.164 group participant"), "minItems": 1, "maxItems": 8, "uniqueItems": True}], "description": "One number, or 2-8 unique numbers for a new group. Existing groups use conversation_id."},
                "reply_to_message_id": string("Visible message ID to quote; requires conversation_id"),
                "group_name": {**string("Creation hint for a new group; not a rename"), "minLength": 1, "maxLength": 100},
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
            {"limit": integer, "offset": {"type": "integer", "minimum": 0},
             "include_groups": {"type": "boolean", "description": "Include group conversations; defaults to false"}},
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
            "Upload one local attachment within the current status/limits allowance (5 MiB by default, 20 MiB hard ceiling). Retain its owned ID; do not re-upload after an uncertain send.",
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
    for name, description, properties, required, operation in specs + v2_specifications():
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
    if envelope.get("event_type") not in EVENTS[1:5]:
        return
    message = (envelope.get("data") or {}).get("message") or {}
    if (
        not message.get("id")
        or str(message.get("agent_identity_id") or "") != str(gateway._identity.id)
        or message.get("direction") != "outbound"
    ):
        return
    store = gateway._channel_store("whatsapp")
    with store._db() as db:
        db.execute("BEGIN IMMEDIATE")
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
    code = detail.get("error") if isinstance(detail, dict) else None
    if not isinstance(code, str) or code not in _REJECTED_SEND_CODES.get(getattr(exc, "status_code", None), set()):
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
