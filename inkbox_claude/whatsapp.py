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
    409: {"whatsapp_group_reply_required"},
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
        raise RuntimeError("WhatsApp requires Inkbox SDK 0.7.26 or newer")
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


def group_sdk_available():
    """Groups require models that preserve the conversation's routing metadata."""
    from inkbox.whatsapp import WhatsAppConversation, WhatsAppMessage
    return all("is_group" in model.__dataclass_fields__ for model in (WhatsAppConversation, WhatsAppMessage))


def require_group_metadata(value):
    """Never expose a group as a direct chat when older models drop its fields."""
    if isinstance(value, (list, tuple)):
        for item in value:
            require_group_metadata(item)
    elif hasattr(value, "participants") and hasattr(value, "remote_number"):
        if field(value, "remote_number") is None and (not is_group(value) or not group_sdk_available()):
            raise RuntimeError("WhatsApp groups require Inkbox SDK 0.7.29 or newer")
    elif hasattr(value, "messages"):
        require_group_metadata(value.messages)
    return value


def is_group(value):
    return field(value, "is_group") is True


def mentioned(text, meta, handle):
    from .prompts import mentions_agent
    handle = str(handle or "").lstrip("@")
    return meta.get("whatsapp_mentioned") is True or mentions_agent(text, "" if handle.isdigit() else handle)


def control_text(text, meta, handle):
    parts = text.strip().split(maxsplit=1)
    self_tokens = {item.get("text") for item in meta.get("mentions") or []
                   if isinstance(item, dict) and item.get("is_agent") is True and isinstance(item.get("text"), str)}
    handle = str(handle or "").lstrip("@").lower()
    tokens = self_tokens | {"@agent"} | ({"@" + handle} if handle and not handle.isdigit() else set())
    if parts and parts[0].rstrip(",:").lower() in tokens:
        return parts[1] if len(parts) == 2 else ""
    return text


async def render_mentions(gateway, text, mentions):
    """Render verified mention tokens using names only, without contact memories."""
    names = {}
    enriched = []
    replacements = {}
    for item in mentions:
        if not isinstance(item, dict):
            continue
        phone = item.get("phone_number")
        if not isinstance(phone, str) or re.fullmatch(r"\+[1-9][0-9]{6,14}", phone) is None:
            phone = None
        is_agent = item.get("is_agent") is True
        if phone and not is_agent and phone not in names:
            try:
                contact = await gateway._resolve_contact_full(kind="phone", value=phone)
            except Exception:
                contact = None
            name = field(contact, "name") if contact else None
            names[phone] = " ".join(re.sub(r"[\x00-\x1f\x7f]", " ", name).split())[:160] if isinstance(name, str) else ""
        name = (field(getattr(gateway, "_identity", None), "agent_handle") or gateway.cfg.identity) if is_agent else names.get(phone) or None
        token = item.get("text")
        enriched.append({"text": token if isinstance(token, str) else None, "phone_number": phone,
                         "is_agent": is_agent, "display_name": name})
        if isinstance(token, str) and re.fullmatch(r"@[0-9]+", token):
            replacement = "@" + (name or phone) if name or phone else "@[unresolved mention]"
            if token in replacements and replacements[token] != replacement:
                replacement = "@[unresolved mention]"
            replacements[token] = replacement
    if not replacements:
        return text, enriched
    tokens = "|".join(re.escape(token) for token in sorted(replacements, key=len, reverse=True))
    pattern = re.compile(r"(?:https?://|www\.)\S+|(?<![\w@.+/-])(?P<mention>" + tokens + r")(?![\w-])(?!\.\w)", re.I)
    return pattern.sub(lambda match: replacements[match.group("mention")] if match.group("mention") else match.group(), text), enriched


async def with_media(gateway, message, text):
    """Refresh owned attachments and preserve native inputs for both receive paths."""
    media = message.get("media") or []
    if not media:
        return text, []
    identity = gateway._identity
    message_id = message["id"]
    refreshed = []
    for item in media:
        if item.get("status") == "unavailable":
            text += f"\n[WhatsApp attachment {item.get('id', 'unknown')} unavailable]"
            continue
        try:
            fresh = await asyncio.to_thread(
                require_method(identity, "get_whatsapp_media"), item["id"]
            )
        except Exception as exc:
            from .companion import retryable_read
            if retryable_read(exc) or getattr(exc, "status_code", None) == 409:
                raise
            text += f"\n[WhatsApp attachment {item['id']} unavailable; do not guess its contents]"
            continue
        if field(fresh, "status") == "ready" and field(fresh, "url"):
            refreshed.append(
                {
                    "url": field(fresh, "url"),
                    "content_type": field(fresh, "content_type"),
                    "size": field(fresh, "size"),
                }
            )
        else:
            text += f"\n[WhatsApp attachment {item.get('id', 'unknown')} unavailable]"
    image_paths = []
    body = await gateway._with_media(
        text, refreshed, prefix=f"whatsapp-{message_id}"
    )
    if refreshed and body == text:
        body += "\n[WhatsApp attachment could not be downloaded; its media ID remains available]"
    return body, image_paths


async def prepare_companion(gateway, envelope, *, enrich=False):
    """Validate signed group context without importing direct-contact memories."""
    from copy import deepcopy
    from .imessage import source_metadata

    message = (envelope.get("data") or {}).get("message") or {}
    participants = message.get("participants") or []
    sender = message.get("sender_number")
    if (not is_group(message) or message.get("direction") != "inbound"
            or str(message.get("agent_identity_id")) != str(gateway._identity.id)
            or not isinstance(participants, list)):
        raise PermissionError("Invalid WhatsApp Companion group source")
    if (message.get("sender_access") not in {"direct", "sponsored"}
            or not group_sdk_available() or unsupported_shape(message) or not participants
            or any(not isinstance(number, str) or re.fullmatch(r"\+[1-9][0-9]{6,14}", number) is None for number in participants)
            or sender not in participants or sender == message.get("local_number")):
        return None  # Current local admission can change without invalidating the signed event.

    raw = str(message.get("content") or "")
    rendered, mentions = raw, message.get("mentions") or []
    body, image_paths = raw, []
    if enrich:
        rendered, mentions = await render_mentions(gateway, raw, mentions)
        body, image_paths = await with_media(gateway, message, rendered)
    meta = {**source_metadata(message, envelope.get("id")), "channel": "whatsapp",
        "is_group": True, "conversation_kind": "group", "conversation_id": message.get("conversation_id"),
        "group_name": message.get("group_name"), "remote_number": None,
        "unresolved_participant_count": 0, "participants": participants,
        "sender": sender, "whatsapp_reply_target": message.get("id"),
        "mentions": mentions, "whatsapp_mentioned": message.get("agent_mentioned") is True,
        "raw_text": raw, "rendered_text": rendered, "original_text": raw if raw != rendered else None,
        "contact": None, "contact_memories": [], "media": bool(message.get("media")),
        "whatsapp_body": body, "whatsapp_image_paths": image_paths, "whatsapp_enriched": enrich,
        "whatsapp_media_ids": [item["id"] for item in message.get("media") or [] if item.get("id")]}
    prepared = deepcopy(envelope)
    prepared["_claude_whatsapp_source"] = meta
    return prepared


def authorize_companion(client, cfg, meta):
    """Validate this exact grant; never borrow another activation or destination."""

    activation = meta.get("companion_activation_id")
    if not activation:
        return False
    sponsor = meta.get("companion_sponsor")
    allowed = {user.lower() for user in cfg.allowed_users}
    if not sponsor or (not cfg.allow_all_users and allowed and sponsor.lower() not in allowed):
        raise PermissionError("WhatsApp Companion sponsor is no longer permitted locally")
    resource = getattr(client, "companion", None)
    method = getattr(resource, "activation_messages", None)
    if not callable(method):
        raise PermissionError("WhatsApp Companion requires a compatible Inkbox SDK")
    page = method(cfg.identity, activation, limit=1)
    if (str(field(page, "scope_id")), str(field(page, "activation_id")),
            str(field(page, "conversation_id")), field(page, "channel")) != (
            meta.get("companion_scope_id"), activation, meta.get("conversation_id"), "whatsapp"):
        raise PermissionError("WhatsApp Companion activation no longer matches its source")
    reply = field(page, "reply_context")
    if field(reply, "channel") != "whatsapp" or str(field(reply, "conversation_id")) != meta.get("conversation_id"):
        raise PermissionError("WhatsApp Companion reply context changed")
    return True


class GroupRouteRejected(PermissionError):
    """This receipt is not currently permitted; later ordinary receipts may be."""


def validate_group_destination(identity, conversation, cfg, *, client=None, route=None):
    """Check the actual destination, including explicit outreach from another chat."""
    sponsored = bool(route and route.get("companion") and route.get("companion_activation_id")
                     and route.get("conversation_id") == conversation)
    if sponsored:
        authorize_companion(client, cfg, route)
    try:
        current = require_group_metadata(require_method(identity, "get_whatsapp_conversation")(conversation))
    except Exception as exc:
        if route and route.get("companion") and not route.get("companion_activation_id") and getattr(exc, "status_code", None) in {403, 404}:
            raise GroupRouteRejected("WhatsApp conversation is not currently permitted") from None
        raise
    if not is_group(current):
        if sponsored:
            raise PermissionError("WhatsApp Companion requires its admitted group")
        peer = field(current, "remote_number")
        allowed = {user.lower() for user in cfg.allowed_users}
        if (unsupported_shape(current) or str(field(current, "id")) != conversation
                or str(field(current, "agent_identity_id")) != str(identity.id)
                or not isinstance(peer, str) or re.fullmatch(r"\+[1-9][0-9]{6,14}", peer) is None
                or field(current, "participants") != [peer]
                or (not cfg.allow_all_users and allowed and peer.lower() not in allowed)):
            raise GroupRouteRejected("WhatsApp conversation recipient is not allowed")
        return
    if field(current, "unresolved_participant_count") or not field(current, "participants"):
        from .imessage import SafeReplyPreflightError
        raise SafeReplyPreflightError("WhatsApp group membership proof is temporarily unavailable")
    allowed = {user.lower() for user in cfg.allowed_users}
    if (unsupported_shape(current) or str(field(current, "id")) != conversation
            or str(field(current, "agent_identity_id")) != str(identity.id)
            or not field(current, "participants")
            or (not sponsored and not cfg.allow_all_users and allowed
                and any(number.lower() not in allowed for number in field(current, "participants")))):
        raise GroupRouteRejected("WhatsApp conversation participants are not allowed")



def unsupported_shape(value):
    """Reject unsupported persisted or incoming conversation shapes."""
    values = value if isinstance(value, dict) else vars(value)
    if is_group(value):
        try:
            uuid.UUID(str(field(value, "conversation_id") or field(value, "id") or ""))
        except ValueError:
            return True
        return (field(value, "remote_number") is not None or "group_name" not in values
                or bool(field(value, "unresolved_participant_count"))
                or any(key.startswith("group_") and key != "group_name" for key in values))
    return (field(value, "is_group") is True
            or field(value, "conversation_kind") == "group"
            or bool(field(value, "unresolved_participant_count", 0))
            or any((key.startswith("group_") or key == "groupName") and item is not None
                   for key, item in values.items())
            or any(field(value, key) is not None and (
                not isinstance(field(value, key), list) or len(field(value, key)) > 1)
                for key in ("participants", "recipients")))


def unsupported_saved_intent(meta):
    attempts = list(meta.get("whatsapp_recipient_sends", {}).values())
    if meta.get("whatsapp_plain_send"):
        attempts.append(meta["whatsapp_plain_send"])
    intents = [attempt.get("intent", {}) for attempt in attempts]
    if meta.get("whatsapp_quote_intent"):
        intents.append(meta["whatsapp_quote_intent"])
    return any(unsupported_shape(intent) or ("to" in intent and (
        not isinstance(intent["to"], str)
        or re.fullmatch(r"\+[1-9][0-9]{6,14}", intent["to"]) is None)) for intent in intents)


def _local_route_allowed(sender_allowed, meta):
    return (not unsupported_shape(meta) and not unsupported_saved_intent(meta)
            and sender_allowed(str(meta.get("sender") or ""))
            and (not is_group(meta) or bool(meta.get("participants"))
                 and all(sender_allowed(number) for number in meta["participants"])))


def local_route_allowed(gateway, meta):
    return _local_route_allowed(gateway._sender_allowed, meta)


def _validate_reply_access(identity, meta, sender_allowed):
    if not _local_route_allowed(sender_allowed, meta):
        raise ReplyPreflightRejected("WhatsApp conversation is no longer locally allowed")
    if is_group(meta):
        current = require_group_metadata(require_method(identity, "get_whatsapp_conversation")(meta["conversation_id"]))
        if (not is_group(current) or unsupported_shape(current)
                or str(field(current, "id")) != meta["conversation_id"]
                or str(field(current, "agent_identity_id")) != str(identity.id)
                or not field(current, "participants")
                or not all(sender_allowed(number) for number in field(current, "participants"))):
            raise ReplyPreflightRejected("WhatsApp group membership is no longer permitted")


async def validate_reply_access(gateway, meta):
    from .companion import retryable_read
    from .imessage import SafeReplyPreflightError
    try:
        if meta.get("companion"):
            await asyncio.to_thread(validate_group_destination, gateway._identity, meta["conversation_id"], gateway.cfg,
                                    client=gateway._inkbox, route=meta)
        else:
            await asyncio.to_thread(_validate_reply_access, gateway._identity, meta, gateway._sender_allowed)
    except Exception as exc:
        if retryable_read(exc) or getattr(exc, "status_code", None) == 409:
            raise SafeReplyPreflightError("WhatsApp reply authorization is temporarily unavailable") from None
        raise ReplyPreflightRejected("WhatsApp reply authorization is unavailable") from None


def delivery_target(meta, conversation=None, recipient=None):
    """Correlate canonical selectors without attributing outreach to this turn."""
    current = str(meta.get("conversation_id") or "")
    try:
        canonical = str(uuid.UUID(current))
    except ValueError:
        canonical = current
    if conversation is not None:
        return current if str(conversation) == canonical else str(conversation)
    if (meta.get("channel") == "whatsapp" and current
            and meta.get("conversation_kind") != "group"
            and recipient == meta.get("sender") and meta.get("sender")):
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
    source_attempt = source_plain_send(meta) if meta.get("whatsapp_reply_source") is not None else None
    source_destination = (intent is None or not (intent.get("conversation_id") or intent.get("to"))
                          or delivery_target(meta, intent.get("conversation_id"), intent.get("to"))
                          == meta.get("conversation_id"))
    if (source_attempt and source_attempt["outcome"] == "rejected"
            and (source_destination or key == source_attempt["key"])
            and (key != source_attempt["key"] or intent != source_attempt["intent"])):
        raise SendRecoveryRequired(source_attempt["key"], source_attempt["intent"])
    if unsupported_saved_intent(meta):
        raise ReplyPreflightRejected("A saved WhatsApp request is unsupported; inspect its original send receipt without retrying")
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
            "message_id": message_id, "reply_style": "plain"}


def auto_reply_kwargs(identity, meta):
    """Validate the source and preserve the reply style saved with each input."""
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
            or (field(message, "is_blocked") is True and not (meta.get("companion_activation_id") and is_group(message) and field(message, "sender_access") == "sponsored"))
            or unsupported_shape(require_group_metadata(message))):
        raise ReplyPreflightRejected("WhatsApp reply source ownership cannot be verified")
    return {} if source.get("reply_style") == "plain" else {"reply_to_message_id": target}


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
        unsupported_shape(message)
        or not message_id
        or not event_id
        or not conversation
        or message.get("direction") != "inbound"
        or not isinstance(participants, list)
        or not participants
        or any(not isinstance(number, str) or re.fullmatch(r"\+[1-9][0-9]{6,14}", number) is None for number in participants)
        or sender not in participants
        or (not is_group(message) and (len(participants) != 1 or sender != message.get("remote_number")))
        or sender == message.get("local_number")
        or str(message.get("agent_identity_id") or "") != str(identity.id)
    ):
        return web.json_response({"ok": True, "ignored": "unsupported-message"})
    if is_group(message) and not group_sdk_available():
        return web.json_response({"ok": True, "ignored": "group-sdk-upgrade-required"})
    if not gateway._sender_allowed(sender) or (is_group(message) and not all(gateway._sender_allowed(number) for number in participants)):
        return web.json_response({"ok": True, "ignored": "sender-not-allowed"})
    text = str(message.get("content") or "")
    media = message.get("media") or []
    if len(media) > 1 or (not text.strip() and not media):
        return web.json_response({"ok": True, "ignored": "unsupported-message"})
    key = f"whatsapp:{message_id}"
    if gateway._dedup_begin(key):
        return web.json_response({"ok": True, "deduped": True})
    try:
        original_text = text
        mentions = message.get("mentions") if isinstance(message.get("mentions"), list) else []
        rendered_text, mentions = await render_mentions(gateway, text, mentions)
        body, _ = await with_media(gateway, message, rendered_text)
        contact = None if is_group(message) else await gateway._resolve_contact_full(kind="phone", value=sender)
        memories = (
            gateway._webhook_contact_memories(
                data,
                resolved_contact_id=(contact or {}).get("id", ""),
                allow_sole_fallback=True,
            )
            if gateway.cfg.contact_memories_enabled and not is_group(message)
            else []
        )
        chat_id = gateway._thread_key("whatsapp", conversation) if is_group(message) else gateway._chat_key(
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
            if contact or is_group(message)
            else gateway._single_agent_identity(
                gateway._webhook_list(
                    data, "agent_identities", "agentIdentities", "identity_agents"
                )
            ),
            "contact_memories": memories,
            "conversation_kind": "group" if is_group(message) else "direct",
            "is_group": is_group(message), "group_name": message.get("group_name"),
            "remote_number": message.get("remote_number"),
            "unresolved_participant_count": message.get("unresolved_participant_count", 0),
            "mentions": mentions, "rendered_text": rendered_text,
            "original_text": original_text if original_text != rendered_text else None,
            "whatsapp_mentioned": message.get("agent_mentioned") is True,
            "imessage_sources": [{"id": message_id, "text": original_text, "rendered_text": rendered_text,
                                   "mentions": mentions, "agent_mentioned": message.get("agent_mentioned") is True}],
            "participants": participants,
            "reply_to_message_id": message.get("reply_to_message_id"),
            "thread_id": message.get("thread_id"),
            "thread_root_message_id": message.get("thread_root_message_id"),
            "whatsapp_reply_target": message_id,
            "whatsapp_reply_source": reply_source(identity, conversation, message_id),
            "media": bool(media),
        }
        if is_group(message):
            body = ("Group WhatsApp policy: follow the conversation as context. Reply only when addressed or a visible answer is expected. "
                    "For ordinary chatter, use [SILENT].\n" + body)
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
    if unsupported_shape(meta):
        raise ReplyPreflightRejected("WhatsApp requires verified direct or existing group metadata")
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
    if not conversation:
        raise ValueError("WhatsApp reply requires its original conversation")
    guard = CHANNEL_DELIVERY_GUARD.get()
    if meta.get("companion"):
        await gateway._companion_receiver().authorize_reply(chat_id, "whatsapp", meta)
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
    if meta.get("whatsapp_interaction_id"):
        key_parts.append(meta["whatsapp_interaction_id"])
    elif not meta.get("final_reply") and not meta.get("companion"):
        key_parts.append(uuid.uuid4().hex)
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
    if meta.get("companion"):
        gateway._companion_receiver().begin_send(chat_id)
    elif meta.get("final_reply"):
        gateway._channel_store("whatsapp").begin_send(meta, chat_id)

    def checked():
        if guard is not None:
            guard()
        return method(conversation_id=conversation, text=text, idempotency_key=key, **quote)

    try:
        sent = await _to_thread_drained(checked)
    except Exception as exc:
        if public_quota_error(exc):
            raise ReplyPreflightRejected("WhatsApp reply was rejected by current policy; no replacement send") from None
        raise
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

        def persist_send_state():
            if meta.get("companion"):
                if turn is None or not callable(turn.checkpoint):
                    raise PermissionError("Companion send checkpoint is unavailable")
                from copy import deepcopy
                turn.checkpoint("tool_send", reply_meta=deepcopy(meta))
            else:
                from .imessage import IMessageState
                IMessageState(cfg, channel="whatsapp").mark(meta, "running", chat_id=session.chat_id)

        def authorize_effect():
            authorize()
            if meta.get("channel") == "whatsapp" and unsupported_shape(meta):
                raise ReplyPreflightRejected("WhatsApp requires verified direct or existing group metadata")
            if operation != "send_whatsapp":
                validate_send_uncertainty(meta)
            if meta.get("companion"):
                authorize_companion(client, cfg, meta)

        if operation in {"send_whatsapp", "upload_whatsapp_media", "send_whatsapp_reaction", "remove_whatsapp_reaction"}:
            authorize_effect()
        identity = client.get_identity(identity_handle)
        authorize()
        if operation in V2_OPERATIONS:
            def reaction_authority():
                authorize_effect()
                target = require_method(identity, "get_whatsapp_message")(args["message_id"]) if operation == "send_whatsapp_reaction" else require_method(identity, "get_whatsapp_reaction")(args["reaction_id"])
                require_group_metadata(target)
                validate_group_destination(identity, str(field(target, "conversation_id")), cfg, client=client, route=meta)
                authorize_effect()
            return require_group_metadata(run_v2(identity, operation, args, authorize=reaction_authority))
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
            if meta.get("channel") == "whatsapp" and unsupported_shape(meta):
                raise ReplyPreflightRejected("WhatsApp requires verified direct or existing group metadata")
            if ("to" not in args and "conversation_id" not in args
                    and turn is not None and meta.get("channel") == "whatsapp"
                    and meta.get("conversation_id")):
                args["conversation_id"] = meta["conversation_id"]
            recipient, conversation, reply_target = send_selector(args)
            native = (meta.get("channel") == "whatsapp" and conversation is not None
                      and delivery_target(meta, conversation) == meta.get("conversation_id"))
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

            if recipient is not None and not recipient_allowed(recipient):
                raise ReplyPreflightRejected("WhatsApp recipient is no longer locally allowed")
            if conversation:
                validate_group_destination(identity, conversation, cfg, client=client, route=meta)
                authorize()
            if source_bound and not meta.get("companion"):
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
                    persist_send_state()
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
                    persist_send_state()
            authorize()
            try:
                sent = method(
                    **kwargs, idempotency_key=key
                )
            except Exception as exc:
                exc.idempotency_key = key
                exc.whatsapp_media_id = args.get("media_id")
                if (prior_uncertainty or not public_quota_error(exc)
                        or (source_bound and meta.get("whatsapp_reply_source") is not None)):
                    # Uncertain or source-owned rejected sends must not be
                    # replaced by an automatic final with a fresh request key.
                    _mark_tool_delivery("whatsapp", target)
                with send_lock:
                    if attempt is not None:
                        attempt["outcome"] = "rejected" if public_quota_error(exc) and not prior_uncertainty else "uncertain"
                        try:
                            persist_send_state()
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
                    target = returned_target
                _mark_tool_delivery("whatsapp", target)
                if recipient_attempt:
                    try:
                        persist_send_state()
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
                            persist_send_state()
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
        return require_group_metadata(require_method(identity, operation)(**args))

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
            "Send a requested direct WhatsApp message. New sends are plain by default. Set reply_to_message_id selectively for an older message, multiple requests, or a specific attachment. Omit the destination to use the active WhatsApp conversation. Your normal final response already replies automatically. Use one recipient or conversation; text and one owned media ID are optional individually. Preserve the returned request key and media ID after uncertainty; never retry as a new message.",
            {
                "to": {**string("One E.164 recipient; arrays are not supported"), "pattern": r"^\+[1-9][0-9]{6,14}$"},
                "reply_to_message_id": string("Optional visible message ID to quote selectively; omit for plain messages. Requires conversation_id"),
                "conversation_id": string(
                    "Existing direct conversation; mutually exclusive with to"
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
            "List WhatsApp direct and existing group conversations with local unread counts.",
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
