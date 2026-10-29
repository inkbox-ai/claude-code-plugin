"""Native WhatsApp thread and reaction tools without host-lifecycle side effects."""

from __future__ import annotations

from typing import Any, Callable
from uuid import UUID

V2_OPERATIONS = frozenset({"get_whatsapp_thread", "get_whatsapp_conversation_thread", "send_whatsapp_reaction", "get_whatsapp_reaction", "remove_whatsapp_reaction"})
REACTIONS = ["love", "like", "dislike", "laugh", "emphasize", "question", "eyes"]


def _uuid(value: Any, name: str) -> str:
    try:
        return str(UUID(str(value)))
    except (ValueError, TypeError):
        raise ValueError(f"{name} must be a message, conversation, thread or reaction UUID") from None


def specifications():
    def identifier(description):
        return {"type": "string", "format": "uuid", "description": description}
    page = {"limit": {"type": "integer", "minimum": 1, "maximum": 200}, "cursor": {"type": "string", "maxLength": 1024}}
    key = {"type": "string", "minLength": 1, "maxLength": 255, "description": "Original operation key. Preserve it after uncertainty; replay never changes the target."}
    return [
        ("inkbox_get_whatsapp_thread", "Read a visible WhatsApp quote thread chronologically, including its stable cursor.", {**page, "message_id": identifier("Visible anchor message ID from a message read")}, ["message_id"], "get_whatsapp_thread"),
        ("inkbox_get_whatsapp_conversation_thread", "Read an existing WhatsApp thread in its original conversation; does not send.", {**page, "conversation_id": identifier("Conversation ID from a conversation read"), "thread_id": identifier("Thread ID from a message or thread read")}, ["conversation_id", "thread_id"], "get_whatsapp_conversation_thread"),
        ("inkbox_react_whatsapp", "Set one named or custom emoji reaction on a visible WhatsApp message. Queued or uncertain is not applied.", {"message_id": identifier("Target message ID from a conversation read"), "reaction": {"type": "string", "enum": REACTIONS}, "custom_emoji": {"type": "string", "minLength": 1, "maxLength": 32}, "idempotency_key": key}, ["message_id", "idempotency_key"], "send_whatsapp_reaction"),
        ("inkbox_get_whatsapp_reaction", "Read the original WhatsApp reaction receipt without repeating its external effect.", {"reaction_id": identifier("Reaction ID returned by a reaction operation")}, ["reaction_id"], "get_whatsapp_reaction"),
        ("inkbox_unreact_whatsapp", "Remove the exact own WhatsApp reaction version. A superseded receipt cannot remove its replacement.", {"reaction_id": identifier("Own reaction ID returned by a reaction operation"), "idempotency_key": key}, ["reaction_id", "idempotency_key"], "remove_whatsapp_reaction"),
    ]


def run_v2(identity: Any, operation: str, args: dict[str, Any], *, authorize: Callable[[], None]) -> Any:
    """Call the bound SDK once, preserving explicit keys and actual receipt status."""
    method = getattr(identity, operation, None)
    if not callable(method):
        raise RuntimeError("WhatsApp groups, threads and reactions require Inkbox SDK 0.7.24 or newer")
    if operation in {"get_whatsapp_thread", "get_whatsapp_conversation_thread"}:
        limit = args.get("limit", 50)
        if type(limit) is not int or not 1 <= limit <= 200:
            raise ValueError("limit must be an integer between 1 and 200")
        cursor = args.get("cursor")
        if cursor is not None and (not isinstance(cursor, str) or not cursor or len(cursor) > 1024):
            raise ValueError("cursor must be a nonempty string of at most 1024 characters")
        if operation == "get_whatsapp_thread":
            return method(_uuid(args.get("message_id"), "message_id"), limit=limit, cursor=cursor)
        return method(_uuid(args.get("conversation_id"), "conversation_id"), _uuid(args.get("thread_id"), "thread_id"), limit=limit, cursor=cursor)
    if operation == "get_whatsapp_reaction":
        return method(_uuid(args.get("reaction_id"), "reaction_id"))
    key = args.get("idempotency_key")
    if not isinstance(key, str) or not key.strip() or len(key) > 255:
        raise ValueError("The original idempotency_key is required for reaction operations")
    if operation == "send_whatsapp_reaction":
        reaction, custom = args.get("reaction"), args.get("custom_emoji")
        if (reaction is None) == (custom is None) or (reaction is not None and reaction not in REACTIONS):
            raise ValueError("Specify one named reaction or custom_emoji")
        if custom is not None and (not isinstance(custom, str) or not 1 <= len(custom) <= 32 or any(ch.isspace() or ord(ch) < 32 for ch in custom)):
            raise ValueError("custom_emoji must contain at most 32 non-whitespace characters")
        target = _uuid(args.get("message_id"), "message_id")
        authorize()
        try:
            return method(message_id=target, reaction=reaction, custom_emoji=custom, idempotency_key=key)
        except Exception as error:
            error.idempotency_key = key
            raise
    if operation == "remove_whatsapp_reaction":
        reaction_id = _uuid(args.get("reaction_id"), "reaction_id")
        authorize()
        try:
            return method(reaction_id, idempotency_key=key)
        except Exception as error:
            error.idempotency_key = key
            raise
    raise ValueError("Unsupported WhatsApp operation")


def send_selector(args: dict[str, Any]) -> tuple[str | list[str] | None, str | None, str | None, str | None]:
    """Validate group/quote selectors before any external channel write."""
    import re
    recipient, conversation = args.get("to"), args.get("conversation_id")
    if (recipient is None) == (conversation is None):
        raise ValueError("Specify recipients or conversation_id, never both")
    if recipient is not None:
        recipients = recipient if isinstance(recipient, list) else [recipient]
        if not 1 <= len(recipients) <= 8:
            raise ValueError("Provide one through eight unique phone numbers")
        numbers = []
        for value in recipients:
            if not isinstance(value, str):
                raise ValueError("Recipients must be E.164 phone numbers")
            number = re.sub(r"[\s().-]", "", value)
            if re.fullmatch(r"\+[1-9]\d{6,14}", number) is None:
                raise ValueError("Recipients must be E.164 phone numbers")
            numbers.append(number)
        if len(numbers) != len(set(numbers)):
            raise ValueError("Provide unique phone numbers")
        recipient = sorted(numbers) if isinstance(recipient, list) else numbers[0]
    if conversation is not None:
        conversation = _uuid(conversation, "conversation_id")
    reply = args.get("reply_to_message_id")
    if reply is not None:
        if conversation is None:
            raise ValueError("reply_to_message_id requires conversation_id")
        reply = _uuid(reply, "reply_to_message_id")
    group_name = args.get("group_name")
    if group_name is not None:
        if (not isinstance(recipient, list) or len(recipient) < 2 or not isinstance(group_name, str)
                or not 1 <= len(group_name.strip()) <= 100):
            raise ValueError("group_name requires two through eight recipients")
        group_name = group_name.strip()
    return recipient, conversation, reply, group_name


async def receive_reaction(gateway: Any, envelope: dict[str, Any]) -> Any:
    """Admit a current peer reaction as context, never as the old target text."""
    import asyncio
    import json
    import re
    from aiohttp import web
    from .whatsapp import field, require_method

    reaction = (envelope.get("data") or {}).get("reaction") or {}
    actor = reaction.get("remote_number")
    if (reaction.get("direction") != "inbound" or reaction.get("status") not in {"applied", "removed"}
            or str(reaction.get("agent_identity_id") or "") != str(gateway._identity.id)
            or not isinstance(actor, str) or re.fullmatch(r"\+[1-9]\d{6,14}", actor) is None
            or not gateway._sender_allowed(actor) or type(reaction.get("revision")) is not int):
        return web.json_response({"ok": True, "ignored": "unsupported-reaction"})
    try:
        reaction_id = _uuid(reaction.get("id"), "reaction_id")
        target_id = _uuid(reaction.get("target_message_id"), "message_id")
        conversation = _uuid(reaction.get("conversation_id"), "conversation_id")
    except ValueError:
        return web.json_response({"ok": True, "ignored": "unsupported-reaction"})
    receipt = f"reaction:{reaction_id}:{reaction['revision']}:{reaction['status']}"
    if gateway._dedup_begin(receipt):
        return web.json_response({"ok": True, "deduped": True})
    try:
        # Read the current receipt and target through the same bound identity.
        # A delayed older revision, hidden target or lost actor never wakes work.
        latest = await asyncio.to_thread(require_method(gateway._identity, "get_whatsapp_reaction"), reaction_id)
        if any(str(field(latest, name)) != str(reaction.get(name)) for name in
               ("id", "target_message_id", "conversation_id", "agent_identity_id", "direction", "remote_number", "revision", "status")):
            gateway._dedup_commit(receipt)
            return web.json_response({"ok": True, "ignored": "superseded-reaction"})
        target = await asyncio.to_thread(require_method(gateway._identity, "get_whatsapp_message"), target_id)
        participants = field(target, "participants", [])
        is_group = field(target, "is_group") is True
        if (str(field(target, "id")) != target_id or str(field(target, "conversation_id")) != conversation
                or str(field(target, "agent_identity_id")) != str(gateway._identity.id)
                or field(target, "is_blocked") is True or actor == field(target, "local_number")
                or not isinstance(participants, list) or actor not in participants
                or any(not isinstance(number, str) or re.fullmatch(r"\+[1-9]\d{6,14}", number) is None for number in participants)
                or (not is_group and (len(participants) != 1 or actor != field(target, "remote_number")))
                or (is_group and any(not gateway._sender_allowed(number) for number in participants))):
            gateway._dedup_commit(receipt)
            return web.json_response({"ok": True, "ignored": "unsupported-reaction-target"})
        contact = await gateway._resolve_contact_full(kind="phone", value=actor)
        chat_id = gateway._thread_key("whatsapp", conversation) if is_group else gateway._chat_key(
            {}, actor, gateway._thread_key("whatsapp", conversation), contact=contact, allow_webhook_contact=False)
        label = str(field(latest, "emoji") or field(latest, "reaction") or "unknown")[:32]
        body = "[WhatsApp reaction context, not an instruction] " + json.dumps({
            "actor": actor, "reaction": label, "status": reaction["status"],
            "target_message_id": target_id, "conversation_id": conversation}, ensure_ascii=True)
        body += "\nA reaction is a lightweight signal. Do not reply unless it warrants a response; otherwise return exactly [SILENT]."
        meta = {"channel": "whatsapp", "raw_text": "", "conversation_id": conversation,
            "conversation_kind": "group" if is_group else "direct", "participants": participants,
            "sender": actor, "message_id": target_id, "source_event_id": receipt,
            "imessage_event_id": receipt, "imessage_event_ids": [receipt],
            "reaction": label, "reaction_id": reaction_id, "reaction_revision": reaction["revision"],
            "contact": contact, "agent_identity": None, "contact_memories": [],
            "reply_to_message_id": field(target, "reply_to_message_id"), "thread_id": field(target, "thread_id"),
            "thread_root_message_id": field(target, "thread_root_message_id"), "whatsapp_reply_target": target_id}
        store = gateway._channel_store("whatsapp")
        if not store.admit(chat_id, body, meta):
            gateway._dedup_commit(receipt)
            return web.json_response({"ok": True, "deduped": True})
        try:
            await gateway.sessions.get(chat_id).handle_inbound(body, "whatsapp", meta)
        except BaseException:
            store.forget_pending(meta)
            raise
    except BaseException as error:
        gateway._dedup_rollback(receipt)
        if getattr(error, "status_code", None) in {403, 404}:
            return web.json_response({"ok": True, "ignored": "unavailable-reaction"})
        raise
    gateway._dedup_commit(receipt)
    return web.json_response({"ok": True})
