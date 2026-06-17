"""Inkbox messaging tools exposed to Claude Code via in-process MCP.

Mirrors the hermes-agent-plugin direct-tool surface: whoami, outbound
email/SMS/iMessage, and text-conversation triage. The Inkbox SDK is
synchronous, so every call is pushed onto a thread.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
import mimetypes
from pathlib import Path
from typing import Any, Dict, List, Tuple

try:
    from .media import file_to_email_attachment
except ImportError:  # pragma: no cover - direct local import/test fallback
    from media import file_to_email_attachment

try:
    from claude_agent_sdk import create_sdk_mcp_server, tool

    CLAUDE_SDK_AVAILABLE = True
except ImportError:  # pragma: no cover - doctor reports this cleanly
    create_sdk_mcp_server = tool = None  # type: ignore
    CLAUDE_SDK_AVAILABLE = False

logger = logging.getLogger(__name__)


def _json_safe(value: Any) -> Any:
    """Convert SDK dataclasses (UUIDs, datetimes, enums) into JSON-safe data."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: _json_safe(getattr(value, field.name))
            for field in dataclasses.fields(value)
        }
    return str(getattr(value, "value", value))


def _result(data: Any) -> Dict[str, Any]:
    return {"content": [{"type": "text", "text": json.dumps(_json_safe(data), ensure_ascii=False)}]}


def _error(message: str) -> Dict[str, Any]:
    return {"content": [{"type": "text", "text": json.dumps({"error": message})}], "is_error": True}


def _upload_media_url(identity: Any, path: str) -> str:
    """Upload a local file via the SDK and return its hosted media URL.

    The send tools call this so attaching a local file is one tool call for the
    agent, even though it's an upload-then-send round trip under the hood.

    Args:
        identity (Any): The agent identity (has ``upload_imessage_media``).
        path (str): Local file path to upload.

    Returns:
        str: The hosted ``media_url`` to pass as a ``media_urls`` entry.
    """
    resolved = Path(path).expanduser()
    upload = identity.upload_imessage_media(
        content=resolved.read_bytes(),
        filename=resolved.name,
        content_type=mimetypes.guess_type(resolved.name)[0],
    )
    return upload.media_url


def build_inkbox_mcp_server(client: Any, identity_handle: str) -> Tuple[Any, List[str]]:
    """Build the in-process MCP server carrying the Inkbox tools.

    Args:
        client (Inkbox): Authenticated Inkbox SDK client.
        identity_handle (str): Agent identity handle the tools act as.

    Returns:
        Tuple[Any, List[str]]: (sdk mcp server, fully-qualified tool names
        for ``allowed_tools``, e.g. ``mcp__inkbox__inkbox_send_sms``).
    """
    if not CLAUDE_SDK_AVAILABLE:
        raise RuntimeError("claude-agent-sdk is not installed")

    def _identity():
        return client.get_identity(identity_handle)

    @tool(
        "inkbox_whoami",
        "Show this agent's Inkbox identity: handle, email address, and phone number.",
        {},
    )
    async def inkbox_whoami(args: Dict[str, Any]) -> Dict[str, Any]:
        def _run():
            identity = _identity()
            phone = identity.phone_number
            mailbox = identity.mailbox
            return {
                "handle": identity.agent_handle,
                "email": getattr(mailbox, "email_address", None),
                "phone": getattr(phone, "number", None),
                "imessage_enabled": getattr(identity, "imessage_enabled", False),
            }

        try:
            return _result(await asyncio.to_thread(_run))
        except Exception as exc:
            return _error(str(exc))

    @tool(
        "inkbox_send_email",
        "Send an email from this agent's Inkbox mailbox. To attach files, pass "
        "attachment_paths as a list of local file paths (max ~25 MB total).",
        {"to": str, "subject": str, "body": str, "attachment_paths": list},
    )
    async def inkbox_send_email(args: Dict[str, Any]) -> Dict[str, Any]:
        def _run():
            paths = args.get("attachment_paths") or []
            attachments = [file_to_email_attachment(str(p)) for p in paths] or None
            msg = _identity().send_email(
                to=[str(args["to"])],
                subject=str(args.get("subject") or ""),
                body_text=str(args.get("body") or ""),
                attachments=attachments,
            )
            return {"sent": True, "id": str(getattr(msg, "id", "")), "attachments": len(paths)}

        try:
            return _result(await asyncio.to_thread(_run))
        except Exception as exc:
            return _error(str(exc))

    @tool(
        "inkbox_send_sms",
        "Send an SMS/MMS from this agent's Inkbox phone number. Reply in a thread "
        "with conversation_id, or start one with to (E.164). To send images/files "
        "(MMS), pass media_paths as a list of LOCAL file paths — they're uploaded "
        "and attached for you (each up to 10 MB). Already-hosted URLs may instead "
        "be passed as media_urls.",
        {"to": str, "text": str, "media_paths": list, "media_urls": list},
    )
    async def inkbox_send_sms(args: Dict[str, Any]) -> Dict[str, Any]:
        def _run():
            identity = _identity()
            kwargs: Dict[str, Any] = {"text": str(args.get("text") or "")}
            target = str(args.get("to") or "").strip()
            if target.startswith("+"):
                kwargs["to"] = target
            else:
                kwargs["conversation_id"] = target
            # One tool call for the agent; the upload→send two-step is internal.
            urls = [str(u) for u in (args.get("media_urls") or [])]
            for path in (args.get("media_paths") or []):
                urls.append(_upload_media_url(identity, str(path)))
            if urls:
                kwargs["media_urls"] = urls
            msg = identity.send_text(**kwargs)
            return {"sent": True, "id": str(getattr(msg, "id", "")), "media": len(urls)}

        try:
            return _result(await asyncio.to_thread(_run))
        except Exception as exc:
            return _error(str(exc))

    @tool(
        "inkbox_send_imessage",
        "Send an iMessage. Pass an existing conversation_id — get it from "
        "inkbox_list_imessage_conversations (iMessage is recipient-first: a "
        "conversation exists only after the person has messaged this agent). To "
        "attach an image/file, pass media_path as a local file path (uploaded "
        "automatically, max 10 MB).",
        {"conversation_id": str, "text": str, "media_path": str},
    )
    async def inkbox_send_imessage(args: Dict[str, Any]) -> Dict[str, Any]:
        def _run():
            identity = _identity()
            kwargs: Dict[str, Any] = {
                "conversation_id": str(args["conversation_id"]),
                "text": str(args.get("text") or ""),
            }
            media_path = str(args.get("media_path") or "").strip()
            if media_path:
                # One tool call for the agent; the upload→send two-step is internal.
                kwargs["media_urls"] = [_upload_media_url(identity, media_path)]
            msg = identity.send_imessage(**kwargs)
            return {"sent": True, "id": str(getattr(msg, "id", ""))}

        try:
            return _result(await asyncio.to_thread(_run))
        except Exception as exc:
            return _error(str(exc))

    @tool(
        "inkbox_list_text_conversations",
        "List this agent's SMS conversations, newest first.",
        {"limit": int},
    )
    async def inkbox_list_text_conversations(args: Dict[str, Any]) -> Dict[str, Any]:
        def _run():
            return _identity().list_text_conversations(limit=int(args.get("limit") or 25))

        try:
            return _result(await asyncio.to_thread(_run))
        except Exception as exc:
            return _error(str(exc))

    @tool(
        "inkbox_list_imessage_conversations",
        "List this agent's iMessage conversations (conversation_id + the "
        "remote number), newest first. Use this to find the conversation_id "
        "to pass to inkbox_send_imessage.",
        {"limit": int},
    )
    async def inkbox_list_imessage_conversations(args: Dict[str, Any]) -> Dict[str, Any]:
        def _run():
            return _identity().list_imessage_conversations(limit=int(args.get("limit") or 25))

        try:
            return _result(await asyncio.to_thread(_run))
        except Exception as exc:
            return _error(str(exc))

    @tool(
        "inkbox_get_imessage_conversation",
        "Fetch message history for one iMessage conversation by conversation_id.",
        {"conversation_id": str, "limit": int},
    )
    async def inkbox_get_imessage_conversation(args: Dict[str, Any]) -> Dict[str, Any]:
        def _run():
            return _identity().get_imessage_conversation(
                str(args["conversation_id"]), limit=int(args.get("limit") or 50)
            )

        try:
            return _result(await asyncio.to_thread(_run))
        except Exception as exc:
            return _error(str(exc))

    @tool(
        "inkbox_get_text_conversation",
        "Fetch message history for one SMS conversation by conversation_id.",
        {"conversation_id": str, "limit": int},
    )
    async def inkbox_get_text_conversation(args: Dict[str, Any]) -> Dict[str, Any]:
        def _run():
            return _identity().get_text_conversation(
                str(args["conversation_id"]), limit=int(args.get("limit") or 50)
            )

        try:
            return _result(await asyncio.to_thread(_run))
        except Exception as exc:
            return _error(str(exc))

    # ------------------------------------------------------------------
    # Contacts — the org address book, filtered server-side to what this
    # identity may see (ported from hermes-agent-plugin's contact-lookup).
    # ------------------------------------------------------------------

    @tool(
        "inkbox_lookup_contact",
        "Reverse-lookup contacts by exactly ONE field. The cheapest way to "
        "resolve a known email/phone to a person. Pass one of: email, phone, "
        "email_domain, email_contains, phone_contains.",
        {"email": str, "phone": str, "email_domain": str,
         "email_contains": str, "phone_contains": str},
    )
    async def inkbox_lookup_contact(args: Dict[str, Any]) -> Dict[str, Any]:
        def _run():
            keys = ("email", "phone", "email_domain", "email_contains", "phone_contains")
            supplied = {k: str(args[k]) for k in keys if args.get(k)}
            if len(supplied) != 1:
                raise ValueError("pass exactly one of: " + ", ".join(keys))
            return client.contacts.lookup(**supplied)

        try:
            return _result(await asyncio.to_thread(_run))
        except Exception as exc:
            return _error(str(exc))

    @tool(
        "inkbox_list_contacts",
        "Search the address book by free text (matches name, company, job "
        "title, notes). Use for name-based queries like 'find Ada'. "
        "order is 'recent' or 'name'.",
        {"q": str, "order": str, "limit": int},
    )
    async def inkbox_list_contacts(args: Dict[str, Any]) -> Dict[str, Any]:
        def _run():
            return client.contacts.list(
                q=str(args["q"]) if args.get("q") else None,
                order=str(args["order"]) if args.get("order") else None,
                limit=int(args.get("limit") or 25),
            )

        try:
            return _result(await asyncio.to_thread(_run))
        except Exception as exc:
            return _error(str(exc))

    @tool(
        "inkbox_get_contact",
        "Fetch one contact's full record (all emails, phones, addresses, notes) "
        "by its contact id.",
        {"contact_id": str},
    )
    async def inkbox_get_contact(args: Dict[str, Any]) -> Dict[str, Any]:
        def _run():
            return client.contacts.get(str(args["contact_id"]))

        try:
            return _result(await asyncio.to_thread(_run))
        except Exception as exc:
            return _error(str(exc))

    @tool(
        "inkbox_create_contact",
        "Save a new contact in the address book. Provide any of given_name, "
        "family_name, preferred_name, company_name, job_title, notes, and "
        "emails / phones as lists of strings (first entry is marked primary).",
        {"given_name": str, "family_name": str, "preferred_name": str,
         "company_name": str, "job_title": str, "notes": str,
         "emails": list, "phones": list},
    )
    async def inkbox_create_contact(args: Dict[str, Any]) -> Dict[str, Any]:
        def _run():
            from inkbox import ContactEmail, ContactPhone
            emails = [ContactEmail(label=None, value=str(e), is_primary=(i == 0))
                      for i, e in enumerate(args.get("emails") or [])]
            phones = [ContactPhone(label=None, value=str(p), is_primary=(i == 0))
                      for i, p in enumerate(args.get("phones") or [])]
            return client.contacts.create(
                given_name=str(args["given_name"]) if args.get("given_name") else None,
                family_name=str(args["family_name"]) if args.get("family_name") else None,
                preferred_name=str(args["preferred_name"]) if args.get("preferred_name") else None,
                company_name=str(args["company_name"]) if args.get("company_name") else None,
                job_title=str(args["job_title"]) if args.get("job_title") else None,
                notes=str(args["notes"]) if args.get("notes") else None,
                emails=emails or None,
                phones=phones or None,
            )

        try:
            return _result(await asyncio.to_thread(_run))
        except Exception as exc:
            return _error(str(exc))

    @tool(
        "inkbox_update_contact",
        "Update an existing contact by id (look it up first). Only the fields "
        "you pass change; emails / phones replace the whole list (strings, first "
        "is primary).",
        {"contact_id": str, "given_name": str, "family_name": str,
         "preferred_name": str, "company_name": str, "job_title": str,
         "notes": str, "emails": list, "phones": list},
    )
    async def inkbox_update_contact(args: Dict[str, Any]) -> Dict[str, Any]:
        def _run():
            from inkbox import ContactEmail, ContactPhone
            # Only forward fields the caller actually supplied (the SDK leaves
            # omitted fields untouched).
            kwargs: Dict[str, Any] = {}
            for field in ("given_name", "family_name", "preferred_name",
                          "company_name", "job_title", "notes"):
                if args.get(field):
                    kwargs[field] = str(args[field])
            if args.get("emails") is not None and args.get("emails") != "":
                kwargs["emails"] = [
                    ContactEmail(label=None, value=str(e), is_primary=(i == 0))
                    for i, e in enumerate(args.get("emails") or [])
                ]
            if args.get("phones") is not None and args.get("phones") != "":
                kwargs["phones"] = [
                    ContactPhone(label=None, value=str(p), is_primary=(i == 0))
                    for i, p in enumerate(args.get("phones") or [])
                ]
            return client.contacts.update(str(args["contact_id"]), **kwargs)

        try:
            return _result(await asyncio.to_thread(_run))
        except Exception as exc:
            return _error(str(exc))

    @tool(
        "inkbox_export_contact_vcard",
        "Export one contact as a vCard 4.0 string by its contact id.",
        {"contact_id": str},
    )
    async def inkbox_export_contact_vcard(args: Dict[str, Any]) -> Dict[str, Any]:
        def _run():
            return {"vcard": client.contacts.vcards.export_vcard(str(args["contact_id"]))}

        try:
            return _result(await asyncio.to_thread(_run))
        except Exception as exc:
            return _error(str(exc))

    tools = [
        inkbox_whoami,
        inkbox_send_email,
        inkbox_send_sms,
        inkbox_send_imessage,
        inkbox_list_text_conversations,
        inkbox_get_text_conversation,
        inkbox_list_imessage_conversations,
        inkbox_get_imessage_conversation,
        inkbox_lookup_contact,
        inkbox_list_contacts,
        inkbox_get_contact,
        inkbox_create_contact,
        inkbox_update_contact,
        inkbox_export_contact_vcard,
    ]
    server = create_sdk_mcp_server(name="inkbox", version="0.1.0", tools=tools)
    tool_names = [
        "mcp__inkbox__inkbox_whoami",
        "mcp__inkbox__inkbox_send_email",
        "mcp__inkbox__inkbox_send_sms",
        "mcp__inkbox__inkbox_send_imessage",
        "mcp__inkbox__inkbox_list_text_conversations",
        "mcp__inkbox__inkbox_get_text_conversation",
        "mcp__inkbox__inkbox_list_imessage_conversations",
        "mcp__inkbox__inkbox_get_imessage_conversation",
        "mcp__inkbox__inkbox_lookup_contact",
        "mcp__inkbox__inkbox_list_contacts",
        "mcp__inkbox__inkbox_get_contact",
        "mcp__inkbox__inkbox_create_contact",
        "mcp__inkbox__inkbox_update_contact",
        "mcp__inkbox__inkbox_export_contact_vcard",
    ]
    return server, tool_names
