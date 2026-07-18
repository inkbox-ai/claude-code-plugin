"""Inkbox messaging tools exposed to Claude Code via in-process MCP.

Whoami, outbound email/SMS/iMessage, calls, contacts, and
text-conversation triage. The Inkbox SDK is synchronous, so every call
is pushed onto a thread.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
import mimetypes
import secrets
import time
from contextvars import ContextVar
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

try:
    from .media import file_to_email_attachment
except ImportError:  # pragma: no cover - direct local import/test fallback
    from media import file_to_email_attachment

try:
    from .config import INKBOX_WS_PATH, call_contexts_dir
except ImportError:  # pragma: no cover - direct local import/test fallback
    from config import INKBOX_WS_PATH, call_contexts_dir

try:
    from claude_agent_sdk import create_sdk_mcp_server, tool

    CLAUDE_SDK_AVAILABLE = True
except ImportError:  # pragma: no cover - doctor reports this cleanly
    create_sdk_mcp_server = tool = None  # type: ignore
    CLAUDE_SDK_AVAILABLE = False

logger = logging.getLogger(__name__)

SMS_MAX_LENGTH = 1600
IMESSAGE_MAX_LENGTH = 18995

# The contact session whose turn is driving the current tool call. Each
# session binds itself here right before its agent client connects, so the
# client's tool-dispatch tasks inherit a reference to it; the session object
# is long-lived and its ``mode`` mutates per inbound message, giving tools a
# live view of the conversation's channel.
CURRENT_SESSION: ContextVar[Any] = ContextVar("inkbox_claude_current_session", default=None)


def _mark_tool_delivery(mode: str, target: str) -> None:
    """Tell the active session that this tool already delivered its reply."""
    session = CURRENT_SESSION.get()
    mark = getattr(session, "mark_tool_delivery", None)
    if callable(mark):
        mark(mode, target)


def _current_channel_hint() -> Optional[str]:
    """Which Inkbox channel is the current agent turn happening on?

    Reads the bound session's last inbound modality (concurrency-safe: each
    agent client's dispatch tasks see only their own session).

    Returns:
        Optional[str]: ``"imessage"`` | ``"dedicated"`` | ``None`` (unknown /
        not in a gateway turn, e.g. tests or a non-phone channel).
    """
    session = CURRENT_SESSION.get()
    mode = str(getattr(session, "mode", "") or "").strip().lower()
    if mode == "imessage":
        return "imessage"
    if mode in {"sms", "voice"}:
        return "dedicated"
    return None


def _resolve_call_origination(identity: Any, explicit: str) -> Optional[str]:
    """Pick which line an outbound call originates from.

    Calls can go out over two paths: the agent's own ``dedicated_number`` or
    the ``shared_imessage_number`` it's already messaging the recipient on.
    Resolution order:

    1. An explicit choice (from the agent) always wins.
    2. If only one path exists, use it (dedicated number but no iMessage →
       dedicated; iMessage enabled but no number → shared).
    3. If BOTH exist, follow the channel the current conversation is on — an
       iMessage turn calls over the shared iMessage line, an SMS/phone turn
       over the dedicated number.  This makes "call me" do the right thing
       without the agent having to specify the line.
    4. If both exist but we can't tell the channel, default to the dedicated
       number (the open line that can reach anyone).

    Args:
        identity (Any): The agent identity (``phone_number`` +
            ``imessage_enabled`` are read).
        explicit (str): The agent's explicit ``origination`` arg, if any.

    Returns:
        Optional[str]: The resolved origination, or None when neither path
        exists (nothing to call from).
    """
    explicit = (explicit or "").strip().lower()
    if explicit in {"dedicated_number", "shared_imessage_number"}:
        return explicit
    has_number = getattr(identity, "phone_number", None) is not None
    imessage_enabled = bool(getattr(identity, "imessage_enabled", False))
    if has_number and imessage_enabled:
        # Both lines available — follow the conversation's channel.
        return "shared_imessage_number" if _current_channel_hint() == "imessage" else "dedicated_number"
    if has_number:
        return "dedicated_number"
    if imessage_enabled:
        return "shared_imessage_number"
    return None


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


def _error(message: str, **fields: Any) -> Dict[str, Any]:
    payload = {"error": message, **fields}
    return {
        "content": [{"type": "text", "text": json.dumps(_json_safe(payload), ensure_ascii=False)}],
        "is_error": True,
    }


def _log_send_rejection(tool_name: str, exc: Exception) -> None:
    """Surface a rejected tool send in the gateway log.

    When the agent sends directly via a tool (not a normal reply), a server
    content-policy rejection comes back inline as the tool result. Logging the
    rule slug (e.g. ``message_blocked_spam_filter``) leaves the same
    delivery-failure fingerprint the wake-up path logs, so operators (and the
    live retry test) can see the block reached the agent by either route.

    Args:
        tool_name (str): The send tool that was rejected.
        exc (Exception): The exception the send raised.

    Returns:
        None
    """
    detail = getattr(exc, "detail", None)
    rule = ""
    if isinstance(detail, dict):
        rule = str(detail.get("error") or "").strip()
        sub_rule = str(detail.get("rule") or "").strip()
        if rule and sub_rule:
            rule = f"{rule} rule={sub_rule}"
    logger.warning("[bridge] %s rejected: %s", tool_name, rule or str(exc))


def _message_too_long_reason(channel: str, content: str, max_chars: int) -> str:
    char_count = len(content or "")
    return (
        f"{channel} text is {char_count} characters; maximum is {max_chars}. "
        f"Shorten it or split it into smaller {channel} messages."
    )


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


def _append_query_param(raw_url: str, key: str, value: str) -> str:
    """Append or replace one query param while preserving the rest."""
    parts = urlparse(raw_url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query[key] = value
    return urlunparse(parts._replace(query=urlencode(query)))


def _write_call_context(
    *, purpose: str, opening_message: str, context: str, to_number: str
) -> str:
    """Persist outbound-call context for the gateway to load on connect."""
    token = secrets.token_urlsafe(18)
    payload = {
        "created_at": time.time(),
        "purpose": purpose,
        "opening_message": opening_message,
        "context": context,
        "to_number": to_number,
    }
    (call_contexts_dir() / f"{token}.json").write_text(
        json.dumps(payload, indent=2) + "\n"
    )
    return token


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
        "Show this agent's Inkbox identity: handle, email address, and its two "
        "calling lines (dedicated phone number + shared iMessage line).",
        {},
    )
    async def inkbox_whoami(args: Dict[str, Any]) -> Dict[str, Any]:
        def _run():
            identity = _identity()
            phone = identity.phone_number
            mailbox = identity.mailbox
            # Present the two lines with explicit labels so the agent
            # describes them correctly: its OWN dedicated phone line vs the
            # SHARED iMessage line. The dedicated number is the one for SMS +
            # voice; the iMessage line's number is managed by Inkbox and
            # never surfaced.
            dedicated_number = getattr(phone, "number", None)
            imessage_enabled = bool(getattr(identity, "imessage_enabled", False))
            return {
                "handle": identity.agent_handle,
                "email": getattr(mailbox, "email_address", None),
                "phone": dedicated_number,
                "imessage_enabled": imessage_enabled,
                "lines": {
                    "dedicated_phone_line": dedicated_number or "(none provisioned)",
                    "dedicated_phone_line_note": (
                        "Your own phone line for SMS and voice calls. Call from it with "
                        "origination=dedicated_number."
                    ),
                    "shared_imessage_line": "enabled" if imessage_enabled else "disabled",
                    "shared_imessage_line_note": (
                        "Voice + iMessage with people connected to you over iMessage. Its "
                        "number is managed by Inkbox and not shown. Call over it with "
                        "origination=shared_imessage_number."
                    ),
                },
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
        target = str(args["to"])

        def _run():
            paths = args.get("attachment_paths") or []
            attachments = [file_to_email_attachment(str(p)) for p in paths] or None
            msg = _identity().send_email(
                to=[target],
                subject=str(args.get("subject") or ""),
                body_text=str(args.get("body") or ""),
                attachments=attachments,
            )
            return {"sent": True, "id": str(getattr(msg, "id", "")), "attachments": len(paths)}

        try:
            result = await asyncio.to_thread(_run)
            _mark_tool_delivery("email", target)
            return _result(result)
        except Exception as exc:
            return _error(str(exc))

    @tool(
        "inkbox_send_sms",
        "Send an SMS/MMS from this agent's Inkbox phone number. Reply in a thread "
        "with conversation_id, or start one with to (E.164). To send images/files "
        "(MMS), pass media_paths as a list of LOCAL file paths — they're uploaded "
        "and attached for you (each up to 10 MB). Already-hosted URLs may instead "
        "be passed as media_urls. Text is limited to 1600 characters.",
        {"to": str, "text": str, "media_paths": list, "media_urls": list},
    )
    async def inkbox_send_sms(args: Dict[str, Any]) -> Dict[str, Any]:
        text = str(args.get("text") or "")
        target = str(args.get("to") or "").strip()
        if len(text) > SMS_MAX_LENGTH:
            return _error(
                _message_too_long_reason("SMS", text, SMS_MAX_LENGTH),
                error_code="sms_too_long",
                char_count=len(text),
                max_chars=SMS_MAX_LENGTH,
            )

        def _run():
            identity = _identity()
            kwargs: Dict[str, Any] = {"text": text}
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
            result = await asyncio.to_thread(_run)
            _mark_tool_delivery("sms", target)
            return _result(result)
        except Exception as exc:
            _log_send_rejection("inkbox_send_sms", exc)
            return _error(str(exc))

    @tool(
        "inkbox_send_imessage",
        "Send an iMessage. Pass an existing conversation_id — get it from "
        "inkbox_list_imessage_conversations (iMessage is recipient-first: a "
        "conversation exists only after the person has messaged this agent). To "
        "attach an image/file, pass media_path as a local file path (uploaded "
        "automatically, max 10 MB). Text is limited to 18995 characters.",
        {"conversation_id": str, "text": str, "media_path": str},
    )
    async def inkbox_send_imessage(args: Dict[str, Any]) -> Dict[str, Any]:
        text = str(args.get("text") or "")
        conversation_id = str(args["conversation_id"])
        if len(text) > IMESSAGE_MAX_LENGTH:
            return _error(
                _message_too_long_reason("iMessage", text, IMESSAGE_MAX_LENGTH),
                error_code="imessage_too_long",
                char_count=len(text),
                max_chars=IMESSAGE_MAX_LENGTH,
            )

        def _run():
            identity = _identity()
            kwargs: Dict[str, Any] = {
                "conversation_id": conversation_id,
                "text": text,
            }
            media_path = str(args.get("media_path") or "").strip()
            if media_path:
                # One tool call for the agent; the upload→send two-step is internal.
                kwargs["media_urls"] = [_upload_media_url(identity, media_path)]
            msg = identity.send_imessage(**kwargs)
            return {"sent": True, "id": str(getattr(msg, "id", ""))}

        try:
            result = await asyncio.to_thread(_run)
            _mark_tool_delivery("imessage", conversation_id)
            return _result(result)
        except Exception as exc:
            _log_send_rejection("inkbox_send_imessage", exc)
            return _error(str(exc))

    @tool(
        "inkbox_place_call",
        "Place an outbound voice call. Calls can go out over two lines: your own "
        "dedicated phone number, or the shared Inkbox iMessage line you are "
        "already messaging the recipient on. Match the channel you're talking on "
        "— call SMS/phone contacts from your dedicated number "
        '(origination "dedicated_number"), and call an iMessage contact over the '
        'shared iMessage line (origination "shared_imessage_number"; only works '
        "if they are connected to you over iMessage, otherwise the call is "
        "rejected). If origination is omitted it is resolved automatically. The "
        "call's audio bridges to the running gateway. Always pass purpose so the "
        "live call opens with context; optionally pass opening_message and context.",
        {"to_number": str, "purpose": str, "origination": str, "opening_message": str, "context": str},
    )
    async def inkbox_place_call(args: Dict[str, Any]) -> Dict[str, Any]:
        def _run():
            to_number = str(args.get("to_number") or "").strip()
            if not to_number:
                raise ValueError("to_number is required (E.164, e.g. +15551234567)")
            purpose = str(args.get("purpose") or "").strip()
            if not purpose:
                raise ValueError(
                    "purpose is required so the live call opens with context"
                )
            identity = _identity()

            # Resolve the outbound line (dedicated number vs shared iMessage line).
            origination = _resolve_call_origination(
                identity, str(args.get("origination") or "")
            )
            if origination is None:
                raise RuntimeError(
                    "This identity can't place calls: it has no dedicated phone "
                    "number and iMessage is not enabled. Provision a number or "
                    "enable iMessage first."
                )

            phone = getattr(identity, "phone_number", None)
            ws_url = str(getattr(phone, "client_websocket_url", "") or "").strip()
            if not ws_url:
                tunnel = getattr(identity, "tunnel", None)
                host = str(getattr(tunnel, "public_host", "") or "").strip()
                if host:
                    ws_url = f"wss://{host}{INKBOX_WS_PATH}"
            if not ws_url:
                raise RuntimeError(
                    "no call-media WebSocket URL available; start the Inkbox "
                    "Claude gateway first"
                )
            token = _write_call_context(
                purpose=purpose,
                opening_message=str(args.get("opening_message") or "").strip(),
                context=str(args.get("context") or "").strip(),
                to_number=to_number,
            )
            ws_url = _append_query_param(ws_url, "context_token", token)
            try:
                call = identity.place_call(
                    to_number=to_number,
                    origination=origination,
                    client_websocket_url=ws_url,
                )
            except TypeError:
                # Older SDK without ``origination`` support → dedicated only.
                call = identity.place_call(to_number=to_number, client_websocket_url=ws_url)
            return {
                "placed": True,
                "id": str(getattr(call, "id", "")),
                "to": to_number,
                "origination": origination,
                "context_token": token,
                "status": _json_safe(getattr(call, "status", None)),
            }

        try:
            return _result(await asyncio.to_thread(_run))
        except Exception as exc:
            msg = str(exc)
            if "no_shared_connection" in msg:
                # Surface a legible reason: shared-line calls only work for
                # people already connected over iMessage.
                return _error(
                    "Can't place a shared iMessage-line call: this person isn't "
                    "connected to you over iMessage yet. They need to message your "
                    "iMessage number first. To call from your own phone number "
                    'instead, set origination to "dedicated_number".',
                    detail=msg,
                )
            return _error(msg)

    @tool(
        "inkbox_list_calls",
        "List recent phone calls on this agent's Inkbox number, newest first.",
        {"limit": int, "offset": int},
    )
    async def inkbox_list_calls(args: Dict[str, Any]) -> Dict[str, Any]:
        def _run():
            return _identity().list_calls(
                limit=int(args.get("limit") or 25),
                offset=int(args.get("offset") or 0),
            )

        try:
            return _result(await asyncio.to_thread(_run))
        except Exception as exc:
            return _error(str(exc))

    @tool(
        "inkbox_get_call_transcript",
        "Fetch transcript segments for one phone call by call_id.",
        {"call_id": str},
    )
    async def inkbox_get_call_transcript(args: Dict[str, Any]) -> Dict[str, Any]:
        def _run():
            call_id = str(args.get("call_id") or "").strip()
            if not call_id:
                raise ValueError("call_id is required (get one from inkbox_list_calls)")
            return _identity().list_transcripts(call_id)

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
    # Contacts and generated facts are organization-wide. Channel history is
    # still scoped to this identity.
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
        "title, user-managed notes). Use for name-based queries like 'find Ada'. "
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
        "Fetch one contact's full record by contact id.",
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
        "family_name, preferred_name, company_name, job_title, user-managed notes, and "
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
        "inkbox_delete_contact",
        "Remove a contact from the address book by its contact id. Look it up "
        "first to confirm you have the right person.",
        {"contact_id": str},
    )
    async def inkbox_delete_contact(args: Dict[str, Any]) -> Dict[str, Any]:
        def _run():
            client.contacts.delete(str(args["contact_id"]))
            return {"deleted": str(args["contact_id"])}

        try:
            return _result(await asyncio.to_thread(_run))
        except Exception as exc:
            return _error(str(exc))

    tools = [
        inkbox_whoami,
        inkbox_send_email,
        inkbox_send_sms,
        inkbox_send_imessage,
        inkbox_place_call,
        inkbox_list_calls,
        inkbox_get_call_transcript,
        inkbox_list_text_conversations,
        inkbox_get_text_conversation,
        inkbox_list_imessage_conversations,
        inkbox_get_imessage_conversation,
        inkbox_lookup_contact,
        inkbox_list_contacts,
        inkbox_get_contact,
        inkbox_create_contact,
        inkbox_update_contact,
        inkbox_delete_contact,
    ]
    server = create_sdk_mcp_server(name="inkbox", version="0.1.0", tools=tools)
    tool_names = [
        "mcp__inkbox__inkbox_whoami",
        "mcp__inkbox__inkbox_send_email",
        "mcp__inkbox__inkbox_send_sms",
        "mcp__inkbox__inkbox_send_imessage",
        "mcp__inkbox__inkbox_place_call",
        "mcp__inkbox__inkbox_list_calls",
        "mcp__inkbox__inkbox_get_call_transcript",
        "mcp__inkbox__inkbox_list_text_conversations",
        "mcp__inkbox__inkbox_get_text_conversation",
        "mcp__inkbox__inkbox_list_imessage_conversations",
        "mcp__inkbox__inkbox_get_imessage_conversation",
        "mcp__inkbox__inkbox_lookup_contact",
        "mcp__inkbox__inkbox_list_contacts",
        "mcp__inkbox__inkbox_get_contact",
        "mcp__inkbox__inkbox_create_contact",
        "mcp__inkbox__inkbox_update_contact",
        "mcp__inkbox__inkbox_delete_contact",
    ]
    return server, tool_names
