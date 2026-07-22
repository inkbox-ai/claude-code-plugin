"""Channel prompt injected into Claude Code for messaging contexts."""

from __future__ import annotations

import re
from typing import Any, Dict, Optional

# Appended to the claude_code system prompt preset for every bridged
# session. The agent is a full Claude Code instance with tool access —
# but the human is on a phone, not in a terminal.
CHANNEL_PROMPT = """
# Messaging bridge

You are NOT in a terminal. You are an Inkbox agent ({identity_line}). The
human is talking to you over {channels}. Your replies are delivered to
their phone or inbox, so:

- Each incoming message starts with a small [inkbox:...] metadata tag showing
  how it reached you, the remote phone/email, and any resolved Inkbox contact.
  Read it to know who you are talking to and which channel you're on right now,
  but never repeat the tag back in your reply.
- Plain text only. No markdown — no **bold**, no backticks, no headers,
  no bullet lists, no code blocks unless they explicitly ask for code.
- Keep it short and conversational. Think texts, not essays. Lead with
  the outcome ("Done — tests pass" beats a paragraph of process).
- Keep jargon to a minimum. Say "saved and published the change", not
  "committed and pushed to origin/main". Say "the signup page", not
  "src/app/(auth)/signup/page.tsx". Only go technical when they do.
- One idea per message. For SMS/iMessage, separate short thoughts with
  a blank line — each block is delivered as its own bubble.
- Never paste diffs, stack traces, or logs. Summarize in a sentence and
  offer to email details (email handles long content better than SMS).
- If a reply needs more than ~2 short paragraphs, send the short
  version on the current channel and offer the long version by email.

# Working style

- You have full tool access to the project at {project_dir}. Work
  autonomously; don't narrate every step.
- Anything risky (running commands, editing files, etc.) is
  automatically escalated to the human as a text they answer with a
  quick reply. Don't also ask for permission in prose — just use the
  tool and the bridge handles the rest.
- When you genuinely need the human to choose between options, use the
  AskUserQuestion tool. It is delivered to them as a numbered poll and
  their reply comes back as the answer.
- Long tasks are fine: the human walked away from the keyboard on
  purpose. Text them the result when you're done, not play-by-play.

# Outbound messaging

You also have Inkbox tools (inkbox_send_email, inkbox_send_sms,
inkbox_send_imessage, ...) to reach the human or third parties
proactively — e.g. "email me the full report" or a cron-style ping.
Replies on the channel you were messaged on are sent automatically;
only use these tools for a *different* channel or recipient.

# Calling someone

Outbound calls (inkbox_place_call) can go out over two lines. Match the
line to the channel you're talking on: call SMS/phone contacts from your
dedicated phone number (origination "dedicated_number"), and call an
iMessage contact over the shared iMessage line (origination
"shared_imessage_number") — the same line you already message them on.
The shared line only works for people connected to you over iMessage
(otherwise the call is rejected — ask them to message you on iMessage
first, or fall back to your dedicated number), and its number is managed
by Inkbox: never state a number for it. If you omit origination it
follows the current conversation's channel, or the only line available.

# Inkbox contacts

Claude can read and write the organization's shared Inkbox contacts.

- Use inkbox_list_contacts for name-based searches like "who is Alex?".
- Use inkbox_lookup_contact when you have an exact or partial email/phone filter.
- Use inkbox_get_contact to fetch a full contact by UUID after list/lookup returns one.
- Use inkbox_create_contact when the user asks you to save a new person or contact card.
- Use inkbox_update_contact when the user asks you to change an existing contact; look up the contact first if you do not already have its UUID.
- Use inkbox_delete_contact only after the target contact is explicit and confirmed.
- There is no vCard export/import, contact access, or contact rule tool in this harness.
- Every identity in the organization can read contacts. Creating, updating, or deleting a contact affects the shared address book.
""".strip()


def build_channel_prompt(
    project_dir: str,
    identity_handle: str = "",
    email_address: str = "",
    phone_number: str = "",
    channels: str = "email, SMS, iMessage, and voice calls",
) -> str:
    """Render the channel prompt for one bridged session.

    Args:
        project_dir (str): Absolute path of the project Claude works in.
        identity_handle (str): Inkbox agent identity handle.
        email_address (str): Identity mailbox address, if provisioned.
        phone_number (str): Identity phone number, if provisioned.
        channels (str): Human-readable list of reachable channels.

    Returns:
        str: The prompt text to append to the claude_code preset.
    """
    parts = [p for p in (identity_handle, email_address, phone_number) if p]
    identity_line = " / ".join(parts) or "not yet provisioned"
    return CHANNEL_PROMPT.format(
        channels=channels,
        identity_line=identity_line,
        project_dir=project_dir or "the current directory",
    )


def contact_marker(
    details: Optional[Dict[str, Any]],
    agent_identity: Optional[Dict[str, Any]] = None,
) -> str:
    """Render a one-line Inkbox contact summary for inbound turn tags.

    An address-book contact always wins. With no contact match, a sender
    Inkbox resolved to exactly one agent identity is labeled with that
    identity instead of the unknown marker.
    """
    if not details or not details.get("id"):
        if agent_identity and agent_identity.get("id"):
            # Handle and display name are remote-controlled strings — repr
            # them so quotes/newlines can't break out of the one-line tag.
            parts = [f"contact_agent_identity_id={agent_identity['id']}"]
            if agent_identity.get("handle"):
                parts.append(f"contact_agent_handle={agent_identity['handle']!r}")
            if agent_identity.get("name"):
                parts.append(f"contact_name={agent_identity['name']!r}")
            return " ".join(parts)
        return "contact=unknown_in_inkbox"
    parts = [f"contact_id={details['id']}"]
    if details.get("name"):
        parts.append(f"contact_name={details['name']!r}")
    if details.get("company"):
        parts.append(f"contact_company={details['company']!r}")
    if details.get("emails"):
        parts.append(f"contact_emails={details['emails']}")
    if details.get("phones"):
        parts.append(f"contact_phones={details['phones']}")
    return " ".join(parts)


def frame_inbound(mode: str, meta: Dict[str, Any], text: str) -> str:
    """Prefix an inbound message with a tag naming its channel and sender.

    Gives Claude the per-turn context the static system prompt can't — which
    channel this message arrived on and who sent it — so it can answer
    "what channel are we on?" and tailor the reply.

    Args:
        mode (str): Channel the message arrived on (email/sms/imessage/voice).
        meta (dict): Inbound routing metadata; ``sender`` and ``subject`` used.
        text (str): The human's message body.

    Returns:
        str: ``text`` prefixed with a one-line bracketed channel tag.
    """
    if text.lstrip().startswith("[inkbox:"):
        return text

    meta = meta or {}
    sender = str(meta.get("sender") or "").strip()
    from_part = f" from={sender}" if sender else ""
    marker = contact_marker(meta.get("contact"), meta.get("agent_identity"))
    if mode == "email":
        subject = str(meta.get("subject") or "").strip()
        subject_part = f" subject={subject!r}" if subject else ""
        header = f"[inkbox:email{from_part}{subject_part} | {marker}]"
    elif mode == "sms":
        conversation_id = str(meta.get("conversation_id") or "").strip()
        conversation_part = f" conversation_id={conversation_id}" if conversation_id else ""
        label = "group_sms" if meta.get("conversation_kind") == "group" else "sms"
        header = f"[inkbox:{label}{from_part}{conversation_part} | {marker}]"
    elif mode == "imessage":
        conversation_id = str(meta.get("conversation_id") or "").strip()
        conversation_part = f" conversation_id={conversation_id}" if conversation_id else ""
        header = f"[inkbox:imessage{from_part}{conversation_part} | {marker}]"
    elif mode == "voice":
        call_id = str(meta.get("call_id") or "").strip()
        call_part = f" call_id={call_id}" if call_id else ""
        header = f"[inkbox:voice_call{call_part} | {marker}]"
    else:
        header = f"[inkbox:{mode}{from_part} | {marker}]"
    return f"{header}\n{text}"


_MD_PATTERNS = [
    (re.compile(r"```[a-zA-Z0-9]*\n?"), ""),       # code fences
    (re.compile(r"^#{1,6}\s+", re.MULTILINE), ""),  # headers
    (re.compile(r"\*\*([^*]+)\*\*"), r"\1"),        # bold
    (re.compile(r"\*([^*]+)\*"), r"\1"),            # italic
    (re.compile(r"`([^`]+)`"), r"\1"),              # inline code
    (re.compile(r"\[([^\]]+)\]\(([^)]+)\)"), r"\1 (\2)"),  # links
]


def strip_markdown(text: str) -> str:
    """Best-effort markdown→plain-text for SMS/iMessage/voice delivery.

    Args:
        text (str): Possibly-markdown reply text from the agent.

    Returns:
        str: The same text with common markdown syntax flattened.
    """
    out = text or ""
    for pattern, repl in _MD_PATTERNS:
        out = pattern.sub(repl, out)
    return out.strip()
