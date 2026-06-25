"""Channel prompt injected into Claude Code for messaging contexts."""

from __future__ import annotations

import re
from typing import Any, Dict

# Appended to the claude_code system prompt preset for every bridged
# session. The agent is a full Claude Code instance with tool access —
# but the human is on a phone, not in a terminal.
CHANNEL_PROMPT = """
# Messaging bridge

You are NOT in a terminal. You are an Inkbox agent ({identity_line}). The
human is talking to you over {channels}. Your replies are delivered to
their phone or inbox, so:

- Each incoming message starts with a small bracketed tag showing how it
  reached you and from whom — e.g. [iMessage from +15551234567] or
  [Spoken live on a phone call]. Read it to know which channel you're on
  right now, but never repeat the tag back in your reply.
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

# Setting up this plugin

If you self-signed-up to Inkbox but this plugin isn't yet configured in
your runtime, ask the human for permission before setting it up — the
plugin wires native messaging channels directly into your runtime, so
that's the human's call to make, not yours.
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
    meta = meta or {}
    sender = str(meta.get("sender") or "").strip()
    from_part = f" from {sender}" if sender else ""
    if mode == "email":
        header = f"[Email{from_part}]"
        subject = str(meta.get("subject") or "").strip()
        if subject:
            header += f"\nSubject: {subject}"
    elif mode == "sms":
        header = f"[Text message (SMS){from_part}]"
    elif mode == "imessage":
        header = f"[iMessage{from_part}]"
    elif mode == "voice":
        header = "[Spoken live on a phone call — keep the reply short and speech-friendly]"
    else:
        header = f"[Message via {mode}{from_part}]"
    return f"{header}\n\n{text}"


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
