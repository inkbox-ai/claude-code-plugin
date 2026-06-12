"""Human-in-the-loop escalation over text channels.

Claude Code surfaces two kinds of interactive prompts that normally
render in the terminal: tool-permission requests and AskUserQuestion
polls. On this bridge both are reformatted as a plain-text message,
sent to the human on whatever channel they are talking on, and the
*next inbound message from that contact* is consumed as the answer
instead of starting a new agent turn.
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

# Words accepted as a yes/no on a permission text. Numbers map to the
# options in the order they are printed.
_ALLOW_WORDS = {"y", "yes", "ok", "okay", "sure", "approve", "allow", "go", "1"}
_ALWAYS_WORDS = {"always", "allow always", "yes always", "2"}
_DENY_WORDS = {"n", "no", "deny", "stop", "block", "don't", "dont", "3"}


@dataclass
class PendingInteraction:
    """One in-flight permission request or poll awaiting a text reply."""

    kind: str  # "permission" | "poll"
    prompt_text: str
    future: "asyncio.Future[str]"
    questions: List[Dict[str, Any]] = field(default_factory=list)
    tool_name: str = ""
    created_at: float = field(default_factory=time.time)


def _one_line(value: Any, limit: int = 160) -> str:
    text = str(value or "").replace("\n", " ").strip()
    return text if len(text) <= limit else text[: limit - 1] + "…"


def summarize_tool_call(tool_name: str, input_data: Dict[str, Any]) -> str:
    """Render a tool call as one human-readable line for a text message.

    Args:
        tool_name (str): Claude Code tool name (Bash, Write, Edit, ...).
        input_data (dict): The tool's input parameters.

    Returns:
        str: A short plain-language description of what the tool will do.
    """
    if tool_name == "Bash":
        desc = _one_line(input_data.get("description") or "")
        cmd = _one_line(input_data.get("command") or "")
        return f"run the command: {cmd}" + (f" ({desc})" if desc else "")
    if tool_name in ("Write", "NotebookEdit"):
        return f"create or overwrite the file {_one_line(input_data.get('file_path') or input_data.get('notebook_path'))}"
    if tool_name in ("Edit", "MultiEdit"):
        return f"edit the file {_one_line(input_data.get('file_path'))}"
    if tool_name == "WebFetch":
        return f"fetch the web page {_one_line(input_data.get('url'))}"
    if tool_name.startswith("mcp__"):
        pretty = tool_name.split("__")[-1].replace("_", " ")
        return f"use {pretty} with {_one_line(json.dumps(input_data, ensure_ascii=False), 120)}"
    return f"use the {tool_name} tool with {_one_line(json.dumps(input_data, ensure_ascii=False), 120)}"


def format_permission_request(tool_name: str, input_data: Dict[str, Any]) -> str:
    """Format a tool-permission escalation as an SMS-friendly message.

    Args:
        tool_name (str): Tool Claude wants to run.
        input_data (dict): Tool input parameters.

    Returns:
        str: Message text ending with the reply instructions.
    """
    summary = summarize_tool_call(tool_name, input_data)
    return (
        f"Claude wants to {summary}\n\n"
        "Reply 1 (or YES) to allow once, 2 (or ALWAYS) to allow this kind "
        "of action for the rest of the session, 3 (or NO) to block it."
    )


def parse_permission_reply(reply: str) -> Optional[str]:
    """Map a free-text reply onto a permission decision.

    Args:
        reply (str): Raw inbound message text from the human.

    Returns:
        Optional[str]: "allow", "always", or "deny"; None if unparseable.
    """
    word = (reply or "").strip().lower().rstrip(".!")
    if word in _ALWAYS_WORDS:
        return "always"
    if word in _ALLOW_WORDS:
        return "allow"
    if word in _DENY_WORDS:
        return "deny"
    return None


def format_poll(questions: List[Dict[str, Any]]) -> str:
    """Format AskUserQuestion questions as a numbered text poll.

    Args:
        questions (list[dict]): The tool's ``questions`` array — each has
            ``question``, ``options`` ([{label, description}]), and
            optionally ``multiSelect``.

    Returns:
        str: Poll text the human can answer with option numbers.
    """
    blocks: List[str] = []
    for q_index, q in enumerate(questions):
        lines = []
        if len(questions) > 1:
            lines.append(f"Q{q_index + 1}: {q.get('question', '')}")
        else:
            lines.append(str(q.get("question", "")))
        for o_index, option in enumerate(q.get("options") or []):
            label = option.get("label", "")
            desc = _one_line(option.get("description") or "", 80)
            lines.append(f"{o_index + 1}. {label}" + (f" — {desc}" if desc else ""))
        if q.get("multiSelect"):
            lines.append("(pick one or more, e.g. \"1 3\")")
        blocks.append("\n".join(lines))
    blocks.append(
        "Reply with the number of your choice"
        + (" for each question (e.g. \"1, 2\")" if len(questions) > 1 else "")
        + ", or just text your own answer."
    )
    return "\n\n".join(blocks)


def parse_poll_reply(reply: str, questions: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Map a text reply onto AskUserQuestion answers.

    Numeric replies select option labels; anything else is passed
    through as a free-text answer (AskUserQuestion supports "Other").

    Args:
        reply (str): Raw inbound message text from the human.
        questions (list[dict]): The original ``questions`` array.

    Returns:
        dict: Mapping of question text → chosen label(s) or free text.
    """
    raw = (reply or "").strip()
    # Split multi-question replies on commas/semicolons/newlines.
    chunks = [c.strip() for c in raw.replace(";", ",").replace("\n", ",").split(",")]
    chunks = [c for c in chunks if c] or [raw]

    answers: Dict[str, Any] = {}
    for index, question in enumerate(questions):
        chunk = chunks[index] if index < len(chunks) else chunks[-1]
        options = question.get("options") or []
        picked: List[str] = []
        for token in chunk.split():
            if token.isdigit() and 1 <= int(token) <= len(options):
                picked.append(options[int(token) - 1].get("label", token))
        if not picked:
            # Try matching an option label verbatim before falling back
            # to free text.
            lowered = chunk.lower()
            for option in options:
                if option.get("label", "").lower() == lowered:
                    picked = [option["label"]]
                    break
        key = str(question.get("question", f"q{index}"))
        if question.get("multiSelect"):
            answers[key] = picked or [chunk]
        else:
            answers[key] = picked[0] if picked else chunk
    return answers
