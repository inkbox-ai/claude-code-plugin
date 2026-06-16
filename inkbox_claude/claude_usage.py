"""Fetch Claude subscription usage — mirrors the Claude Code ``/usage`` command.

Claude Code's ``/usage`` reports rate-limit utilization against the rolling
5-hour session window and the weekly windows. It gets this from the OAuth
endpoint ``/api/oauth/usage`` using the subscription login. We do the same:
read the token from Claude Code's own credential store and call that endpoint,
then format the blocks for a text reply.

The response shape (matching the CLI) is::

    {"five_hour":  {"utilization": 0.0-1.0, "resets_at": <iso|epoch>},
     "seven_day":  {...},
     "seven_day_opus": {...}}
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

# Claude Code calls GET /api/oauth/usage; try the API host first, then claude.ai.
USAGE_URLS = (
    "https://api.anthropic.com/api/oauth/usage",
    "https://claude.ai/api/oauth/usage",
)
_OAUTH_BETA = "oauth-2025-04-20"
# Friendly labels for each window the endpoint reports.
_BLOCKS = (
    ("five_hour", "5-hour session"),
    ("seven_day", "This week (all models)"),
    ("seven_day_opus", "This week (Opus)"),
)


def _read_oauth_token() -> Optional[str]:
    """Read the subscription OAuth access token from Claude Code's creds.

    Returns:
        Optional[str]: The access token, or None if not logged in via subscription.
    """
    path = Path(os.getenv("CLAUDE_CONFIG_DIR") or (Path.home() / ".claude")) / ".credentials.json"
    try:
        data = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None
    return ((data.get("claudeAiOauth") or {}).get("accessToken")) or None


def fetch_usage() -> dict:
    """Call the usage endpoint and return the raw JSON payload.

    Returns:
        dict: The parsed usage response.

    Raises:
        RuntimeError: "no-auth" when there's no subscription token.
        urllib.error.HTTPError / URLError: on a failed request.
    """
    token = _read_oauth_token()
    if not token:
        raise RuntimeError("no-auth")
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "anthropic-beta": _OAUTH_BETA,
        "User-Agent": "inkbox-claude",
    }
    last: Exception = RuntimeError("no endpoint")
    for url in USAGE_URLS:
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=8) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:
            # Auth failures are terminal (same answer on either host); surface them.
            if exc.code in (401, 403):
                raise
            last = exc
        except Exception as exc:  # connection error → try the next host
            last = exc
    raise last


def _parse_dt(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _fmt_reset(value: Any, *, now: Optional[datetime] = None) -> str:
    """Render a reset timestamp as a short 'in 2h 15m' string."""
    dt = _parse_dt(value)
    if dt is None:
        return ""
    now = now or datetime.now(timezone.utc)
    seconds = (dt - now).total_seconds()
    if seconds <= 0:
        return "now"
    hours, minutes = int(seconds // 3600), int((seconds % 3600) // 60)
    if hours >= 24:
        return f"in {hours // 24}d {hours % 24}h"
    if hours >= 1:
        return f"in {hours}h {minutes}m"
    return f"in {minutes}m"


def format_usage(data: dict, *, now: Optional[datetime] = None) -> str:
    """Format the usage payload like Claude Code's /usage (5h + weekly blocks).

    Args:
        data (dict): The /api/oauth/usage response.
        now (Optional[datetime]): Reference time (for testing reset deltas).

    Returns:
        str: One line per reported window, or a fallback if none are present.
    """
    lines = []
    for key, label in _BLOCKS:
        block = data.get(key)
        if not isinstance(block, dict) or block.get("utilization") is None:
            continue
        pct = round(float(block["utilization"]) * 100)
        reset = _fmt_reset(block.get("resets_at"), now=now)
        lines.append(f"{label}: {pct}% used" + (f", resets {reset}" if reset else ""))
    return "\n".join(lines) if lines else "No usage windows reported."


def usage_report() -> str:
    """Fetch and format Claude usage, returning a text-ready summary.

    Returns:
        str: The usage report, or a friendly error message.
    """
    try:
        data = fetch_usage()
    except RuntimeError as exc:
        if str(exc) == "no-auth":
            return (
                "Can't read your Claude usage — no subscription login found on this "
                "machine. (If you're on an API key instead of a Claude plan, there's "
                "no /usage to show.)"
            )
        return f"Couldn't fetch Claude usage: {exc}"
    except urllib.error.HTTPError as exc:
        if exc.code in (401, 403):
            return "Couldn't fetch Claude usage — the login looks expired. Run claude once to refresh it, then try again."
        return f"Couldn't fetch Claude usage (HTTP {exc.code})."
    except Exception as exc:
        return f"Couldn't fetch Claude usage: {exc}"
    return "Claude usage:\n" + format_usage(data)
