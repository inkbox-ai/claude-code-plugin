"""Readiness checks for the bridge: config, SDKs, CLI, and identity reachability."""

from __future__ import annotations

import os
import shutil
from typing import List, Tuple

try:
    from .config import inkbox_client_kwargs, read_config
except ImportError:  # pragma: no cover - direct local import/test fallback
    from config import inkbox_client_kwargs, read_config


def run_doctor() -> List[Tuple[str, bool, str]]:
    """Run every readiness check.

    Returns:
        List[Tuple[str, bool, str]]: (check name, passed, detail) rows.
    """
    cfg = read_config()
    checks: List[Tuple[str, bool, str]] = []

    checks.append(("INKBOX_API_KEY", bool(cfg.api_key), "set" if cfg.api_key else "missing"))
    checks.append(("INKBOX_IDENTITY", bool(cfg.identity), cfg.identity or "missing"))
    checks.append((
        "INKBOX_SIGNING_KEY",
        bool(cfg.signing_key) or not cfg.require_signature,
        "set" if cfg.signing_key else "missing (required for signed inbound webhooks)",
    ))

    try:
        import inkbox  # noqa: F401
        checks.append(("inkbox SDK", True, "installed"))
    except ImportError:
        checks.append(("inkbox SDK", False, "pip install 'inkbox>=0.5.1,<1.0.0'"))

    try:
        import claude_agent_sdk  # noqa: F401
        checks.append(("claude-agent-sdk", True, "installed"))
    except ImportError:
        checks.append(("claude-agent-sdk", False, "pip install claude-agent-sdk"))

    try:
        import aiohttp  # noqa: F401
        checks.append(("aiohttp", True, "installed"))
    except ImportError:
        checks.append(("aiohttp", False, "pip install 'aiohttp>=3.9'"))

    claude_bin = shutil.which("claude")
    checks.append((
        "claude CLI",
        bool(claude_bin),
        claude_bin or "not on PATH — install Claude Code first",
    ))

    project_dir = cfg.project_dir
    checks.append((
        "project dir",
        bool(project_dir) and os.path.isdir(project_dir),
        project_dir or "unset (defaults to cwd)",
    ))

    if cfg.api_key and cfg.identity:
        try:
            from inkbox import Inkbox

            identity = Inkbox(**inkbox_client_kwargs(cfg.api_key, cfg.base_url)).get_identity(cfg.identity)
            mailbox = getattr(identity, "mailbox", None)
            phone = getattr(identity, "phone_number", None)
            detail = ", ".join(filter(None, [
                getattr(mailbox, "email_address", None),
                getattr(phone, "number", None),
                "imessage" if getattr(identity, "imessage_enabled", False) else None,
            ])) or "no channels provisioned"
            checks.append(("identity reachable", True, detail))
        except Exception as exc:
            checks.append(("identity reachable", False, str(exc)))

    return checks


def print_doctor() -> int:
    """Print check results.

    Returns:
        int: Process exit code — 0 when everything passed, 1 otherwise.
    """
    rows = run_doctor()
    failed = 0
    for name, ok, detail in rows:
        mark = "✓" if ok else "✗"
        print(f" {mark} {name:<20} {detail}")
        failed += 0 if ok else 1
    return 0 if failed == 0 else 1
