"""Durable pre-send guard for hosted-call SMS commitments."""

from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional


def _root() -> Path:
    home = Path(os.getenv("INKBOX_CLAUDE_HOME") or (Path.home() / ".inkbox-claude"))
    root = home / "hosted_sms_attempts"
    root.mkdir(parents=True, exist_ok=True)
    root.chmod(0o700)
    return root


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _path(call_id: str, attempt: int) -> Path:
    return _root() / f"{_digest(call_id)}-{attempt}.json"


def _atomic_replace(path: Path, value: Dict[str, Any]) -> None:
    tmp = path.with_suffix(f"{path.suffix}.tmp")
    tmp.unlink(missing_ok=True)
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(value, sort_keys=True) + "\n")
    except Exception:
        tmp.unlink(missing_ok=True)
        raise
    os.replace(tmp, path)
    path.chmod(0o600)


def reserve_hosted_sms_attempt(
    call_id: str,
    attempt: int,
    target: str,
) -> bool:
    """Atomically reserve one provider attempt before any external side effect."""
    path = _path(call_id, attempt)
    value = {
        "state": "pending",
        "target_digest": _digest(target),
        "updated_at": time.time(),
    }
    encoded = json.dumps(value, sort_keys=True) + "\n"
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        return False
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(encoded)
    except Exception:
        # Keep the reservation file even if writing fails. Its existence is
        # the fail-closed signal that a provider call may have started.
        raise
    path.chmod(0o600)
    return True


def settle_hosted_sms_attempt(call_id: str, attempt: int, state: str) -> None:
    """Persist the sanitized synchronous result for a reserved attempt."""
    path = _path(call_id, attempt)
    try:
        current = json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        current = {}
    value = current if isinstance(current, dict) else {}
    value = {
        "state": state,
        "target_digest": str(value.get("target_digest") or ""),
        "updated_at": time.time(),
    }
    _atomic_replace(path, value)


def hosted_sms_attempt_state(call_id: str, attempt: int) -> Optional[str]:
    """Read one attempt state; an unreadable reservation is commit-ambiguous."""
    path = _path(call_id, attempt)
    if not path.exists():
        return None
    try:
        value = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return "pending"
    if not isinstance(value, dict):
        return "pending"
    return str(value.get("state") or "pending")
