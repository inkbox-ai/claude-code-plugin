"""Durable caller-side A2A delegation routing records."""

from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlsplit

_LOCK = threading.Lock()


def _path() -> Path:
    root = Path(
        os.getenv("INKBOX_CLAUDE_HOME")
        or (Path.home() / ".inkbox-claude")
    )
    return root / "a2a_delegations.json"


def _origin(url: str) -> str:
    parsed = urlsplit(url)
    return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}"


def _read() -> Dict[str, Dict[str, Any]]:
    try:
        loaded = json.loads(_path().read_text())
        return loaded if isinstance(loaded, dict) else {}
    except FileNotFoundError:
        return {}


def _write(records: Dict[str, Dict[str, Any]]) -> None:
    path = _path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(records, indent=2, sort_keys=True) + "\n")
    tmp.chmod(0o600)
    os.replace(tmp, path)
    path.chmod(0o600)


def record_before_send(
    *,
    identity_id: str,
    rpc_url: str,
    card_url: str,
    message_id: str,
    session_key: Optional[str],
    context_id: Optional[str] = None,
    task_id: Optional[str] = None,
) -> str:
    """Persist retry and routing data before the SendMessage request."""
    origin = _origin(rpc_url)
    context_key = context_id or f"pending:{message_id}"
    key = f"{identity_id}|{origin}|{context_key}"
    with _LOCK:
        records = _read()
        records[key] = {
            "identity_id": identity_id,
            "origin": origin,
            "card_url": card_url,
            "context_id": context_id,
            "task_id": task_id,
            "message_id": message_id,
            "session_key": session_key,
            "updated_at": time.time(),
        }
        _write(records)
    return key


def promote_after_send(
    pending_key: str,
    *,
    context_id: str,
    task_id: str,
) -> None:
    """Promote a provisional record to its canonical context key."""
    with _LOCK:
        records = _read()
        record = records.pop(pending_key, None)
        if record is None:
            return
        record["context_id"] = context_id
        record["task_id"] = task_id
        record["updated_at"] = time.time()
        key = (
            f"{record['identity_id']}|{record['origin']}|{context_id}"
        )
        records[key] = record
        _write(records)


def find_by_task(task_id: str) -> Optional[Dict[str, Any]]:
    """Return the newest local delegation record for a remote task."""
    matches = [
        record
        for record in _read().values()
        if str(record.get("task_id") or "") == task_id
    ]
    return max(matches, key=lambda item: item.get("updated_at", 0), default=None)
