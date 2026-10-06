"""Durable iMessage receipts and narrowly scoped active-turn reply context."""

from __future__ import annotations

from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sqlite3
from typing import Any, Iterator


_STATES = {"pending", "running", "reply_pending", "sending", "done", "cancelled", "uncertain", "failed"}
_TERMINAL = {"done", "cancelled"}


class SafeReplyPreflightError(ConnectionError):
    """A transient read failed before any channel send was submitted."""


def _private_dir(name: str) -> Path:
    root = Path(os.getenv("INKBOX_CLAUDE_HOME") or (Path.home() / ".inkbox-claude"))
    path = root / name
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.chmod(0o700)
    return path


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _field(value: Any, name: str) -> Any:
    return value.get(name) if isinstance(value, dict) else getattr(value, name, None)


def _id(value: Any) -> str | None:
    return str(value) if value not in (None, "") else None


def source_metadata(message: Any, event_id: str | None = None) -> dict[str, Any]:
    """Anchor a reply to its trigger without inferring any native ancestry."""
    message_id = _id(_field(message, "id"))
    source = {
        "id": message_id,
        "text": _field(message, "content") or _field(message, "text") or "",
        **{name: _id(_field(message, name)) for name in (
            "reply_to_message_id", "thread_id", "thread_root_message_id",
        )},
    }
    receipt_id = _id(event_id) or message_id
    return {
        "message_id": message_id,
        "imessage_event_id": receipt_id,
        "imessage_event_ids": [receipt_id] if receipt_id else [],
        "imessage_sources": [source] if message_id else [],
        "imessage_reply_target": message_id,
        **{name: source[name] for name in ("reply_to_message_id", "thread_id", "thread_root_message_id")},
    }


def auto_reply_kwargs(meta: dict[str, Any]) -> dict[str, Any]:
    target = meta.get("imessage_reply_target")
    return {"reply_to_message_id": target, "plain_reply_fallback": True} if target else {}


def _event_ids(meta: dict[str, Any]) -> list[str]:
    ids = meta.get("imessage_event_ids") or [meta.get("imessage_event_id") or meta.get("source_event_id") or meta.get("message_id")]
    return list(dict.fromkeys(str(value) for value in ids if value))


def _outbound(message: Any) -> dict[str, Any]:
    return {name: _id(_field(message, name)) for name in (
        "id", "status", "reply_to_message_id", "thread_id", "thread_root_message_id",
    )}


class IMessageState:
    """One identity/environment's admitted work, with no automatic uncertain replay."""

    def __init__(self, cfg: Any, channel: str = "imessage"):
        scope = _json([str(cfg.base_url or "").rstrip("/"), str(cfg.identity)])
        self.path = _private_dir(channel + "_state") / f"{hashlib.sha256(scope.encode()).hexdigest()}.sqlite3"
        fd = os.open(self.path, os.O_CREAT | os.O_WRONLY, 0o600)
        os.close(fd)
        self.path.chmod(0o600)
        self._owner_file = None
        with self._db() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS receipts (
                    event_id TEXT PRIMARY KEY, chat_id TEXT NOT NULL,
                    text TEXT NOT NULL, meta TEXT NOT NULL,
                    state TEXT NOT NULL, reply TEXT, batch_anchor TEXT
                );
                CREATE TABLE IF NOT EXISTS controls (
                    event_id TEXT PRIMARY KEY, targets TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS outbound (
                    message_id TEXT PRIMARY KEY, route TEXT NOT NULL
                );
            """)

    def acquire_owner(self):
        if self._owner_file is not None:
            return
        owner = self.path.with_suffix(".lock").open("a+")
        try:
            fcntl.flock(owner.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            owner.close()
            raise RuntimeError("Another bridge owns this channel journal") from None
        self._owner_file = owner

    def close(self):
        if self._owner_file is not None:
            self._owner_file.close()
            self._owner_file = None

    def interrupted(self):
        with self._db() as db:
            return [self._receipt(row) for row in db.execute(
                "SELECT * FROM receipts WHERE state IN ('running','sending','uncertain') ORDER BY rowid")
                if not json.loads(row["meta"]).get("host_fenced")]

    def consume_control(self, event_id: str, targets: list[dict]) -> bool:
        """Persist a Stop tombstone and its exact targets before interrupting anything."""
        if not event_id:
            raise ValueError("A control requires a stable event ID")
        with self._db() as db:
            result = db.execute("INSERT OR IGNORE INTO controls(event_id,targets) VALUES(?,?)",
                                (event_id, _json(targets)))
            return result.rowcount == 1

    @contextmanager
    def _db(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    def admit(self, chat_id: str, text: str, meta: dict[str, Any]) -> bool:
        event_id = meta.get("imessage_event_id") or meta.get("source_event_id") or meta.get("message_id")
        if not event_id:
            raise ValueError("An iMessage receipt requires a stable event or message ID")
        with self._db() as db:
            result = db.execute(
                "INSERT OR IGNORE INTO receipts(event_id,chat_id,text,meta,state) VALUES(?,?,?,?, 'pending')",
                (str(event_id), chat_id, text, _json(meta)),
            )
            return result.rowcount == 1

    def forget_pending(self, meta):
        """Release a failed admission only before model ownership was acquired."""
        with self._db() as db:
            for event_id in _event_ids(meta):
                db.execute("DELETE FROM receipts WHERE event_id=? AND state='pending'", (event_id,))

    @staticmethod
    def _receipt(row: sqlite3.Row) -> dict[str, Any]:
        result = {"chat_id": row["chat_id"], "text": row["text"], "meta": json.loads(row["meta"])}
        if row["reply"] is not None:
            result["reply"] = json.loads(row["reply"])
        return result

    def replay_pending(self) -> list[dict[str, Any]]:
        """Call once at startup: interrupted work needs review, not a second model run."""
        with self._db() as db:
            db.execute("UPDATE receipts SET state='uncertain' WHERE state IN ('running','sending')")
            return [self._receipt(row) for row in db.execute(
                "SELECT * FROM receipts WHERE state='pending' ORDER BY rowid"
            )]

    def pending_replies(self) -> list[dict[str, Any]]:
        with self._db() as db:
            rows = db.execute("SELECT * FROM receipts WHERE state='reply_pending' ORDER BY rowid").fetchall()
        seen: set[str] = set()
        result = []
        for row in rows:
            anchor = row["batch_anchor"] or row["event_id"]
            if anchor not in seen and row["reply"] is not None:
                seen.add(anchor)
                result.append(self._receipt(row))
        return result

    def mark(self, meta: dict[str, Any], state: str, reply: Any = None, chat_id: str | None = None) -> None:
        if state not in _STATES:
            raise ValueError("Unknown iMessage receipt state")
        ids = _event_ids(meta)
        if not ids:
            return
        with self._db() as db:
            # Serialize batch selection and updates across the gateway and tool processes.
            db.execute("BEGIN IMMEDIATE")
            rows = [db.execute("SELECT * FROM receipts WHERE event_id=?", (event_id,)).fetchone() for event_id in ids]
            rows = [row for row in rows if row is not None and row["state"] not in _TERMINAL]
            if chat_id is not None:
                rows = [row for row in rows if row["chat_id"] == chat_id]
            anchor = next((row["batch_anchor"] for row in rows if row["batch_anchor"]), ids[0])
            for row in rows:
                merged = {**json.loads(row["meta"]), **meta}
                # A completed answer remains safely retryable until the send
                # checkpoint; a failed read-only preflight cannot have sent it.
                next_state = "reply_pending" if state == "uncertain" and row["state"] == "reply_pending" else state
                if state == "cancelled" and row["state"] == "sending":
                    next_state = "uncertain"
                db.execute(
                    "UPDATE receipts SET meta=?,state=?,reply=?,batch_anchor=? WHERE event_id=?",
                    (_json(merged), next_state, _json(reply) if reply is not None else row["reply"], anchor, row["event_id"]),
                )

    def begin_send(self, meta: dict[str, Any], chat_id: str) -> None:
        """Atomically reject cancelled or already-attempted final deliveries."""
        ids = _event_ids(meta)
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            rows = [db.execute("SELECT * FROM receipts WHERE event_id=?", (event_id,)).fetchone() for event_id in ids]
            if not rows or any(row is None or row["chat_id"] != chat_id or row["state"] != "reply_pending" for row in rows):
                raise PermissionError("Reply receipt is no longer awaiting delivery")
            for event_id in ids:
                db.execute("UPDATE receipts SET state='sending' WHERE event_id=?", (event_id,))

    def record_outbound(self, message: Any, meta: dict[str, Any], chat_id: str) -> None:
        sent = _outbound(message)
        if not sent["id"]:
            return
        route = {"chat_id": chat_id, "meta": meta, "message_id": sent.pop("id"), **sent}
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT route FROM outbound WHERE message_id=?", (route["message_id"],)).fetchone()
            if row is None:
                db.execute("INSERT INTO outbound(message_id,route) VALUES(?,?)", (route["message_id"], _json(route)))
            elif json.loads(row["route"]).get("unknown_route"):
                # Delivery callbacks can win the race with the accepted-send
                # response. Fill its missing route without undoing that failure.
                route["status"] = json.loads(row["route"])["status"]
                db.execute("UPDATE outbound SET route=? WHERE message_id=?", (_json(route), route["message_id"]))

    def lookup_outbound(self, message_id: str) -> dict[str, Any] | None:
        with self._db() as db:
            row = db.execute("SELECT route FROM outbound WHERE message_id=?", (str(message_id),)).fetchone()
        return json.loads(row["route"]) if row else None

    def mark_delivery_failed(self, message_id: str) -> None:
        """Record late failure without changing the completed model work or its route."""
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT route FROM outbound WHERE message_id=?", (str(message_id),)).fetchone()
            if row is not None:
                route = {**json.loads(row["route"]), "status": "failed"}
                db.execute("UPDATE outbound SET route=? WHERE message_id=?", (_json(route), str(message_id)))
            else:
                route = {"chat_id": None, "meta": {}, "message_id": str(message_id),
                         "status": "failed", "unknown_route": True}
                db.execute("INSERT INTO outbound(message_id,route) VALUES(?,?)", (str(message_id), _json(route)))

    def summary(self) -> dict[str, int]:
        counts = dict.fromkeys(sorted(_STATES), 0)
        with self._db() as db:
            counts.update({row[0]: row[1] for row in db.execute("SELECT state,COUNT(*) FROM receipts GROUP BY state")})
        counts["unfinished"] = sum(count for state, count in counts.items() if state not in _TERMINAL)
        with self._db() as db:
            counts["outbound_failed_count"] = db.execute(
                "SELECT COUNT(*) FROM outbound WHERE json_extract(route, '$.status') IN ('failed', 'delivery_failed')"
            ).fetchone()[0]
        return counts



def validate_reply_target(identity, meta):
    """Read-only preflight: admitted source and identity-scoped conversation agree."""
    from .config import imessage_threading_capability
    supported, detail = imessage_threading_capability(identity)
    if not supported:
        raise RuntimeError(detail)
    target = meta.get("imessage_reply_target")
    if not target:
        return {}
    if target not in {item.get("id") for item in meta.get("imessage_sources", [])}:
        raise ValueError("Native reply target does not belong to this input")
    source = identity.get_imessage(target)
    page = identity.get_imessage_thread(target, limit=1)
    conversation = str(meta.get("conversation_id") or "")
    if not conversation or any(str(_field(item, "conversation_id") or "") != conversation for item in (source, page)):
        raise ValueError("Native reply conversation cannot be verified")
    return auto_reply_kwargs(meta)


def compatible_key(meta):
    message = meta.get("message_id") or meta.get("source_message_id")
    parent, root = meta.get("reply_to_message_id"), meta.get("thread_root_message_id")
    context = (root or meta.get("thread_id") or parent) if parent or (root and message and root != message) else None
    return tuple(meta.get(k) for k in ("sender", "conversation_id", "conversation_kind", "sender_access",
        "companion_scope_id", "companion_activation_id", "identity_id")) + (context,)
