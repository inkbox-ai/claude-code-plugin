"""Delay voice-note delivery without holding the webhook connection open."""

from __future__ import annotations

import asyncio
from contextlib import closing
from copy import deepcopy
import json
import logging
import os
from pathlib import Path
import sqlite3
import time
from typing import Any, Awaitable, Callable
from urllib.parse import quote

from .slack import SLACK_INCOMING_EVENTS, SLACK_TRANSCRIPTION_EVENT

logger = logging.getLogger(__name__)
RECEIVED = {
    "text.received": "text",
    "imessage.received": "imessage",
    "whatsapp.received": "whatsapp",
    **{event_type: "slack" for event_type in SLACK_INCOMING_EVENTS},
}
FINISHED = SLACK_TRANSCRIPTION_EVENT
WAIT_SECONDS = 60.0
RETRY_SECONDS = 5.0
RETRY_MAX_SECONDS = 300.0


class VoiceNoteNotAdmitted(Exception):
    """The handler stopped before admission, so the resolved input may be replayed."""
POLL_SECONDS = 10.0


def message_for(envelope: dict[str, Any]) -> dict[str, Any]:
    """Extract a channel's original message without changing its content."""
    data = envelope.get("data")
    if not isinstance(data, dict):
        return {}
    event_type = envelope.get("event_type")
    channel = data.get("channel") if event_type == FINISHED else str(event_type).split(".", 1)[0]
    if channel == "slack":
        # Slack keys results by the retained message and lists them on event.files.
        event = data.get("event")
        files = event.get("files") if isinstance(event, dict) else None
        return {
            "id": data.get("message_id"),
            "conversation_id": data.get("conversation_id"),
            "connection_id": data.get("connection_id"),
            "media": files if isinstance(files, list) else [],
        }
    message = data.get("text_message" if channel == "text" else "message")
    return message if isinstance(message, dict) else {}


def envelope_for(channel: str, message: dict[str, Any]) -> dict[str, Any]:
    """Build the received-event shape whose message_for view is ``message``."""
    if channel == "slack":
        return {"event_type": SLACK_INCOMING_EVENTS[0], "data": {
            "message_id": message.get("id"),
            "conversation_id": message.get("conversation_id"),
            "connection_id": message.get("connection_id"),
            "event": {"files": message.get("media") or []},
        }}
    return {
        "event_type": f"{channel}.received",
        "data": {"text_message" if channel == "text" else "message": message},
    }


def store_message(channel: str, envelope: dict[str, Any], message: dict[str, Any]) -> None:
    """Write resolved attachment results back into the original envelope."""
    data = envelope["data"]
    if channel == "slack":
        data["event"]["files"] = message.get("media") or []
    else:
        data["text_message" if channel == "text" else "message"] = message


def transcription_unsupported(exc: Exception) -> bool:
    """Recognize a server that does not offer the completion event yet."""
    if isinstance(exc, ValueError):
        return str(exc).startswith("Identity-wide webhook subscriptions are not supported by this server yet.")
    errors = getattr(exc, "detail", None)
    if getattr(exc, "status_code", None) != 422 or not isinstance(errors, list) or not errors:
        return False
    # The API reports one enum error per channel when none recognize the event.
    return all(
        isinstance(error, dict)
        and error.get("type") == "enum"
        and isinstance(error.get("loc"), list)
        and len(error["loc"]) >= 3
        and error["loc"][:2] == ["body", "event_types"]
        and type(error["loc"][2]) is int
        and error.get("input") == FINISHED
        for error in errors
    )


def pending(message: dict[str, Any]) -> bool:
    """A message is ready only when every enrolled attachment is terminal."""
    return any(
        (item.get("transcription") or {}).get("status") == "pending"
        for item in message.get("media") or []
        if isinstance(item, dict)
    )


def transcript_note(media: list[dict[str, Any]]) -> str:
    """Keep generated speech distinct from the sender's typed message."""
    notes = []
    for index, item in enumerate(media, 1):
        result = item.get("transcription")
        if not isinstance(result, dict) or result.get("error_code") == "not_audio":
            continue
        if result.get("status") == "completed":
            text = str(result.get("text") or "").strip()
            notes.append(
                f"[Voice note {index} — automatically transcribed]\n{text}"
                if text
                else f"[Voice note {index}: no speech detected in the recording]"
            )
        else:
            notes.append(
                f"[Voice note {index}: transcript unavailable; process the original attachment normally and do not guess its contents]"
            )
    return "\n\n" + "\n\n".join(notes) if notes else ""


def merge_results(original: dict[str, Any], current: dict[str, Any]) -> dict[str, Any]:
    """Merge only attachment results, never replace signed routing or sender text."""
    result = deepcopy(original)
    if str(current.get("id") or "") != str(original.get("id") or ""):
        return result
    for field in ("conversation_id", "connection_id"):
        if current.get(field) and original.get(field) and str(current[field]) != str(original[field]):
            return result
    refreshed = current.get("media") or []
    by_id = {
        str(item["id"]): item
        for item in refreshed
        if isinstance(item, dict) and item.get("id")
    }
    for position, item in enumerate(result.get("media") or []):
        if not isinstance(item, dict):
            continue
        source = (
            by_id.get(str(item["id"]))
            if item.get("id")
            else (refreshed[position] if position < len(refreshed) else None)
        )
        if isinstance(source, dict) and isinstance(source.get("transcription"), dict):
            item["transcription"] = deepcopy(source["transcription"])
    return result


def inbox_path(identity_id: str) -> Path:
    root = Path(os.getenv("INKBOX_CLAUDE_HOME") or (Path.home() / ".inkbox-claude"))
    return root / f"voice-notes-{identity_id}.sqlite3"


def unconfirmed_ids(path: Path) -> list[str]:
    """Inspect ordinary handoffs without opening a receiver or replaying work."""
    if not path.exists():
        return []
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as db:
        return [row[0] for row in db.execute(
            "SELECT key FROM voice_notes WHERE direct=1 AND done=2 ORDER BY key"
        )]


class VoiceNotes:
    """Persist pending originals before ACK and resume their original deadline."""

    def __init__(
        self,
        path: Path,
        identity: Any,
        client: Any,
        deliver: Callable[[dict[str, Any]], Awaitable[Any]],
        *,
        wait_seconds: float = WAIT_SECONDS,
        ready: Callable[[], bool] = lambda: True,
    ):
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.db = sqlite3.connect(path)
        path.chmod(0o600)
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS voice_notes (key TEXT PRIMARY KEY, original TEXT, completion TEXT, result TEXT, deadline REAL, direct INTEGER NOT NULL DEFAULT 0, polled INTEGER NOT NULL DEFAULT 0, done INTEGER NOT NULL DEFAULT 0, created REAL NOT NULL DEFAULT (unixepoch()))"
        )
        columns = {row[1] for row in self.db.execute("PRAGMA table_info(voice_notes)")}
        if "retry_at" not in columns:
            self.db.execute("ALTER TABLE voice_notes ADD COLUMN retry_at REAL")
            # Older resolved receipts had no marker around the external call.
            self.db.execute("UPDATE voice_notes SET done=2 WHERE direct=1 AND result IS NOT NULL AND done=0")
        self.db.execute(
            "DELETE FROM voice_notes WHERE created<? AND (done=1 OR original IS NULL OR (direct=0 AND result IS NOT NULL))",
            (time.time() - 7 * 86400,),
        )
        self.db.commit()
        self.ready = ready
        self.identity, self.client, self.deliver = identity, client, deliver
        self.wait_seconds = wait_seconds
        self.events: dict[str, asyncio.Event] = {}
        self.tasks: dict[str, asyncio.Task[Any]] = {}
        self.closing = False

    @staticmethod
    def key(channel: str, message: dict[str, Any]) -> str:
        """The inbox file is identity-scoped; callbacks correlate by source ID."""
        return f"{channel}:{message.get('id')}" if message.get("id") else ""

    def recover(self) -> None:
        """Resume acknowledged originals, including already-resolved receipts."""
        if os.getenv("INKBOX_HOSTED_MODE") == "1":
            return
        for (key,) in self.db.execute(
            "SELECT key FROM voice_notes WHERE original IS NOT NULL AND direct=1 AND done=0"
        ).fetchall():
            self._schedule(key)

    def complete(self, envelope: dict[str, Any]) -> None:
        """A callback updates a receipt; it never creates an agent turn."""
        data = envelope.get("data")
        if envelope.get("event_type") != FINISHED or not isinstance(data, dict):
            return
        channel = data.get("channel")
        message = message_for(envelope)
        if (channel not in RECEIVED.values() or not isinstance(data.get("message_id"), str)
                or data["message_id"] != message.get("id")):
            return
        if message.get("direction") == "outbound" or pending(message):
            return
        key = self.key(channel, message)
        if not key:
            return
        if not self.db.execute(
            "SELECT 1 FROM voice_notes WHERE key=?", (key,)
        ).fetchone():
            self.db.execute(
                "DELETE FROM voice_notes WHERE key IN (SELECT key FROM voice_notes WHERE original IS NULL ORDER BY created DESC LIMIT -1 OFFSET 1023)"
            )
        self.db.execute(
            "INSERT INTO voice_notes(key,completion) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET completion=excluded.completion WHERE voice_notes.done=0 AND voice_notes.result IS NULL",
            (key, json.dumps(message)),
        )
        self.db.commit()
        if key in self.events:
            self.events[key].set()

    def defer(self, envelope: dict[str, Any]) -> bool:
        """Commit pending ordinary input before returning the webhook receipt."""
        if os.getenv("INKBOX_HOSTED_MODE") == "1":
            return False
        channel = RECEIVED.get(str(envelope.get("event_type")))
        message = message_for(envelope)
        key = self.key(channel or "", message)
        if not channel or not key or message.get("direction") == "outbound":
            return False
        row = self.db.execute(
            "SELECT original,direct,done FROM voice_notes WHERE key=?", (key,)
        ).fetchone()
        if not pending(message) and not (row and row[0] and row[1]):
            return False
        self._remember(key, envelope, direct=True)
        self._schedule(key)
        return True

    def _remember(self, key: str, envelope: dict[str, Any], *, direct: bool) -> None:
        self.db.execute(
            "INSERT INTO voice_notes(key,original,deadline,direct) VALUES (?,?,?,?) ON CONFLICT(key) DO UPDATE SET original=COALESCE(voice_notes.original,excluded.original), deadline=COALESCE(voice_notes.deadline,excluded.deadline),direct=MAX(voice_notes.direct,excluded.direct)",
            (key, json.dumps(envelope), time.time() + self.wait_seconds, int(direct)),
        )
        self.db.commit()

    def _schedule(self, key: str) -> None:
        if self.closing or key in self.tasks:
            return
        if self.db.execute(
            "SELECT done FROM voice_notes WHERE key=?", (key,)
        ).fetchone()[0]:
            return
        task = self.tasks[key] = asyncio.create_task(self._deliver(key))
        task.add_done_callback(lambda completed: self.tasks.pop(key, None))

    async def _deliver(self, key: str) -> None:
        envelope = await self._resolve(key)
        delay = RETRY_SECONDS
        while True:
            retry_at, = self.db.execute("SELECT retry_at FROM voice_notes WHERE key=?", (key,)).fetchone()
            if retry_at and retry_at > time.time():
                await asyncio.sleep(retry_at - time.time())
            while not self.ready():
                self.db.execute("UPDATE voice_notes SET retry_at=? WHERE key=?", (time.time() + 5, key))
                self.db.commit()
                await asyncio.sleep(5)

            # An interrupted handler may already own the input. Only pending rows
            # are recoverable; keep ambiguous receipts for inspection.
            claimed = self.db.execute("UPDATE voice_notes SET done=2 WHERE key=? AND done=0", (key,))
            self.db.commit()
            if not claimed.rowcount:
                return
            try:
                response = await self.deliver(envelope)
            except VoiceNoteNotAdmitted:
                # Nothing reached a session; retry the stored result without polling again.
                logger.warning("Voice-note admission could not start yet; retrying", exc_info=True)
                self.db.execute("UPDATE voice_notes SET done=0, retry_at=? WHERE key=?", (time.time() + delay, key))
                self.db.commit()
                delay = min(delay * 2, RETRY_MAX_SECONDS)
                continue
            except Exception:
                logger.exception("Voice-note admission is unconfirmed; not replaying")
                return
            break
        status = getattr(response, "status", 200)
        if 200 <= status < 300 or status in (400, 401, 403, 404, 422):
            self.db.execute("UPDATE voice_notes SET done=1 WHERE key=?", (key,))
            self.db.commit()
        else:
            logger.warning("Voice-note admission is unconfirmed (status %s); not replaying", status)

    async def resolve_message(
        self, channel: str, message: dict[str, Any]
    ) -> dict[str, Any]:
        """Wait inside an already-durable conversation consumer, not its ingress."""
        if os.getenv("INKBOX_HOSTED_MODE") == "1":
            return message
        key = self.key(channel, message)
        if not key or not pending(message):
            return message
        envelope = envelope_for(channel, message)
        self._remember(key, envelope, direct=False)
        return message_for(await self._resolve(key))

    async def _resolve(self, key: str) -> dict[str, Any]:
        original, result, deadline = self.db.execute(
            "SELECT original,result,deadline FROM voice_notes WHERE key=?", (key,)
        ).fetchone()
        envelope = json.loads(original)
        message = message_for(envelope)
        channel = key.split(":", 1)[0]
        if result:
            message.update(json.loads(result))
            store_message(channel, envelope, message)
            return envelope
        event = self.events.setdefault(key, asyncio.Event())
        while pending(message):
            event.clear()
            completion = self.db.execute(
                "SELECT completion FROM voice_notes WHERE key=?", (key,)
            ).fetchone()[0]
            if completion:
                message = merge_results(message, json.loads(completion))
                if not pending(message):
                    break
            remaining = deadline - time.time()
            if remaining <= 0:
                polled = self.db.execute(
                    "SELECT polled FROM voice_notes WHERE key=?", (key,)
                ).fetchone()[0]
                if not polled:
                    self.db.execute(
                        "UPDATE voice_notes SET polled=1 WHERE key=?", (key,)
                    )
                    self.db.commit()
                    try:
                        current = await asyncio.wait_for(
                            asyncio.to_thread(
                                self._poll, channel, message
                            ),
                            POLL_SECONDS,
                        )
                        message = merge_results(message, current)
                    except Exception:
                        pass  # The original attachment path remains authoritative on read failure.
                completion = self.db.execute(
                    "SELECT completion FROM voice_notes WHERE key=?", (key,)
                ).fetchone()[0]
                if completion:
                    message = merge_results(message, json.loads(completion))
                break
            try:
                await asyncio.wait_for(event.wait(), remaining)
            except TimeoutError:
                continue
        self.db.execute(
            "UPDATE voice_notes SET result=? WHERE key=?", (json.dumps(message), key)
        )
        self.db.commit()
        self.events.pop(key, None)
        store_message(channel, envelope, message)
        return envelope

    def _poll(self, channel: str, message: dict[str, Any]) -> dict[str, Any]:
        """Read raw additive metadata using the existing authenticated SDK transport."""
        message_id = str(message["id"])
        if channel == "slack":
            connection_id = quote(str(message["connection_id"]), safe="")
            page = self.client._api_http.get(
                f"/slack/connections/{connection_id}/archive/messages",
                params={"message_id": message_id}, timeout=POLL_SECONDS,
            )
            found = [
                item for item in page.get("messages") or []
                if isinstance(item, dict) and str(item.get("id")) == message_id
            ]
            if len(found) != 1:
                return {}
            return {
                "id": message_id,
                "conversation_id": found[0].get("conversation_id"),
                "connection_id": found[0].get("connection_id"),
                "media": found[0].get("files") or [],
            }
        if channel == "text":
            phone_id = str(self.identity.phone_number.id)
            return self.client._phone_http.get(
                f"/numbers/{phone_id}/texts/{message_id}", timeout=POLL_SECONDS
            )
        transport = (
            self.client._imessage_http
            if channel == "imessage"
            else self.client._whatsapp_http
        )
        return transport.get(
            f"/messages/{message_id}",
            params={"agent_identity_id": str(self.identity.id)},
            timeout=POLL_SECONDS,
        )

    async def close(self) -> None:
        """Retain acknowledged originals while cancelling gateway-owned waiters."""
        self.closing = True
        tasks = list(self.tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.db.close()
