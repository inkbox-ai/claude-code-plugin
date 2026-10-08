"""One source-bound, coalesced progress message per admitted Slack turn."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
from pathlib import Path
from uuid import UUID

from .slack_streams import SlackTaskStreams

logger = logging.getLogger(__name__)
PREFIX = "inkbox-slack-progress:"
RECOVERY_TIMEOUT_SECONDS = 5
_ROUTE = ("connection_id", "conversation_id", "thread_ts", "message_ts", "source_event_id")
_TERMINAL = {"completed": "Completed.", "cancelled": "Stopped.", "failed": "Could not complete.",
             "interrupted": "Progress paused while reconnecting."}
_STREAM_UNSUPPORTED = {"feature_disabled", "feature_not_enabled", "app_not_eligible", "missing_scope",
                       "not_allowed_token_type", "channel_type_not_supported", "invalid_thread_ts",
                       "method_not_supported_for_channel_type", "unknown_method"}


class SlackProgress:
    def __init__(self, resource, path: Path, *, validate_route=None, interval=1.0):
        self.resource, self.path, self.validate_route = resource, path, validate_route
        self.interval = interval
        self.streams = SlackTaskStreams(resource)
        self.active: dict[str, dict] = {}
        self.records: dict[str, dict] = {}
        self.tasks: dict[str, asyncio.Task] = {}
        self.closing = False

    def _save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp = self.path.with_suffix(".tmp")
        with os.fdopen(os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "w") as stream:
            json.dump(self.records, stream)
            stream.flush()
            os.fsync(stream.fileno())
        temp.replace(self.path)

    @staticmethod
    def _key(chat_id, meta):
        return hashlib.sha256(json.dumps([str(chat_id), *[meta.get(k) for k in _ROUTE]]).encode()).hexdigest()

    def has_chat(self, chat_id):
        return any(record["chat_id"] == str(chat_id) for record in self.active.values())

    async def notify(self, chat_id, meta, state):
        if self.closing:
            return
        if not all(isinstance(meta.get(k), str) and meta[k]
                   for k in ("connection_id", "conversation_id", "message_ts", "source_event_id")):
            return
        key = self._key(chat_id, meta)
        if state == "accepted":
            # An unresolved message from an earlier process is never replaced.
            if key not in self.records:
                self.active.setdefault(key, {"chat_id": str(chat_id), "route": {
                    k: meta.get(k) for k in (*_ROUTE, "identity_id", "workspace_id", "actor_id", "recipient_team_id")}, "revision": 0})
            return
        record = self.active.get(key)
        if record is None:
            return
        if state in _TERMINAL:
            self.active.pop(key, None)
            if key in self.records:
                record["terminal"] = True
                record["outcome"] = state
                self._queue(key, record, _TERMINAL[state])
        elif key in self.records and state in {"waiting", "resumed"}:
            self._queue(key, record, "Waiting for your approval." if state == "waiting" else "Working…")

    async def progress(self, chat_id, meta, content, message_id=None):
        if self.closing:
            return None
        key = str(message_id)[len(PREFIX):] if str(message_id).startswith(PREFIX) else self._key(chat_id, meta)
        record = self.active.get(key)
        if record is None or record["chat_id"] != str(chat_id):
            return None
        # Handles may locate an edit, but cannot override a supplied source route.
        if any(k in meta and meta[k] != record["route"].get(k) for k in (*_ROUTE, "identity_id", "workspace_id", "actor_id", "recipient_team_id")):
            return None
        text = " ".join(content.split())[:240]
        if not text:
            return PREFIX + key
        # Progress is plain text, never an opportunity to trigger Slack mentions.
        text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        self.records[key] = record
        self._queue(key, record, text)
        return PREFIX + key

    def _queue(self, key, record, text):
        record["desired"] = text
        if key not in self.tasks or self.tasks[key].done():
            task = asyncio.create_task(self._run(key, record))
            self.tasks[key] = task
            task.add_done_callback(lambda done: self.tasks.pop(key, None) if self.tasks.get(key) is done else None)

    async def _validate(self, record):
        if self.validate_route is not None:
            await asyncio.to_thread(self.validate_route, record["route"])

    def _rejected(self, record, result, text):
        """Bound retries to terminal rate limits and confirmed pre-send failures."""
        record.pop("uncertain", None)
        retry_after = getattr(result, "retry_after", None)
        if isinstance(retry_after, int) and retry_after > 0:
            record["retry_at"] = time.time() + retry_after
        retry = record["desired"] != text
        if (record.get("terminal")
                and getattr(result, "error_code", None) in {"rate_limited", "ratelimited", "connection_failed"}
                and record.get("terminal_retries", 0) < 3):
            record["terminal_retries"] = record.get("terminal_retries", 0) + 1
            record["retry_at"] = max(record.get("retry_at", 0), time.time() + 1)
            retry = True
        self._save()
        return retry

    async def _resolve_pending(self, key, record):
        """Bound read-only reconciliation; terminal uncertainty is never polled."""
        if record.get("pending_status") == "unknown":
            return False
        for attempt in range(3):
            await self._validate(record)
            kind = record.get("pending_kind")
            if record.get("transport") == "stream":
                if not kind or not record.get("pending_key"):
                    return False
                result = await asyncio.to_thread(self.streams.lookup, record["route"],
                    kind=kind, key=record["pending_key"])
            elif not record.get("message_ts"):
                result = await asyncio.to_thread(self.resource.get_action_by_key,
                    record["route"]["connection_id"], f"claude:progress:{key}")
            elif record.get("operation_id"):
                result = await asyncio.to_thread(self.resource.get_operation,
                    record["route"]["connection_id"], record["operation_id"])
            elif self.streams.lookup_supported and kind == "message_update" and record.get("pending_key"):
                result = await asyncio.to_thread(self.streams.lookup, record["route"],
                    kind=kind, key=record["pending_key"])
            else:
                return False
            status = getattr(result, "status", None)
            record["pending_status"] = status
            self._save()
            if status in {"sending", "in_progress"}:
                if attempt < 2:
                    await asyncio.sleep(self.interval)
                continue
            if status not in {"sent", "succeeded", "failed"}:
                return False
            if status != "failed":
                timestamp = getattr(result, "message_ts", None)
                if not record.get("message_ts") and (not isinstance(timestamp, str) or not timestamp):
                    return False
                if timestamp:
                    record["message_ts"] = timestamp
                if isinstance(record.get("pending_text"), str):
                    record["applied"] = record["pending_text"]
                if record.get("transport") == "stream":
                    if kind == "stream_start":
                        record["stream_id"] = result.id
                    elif kind == "stream_stop":
                        record["stream_closed"] = True
            elif not record.get("message_ts"):
                record["started"] = False
            record.pop("uncertain", None)
            self._save()
            return True
        return False

    async def _run(self, key, record):
        try:
            while (record.get("applied") != record["desired"]
                   or record.get("terminal") and record.get("stream_id") and not record.get("stream_closed")):
                if record.get("uncertain"):
                    if not record.get("terminal") or not await self._resolve_pending(key, record):
                        return
                    if record.get("stream_closed"):
                        self.records.pop(key, None)
                        self._save()
                        return
                    if record.get("transport") != "stream" and record.get("applied") == record["desired"]:
                        self.records.pop(key, None)
                        self._save()
                        return
                if record.get("terminal") and (not record.get("started")
                        or record.get("transport") == "stream" and not record.get("stream_id")):
                    self.records.pop(key, None)
                    self._save()
                    return
                delay = record.get("retry_at", 0) - time.time()
                if delay > 0:
                    await asyncio.sleep(delay)
                await self._validate(record)
                if "transport" not in record:
                    record["transport"] = "stream" if await asyncio.to_thread(self.streams.capable, record["route"]) else "message"
                if record.get("terminal") and not record.get("started"):
                    self.records.pop(key, None)
                    self._save()
                    return
                text = record["desired"]
                before = dict(record)
                record["started"] = True
                record["revision"] += 1
                record["uncertain"] = True
                record.pop("operation_id", None)
                record.pop("pending_status", None)
                route = record["route"]
                if record["transport"] == "stream":
                    kind = "stream_start" if not record.get("stream_id") else "stream_stop" if record.get("terminal") else "stream_append"
                    operation_key = f"claude:task:{key}:{record['revision']}"
                else:
                    kind = "message_update" if record.get("message_ts") else "message_create"
                    operation_key = f"claude:progress:{key}" + (f":{record['revision']}" if kind == "message_update" else "")
                record.update(pending_kind=kind, pending_key=operation_key, pending_text=text)
                try:
                    self._save()  # Persist the complete intent before any external effect.
                except Exception:
                    record.clear()
                    record.update(before)
                    raise
                if record["transport"] == "stream":
                    status = "error" if record.get("outcome") in {"failed", "interrupted"} else "complete" if record.get("terminal") else "in_progress"
                    result = await asyncio.to_thread(self.streams.write, route, kind=kind, key=operation_key,
                        stream_id=record.get("stream_id"), chunks=[{
                            "type": "task_update", "id": key[:32], "title": text[:256], "status": status,
                        }])
                    if result.status == "failed" and kind == "stream_start" and result.error_code in _STREAM_UNSUPPORTED:
                        # Only a definitive unsupported start permits fallback.
                        # A timeout/unknown start might already have made a card.
                        record.update(transport="message", started=False)
                        record.pop("uncertain", None)
                        self._save()
                        continue
                    if result.status == "failed":
                        if self._rejected(record, result, text):
                            continue
                        return
                    if result.status != "succeeded":
                        record["pending_status"] = result.status
                        self._save()
                        logger.warning("Slack task progress is unconfirmed; no automatic fallback or replay")
                        if record.get("terminal"):
                            continue
                        return
                    if kind == "stream_start":
                        record["stream_id"] = result.id
                    record["message_ts"] = result.message_ts
                    if kind == "stream_stop":
                        record["stream_closed"] = True
                elif not record.get("message_ts"):
                    result = await asyncio.to_thread(
                        self.resource.send_message, route["connection_id"],
                        conversation_id=route["conversation_id"], thread_ts=route.get("thread_ts"),
                        text=text, idempotency_key=f"claude:progress:{key}",
                    )
                    if getattr(result, "status", None) == "failed":
                        # No message exists to update; retire a definitively rejected create.
                        self.active.pop(key, None)
                        self.records.pop(key, None)
                        self._save()
                        return
                    if (getattr(result, "status", None) != "sent"
                            or not isinstance(getattr(result, "message_ts", None), str) or not result.message_ts):
                        record["pending_status"] = getattr(result, "status", None)
                        self._save()
                        logger.warning("Slack progress creation is unconfirmed; no automatic resend")
                        if record.get("terminal"):
                            continue
                        return
                    record["message_ts"] = result.message_ts
                else:
                    result = await asyncio.to_thread(
                        self.resource.update_message, route["connection_id"], route["conversation_id"],
                        record["message_ts"], text, idempotency_key=operation_key,
                    )
                    if getattr(result, "status", None) == "failed":
                        # Definitive rejection is not an unknown side effect.
                        # A later status (especially Stop/completion) may try a
                        # new edit, without spinning on the rejected payload.
                        if self._rejected(record, result, text):
                            continue
                        if record.get("terminal"):
                            # The edit definitely failed and its retry budget is
                            # exhausted. Do not repeat doomed edits on restart.
                            self.records.pop(key, None)
                            self._save()
                        return
                    if getattr(result, "status", None) != "succeeded":
                        record["pending_status"] = getattr(result, "status", None)
                        operation_id = getattr(result, "id", None)
                        if isinstance(operation_id, (str, UUID)):
                            record["operation_id"] = str(operation_id)
                        self._save()
                        logger.warning("Slack progress update is unconfirmed; further edits are deferred")
                        if record.get("terminal"):
                            continue
                        return
                record.pop("uncertain", None)
                record["applied"] = text
                if (record.get("terminal") and text == record["desired"]
                        and (record["transport"] != "stream" or record.get("stream_closed"))):
                    self.records.pop(key, None)
                self._save()
                if not record.get("terminal"):
                    # Stream methods have a tighter limit than message edits.
                    delay = max(self.interval, 3.1) if self.interval and record["transport"] == "stream" else self.interval
                    await asyncio.sleep(delay)
        except Exception:
            logger.warning("Slack progress unavailable; the agent turn is unaffected")
            if record.get("terminal") and record.get("uncertain") and not record.get("terminal_reconciled"):
                record["terminal_reconciled"] = True
                try:
                    if await self._resolve_pending(key, record):
                        if (record.get("stream_closed") or record.get("transport") != "stream"
                                and record.get("applied") == record.get("desired")):
                            self.records.pop(key, None)
                            self._save()
                        else:
                            await self._run(key, record)
                except Exception:
                    logger.warning("Slack terminal progress remains unconfirmed")

    async def recover(self):
        try:
            data = json.loads(self.path.read_text())
        except FileNotFoundError:
            return
        except (OSError, ValueError):
            logger.warning("Slack progress state could not be read")
            return
        if not isinstance(data, dict):
            return
        valid = {}
        for key, record in data.items():
            if (not isinstance(record, dict) or not isinstance(record.get("route"), dict)
                    or not isinstance(record.get("chat_id"), str)
                    or key != self._key(record["chat_id"], record["route"])
                    or not isinstance(record.get("revision"), int)):
                continue
            valid[key] = record
        # Cleanup can persist or yield to another cleanup task. Load the entire
        # journal first so either path retains streams not yet reconciled.
        self.records.update(valid)
        try:
            async with asyncio.timeout(RECOVERY_TIMEOUT_SECONDS):
                await self._recover_records(valid)
        except TimeoutError:
            logger.warning("Slack progress recovery is deferred; unresolved receipts are retained")

    async def _recover_records(self, valid):
        for key, record in valid.items():
            try:
                # Intent is persisted with started=True before every effect.
                # A concurrently journaled record without it needs no lookup.
                if not record.get("started"):
                    self.records.pop(key, None)
                    self._save()
                    continue
                if record.get("uncertain") and record.get("pending_status") == "unknown":
                    continue
                await self._validate(record)
                if record.get("uncertain") or not record.get("message_ts"):
                    if not await self._resolve_pending(key, record):
                        continue
                if (record.get("stream_closed") or not record.get("started")
                        or record.get("transport") == "stream" and not record.get("stream_id")
                        or record.get("terminal") and record.get("transport") != "stream"
                        and record.get("applied") == record.get("desired")):
                    self.records.pop(key, None)
                    self._save()
                    continue
                record.pop("uncertain", None)
                record.pop("terminal_reconciled", None)
                record["terminal"] = True
                record["outcome"] = "interrupted"
                self._queue(key, record, "Progress interrupted after reconnecting.")
            except Exception:
                logger.warning("Slack progress cleanup remains unconfirmed; no message was resent")

    async def flush(self):
        while self.tasks:
            await asyncio.gather(*list(self.tasks.values()), return_exceptions=True)
            await asyncio.sleep(0)

    async def close(self):
        for record in list(self.active.values()):
            await self.notify(record["chat_id"], record["route"], "interrupted")
        self.closing = True
        try:
            await asyncio.wait_for(asyncio.shield(self.flush()), timeout=5)
        except asyncio.TimeoutError:
            logger.warning("Slack progress cleanup is deferred until reconnect")
