"""CI-only, fixed-label lifecycle tracing; never record request or result data."""

import asyncio
from collections import Counter
from contextvars import ContextVar
from functools import wraps
import inspect
import json
import os
from pathlib import Path
import sys
import threading
import time


PHASES = frozenset({
    "mail_ingress", "mail_fetch", "inbound", "turn", "connect", "query", "receive",
    "delivery", "permission", "prompt", "interrupt", "close", "tool",
    "contacts_create", "contacts_update", "contacts_delete", "contacts_get",
    "mail_send", "mail_get", "message", "automatic_reply",
})
STATUSES = frozenset({"start", "ok", "error", "cancelled", "observed"})
DETAILS = frozenset({
    "none", "other", "timeout", "connection", "permission", "value", "cancelled",
    "assistant", "result", "system", "user", "result_error", "result_success",
    "withheld_after_tool", "withheld_after_interrupt", "native_returned",
})
TOOLS = frozenset({
    "inkbox_create_contact", "inkbox_update_contact", "inkbox_delete_contact",
    "inkbox_get_contact", "inkbox_list_contacts", "inkbox_send_email", "inkbox_send_sms",
    "inkbox_send_imessage", "inkbox_start_call", "AskUserQuestion", "Bash", "Read",
})


def safe_record(value):
    """An allowlist, not redaction: even a tampered trace cannot print free text."""
    if not isinstance(value, dict) or value.get("phase") not in PHASES or value.get("status") not in STATUSES:
        return None
    result = {"phase": value["phase"], "status": value["status"]}
    for key, maximum in (("time_ms", 10**15), ("turn", 10**6), ("generation", 10**6)):
        item = value.get(key)
        if type(item) is int and 0 <= item <= maximum:
            result[key] = item
    if type(value.get("http_status")) is int and 100 <= value["http_status"] <= 599:
        result["http_status"] = value["http_status"]
    for key in ("blocked", "pending"):
        if type(value.get(key)) is bool:
            result[key] = value[key]
    result["detail"] = value.get("detail") if value.get("detail") in DETAILS else "other"
    if value.get("tool") in TOOLS:
        result["tool"] = value["tool"]
    return result


def error_kind(error):
    for cls, label in ((asyncio.CancelledError, "cancelled"), (TimeoutError, "timeout"),
                       (ConnectionError, "connection"), (PermissionError, "permission"), (ValueError, "value")):
        if isinstance(error, cls):
            return label
    return "other"


class Trace:
    def __init__(self, path):
        self.path = Path(path)
        self.lock = threading.Lock()
        self.turn = ContextVar("ci_trace_turn", default=0)
        self.sequence = 0
        self.session_turns = {}

    def emit(self, phase, status, *, session=None, detail="none", tool=None, error=None):
        # Diagnostic I/O must not change a model task's return or exception.
        try:
            if session is None:
                from inkbox_claude.tools import CURRENT_SESSION
                session = CURRENT_SESSION.get()
            record = {"time_ms": int(time.time() * 1000), "phase": phase, "status": status,
                      "turn": self.turn.get() or self.session_turns.get(id(session), 0), "detail": detail,
                      "tool": tool}
            if session is not None:
                record.update(generation=getattr(session, "_client_generation", 0),
                              blocked=getattr(session, "_execution_blocked", False),
                              pending=getattr(session, "pending", None) is not None)
            if error is not None:
                record.update(detail=error_kind(error), http_status=getattr(error, "status_code", None))
            clean = safe_record(record)
            if clean is None:
                return
            with self.lock:
                fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
                with os.fdopen(fd, "a") as stream:
                    stream.write(json.dumps(clean, sort_keys=True) + "\n")
        except Exception:
            pass

    def wrap(self, cls, name, phase, *, session_method=False):
        original = getattr(cls, name)

        def start(instance, args):
            session = instance if session_method else None
            token = None
            if phase == "turn":
                self.sequence += 1
                self.session_turns[id(instance)] = self.sequence
                token = self.turn.set(self.sequence)
            if phase == "tool":
                value = args[0].get("tool_name") if args and isinstance(args[0], dict) else None
                name = value.removeprefix("mcp__inkbox__") if isinstance(value, str) else None
                self.emit(phase, "observed", session=session, tool=name)
            else:
                self.emit(phase, "start", session=session)
            return session, token

        def finish(session, token, completed):
            if phase == "turn" and completed:
                detail = ("withheld_after_interrupt" if getattr(session, "_interrupting", False)
                          else "withheld_after_tool" if getattr(session, "_current_channel_tool_delivery", False)
                          else "native_returned")
                self.emit("automatic_reply", "observed", session=session, detail=detail)
            if token is not None:
                self.turn.reset(token)

        if inspect.iscoroutinefunction(original):
            @wraps(original)
            async def async_call(instance, *args, **kwargs):
                session, token = start(instance, args)
                completed = False
                try:
                    result = await original(instance, *args, **kwargs)
                    completed = True
                    self.emit(phase, "ok", session=session)
                    return result
                except BaseException as error:
                    self.emit(phase, "cancelled" if isinstance(error, asyncio.CancelledError) else "error", session=session, error=error)
                    raise
                finally:
                    finish(session, token, completed)
            setattr(cls, name, async_call)
        else:
            @wraps(original)
            def sync_call(instance, *args, **kwargs):
                session, token = start(instance, args)
                completed = False
                try:
                    result = original(instance, *args, **kwargs)
                    completed = True
                    self.emit(phase, "ok", session=session)
                    return result
                except BaseException as error:
                    self.emit(phase, "error", session=session, error=error)
                    raise
                finally:
                    finish(session, token, completed)
            setattr(cls, name, sync_call)

    def wrap_receive(self, cls):
        receive = cls.receive_response

        @wraps(receive)
        async def receive_response(client, *args, **kwargs):
            self.emit("receive", "start")
            iterator = receive(client, *args, **kwargs)
            try:
                async for message in iterator:
                    detail = {"AssistantMessage": "assistant", "SystemMessage": "system", "UserMessage": "user",
                              "ResultMessage": "result_error" if getattr(message, "is_error", False) else "result_success"}.get(type(message).__name__, "other")
                    self.emit("message", "observed", detail=detail)
                    yield message
                self.emit("receive", "ok")
            except BaseException as error:
                self.emit("receive", "cancelled" if isinstance(error, asyncio.CancelledError) else "error", error=error)
                raise
            finally:
                await iterator.aclose()
        cls.receive_response = receive_response


def install(path):
    from claude_agent_sdk import ClaudeSDKClient
    from inkbox.contacts.resources.contacts import ContactsResource
    from inkbox.mail.resources.messages import MessagesResource
    from inkbox_claude.gateway import InkboxGateway
    from inkbox_claude.sessions import ContactSession

    trace = Trace(path)
    for method, phase in {"handle_inbound": "inbound", "_run_turn": "turn", "_ensure_client": "connect",
                          "_deliver_reply": "delivery", "_can_use_tool": "permission", "_escalate": "prompt",
                          "_abort_in_flight": "interrupt", "close": "close", "_observe_a2a_tool_start": "tool"}.items():
        trace.wrap(ContactSession, method, phase, session_method=True)
    trace.wrap(InkboxGateway, "_on_mail_received", "mail_ingress")
    trace.wrap(InkboxGateway, "_fetch_mail_body", "mail_fetch")
    trace.wrap(ClaudeSDKClient, "query", "query")
    for method in ("create", "update", "delete", "get"):
        trace.wrap(ContactsResource, method, f"contacts_{method}")
    for method in ("send", "get"):
        trace.wrap(MessagesResource, method, f"mail_{method}")
    trace.wrap_receive(ClaudeSDKClient)
    return trace


def report(path):
    counts = Counter()
    try:
        with Path(path).open() as stream:
            for line in stream:
                try:
                    record = safe_record(json.loads(line))
                except (ValueError, TypeError):
                    record = None
                if record is not None:
                    print(json.dumps(record, sort_keys=True))
                    counts[f'{record["phase"]}.{record["status"]}'] += 1
    except OSError:
        print("Lifecycle trace unavailable.")
    print(json.dumps({"phase_counts": dict(sorted(counts.items()))}, sort_keys=True))


if __name__ == "__main__":
    path = os.environ.get("INKBOX_CI_TRACE_FILE", "/tmp/inkbox-ci-lifecycle.jsonl")
    if sys.argv[1:] == ["report"]:
        report(path)
    else:
        install(path)
        from inkbox_claude.daemon import run_foreground
        raise SystemExit(run_foreground())
