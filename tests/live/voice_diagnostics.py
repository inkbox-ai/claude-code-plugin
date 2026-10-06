"""CI-only, bounded voice evidence. No speech, arguments, IDs or credentials leave it."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import time

PHASES = frozenset({"driver_speech", "driver_transcript", "driver_exit", "persisted_transcript", "persisted_action", "tool_activity", "case", "observer"})
STATUSES = frozenset({"observed", "ok", "error", "passed", "failed", "unavailable"})
ENUMS = {
    "leg": frozenset({"driver_local", "driver_remote", "aut_local", "aut_remote", "unknown"}),
    "request": frozenset({"greeting", "initial", "reask", "other"}),
    "tool": frozenset({"register_post_call_action", "edit_post_call_action", "delete_post_call_action", "list_post_call_actions", "other"}),
    "tool_status": frozenset({"started", "succeeded", "failed", "other"}),
    "error_code": frozenset({"none", "other", "tool_unavailable", "invalid_tool_call", "tool_cancelled", "tool_timeout", "invalid_tool_result", "already_completed", "previous_attempt_failed", "not_found", "permission_denied", "invalid_arguments"}),
}
BOOLEANS = frozenset({"final", "nonempty", "marker_present", "marker_all_words", "marker_words_ordered", "marker_word_1", "marker_word_2", "marker_word_3", "own_prompt", "answer_latched", "answer_transition", "sms_intent", "open", "has_more"})
NUMBERS = {"ordinal": 128, "count": 1000, "request_ordinal": 3, "elapsed_ms": 360_000, "partial_age_ms": 360_000, "final_age_ms": 360_000}


def safe_record(value):
    """Independently reproject even a tampered diagnostic file before reporting."""
    if not isinstance(value, dict):
        return None
    if not isinstance(value.get("phase"), str) or not isinstance(value.get("status"), str):
        return None
    if value["phase"] not in PHASES or value["status"] not in STATUSES:
        return None
    out = {"phase": value["phase"], "status": value["status"]}
    for key, allowed in ENUMS.items():
        if isinstance(value.get(key), str) and value[key] in allowed:
            out[key] = value[key]
    for key in BOOLEANS:
        if type(value.get(key)) is bool:
            out[key] = value[key]
    for key, maximum in NUMBERS.items():
        if type(value.get(key)) is int and 0 <= value[key] <= maximum:
            out[key] = value[key]
    return out


def speech_key(text):
    return "".join(c for c in text.casefold() if c.isalnum())


def marker_shape(text, marker):
    # Marker contents never enter the output; the indices have fixed meanings.
    value = speech_key(text[:20_000])
    words = [speech_key(word) for word in marker.split()][:3]
    words = words if len(words) == 3 and all(words) else ["", "", ""]
    positions = [value.find(word) if word else -1 for word in words]
    present = [position >= 0 for position in positions]
    return {
        "marker_present": bool(marker) and speech_key(marker) in value,
        "marker_all_words": all(present),
        "marker_words_ordered": all(present) and positions == sorted(positions),
        **{f"marker_word_{i + 1}": flag for i, flag in enumerate(present)},
    }


class Trace:
    def __init__(self, path):
        self.path = path
        self.records = []

    def emit(self, phase, status="observed", **fields):
        try:
            record = safe_record({"phase": phase, "status": status, **fields})
            if record:
                if len(self.records) >= 128:
                    self.records.pop(16)  # Keep startup plus the newest bounded evidence.
                self.records.append(record)
        except Exception:
            pass

    def flush(self):
        try:
            if not self.path:
                return
            fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
            with os.fdopen(fd, "a") as stream:
                for record in self.records:
                    stream.write(json.dumps(safe_record(record), sort_keys=True) + "\n")
            self.records.clear()
        except Exception:
            pass


class ObservedSocket:
    """Delegate the exact WS data/returns/exceptions; observe only fixed predicates."""
    def __init__(self, socket, driver, trace, clock=time.monotonic):
        self.socket, self.driver, self.trace, self.clock = socket, driver, trace, clock
        self.started = clock()
        self.partial = self.final = None
        self.requests = 0
        self.answered = False
        self.answer = driver._FinalAnswerMatcher(driver.ANSWER_CONTAINS)

    def __getattr__(self, name):
        return getattr(self.socket, name)

    def timing(self):
        now = self.clock()
        result = {"elapsed_ms": min(360_000, max(0, int((now - self.started) * 1000)))}
        for label, timestamp in (("partial_age_ms", self.partial), ("final_age_ms", self.final)):
            if timestamp is not None:
                result[label] = min(360_000, max(0, int((now - timestamp) * 1000)))
        return result

    async def send_text(self, raw):
        result = await self.socket.send_text(raw)
        try:
            event = json.loads(raw)
            text = event.get("delta")
            if event.get("event") == "text" and isinstance(text, str):
                request = "greeting" if text == self.driver.GREETING else "other"
                if text == self.driver.LINE:
                    self.requests = min(3, self.requests + 1)
                    request = "initial" if self.requests == 1 else "reask"
                self.trace.emit("driver_speech", request=request, request_ordinal=self.requests,
                                answer_latched=self.answered, **self.timing())
        except Exception:
            pass
        return result

    async def receive_text(self):
        raw = await self.socket.receive_text()
        try:
            event = json.loads(raw)
            text = event.get("text")
            if event.get("event") == "transcript" and isinstance(text, str):
                final = bool(event.get("is_final"))
                nonempty = bool(text.strip())
                prior = self.answered
                if nonempty:
                    if final:
                        self.final = self.clock()
                    else:
                        self.partial = self.clock()
                    self.answered |= self.answer.observe(text, final=final)
                self.trace.emit("driver_transcript", final=final, nonempty=nonempty,
                                own_prompt=speech_key(text) == speech_key(self.driver.LINE),
                                answer_latched=self.answered, answer_transition=self.answered and not prior,
                                **marker_shape(text, self.driver.ANSWER_CONTAINS), **self.timing())
        except Exception:
            pass
        return raw


def run_driver():
    import voice_driver as driver
    from fastapi import FastAPI, WebSocket
    app = FastAPI()
    app.get("/health")(driver.health)

    async def observed(socket):
        trace = Trace(os.environ.get("VOICE_DIAGNOSTIC_DRIVER", ""))
        try:
            return await driver.phone_media_ws(ObservedSocket(socket, driver, trace))
        finally:
            trace.emit("driver_exit", "ok")
            trace.flush()

    # FastAPI resolves this runtime annotation without importing optional web
    # dependencies into offline observer/privacy tests.
    observed.__annotations__["socket"] = WebSocket
    app.websocket("/phone/media/ws")(observed)
    driver.app = app
    driver.main()


def install_test_observers(module, trace, scope):
    """Wrap exact existing readers; never change their results or exceptions."""
    original_wait = module._wait_for_persisted_hosted_request
    original_segments = module._segments
    original_actions = module._post_call_action_diagnostic

    def wait(remote, number_id, call_id, aut, aut_call_id, marker, **kwargs):
        scope.update(remote=remote, driver_id=call_id, aut=aut, aut_id=aut_call_id, marker=marker)
        return original_wait(remote, number_id, call_id, aut, aut_call_id, marker, **kwargs)

    def segments(client, number_id, call_id):
        result = original_segments(client, number_id, call_id)
        try:
            prefix = ("driver" if client is scope.get("remote") and call_id == scope.get("driver_id") else
                      "aut" if client is scope.get("aut") and call_id == scope.get("aut_id") else None)
            if prefix:
                for party, items in (("remote", result[1]), ("local", result[2])):
                    text = " ".join(item.text for item in items[-30:] if isinstance(item.text, str))
                    # Retain only the latest sanitized snapshot, not every poll.
                    scope[f"{prefix}_{party}"] = {"leg": f"{prefix}_{party}", "count": min(len(items), 1000),
                        "sms_intent": module._has_after_call_sms_intent(text), **marker_shape(text, scope["marker"])}
        except Exception:
            pass
        return result

    def actions(call, marker):
        result = original_actions(call, marker)
        try:
            records = []
            for item in list(getattr(call, "post_call_action_items", None) or [])[:10]:
                get = item.get if isinstance(item, dict) else lambda key, default="": getattr(item, key, default)
                text = f"{get('action', '')} {get('details', '')}"
                records.append({"open": str(get("status", "")).casefold() == "open",
                                "sms_intent": module._has_sms_action_intent(text), **marker_shape(text, marker)})
            scope["actions"] = records
        except Exception:
            pass
        return result

    module._wait_for_persisted_hosted_request, module._segments, module._post_call_action_diagnostic = wait, segments, actions

    def restore():
        module._wait_for_persisted_hosted_request, module._segments, module._post_call_action_diagnostic = original_wait, original_segments, original_actions
    return restore


def summarize(scope, trace):
    for key in ("driver_local", "driver_remote", "aut_local", "aut_remote"):
        if key in scope:
            trace.emit("persisted_transcript", **scope[key])
    for ordinal, action in enumerate(scope.get("actions", []), 1):
        trace.emit("persisted_action", ordinal=ordinal, **action)


def read_tools(scope, trace, client_factory=None):
    """One SDK GET scoped to the exact AUT call; subprocess owns its wall bound."""
    if "aut_id" not in scope:
        return
    try:
        if client_factory is None:
            from inkbox import Inkbox
            def client_factory():
                return Inkbox(api_key=os.environ["CLAUDE_CODE_INKBOX_API_KEY"],
                    base_url=os.environ.get("INKBOX_BASE_URL", "https://inkbox.ai"), timeout=3)
        with client_factory() as client:
            page = client.calls.tool_invocations(scope["aut_id"], limit=50)
        for ordinal, item in enumerate(page.items[:50], 1):
            name = item.tool_name if item.tool_name in ENUMS["tool"] else "other"
            status = str(item.status)
            code = (item.result or {}).get("error_code")
            code = "none" if code is None else code if isinstance(code, str) and code in ENUMS["error_code"] else "other"
            trace.emit("tool_activity", ordinal=ordinal, tool=name,
                       tool_status=status if status in ENUMS["tool_status"] else "other", error_code=code)
        trace.emit("observer", "ok", count=min(len(page.items), 1000), has_more=bool(page.has_more))
    except Exception:
        trace.emit("observer", "unavailable")


def read_tools_bounded(scope, trace, *, timeout=8):
    """Keep SDK connection retries/DNS outside the completed task and wall bound."""
    if "aut_id" not in scope:
        return
    try:
        result = subprocess.run(
            [sys.executable, str(Path(__file__).resolve()), "tools"],
            input=json.dumps({"aut_id": str(scope["aut_id"])}),
            text=True, capture_output=True, timeout=timeout, check=False,
        )
        if result.returncode != 0 or len(result.stdout) > 65_536:
            raise ValueError("diagnostic unavailable")
        records = json.loads(result.stdout)
        if not isinstance(records, list) or len(records) > 51:
            raise ValueError("diagnostic unavailable")
        for record in records:
            clean = safe_record(record)
            if clean is None or clean["phase"] not in {"tool_activity", "observer"}:
                raise ValueError("diagnostic unavailable")
        for record in records:
            clean = safe_record(record)
            trace.emit(**clean)
    except Exception:
        # subprocess.run kills and reaps its child on timeout. Never expose its
        # stdout/stderr/exception or the exact call scope supplied on stdin.
        trace.emit("observer", "unavailable")


def run_tests(args):
    import pytest

    class Observer:
        @pytest.hookimpl(hookwrapper=True)
        def pytest_runtest_call(self, item):
            if item.name != "test_outbound_call_hosted_and_post_call_wakeup":
                yield
                return
            trace = Trace(os.environ.get("VOICE_DIAGNOSTIC_TEST", ""))
            scope = {}
            try:
                restore = install_test_observers(item.module, trace, scope)
            except Exception:
                trace.emit("observer", "unavailable")
                yield
                trace.flush()
                return
            try:
                outcome = yield
            finally:
                try:
                    restore()
                except Exception:
                    trace.emit("observer", "unavailable")
            # The original test has returned/raised and its finally hangup ran.
            try:
                trace.emit("case", "failed" if outcome.excinfo else "passed")
                summarize(scope, trace)
                read_tools_bounded(scope, trace)
            except Exception:
                trace.emit("observer", "unavailable")
            finally:
                trace.flush()

    return pytest.main(args, plugins=[Observer()])


def report(paths):
    for path in paths:
        try:
            with open(path, encoding="utf-8") as stream:
                for _ in range(128):
                    line = stream.readline(8193)
                    if not line:
                        break
                    if len(line) > 8192:
                        break
                    try:
                        clean = safe_record(json.loads(line))
                    except (TypeError, ValueError):
                        clean = None
                    if clean:
                        print(json.dumps(clean, sort_keys=True))
        except (OSError, UnicodeError):
            print('{"phase":"observer","status":"unavailable"}')


if __name__ == "__main__":
    mode, *args = sys.argv[1:]
    if mode == "driver":
        run_driver()
    elif mode == "test":
        raise SystemExit(run_tests(args))
    elif mode == "report":
        report(args)
    elif mode == "tools":
        trace = Trace(None)
        try:
            scope = json.loads(sys.stdin.read(1024))
            if not isinstance(scope, dict) or not isinstance(scope.get("aut_id"), str):
                raise ValueError("missing scope")
            read_tools(scope, trace)
        except Exception:
            trace.emit("observer", "unavailable")
        print(json.dumps(trace.records))
    else:
        raise SystemExit(2)
