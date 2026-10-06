"""Privacy and semantic-preservation contracts for CI-only voice observation."""
import asyncio
import importlib.util
import json
from pathlib import Path
import subprocess
import time
from types import SimpleNamespace

import httpx
import pytest

SPEC = importlib.util.spec_from_file_location("voice_diagnostics", Path(__file__).parent / "live/voice_diagnostics.py")
diag = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(diag)
SECRET = "private-body-key@example.invalid"


def test_report_independently_projects_tampered_records(tmp_path, capsys):
    path = tmp_path / "trace"
    records = [
        {"phase": "driver_transcript", "status": "observed", "text": SECRET,
         "marker_word_1": True, "marker_word_2": SECRET, "elapsed_ms": 999999999,
         "tool": SECRET, "error_code": SECRET, "request_ordinal": -1},
        {"phase": SECRET, "status": "observed"},
        {"phase": [SECRET], "status": "observed"},
        {"phase": "observer", "status": "error", "exception": SECRET},
    ]
    path.write_text("\n".join(json.dumps(record) for record in records))
    diag.report([str(path)])
    output = capsys.readouterr().out
    assert SECRET not in output
    assert [json.loads(line) for line in output.splitlines()] == [
        {"phase": "driver_transcript", "status": "observed", "marker_word_1": True},
        {"phase": "observer", "status": "error"},
    ]


def test_trace_is_bounded_preserves_startup_and_latest_and_tolerates_io_failure(tmp_path):
    trace = diag.Trace(tmp_path / "missing" / "trace")
    for ordinal in range(500):
        trace.emit("driver_transcript", elapsed_ms=ordinal, text=SECRET)
    assert len(trace.records) == 128
    assert trace.records[0]["elapsed_ms"] == 0
    assert trace.records[15]["elapsed_ms"] == 15
    assert trace.records[-1]["elapsed_ms"] == 499
    trace.flush()  # Observer failure never changes a call/test.
    assert SECRET not in json.dumps(trace.records)


def test_marker_projection_distinguishes_missing_reordered_and_exact_without_text():
    marker = "banana telescope elephant"
    exact = diag.marker_shape(f"{SECRET} Banana, Telescope! Elephant.", marker)
    assert exact["marker_present"] and exact["marker_all_words"] and exact["marker_words_ordered"]
    reordered = diag.marker_shape("elephant banana telescope", marker)
    assert reordered["marker_all_words"] and not reordered["marker_words_ordered"]
    assert not reordered["marker_present"]
    missing = diag.marker_shape("banana telescope", marker)
    assert not missing["marker_word_3"] and not missing["marker_all_words"]
    assert SECRET not in json.dumps(exact)


def driver_module(monkeypatch):
    # Reuse the actual driver's dependency-isolated loader, not its logic.
    from test_voice_driver_lifetime import _driver
    driver = _driver(monkeypatch, "false")
    driver.ANSWER_CONTAINS = "banana telescope elephant"
    driver.LINE = "Requested " + driver.ANSWER_CONTAINS
    return driver


def test_socket_preserves_frames_returns_and_partial_final_observations(monkeypatch):
    driver = driver_module(monkeypatch)
    trace = diag.Trace(None)
    clock = [0.0]
    frames = [
        json.dumps({"event": "transcript", "text": driver.ANSWER_CONTAINS, "is_final": False}),
        json.dumps({"event": "transcript", "text": "banana telescope", "is_final": True}),
        json.dumps({"event": "transcript", "text": "elephant", "is_final": True}),
        "not json " + SECRET,
    ]
    sent = []
    class Socket:
        client_state = "connected"
        async def receive_text(self):
            return frames.pop(0)
        async def send_text(self, value):
            sent.append(value)
            return "same-return"
    observed = diag.ObservedSocket(Socket(), driver, trace, clock=lambda: clock[0])
    async def run():
        originals = list(frames)
        for index, original in enumerate(originals):
            clock[0] += 1
            assert await observed.receive_text() is original
            assert observed.answered == (index >= 2)
        raw = json.dumps({"event": "text", "delta": driver.LINE})
        assert await observed.send_text(raw) == "same-return"
        assert sent == [raw]
    asyncio.run(run())
    assert observed.client_state == "connected"
    assert trace.records[-1]["partial_age_ms"] == 3000
    assert trace.records[-1]["final_age_ms"] == 1000
    assert trace.records[-1]["answer_latched"]
    assert SECRET not in json.dumps(trace.records)


@pytest.mark.parametrize("error", [ValueError(SECRET), asyncio.CancelledError(SECRET)])
def test_socket_preserves_exact_transport_exception(monkeypatch, error):
    driver = driver_module(monkeypatch)
    class Socket:
        async def receive_text(self):
            raise error
        async def send_text(self, _raw):
            raise error
    trace = diag.Trace(None)
    observed = diag.ObservedSocket(Socket(), driver, trace)
    async def run():
        for call in (observed.receive_text(), observed.send_text("{}")):
            with pytest.raises(type(error)) as caught:
                await call
            assert caught.value is error
    asyncio.run(run())
    assert trace.records == []


@pytest.mark.parametrize("raise_error", [False, True])
def test_read_wrappers_preserve_results_exceptions_and_restore(raise_error):
    trace, scope = diag.Trace(None), {}
    remote, aut = object(), object()
    item = SimpleNamespace(text="banana telescope elephant after call SMS")
    segments_result = ([item], [item], [])
    action_result = {"unchanged": object()}
    result = object()
    error = ValueError(SECRET)
    module = SimpleNamespace()
    def wait(*_args, **_kwargs):
        assert module._segments(aut, "ignored", "current") is segments_result
        call = SimpleNamespace(post_call_action_items=[{"action": "SMS banana telescope elephant", "details": SECRET, "status": "open"}])
        assert module._post_call_action_diagnostic(call, "banana telescope elephant") is action_result
        if raise_error:
            raise error
        return result
    module._wait_for_persisted_hosted_request = wait
    module._segments = lambda *_args: segments_result
    module._post_call_action_diagnostic = lambda *_args: action_result
    module._has_after_call_sms_intent = lambda text: "SMS" in text
    module._has_sms_action_intent = module._has_after_call_sms_intent
    originals = (module._wait_for_persisted_hosted_request, module._segments, module._post_call_action_diagnostic)
    restore = diag.install_test_observers(module, trace, scope)
    try:
        if raise_error:
            with pytest.raises(ValueError) as caught:
                module._wait_for_persisted_hosted_request(remote, "unused", "driver", aut, "current", "banana telescope elephant", deadline=123)
            assert caught.value is error
        else:
            assert module._wait_for_persisted_hosted_request(remote, "unused", "driver", aut, "current", "banana telescope elephant", deadline=123) is result
        diag.summarize(scope, trace)
    finally:
        restore()
    assert (module._wait_for_persisted_hosted_request, module._segments, module._post_call_action_diagnostic) == originals
    assert [record["phase"] for record in trace.records] == ["persisted_transcript", "persisted_transcript", "persisted_action"]
    assert SECRET not in json.dumps(trace.records)


def test_actual_sdk_activity_read_is_exact_scoped_whitelisted_and_closed(monkeypatch):
    from inkbox import Inkbox
    requests = []
    def handle(request):
        requests.append(request)
        assert request.method == "GET"
        assert request.url.path.endswith("/calls/current-call/tool-invocations")
        assert dict(request.url.params) == {"limit": "50", "offset": "0"}
        return httpx.Response(200, json={"items": [{
            "id": "00000000-0000-4000-8000-000000000001", "call_id": "00000000-0000-4000-8000-000000000002",
            "tool_name": "register_post_call_action", "status": "succeeded",
            "result": {"error_code": SECRET, "message": SECRET}, "started_at": "2026-01-01T00:00:00Z",
        }], "limit": 50, "offset": 0, "has_more": False})
    monkeypatch.setattr(httpx, "HTTPTransport", lambda **_kwargs: httpx.MockTransport(handle))
    client = Inkbox(api_key="synthetic-key", base_url="https://example.invalid", timeout=3)
    trace = diag.Trace(None)
    diag.read_tools({"aut_id": "current-call"}, trace, client_factory=lambda: client)
    assert len(requests) == 1
    assert client.calls._http._client.is_closed
    assert trace.records[0] == {"phase": "tool_activity", "status": "observed", "ordinal": 1,
                                "tool": "register_post_call_action", "tool_status": "succeeded", "error_code": "other"}
    assert SECRET not in json.dumps(trace.records)


def test_activity_failure_does_not_emit_exception_or_change_result():
    trace = diag.Trace(None)
    def fail():
        raise RuntimeError(SECRET)
    assert diag.read_tools({"aut_id": "current"}, trace, client_factory=fail) is None
    assert trace.records == [{"phase": "observer", "status": "unavailable"}]


@pytest.mark.parametrize("fails", [False, True])
def test_launcher_preserves_original_pytest_outcome_and_finally(tmp_path, monkeypatch, capsys, fails):
    path = tmp_path / f"test_launcher_{fails}.py"
    cleanup = tmp_path / "cleanup"
    path.write_text(f'''from pathlib import Path
def _wait_for_persisted_hosted_request(*args, **kwargs): pass
def _segments(*args): pass
def _post_call_action_diagnostic(*args): pass
def test_outbound_call_hosted_and_post_call_wakeup():
    try:
        assert {not fails!r}
    finally:
        Path({str(cleanup)!r}).write_text("cleaned")
''')
    monkeypatch.setenv("VOICE_DIAGNOSTIC_TEST", str(tmp_path / "trace"))
    result = diag.run_tests([str(path), "-q", "--confcutdir", str(tmp_path)])
    assert result == (1 if fails else 0)
    assert cleanup.read_text() == "cleaned"
    records = [json.loads(line) for line in (tmp_path / "trace").read_text().splitlines()]
    assert records[0]["status"] == ("failed" if fails else "passed")
    capsys.readouterr()


@pytest.mark.parametrize("boundary", ["setup", "restore"])
def test_observer_boundary_failure_preserves_test_and_cleanup(tmp_path, monkeypatch, capsys, boundary):
    path = tmp_path / f"test_broken_observer_{boundary}.py"
    cleanup = tmp_path / "cleanup"
    path.write_text(f'''from pathlib import Path
def test_outbound_call_hosted_and_post_call_wakeup():
    try:
        assert True
    finally:
        Path({str(cleanup)!r}).write_text("cleaned")
''')
    def fail():
        raise RuntimeError(SECRET)
    def install(*_args):
        if boundary == "setup":
            fail()
        return fail
    monkeypatch.setattr(diag, "install_test_observers", install)
    trace = tmp_path / "trace"
    monkeypatch.setenv("VOICE_DIAGNOSTIC_TEST", str(trace))
    assert diag.run_tests([str(path), "-q", "--confcutdir", str(tmp_path)]) == 0
    assert cleanup.read_text() == "cleaned"
    assert SECRET not in trace.read_text()
    assert '"unavailable"' in trace.read_text()
    capsys.readouterr()


@pytest.mark.parametrize("kind", ["nonzero", "malformed", "oversized", "malicious", "safe"])
def test_child_result_is_bounded_and_independently_reprojected(monkeypatch, kind):
    record = {"phase": "tool_activity", "status": "observed", "tool": "register_post_call_action", "text": SECRET}
    def run(argv, **kwargs):
        assert SECRET not in str(argv)
        assert kwargs["timeout"] == 8
        assert kwargs["capture_output"]
        assert json.loads(kwargs["input"]) == {"aut_id": SECRET}
        return SimpleNamespace(returncode=1 if kind == "nonzero" else 0, stderr=SECRET,
            stdout={"malformed": SECRET, "oversized": "x" * 65537,
                    "malicious": json.dumps([{"phase": SECRET, "status": "observed"}]),
                    "safe": json.dumps([record])}.get(kind, "[]"))
    monkeypatch.setattr(subprocess, "run", run)
    trace = diag.Trace(None)
    diag.read_tools_bounded({"aut_id": SECRET}, trace)
    assert SECRET not in json.dumps(trace.records)
    if kind == "safe":
        assert trace.records == [{"phase": "tool_activity", "status": "observed", "tool": "register_post_call_action"}]
    else:
        assert trace.records == [{"phase": "observer", "status": "unavailable"}]


def test_diagnostic_child_timeout_kills_and_reaps_process(tmp_path, monkeypatch):
    child = tmp_path / "slow.py"
    pid_file = tmp_path / "pid"
    child.write_text(f"import os,time\nfrom pathlib import Path\nPath({str(pid_file)!r}).write_text(str(os.getpid()))\ntime.sleep(60)\n")
    monkeypatch.setattr(diag, "__file__", str(child))
    trace = diag.Trace(None)
    started = time.monotonic()
    # Allow a cold interpreter to start even on a loaded CI worker. Its PID
    # handshake proves the child entered the long-lived work before termination.
    diag.read_tools_bounded({"aut_id": SECRET}, trace, timeout=3)
    assert time.monotonic() - started < 10
    assert pid_file.exists(), "diagnostic child did not reach its startup handshake"
    assert trace.records == [{"phase": "observer", "status": "unavailable"}]
    import os
    if os.name == "posix":
        with pytest.raises(ProcessLookupError):
            os.kill(int(pid_file.read_text()), 0)


def test_diagnostic_failure_cannot_replace_original_pytest_failure(tmp_path, monkeypatch, capsys):
    path = tmp_path / "test_diagnostic_failure.py"
    path.write_text('''
def _wait_for_persisted_hosted_request(*args, **kwargs): pass
def _segments(*args): pass
def _post_call_action_diagnostic(*args): pass
def test_outbound_call_hosted_and_post_call_wakeup():
    _wait_for_persisted_hosted_request(None, "number", "driver", None, "aut", "marker")
    assert False, "original task failure"
''')
    def fail(*_args):
        raise RuntimeError(SECRET)
    monkeypatch.setattr(diag, "read_tools_bounded", fail)
    trace = tmp_path / "trace"
    monkeypatch.setenv("VOICE_DIAGNOSTIC_TEST", str(trace))
    assert diag.run_tests([str(path), "-q", "--confcutdir", str(tmp_path)]) == 1
    records = [json.loads(line) for line in trace.read_text().splitlines()]
    assert records == [{"phase": "case", "status": "failed"}, {"phase": "observer", "status": "unavailable"}]
    output = capsys.readouterr().out
    assert "original task failure" in output and SECRET not in output
