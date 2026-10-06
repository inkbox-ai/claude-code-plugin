"""Privacy and behavioral transparency of the optional live-test trace launcher."""

import asyncio
import importlib.util
import json
from pathlib import Path
import subprocess
import sys

import httpx
import pytest


SPEC = importlib.util.spec_from_file_location(
    "ci_lifecycle_diagnostics", Path(__file__).parent / "live" / "diagnostic_gateway.py",
)
diagnostics = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(diagnostics)
SECRET = "synthetic-sensitive-body-argument-result-key@example.invalid"


def records(path):
    raw = path.read_text()
    assert SECRET not in raw
    return [json.loads(line) for line in raw.splitlines()]


def test_observer_preserves_async_result_and_observes_suppression_without_content(tmp_path):
    result = {"private_result": SECRET}

    class Session:
        _client_generation = 2
        _current_channel_tool_delivery = True
        _interrupting = False
        pending = None

        async def run(self, text, **kwargs):
            assert text == SECRET and kwargs == {"credential": SECRET}
            return result

    path = tmp_path / "trace"
    trace = diagnostics.Trace(path)
    trace.wrap(Session, "run", "turn", session_method=True)
    assert asyncio.run(Session().run(SECRET, credential=SECRET)) is result
    rows = records(path)
    assert [(row["phase"], row["status"]) for row in rows] == [
        ("turn", "start"), ("turn", "ok"), ("automatic_reply", "observed"),
    ]
    assert rows[-1]["detail"] == "withheld_after_tool"
    assert all(row["turn"] == 1 and row["generation"] == 2 for row in rows)


@pytest.mark.parametrize("cancelled", [False, True])
def test_observer_preserves_exact_exception_and_cancellation_without_message(tmp_path, cancelled):
    error = asyncio.CancelledError(SECRET) if cancelled else RuntimeError(SECRET)

    class Host:
        async def query(self, text):
            assert text == SECRET
            raise error

    path = tmp_path / "trace"
    trace = diagnostics.Trace(path)
    trace.wrap(Host, "query", "query")
    with pytest.raises(type(error)) as caught:
        asyncio.run(Host().query(SECRET))
    assert caught.value is error
    rows = records(path)
    assert rows[-1]["status"] == ("cancelled" if cancelled else "error")


def test_actual_sdk_delete_is_observed_without_identifiers_headers_or_response(tmp_path, monkeypatch):
    from inkbox import Inkbox
    from inkbox.contacts.resources.contacts import ContactsResource

    class ObservedContacts(ContactsResource):
        pass

    requests = []

    def handle(request):
        requests.append(request)
        return httpx.Response(204)

    # Keep the real SDK HTTP stack and contact method; observe a subclass only.
    monkeypatch.setenv("INKBOX_VAULT_KEY", "")
    monkeypatch.setattr("inkbox._config._CONFIG_PATH", tmp_path / "absent-sdk-config")
    monkeypatch.setattr(httpx, "HTTPTransport", lambda **kwargs: httpx.MockTransport(handle))
    client = Inkbox(api_key="synthetic-test-key", base_url="https://example.invalid")
    path = tmp_path / "trace"
    trace = diagnostics.Trace(path)
    trace.wrap(ObservedContacts, "delete", "contacts_delete")
    try:
        resource = ObservedContacts(client.contacts._http)
        assert resource.delete(SECRET) is None
    finally:
        client.close()
    assert requests and requests[0].method == "DELETE"
    assert SECRET in str(requests[0].url)
    assert [row["status"] for row in records(path)] == ["start", "ok"]


def test_tool_observation_uses_fixed_names_not_arguments_or_unknown_names(tmp_path):
    class Session:
        async def hook(self, data):
            return data

    path = tmp_path / "trace"
    trace = diagnostics.Trace(path)
    trace.wrap(Session, "hook", "tool", session_method=True)
    session = Session()
    for name in ("mcp__inkbox__inkbox_delete_contact", f"mcp__inkbox__{SECRET}"):
        data = {"tool_name": name, "tool_input": {"contact_id": SECRET}, "result": SECRET}
        assert asyncio.run(session.hook(data)) is data
    rows = records(path)
    assert rows[0]["tool"] == "inkbox_delete_contact"
    assert "tool" not in rows[2]


def test_trace_io_failure_does_not_change_real_call(tmp_path):
    class Host:
        def send(self, payload):
            return payload

    diagnostics.Trace(tmp_path / "missing" / "trace").wrap(Host, "send", "mail_send")
    value = {"body": SECRET}
    assert Host().send(value) is value


def test_report_revalidates_tampered_trace_and_never_prints_sensitive_fields(tmp_path, capsys):
    path = tmp_path / "trace"
    path.write_text("\n".join([
        json.dumps({"phase": "contacts_delete", "status": "error", "detail": SECRET,
                    "tool": SECRET, "args": SECRET, "error": SECRET, "time_ms": SECRET,
                    "http_status": 123456789, "blocked": SECRET}),
        json.dumps({"phase": SECRET, "status": "ok", "body": SECRET}),
        json.dumps({"phase": "query", "status": "start", "detail": {"secret": SECRET}}),
        SECRET,
    ]))
    diagnostics.report(path)
    output = capsys.readouterr().out
    assert SECRET not in output and "123456789" not in output
    rows = [json.loads(line) for line in output.splitlines()]
    assert rows[0] == {"phase": "contacts_delete", "status": "error", "detail": "other"}
    assert rows[-1] == {"phase_counts": {"contacts_delete.error": 1}}


@pytest.mark.parametrize("cancel", [False, True])
def test_receive_observation_closes_underlying_generator_when_consumer_stops(tmp_path, cancel):
    closed = []
    entered = asyncio.Event()
    message = {"body": SECRET}

    class Host:
        async def receive_response(self):
            try:
                yield message
                entered.set()
                await asyncio.Event().wait()
            finally:
                closed.append(True)

    trace = diagnostics.Trace(tmp_path / "trace")
    trace.wrap_receive(Host)

    async def run():
        response = Host().receive_response()
        assert await anext(response) is message
        if cancel:
            waiting = asyncio.create_task(anext(response))
            await entered.wait()
            waiting.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiting
        else:
            await response.aclose()
        assert closed == [True]

    asyncio.run(run())
    records(tmp_path / "trace")


def test_late_callback_retains_captured_turn_after_session_mapping_advances(tmp_path):
    session = object()
    trace = diagnostics.Trace(tmp_path / "trace")

    async def run():
        release = asyncio.Event()

        async def old_callback():
            await release.wait()
            trace.emit("delivery", "ok", session=session)

        token = trace.turn.set(1)
        task = asyncio.create_task(old_callback())
        trace.turn.reset(token)
        trace.session_turns[id(session)] = 2
        release.set()
        await task
        trace.emit("inbound", "start", session=session)

    asyncio.run(run())
    assert [row["turn"] for row in records(tmp_path / "trace")] == [1, 2]


def test_install_targets_supported_real_sdk_and_native_host_methods_in_isolated_process(tmp_path):
    script = str(Path(__file__).parent / "live" / "diagnostic_gateway.py")
    program = """
import inspect, runpy, sys
module = runpy.run_path(sys.argv[1])
module['install'](sys.argv[2])
from claude_agent_sdk import ClaudeSDKClient
from inkbox.contacts.resources.contacts import ContactsResource
from inkbox_claude.sessions import ContactSession
assert inspect.iscoroutinefunction(ClaudeSDKClient.query)
assert inspect.isasyncgenfunction(ClaudeSDKClient.receive_response)
assert inspect.signature(ContactsResource.delete) == inspect.signature(ContactsResource.delete.__wrapped__)
assert inspect.signature(ContactSession._run_turn) == inspect.signature(ContactSession._run_turn.__wrapped__)
print('installed')
"""
    result = subprocess.run([sys.executable, "-c", program, script, str(tmp_path / "trace")],
                            capture_output=True, text=True, check=True)
    assert result.stdout.strip() == "installed"
