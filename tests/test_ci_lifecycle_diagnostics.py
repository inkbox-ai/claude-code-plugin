"""Privacy and behavioral transparency of the optional live-test trace launcher."""

import asyncio
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

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
    for name in ("mcp__inkbox__inkbox_delete_contact", "mcp__inkbox__inkbox_lookup_contact",
                 f"mcp__inkbox__{SECRET}"):
        data = {"tool_name": name, "tool_input": {"contact_id": SECRET}, "result": SECRET}
        assert asyncio.run(session.hook(data)) is data
    rows = records(path)
    assert rows[0]["tool"] == "inkbox_delete_contact"
    assert rows[2]["tool"] == "inkbox_lookup_contact"
    assert "tool" not in rows[4]


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


def test_real_native_init_reports_only_known_list_presence_and_preserves_messages(tmp_path):
    from claude_agent_sdk import SystemMessage

    known = diagnostics.CONTACT_TOOLS
    messages = [
        SystemMessage(subtype="init", data={"tools": [
            "mcp__inkbox__" + name for name in known if name != "inkbox_delete_contact"
        ] + [SECRET], "session_id": SECRET}),
        SystemMessage(subtype="compact_boundary", data={"tools": list(known), "body": SECRET}),
        SystemMessage(subtype="init", data={"tools": [{"name": SECRET}]}),
        SystemMessage(subtype="init", data={"unavailable": SECRET}),
        SimpleNamespace(subtype="init", data={"tools": list(known)}),
    ]

    class Host:
        async def receive_response(self):
            for message in messages:
                yield message

    path = tmp_path / "trace"
    trace = diagnostics.Trace(path)
    trace.wrap_receive(Host)

    async def receive():
        return [message async for message in Host().receive_response()]

    received = asyncio.run(receive())
    assert all(a is b for a, b in zip(received, messages, strict=True))
    observed = [r for r in records(path) if r["phase"] == "native_tools"]
    assert len(observed) == 1
    assert observed[0]["contact_tools"] == {name: name != "inkbox_delete_contact" for name in known}


def test_native_init_observation_failure_cannot_change_delivery(tmp_path):
    from claude_agent_sdk import SystemMessage

    class Unreadable(dict):
        def get(self, _key):
            raise RuntimeError(SECRET)

    message = SystemMessage(subtype="init", data=Unreadable())
    trace = diagnostics.Trace(tmp_path / "trace")
    trace.observe_native_tools(message)
    assert not (tmp_path / "trace").exists()


@pytest.mark.parametrize("ignored,detail", [(None, "none"), ("stale-a2a-event", "a2a_stale"),
                                           ("task-completed", "a2a_stopped"),
                                           ("task-input_required", "a2a_stopped"),
                                           ("task-auth_required", "a2a_stopped"), (SECRET, "other")])
def test_a2a_admission_observer_preserves_exact_return_and_projects_only_outcome(tmp_path, ignored, detail):
    result = ({"private_task": SECRET}, ignored)

    class Gateway:
        async def admit(self, private_payload):
            assert private_payload == SECRET
            return result

    trace = diagnostics.Trace(tmp_path / "trace")
    trace.wrap(Gateway, "admit", "a2a_admission")
    assert asyncio.run(Gateway().admit(SECRET)) is result
    observed = [r for r in records(tmp_path / "trace") if r["status"] == "observed"]
    assert len(observed) == 1 and observed[0]["admitted"] is (ignored is None)
    assert observed[0]["detail"] == detail


@pytest.mark.parametrize("status", [200, 502])
def test_actual_published_a2a_card_send_observation_does_not_change_protocol(tmp_path, monkeypatch, status):
    from inkbox.a2a.client import A2AClient

    class ObservedA2A(A2AClient):
        pass

    requests = []

    def handle(request):
        requests.append(request)
        if request.method == "GET":
            return httpx.Response(200, json={"supportedInterfaces": [{
                "protocolVersion": "1.0", "protocolBinding": "JSONRPC", "url": "https://example.invalid/rpc",
            }]})
        body = json.loads(request.content)
        assert body["params"]["message"]["parts"] == [{"text": SECRET}]
        return httpx.Response(status, json={"jsonrpc": "2.0", "id": body["id"], "result": {"task": {
            "id": SECRET, "contextId": SECRET, "status": {"state": "TASK_STATE_WORKING"},
        }}})

    monkeypatch.setattr(httpx, "HTTPTransport", lambda **_kwargs: httpx.MockTransport(handle))
    trace = diagnostics.Trace(tmp_path / "trace")
    trace.wrap(ObservedA2A, "fetch_card", "a2a_card")
    trace.wrap(ObservedA2A, "send", "a2a_send")
    client = ObservedA2A(api_key=SECRET, platform_base_url="https://example.invalid")
    try:
        target = client.fetch_card("https://example.invalid/card")
        if status == 200:
            result = client.send(target, text=SECRET)
            assert result.task.id == SECRET
        else:
            with pytest.raises(httpx.HTTPStatusError) as caught:
                client.send(target, text=SECRET)
            assert caught.value.response.status_code == status
    finally:
        client.close()
    assert [r.method for r in requests] == ["GET", "POST"]
    rows = records(tmp_path / "trace")
    assert [(r["phase"], r["status"]) for r in rows] == [
        ("a2a_card", "start"), ("a2a_card", "ok"), ("a2a_send", "start"),
        ("a2a_send", "ok" if status == 200 else "error"),
    ]
    if status != 200:
        assert rows[-1]["http_status"] == status


def test_partial_install_failure_restores_originals_and_keeps_gateway_result(tmp_path, monkeypatch, capsys):
    from inkbox_claude.sessions import ContactSession

    original = ContactSession.handle_inbound
    wrap = diagnostics.Trace.wrap
    calls = []

    def fail_second(self, cls, name, *args, **kwargs):
        calls.append(name)
        if len(calls) == 2:
            raise RuntimeError(SECRET)
        wrap(self, cls, name, *args, **kwargs)

    monkeypatch.setattr(diagnostics.Trace, "wrap", fail_second)
    result = object()
    assert diagnostics.run_gateway(tmp_path / "trace", lambda: result) is result
    assert calls == ["handle_inbound", "_run_turn"]
    assert ContactSession.handle_inbound is original
    assert SECRET not in capsys.readouterr().err


def test_failed_observer_startup_preserves_exact_gateway_exception(tmp_path, monkeypatch, capsys):
    failure = RuntimeError(SECRET)

    def unavailable(_path):
        raise ValueError(SECRET)

    def gateway():
        raise failure

    monkeypatch.setattr(diagnostics, "install", unavailable)
    with pytest.raises(RuntimeError) as caught:
        diagnostics.run_gateway(tmp_path / "trace", gateway)
    assert caught.value is failure
    assert SECRET not in capsys.readouterr().err


def test_init_list_presence_report_reprojects_fixed_boolean_schema(tmp_path, capsys):
    path = tmp_path / "trace"
    known = {name: True for name in diagnostics.CONTACT_TOOLS}
    path.write_text("\n".join(json.dumps({"phase": "native_tools", "status": "observed",
        "contact_tools": value, "tool_arguments": SECRET, "session_id": SECRET}) for value in [
            known, {**known, SECRET: True}, {**known, "inkbox_get_contact": SECRET},
        ]))
    diagnostics.report(path)
    output = capsys.readouterr().out
    assert SECRET not in output
    rows = [json.loads(line) for line in output.splitlines()]
    assert rows[0]["contact_tools"] == known
    assert "contact_tools" not in rows[1] and "contact_tools" not in rows[2]
