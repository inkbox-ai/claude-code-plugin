import asyncio
import base64
import hashlib
import hmac
import json
import types

import aiohttp
import aiohttp.web
import pytest

from inkbox_claude import gateway as gateway_module
from inkbox_claude import inbound
from inkbox_claude.config import BridgeConfig, read_config
from inkbox_claude.gateway import InkboxGateway

SECRET = "whsec_test"


@pytest.fixture(autouse=True)
def isolated_home(monkeypatch, tmp_path):
    monkeypatch.setattr(gateway_module, "web", aiohttp.web)
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("INKBOX_CLAUDE_HOME", str(tmp_path / ".inkbox-claude"))
    return tmp_path


class _Session:
    def __init__(self, turns, chat_id):
        self.turns, self.chat_id = turns, chat_id

    async def run_consult(self, prompt):
        self.turns.append((self.chat_id, prompt))
        return ""


class _Sessions:
    def __init__(self):
        self.turns = []

    def get(self, chat_id, system_prompt_extra=""):
        return _Session(self.turns, chat_id)

    async def close_all(self):
        return None


class _Inbound:
    def __init__(self):
        self.reports = []

    def report_inbound_event_result(self, handle, source_id, event_id, *, status, error):
        self.reports.append((handle, source_id, event_id, status, error))


def _gateway(*, local=True):
    gw = InkboxGateway(BridgeConfig(identity="agent", signing_key=SECRET, local_delivery=local, port=0))
    gw.sessions = _Sessions()
    gw._inkbox = types.SimpleNamespace(inbound=_Inbound())
    return gw


def _data(handler=None, body='{"ok":true}', encoding="utf8", event_id="inbev_1", instructions="Log it.",
          delivery_id="inbdel_1"):
    return {
        "event_id": event_id,
        "delivery_id": delivery_id,
        "received_at": "2026-01-01T00:00:00Z",
        "source": {"id": "inbsrc_1", "name": "payments", "instructions": instructions},
        "handler": handler or {"type": "agent"},
        "signature_verified": True,
        "request": {
            "method": "POST", "query": {}, "headers": {"content-type": "application/json"},
            "content_type": "application/json", "body": body, "body_encoding": encoding,
        },
    }


def _signed(body: bytes, secret=SECRET):
    request_id, timestamp = "rid-1", "1700000000"
    digest = hmac.new(secret.removeprefix("whsec_").encode(), f"{request_id}.{timestamp}.".encode() + body,
                      hashlib.sha256).hexdigest()
    return {"X-Inkbox-Signature": "sha256=" + digest, "X-Inkbox-Request-Id": request_id,
            "X-Inkbox-Timestamp": timestamp, "Content-Type": "application/json"}


async def _drain():
    for _ in range(50):
        await asyncio.sleep(0)
    while any(t is not asyncio.current_task() and not t.done() for t in asyncio.all_tasks()):
        await asyncio.sleep(0.01)


def test_delivery_mode_env(monkeypatch):
    monkeypatch.delenv("INKBOX_DELIVERY_MODE", raising=False)
    assert read_config().local_delivery is False
    monkeypatch.setenv("INKBOX_DELIVERY_MODE", "local")
    assert read_config().local_delivery is True
    monkeypatch.setenv("INKBOX_DELIVERY_MODE", "remote")
    with pytest.raises(ValueError):
        read_config()


def test_local_listener_is_loopback_signed_and_accepts_2_mib():

    async def scenario():
        gw = _gateway()
        gw.cfg.host = "0.0.0.0"
        await gw._start_http_server()
        try:
            host, port = gw._runner.addresses[0][:2]
            assert host == "127.0.0.1"
            url = f"http://127.0.0.1:{port}/webhook"
            large = json.dumps({"event_type": "inbound.received",
                                "data": _data(body="x" * (1536 * 1024))}).encode()
            async with aiohttp.ClientSession() as http:
                async with http.post(url, data=large, headers=_signed(large)) as resp:
                    assert resp.status == 200
                async with http.post(url, data=large, headers=_signed(large, "whsec_wrong")) as resp:
                    assert resp.status == 401
                oversized = b"{" + b" " * (2 * 1024 * 1024) + b"}"
                async with http.post(url, data=oversized, headers=_signed(oversized)) as resp:
                    assert resp.status == 413
            await _drain()
            assert len(gw.sessions.turns) == 1
        finally:
            await gw._runner.cleanup()

    asyncio.run(scenario())


def test_local_startup_opens_no_tunnel_and_registers_nothing(monkeypatch):
    monkeypatch.setattr(gateway_module, "AIOHTTP_AVAILABLE", True)
    monkeypatch.setattr(gateway_module, "INKBOX_AVAILABLE", True)
    identity = types.SimpleNamespace(agent_handle="agent", mailbox=None, phone_number=None, tunnel=None, id="id")
    monkeypatch.setattr(gateway_module, "Inkbox",
                        lambda **_kw: types.SimpleNamespace(get_identity=lambda _h: identity), raising=False)
    monkeypatch.setattr(gateway_module, "build_inkbox_mcp_server", lambda *_a: (None, []))
    monkeypatch.setattr(gateway_module, "SessionManager", lambda **_kw: _Sessions())
    gw = InkboxGateway(BridgeConfig(api_key="k", identity="agent", local_delivery=True,
                                    public_url="https://example.invalid"))

    async def noop(*_a, **_kw):
        return None

    def forbidden(*_a, **_kw):
        raise AssertionError("local delivery must not open a tunnel or register a URL")

    for name in ("_start_http_server", "_recover_channel_inputs", "_catch_up_a2a_tasks",
                 "_recover_hosted_call_completions"):
        monkeypatch.setattr(gw, name, noop)
    monkeypatch.setattr(gw, "_open_tunnel", forbidden)
    monkeypatch.setattr(gw, "_patch_identity_objects", forbidden)
    recover = types.SimpleNamespace(recover=lambda: None)
    monkeypatch.setattr(gw, "_voice_note_inbox", lambda: recover)
    monkeypatch.setattr(gw, "_companion_receiver", lambda: recover)

    async def scenario():
        task = asyncio.create_task(gw.run())
        while not gw._runtime_ready:
            assert not task.done(), task.exception()
            await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert gw._public_url == ""
        assert "local delivery" in await gw.health_report()

    monkeypatch.setattr(gateway_module, "_claude_health", lambda: "ok")
    asyncio.run(scenario())


def test_inbound_ignored_outside_local_mode():
    async def scenario():
        gw = _gateway(local=False)
        resp = await gw._dispatch_verified({"event_type": "inbound.received", "data": _data()}, "inkbox", "r")
        await _drain()
        return gw, resp

    gw, resp = asyncio.run(scenario())
    assert resp.status == 200 and gw.sessions.turns == []


def test_agent_handler_prompt_session_and_dedupe():
    async def scenario():
        gw = _gateway()
        envelope = {"event_type": "inbound.received", "data": _data()}
        first = await gw._dispatch_verified(envelope, "inkbox", "r1")
        second = await gw._dispatch_verified(envelope, "inkbox", "r2")
        await _drain()
        third = await gw._dispatch_verified(envelope, "inkbox", "r3")
        await _drain()
        return gw, first, second, third

    gw, first, second, third = asyncio.run(scenario())
    assert first.status == 200
    assert json.loads(second.text)["deduped"] and json.loads(third.text)["deduped"]
    [(chat_id, prompt)] = gw.sessions.turns
    assert chat_id == "inbound:payments"
    assert "untrusted external content" in prompt and '"payments"' in prompt
    assert prompt.index("Log it.") < prompt.index('{"ok":true}')


def _dispatch_all(datas):
    async def scenario():
        gw = _gateway()
        responses = []
        for data in datas:
            responses.append(await gw._dispatch_verified({"event_type": "inbound.received", "data": data}, "inkbox", "r"))
            await _drain()
        return gw, [json.loads(r.text).get("deduped", False) for r in responses]

    return asyncio.run(scenario())


def test_replay_with_new_delivery_id_runs_again():
    gw, deduped = _dispatch_all([_data(), _data(), _data(delivery_id="inbdel_2")])
    assert deduped == [False, True, False]
    assert len(gw.sessions.turns) == 2


def test_dedupe_falls_back_to_event_id():
    gw, deduped = _dispatch_all([_data(delivery_id=None), _data(delivery_id=None), _data(delivery_id=None, event_id="inbev_2")])
    assert deduped == [False, True, False]
    assert len(gw.sessions.turns) == 2


def test_prompt_body_capped_at_64_kib_on_char_boundary():
    body = "a" + "\u00e9" * 40000  # the 65536-byte cut lands inside a 2-byte char
    shown = inbound.inbound_prompt("s", "e", "", body, "utf8").split("Body:\n", 1)[1]
    assert shown.endswith("\n(truncated)")
    kept = shown[: -len("\n(truncated)")]
    assert len(kept.encode()) == 65535 and kept == body[: len(kept)]
    assert inbound.inbound_prompt("s", "e", "", "x" * 65536, "utf8").endswith("Body:\n" + "x" * 65536)


def test_agent_handler_binary_body_is_not_inlined():
    prompt = inbound.inbound_prompt("s", "e", "", base64.b64encode(b"\x00\x01").decode(), "base64")
    assert "binary content" in prompt and "AAE=" not in prompt


def _run_event(gw, data):
    asyncio.run(inbound.run_inbound_event(gw, data))


def test_script_empty_stdout_is_done_and_reads_stdin_in_home(isolated_home):
    gw = _gateway()
    _run_event(gw, _data({"type": "script", "command": "cat > seen.json; pwd > cwd.txt"}))
    assert gw.sessions.turns == [] and gw._inkbox.inbound.reports == []
    stdin = json.loads((isolated_home / "seen.json").read_text())
    assert stdin == {"source": "payments", "event_id": "inbev_1", "headers": {"content-type": "application/json"},
                     "body": '{"ok":true}', "body_encoding": "utf8"}
    assert (isolated_home / "cwd.txt").read_text().strip() == str(isolated_home)


def test_script_stdout_becomes_agent_message():
    gw = _gateway()
    _run_event(gw, _data({"type": "script", "command": "echo charge succeeded"}))
    [(chat_id, prompt)] = gw.sessions.turns
    assert chat_id == "inbound:payments" and "charge succeeded" in prompt and '{"ok":true}' not in prompt


def test_script_failure_retries_then_reports(monkeypatch, isolated_home):
    sleeps = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(inbound.asyncio, "sleep", fake_sleep)
    gw = _gateway()
    _run_event(gw, _data({"type": "script", "command": "echo run >> runs; echo boom >&2; exit 3"}))
    assert sleeps == [10.0, 30.0, 120.0, 600.0]
    assert (isolated_home / "runs").read_text().count("run") == 5
    assert gw._inkbox.inbound.reports == [("agent", "inbsrc_1", "inbev_1", "failed", "boom")]
    assert gw.sessions.turns == []


def test_script_timeout_counts_as_failure(monkeypatch):
    async def fake_sleep(_delay):
        return None

    monkeypatch.setattr(inbound.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(inbound, "SCRIPT_RETRY_DELAYS", ())
    monkeypatch.setattr(inbound.run_script, "__defaults__", (0.2,))
    gw = _gateway()
    _run_event(gw, _data({"type": "script", "command": "sleep 5"}))
    assert gw._inkbox.inbound.reports[0][3:] == ("failed", "timed out after 0.2s")


def test_report_is_skipped_when_sdk_lacks_method(monkeypatch):
    monkeypatch.setattr(inbound, "SCRIPT_RETRY_DELAYS", ())
    gw = _gateway()
    gw._inkbox = types.SimpleNamespace()
    _run_event(gw, _data({"type": "script", "command": "exit 1"}))
