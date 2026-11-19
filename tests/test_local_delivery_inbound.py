import asyncio
import base64
import hashlib
import hmac
import json
import time
import types

import aiohttp
import aiohttp.web
import pytest

from inkbox_claude import gateway as gateway_module
from inkbox_claude import inbound
from inkbox_claude import managed_work
from inkbox_claude.config import BridgeConfig, read_config
from inkbox_claude.gateway import InkboxGateway

SECRET = "whsec_test"


@pytest.fixture(autouse=True)
def isolated_home(monkeypatch, tmp_path):
    monkeypatch.setattr(gateway_module, "web", aiohttp.web)
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("INKBOX_CLAUDE_HOME", str(tmp_path / ".inkbox-claude"))
    return tmp_path


@pytest.fixture
def instant_waits(monkeypatch):
    """Skip retry waits, recording each delay in whole seconds."""
    waits = []

    async def wait(self, due):
        delay = round(due - time.time())
        if delay > 0:
            waits.append(delay)

    monkeypatch.setattr(inbound.InboundRunner, "_wait", wait)
    return waits


class _Session:
    def __init__(self, sessions, chat_id):
        self.sessions, self.chat_id = sessions, chat_id

    async def run_consult(self, prompt):
        self.sessions.turns.append((self.chat_id, prompt))
        self.sessions.owners.append(managed_work._current.get())
        if self.sessions.block is not None:
            await self.sessions.block.wait()
        if self.sessions.fail:
            raise RuntimeError("model unavailable")
        return ""


class _Sessions:
    def __init__(self):
        self.turns = []
        self.owners = []
        self.block = None
        self.fail = False

    def get(self, chat_id, system_prompt_extra=""):
        return _Session(self, chat_id)

    async def close_all(self):
        return None


class StaleDelivery(Exception):
    status_code = 409
    detail = "stale_delivery"


class _Inbound:
    def __init__(self, errors=()):
        self.reports = []
        self.errors = list(errors)

    def report_inbound_event_result(self, handle, source_id, event_id, *, status, error, delivery_id):
        self.reports.append((handle, source_id, event_id, status, error, delivery_id))
        if self.errors:
            raise self.errors.pop(0)


def _gateway(*, local=True, require_signature=True):
    gw = InkboxGateway(BridgeConfig(identity="agent", signing_key=SECRET, local_delivery=local, port=0,
                                    require_signature=require_signature))
    gw.sessions = _Sessions()
    gw._inkbox = types.SimpleNamespace(inbound=_Inbound())
    return gw


def _data(handler=None, body='{"ok":true}', encoding="utf8", event_id="inbev_1", instructions="Log it.",
          delivery_id="5f0c6c1e-0000-4000-8000-000000000001"):
    return {
        "event_id": event_id,
        "delivery_id": delivery_id,
        "received_at": "2026-01-01T00:00:00+00:00",
        "source": {"id": "inbsrc_1", "name": "payments", "instructions": instructions},
        "handler": handler or {"type": "agent"},
        "signature_verified": True,
        "request": {
            "method": "POST", "query": {}, "headers": {"content-type": "application/json"},
            "content_type": "application/json", "body": body, "body_encoding": encoding,
        },
    }


def _envelope(data, activation_id="0b7d8a52-1f8e-4c4e-9a52-5c2f9d0e3c11"):
    """The managed delivery exactly as the server sends it: payload plus id and activation binding."""
    event_id = "evt_" + hashlib.sha256(f"inbound:{data['delivery_id']}".encode()).hexdigest()
    return {
        "event_type": "inbound.received",
        "timestamp": "2026-01-01T00:00:01+00:00",
        "data": data,
        "id": event_id,
        "hosted_delivery": {"version": 1, "activation_id": activation_id, "event_id": event_id,
                            "starts_agent": True},
    }


def _encode(envelope):
    return json.dumps(envelope, separators=(",", ":"), sort_keys=True, ensure_ascii=False).encode()


def _signed(body: bytes, secret=SECRET):
    request_id, timestamp = "rid-1", str(int(time.time()))
    digest = hmac.new(secret.removeprefix("whsec_").encode(), f"{request_id}.{timestamp}.".encode() + body,
                      hashlib.sha256).hexdigest()
    return {"X-Inkbox-Signature": "sha256=" + digest, "X-Inkbox-Request-Id": request_id,
            "X-Inkbox-Timestamp": timestamp, "Content-Type": "application/json"}


async def _settled(gw):
    async with asyncio.timeout(10):
        while gw._inbound.queue.pending():
            await asyncio.sleep(0.01)


async def _receive(gw, data):
    response = await gw._dispatch_verified(_envelope(data), "inkbox", "r")
    return response.status, json.loads(response.text) if response.status == 200 else response.text


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
        gw._inbound.start()
        await gw._start_http_server()
        try:
            host, port = gw._runner.addresses[0][:2]
            assert host == "127.0.0.1"
            url = f"http://127.0.0.1:{port}/webhook"
            large = _encode(_envelope(_data(body="x" * (1536 * 1024))))
            async with aiohttp.ClientSession() as http:
                async with http.post(url, data=large, headers=_signed(large)) as resp:
                    assert resp.status == 200
                async with http.post(url, data=large, headers=_signed(large, "whsec_wrong")) as resp:
                    assert resp.status == 401
                oversized = b"{" + b" " * (2 * 1024 * 1024) + b"}"
                async with http.post(url, data=oversized, headers=_signed(oversized)) as resp:
                    assert resp.status == 413
            await _settled(gw)
            assert len(gw.sessions.turns) == 1
        finally:
            await gw._runner.cleanup()
            await gw._inbound.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("headers", [{}, {"X-Hub-Signature-256": "sha256=00"}])
def test_inbound_requires_inkbox_signature_even_when_signatures_are_optional(headers):
    async def scenario():
        gw = _gateway(require_signature=False)
        body = _encode(_envelope(_data()))
        request = types.SimpleNamespace(read=lambda: _async(body), headers=headers, url="http://127.0.0.1/webhook")
        rejected = await gw._handle_webhook(request)
        request.headers = _signed(body)
        accepted = await gw._handle_webhook(request)
        pending = gw.inbound_activity()
        await gw._inbound.close()
        return rejected.status, accepted.status, pending

    assert asyncio.run(scenario()) == (401, 200, 1)


async def _async(value):
    return value


def test_local_startup_opens_no_tunnel_resumes_queue_and_registers_nothing(monkeypatch):
    monkeypatch.setattr(gateway_module, "AIOHTTP_AVAILABLE", True)
    monkeypatch.setattr(gateway_module, "INKBOX_AVAILABLE", True)
    identity = types.SimpleNamespace(agent_handle="agent", mailbox=None, phone_number=None, tunnel=None, id="id")
    monkeypatch.setattr(gateway_module, "Inkbox",
                        lambda **_kw: types.SimpleNamespace(get_identity=lambda _h: identity), raising=False)
    monkeypatch.setattr(gateway_module, "build_inkbox_mcp_server", lambda *_a: (None, []))
    sessions = _Sessions()
    monkeypatch.setattr(gateway_module, "SessionManager", lambda **_kw: sessions)
    gw = InkboxGateway(BridgeConfig(api_key="k", identity="agent", local_delivery=True,
                                    public_url="https://example.invalid"))
    # Received before the runtime was ready: persisted, then run once it is.
    assert gw._inbound.receive(_data())

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
        await _settled(gw)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert gw._public_url == ""
        assert "local delivery" in await gw.health_report()

    monkeypatch.setattr(gateway_module, "_claude_health", lambda: "ok")
    asyncio.run(scenario())
    assert [chat for chat, _ in sessions.turns] == ["inbound:payments"]


def test_inbound_ignored_outside_local_mode():
    async def scenario():
        gw = _gateway(local=False)
        return gw, await _receive(gw, _data())

    gw, (status, body) = asyncio.run(scenario())
    assert status == 200 and body["ignored"] == "inbound.received" and gw.sessions.turns == []


def test_managed_delivery_completes_once_persisted_while_script_is_pending(isolated_home, instant_waits):
    """The delivery receipt settles at persistence; the script keeps running outside it."""
    gate = isolated_home / "gate"

    async def scenario():
        gw = _gateway()
        gw._inbound.start()
        work = managed_work.DeliveryWork()
        with managed_work.scope((work,)):
            status, _ = await _receive(gw, _data({"type": "script", "command": f"while [ ! -e {gate} ]; do sleep 0.02; done; echo ready"}))
        work.seal()
        async with asyncio.timeout(1):
            assert await work.done is True
        assert status == 200 and gw.inbound_activity() == 1
        gate.touch()
        await _settled(gw)
        await gw._inbound.close()
        return gw

    gw = asyncio.run(scenario())
    [(chat, prompt)] = gw.sessions.turns
    assert chat == "inbound:payments" and "ready" in prompt
    # The later agent turn must not join the already completed delivery.
    assert gw.sessions.owners == [()]


def test_queue_survives_restart_including_retry_wait(isolated_home, monkeypatch):
    command = "echo run >> runs; [ -e ok ] || exit 1"

    async def first():
        gw = _gateway()
        gw._inbound.start()
        await _receive(gw, _data({"type": "script", "command": command}))
        async with asyncio.timeout(5):
            while not (isolated_home / "runs").exists() or gw._inbound.queue.get(_data()["delivery_id"])["due"] <= time.time():
                await asyncio.sleep(0.01)
        # Waiting out the first retry delay is still activity.
        assert gw.inbound_activity() == 1
        await gw._inbound.close()

    asyncio.run(first())
    (isolated_home / "ok").touch()

    async def second():
        gw = _gateway()
        assert gw.inbound_activity() == 1
        gw._inbound.start()
        await _settled(gw)
        await gw._inbound.close()
        return gw

    waits = []

    async def wait(self, due):
        waits.append(round(due - time.time()))

    monkeypatch.setattr(inbound.InboundRunner, "_wait", wait)
    gw = asyncio.run(second())
    assert waits == [10]
    assert (isolated_home / "runs").read_text().count("run") == 2
    assert gw._inkbox.inbound.reports == [] and gw.sessions.turns == []


def test_dedupe_on_delivery_id_across_restart_and_replay_runs_again():
    async def scenario(gw, *datas):
        gw._inbound.start()
        results = []
        for data in datas:
            results.append((await _receive(gw, data))[1].get("deduped", False))
            await _settled(gw)
        await gw._inbound.close()
        return results

    gw = _gateway()
    assert asyncio.run(scenario(gw, _data(), _data())) == [False, True]
    again = _gateway()
    replay = _data(delivery_id="5f0c6c1e-0000-4000-8000-000000000002")
    assert asyncio.run(scenario(again, _data(), replay)) == [True, False]
    assert len(gw.sessions.turns) == 1 and len(again.sessions.turns) == 1


def test_missing_delivery_id_is_rejected():
    async def scenario():
        gw = _gateway()
        data = _data()
        data["delivery_id"] = None
        response = await gw._dispatch_verified({"event_type": "inbound.received", "data": data}, "inkbox", "r")
        await gw._inbound.close()
        return response.status

    assert asyncio.run(scenario()) == 400


def test_agent_handler_prompt_session():
    async def scenario():
        gw = _gateway()
        gw._inbound.start()
        await _receive(gw, _data())
        await _settled(gw)
        await gw._inbound.close()
        return gw

    gw = asyncio.run(scenario())
    [(chat_id, prompt)] = gw.sessions.turns
    assert chat_id == "inbound:payments"
    assert "untrusted external content" in prompt and '"payments"' in prompt
    assert prompt.index("Log it.") < prompt.index('{"ok":true}')


def test_prompt_body_capped_at_64_kib_on_char_boundary():
    body = "a" + "é" * 40000  # the 65536-byte cut lands inside a 2-byte char
    shown = inbound.inbound_prompt("s", "e", "", body, "utf8").split("Body:\n", 1)[1]
    assert shown.endswith("\n(truncated)")
    kept = shown[: -len("\n(truncated)")]
    assert len(kept.encode()) == 65535 and kept == body[: len(kept)]
    assert inbound.inbound_prompt("s", "e", "", "x" * 65536, "utf8").endswith("Body:\n" + "x" * 65536)


def test_agent_handler_binary_body_is_not_inlined():
    prompt = inbound.inbound_prompt("s", "e", "", base64.b64encode(b"\x00\x01\xff").decode(), "base64")
    assert prompt.endswith("Body:\n(binary body, 3 bytes, not shown)")


def test_agent_handler_base64_utf8_body_is_shown():
    gw = _run(_gateway(), _data(body=base64.b64encode("café ✓".encode()).decode(), encoding="base64"))
    [(_, prompt)] = gw.sessions.turns
    assert prompt.endswith("Body:\ncafé ✓")
    big = base64.b64encode(b"x" * 70000).decode()
    assert inbound.inbound_prompt("s", "e", "", big, "base64").endswith("x" * 65536 + "\n(truncated)")


def _run(gw, data, *, inkbox=None, setup=None):
    async def scenario():
        if inkbox is not None:
            gw._inkbox = inkbox
        if setup:
            setup(gw)
        gw._inbound.start()
        await _receive(gw, data)
        await _settled(gw)
        await gw._inbound.close()

    asyncio.run(scenario())
    return gw


def test_script_empty_stdout_is_done_and_reads_stdin_in_home(isolated_home):
    gw = _run(_gateway(), _data({"type": "script", "command": "cat > seen.json; pwd > cwd.txt"}))
    assert gw.sessions.turns == [] and gw._inkbox.inbound.reports == []
    stdin = json.loads((isolated_home / "seen.json").read_text())
    assert stdin == {"source": "payments", "event_id": "inbev_1", "headers": {"content-type": "application/json"},
                     "body": '{"ok":true}', "body_encoding": "utf8"}
    assert (isolated_home / "cwd.txt").read_text().strip() == str(isolated_home)


def test_script_stdout_becomes_agent_message():
    gw = _run(_gateway(), _data({"type": "script", "command": "echo charge succeeded"}))
    [(chat_id, prompt)] = gw.sessions.turns
    assert chat_id == "inbound:payments" and "charge succeeded" in prompt and '{"ok":true}' not in prompt


def test_script_output_is_drained_and_bounded():
    big_body = "y" * (1536 * 1024)
    # Ignores stdin and floods both pipes well past their buffers.
    command = "head -c 300000 /dev/zero | tr '\\0' o; head -c 200000 /dev/zero | tr '\\0' e >&2; echo END >&2"
    gw = _gateway()

    async def scenario():
        gw._inbound.start()
        await _receive(gw, _data({"type": "script", "command": command}, body=big_body))
        await _settled(gw)
        await gw._inbound.close()

    asyncio.run(scenario())
    [(_, prompt)] = gw.sessions.turns
    assert prompt.count("o") >= inbound.MAX_STDOUT_BYTES and "o" * (inbound.MAX_STDOUT_BYTES + 1) not in prompt
    code, out, err = asyncio.run(inbound.run_script(command, big_body.encode()))
    assert code == 0 and out == "o" * inbound.MAX_STDOUT_BYTES + "\n(truncated)"
    assert len(err) == inbound.STDERR_TAIL_BYTES and err.endswith("END\n")


def test_script_failure_retries_then_reports_with_delivery_id(isolated_home, instant_waits):
    gw = _run(_gateway(), _data({"type": "script", "command": "echo run >> runs; echo boom >&2; exit 3"}))
    assert instant_waits == [10, 30, 120, 600]
    assert (isolated_home / "runs").read_text().count("run") == 5
    assert gw._inkbox.inbound.reports == [
        ("agent", "inbsrc_1", "inbev_1", "failed", "boom", "5f0c6c1e-0000-4000-8000-000000000001")]
    assert gw.sessions.turns == []


def test_failed_report_is_retried_without_rerunning_script(isolated_home, instant_waits, monkeypatch):
    monkeypatch.setattr(inbound, "SCRIPT_RETRY_DELAYS", ())
    inkbox = types.SimpleNamespace(inbound=_Inbound([ConnectionError("offline"), TimeoutError()]))
    gw = _run(_gateway(), _data({"type": "script", "command": "echo run >> runs; exit 1"}), inkbox=inkbox)
    assert (isolated_home / "runs").read_text().count("run") == 1
    assert len(inkbox.inbound.reports) == 3 and instant_waits == [30, 60]


def test_pending_report_resumes_after_restart_without_rerunning_script(isolated_home):
    delivery = _data()["delivery_id"]
    gw = _gateway()
    gw._inbound.receive(_data({"type": "script", "command": "echo run >> runs; exit 1"}))
    gw._inbound.queue.update(delivery, stage="report", due=time.time() + 3600, error="exit code 1")
    gw._inbound.queue.close()
    restarted = _gateway()

    async def scenario():
        restarted._inbound.start()
        await _settled(restarted)
        await restarted._inbound.close()

    asyncio.run(scenario())
    assert not (isolated_home / "runs").exists()
    assert restarted._inkbox.inbound.reports == [("agent", "inbsrc_1", "inbev_1", "failed", "exit code 1", delivery)]


def test_pending_report_is_not_activity(isolated_home):
    gw = _gateway()
    gw._inbound.receive(_data({"type": "script", "command": "exit 1"}))
    assert gw.inbound_activity() == 1
    gw._inbound.queue.update(_data()["delivery_id"], stage="report", due=time.time(), error="exit code 1")
    assert gw.inbound_activity() == 0 and gw._inbound.queue.pending()
    gw._inbound.queue.close()


def test_stale_delivery_report_counts_as_done(instant_waits, monkeypatch):
    monkeypatch.setattr(inbound, "SCRIPT_RETRY_DELAYS", ())
    inkbox = types.SimpleNamespace(inbound=_Inbound([StaleDelivery()]))
    _run(_gateway(), _data({"type": "script", "command": "exit 1"}), inkbox=inkbox)
    assert len(inkbox.inbound.reports) == 1 and instant_waits == []


def test_agent_handler_failure_is_reported():
    gw = _gateway()
    gw.sessions.fail = True
    _run(gw, _data())
    [report] = gw._inkbox.inbound.reports
    assert report[3:] == ("failed", "agent turn failed: RuntimeError", _data()["delivery_id"])


def test_stuck_agent_turn_is_reported_after_the_bound(monkeypatch):
    monkeypatch.setattr(inbound, "TURN_TIMEOUT_SECONDS", 0.05)
    gw = _gateway()
    gw.sessions.block = asyncio.Event()
    _run(gw, _data())
    [report] = gw._inkbox.inbound.reports
    assert report[3:] == ("failed", "agent turn failed: TimeoutError", _data()["delivery_id"])


def test_interrupted_agent_turn_is_reported_not_repeated():
    async def first():
        gw = _gateway()
        gw.sessions.block = asyncio.Event()
        gw._inbound.start()
        await _receive(gw, _data())
        async with asyncio.timeout(2):
            while not gw.sessions.turns:
                await asyncio.sleep(0.01)
        await gw._inbound.close()

    asyncio.run(first())
    gw = _gateway()
    _run(gw, _data())
    assert gw.sessions.turns == []
    [report] = gw._inkbox.inbound.reports
    assert report[4] == "agent turn was interrupted by a restart"


def test_script_timeout_counts_as_failure(monkeypatch, instant_waits):
    monkeypatch.setattr(inbound, "SCRIPT_RETRY_DELAYS", ())
    monkeypatch.setattr(inbound.run_script, "__defaults__", (0.2,))
    gw = _run(_gateway(), _data({"type": "script", "command": "sleep 5"}))
    assert gw._inkbox.inbound.reports[0][3:5] == ("failed", "timed out after 0.2s")


def test_report_keeps_retrying_past_a_day_with_capped_backoff(monkeypatch, instant_waits):
    monkeypatch.setattr(inbound, "SCRIPT_RETRY_DELAYS", ())
    inkbox = types.SimpleNamespace(inbound=_Inbound([ConnectionError("offline")] * 32))
    _run(_gateway(), _data({"type": "script", "command": "exit 1"}), inkbox=inkbox)
    assert len(inkbox.inbound.reports) == 33
    assert max(instant_waits) == 3600 and sum(instant_waits) > 86400


def test_interrupted_last_script_run_is_not_repeated(isolated_home):
    delivery = _data()["delivery_id"]
    gw = _gateway()
    gw._inbound.receive(_data({"type": "script", "command": "echo run >> runs"}))
    # Five runs counted; the fifth was cut off by a restart.
    gw._inbound.queue.update(delivery, attempts=5, error="boom")
    gw._inbound.queue.close()
    restarted = _gateway()

    async def scenario():
        restarted._inbound.start()
        await _settled(restarted)
        await restarted._inbound.close()

    asyncio.run(scenario())
    assert not (isolated_home / "runs").exists()
    assert [r[4] for r in restarted._inkbox.inbound.reports] == ["boom"]



@pytest.mark.parametrize(("status", "done"), [(409, True), (400, True), (404, True), (408, False),
                                               (429, False), (500, False), (503, False), (None, False)])
def test_report_response_handling(status, done):
    exc = ConnectionError("offline") if status is None else type("ApiError", (Exception,), {"status_code": status})()
    assert inbound._report_done(exc) is done
