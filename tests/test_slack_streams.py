"""Typed SDK dispatch preserves the same uncertainty and source boundaries."""

import asyncio
import json
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest

from inkbox_claude.slack_progress import SlackProgress
from inkbox_claude.slack_streams import SlackTaskStreams
from tests.test_slack_activity import route

STREAM_ID = "00000000-0000-4000-8000-000000000010"
OP_ID = "00000000-0000-4000-8000-000000000011"


def source():
    return {**route(), "workspace_id": "T123", "actor_id": "U123", "recipient_team_id": "T123"}


def operation(kind="stream_start", status="succeeded", **overrides):
    return {"id": STREAM_ID if kind == "stream_start" else OP_ID, "operation": kind,
            "connection_id": source()["connection_id"], "conversation_id": "C123", "status": status,
            "message_ts": "1234567890.000010" if status == "succeeded" else None, **overrides}


class TypedResource:
    def __init__(self, result=None):
        self.calls = []
        self.result = result
        self.send_message = Mock()
        self.update_message = Mock()

    @property
    def _http(self):
        raise AssertionError("The typed integration must not access private transport")

    def capabilities(self, connection_id):
        return NS(capabilities={"task_streaming": NS(scopes_satisfied=True)})

    def _write(self, kind, args, kwargs):
        self.calls.append((kind, args, kwargs))
        if isinstance(self.result, Exception):
            raise self.result
        return NS(**(self.result or operation(kind)))

    def start_stream(self, *args, **kwargs):
        return self._write("stream_start", args, kwargs)

    def append_stream(self, *args, **kwargs):
        return self._write("stream_append", args, kwargs)

    def stop_stream(self, *args, **kwargs):
        return self._write("stream_stop", args, kwargs)

    def get_operation_by_key(self, *args, **kwargs):
        self.calls.append(("lookup", args, kwargs))
        return NS(**operation())


def test_typed_lifecycle_uses_public_methods_and_original_source(tmp_path):
    async def run():
        sdk = TypedResource()
        tracker = SlackProgress(sdk, tmp_path / "progress.json", interval=0)
        await tracker.notify("chat", source(), "accepted")
        await tracker.progress("chat", source(), "Reading information")
        await tracker.flush()
        await tracker.progress("chat", source(), "Checking results")
        await tracker.flush()
        await tracker.notify("chat", source(), "completed")
        await tracker.flush()
        assert [call[0] for call in sdk.calls] == ["stream_start", "stream_append", "stream_stop"]
        initial = sdk.calls[0]
        assert initial[1] == (source()["connection_id"], "C123")
        assert {key: initial[2][key] for key in ("thread_ts", "recipient_user_id", "recipient_team_id", "task_display_mode")} == {
            "thread_ts": source()["thread_ts"], "recipient_user_id": "U123",
            "recipient_team_id": "T123", "task_display_mode": "timeline",
        }
        assert [call[1] for call in sdk.calls[1:]] == [(*initial[1], STREAM_ID)] * 2
        assert len({call[2]["idempotency_key"] for call in sdk.calls}) == 3
        assert sdk.calls[-1][2]["chunks"][0]["status"] == "complete"
        sdk.send_message.assert_not_called()
        sdk.update_message.assert_not_called()
    asyncio.run(run())


@pytest.mark.parametrize("failure", ["timeout", "wrong_route", "unknown", "in_progress"])
def test_unconfirmed_typed_call_never_replays_via_legacy_transport(tmp_path, failure):
    async def run():
        result = TimeoutError() if failure == "timeout" else operation(
            status=failure if failure in {"unknown", "in_progress"} else "succeeded",
            **({"conversation_id": "COTHER"} if failure == "wrong_route" else {}),
        )
        sdk = TypedResource(result)
        tracker = SlackProgress(sdk, tmp_path / "progress.json", interval=0)
        await tracker.notify("chat", source(), "accepted")
        await tracker.progress("chat", source(), "Working")
        await tracker.flush()
        assert len(sdk.calls) == 1
        key = sdk.calls[0][2]["idempotency_key"]
        sdk.result = None
        await tracker.notify("chat", source(), "cancelled")
        await tracker.flush()
        recovered = SlackProgress(sdk, tracker.path, interval=0)
        await recovered.recover()
        await recovered.flush()
        if failure == "unknown":
            assert [call[0] for call in sdk.calls] == ["stream_start"]
            sdk.send_message.assert_not_called()
            return
        assert [call[0] for call in sdk.calls] == ["stream_start", "lookup", "stream_stop"]
        assert sdk.calls[1][1:] == ((source()["connection_id"],), {"idempotency_key": key})
        assert sdk.calls[-1][1][-1] == STREAM_ID
        sdk.send_message.assert_not_called()
    asyncio.run(run())


def test_partial_typed_surface_falls_back_before_any_write():
    class PartialResource:
        def start_stream(self, *args, **kwargs):
            raise AssertionError("Partial SDK surface must not receive stream writes")
        @property
        def _http(self):
            raise AssertionError("Private transport must never be accessed")
    assert not SlackTaskStreams(PartialResource()).capable(source())


@pytest.mark.parametrize("code", ["missing_scope", "app_not_eligible", "method_not_supported_for_channel_type"])
def test_definitive_unsupported_start_falls_back_once(tmp_path, code):
    async def run():
        sdk = TypedResource(operation(status="failed", error_code=code))
        sdk.send_message.return_value = NS(status="sent", message_ts="1234567890.000010")
        sdk.update_message.return_value = NS(status="succeeded")
        tracker = SlackProgress(sdk, tmp_path / "p.json", interval=0)
        await tracker.notify("chat", source(), "accepted")
        await tracker.progress("chat", source(), "Working")
        await tracker.flush()
        await tracker.notify("chat", source(), "completed")
        await tracker.flush()
        assert [call[0] for call in sdk.calls] == ["stream_start"]
        sdk.send_message.assert_called_once()
        assert sdk.update_message.call_args.args[3] == "Completed."
    asyncio.run(run())


@pytest.mark.parametrize("kind", ["stream_append", "stream_stop"])
def test_unknown_append_or_stop_blocks_all_further_writes_and_fallback(tmp_path, kind):
    async def run():
        sdk = TypedResource()
        tracker = SlackProgress(sdk, tmp_path / "p.json", interval=0)
        await tracker.notify("chat", source(), "accepted")
        await tracker.progress("chat", source(), "Working")
        await tracker.flush()
        sdk.result = operation(kind, status="unknown", error_code="connection_failed")
        if kind == "stream_append":
            await tracker.progress("chat", source(), "Checking")
        else:
            await tracker.notify("chat", source(), "cancelled")
        await tracker.flush()
        await tracker.notify("chat", source(), "completed")
        await tracker.flush()
        assert [call[0] for call in sdk.calls] == ["stream_start", kind]
        sdk.send_message.assert_not_called()
        sdk.update_message.assert_not_called()
        saved = next(iter(json.loads(tracker.path.read_text()).values()))
        assert saved["uncertain"] and saved["pending_kind"] == kind
    asyncio.run(run())


@pytest.mark.parametrize("error", ["connection_failed", "rate_limited"])
def test_definitive_terminal_rejection_has_bounded_retries(tmp_path, monkeypatch, error):
    from inkbox_claude import slack_progress
    clock = iter(range(10000))
    monkeypatch.setattr(slack_progress.time, "time", lambda: next(clock))
    async def run():
        sdk = TypedResource()
        tracker = SlackProgress(sdk, tmp_path / "p.json", interval=0)
        await tracker.notify("chat", source(), "accepted")
        await tracker.progress("chat", source(), "Working")
        await tracker.flush()
        sdk.result = operation("stream_stop", status="failed", error_code=error)
        await tracker.notify("chat", source(), "cancelled")
        await tracker.flush()
        stops = [call for call in sdk.calls if call[0] == "stream_stop"]
        assert len(stops) == 4
        assert len({call[2]["idempotency_key"] for call in stops}) == 4
        assert all(call[1][-1] == STREAM_ID for call in stops)
        sdk.send_message.assert_not_called()
    asyncio.run(run())


def test_inline_dm_or_absent_recipient_uses_original_ordinary_route(tmp_path):
    async def run():
        for field in ("thread_ts", "actor_id", "workspace_id", "recipient_team_id"):
            sdk = TypedResource()
            sdk.send_message.return_value = NS(status="sent", message_ts="1234567890.000010")
            meta = {**source(), field: None}
            tracker = SlackProgress(sdk, tmp_path / (field + ".json"), interval=0)
            await tracker.notify("chat", meta, "accepted")
            await tracker.progress("chat", meta, "Working")
            await tracker.flush()
            assert sdk.calls == []
            assert sdk.send_message.call_args.kwargs["thread_ts"] == meta["thread_ts"]
    asyncio.run(run())


@pytest.mark.parametrize("kind", ["stream_start", "stream_append", "stream_stop"])
def test_pending_native_operation_reconciles_before_terminal_without_replay(tmp_path, monkeypatch, kind):
    async def run():
        sdk = TypedResource()
        readings = [operation(kind, "in_progress"), operation(kind)]
        def lookup(self, *args, **kwargs):
            self.calls.append(("lookup", args, kwargs))
            return NS(**readings.pop(0))
        monkeypatch.setattr(TypedResource, "get_operation_by_key", lookup)
        tracker = SlackProgress(sdk, tmp_path / "p.json", interval=0)
        await tracker.notify("chat", source(), "accepted")
        if kind == "stream_start":
            sdk.result = operation(kind, "in_progress")
        await tracker.progress("chat", source(), "Working")
        await tracker.flush()
        if kind == "stream_append":
            sdk.result = operation(kind, "in_progress")
            await tracker.progress("chat", source(), "Checking")
            await tracker.flush()
        sdk.result = operation("stream_stop", "in_progress") if kind == "stream_stop" else None
        await tracker.notify("chat", source(), "completed")
        await tracker.flush()
        writes = [call for call in sdk.calls if call[0] != "lookup"]
        assert [call[0] for call in writes] == (["stream_start", "stream_append", "stream_stop"]
            if kind == "stream_append" else ["stream_start", "stream_stop"])
        lookups = [call for call in sdk.calls if call[0] == "lookup"]
        assert len(lookups) == 2
        original = next(call for call in writes if call[0] == kind)
        assert all(call[2]["idempotency_key"] == original[2]["idempotency_key"] for call in lookups)
        assert json.loads(tracker.path.read_text()) == {}
        sdk.send_message.assert_not_called()
    asyncio.run(run())


def test_shutdown_pauses_progress_without_claiming_confirmed_stop_or_success(tmp_path):
    async def run():
        sdk = TypedResource()
        tracker = SlackProgress(sdk, tmp_path / "p.json", interval=0)
        await tracker.notify("chat", source(), "accepted")
        await tracker.progress("chat", source(), "Working")
        await tracker.flush()
        await tracker.close()
        chunk = sdk.calls[-1][2]["chunks"][0]
        assert chunk == {"type": "task_update", "id": chunk["id"],
                         "title": "Progress paused while reconnecting.", "status": "error"}
    asyncio.run(run())


def test_stream_recipient_uses_verified_home_team_not_installation_workspace(tmp_path):
    async def run():
        sdk = TypedResource()
        meta = {**source(), "recipient_team_id": "TREMOTE"}
        tracker = SlackProgress(sdk, tmp_path / "p.json", interval=0)
        await tracker.notify("chat", meta, "accepted")
        await tracker.progress("chat", meta, "Working")
        await tracker.flush()
        assert sdk.calls[0][2]["recipient_team_id"] == "TREMOTE"
    asyncio.run(run())


def test_real_typed_sdk_dispatch_and_recovery_lookup():
    import httpx
    from inkbox import Inkbox

    client = Inkbox(api_key="synthetic-test-key", base_url="https://api.example")
    methods = ("start_stream", "append_stream", "stop_stream", "get_operation_by_key")
    if not all(callable(getattr(type(client.slack), name, None)) for name in methods):
        client.close()
        pytest.skip("Installed SDK predates typed task streams; ordinary progress is tested separately")
    requests = []
    def handle(request):
        requests.append(request)
        kind = "stream_stop" if request.url.path.endswith("/stop") else "stream_append" if request.url.path.endswith("/append") else "stream_start"
        return httpx.Response(200, json=operation(kind))
    client._api_http._client.close()
    client._api_http._client = httpx.Client(base_url="https://api.example/api/v1",
        headers={"X-API-Key": "synthetic-test-key"}, transport=httpx.MockTransport(handle))
    for name in methods:
        setattr(client.slack, name, Mock(wraps=getattr(client.slack, name)))
    try:
        streams = SlackTaskStreams(client.slack)
        assert streams.supported
        chunks = [{"type": "task_update", "id": "task", "title": "Reading", "status": "in_progress"}]
        started = streams.write(source(), kind="stream_start", key="start", chunks=chunks)
        streams.write(source(), kind="stream_append", key="append", chunks=chunks, stream_id=started.id)
        streams.write(source(), kind="stream_stop", key="stop", chunks=[], stream_id=started.id)
        assert streams.lookup(source(), kind="stream_start", key="start").id == started.id
        for name in methods:
            getattr(client.slack, name).assert_called_once()
        assert [r.headers["Idempotency-Key"] for r in requests] == ["start", "append", "stop", "start"]
        assert requests[-1].method == "GET" and requests[-1].url.path.endswith("/operations/by-key")
        assert json.loads(requests[-2].content) == {"chunks": []}
        assert all(r.headers["X-API-Key"] == "synthetic-test-key" for r in requests)
    finally:
        client.close()
