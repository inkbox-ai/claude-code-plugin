"""Progress uses its own source-bound message, never final-answer delivery."""

import asyncio
import json
import threading
from types import SimpleNamespace as NS
from unittest.mock import Mock
from uuid import UUID

import pytest

from inkbox_claude.slack_activity import SlackActivity
from inkbox_claude.slack_progress import SlackProgress
from tests.test_slack_activity import route, dm


def resource():
    return NS(send_message=Mock(return_value=NS(status="sent", message_ts="1234567890.000010")),
              update_message=Mock(return_value=NS(status="succeeded")),
              get_action_by_key=Mock(return_value=NS(status="sent", message_ts="1234567890.000010")),
              get_operation=Mock(return_value=NS(status="succeeded")))


@pytest.mark.parametrize("meta", [route(), dm()])
@pytest.mark.parametrize("outcome,text", [("completed", "Completed."), ("cancelled", "Stopped."),
                                          ("failed", "Could not complete.")])
def test_one_message_coalesces_updates_and_finishes_on_original_route(tmp_path, meta, outcome, text):
    async def scenario():
        sdk = resource()
        tracker = SlackProgress(sdk, tmp_path / "progress.json", interval=0)
        await tracker.notify("chat", meta, "accepted")
        handle = await tracker.progress("chat", meta, "Reading documentation…")
        await tracker.flush()
        for phrase in ("Searching…", "Checking…", "Testing…"):
            assert await tracker.progress("chat", meta, phrase) == handle
        await tracker.flush()
        sdk.send_message.assert_called_once()
        assert sdk.send_message.call_args.kwargs["thread_ts"] == meta["thread_ts"]
        assert sdk.send_message.call_args.args == (meta["connection_id"],)
        assert sdk.send_message.call_args.kwargs["conversation_id"] == meta["conversation_id"]
        assert [call.args[3] for call in sdk.update_message.call_args_list] == ["Testing…"]
        await tracker.notify("chat", meta, outcome)
        await tracker.flush()
        assert sdk.update_message.call_args.args == (meta["connection_id"], meta["conversation_id"],
                                                     "1234567890.000010", text)
        assert json.loads(tracker.path.read_text()) == {}
        assert await tracker.progress("chat", meta, "Late callback", handle) is None
    asyncio.run(scenario())


def test_no_progress_message_for_quiet_or_already_finished_turn(tmp_path):
    async def scenario():
        sdk = resource()
        tracker = SlackProgress(sdk, tmp_path / "p.json", interval=0)
        assert await tracker.progress("chat", route(), "No admission") is None
        await tracker.notify("chat", route(), "accepted")
        await tracker.progress("chat", route(), "Short operation")
        await tracker.notify("chat", route(), "completed")
        await tracker.flush()
        sdk.send_message.assert_not_called()
        sdk.update_message.assert_not_called()
    asyncio.run(scenario())


def test_old_handles_other_chats_and_changed_routes_cannot_update_new_turn(tmp_path):
    async def scenario():
        sdk = resource()
        tracker = SlackProgress(sdk, tmp_path / "p.json", interval=0)
        await tracker.notify("chat", route(), "accepted")
        handle = await tracker.progress("chat", route(), "Working")
        await tracker.flush()
        for meta in ({**route(), "connection_id": "other"}, {**route(), "thread_ts": None},
                     {**route(), "source_event_id": "other"}):
            assert await tracker.progress("chat", meta, "Wrong source", handle) is None
        assert await tracker.progress("other", {}, "Wrong chat", handle) is None
        await tracker.notify("chat", route(), "cancelled")
        await tracker.notify("chat", route("event-2"), "accepted")
        assert await tracker.progress("chat", {}, "Old edit", handle) is None
        assert await tracker.progress("chat", route(), "Old callback") is None
        await tracker.flush()
        assert sdk.send_message.call_count == 1
    asyncio.run(scenario())


def test_stop_during_create_orders_terminal_edit_after_the_send(tmp_path):
    async def scenario():
        sdk = resource()
        started, release = threading.Event(), threading.Event()
        def send(*args, **kwargs):
            started.set()
            assert release.wait(5)
            return NS(status="sent", message_ts="1234567890.000010")
        sdk.send_message.side_effect = send
        tracker = SlackProgress(sdk, tmp_path / "p.json", interval=0)
        await tracker.notify("chat", route(), "accepted")
        await tracker.progress("chat", route(), "Working")
        assert await asyncio.to_thread(started.wait, 5)
        try:
            await tracker.notify("chat", route(), "cancelled")
            sdk.update_message.assert_not_called()
        finally:
            release.set()
        await tracker.flush()
        assert sdk.update_message.call_args.args[3] == "Stopped."
    asyncio.run(scenario())


@pytest.mark.parametrize("status", ["unknown", "sending", "exception"])
def test_uncertain_creation_never_resends_and_restart_looks_up_original_key(tmp_path, status):
    async def scenario():
        sdk = resource()
        if status == "exception":
            sdk.send_message.side_effect = TimeoutError()
        else:
            sdk.send_message.return_value = NS(status=status, message_ts=None)
        path = tmp_path / "p.json"
        tracker = SlackProgress(sdk, path, interval=0)
        await tracker.notify("chat", route(), "accepted")
        await tracker.progress("chat", route(), "Working")
        await tracker.flush()
        await tracker.progress("chat", route(), "Another update")
        await tracker.notify("chat", route(), "cancelled")
        await tracker.flush()
        sdk.send_message.assert_called_once()
        if status == "unknown":
            sdk.update_message.assert_not_called()
            sdk.get_action_by_key.assert_not_called()
            await SlackProgress(sdk, path, interval=0).recover()
            sdk.get_action_by_key.assert_not_called()
            return
        assert sdk.update_message.call_args.args[3] == "Stopped."
        restarted = SlackProgress(sdk, path, interval=0)
        await restarted.recover()
        await restarted.flush()
        assert sdk.get_action_by_key.call_args.args[1] == sdk.send_message.call_args.kwargs["idempotency_key"]
        sdk.send_message.assert_called_once()
        assert sdk.update_message.call_args.args[3] == "Stopped."
        assert json.loads(path.read_text()) == {}
    asyncio.run(scenario())


def test_uncertain_edit_defers_newer_edits_until_reconciliation(tmp_path):
    async def scenario():
        sdk = resource()
        path = tmp_path / "p.json"
        tracker = SlackProgress(sdk, path, interval=0)
        await tracker.notify("chat", route(), "accepted")
        await tracker.progress("chat", route(), "Working")
        await tracker.flush()
        operation_id = UUID("00000000-0000-4000-8000-000000000001")
        sdk.update_message.return_value = NS(status="in_progress", id=operation_id)
        sdk.get_operation.return_value = NS(status="in_progress")
        await tracker.progress("chat", route(), "Testing")
        await tracker.flush()
        await tracker.notify("chat", route(), "completed")
        await tracker.flush()
        sdk.update_message.assert_called_once()
        sdk.get_operation.return_value = NS(status="succeeded")
        sdk.update_message.return_value = NS(status="succeeded")
        restarted = SlackProgress(sdk, path, interval=0)
        await restarted.recover()
        await restarted.flush()
        assert sdk.get_operation.call_args.args[1] == str(operation_id)
        assert sdk.update_message.call_count == 2
    asyncio.run(scenario())


@pytest.mark.parametrize("outcome", ["succeeded", "failed", "unknown", "in_progress", "missing", "old_sdk"])
def test_timed_out_edit_uses_original_key_or_remains_fenced(tmp_path, outcome):
    class LookupResource:
        def get_operation_by_key(self, connection_id, *, idempotency_key):
            lookups.append((connection_id, idempotency_key))
            if outcome == "missing":
                raise LookupError("not recorded")
            return NS(id="00000000-0000-4000-8000-000000000010", operation="message_update",
                      status=outcome, connection_id=connection_id, conversation_id="C123",
                      message_ts="1234567890.000010")
    async def scenario():
        sdk = resource()
        if outcome != "old_sdk":
            upgraded = LookupResource()
            upgraded.__dict__.update(vars(sdk))
            sdk = upgraded
        tracker = SlackProgress(sdk, tmp_path / "p.json", interval=0)
        await tracker.notify("chat", route(), "accepted")
        await tracker.progress("chat", route(), "Working")
        await tracker.flush()
        sdk.update_message.side_effect = TimeoutError()
        await tracker.progress("chat", route(), "Checking")
        await tracker.flush()
        pending_key = sdk.update_message.call_args.kwargs["idempotency_key"]
        sdk.update_message.side_effect = None
        restarted = SlackProgress(sdk, tracker.path, interval=0)
        await restarted.recover()
        await restarted.flush()
        count = 3 if outcome == "in_progress" else 1
        assert lookups == ([] if outcome == "old_sdk" else [(route()["connection_id"], pending_key)] * count)
        sdk.send_message.assert_called_once()
        if outcome in {"succeeded", "failed"}:
            assert sdk.update_message.call_count == 2
            assert sdk.update_message.call_args.args[3] == "Progress interrupted after reconnecting."
            assert json.loads(tracker.path.read_text()) == {}
        else:
            assert sdk.update_message.call_count == 1
            assert next(iter(json.loads(tracker.path.read_text()).values()))["uncertain"]
    lookups = []
    asyncio.run(scenario())


def test_definitive_edit_rejection_does_not_freeze_terminal_cleanup(tmp_path, monkeypatch):
    async def scenario():
        sdk = resource()
        tracker = SlackProgress(sdk, tmp_path / "p.json", interval=0)
        await tracker.notify("chat", route(), "accepted")
        await tracker.progress("chat", route(), "Working")
        await tracker.flush()
        sdk.update_message.return_value = NS(status="failed", error_code="rate_limited", retry_after=1)
        await tracker.progress("chat", route(), "Testing")
        await tracker.flush()
        sdk.update_message.assert_called_once()
        saved = next(iter(json.loads(tracker.path.read_text()).values()))
        assert not saved.get("uncertain")
        # Move past the provider's cooldown without making the test sleep.
        monkeypatch.setattr("inkbox_claude.slack_progress.time.time", lambda: saved["retry_at"] + 1)
        sdk.update_message.return_value = NS(status="succeeded")
        await tracker.notify("chat", route(), "completed")
        await tracker.flush()
        assert sdk.update_message.call_args.args[3] == "Completed."
        assert sdk.update_message.call_count == 2
        assert json.loads(tracker.path.read_text()) == {}
    asyncio.run(scenario())


def test_activity_lifecycle_and_progress_are_independent(tmp_path):
    async def scenario():
        sdk = resource()
        sdk.set_processing_status = Mock(return_value=NS(status="succeeded"))
        tracker = SlackActivity(sdk, tmp_path / "activity.json")
        tracker._progress.interval = 0
        await tracker.notify("chat", "slack", route(), "accepted")
        await tracker.progress("chat", route(), "Checking <@U123> & tests")
        await tracker.flush()
        assert "&lt;@U123&gt; &amp;" in sdk.send_message.call_args.kwargs["text"]
        await tracker.notify("chat", "slack", route(), "waiting")
        await tracker.flush()
        assert sdk.update_message.call_args.args[3] == "Waiting for your approval."
        await tracker.close()
        assert sdk.update_message.call_args.args[3] == "Progress paused while reconnecting."
        assert [call.args[3] for call in sdk.set_processing_status.call_args_list] == ["processing", "suspended", "active"]
        assert not tracker.has_chat("chat")
    asyncio.run(scenario())


def test_no_external_effect_without_durable_intent(tmp_path, monkeypatch):
    async def scenario():
        sdk = resource()
        tracker = SlackProgress(sdk, tmp_path / "p.json", interval=0)
        save = tracker._save
        def fail():
            raise OSError("unavailable")
        monkeypatch.setattr(tracker, "_save", fail)
        await tracker.notify("chat", route(), "accepted")
        await tracker.progress("chat", route(), "Working")
        await tracker.flush()
        sdk.send_message.assert_not_called()
        assert not next(iter(tracker.records.values())).get("uncertain")
        monkeypatch.setattr(tracker, "_save", save)
        await tracker.progress("chat", route(), "Checking again")
        await tracker.flush()
        sdk.send_message.assert_called_once()
    asyncio.run(scenario())


def test_definitively_failed_creation_retires_without_uncertainty_or_retry(tmp_path):
    async def run():
        sdk = resource()
        sdk.send_message.return_value = NS(status="failed", error_code="rate_limited")
        tracker = SlackProgress(sdk, tmp_path / "p.json", interval=0)
        await tracker.notify("chat", route(), "accepted")
        await tracker.progress("chat", route(), "Working")
        await tracker.flush()
        await tracker.progress("chat", route(), "Later")
        await tracker.notify("chat", route(), "completed")
        await tracker.flush()
        await SlackProgress(sdk, tracker.path, interval=0).recover()
        assert json.loads(tracker.path.read_text()) == {}
        sdk.send_message.assert_called_once()
        sdk.get_action_by_key.assert_not_called()
        sdk.update_message.assert_not_called()
    asyncio.run(run())


@pytest.mark.parametrize("complete", [True, False])
def test_terminal_sending_reconciles_with_bounded_read_only_lookup(tmp_path, complete):
    async def run():
        sdk = resource()
        sdk.send_message.return_value = NS(status="sending", message_ts=None)
        sdk.get_action_by_key.side_effect = ([NS(status="sending"), NS(status="sent", message_ts="1234567890.000010")]
            if complete else [NS(status="sending")] * 3)
        tracker = SlackProgress(sdk, tmp_path / "p.json", interval=0)
        await tracker.notify("chat", route(), "accepted")
        await tracker.progress("chat", route(), "Working")
        await tracker.flush()
        await tracker.notify("chat", route(), "completed")
        await tracker.flush()
        sdk.send_message.assert_called_once()
        assert sdk.get_action_by_key.call_count == (2 if complete else 3)
        assert all(call.args[1] == sdk.send_message.call_args.kwargs["idempotency_key"]
                   for call in sdk.get_action_by_key.call_args_list)
        if complete:
            sdk.update_message.assert_called_once()
            assert sdk.update_message.call_args.args[3] == "Completed."
            assert json.loads(tracker.path.read_text()) == {}
        else:
            sdk.update_message.assert_not_called()
            assert next(iter(json.loads(tracker.path.read_text()).values()))["uncertain"]
    asyncio.run(run())


def test_recovery_budget_preserves_unvisited_receipts_without_blocking_startup(tmp_path, monkeypatch):
    from inkbox_claude import slack_progress
    async def run():
        sdk = resource()
        sdk.send_message.side_effect = TimeoutError()
        tracker = SlackProgress(sdk, tmp_path / "p.json", interval=0)
        for event in ("first", "second"):
            await tracker.notify("chat", route(event), "accepted")
            await tracker.progress("chat", route(event), "Working")
            await tracker.flush()
        release = threading.Event()
        def validate(_):
            assert release.wait(5)
        monkeypatch.setattr(slack_progress, "RECOVERY_TIMEOUT_SECONDS", 0.01)
        recovered = SlackProgress(sdk, tracker.path, interval=0, validate_route=validate)
        try:
            await asyncio.wait_for(recovered.recover(), 1)
            assert len(recovered.records) == len(json.loads(tracker.path.read_text())) == 2
            sdk.get_action_by_key.assert_not_called()
            sdk.update_message.assert_not_called()
        finally:
            release.set()
    asyncio.run(run())


@pytest.mark.parametrize("meta", [route(), dm()])
def test_progress_uses_published_sdk_message_edit_wire(tmp_path, meta):
    import httpx
    from inkbox import Inkbox

    requests = []
    message_ts = "1234567890.000010"
    def handle(request):
        requests.append(request)
        payload = {"id": "00000000-0000-4000-8000-000000000003",
                   "connection_id": meta["connection_id"], "conversation_id": meta["conversation_id"],
                   "message_ts": message_ts}
        if request.method == "POST":
            payload.update(status="sent", thread_ts=meta["thread_ts"])
        else:
            payload.update(status="succeeded", operation="message_update")
        return httpx.Response(200, json=payload)

    client = Inkbox(api_key="synthetic-test-key", base_url="https://api.example")
    client._api_http._client.close()
    client._api_http._client = httpx.Client(base_url="https://api.example/api/v1",
                                           transport=httpx.MockTransport(handle))
    async def scenario():
        tracker = SlackProgress(client.slack, tmp_path / "p.json", interval=0)
        await tracker.notify("chat", meta, "accepted")
        await tracker.progress("chat", meta, "Checking information")
        await tracker.flush()
        await tracker.progress("chat", meta, "Running checks")
        await tracker.flush()
        await tracker.notify("chat", meta, "completed")
        await tracker.flush()
    try:
        asyncio.run(scenario())
        assert [request.method for request in requests] == ["POST", "PATCH", "PATCH"]
        created = json.loads(requests[0].content)
        assert created == {"conversation_id": meta["conversation_id"], "text": "Checking information",
                           **({"thread_ts": meta["thread_ts"]} if meta["thread_ts"] else {})}
        assert all(request.url.path.endswith(f"/messages/{message_ts}") for request in requests[1:])
        assert json.loads(requests[-1].content) == {"text": "Completed."}
        assert len({request.headers["Idempotency-Key"] for request in requests}) == 3
    finally:
        client.close()


@pytest.mark.parametrize("reconciled", [False, True])
def test_restart_preserves_confirmed_terminal_message(tmp_path, reconciled):
    async def scenario():
        sdk = resource()
        tracker = SlackProgress(sdk, tmp_path / "p.json", interval=0)
        key = tracker._key("chat", route())
        tracker.records[key] = dict(chat_id="chat", route=route(), revision=2,
            transport="message", started=True, terminal=True, desired="Completed.",
            applied="Working", message_ts="1234567890.000010", uncertain=True,
            pending_kind="message_update", pending_text="Completed.",
            operation_id="00000000-0000-4000-8000-000000000001")
        if reconciled:
            tracker.records[key].update(applied="Completed.", uncertain=False)
        tracker._save()
        recovered = SlackProgress(sdk, tracker.path, interval=0)
        await recovered.recover()
        await recovered.flush()
        assert sdk.get_operation.call_count == (0 if reconciled else 1)
        sdk.send_message.assert_not_called()
        sdk.update_message.assert_not_called()
        assert json.loads(tracker.path.read_text()) == {}
    asyncio.run(scenario())


@pytest.mark.parametrize("fallback", [False, True])
def test_restart_retires_never_started_intent_before_network(tmp_path, fallback):
    async def scenario():
        sdk = resource()
        validate = Mock(side_effect=AssertionError("Never-started intent needs no network"))
        tracker = SlackProgress(sdk, tmp_path / "p.json", interval=0)
        key = tracker._key("chat", route())
        tracker.records[key] = dict(chat_id="chat", route=route(), revision=0, desired="Working")
        if fallback:
            tracker.records[key].update(transport="message", started=False,
                                       pending_kind="stream_start", revision=1)
        tracker._save()
        recovered = SlackProgress(sdk, tracker.path, interval=0, validate_route=validate)
        await recovered.recover()
        await recovered.flush()
        validate.assert_not_called()
        sdk.get_action_by_key.assert_not_called()
        sdk.send_message.assert_not_called()
        sdk.update_message.assert_not_called()
        assert json.loads(tracker.path.read_text()) == {}
    asyncio.run(scenario())


def test_restart_can_reconcile_a_new_terminal_timeout(tmp_path):
    class LookupResource:
        def get_operation_by_key(self, connection_id, *, idempotency_key):
            lookups.append(idempotency_key)
            return NS(id="00000000-0000-4000-8000-000000000001", operation="message_update",
                      status="succeeded", connection_id=connection_id, conversation_id="C123",
                      message_ts="1234567890.000010")
    async def scenario():
        sdk = LookupResource()
        sdk.__dict__.update(vars(resource()))
        sdk.update_message.side_effect = TimeoutError()
        tracker = SlackProgress(sdk, tmp_path / "p.json", interval=0)
        key = tracker._key("chat", route())
        tracker.records[key] = dict(chat_id="chat", route=route(), revision=2,
            transport="message", started=True, terminal=True, terminal_reconciled=True,
            desired="Completed.", applied="Working", message_ts="1234567890.000010")
        tracker._save()
        recovered = SlackProgress(sdk, tracker.path, interval=0)
        await recovered.recover()
        await recovered.flush()
        sdk.update_message.assert_called_once()
        assert sdk.update_message.call_args.args[3] == "Progress interrupted after reconnecting."
        assert lookups == [sdk.update_message.call_args.kwargs["idempotency_key"]]
        assert json.loads(tracker.path.read_text()) == {}
    lookups = []
    asyncio.run(scenario())


def test_lost_create_uses_published_sdk_by_key_wire_without_replay(tmp_path):
    import httpx
    from inkbox import Inkbox

    requests = []
    meta = route()
    def handle(request):
        requests.append(request)
        if request.method == "POST":
            raise httpx.ReadTimeout("Lost create response", request=request)
        payload = {"id": "00000000-0000-4000-8000-000000000003",
                   "connection_id": meta["connection_id"], "conversation_id": meta["conversation_id"],
                   "message_ts": "1234567890.000010"}
        if request.method == "GET":
            payload.update(status="sent", thread_ts=meta["thread_ts"])
        else:
            payload.update(status="succeeded", operation="message_update")
        return httpx.Response(200, json=payload)

    client = Inkbox(api_key="synthetic-test-key", base_url="https://api.example")
    client._api_http._client.close()
    client._api_http._client = httpx.Client(base_url="https://api.example/api/v1",
                                           transport=httpx.MockTransport(handle))
    async def scenario():
        tracker = SlackProgress(client.slack, tmp_path / "p.json", interval=0)
        await tracker.notify("chat", meta, "accepted")
        await tracker.progress("chat", meta, "Checking information")
        await tracker.flush()
        await tracker.notify("chat", meta, "completed")
        await tracker.flush()
        assert json.loads(tracker.path.read_text()) == {}
    try:
        asyncio.run(scenario())
        assert [request.method for request in requests] == ["POST", "GET", "PATCH"]
        assert requests[1].url.path.endswith("/actions/by-key")
        assert requests[1].headers["Idempotency-Key"] == requests[0].headers["Idempotency-Key"]
        assert json.loads(requests[-1].content) == {"text": "Completed."}
    finally:
        client.close()


@pytest.mark.parametrize("error,retries", [("message_not_found", 0), ("rate_limited", 3),
                                          ("connection_failed", 3)])
def test_definitive_terminal_edit_failure_retires_after_retry_budget(tmp_path, error, retries):
    async def scenario():
        sdk = resource()
        tracker = SlackProgress(sdk, tmp_path / "p.json", interval=0)
        await tracker.notify("chat", route(), "accepted")
        await tracker.progress("chat", route(), "Working")
        await tracker.flush()
        next(iter(tracker.records.values()))["terminal_retries"] = retries
        sdk.update_message.return_value = NS(status="failed", error_code=error)
        await tracker.notify("chat", route(), "completed")
        await tracker.flush()
        assert tracker.records == {}
        assert json.loads(tracker.path.read_text()) == {}
        restarted = SlackProgress(sdk, tracker.path, interval=0)
        await restarted.recover()
        await restarted.flush()
        sdk.send_message.assert_called_once()
        sdk.update_message.assert_called_once()
        sdk.get_operation.assert_not_called()
        sdk.get_action_by_key.assert_not_called()
    asyncio.run(scenario())
