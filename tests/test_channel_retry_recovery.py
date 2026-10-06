"""Durable pre-effect retries, cancellation, and no replay after submission."""

import asyncio
import json
import threading
from functools import wraps
from types import SimpleNamespace as NS

import pytest

from inkbox_claude.config import BridgeConfig
from inkbox_claude.gateway import InkboxGateway
from inkbox_claude.imessage import source_metadata
from tests.test_imessage_sdk import sdk as sdk, pytestmark as pytestmark, SOURCE_ID, CONVERSATION_ID
from tests.test_native_channel_sessions import Client
from tests.test_sessions import make_session
from tests.test_slack import connected_client, event, inbound_message, IDENTITY


@pytest.fixture(autouse=True)
def fast_backoff(monkeypatch):
    monkeypatch.setattr("inkbox_claude.sessions.CHANNEL_RETRY_INITIAL_DELAY", .001)
    monkeypatch.setattr("inkbox_claude.sessions.CHANNEL_RETRY_MAX_DELAY", .002)


def harness(sdk, *, mode="imessage"):
    cfg = BridgeConfig(identity="agent", imessage_threaded_replies=True, slack_enabled=True)
    gw = InkboxGateway(cfg)
    gw._inkbox = sdk.client
    gw._identity = sdk.client.get_identity("agent")
    session = make_session([])
    session.cfg = cfg
    session.send_fn = gw.send_to_contact
    session.receipt_fn = gw._channel_receipt
    host = Client()
    session._client = host
    activity = []

    async def notify(_chat, _mode, meta, state):
        activity.append((meta.get("imessage_event_id") or meta.get("source_event_id"), state))

    session.activity_fn = notify
    gw.sessions = NS(get=lambda _key: session, sessions={session.chat_id: session})
    meta = {"conversation_id": CONVERSATION_ID, "sender": "+15555550123", "media": True,
            "imessage_threaded_replies": True, **source_metadata(sdk.source, "original")}
    if mode == "slack":
        gw._identity = NS(id=IDENTITY)
        gw._inkbox = connected_client()
        gw._inkbox.slack.send_message.return_value = NS(id="action", status="sent")
        _, _, meta = inbound_message(event(), IDENTITY)
    store = gw._channel_store(mode)
    assert store.admit(session.chat_id, "Original request", meta)
    return gw, session, host, store, meta, activity


def writes(sdk):
    return [request for request in sdk.requests if request.method == "POST"]


async def waiting(session):
    async def wait():
        while session._retrying_turn is None:
            await asyncio.sleep(0)
    await asyncio.wait_for(wait(), timeout=2)


def test_safe_start_survives_more_than_three_failures_without_new_input(sdk, monkeypatch):
    async def run():
        gw, session, host, store, meta, activity = harness(sdk)
        attempts = 0

        async def ensure():
            nonlocal attempts
            attempts += 1
            if attempts <= 4:
                assert store.summary()["pending"] == 1
                assert [state for _, state in activity] == ["accepted"]
                raise ConnectionError("native host unavailable")
            session._client = host
            return host

        monkeypatch.setattr(session, "_ensure_client", ensure)
        await session.handle_inbound("Original request", "imessage", meta)
        await asyncio.wait_for(session._worker, timeout=3)
        assert attempts == 5 and len(host.queries) == 1
        assert store.summary()["done"] == 1
        assert len(writes(sdk)) == 1
        assert [state for _, state in activity] == ["accepted", "completed"]
        await session.stop_companion()
        store.close()

    asyncio.run(run())


def test_accepted_reply_stays_done_when_quiet_notice_flush_fails(sdk, monkeypatch):
    from tests.test_imessage_sdk import OUTBOUND_ID

    async def run():
        gw, session, host, store, meta, activity = harness(sdk)
        store.mark_delivery_failed(OUTBOUND_ID, {"chat_id": session.chat_id, "meta": meta})
        monkeypatch.setattr(session, "buffer_delivery_notice", lambda *_args: (_ for _ in ()).throw(OSError("disk unavailable")))
        await session.handle_inbound("Original request", "imessage", meta)
        await asyncio.wait_for(session._worker, 2)
        assert len(host.queries) == 1 and len(writes(sdk)) == 1
        assert store.summary()["done"] == 1 and store.summary()["uncertain"] == 0
        assert len(store.pending_failure_notices()) == 1
        assert store.lookup_outbound(OUTBOUND_ID)["status"] == "failed"
        assert [state for _, state in activity] == ["accepted", "completed"]
        await session.stop_companion()
        store.close()
    asyncio.run(run())


@pytest.mark.parametrize("restarted", [False, True])
def test_saved_answer_retries_only_preflight_through_actual_sdk(sdk, monkeypatch, restarted):
    async def run():
        gw, session, host, store, meta, activity = harness(sdk)
        original = gw._identity.get_imessage_thread
        attempts = 0

        @wraps(original)
        def read(*args, **kwargs):
            nonlocal attempts
            attempts += 1
            if attempts <= 4:
                assert store.summary()["reply_pending"] == 1
                assert all(state == "accepted" for _, state in activity)
                raise ConnectionError("read unavailable")
            return original(*args, **kwargs)

        monkeypatch.setattr(gw._identity, "get_imessage_thread", read)
        if restarted:
            store.mark(meta, "reply_pending", reply="Saved answer")
            store.close()
            gw._channel_stores.clear()
            await gw._recover_channel_inputs()
            store = gw._channel_store("imessage")
        else:
            await session.handle_inbound("Original request", "imessage", meta)
        await asyncio.wait_for(session._worker, timeout=3)
        assert attempts == 5 and len(host.queries) == (0 if restarted else 1)
        assert len(writes(sdk)) == 1
        body = json.loads(writes(sdk)[0].content)
        assert body["reply_to_message_id"] == SOURCE_ID
        assert body["conversation_id"] == CONVERSATION_ID
        assert body["text"] == ("Saved answer" if restarted else "Answer")
        assert store.summary()["done"] == 1
        assert [state for _, state in activity] == ["accepted", "completed"]
        await session.stop_companion()
        store.close()

    asyncio.run(run())


@pytest.mark.parametrize("mode", ["imessage", "slack"])
def test_recovered_answer_survives_nonretryable_read_before_send(sdk, monkeypatch, mode):
    async def run():
        gw, session, host, store, meta, activity = harness(sdk, mode=mode)
        store.mark(meta, "reply_pending", reply="Saved answer")
        if mode == "slack":
            original = gw._inkbox.slack.list_connections.side_effect
            connections = gw._inkbox.slack.list_connections.return_value
            gw._inkbox.slack.list_connections.return_value = []
        else:
            original = gw._identity.get_imessage
            monkeypatch.setattr(gw._identity, "get_imessage", lambda *_args, **_kwargs: (_ for _ in ()).throw(PermissionError("source unavailable")))
        await session.recover_reply("Saved answer", mode, meta)
        await asyncio.wait_for(session._worker, 2)
        assert store.summary()["reply_pending"] == 1 and store.summary()["failed"] == 0
        assert host.queries == [] and not writes(sdk)
        assert session._retrying_turn is None and session._queue.empty()
        assert [state for _, state in activity] == ["accepted", "failed"]
        if mode == "slack":
            gw._inkbox.slack.send_message.assert_not_called()
            gw._inkbox.slack.list_connections.side_effect = original
            gw._inkbox.slack.list_connections.return_value = connections
        else:
            monkeypatch.setattr(gw._identity, "get_imessage", original)
        # Explicit recovery retries only the saved output after the route is
        # available again. It never reruns the completed model request.
        store.close()
        gw._channel_stores.clear()
        await gw._recover_channel_inputs()
        await asyncio.wait_for(session._worker, 2)
        recovered = gw._channel_store(mode)
        assert recovered.summary()["done"] == 1 and host.queries == []
        if mode == "slack":
            assert gw._inkbox.slack.send_message.call_count == 1
        else:
            assert len(writes(sdk)) == 1
        recovered.close()

    asyncio.run(run())


def test_stop_during_retry_wait_does_not_revive_cancelled_answer(sdk, monkeypatch):
    async def run():
        gw, session, host, store, meta, activity = harness(sdk)
        original = gw._identity.get_imessage_thread
        monkeypatch.setattr("inkbox_claude.sessions.CHANNEL_RETRY_INITIAL_DELAY", 30)
        monkeypatch.setattr("inkbox_claude.sessions.CHANNEL_RETRY_MAX_DELAY", 30)
        monkeypatch.setattr(gw._identity, "get_imessage_thread", wraps(original)(lambda *_a, **_k: (_ for _ in ()).throw(ConnectionError())))
        await session.handle_inbound("Original request", "imessage", meta)
        await waiting(session)
        await session._abort_in_flight(only_mode="imessage")
        await asyncio.wait_for(session._worker, timeout=2)
        assert store.summary()["cancelled"] == 1 and not writes(sdk)
        assert [state for _, state in activity] == ["accepted", "cancelled"]
        monkeypatch.setattr(gw._identity, "get_imessage_thread", original)
        later = {**meta, "imessage_event_id": "later", "imessage_event_ids": ["later"]}
        assert store.admit(session.chat_id, "Fresh request", later)
        await session.handle_inbound("Fresh request", "imessage", later)
        await asyncio.wait_for(session._worker, timeout=2)
        assert len(host.queries) == 2 and len(writes(sdk)) == 1
        assert store.summary()["done"] == 1 and store.summary()["cancelled"] == 1
        await session.stop_companion()
        store.close()

    asyncio.run(run())


@pytest.mark.parametrize("cancel", ["stop", "generation"])
def test_preflight_completion_cannot_send_for_lost_owner(sdk, monkeypatch, cancel):
    async def run():
        gw, session, host, store, meta, activity = harness(sdk)
        entered, release = threading.Event(), threading.Event()
        original = gw._identity.get_imessage_thread

        @wraps(original)
        def read(*args, **kwargs):
            entered.set()
            assert release.wait(3)
            return original(*args, **kwargs)

        monkeypatch.setattr(gw._identity, "get_imessage_thread", read)
        await session.handle_inbound("Original request", "imessage", meta)
        try:
            assert await asyncio.to_thread(entered.wait, 2)
            if cancel == "stop":
                await session._abort_in_flight(only_mode="imessage")
            else:
                await session.close()
        finally:
            release.set()
        await asyncio.wait_for(session._worker, timeout=2)
        assert len(host.queries) == 1 and not writes(sdk)
        assert store.summary()["cancelled" if cancel == "stop" else "reply_pending"] == 1
        await session.stop_companion()
        store.close()

    asyncio.run(run())


def test_unknown_send_is_not_retried_or_replaced_by_error_send(sdk, monkeypatch):
    async def run():
        gw, session, host, store, meta, activity = harness(sdk)
        attempts = []

        def send(**kwargs):
            attempts.append(kwargs)
            raise ConnectionError("response lost after submission")

        monkeypatch.setattr(gw._identity, "send_imessage", send)
        monkeypatch.setattr("inkbox_claude.config.imessage_threading_capability", lambda *_: (True, "supported"))
        await session.handle_inbound("Original request", "imessage", meta)
        await asyncio.wait_for(session._worker, timeout=2)
        assert len(host.queries) == 1 and len(attempts) == 1
        assert store.summary()["uncertain"] == 1
        assert not store.pending_replies()
        assert [state for _, state in activity] == ["accepted", "failed"]
        await session.stop_companion()
        store.close()

    asyncio.run(run())


@pytest.mark.parametrize("failure", ["server_error", "response_lost"])
def test_recovered_saved_answer_keeps_unknown_post_uncertain_without_retry(sdk, monkeypatch, failure):
    async def run():
        gw, session, host, store, meta, activity = harness(sdk)
        store.mark(meta, "reply_pending", reply="Saved answer")
        if failure == "server_error":
            # SDK retries 503 internally with the same idempotency key. Use
            # its non-retried 500 to isolate the bridge's own recovery policy.
            sdk.send_status = 500
        else:
            original = gw._identity.send_imessage

            @wraps(original)
            def lose_response(**kwargs):
                original(**kwargs)
                raise ConnectionError("Synthetic response loss after POST")

            monkeypatch.setattr(gw._identity, "send_imessage", lose_response)

        async def forbidden_retry(*_args, **_kwargs):
            pytest.fail("Unknown saved-answer POST cannot enter the safe retry path")

        monkeypatch.setattr(session, "_wait_channel_retry", forbidden_retry)
        store.close()
        gw._channel_stores.clear()
        await gw._recover_channel_inputs()
        await asyncio.wait_for(session._worker, 3)
        recovered = gw._channel_store("imessage")
        assert host.queries == [] and len(writes(sdk)) == 1
        assert recovered.summary()["uncertain"] == 1 and recovered.summary()["failed"] == 0
        assert not recovered.pending_replies() and not recovered.replay_pending()
        await gw._recover_channel_inputs()
        assert len(writes(sdk)) == 1 and host.queries == []
        assert [state for _, state in activity] == ["accepted", "failed"]
        recovered.close()
    asyncio.run(run())


def test_shutdown_retains_answer_for_one_delivery_after_restart(sdk, monkeypatch):
    async def run():
        gw, session, host, store, meta, activity = harness(sdk)
        original = gw._identity.get_imessage_thread
        monkeypatch.setattr("inkbox_claude.sessions.CHANNEL_RETRY_INITIAL_DELAY", 30)
        monkeypatch.setattr("inkbox_claude.sessions.CHANNEL_RETRY_MAX_DELAY", 30)
        monkeypatch.setattr(gw._identity, "get_imessage_thread", wraps(original)(lambda *_a, **_k: (_ for _ in ()).throw(ConnectionError())))
        await session.handle_inbound("Original request", "imessage", meta)
        await waiting(session)
        await asyncio.wait_for(session.stop_companion(), timeout=2)
        assert store.summary()["reply_pending"] == 1 and not writes(sdk)
        assert all(state == "accepted" for _, state in activity)
        store.close()
        gw._channel_stores.clear()
        monkeypatch.setattr(gw._identity, "get_imessage_thread", original)
        fresh = make_session([])
        fresh.cfg, fresh.send_fn, fresh.receipt_fn = gw.cfg, gw.send_to_contact, gw._channel_receipt
        gw.sessions = NS(get=lambda _: fresh, sessions={fresh.chat_id: fresh})
        await gw._recover_channel_inputs()
        await asyncio.wait_for(fresh._worker, timeout=2)
        assert fresh._client is None and len(host.queries) == 1
        assert len(writes(sdk)) == 1
        assert gw._channel_store("imessage").summary()["done"] == 1
        await fresh.stop_companion()
        gw._channel_store("imessage").close()

    asyncio.run(run())


def test_slack_safe_preflight_wait_retains_activity_and_exact_thread(sdk):
    async def run():
        gw, session, host, store, meta, activity = harness(sdk, mode="slack")
        resource = gw._inkbox.slack
        connections = resource.list_connections.return_value
        resource.list_connections.side_effect = [ConnectionError()] * 4 + [connections]
        await session.handle_inbound("Original request", "slack", meta)
        await asyncio.wait_for(session._worker, timeout=2)
        assert len(host.queries) == 1 and resource.list_connections.call_count == 5
        assert resource.send_message.call_count == 1
        assert resource.send_message.call_args.kwargs["thread_ts"] == meta["thread_ts"]
        assert store.summary()["done"] == 1
        assert [state for _, state in activity] == ["accepted", "completed"]
        await session.stop_companion()
        store.close()

    asyncio.run(run())


def test_stop_during_safe_start_wait_cancels_before_any_query(sdk, monkeypatch):
    async def run():
        _gw, session, host, store, meta, activity = harness(sdk)
        monkeypatch.setattr("inkbox_claude.sessions.CHANNEL_RETRY_INITIAL_DELAY", 30)
        monkeypatch.setattr("inkbox_claude.sessions.CHANNEL_RETRY_MAX_DELAY", 30)
        attempts = []

        async def ensure():
            attempts.append(True)
            raise ConnectionError("host startup unavailable")

        monkeypatch.setattr(session, "_ensure_client", ensure)
        await session.handle_inbound("Original request", "imessage", meta)
        await waiting(session)
        await session._abort_in_flight(only_mode="imessage")
        await asyncio.wait_for(session._worker, timeout=2)
        assert len(attempts) == 1 and host.queries == [] and not writes(sdk)
        assert store.summary()["cancelled"] == 1
        assert [state for _, state in activity] == ["accepted", "cancelled"]
        await session.stop_companion()
        store.close()

    asyncio.run(run())


def test_lost_query_response_never_requeries_even_for_transport_error(sdk, monkeypatch):
    async def run():
        _gw, session, host, store, meta, _activity = harness(sdk)

        async def query(text):
            host.queries.append(text)
            raise ConnectionError("host may have accepted this query")

        monkeypatch.setattr(host, "query", query)
        await session.handle_inbound("Original request", "imessage", meta)
        await asyncio.wait_for(session._worker, timeout=2)
        assert len(host.queries) == 1
        assert store.summary()["uncertain"] == 1 and not store.replay_pending()
        assert not store.pending_replies()
        await session.stop_companion()
        store.close()

    asyncio.run(run())


def test_atomic_delivery_claim_rejects_cancelled_wrong_owner_and_duplicate(sdk):
    _gw, session, _host, store, meta, _activity = harness(sdk)
    store.mark(meta, "reply_pending", reply="Saved answer")
    with pytest.raises(PermissionError):
        store.begin_send(meta, "another-chat")
    assert store.summary()["reply_pending"] == 1
    store.begin_send(meta, session.chat_id)
    with pytest.raises(PermissionError):
        store.begin_send(meta, session.chat_id)
    store.mark(meta, "cancelled")
    assert store.summary()["uncertain"] == 1
    with pytest.raises(PermissionError):
        store.begin_send(meta, session.chat_id)
    store.close()


def test_slack_retry_control_requires_exact_actor_and_thread(sdk, monkeypatch):
    async def run():
        gw, session, host, store, meta, activity = harness(sdk, mode="slack")
        monkeypatch.setattr("inkbox_claude.sessions.CHANNEL_RETRY_INITIAL_DELAY", 30)
        monkeypatch.setattr("inkbox_claude.sessions.CHANNEL_RETRY_MAX_DELAY", 30)
        gw._inkbox.slack.list_connections.side_effect = ConnectionError()
        await session.handle_inbound("Original request", "slack", meta)
        await waiting(session)
        assert session.slack_stop_targets({**meta, "actor_id": "U_OTHER"}) == []
        assert session.slack_stop_targets({**meta, "thread_ts": "other-thread"}) == []
        targets = session.slack_stop_targets(meta)
        assert len(targets) == 1
        await session._abort_in_flight(only_mode="slack", only_turns=targets)
        await asyncio.wait_for(session._worker, timeout=2)
        assert len(host.queries) == 1 and not gw._inkbox.slack.send_message.called
        assert store.summary()["cancelled"] == 1
        assert [state for _, state in activity] == ["accepted", "cancelled"]
        await session.stop_companion()
        store.close()

    asyncio.run(run())
