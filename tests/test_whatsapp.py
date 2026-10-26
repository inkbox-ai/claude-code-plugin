"""Dedicated WhatsApp routes preserve ownership, durable sends, and no-wake statuses."""

import asyncio
import json
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from inkbox import Inkbox
from inkbox_claude.config import BridgeConfig
from inkbox_claude.gateway import InkboxGateway
from inkbox_claude import gateway as gateway_module
from inkbox_claude.whatsapp import receive, send_reply, register_tools

OWNER = "10000000-0000-4000-8000-000000000001"
CONV = "20000000-0000-4000-8000-000000000002"
MESSAGE = "30000000-0000-4000-8000-000000000003"
MEDIA = "40000000-0000-4000-8000-000000000004"
TIME = "2026-10-01T12:00:00Z"
MESSAGE_WIRE = {
    "id": MESSAGE,
    "agent_identity_id": OWNER,
    "conversation_id": CONV,
    "direction": "outbound",
    "local_number": "+15555550100",
    "remote_number": "+15555550101",
    "sender_number": "+15555550100",
    "participants": ["+15555550101"],
    "is_group": False,
    "content": "Answer",
    "media": [],
    "status": "sent",
    "delivery_state": "unknown",
    "recipients": [],
    "is_read": False,
    "is_blocked": False,
    "error_code": None,
    "error_detail": None,
    "source_at": None,
    "created_at": TIME,
    "updated_at": TIME,
    "sent_at": TIME,
    "delivered_at": None,
    "failed_at": None,
    "status_observed_at": TIME,
    "status_refresh_pending": False,
}


def event(**changes):
    return {
        "id": "evt_whatsapp_example",
        "event_type": "whatsapp.received",
        "timestamp": TIME,
        "data": {
            "contacts": [],
            "agent_identities": [],
            "message": {
                **MESSAGE_WIRE,
                "direction": "inbound",
                "sender_number": "+15555550101",
                "status": "received",
                "delivery_state": None,
                **changes,
            },
        },
    }


@pytest.fixture
def setup(tmp_path, monkeypatch):
    monkeypatch.setenv("INKBOX_CLAUDE_HOME", str(tmp_path))
    from aiohttp import web

    monkeypatch.setattr(gateway_module, "web", web)
    client = Inkbox("test-only")
    client._ids_resource._http = MagicMock()
    client._ids_resource._http.get.return_value = {
        "id": OWNER,
        "organization_id": "org_example",
        "agent_handle": "agent",
        "created_at": TIME,
        "updated_at": TIME,
    }
    identity = client.get_identity("agent")
    client.whatsapp._http = MagicMock()
    client.whatsapp._http.post.return_value = {"message": MESSAGE_WIRE}
    gw = InkboxGateway(
        BridgeConfig(identity="agent", allow_all_users=True, require_signature=False)
    )
    gw._identity = identity
    gw._inkbox = client
    session = NS(handle_inbound=AsyncMock())
    gw.sessions = NS(get=MagicMock(return_value=session))
    gw._resolve_contact_full = AsyncMock(return_value=None)
    gw._with_media = AsyncMock(side_effect=lambda text, media, **kwargs: text)
    yield gw, session, client
    for store in gw._channel_stores.values():
        store.close()
    client.close()


def test_received_is_owned_deduplicated_and_retained_even_without_cached_approval(
    setup,
):
    gw, session, client = setup
    assert not gw._identity.whatsapp_enabled
    asyncio.run(receive(gw, event()))
    asyncio.run(receive(gw, event()))
    assert session.handle_inbound.await_count == 1
    body, mode, meta = session.handle_inbound.call_args.args
    assert (
        mode == "whatsapp"
        and meta["conversation_id"] == CONV
        and meta["channel"] == "whatsapp"
    )
    with gw._channel_store("whatsapp")._db() as db:
        assert db.execute("SELECT count(*) FROM receipts").fetchone()[0] == 1


@pytest.mark.parametrize(
    "changes",
    [
        {"agent_identity_id": "wrong"},
        {"direction": "outbound"},
        {"is_group": True},
        {"participants": ["+15555550101", "+15555550102"]},
        {"sender_number": "+15555550100"},
    ],
)
def test_wrong_owner_echo_and_groups_do_not_wake(setup, changes):
    gw, session, _ = setup
    asyncio.run(receive(gw, event(**changes)))
    session.handle_inbound.assert_not_called()


def test_received_media_is_renewed_by_owned_id_and_unavailable_is_explicit(setup):
    gw, session, client = setup
    client.whatsapp._http.get.return_value = {
        "id": MEDIA,
        "filename": "image.png",
        "content_type": "image/png",
        "size": 3,
        "status": "ready",
        "url": "https://media.example/fresh",
    }
    asyncio.run(
        receive(
            gw,
            event(
                content=None,
                media=[
                    {
                        "id": MEDIA,
                        "status": "ready",
                        "url": "https://media.example/expired",
                    }
                ],
            ),
        )
    )
    client.whatsapp._http.get.assert_called_once_with(
        f"/media/{MEDIA}", params={"agent_identity_id": OWNER}
    )
    assert gw._with_media.call_args.args[1][0]["url"] == "https://media.example/fresh"
    assert "could not be downloaded" in session.handle_inbound.call_args.args[0]


def test_ordinary_reply_uses_stable_key_and_never_replays_after_send_checkpoint(
    setup, monkeypatch
):
    gw, _, client = setup
    monkeypatch.setattr("inkbox._message_requests.time.sleep", lambda _: None)
    meta = {
        "channel": "whatsapp",
        "conversation_id": CONV,
        "source_event_id": "evt_example",
        "imessage_event_ids": ["evt_example"],
        "message_id": MESSAGE,
        "sender": "+15555550101",
    }
    store = gw._channel_store("whatsapp")
    assert store.admit("chat", "request", meta)
    store.mark(meta, "reply_pending", reply="Answer", chat_id="chat")
    client.whatsapp._http.post.side_effect = httpx.ReadTimeout("response lost")
    with pytest.raises(httpx.ReadTimeout):
        asyncio.run(send_reply(gw, "chat", "Answer", {**meta, "final_reply": True}))
    calls = client.whatsapp._http.post.call_args_list
    assert len(calls) == 3 and all(call == calls[0] for call in calls)
    assert calls[0].kwargs["params"] == {"agent_identity_id": OWNER}
    with pytest.raises(PermissionError):
        asyncio.run(send_reply(gw, "chat", "Answer", {**meta, "final_reply": True}))
    assert client.whatsapp._http.post.call_count == 3


def test_statuses_require_signature_even_when_generic_verification_disabled_and_never_wake(
    setup, monkeypatch
):
    gw, session, _ = setup
    gw.cfg.external_events_enabled = True
    provider = NS(name="inkbox", verify=MagicMock(return_value=False))
    monkeypatch.setattr(gateway_module, "match_provider", lambda _: provider)
    gw._on_external_event = AsyncMock()
    payload = {
        **event(),
        "event_type": "whatsapp.delivery_unconfirmed",
        "data": {"message": MESSAGE_WIRE},
    }

    class Request:
        headers = {
            "X-Inkbox-Signature": "sha256=test",
            "X-Inkbox-Request-Id": "wa-status",
        }
        url = "https://example.com/webhook"

        async def read(self):
            return json.dumps(payload).encode()

    assert asyncio.run(gw._handle_webhook(Request())).status == 401
    provider.verify.return_value = True
    assert asyncio.run(gw._handle_webhook(Request())).status == 200
    session.handle_inbound.assert_not_called()
    gw._on_external_event.assert_not_called()
    assert (
        gw._channel_store("whatsapp").lookup_outbound(MESSAGE)["delivery_state"]
        == "unknown"
    )


def test_tools_keep_owned_media_and_turn_authorization(setup, tmp_path):
    gw, _, client = setup
    registered = {}
    register_tools(
        lambda name, description, properties, required, operation: registered.update(
            {name: operation}
        ),
        client,
        "agent",
        gw.cfg,
    )
    assert (
        "inkbox_send_whatsapp" in registered
        and "inkbox_get_whatsapp_status" in registered
    )
    auth = MagicMock()
    registered["inkbox_send_whatsapp"](
        {"conversation_id": CONV, "media_id": MEDIA}, auth
    )
    assert client.whatsapp._http.post.call_args.kwargs["json"] == {
        "conversation_id": CONV,
        "media_ids": [MEDIA],
    }
    path = tmp_path / "image.png"
    path.write_bytes(b"png")
    with pytest.raises(PermissionError):
        registered["inkbox_upload_whatsapp_media"](
            {"media_path": str(path)},
            MagicMock(side_effect=PermissionError("cancelled")),
        )
    client.whatsapp._http.post_multipart.assert_not_called()


def test_cross_channel_permission_reply_cannot_approve_a_whatsapp_turn():
    from inkbox_claude.escalation import PendingInteraction
    from tests.test_sessions import make_session

    async def run():
        session = make_session([])
        session._worker = asyncio.current_task()
        future = asyncio.get_running_loop().create_future()
        session.pending = PendingInteraction(
            kind="permission",
            prompt_text="approve",
            future=future,
            sender="+15555550101",
            route={
                "channel": "whatsapp",
                "conversation_id": CONV,
                "sender": "+15555550101",
            },
        )
        await session.handle_inbound(
            "yes", "sms", {"conversation_id": CONV, "sender": "+15555550101"}
        )
        assert not future.done()
        assert session._queue.get_nowait().mode == "sms"

    asyncio.run(run())


def test_burst_never_merges_whatsapp_with_another_channel():
    from inkbox_claude.sessions import _Turn
    from tests.test_sessions import make_session

    async def run():
        session = make_session([])
        wa = _Turn(
            text="WhatsApp",
            mode="whatsapp",
            reply_meta={"channel": "whatsapp", "conversation_id": CONV},
            queued_at=0,
        )
        sms = _Turn(
            text="SMS", mode="sms", reply_meta={"conversation_id": CONV}, queued_at=0
        )
        session._batching_turn = wa
        await session._queue.put(sms)
        assert await session._batch(wa) is wa
        assert wa.text == "WhatsApp" and session._deferred_turn is sms

    asyncio.run(run())


def test_lookup_tool_recovers_id_without_sending(setup):
    gw, _, client = setup
    client.message_sends._http = MagicMock()
    client.message_sends._http.get.return_value = {"message_id": MESSAGE}
    registered = {}
    register_tools(
        lambda name, description, properties, required, operation: registered.update(
            {name: operation}
        ),
        client,
        "agent",
        gw.cfg,
    )
    assert registered["inkbox_lookup_whatsapp_send"](
        {"idempotency_key": "stable-key"}, MagicMock()
    ) == {"message_id": MESSAGE}
    client.message_sends._http.get.assert_called_once_with(
        "/message-sends/lookup",
        params={
            "sender_kind": "whatsapp_identity",
            "sender_id": OWNER,
            "operation": "whatsapp.send",
        },
        headers={"Idempotency-Key": "stable-key"},
    )
    client.whatsapp._http.post.assert_not_called()


def test_status_before_send_response_and_old_callback_keep_latest_facts(setup):
    from inkbox_claude.whatsapp import record_status

    gw, _, _ = setup
    newest = {
        **MESSAGE_WIRE,
        "status": "delivered",
        "delivery_state": "delivered",
        "updated_at": "2026-10-01T12:02:00Z",
    }
    record_status(gw, {"event_type": "whatsapp.delivered", "data": {"message": newest}})
    store = gw._channel_store("whatsapp")
    store.record_outbound(
        NS(id=MESSAGE, status="sent", delivery_state="pending"),
        {"channel": "whatsapp", "conversation_id": CONV},
        "chat",
    )
    record_status(
        gw, {"event_type": "whatsapp.sent", "data": {"message": MESSAGE_WIRE}}
    )
    assert store.lookup_outbound(MESSAGE)["status"] == "delivered"
    assert store.lookup_outbound(MESSAGE)["delivery_state"] == "delivered"
    assert store.lookup_outbound(MESSAGE)["updated_at"] == "2026-10-01T12:02:00Z"


def test_public_quota_errors_retain_only_policy_fields_and_retry_advice():
    from inkbox import InkboxAPIError
    from inkbox_claude.whatsapp import public_quota_error
    error = InkboxAPIError(429, {"error": "whatsapp_rate_limit", "quota": "whatsapp.texts_per_agent_identity_per_24h", "limit": 20, "used": 20, "reset_at": "2026-10-27T00:00:00Z", "retry_condition": "Wait until reset", "message": "untrusted provider body", "credentials": "private"}, retry_after="60")
    assert public_quota_error(error) == {"error": "whatsapp_rate_limit", "quota": "whatsapp.texts_per_agent_identity_per_24h", "limit": 20, "used": 20, "reset_at": "2026-10-27T00:00:00Z", "retry_condition": "Wait until reset", "retry_after_seconds": 60}
    assert public_quota_error(InkboxAPIError(502, {"error": "upstream", "message": "private"})) == {}
