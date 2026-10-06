"""Delivery callbacks remain quiet, durable and tied to their original route."""
import asyncio
import json
from types import SimpleNamespace as NS

import pytest

from inkbox_claude.config import BridgeConfig
from inkbox_claude.gateway import InkboxGateway
from inkbox_claude.imessage import source_metadata
from inkbox_claude.sessions import SessionManager


@pytest.fixture(autouse=True)
def isolated(monkeypatch, tmp_path):
    monkeypatch.setenv("INKBOX_CLAUDE_HOME", str(tmp_path))


def gateway():
    cfg = BridgeConfig(identity="test-agent", imessage_threaded_replies=True)
    gw = InkboxGateway(cfg)

    async def forbidden(*_args):
        pytest.fail("A delivery status must not send, type, interrupt or start model work")

    gw.sessions = SessionManager(cfg, forbidden, None, [], {}, typing_fn=forbidden, activity_fn=forbidden)
    gw._note_outbound_delivery_failure = forbidden
    return gw


def failure(message_id="outbound-one", **message):
    return {"data": {"message": {"id": message_id, "direction": "outbound",
            "conversation_id": "conversation-one", "remote_number": "+15555550123", **message}}}


@pytest.mark.parametrize("correlated", [False, True])
def test_failure_context_survives_restart_once_and_never_wakes(correlated):
    async def run():
        gw = gateway()
        store = gw._channel_store("imessage")
        if correlated:
            store.record_outbound(NS(id="outbound-one", status="pending"),
                                  {"conversation_id": "original-conversation"}, "original-chat")
        await gw._on_imessage_delivery_failed(failure())
        chat = "original-chat" if correlated else "imessage:conversation-one"
        session = gw.sessions.get(chat)
        assert len(session._context) == 1
        assert ("original-conversation" if correlated else "conversation-one") in session._context[0]["text"]
        assert "not a new request" in session._context[0]["text"]
        assert session._worker is None and session._queue.empty() and session._client is None
        await gw._on_imessage_delivery_failed(failure())
        assert len(session._context) == 1
        # Once context is consumed, a replay must not reintroduce the notice.
        session._context.clear()
        session._save_context()
        store.close()
        restarted = gateway()
        await restarted._on_imessage_delivery_failed(failure())
        assert restarted.sessions.get(chat)._context == []
        assert restarted._channel_store("imessage").summary()["outbound_failed_count"] == 1
        restarted._channel_store("imessage").close()
    asyncio.run(run())


def test_callback_first_preserves_fallback_notice_without_inventing_native_parent():
    async def run():
        gw = gateway()
        envelope = failure()
        envelope["data"]["contacts"] = [{"id": "contact-one"}]
        await gw._on_imessage_delivery_failed(envelope)
        store = gw._channel_store("imessage")
        original = list(gw.sessions.get("contact-one")._context)
        store.record_outbound(NS(id="outbound-one", status="pending", reply_to_message_id=None),
                              {"conversation_id": "conversation-one", **source_metadata({"id": "source-one"})}, "contact-one")
        await gw._on_imessage_delivery_failed(envelope)
        assert gw.sessions.get("contact-one")._context == original
        saved = store.lookup_outbound("outbound-one")
        assert saved["status"] == "failed" and saved["reply_to_message_id"] is None
        assert store.pending_failure_notices() == []
        store.close()
    asyncio.run(run())


def test_notice_checkpoint_failure_recovers_before_any_pending_model_work(monkeypatch):
    async def run():
        gw = gateway()
        store = gw._channel_store("imessage")
        original = store.mark_failure_notice_buffered
        monkeypatch.setattr(store, "mark_failure_notice_buffered", lambda _id: (_ for _ in ()).throw(OSError("checkpoint unavailable")))
        with pytest.raises(OSError):
            await gw._on_imessage_delivery_failed(failure())
        assert len(gw.sessions.get("imessage:conversation-one")._context) == 1
        monkeypatch.setattr(store, "mark_failure_notice_buffered", original)
        store.close()
        restarted = gateway()
        await restarted._recover_channel_inputs()
        session = restarted.sessions.get("imessage:conversation-one")
        assert len(session._context) == 1 and session._worker is None
        assert restarted._channel_store("imessage").pending_failure_notices() == []
        restarted._channel_store("imessage").close()
    asyncio.run(run())


def test_failed_context_flush_is_not_mistaken_for_durable_notice_on_retry(monkeypatch):
    async def run():
        gw = gateway()
        session = gw.sessions.get("imessage:conversation-one")
        save = session._save_context
        monkeypatch.setattr(session, "_save_context", lambda: (_ for _ in ()).throw(OSError("disk unavailable")))
        with pytest.raises(OSError):
            await gw._on_imessage_delivery_failed(failure())
        assert not session._context_path.exists()
        assert len(gw._channel_store("imessage").pending_failure_notices()) == 1
        monkeypatch.setattr(session, "_save_context", save)
        await gw._on_imessage_delivery_failed(failure())
        assert len(json.loads(session._context_path.read_text())) == 1
        assert gw._channel_store("imessage").pending_failure_notices() == []
        gw._channel_store("imessage").close()
    asyncio.run(run())


def test_route_missing_at_callback_is_filled_by_accepted_output():
    async def run():
        gw = gateway()
        await gw._on_imessage_delivery_failed(failure(conversation_id=None, remote_number=None))
        assert not gw.sessions.sessions
        store = gw._channel_store("imessage")
        store.record_outbound(NS(id="outbound-one", status="pending"), {"conversation_id": "actual"}, "actual-chat")
        gw._retain_imessage_failure_notices()
        assert len(gw.sessions.get("actual-chat")._context) == 1
        assert store.lookup_outbound("outbound-one")["status"] == "failed"
        store.close()
    asyncio.run(run())


def test_callback_first_notice_reaches_later_authoritative_contact_route_once():
    async def run():
        gw = gateway()
        await gw._on_imessage_delivery_failed(failure())
        assert len(gw.sessions.get("imessage:conversation-one")._context) == 1
        store = gw._channel_store("imessage")
        store.record_outbound(NS(id="outbound-one", status="pending"),
                              {"conversation_id": "conversation-one"}, "resolved-contact")
        gw._retain_imessage_failure_notices()
        target = gw.sessions.get("resolved-contact")
        assert len(target._context) == 1
        assert "conversation-one" in target._context[0]["text"]
        assert target._worker is None and target._client is None
        target._context.clear()
        target._save_context()
        await gw._on_imessage_delivery_failed(failure())
        assert target._context == []
        assert store.pending_failure_notices() == []
        store.close()
    asyncio.run(run())


def test_notice_buffer_is_bounded_and_preserves_other_quiet_input():
    async def run():
        gw = gateway()
        session = gw.sessions.get("imessage:conversation-one")
        session.buffer_context("Earlier human context", "human-source")
        for number in range(40):
            await gw._on_imessage_delivery_failed(failure(f"outbound-{number}"))
        assert len(session._context) == 33
        assert session._context[0] == {"id": "human-source", "text": "Earlier human context"}
        assert all(len(item["text"]) <= 1024 for item in session._context)
        persisted = json.loads(session._context_path.read_text())
        assert persisted == session._context
        await gw._on_imessage_delivery_failed(failure("outbound-0"))
        assert session._context == persisted
        gw._channel_store("imessage").close()
    asyncio.run(run())


@pytest.mark.parametrize("changes", [{"direction": "inbound"}, {"id": ""}])
def test_nonoutbound_or_unidentified_callback_does_not_create_context(changes):
    async def run():
        gw = gateway()
        await gw._on_imessage_delivery_failed(failure(**changes))
        assert not gw.sessions.sessions and not gw._channel_stores
    asyncio.run(run())
