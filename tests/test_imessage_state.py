"""Receipt recovery and native source reply scoping use real local storage."""

import json
import sqlite3
from types import SimpleNamespace

import pytest

from inkbox_claude.config import BridgeConfig
from inkbox_claude.imessage import IMessageState, auto_reply_kwargs, source_metadata


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv("INKBOX_CLAUDE_HOME", str(tmp_path))


def metadata(message_id="message-1", **kwargs):
    return {"conversation_id": "conversation-1", **source_metadata({
        "id": message_id, "content": "A synthetic question", **kwargs,
    }, event_id=f"event-{message_id}")}


def test_durable_duplicate_receipts_preserve_first_admitted_route():
    cfg = BridgeConfig(identity="test-agent")
    state = IMessageState(cfg)
    meta = metadata()
    assert state.admit("chat-1", "original text", meta)
    restarted = IMessageState(cfg)
    assert not restarted.admit("chat-2", "changed text", meta)
    assert restarted.replay_pending() == [{"chat_id": "chat-1", "text": "original text", "meta": meta}]


@pytest.mark.parametrize("phase", ["pending", "reply_pending", "running", "sending"])
def test_stop_is_durable_before_in_memory_abort_and_keeps_uncertain_ownership(phase):
    cfg = BridgeConfig(identity="test-agent")
    state = IMessageState(cfg, channel="slack")
    old = {"source_event_id": "old"}
    state.admit("chat-1", "Old work", old)
    state.mark(old, phase, reply="Saved answer" if phase == "reply_pending" else None)
    state.admit("chat-2", "Other work", {"source_event_id": "other"})
    assert state.consume_control("stop", [{"chat_id": "chat-1", "source_event_ids": ["old"]}])
    # Restart immediately, before a session has processed the Stop at all.
    restarted = IMessageState(cfg, channel="slack")
    assert restarted.was_stopped("chat-1", "old")
    assert not restarted.was_stopped("chat-2", "old")
    assert [item["text"] for item in restarted.replay_pending()] == ["Other work"]
    assert restarted.pending_replies() == []
    assert restarted.summary()["cancelled"] == (1 if phase in {"pending", "reply_pending"} else 0)
    assert len(restarted.interrupted()) == (1 if phase in {"running", "sending"} else 0)
    restarted.admit("chat-1", "Fresh work", {"source_event_id": "fresh"})
    assert not restarted.consume_control("stop", [{"chat_id": "chat-1", "source_event_ids": ["fresh"]}])
    assert not restarted.was_stopped("chat-1", "fresh")
    assert [item["text"] for item in restarted.replay_pending()] == ["Other work", "Fresh work"]


def test_stop_tombstone_and_queued_cancellation_commit_together():
    state = IMessageState(BridgeConfig(identity="test-agent"), channel="slack")
    state.admit("chat", "Work", {"source_event_id": "source"})
    with state._db() as db:
        db.execute("CREATE TRIGGER fail_cancel BEFORE UPDATE ON receipts "
                   "BEGIN SELECT RAISE(ABORT, 'write unavailable'); END")
    targets = [{"chat_id": "chat", "source_event_ids": ["source"]}]
    with pytest.raises(sqlite3.IntegrityError, match="write unavailable"):
        state.consume_control("stop", targets)
    assert not state.was_stopped("chat", "source")
    assert len(state.replay_pending()) == 1
    with state._db() as db:
        db.execute("DROP TRIGGER fail_cancel")
    assert state.consume_control("stop", targets)
    assert state.replay_pending() == []


def test_trigger_anchors_survive_restart_without_combining_unsubmitted_fragments():
    state = IMessageState(BridgeConfig(identity="test-agent"))
    first, second = metadata("first"), metadata("second")
    state.admit("chat-1", "first task", first)
    state.admit("chat-1", "second task", second)
    replay = state.replay_pending()
    assert [row["text"] for row in replay] == ["first task", "second task"]
    assert [row["meta"]["imessage_reply_target"] for row in replay] == ["first", "second"]
    assert [row["meta"]["imessage_sources"][0]["id"] for row in replay] == ["first", "second"]


def test_restart_replays_only_unstarted_work_and_never_uncertain_sends():
    state = IMessageState(BridgeConfig(identity="test-agent"))
    for index, phase in enumerate(("pending", "running", "sending", "done", "cancelled", "failed")):
        meta = metadata(f"message-{index}")
        state.admit("chat-1", phase, meta)
        state.mark(meta, phase)
    assert [row["text"] for row in state.replay_pending()] == ["pending"]
    assert state.summary()["uncertain"] == 2
    assert state.summary()["unfinished"] == 4
    assert state.pending_replies() == []
    state.mark(metadata("message-3"), "running")
    state.mark(metadata("message-4"), "reply_pending", reply="must not revive")
    assert state.summary()["done"] == 1
    assert state.summary()["cancelled"] == 1
    assert state.pending_replies() == []


def test_completed_burst_recovers_one_saved_reply_without_model_work():
    cfg = BridgeConfig(identity="test-agent")
    state = IMessageState(cfg)
    first, second = metadata("message-1"), metadata("message-2")
    state.admit("chat-1", "first", first)
    state.admit("chat-1", "second", second)
    batch = {
        **first,
        "imessage_event_ids": first["imessage_event_ids"] + second["imessage_event_ids"],
        "imessage_sources": first["imessage_sources"] + second["imessage_sources"],
    }
    state.mark(batch, "running", chat_id="chat-1")
    state.mark(batch, "reply_pending", reply="One completed answer", chat_id="chat-1")
    restarted = IMessageState(cfg)
    assert restarted.replay_pending() == []
    replies = restarted.pending_replies()
    assert len(replies) == 1
    assert replies[0]["reply"] == "One completed answer"
    assert replies[0]["meta"] == batch
    restarted.mark(replies[0]["meta"], "sending")
    restarted.mark(replies[0]["meta"], "done")
    assert restarted.pending_replies() == []
    assert restarted.summary()["done"] == 2


def test_read_preflight_failure_retains_answer_but_send_uncertainty_cannot_replay():
    cfg = BridgeConfig(identity="test-agent")
    state = IMessageState(cfg)
    meta = metadata()
    state.admit("chat-1", "question", meta)
    state.mark(meta, "running")
    state.mark(meta, "reply_pending", reply="Completed answer")
    state.mark(meta, "uncertain")
    restarted = IMessageState(cfg)
    assert restarted.replay_pending() == []
    assert restarted.pending_replies()[0]["reply"] == "Completed answer"
    restarted.mark(meta, "sending")
    restarted.mark(meta, "uncertain")
    assert restarted.pending_replies() == []
    assert restarted.summary()["uncertain"] == 1


def test_identity_and_environment_have_isolated_receipts(tmp_path):
    configurations = [
        BridgeConfig(identity="one", base_url="https://example.test/api"),
        BridgeConfig(identity="two", base_url="https://example.test/api"),
        BridgeConfig(identity="one", base_url="https://other.example.test/api"),
    ]
    states = [IMessageState(cfg) for cfg in configurations]
    assert len({state.path for state in states}) == 3
    assert all(state.admit("chat-1", "same event", metadata()) for state in states)
    for state in states:
        assert state.path.stat().st_mode & 0o777 == 0o600
        assert state.path.parent.stat().st_mode & 0o777 == 0o700
        assert state.path.is_relative_to(tmp_path)


def test_wrong_chat_cannot_update_receipt():
    state = IMessageState(BridgeConfig(identity="one"))
    state.admit("chat-1", "question", metadata())
    state.mark(metadata(), "done", chat_id="chat-2")
    assert state.summary()["pending"] == 1


def test_outbound_ids_and_native_nulls_are_not_inferred_or_rerouted():
    cfg = BridgeConfig(identity="one")
    state = IMessageState(cfg)
    meta = metadata(reply_to_message_id="earlier-message")
    accepted = SimpleNamespace(id="sent-1", status="pending", reply_to_message_id=None,
                               thread_id=None, thread_root_message_id=None)
    state.record_outbound(accepted, meta, "chat-1")
    state.record_outbound({"id": "sent-1", "status": "delivered"}, metadata("other"), "chat-2")
    route = IMessageState(cfg).lookup_outbound("sent-1")
    assert route == {
        "chat_id": "chat-1", "meta": meta, "message_id": "sent-1", "status": "pending",
        "reply_to_message_id": None, "thread_id": None, "thread_root_message_id": None,
    }
    assert state.lookup_outbound("unknown") is None


def test_late_failure_keeps_original_route_and_completed_model_receipt():
    cfg = BridgeConfig(identity="one")
    state = IMessageState(cfg)
    meta = metadata()
    state.admit("chat-1", "question", meta)
    state.mark(meta, "done")
    state.record_outbound({"id": "sent-1", "status": "pending"}, meta, "chat-1")
    state.mark_delivery_failed("sent-1")
    restarted = IMessageState(cfg)
    assert restarted.lookup_outbound("sent-1")["meta"] == meta
    assert restarted.lookup_outbound("sent-1")["status"] == "failed"
    assert restarted.summary()["done"] == 1
    assert restarted.summary()["outbound_failed_count"] == 1
    assert restarted.summary()["unfinished"] == 0


def test_callback_before_send_response_keeps_failure_and_later_fills_real_route():
    cfg = BridgeConfig(identity="one")
    state = IMessageState(cfg)
    state.mark_delivery_failed("sent-1")
    assert state.lookup_outbound("sent-1") == {
        "chat_id": None, "meta": {}, "message_id": "sent-1", "status": "failed", "unknown_route": True,
    }
    meta = metadata(reply_to_message_id="earlier-message")
    state.record_outbound({"id": "sent-1", "status": "pending", "reply_to_message_id": "message-1"}, meta, "chat-1")
    restarted = IMessageState(cfg)
    route = restarted.lookup_outbound("sent-1")
    assert route["status"] == "failed"
    assert route["chat_id"] == "chat-1"
    assert route["meta"] == meta
    assert route["reply_to_message_id"] == "message-1"
    assert "unknown_route" not in route
    assert restarted.summary()["outbound_failed_count"] == 1
    restarted.record_outbound({"id": "sent-1", "status": "delivered"}, metadata("other"), "wrong-chat")
    assert restarted.lookup_outbound("sent-1") == route


def test_any_known_trigger_selects_an_automatic_target():
    ordinary = source_metadata({"id": "message-1", "thread_id": "standalone-thread"})
    assert ordinary["imessage_event_id"] == "message-1"
    assert ordinary["imessage_sources"][0]["thread_root_message_id"] is None
    assert auto_reply_kwargs(ordinary) == {"reply_to_message_id": "message-1", "plain_reply_fallback": True}
    reply = source_metadata({"id": "message-2", "reply_to_message_id": "message-1"})
    assert reply["reply_to_message_id"] == "message-1"
    assert ordinary["thread_id"] == "standalone-thread"
    assert auto_reply_kwargs(reply) == {"reply_to_message_id": "message-2", "plain_reply_fallback": True}


def test_no_source_cannot_invent_a_reply_target_from_native_ancestry():
    meta = source_metadata({"thread_id": "opaque-thread", "thread_root_message_id": "old-root"})
    assert auto_reply_kwargs(meta) == {}


def test_visible_root_can_confirm_reply_without_inventing_hidden_parent():
    root = source_metadata({"id": "message-1", "thread_root_message_id": "message-1"})
    assert auto_reply_kwargs(root) == {"reply_to_message_id": "message-1", "plain_reply_fallback": True}
    descendant = source_metadata({"id": "message-2", "thread_root_message_id": "message-1"})
    assert descendant["reply_to_message_id"] is None
    assert descendant["imessage_sources"][0]["reply_to_message_id"] is None
    assert auto_reply_kwargs(descendant) == {"reply_to_message_id": "message-2", "plain_reply_fallback": True}














def test_empty_receipt_ids_fail_closed_and_unknown_states_are_rejected():
    state = IMessageState(BridgeConfig(identity="one"))
    with pytest.raises(ValueError, match="stable event"):
        state.admit("chat-1", "question", {})
    with pytest.raises(ValueError, match="Unknown"):
        state.mark(metadata(), "not-a-state")
    with sqlite3.connect(state.path) as db:
        assert db.execute("SELECT COUNT(*) FROM receipts").fetchone()[0] == 0
