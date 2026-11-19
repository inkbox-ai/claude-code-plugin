"""Voice-note admission, completion ordering, fallback and restart contracts."""

import asyncio
from copy import deepcopy
import types
from unittest.mock import AsyncMock, Mock
import time

import pytest

from inkbox_claude.voice_notes import (
    VoiceNotes,
    merge_results,
    unconfirmed_ids,
    message_for,
    pending,
    transcript_note,
)
from inkbox_claude import gateway


def envelope(
    channel="imessage", status="pending", *, event_id="received-event", count=1
):
    message = {
        "id": "message-1",
        "conversation_id": "conversation-1",
        "direction": "inbound",
        "content": "Typed caption",
        "text": "Typed caption",
        "media": [
            {
                "id": f"attachment-{index}",
                "url": "https://example.com/voice.wav",
                "content_type": "audio/wav",
                "transcription": {
                    "status": status,
                    "text": "Move the review to Thursday."
                    if status == "completed"
                    else None,
                },
            }
            for index in range(count)
        ],
    }
    return {
        "id": event_id,
        "event_type": f"{channel}.received",
        "data": {"text_message" if channel == "text" else "message": message},
    }


def completion(original, status="completed"):
    result = deepcopy(original)
    result["id"] = "separate-completion-event"
    result["data"].update(channel=original["event_type"].split(".")[0], message_id=message_for(original)["id"])
    result["event_type"] = "message.transcription_finished"
    for item in message_for(result)["media"]:
        item["transcription"] = {
            "status": status,
            "text": "Move the review to Thursday." if status == "completed" else None,
        }
    return result


def inbox(tmp_path, deliver, *, wait=60, client=None):
    identity = types.SimpleNamespace(
        id="identity-1", phone_number=types.SimpleNamespace(id="number-1")
    )
    return VoiceNotes(
        tmp_path / "voice.sqlite3", identity, client, deliver, wait_seconds=wait
    )


@pytest.mark.parametrize("channel", ["text", "imessage", "whatsapp"])
def test_pending_receipt_is_immediate_and_matching_completion_wakes_once(
    tmp_path, channel
):
    async def run():
        delivered = []

        async def deliver(item):
            delivered.append(item)

        state = inbox(tmp_path, deliver)
        original = envelope(channel)
        assert state.defer(original)  # No await: the HTTP handler can ACK now.
        assert state.defer(deepcopy(original))
        assert delivered == []
        mismatched = completion(original)
        mismatched["data"]["message_id"] = "different-source"
        state.complete(mismatched)
        assert state.db.execute("SELECT COUNT(*) FROM voice_notes WHERE completion IS NOT NULL").fetchone()[0] == 0
        state.complete(completion(original))
        await next(iter(state.tasks.values()))
        assert len(delivered) == 1
        assert message_for(delivered[0])["content"] == "Typed caption"
        assert (
            message_for(delivered[0])["media"][0]["url"]
            == "https://example.com/voice.wav"
        )
        assert "[Voice note 1 — automatically transcribed]" in transcript_note(
            message_for(delivered[0])["media"]
        )
        state.complete(completion(original))
        assert state.defer(original)
        assert len(delivered) == 1
        await state.close()

    asyncio.run(run())


def test_completion_before_original_is_cached_without_waking(tmp_path):
    async def run():
        delivered = []

        async def deliver(item):
            delivered.append(item)

        state = inbox(tmp_path, deliver)
        original = envelope()
        state.complete(completion(original))
        assert not state.tasks and not delivered
        state.defer(original)
        await next(iter(state.tasks.values()))
        assert len(delivered) == 1 and not pending(message_for(delivered[0]))
        await state.close()

    asyncio.run(run())


@pytest.mark.parametrize("outcome", ["completed", "pending", "failed", "error"])
def test_deadline_polls_once_then_delivers_original_attachment_path(tmp_path, outcome):
    async def run():
        delivered, reads = [], []
        original = envelope()

        def get(path, **kwargs):
            reads.append((path, kwargs))
            if outcome == "error":
                raise OSError("read unavailable")
            return message_for(envelope(status=outcome))

        async def deliver(item):
            delivered.append(item)

        state = inbox(
            tmp_path,
            deliver,
            wait=0,
            client=types.SimpleNamespace(_imessage_http=types.SimpleNamespace(get=get)),
        )
        state.defer(original)
        await next(iter(state.tasks.values()))
        assert len(reads) == 1 and reads[0][1]["timeout"] == 10
        assert reads[0][1]["params"] == {"agent_identity_id": "identity-1"}
        assert len(delivered) == 1
        body = message_for(delivered[0])
        assert (
            body["content"] == "Typed caption"
            and body["media"][0]["url"] == message_for(original)["media"][0]["url"]
        )
        assert (
            "[Voice note 1 — automatically transcribed]" in transcript_note(body["media"])
            if outcome == "completed"
            else "transcript unavailable" in transcript_note(body["media"])
        )
        state.complete(completion(original))
        assert len(delivered) == 1
        await state.close()

    asyncio.run(run())


def test_shutdown_retains_original_and_does_not_restart_sixty_second_deadline(tmp_path):
    async def run():
        delivered = []

        async def deliver(item):
            delivered.append(item)

        first = inbox(tmp_path, deliver)
        first.defer(envelope())
        deadline = first.db.execute("SELECT deadline FROM voice_notes").fetchone()[0]
        await first.close()
        second = inbox(tmp_path, deliver)
        assert (
            second.db.execute("SELECT deadline FROM voice_notes").fetchone()[0]
            == deadline
        )
        second.complete(completion(envelope()))
        second.recover()
        await next(iter(second.tasks.values()))
        assert len(delivered) == 1
        await second.close()

    asyncio.run(run())


def test_multiple_attachments_and_result_merge_preserve_original_routing():
    original = message_for(envelope(count=2))
    current = deepcopy(original)
    current["content"] = "Replacement text must not be used"
    current["media"].reverse()
    current["media"][0]["transcription"] = {
        "status": "completed",
        "text": "Second recording",
    }
    result = merge_results(original, current)
    assert pending(result)
    assert result["media"][1]["transcription"]["text"] == "Second recording"
    assert result["content"] == "Typed caption"
    current["conversation_id"] = "another-conversation"
    assert merge_results(original, current) == original


def test_terminal_and_legacy_messages_do_not_wait_and_silence_is_not_failure(tmp_path):
    async def run():
        async def deliver(item):
            raise AssertionError("No deferred turn")

        state = inbox(tmp_path, deliver)
        assert not state.defer(envelope(status="completed"))
        original = envelope()
        message_for(original)["media"][0].pop("transcription")
        assert not state.defer(original)
        assert not state.defer({"event_type": "message.received", "data": {}})
        assert "no speech detected" in transcript_note(
            [{"transcription": {"status": "completed", "text": ""}}]
        )
        await state.close()

    asyncio.run(run())


def test_subscription_catalogs_register_completion_events():
    from inkbox_claude.whatsapp import EVENTS

    assert gateway.TRANSCRIPTION_EVENTS == ["message.transcription_finished"]
    assert not set(gateway.TRANSCRIPTION_EVENTS).intersection(gateway.TEXT_EVENTS + gateway.IMESSAGE_EVENTS + list(EVENTS))


@pytest.mark.parametrize("channel", ["text", "imessage", "whatsapp"])
def test_configured_hosted_runtime_does_not_wait_again(tmp_path, monkeypatch, channel):
    monkeypatch.setenv("INKBOX_HOSTED_MODE", "1")

    async def run():
        async def deliver(item):
            raise AssertionError("The normal consumer owns this turn")

        state = inbox(tmp_path, deliver)
        original = envelope(channel)
        item = message_for(original)
        try:
            assert not state.defer(original)
            assert await asyncio.wait_for(state.resolve_message(channel, item), 0.1) is item
            assert not state.tasks
            assert "transcript unavailable" in transcript_note(item["media"])
            state.complete(completion(original))
            state.recover()
            assert not state.tasks
        finally:
            await state.close()

    asyncio.run(run())


def test_payload_cannot_claim_hosted_transcription_ownership(tmp_path, monkeypatch):
    monkeypatch.delenv("INKBOX_HOSTED_MODE", raising=False)

    async def run():
        async def deliver(item):
            raise AssertionError("Pending original should still wait")

        state = inbox(tmp_path, deliver)
        original = envelope()
        original["hosted_delivery"] = {"version": 1}
        try:
            assert state.defer(original)
            assert state.tasks
        finally:
            await state.close()

    asyncio.run(run())


def test_payload_cannot_disable_the_standalone_wait(tmp_path, monkeypatch):
    monkeypatch.delenv("INKBOX_HOSTED_MODE", raising=False)

    async def run():
        async def deliver(item):
            raise AssertionError("The pending message must wait")

        state = inbox(tmp_path, deliver)
        original = envelope()
        original["hosted_delivery"] = {"starts_agent": True}
        try:
            assert state.defer(original)
            assert state.tasks
        finally:
            await state.close()

    asyncio.run(run())


def test_non_audio_metadata_is_not_labelled_as_a_voice_note():
    media = [{"transcription": {"status": "skipped", "error_code": "not_audio"}}]
    assert transcript_note(media) == ""
    media[0]["transcription"]["error_code"] = "unsupported_audio"
    assert "transcript unavailable" in transcript_note(media)


def test_startup_prunes_old_resolved_companion_rows_but_keeps_unresolved_work(tmp_path):
    async def run():
        async def deliver(_item):
            raise AssertionError("Cleanup must not dispatch")

        state = inbox(tmp_path, deliver)
        old = int(time.time()) - 8 * 86400
        for key, direct, result, created in [
            ("old-resolved", 0, "{}", old),
            ("old-unresolved", 0, None, old),
            ("old-direct", 1, "{}", old),
            ("recent-resolved", 0, "{}", int(time.time())),
        ]:
            state.db.execute("INSERT INTO voice_notes(key,original,direct,result,created) VALUES(?,?,?,?,?)",
                             (key, "{}", direct, result, created))
        state.db.commit()
        await state.close()
        reopened = inbox(tmp_path, deliver)
        try:
            assert {row[0] for row in reopened.db.execute("SELECT key FROM voice_notes")} == {
                "old-unresolved", "old-direct", "recent-resolved",
            }
        finally:
            await reopened.close()

    asyncio.run(run())


def test_ambiguous_handler_failure_is_not_replayed_after_restart(tmp_path):
    async def run():
        admitted = []

        async def deliver(item):
            admitted.append(item)
            raise OSError("connection lost after handoff")

        state = inbox(tmp_path, deliver)
        original = envelope()
        state.complete(completion(original))
        state.defer(original)
        await next(iter(state.tasks.values()))
        await state.close()
        assert unconfirmed_ids(tmp_path / "voice.sqlite3") == ["imessage:message-1"]
        recovered = inbox(tmp_path, deliver)
        recovered.recover()
        refreshed = deepcopy(original)
        message_for(refreshed)["media"] = []
        assert recovered.defer(refreshed) and not recovered.tasks
        assert len(admitted) == 1
        await recovered.close()
    asyncio.run(run())


def test_gateway_retries_saved_message_after_sessions_start(tmp_path, monkeypatch):
    async def run():
        bridge = object.__new__(gateway.InkboxGateway)
        bridge._identity = types.SimpleNamespace(id="agent-1")
        bridge._inkbox = None
        bridge.sessions = None
        monkeypatch.setenv("INKBOX_CLAUDE_HOME", str(tmp_path))
        state = bridge._voice_note_inbox()
        original = envelope("text")
        message_for(original)["sender_phone_number"] = "+15555550123"
        state.complete(completion(original))
        state.defer(original)
        await asyncio.sleep(0)
        result, retry_at = state.db.execute("SELECT result,retry_at FROM voice_notes").fetchone()
        assert result and retry_at > time.time()
        await state.close()
        bridge._voice_note_state = None
        native = AsyncMock()
        bridge.sessions = Mock(get=Mock(return_value=native))
        bridge._recent_request_ids = {}
        bridge._inflight_request_ids = {}
        bridge.cfg = types.SimpleNamespace(contact_memories_enabled=False)
        bridge._sender_allowed = lambda _: True
        bridge._with_media = AsyncMock(return_value="caption and transcript")
        bridge._lookup_text_conversation_summary = AsyncMock(return_value=None)
        bridge._resolve_contact_full = AsyncMock(return_value=None)
        bridge._chat_key = Mock(return_value="sms:conversation-1")
        bridge._clear_outbound_failures = Mock()
        recovered = bridge._voice_note_inbox()
        recovered.db.execute("UPDATE voice_notes SET retry_at=0")
        recovered.db.commit()
        recovered.recover()
        await next(iter(recovered.tasks.values()))
        native.handle_inbound.assert_awaited_once()
        body, mode, meta = native.handle_inbound.await_args.args
        assert mode == "sms" and meta["conversation_id"] == "conversation-1"
        assert bridge._with_media.await_args.args[1] == message_for(completion(original))["media"]
        assert recovered.db.execute("SELECT done FROM voice_notes").fetchone() == (1,)
        await recovered.close()
    asyncio.run(run())


SLACK_IDENTITY = "00000000-0000-4000-8000-000000000001"
SLACK_CONNECTION = "00000000-0000-4000-8000-000000000002"
SLACK_MESSAGE = "00000000-0000-4000-8000-000000000009"


def slack_files(status):
    return [
        {"id": "F_VOICE", "name": "voice.m4a", "mimetype": "audio/mp4", "transcription": {
            "status": status,
            "text": "Move the review to Thursday." if status == "completed" else None,
            "error_code": "transcription_failed" if status == "failed" else None,
        }},
        {"id": "F_IMAGE", "name": "photo.png", "mimetype": "image/png"},
    ]


def slack_envelope(status="pending", *, event_type="slack.dm_received", message_id=SLACK_MESSAGE):
    return {"id": "evt_slack_1", "event_type": event_type, "data": {
        "identity_id": SLACK_IDENTITY, "connection_id": SLACK_CONNECTION, "workspace_id": "T_TEST",
        "conversation_id": "D_TEST", "message_ts": "1234567890.000001", "thread_ts": None,
        "actor_id": "U_ALICE", "message_kinds": ["dm"], "message_id": message_id,
        "event": {"type": "message", "text": "Typed caption", "files": slack_files(status)},
    }}


def slack_completion(original, status="completed"):
    result = deepcopy(original)
    result["id"] = "evt_slack_finished"
    result["event_type"] = "message.transcription_finished"
    result["data"]["channel"] = "slack"
    result["data"]["event"]["files"] = slack_files(status)
    return result


@pytest.mark.parametrize("event_type", [
    "slack.dm_received", "slack.group_dm_received", "slack.channel_message_received",
    "slack.mention_received", "slack.thread_reply_received",
])
def test_slack_pending_receipt_waits_for_matching_completion_and_wakes_once(tmp_path, event_type):
    async def run():
        delivered = []

        async def deliver(item):
            delivered.append(item)

        state = inbox(tmp_path, deliver)
        original = slack_envelope(event_type=event_type)
        assert message_for(original)["id"] == SLACK_MESSAGE and pending(message_for(original))
        assert state.defer(original)
        assert state.defer(deepcopy(original))
        assert delivered == []
        other = slack_completion(slack_envelope(message_id="00000000-0000-4000-8000-00000000000a"))
        state.complete(other)
        assert not delivered
        state.complete(slack_completion(original))
        await next(iter(state.tasks.values()))
        assert len(delivered) == 1
        files = delivered[0]["data"]["event"]["files"]
        assert delivered[0]["event_type"] == event_type
        assert delivered[0]["data"]["event"]["text"] == "Typed caption"
        assert delivered[0]["data"]["connection_id"] == SLACK_CONNECTION
        assert files[0]["transcription"]["text"] == "Move the review to Thursday."
        assert "transcription" not in files[1]
        # Late or repeated completions and redelivery never wake again.
        state.complete(slack_completion(original))
        assert state.defer(original)
        await asyncio.sleep(0)
        assert len(delivered) == 1
        await state.close()

    asyncio.run(run())


@pytest.mark.parametrize("outcome", ["completed", "pending", "failed", "error", "missing"])
def test_slack_deadline_polls_archived_message_once_then_falls_back(tmp_path, outcome):
    async def run():
        delivered, reads = [], []

        def get(path, **kwargs):
            reads.append((path, kwargs))
            if outcome == "error":
                raise OSError("read unavailable")
            if outcome == "missing":
                return {"messages": []}
            return {"messages": [{"id": SLACK_MESSAGE, "connection_id": SLACK_CONNECTION,
                                  "conversation_id": "D_TEST", "message_ts": "1234567890.000001",
                                  "text": "Replacement text must not be used",
                                  "files": slack_files(outcome)}]}

        async def deliver(item):
            delivered.append(item)

        state = inbox(tmp_path, deliver, wait=0,
                      client=types.SimpleNamespace(_api_http=types.SimpleNamespace(get=get)))
        original = slack_envelope()
        state.defer(original)
        await next(iter(state.tasks.values()))
        assert reads == [(f"/slack/connections/{SLACK_CONNECTION}/archive/messages",
                          {"params": {"message_id": SLACK_MESSAGE}, "timeout": 10})]
        assert len(delivered) == 1
        data = delivered[0]["data"]
        assert data["event"]["text"] == "Typed caption"
        note = transcript_note(data["event"]["files"])
        if outcome == "completed":
            assert note == "\n\n[Voice note 1 — automatically transcribed]\nMove the review to Thursday."
        else:
            assert "[Voice note 1: transcript unavailable;" in note
        state.complete(slack_completion(original))
        assert len(delivered) == 1
        await state.close()

    asyncio.run(run())


def test_slack_failed_completion_delivers_original_without_polling(tmp_path):
    async def run():
        delivered = []

        async def deliver(item):
            delivered.append(item)

        def get(*_args, **_kwargs):
            raise AssertionError("A terminal completion needs no read")

        state = inbox(tmp_path, deliver,
                      client=types.SimpleNamespace(_api_http=types.SimpleNamespace(get=get)))
        original = slack_envelope()
        state.defer(original)
        state.complete(slack_completion(original, "failed"))
        await next(iter(state.tasks.values()))
        assert len(delivered) == 1
        assert "transcript unavailable" in transcript_note(delivered[0]["data"]["event"]["files"])
        await state.close()

    asyncio.run(run())


def test_slack_messages_without_pending_audio_or_retained_id_do_not_wait(tmp_path):
    async def run():
        async def deliver(item):
            raise AssertionError("No deferred turn")

        state = inbox(tmp_path, deliver)
        assert not state.defer(slack_envelope("completed"))
        legacy = slack_envelope()
        legacy["data"].pop("message_id")
        assert not state.defer(legacy)
        plain = slack_envelope()
        plain["data"]["event"].pop("files")
        assert not state.defer(plain)
        assert not state.defer({**slack_envelope(), "event_type": "slack.session_stopped"})
        await state.close()

    asyncio.run(run())


def test_slack_label_matches_other_channels_and_follows_typed_text():
    from inkbox_claude.slack import inbound_message

    _, body, meta = inbound_message(slack_envelope("completed"), SLACK_IDENTITY)
    note = transcript_note(slack_files("completed"))
    assert note == transcript_note(message_for(envelope(status="completed"))["media"])
    assert body.startswith("Typed caption" + note + "\nAttachment references (not downloaded): ")
    assert meta["raw_text"] == "Typed caption"
    _, plain, _ = inbound_message(slack_envelope("completed"), SLACK_IDENTITY, transcripts=False)
    assert "Voice note" not in plain


def test_slack_companion_receipt_waits_with_the_native_conversation(tmp_path):
    async def run():
        async def deliver(item):
            raise AssertionError("The Companion consumer owns this turn")

        state = inbox(tmp_path, deliver)
        view = {"id": SLACK_MESSAGE, "conversation_id": "D_TEST", "connection_id": SLACK_CONNECTION,
                "media": [{key: item[key] for key in ("id", "name", "mimetype", "transcription") if key in item}
                          for item in slack_files("pending")]}
        waiting = asyncio.create_task(state.resolve_message("slack", view))
        await asyncio.sleep(0)
        state.complete(slack_completion(slack_envelope()))
        resolved = await asyncio.wait_for(waiting, 1)
        assert resolved["media"][0]["transcription"]["status"] == "completed"
        assert resolved["media"][1] == view["media"][1]
        await state.close()

    asyncio.run(run())


def test_gateway_routes_deferred_slack_receipts_to_the_slack_handler():
    async def run():
        bridge = object.__new__(gateway.InkboxGateway)
        bridge._on_slack_received = AsyncMock(return_value="slack")
        bridge._on_text_received = AsyncMock()
        bridge._on_imessage_received = AsyncMock()
        assert await bridge._deliver_voice_note(slack_envelope("completed")) == "slack"
        bridge._on_text_received.assert_not_awaited()
        bridge._on_imessage_received.assert_not_awaited()

    asyncio.run(run())


def test_unsupported_completion_error_is_recognized_at_any_event_position():
    from inkbox_claude.voice_notes import transcription_unsupported

    error = types.SimpleNamespace(status_code=422, detail=[{
        "type": "enum", "loc": ["body", "event_types", 6, "SlackWebhookEventType"],
        "input": "message.transcription_finished",
    }])
    assert transcription_unsupported(error)
    error.detail[0]["input"] = "slack.dm_received"
    assert not transcription_unsupported(error)
