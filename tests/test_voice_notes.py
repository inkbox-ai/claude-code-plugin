"""Voice-note admission, completion ordering, fallback and restart contracts."""

import asyncio
from copy import deepcopy
import types
import time

import pytest

from inkbox_claude.voice_notes import (
    VoiceNotes,
    merge_results,
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
    result["event_type"] = result["event_type"].replace(
        "received", "transcription_finished"
    )
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
        state.complete(completion(original))
        await next(iter(state.tasks.values()))
        assert len(delivered) == 1
        assert message_for(delivered[0])["content"] == "Typed caption"
        assert (
            message_for(delivered[0])["media"][0]["url"]
            == "https://example.com/voice.wav"
        )
        assert "auto-generated transcript" in transcript_note(
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
            "auto-generated transcript" in transcript_note(body["media"])
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

    assert "text.transcription_finished" in gateway.TEXT_EVENTS
    assert "imessage.transcription_finished" in gateway.IMESSAGE_EVENTS
    assert "whatsapp.transcription_finished" in EVENTS


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
