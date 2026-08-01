"""Live voice-call suite — real phone calls, real model, transcript-verified.

Three scenarios, each run against a bridge booted in the matching speech mode (the
workflow sets that up and selects the scenario via VOICE_SCENARIO):

  * inbound_inkbox   — the driver calls the agent; the agent answers with Inkbox
                       STT/TTS and holds a turn.
  * outbound_realtime — the driver texts "call me"; the agent places a call back,
                       powered by the realtime API, and holds a turn.
  * outbound_hosted — the driver texts "call me"; Inkbox Voice AI runs the call,
                      then call.ended wakes Claude Code to perform one action.

A companion driver process (voice_driver.py) bridges the driver's side of the call
over an Inkbox tunnel and speaks one line. We then read the stored call transcript
and assert both parties spoke — proving the agent reached the caller out loud.
"""

from __future__ import annotations

import json
import os
import re
import time
import uuid
from datetime import UTC, datetime

import pytest

# The agent answers a call request by dialing back, not by texting, so these
# driver→AUT SMS never get an SMS reply to reset the server's conversation
# cadence. Two identical no-reply sends to the same number trip the
# duplicate_body rule (422), so every call request must carry a fresh body.
_CALL_ME_PHRASINGS = (
    "Please call me right now by phone and set voicemail_detection to disabled.",
    "Can you ring me now with voicemail_detection disabled?",
    "Give me a call now, using disabled voicemail_detection.",
    "Please phone me right away and disable voicemail_detection.",
)


def _call_me_text() -> str:
    """A fresh call-request body each send (rotating phrasing + unique ref)."""
    phrasing = _CALL_ME_PHRASINGS[uuid.uuid4().int % len(_CALL_ME_PHRASINGS)]
    return f"{phrasing} (ref {uuid.uuid4().hex[:6]})"

REMOTE_KEY = os.environ.get("REMOTE_INKBOX_API_KEY")
AUT_KEY = os.environ.get("CLAUDE_CODE_INKBOX_API_KEY")
BASE_URL = os.environ.get("INKBOX_BASE_URL", "https://inkbox.ai")
REAL = os.environ.get("LIVE_REAL_MODEL") == "1"
SCENARIO = os.environ.get("VOICE_SCENARIO", "")
HOSTED_POST_CALL_MARKER = os.environ.get("HOSTED_POST_CALL_MARKER", "")
GATEWAY_LOG = os.environ.get("GATEWAY_LOG", "")
STATE_FILE = os.environ.get("VOICE_DRIVER_STATE", "/tmp/voice_driver_state.json")
TIMEOUT_S = float(os.environ.get("LIVE_VOICE_TIMEOUT", "220"))
POLL_EVERY_S = 6.0
TERMINAL_FAILURE_STATUSES = {"canceled", "failed"}
# A call can end normally and still never carry a conversation - answering-machine
# detection hanging up on the driver ends it `completed`, hangup_reason=voicemail.
# Transcript rows can still land during teardown, so allow a short grace period
# before giving up rather than polling a finished call for the full timeout.
ENDED_STATUSES = {"completed"}
ENDED_GRACE_S = float(os.environ.get("LIVE_VOICE_ENDED_GRACE", "15"))

pytestmark = pytest.mark.skipif(
    not (REMOTE_KEY and AUT_KEY and REAL),
    reason="voice suite: needs both keys + LIVE_REAL_MODEL=1",
)


def _digits(s: str) -> str:
    return re.sub(r"\D", "", s or "")


def _spoken_tokens(value: str | None) -> list[str]:
    """Normalize speech-carried text without depending on punctuation/case."""
    return re.findall(r"[a-z0-9]+", (value or "").casefold())


def _spoken_key(value: str | None) -> str:
    return "".join(_spoken_tokens(value))


def _message_created_at(message):
    """Return an aware server timestamp from an SDK SMS row."""
    value = getattr(message, "created_at", None)
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _sms_target_numbers(message) -> set[str]:
    """All authoritative targets represented by an outbound SMS row."""
    values = [getattr(message, "remote_phone_number", "") or ""]
    values.extend(
        getattr(recipient, "recipient_phone_number", "") or ""
        for recipient in (getattr(message, "recipients", None) or [])
    )
    return {digits for value in values if (digits := _digits(value))}


def _has_after_call_sms_intent(value: str | None) -> bool:
    tokens = _spoken_tokens(value)
    token_set = set(tokens)
    joined = " ".join(tokens)
    after_call = "after" in token_set and bool(
        token_set & {"call", "hangup", "hang", "hung"}
    )
    send = bool(token_set & {"send", "text"})
    sms = bool(token_set & {"sms", "text", "message"}) or "s m s" in joined
    return after_call and send and sms


def _client(key):
    from inkbox import Inkbox

    return Inkbox(api_key=key, base_url=BASE_URL)


def _driver_state() -> dict:
    with open(STATE_FILE) as fh:
        return json.load(fh)


def _gateway_log_since(offset: int) -> str:
    """Return gateway log output emitted after the hosted-call request."""
    if not GATEWAY_LOG or not os.path.exists(GATEWAY_LOG):
        return ""
    with open(GATEWAY_LOG, encoding="utf-8", errors="replace") as handle:
        handle.seek(offset)
        return handle.read()


def _aut_phone(aut) -> str:
    nums = aut.phone_numbers.list()
    assert nums, "AUT identity has no phone number"
    return nums[0].number


def _segments(remote, number_id, call_id):
    """Transcript segments for a call, split by who spoke."""
    # Identity-centered transcript read (SDK 0.4.15+); number_id is vestigial.
    segs = remote.calls.transcripts(call_id)
    rem = [s for s in segs if (getattr(s, "party", "") or "").lower() == "remote" and (s.text or "").strip()]
    loc = [s for s in segs if (getattr(s, "party", "") or "").lower() == "local" and (s.text or "").strip()]
    return segs, rem, loc


def _wait_for_persisted_hosted_request(remote, number_id, call_id, marker):
    """Wait for the current after-call SMS request in the caller transcript."""
    marker_key = _spoken_key(marker)
    assert marker_key
    deadline = time.monotonic() + min(TIMEOUT_S, 90)
    last = ""
    while time.monotonic() < deadline:
        try:
            _all, _rem, local = _segments(remote, number_id, call_id)
            text = " ".join(segment.text.strip() for segment in local)
            if marker_key in _spoken_key(text) and _has_after_call_sms_intent(text):
                return
            last = f"local transcript so far: {text!r}"
        except Exception as exc:  # transcripts may trail call teardown briefly
            last = f"transcripts not ready: {exc!r}"
        time.sleep(POLL_EVERY_S)
    pytest.fail(
        "call ended before the current after-call SMS request was persisted "
        f"({last})"
    )


def _call_state(remote, call_id) -> tuple[str, str]:
    """Compact current call state for progress and terminal-failure output."""
    call = remote.calls.get(call_id)
    status = (getattr(call, "status", "") or "").lower()
    fields = (
        f"status={status!r}",
        f"reason={getattr(call, 'reason', None)!r}",
        f"hangup_reason={getattr(call, 'hangup_reason', None)!r}",
        f"started_at={getattr(call, 'started_at', None)!r}",
        f"ended_at={getattr(call, 'ended_at', None)!r}",
        f"is_blocked={getattr(call, 'is_blocked', None)!r}",
    )
    return status, " ".join(fields)


def _wait_for_call_end(client, call_id) -> None:
    """Wait for the scripted driver to finish instead of cutting its turn off."""
    deadline = time.monotonic() + TIMEOUT_S
    last = ""
    while time.monotonic() < deadline:
        status, last = _call_state(client, call_id)
        if status in ENDED_STATUSES | TERMINAL_FAILURE_STATUSES:
            return
        time.sleep(POLL_EVERY_S)
    pytest.fail(f"call did not end within {TIMEOUT_S:.0f}s ({last})")


def _wait_for_two_way_call(remote, number_id, call_id):
    """Block until the call transcript shows BOTH the agent and the driver spoke."""
    deadline = time.monotonic() + TIMEOUT_S
    last = ""
    ended_at = None
    while time.monotonic() < deadline:
        transcript_state = ""
        try:
            _all, rem, loc = _segments(remote, number_id, call_id)
        except Exception as exc:  # transcripts may 404 until the call is set up
            rem, loc = [], []
            transcript_state = f"transcripts not ready: {exc!r}"
        if not transcript_state and rem and loc:
            agent_said = " | ".join(s.text.strip() for s in rem)
            return agent_said  # the agent reached the caller out loud, in a two-way call
        try:
            status, state = _call_state(remote, call_id)
        except Exception as exc:
            state = f"call state unavailable: {exc!r}"
            status = ""
        progress = transcript_state or f"segments so far: remote={len(rem)} local={len(loc)}"
        last = f"{progress}; {state}"
        if status in TERMINAL_FAILURE_STATUSES:
            pytest.fail(f"call ended before a two-way conversation ({last})")
        if status in ENDED_STATUSES:
            if ended_at is None:
                ended_at = time.monotonic()
            elif time.monotonic() - ended_at > ENDED_GRACE_S:
                pytest.fail(f"call ended without a two-way conversation ({last})")
        time.sleep(POLL_EVERY_S)
    pytest.fail(f"agent never held a two-way call within {TIMEOUT_S:.0f}s ({last})")


def _aut_speech_mode(aut, direction, driver_number):
    """(use_inkbox_tts, use_inkbox_stt) of the agent's most recent answered call
    in `direction` with the driver. Tells Inkbox STT/TTS (True/True) from realtime
    (False/False), so each leg can prove it ran the speech path it claims."""
    tail = _digits(driver_number)[-10:]
    answered = [c for c in aut.calls.list(limit=10)
                if (getattr(c, "direction", "") or "").lower() == direction
                and _digits(getattr(c, "remote_phone_number", "") or "")[-10:] == tail
                and c.use_inkbox_tts is not None]
    assert answered, f"no answered {direction} agent call with the driver found"
    c = answered[0]  # newest first
    return c.use_inkbox_tts, c.use_inkbox_stt


def _hangup_call(client, call_id) -> None:
    """End a live test call through the control API, tolerating an ended race."""
    if not call_id:
        return
    try:
        client.calls.hangup(call_id)
        return
    except Exception as hangup_error:
        deadline = time.monotonic() + 10
        status = "unknown"
        while time.monotonic() < deadline:
            try:
                status = (getattr(client.calls.get(call_id), "status", "") or "").lower()
            except Exception:
                status = "unknown"
            if status in {"completed", "canceled", "failed"}:
                return
            time.sleep(0.5)
        raise RuntimeError(
            f"failed to hang up live test call {call_id}; status={status!r}"
        ) from hangup_error


@pytest.mark.skipif(SCENARIO != "inbound_inkbox", reason="inbound Inkbox STT/TTS leg only")
def test_inbound_call_inkbox_tts_stt():
    """Driver calls the agent; the agent answers via Inkbox STT/TTS and replies."""
    st = _driver_state()
    remote, aut = _client(REMOTE_KEY), _client(AUT_KEY)
    aut_phone = _aut_phone(aut)

    # Place the call to the agent, handing Inkbox the driver's own media WS.
    call = remote.calls.place(
        from_number=st["number"],
        to_number=aut_phone,
        client_websocket_url=st["ws_url"],
        voicemail_detection="disabled",
    )
    try:
        agent_said = _wait_for_two_way_call(remote, st["number_id"], call.id)
        assert agent_said, "agent produced no speech on the inbound call"

        tts, stt = _aut_speech_mode(aut, "inbound", st["number"])
        assert tts and stt, f"inbound call should run Inkbox STT/TTS, got tts={tts} stt={stt}"
    finally:
        _hangup_call(remote, call.id)


@pytest.mark.skipif(SCENARIO != "outbound_realtime", reason="outbound realtime leg only")
def test_outbound_call_realtime():
    """Driver texts 'call me'; the agent places a realtime-powered call and replies."""
    st = _driver_state()
    remote, aut = _client(REMOTE_KEY), _client(AUT_KEY)
    aut_phone = _aut_phone(aut)
    tail = _digits(aut_phone)[-10:]

    def _inbound_from_aut():
        return [c for c in remote.calls.list(limit=30)
                if (getattr(c, "direction", "") or "").lower() == "inbound"
                and _digits(getattr(c, "remote_phone_number", "") or "")[-10:] == tail]

    before = {c.id for c in _inbound_from_aut()}
    remote.texts.send(st["number_id"], to=aut_phone, text=_call_me_text())

    call_id = None
    try:
        # Wait for the agent to dial back, then verify the call transcript.
        deadline = time.monotonic() + TIMEOUT_S
        while time.monotonic() < deadline:
            fresh = [c for c in _inbound_from_aut() if c.id not in before]
            if fresh:
                call_id = fresh[0].id
                break
            time.sleep(POLL_EVERY_S)
        assert call_id, f"agent never placed a call back within {TIMEOUT_S:.0f}s"

        agent_said = _wait_for_two_way_call(remote, st["number_id"], call_id)
        assert agent_said, "agent produced no speech on the outbound call"

        tts, stt = _aut_speech_mode(aut, "outbound", st["number"])
        assert tts is False and stt is False, \
            f"outbound call must be powered by the realtime API (Inkbox speech off), got tts={tts} stt={stt}"
    finally:
        _hangup_call(remote, call_id)


@pytest.mark.skipif(SCENARIO != "outbound_hosted", reason="outbound Voice AI leg only")
def test_outbound_call_hosted_and_post_call_wakeup():
    """Voice AI runs the call; Claude executes its open action after call.ended."""
    assert HOSTED_POST_CALL_MARKER, "HOSTED_POST_CALL_MARKER is required"
    st = _driver_state()
    remote, aut = _client(REMOTE_KEY), _client(AUT_KEY)
    aut_numbers = aut.phone_numbers.list()
    assert aut_numbers, "AUT identity has no phone number"
    aut_phone = aut_numbers[0].number
    aut_number_id = aut_numbers[0].id
    tail = _digits(aut_phone)[-10:]
    driver_number = _digits(st["number"])

    def _inbound_calls():
        return [c for c in remote.calls.list(limit=30)
                if (getattr(c, "direction", "") or "").lower() == "inbound"
                and _digits(getattr(c, "remote_phone_number", "") or "")[-10:] == tail]

    driver_tail = driver_number[-10:]

    def _outbound_calls():
        return [c for c in aut.calls.list(limit=30)
                if (getattr(c, "direction", "") or "").lower() == "outbound"
                and _digits(getattr(c, "remote_phone_number", "") or "")[-10:] == driver_tail]

    def _outbound_sms_to_driver():
        return [
            message
            for message in aut.texts.list(aut_number_id, limit=200)
            if (getattr(message, "direction", "") or "").lower() == "outbound"
            and driver_number in _sms_target_numbers(message)
        ]

    before_calls = {c.id for c in _inbound_calls()}
    before_outbound = {c.id for c in _outbound_calls()}
    baseline_sms = _outbound_sms_to_driver()
    before_sms = {message.id for message in baseline_sms}
    baseline_times = [
        created_at
        for message in baseline_sms
        if (created_at := _message_created_at(message)) is not None
    ]
    sms_watermark = max(
        baseline_times,
        default=datetime.min.replace(tzinfo=UTC),
    )
    assert GATEWAY_LOG and os.path.exists(GATEWAY_LOG), (
        "GATEWAY_LOG must expose the bridge's host-native tool settlement"
    )
    log_offset = os.path.getsize(GATEWAY_LOG)
    remote.texts.send(st["number_id"], to=aut_phone, text=_call_me_text())

    call_id = None
    try:
        deadline = time.monotonic() + TIMEOUT_S
        while time.monotonic() < deadline:
            fresh = [c for c in _inbound_calls() if c.id not in before_calls]
            if fresh:
                call_id = fresh[0].id
                break
            time.sleep(POLL_EVERY_S)
        assert call_id, f"agent never placed a hosted call within {TIMEOUT_S:.0f}s"
        _wait_for_two_way_call(remote, st["number_id"], call_id)

        # A phone call has two independently handled legs. The driver's inbound
        # leg is intentionally ``client_websocket`` so the scripted media peer
        # can answer it; the AUT's outbound leg is the one Voice AI must own.
        fresh_outbound = [c for c in _outbound_calls() if c.id not in before_outbound]
        assert fresh_outbound, "AUT has no matching outbound call record"
        placed = fresh_outbound[0]
        assert str(getattr(getattr(placed, "mode", ""), "value", getattr(placed, "mode", ""))) == "hosted_agent"
        assert str(getattr(getattr(placed, "voicemail_detection", ""), "value", getattr(placed, "voicemail_detection", ""))) == "disabled"

        # The driver speaks the action after its short human greeting and then
        # ends the call itself. Let that complete so Voice AI can acknowledge
        # and persist the instruction before call.ended wakes Claude Code.
        _wait_for_call_end(remote, call_id)
        _wait_for_persisted_hosted_request(
            remote,
            st["number_id"],
            call_id,
            HOSTED_POST_CALL_MARKER,
        )
    finally:
        _hangup_call(remote, call_id)

    placed_call_id = str(placed.id)
    tool_marker = (
        "confirmed hosted call SMS tool completion "
        f"call_id={placed_call_id}"
    )
    completed_marker = f"completed hosted call completion call_id={placed_call_id}"
    deadline = time.monotonic() + TIMEOUT_S
    log = ""
    marker_sms = []
    while time.monotonic() < deadline:
        log = _gateway_log_since(log_offset)
        marker_sms = [
            message
            for message in _outbound_sms_to_driver()
            if message.id not in before_sms
            and (created_at := _message_created_at(message)) is not None
            and created_at >= sms_watermark
            and _spoken_key(HOSTED_POST_CALL_MARKER)
            in _spoken_key(getattr(message, "text", None))
        ]
        if tool_marker in log and completed_marker in log and marker_sms:
            break
        time.sleep(POLL_EVERY_S)
    assert tool_marker in log, (
        "hosted call ended without one exact-recipient SMS confirmed by the "
        "Claude session's post-SDK-return delivery marker"
    )
    assert completed_marker in log, (
        "hosted SMS was sent but its post-call receipt did not complete"
    )
    assert marker_sms, (
        "hosted completion did not create a current exact-recipient sender-side "
        "SMS row with the spoken marker"
    )

    # Give any accidental second attempt time to become visible, then prove the
    # API-accepted side effect occurred exactly once. Carrier delivery is
    # asynchronous and belongs to the SMS delivery lane, not reconciliation.
    time.sleep(2 * POLL_EVERY_S)
    marker_sms = [
        message
        for message in _outbound_sms_to_driver()
        if message.id not in before_sms
        and (created_at := _message_created_at(message)) is not None
        and created_at >= sms_watermark
        and _spoken_key(HOSTED_POST_CALL_MARKER)
        in _spoken_key(getattr(message, "text", None))
    ]
    assert len(marker_sms) == 1, (
        "hosted post-call processing did not produce exactly one current-marker "
        "SMS to the authoritative caller: "
        f"{[getattr(message, 'text', '') for message in marker_sms]}"
    )
    assert log.count(tool_marker) == 1, (
        "hosted post-call processing recorded duplicate successful SMS side effects"
    )
