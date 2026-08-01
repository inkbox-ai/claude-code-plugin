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
HOSTED_POST_CALL_SETTLEMENT_S = 90.0
HOSTED_DUPLICATE_GRACE_S = 2 * POLL_EVERY_S
HOSTED_SCENARIO_TIMEOUT_S = (
    TIMEOUT_S
    + HOSTED_POST_CALL_SETTLEMENT_S
    + HOSTED_DUPLICATE_GRACE_S
    + POLL_EVERY_S
)
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


def _enum_value(value) -> str:
    return str(getattr(value, "value", value))


def _voicemail_detection_value(call) -> str:
    return _enum_value(getattr(call, "voicemail_detection", ""))


def _fresh_call_records(records, before: set, watermark: datetime):
    return [
        record for record in records
        if record.id not in before
        and (created_at := _message_created_at(record)) is not None
        and created_at >= watermark
    ]


def _call_pair_diagnostic(driver_calls, aut_calls) -> str:
    return (
        "phase=call_pairing "
        f"driver_ids={[str(call.id) for call in driver_calls]} "
        f"aut_ids={[str(call.id) for call in aut_calls]}"
    )


def _correlate_fresh_call_pair(
    driver_records,
    aut_records,
    *,
    before_driver: set,
    before_aut: set,
    driver_watermark: datetime,
    aut_watermark: datetime,
):
    """Return the exact driver/AUT records for one newly persisted call."""
    driver_calls = _fresh_call_records(
        driver_records, before_driver, driver_watermark
    )
    aut_calls = _fresh_call_records(aut_records, before_aut, aut_watermark)
    diagnostic = _call_pair_diagnostic(driver_calls, aut_calls)
    assert len(driver_calls) <= 1, f"{diagnostic} duplicate driver legs"
    assert len(aut_calls) <= 1, f"{diagnostic} duplicate AUT legs"
    if not driver_calls or not aut_calls:
        return None
    driver_created = _message_created_at(driver_calls[0])
    aut_created = _message_created_at(aut_calls[0])
    assert driver_created is not None and aut_created is not None
    assert abs((driver_created - aut_created).total_seconds()) <= 60, (
        f"{diagnostic} timestamps do not describe one call"
    )
    return driver_calls[0], aut_calls[0]


def _spoken_tokens(value: str | None) -> list[str]:
    """Normalize speech-carried text without depending on punctuation/case."""
    return re.findall(r"[a-z0-9]+", (value or "").casefold())


def _spoken_key(value: str | None) -> str:
    key = "".join(_spoken_tokens(value))
    # Claude is a common ASR homophone for "cloud". Canonicalize the observed
    # boundary-less form too ("cloudpapa") without relaxing any of the other
    # current-run marker, action, status, channel, or recipient requirements.
    return key.replace("cloud", "claude")


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


def _has_sms_action_intent(value: str | None) -> bool:
    """Recognize a persisted send-SMS action without requiring prose wording."""
    tokens = _spoken_tokens(value)
    token_set = set(tokens)
    joined = " ".join(tokens)
    send = bool(token_set & {"send", "text"})
    sms = bool(token_set & {"sms", "text", "message"}) or "s m s" in joined
    return send and sms


def _matching_post_call_action(call, marker):
    """Return the open current-marker SMS action persisted for a hosted call."""
    marker_key = _spoken_key(marker)
    for item in getattr(call, "post_call_action_items", None) or []:
        if isinstance(item, dict):
            status = item.get("status", "")
            action = item.get("action", "")
            details = item.get("details", "")
        else:
            status = getattr(item, "status", "")
            action = getattr(item, "action", "")
            details = getattr(item, "details", "")
        value = f"{action} {details}"
        if (
            str(status).casefold() == "open"
            and marker_key in _spoken_key(value)
            and _has_sms_action_intent(value)
        ):
            return item
    return None


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


def _wait_for_persisted_hosted_request(
    remote,
    number_id,
    call_id,
    aut,
    aut_call_id,
    marker,
    *,
    deadline,
):
    """Wait for both caller intent and the AUT's durable open action item."""
    marker_key = _spoken_key(marker)
    assert marker_key
    transcript_ready = False
    action_ready = False
    last_transcript = ""
    last_actions = ""
    while time.monotonic() < deadline:
        try:
            _all, _rem, local = _segments(remote, number_id, call_id)
            text = " ".join(segment.text.strip() for segment in local)
            transcript_ready = (
                marker_key in _spoken_key(text)
                and _has_after_call_sms_intent(text)
            )
            last_transcript = repr(text)
        except Exception as exc:  # transcripts may trail call teardown briefly
            last_transcript = f"not ready: {exc!r}"
        try:
            aut_call = aut.calls.get(aut_call_id)
            action_ready = _matching_post_call_action(aut_call, marker) is not None
            last_actions = repr(getattr(aut_call, "post_call_action_items", None))
        except Exception as exc:
            last_actions = f"not ready: {exc!r}"
        if transcript_ready and action_ready:
            return
        time.sleep(POLL_EVERY_S)
    pytest.fail(
        "hosted call did not persist both current caller intent and its open "
        "post-call SMS action before the shared deadline "
        f"(transcript_ready={transcript_ready}, action_ready={action_ready}, "
        f"local_transcript={last_transcript}, action_items={last_actions})"
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


def _wait_for_two_way_call(remote, number_id, call_id, *, deadline=None):
    """Block until the call transcript shows BOTH the agent and the driver spoke."""
    if deadline is None:
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
        persisted = remote.calls.get(call.id)
        assert _voicemail_detection_value(persisted) == "disabled"

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
    driver_tail = _digits(st["number"])[-10:]

    def _inbound_from_aut():
        return [c for c in remote.calls.list(limit=30)
                if (getattr(c, "direction", "") or "").lower() == "inbound"
                and _digits(getattr(c, "remote_phone_number", "") or "")[-10:] == tail]

    def _outbound_from_aut():
        return [c for c in aut.calls.list(limit=30)
                if (getattr(c, "direction", "") or "").lower() == "outbound"
                and _digits(getattr(c, "remote_phone_number", "") or "")[-10:] == driver_tail]

    baseline_driver = _inbound_from_aut()
    baseline_aut = _outbound_from_aut()
    before_driver = {call.id for call in baseline_driver}
    before_aut = {call.id for call in baseline_aut}
    driver_watermark = max(
        (
            created_at for call in baseline_driver
            if (created_at := _message_created_at(call)) is not None
        ),
        default=datetime.min.replace(tzinfo=UTC),
    )
    aut_watermark = max(
        (
            created_at for call in baseline_aut
            if (created_at := _message_created_at(call)) is not None
        ),
        default=datetime.min.replace(tzinfo=UTC),
    )
    remote.texts.send(st["number_id"], to=aut_phone, text=_call_me_text())

    call_id = None
    aut_call = None
    try:
        # Pair the driver's inbound media leg with the AUT's outbound request.
        deadline = time.monotonic() + TIMEOUT_S
        while time.monotonic() < deadline:
            pair = _correlate_fresh_call_pair(
                _inbound_from_aut(),
                _outbound_from_aut(),
                before_driver=before_driver,
                before_aut=before_aut,
                driver_watermark=driver_watermark,
                aut_watermark=aut_watermark,
            )
            if pair is not None:
                driver_call, aut_call = pair
                call_id = driver_call.id
                break
            time.sleep(POLL_EVERY_S)
        assert call_id and aut_call is not None, (
            f"agent never persisted both call legs within {TIMEOUT_S:.0f}s; "
            + _call_pair_diagnostic(
                _fresh_call_records(_inbound_from_aut(), before_driver, driver_watermark),
                _fresh_call_records(_outbound_from_aut(), before_aut, aut_watermark),
            )
        )
        persisted = aut.calls.get(aut_call.id)
        assert _voicemail_detection_value(persisted) == "disabled"

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

    baseline_driver_calls = _inbound_calls()
    baseline_aut_calls = _outbound_calls()
    before_calls = {c.id for c in baseline_driver_calls}
    before_outbound = {c.id for c in baseline_aut_calls}
    driver_call_watermark = max(
        (
            created_at
            for call in baseline_driver_calls
            if (created_at := _message_created_at(call)) is not None
        ),
        default=datetime.min.replace(tzinfo=UTC),
    )
    aut_call_watermark = max(
        (
            created_at
            for call in baseline_aut_calls
            if (created_at := _message_created_at(call)) is not None
        ),
        default=datetime.min.replace(tzinfo=UTC),
    )
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
    identity_handle = aut.mailboxes.list()[0].email_address.split("@", 1)[0]
    hosted_config = aut.get_identity(identity_handle).get_hosted_agent_config()
    expected_authority = _enum_value(
        getattr(hosted_config, "authority_mode", "contact_scoped")
    ) or "contact_scoped"
    scenario_deadline = time.monotonic() + HOSTED_SCENARIO_TIMEOUT_S
    pre_hangup_deadline = (
        scenario_deadline
        - HOSTED_POST_CALL_SETTLEMENT_S
        - HOSTED_DUPLICATE_GRACE_S
        - POLL_EVERY_S
    )
    remote.texts.send(st["number_id"], to=aut_phone, text=_call_me_text())

    call_id = None
    placed = None
    try:
        while time.monotonic() < pre_hangup_deadline:
            fresh_driver = [
                call
                for call in _inbound_calls()
                if call.id not in before_calls
                and (created_at := _message_created_at(call)) is not None
                and created_at >= driver_call_watermark
            ]
            fresh_aut = [
                call
                for call in _outbound_calls()
                if call.id not in before_outbound
                and (created_at := _message_created_at(call)) is not None
                and created_at >= aut_call_watermark
            ]
            if fresh_driver:
                call_id = max(fresh_driver, key=_message_created_at).id
            if fresh_aut:
                placed = max(fresh_aut, key=_message_created_at)
            if call_id and placed is not None:
                break
            time.sleep(POLL_EVERY_S)
        assert call_id and placed is not None, (
            "hosted call pairing did not find both fresh driver and AUT legs "
            f"(driver_call_id={call_id!r}, aut_call_id={getattr(placed, 'id', None)!r})"
        )
        driver_call = remote.calls.get(call_id)
        driver_created_at = _message_created_at(driver_call)
        aut_created_at = _message_created_at(placed)
        assert driver_created_at is not None and aut_created_at is not None
        assert abs((driver_created_at - aut_created_at).total_seconds()) <= 60, (
            "fresh driver and AUT records are not the same hosted call: "
            f"driver_created_at={driver_created_at!r} "
            f"aut_created_at={aut_created_at!r}"
        )
        _wait_for_two_way_call(
            remote,
            st["number_id"],
            call_id,
            deadline=pre_hangup_deadline,
        )

        # A phone call has two independently handled legs. The driver's inbound
        # leg is intentionally ``client_websocket`` so the scripted media peer
        # can answer it; the AUT's outbound leg is the one Voice AI must own.
        assert str(getattr(getattr(placed, "mode", ""), "value", getattr(placed, "mode", ""))) == "hosted_agent"
        assert _voicemail_detection_value(placed) == "disabled"
        assert getattr(placed, "reason", None)
        assert _enum_value(
            getattr(placed, "hosted_agent_authority_mode", "")
        ) == expected_authority

        # Do not hang up merely because the words reached the caller transcript.
        # Voice AI must also persist the matching open action on the AUT call;
        # both gates share the original placement deadline.
        _wait_for_persisted_hosted_request(
            remote,
            st["number_id"],
            call_id,
            aut,
            placed.id,
            HOSTED_POST_CALL_MARKER,
            deadline=pre_hangup_deadline,
        )
    finally:
        _hangup_call(remote, call_id)

    placed_call_id = str(placed.id)
    tool_marker = (
        "confirmed hosted call SMS tool completion "
        f"call_id={placed_call_id}"
    )
    completed_marker = f"completed hosted call completion call_id={placed_call_id}"
    settlement_deadline = (
        scenario_deadline - HOSTED_DUPLICATE_GRACE_S - POLL_EVERY_S
    )
    log = ""
    marker_sms = []
    while time.monotonic() < settlement_deadline:
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
    current_candidates = [
        {
            "id": getattr(message, "id", None),
            "created_at": getattr(message, "created_at", None),
            "targets": sorted(_sms_target_numbers(message)),
            "text": getattr(message, "text", ""),
        }
        for message in _outbound_sms_to_driver()
        if message.id not in before_sms
        and (created_at := _message_created_at(message)) is not None
        and created_at >= sms_watermark
    ]
    assert marker_sms, (
        "hosted completion did not create a current exact-recipient sender-side "
        f"SMS row with the spoken marker; candidates={current_candidates!r}"
    )

    # Give any accidental second attempt time to become visible, then prove the
    # API-accepted side effect occurred exactly once. Carrier delivery is
    # asynchronous and belongs to the SMS delivery lane, not reconciliation.
    assert time.monotonic() + HOSTED_DUPLICATE_GRACE_S <= scenario_deadline, (
        "hosted settlement left no room for the duplicate-detection grace window"
    )
    time.sleep(HOSTED_DUPLICATE_GRACE_S)
    marker_sms = [
        message
        for message in _outbound_sms_to_driver()
        if message.id not in before_sms
        and (created_at := _message_created_at(message)) is not None
        and created_at >= sms_watermark
        and _spoken_key(HOSTED_POST_CALL_MARKER)
        in _spoken_key(getattr(message, "text", None))
    ]
    current_candidates = [
        {
            "id": getattr(message, "id", None),
            "created_at": getattr(message, "created_at", None),
            "targets": sorted(_sms_target_numbers(message)),
            "text": getattr(message, "text", ""),
        }
        for message in _outbound_sms_to_driver()
        if message.id not in before_sms
        and (created_at := _message_created_at(message)) is not None
        and created_at >= sms_watermark
    ]
    assert len(marker_sms) == 1, (
        "hosted post-call processing did not produce exactly one current-marker "
        "SMS to the authoritative caller: "
        f"candidates={current_candidates!r}"
    )
    assert log.count(tool_marker) == 1, (
        "hosted post-call processing recorded duplicate successful SMS side effects"
    )
