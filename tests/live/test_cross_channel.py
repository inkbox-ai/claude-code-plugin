"""Live cross-channel suite — the agent answers on a DIFFERENT channel.

Ask on one channel; the agent must figure out the sender's *other-channel* address
from the contact card and respond there. Each request carries a short token, and we
assert that token shows up on the other channel — proving the response is tied to
the request.

  * email -> SMS : email asks for a text; we poll SMS for the token.
  * SMS  -> email: SMS asks for an email; we poll email for the token.

Voice is the odd one out: an unanswered call carries no token, so instead of
matching content we assert that a *new inbound call from the AUT's number* lands
on the driver's number within the window — proof the request reasoned its way to
``inkbox_place_call`` and Inkbox actually dialed the driver.

  * email -> call: email asks the agent to call; we poll the driver's calls.
  * SMS   -> call: SMS asks the agent to call; we poll the driver's calls.

Each call leg first resets the AUT's session (see ``_reset_aut_session``) so it
starts from a clean conversation — otherwise a placed call lingers in context and
the next "call me" is answered with a text instead of a fresh dial.

More channels (iMessage) get added here. Real-model only.
"""

from __future__ import annotations

import os
import re
import time
import uuid
from datetime import UTC, datetime

import pytest

REMOTE_KEY = os.environ.get("REMOTE_INKBOX_API_KEY")
AUT_KEY = os.environ.get("CLAUDE_CODE_INKBOX_API_KEY")
BASE_URL = os.environ.get("INKBOX_BASE_URL", "https://inkbox.ai")
REAL = os.environ.get("LIVE_REAL_MODEL") == "1"
TIMEOUT_S = float(os.environ.get("LIVE_XCHANNEL_TIMEOUT", "200"))
POLL_EVERY_S = 6.0
CALL_PAIR_MAX_S = 60.0
CALL_DUPLICATE_GRACE_S = 2 * POLL_EVERY_S

pytestmark = pytest.mark.skipif(
    not (REMOTE_KEY and AUT_KEY and REAL),
    reason="cross-channel suite: needs both keys + LIVE_REAL_MODEL=1",
)


def _digits(s: str) -> str:
    return re.sub(r"\D", "", s or "")


def _voicemail_detection_value(call) -> str:
    value = getattr(call, "voicemail_detection", "")
    return str(getattr(value, "value", value))


def _record_created_at(record):
    value = getattr(record, "created_at", None)
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _client(key):
    from inkbox import Inkbox

    return Inkbox(api_key=key, base_url=BASE_URL)


def _token() -> str:
    return uuid.uuid4().hex[:6]


@pytest.fixture(scope="module")
def xc():
    remote = _client(REMOTE_KEY)
    aut = _client(AUT_KEY)
    remote_email = remote.mailboxes.list()[0].email_address
    aut_email = aut.mailboxes.list()[0].email_address
    rnums = remote.phone_numbers.list()
    anums = aut.phone_numbers.list()
    have_both_numbers = bool(rnums and anums)
    assert have_both_numbers, "both identities need a phone number for cross-channel"
    remote_number = rnums[0]
    remote_phone, remote_pid = remote_number.number, str(remote_number.id)
    aut_phone = anums[0].number

    # The agent can only cross channels if the sender's card has BOTH an email and a
    # phone. Ensure it does (merge in whatever is missing; never clobber existing data).
    from inkbox.contacts.types import ContactEmail, ContactPhone
    matches = aut.contacts.lookup(email=remote_email)
    if not matches:
        aut.contacts.create(
            given_name="Penny", family_name="Tester",
            emails=[ContactEmail("work", remote_email)],
            phones=[ContactPhone("mobile", remote_phone)],
        )
    else:
        c = matches[0]
        emails = list(getattr(c, "emails", []))
        phones = list(getattr(c, "phones", []))
        changed = False
        if not any((e.value or "").lower() == remote_email.lower() for e in emails):
            emails.append(ContactEmail("work", remote_email))
            changed = True
        if not any(_digits(p.value)[-10:] == _digits(remote_phone)[-10:] for p in phones):
            phones.append(ContactPhone("mobile", remote_phone))
            changed = True
        if changed:
            aut.contacts.update(c.id, emails=emails, phones=phones)

    # These tests only need proof that the AUT attempted a fresh dial. Keep the
    # driver parked on auto-reject so each probe terminates immediately instead
    # of occupying both identities until the server's ten-minute duration cap.
    # Re-assert the known idle baseline during teardown as well: preserving a
    # stale auto-accept value here contaminates the voice suite that runs next.
    remote.phone_numbers.update(remote_pid, incoming_call_action="auto_reject")
    try:
        yield {
            "remote": remote, "aut": aut,
            "remote_email": remote_email, "remote_phone": remote_phone,
            "remote_pid": remote_pid,
            "aut_email": aut_email, "aut_phone": aut_phone,
        }
    finally:
        remote.phone_numbers.update(remote_pid, incoming_call_action="auto_reject")


def test_email_request_gets_sms_response(xc):
    """Email asks the agent to TEXT a code; the code must arrive over SMS."""
    remote, remote_pid, aut_phone = xc["remote"], xc["remote_pid"], xc["aut_phone"]
    token = _token()
    tail = _digits(aut_phone)[-10:]

    def _sms_from_aut():
        return [m for m in remote.texts.list(remote_pid, limit=30)
                if (getattr(m, "direction", "") or "").lower() == "inbound"
                and _digits(getattr(m, "remote_phone_number", "") or "")[-10:] == tail]

    before = {m.id for m in _sms_from_aut()}
    remote.messages.send(
        xc["remote_email"], to=[xc["aut_email"]], subject=f"[{token}] text me please",
        body_text=f"Please send me a text message (SMS) that says: lalala {token}",
    )

    deadline = time.monotonic() + TIMEOUT_S
    while time.monotonic() < deadline:
        for m in _sms_from_aut():
            if m.id not in before and token in (getattr(m, "text", "") or "").lower():
                return  # cross-channel confirmed: email request -> SMS response with the token
        time.sleep(POLL_EVERY_S)
    pytest.fail(
        f"agent did not send an SMS with the current marker within {TIMEOUT_S:.0f}s"
    )


def test_sms_request_gets_email_response(xc):
    """SMS asks the agent to EMAIL a code; the code must arrive over email."""
    from inkbox.mail.types import MessageDirection

    remote, remote_email, aut_email = xc["remote"], xc["remote_email"], xc["aut_email"]
    token = _token()

    def _email_from_aut():
        return [m for m in remote.messages.list(remote_email, direction=MessageDirection.INBOUND)
                if aut_email.lower() in (getattr(m, "from_address", "") or "").lower()]

    before = {m.id for m in _email_from_aut()}
    remote.texts.send(xc["remote_pid"], to=xc["aut_phone"], text=f"Please email me the code {token}.")

    deadline = time.monotonic() + TIMEOUT_S
    while time.monotonic() < deadline:
        for m in _email_from_aut():
            if m.id in before:
                continue
            hay = (getattr(m, "subject", "") or "").lower()
            if token not in hay:
                body = getattr(remote.messages.get(remote_email, m.id), "body_text", "") or ""
                hay = body.lower()
            if token in hay:
                return  # cross-channel confirmed: SMS request -> email response with the token
        time.sleep(POLL_EVERY_S)
    pytest.fail(
        f"agent did not send an email with the current marker within {TIMEOUT_S:.0f}s"
    )


def _inbound_calls_from_aut(remote, remote_pid: str, aut_phone: str):
    """The driver's inbound calls originating from the AUT's number."""
    tail = _digits(aut_phone)[-10:]
    return [c for c in remote.calls.list(limit=30)
            if (getattr(c, "direction", "") or "").lower() == "inbound"
            and _digits(getattr(c, "remote_phone_number", "") or "")[-10:] == tail]


def _outbound_calls_to_remote(aut, remote_phone: str):
    """The AUT's outbound records targeting the driver's number."""
    tail = _digits(remote_phone)[-10:]
    return [c for c in aut.calls.list(limit=30)
            if (getattr(c, "direction", "") or "").lower() == "outbound"
            and _digits(getattr(c, "remote_phone_number", "") or "")[-10:] == tail]


def _fresh_calls(records, before: set, watermark: datetime):
    return [
        record for record in records
        if record.id not in before
        and (created_at := _record_created_at(record)) is not None
        and created_at >= watermark
    ]


def _wait_for_new_call_pair(
    remote,
    aut,
    *,
    remote_pid: str,
    remote_phone: str,
    aut_phone: str,
    before_driver: set,
    before_aut: set,
    driver_watermark: datetime,
    aut_watermark: datetime,
):
    """Correlate one fresh driver inbound leg with one fresh AUT outbound leg."""
    deadline = time.monotonic() + TIMEOUT_S
    while time.monotonic() < deadline:
        driver_calls = _fresh_calls(
            _inbound_calls_from_aut(remote, remote_pid, aut_phone),
            before_driver,
            driver_watermark,
        )
        aut_calls = _fresh_calls(
            _outbound_calls_to_remote(aut, remote_phone),
            before_aut,
            aut_watermark,
        )
        assert len(driver_calls) <= 1, (
            f"duplicate driver call records (count={len(driver_calls)})"
        )
        assert len(aut_calls) <= 1, (
            f"duplicate AUT call records (count={len(aut_calls)})"
        )
        if driver_calls and aut_calls:
            driver_created = _record_created_at(driver_calls[0])
            aut_created = _record_created_at(aut_calls[0])
            assert driver_created is not None and aut_created is not None
            assert abs((driver_created - aut_created).total_seconds()) <= CALL_PAIR_MAX_S
            break
        time.sleep(POLL_EVERY_S)
    else:
        pytest.fail(
            f"agent did not persist both call legs within {TIMEOUT_S:.0f}s"
        )

    time.sleep(CALL_DUPLICATE_GRACE_S)
    driver_calls = _fresh_calls(
        _inbound_calls_from_aut(remote, remote_pid, aut_phone),
        before_driver,
        driver_watermark,
    )
    aut_calls = _fresh_calls(
        _outbound_calls_to_remote(aut, remote_phone),
        before_aut,
        aut_watermark,
    )
    assert len(driver_calls) == 1, (
        f"expected one fresh driver record (count={len(driver_calls)})"
    )
    assert len(aut_calls) == 1, (
        f"expected one fresh AUT record (count={len(aut_calls)})"
    )
    return aut_calls[0]


def _call_pair_snapshot(remote, aut, remote_pid, remote_phone, aut_phone):
    driver_calls = _inbound_calls_from_aut(remote, remote_pid, aut_phone)
    aut_calls = _outbound_calls_to_remote(aut, remote_phone)
    driver_times = [
        value for call in driver_calls
        if (value := _record_created_at(call)) is not None
    ]
    aut_times = [
        value for call in aut_calls
        if (value := _record_created_at(call)) is not None
    ]
    return {
        "before_driver": {call.id for call in driver_calls},
        "before_aut": {call.id for call in aut_calls},
        "driver_watermark": max(
            driver_times, default=datetime.min.replace(tzinfo=UTC)
        ),
        "aut_watermark": max(
            aut_times, default=datetime.min.replace(tzinfo=UTC)
        ),
    }


# The driver auto-rejects these call probes so they cannot leak into the voice
# suite. The AUT still keys ONE session per contact across channels, however, so
# back-to-back call legs share context: after the first dial the agent reasons
# "I already called this person" and answers the next "call me" with a text
# instead of dialing again. Resetting the session before each leg (the bridge's
# ``/clear`` control command, intercepted locally and never sent to Claude)
# drops that context so every ask lands in a fresh conversation.
RESET_SETTLE_S = 5.0


def _reset_aut_session(remote, remote_pid: str, aut_phone: str) -> None:
    """Reset the AUT's session for the driver contact via the ``/clear`` control SMS."""
    remote.texts.send(remote_pid, to=aut_phone, text="/clear")
    time.sleep(RESET_SETTLE_S)  # let the bridge intercept + reset before the real ask


def test_email_request_gets_call(xc):
    """Email asks the agent to CALL; a new inbound call must land on the driver."""
    remote, aut = xc["remote"], xc["aut"]
    remote_pid, remote_phone = xc["remote_pid"], xc["remote_phone"]
    aut_phone = xc["aut_phone"]
    _reset_aut_session(remote, remote_pid, aut_phone)  # fresh session, no prior-call context
    snapshot = _call_pair_snapshot(remote, aut, remote_pid, remote_phone, aut_phone)
    remote.messages.send(
        xc["remote_email"], to=[xc["aut_email"]], subject="please call me",
        body_text=(
            "Please place a phone call to my number now with "
            "voicemail_detection disabled — I'd rather talk than type."
        ),
    )
    call = _wait_for_new_call_pair(
        remote, aut,
        remote_pid=remote_pid, remote_phone=remote_phone, aut_phone=aut_phone,
        **snapshot,
    )
    assert _voicemail_detection_value(call) == "disabled"


def test_sms_request_gets_call(xc):
    """SMS asks the agent to CALL; a new inbound call must land on the driver."""
    remote, aut = xc["remote"], xc["aut"]
    remote_pid, remote_phone = xc["remote_pid"], xc["remote_phone"]
    aut_phone = xc["aut_phone"]
    _reset_aut_session(remote, remote_pid, aut_phone)  # fresh session, no prior-call context
    snapshot = _call_pair_snapshot(remote, aut, remote_pid, remote_phone, aut_phone)
    # Be explicit that we want an actual dial — a terse "call me" lets the model
    # answer by text; the email leg's wording is unambiguous, so match it here.
    # Fresh body each send: the agent replies by calling, not texting, so this
    # SMS never gets an SMS reply to reset the conversation cadence — two
    # identical no-reply sends would trip the duplicate_body rule (422).
    remote.texts.send(
        remote_pid, to=aut_phone,
        text=(
            "Please place a phone call to my number right now with "
            "voicemail_detection disabled — actually call me, don't text back. "
            f"(ref {_token()})"
        ),
    )
    call = _wait_for_new_call_pair(
        remote, aut,
        remote_pid=remote_pid, remote_phone=remote_phone, aut_phone=aut_phone,
        **snapshot,
    )
    assert _voicemail_detection_value(call) == "disabled"
