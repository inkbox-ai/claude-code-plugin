"""Live intelligence suite over an external webhook — the agent's REAL brain.

Proves the catch-all external-event path works end to end against a real model:
a signed escalation webhook lands on the bridge's ``/webhook`` asking it to
phone a specific contact — the driver — and we verify the agent actually
*places that call* to the driver's number. The driver sits on ``auto_reject``:
we only care that the agent reasoned "escalation → call this contact" and
dialed; we never handle the call itself.

Trigger path mirrors a real forwarded webhook: HMAC-signed with the AUT signing
key (``inkbox.verify_webhook`` scheme) and POSTed straight at the bridge's local
listener. No tunnel needed — the test runs on the same host as the bridge.

Skipped unless both keys + the signing key + LIVE_REAL_MODEL=1 are set, and the
suite is explicitly selected with LIVE_EXTERNAL_EVENTS=1 (the channels workflow
runs the whole tests/live dir with a bridge that has external events OFF).
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import time
import urllib.request
import uuid

import pytest

REMOTE_KEY = os.environ.get("REMOTE_INKBOX_API_KEY")
AUT_KEY = os.environ.get("CLAUDE_CODE_INKBOX_API_KEY")
SIGNING_KEY = os.environ.get("CLAUDE_CODE_INKBOX_SIGNING_KEY") or os.environ.get("INKBOX_SIGNING_KEY")
BASE_URL = os.environ.get("INKBOX_BASE_URL", "https://inkbox.ai")
# The bridge's local webhook listener (INKBOX_BRIDGE_PORT defaults to 8767).
WEBHOOK_URL = os.environ.get("AUT_WEBHOOK_URL", "http://127.0.0.1:8767/webhook")
TIMEOUT_S = float(os.environ.get("LIVE_EXTERNAL_TIMEOUT", "200"))
POLL_EVERY_S = 6.0

pytestmark = pytest.mark.skipif(
    not (REMOTE_KEY and AUT_KEY and SIGNING_KEY
         and os.environ.get("LIVE_REAL_MODEL") == "1"
         and os.environ.get("LIVE_EXTERNAL_EVENTS") == "1"),
    reason="external-event intelligence suite: needs both keys + signing key + "
           "LIVE_REAL_MODEL=1 + LIVE_EXTERNAL_EVENTS=1",
)


def _digits(s: str) -> str:
    return re.sub(r"\D", "", s or "")


def _client(key):
    from inkbox import Inkbox

    return Inkbox(api_key=key, base_url=BASE_URL)


def _first_phone(client):
    nums = client.phone_numbers.list()
    assert nums, "identity has no phone number"
    return nums[0]


def _sign(payload: bytes, *, request_id: str, timestamp: str, secret: str) -> str:
    """Reproduce Inkbox's webhook HMAC over ``{request_id}.{timestamp}.`` + body."""
    key = secret.removeprefix("whsec_")
    message = f"{request_id}.{timestamp}.".encode() + payload
    return "sha256=" + hmac.new(key.encode(), message, hashlib.sha256).hexdigest()


def _post_external_event(envelope: dict) -> tuple[int, str]:
    """Sign and POST an external event to the bridge's webhook, as a forwarder would."""
    payload = json.dumps(envelope).encode()
    request_id = str(uuid.uuid4())
    timestamp = str(int(time.time()))
    req = urllib.request.Request(
        WEBHOOK_URL,
        data=payload,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "X-Inkbox-Request-Id": request_id,
            "X-Inkbox-Timestamp": timestamp,
            "X-Inkbox-Signature": _sign(payload, request_id=request_id, timestamp=timestamp, secret=SIGNING_KEY),
        },
    )
    with urllib.request.urlopen(req, timeout=15) as resp:  # noqa: S310 — local bridge
        return resp.status, resp.read().decode()


def _ensure_driver_contact(aut, driver_phone: str) -> str:
    """Return the driver's contact name in the AUT org, seeding the card if absent."""
    matches = aut.contacts.lookup(phone=driver_phone)
    if matches:
        c = matches[0]
        return (getattr(c, "preferred_name", None) or getattr(c, "given_name", None)
                or getattr(c, "family_name", None) or "the driver")
    from inkbox.contacts.types import ContactPhone

    aut.contacts.create(
        given_name="Oncall",
        family_name="Driver",
        phones=[ContactPhone("mobile", driver_phone)],
    )
    return "Oncall Driver"


def _outbound_calls_to(aut, driver_phone: str) -> list:
    """AUT's outbound calls dialed to the driver's number (newest first)."""
    tail = _digits(driver_phone)[-10:]
    return [
        c for c in aut.calls.list(limit=30)
        if (getattr(c, "direction", "") or "").lower() == "outbound"
        and _digits(getattr(c, "remote_phone_number", "") or "")[-10:] == tail
    ]


@pytest.fixture(scope="module")
def ctx():
    remote, aut = _client(REMOTE_KEY), _client(AUT_KEY)
    driver_num = _first_phone(remote)

    # Driver auto-rejects: the call rings and drops — we never handle media.
    prev_action = getattr(driver_num, "incoming_call_action", None)
    remote.phone_numbers.update(driver_num.id, incoming_call_action="auto_reject")

    driver_name = _ensure_driver_contact(aut, driver_num.number)
    try:
        yield {"aut": aut, "driver_phone": driver_num.number, "driver_name": driver_name}
    finally:
        # Leave the driver number as we found it for other suites.
        try:
            remote.phone_numbers.update(driver_num.id, incoming_call_action=prev_action or "auto_reject")
        except Exception:
            pass


def test_external_escalation_makes_agent_call_driver(ctx):
    """A signed escalation webhook → the agent places a call to the driver contact."""
    aut = ctx["aut"]
    driver_phone = ctx["driver_phone"]
    driver_name = ctx["driver_name"]

    before = {c.id for c in _outbound_calls_to(aut, driver_phone)}

    run_id = str(uuid.uuid4().int % 10**17)
    envelope = {
        "event": "agent_escalation",
        "title": "Prod deploy failed",
        "severity": "prod",
        "summary": "The example-app deploy failed on main; production is down.",
        "requested_action": (
            f"Call {driver_name} immediately by phone (use inkbox_place_call) and "
            "tell them production is down. This is urgent — place the call now."
        ),
        "github": {
            "repository": "example-org/example-app",
            "workflow": "Deploy",
            "run_id": run_id,
            "run_url": f"https://github.com/example-org/example-app/actions/runs/{run_id}",
        },
    }

    status, body = _post_external_event(envelope)
    assert status == 200 and json.loads(body).get("ok") is True, \
        f"webhook not accepted: {status} {body!r}"

    # Wait for the agent to actually dial the driver's number.
    deadline = time.monotonic() + TIMEOUT_S
    while time.monotonic() < deadline:
        fresh = [c for c in _outbound_calls_to(aut, driver_phone) if c.id not in before]
        if fresh:
            return  # the agent escalated by phoning the driver — exactly what we monitor for
        time.sleep(POLL_EVERY_S)
    pytest.fail(f"agent never called {driver_name} within {TIMEOUT_S:.0f}s")
