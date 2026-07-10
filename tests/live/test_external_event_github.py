"""Live intelligence suite over a GitHub-signed external webhook.

Exercises a real third-party provider end to end: the bridge's ``github``
``WebhookProvider`` verifies ``X-Hub-Signature-256`` (HMAC-SHA256 over the raw
body with ``INKBOX_WEBHOOK_SECRET_GITHUB``). Two events with identical content
— "a GitHub Action failed, call the driver immediately":

  * **forged signature** → rejected at the webhook (401), the agent is never
    woken, and no call is placed;
  * **valid signature** → verified, handed to the agent as an external event,
    and the real model reasons "escalation → call this contact" and *places a
    call* to the driver.

The driver identity is seeded as a contact in the AUT org and parked on
``auto_reject`` — we monitor that the agent dialed, not the call itself.
Skipped unless both keys + the GitHub webhook secret + LIVE_REAL_MODEL=1 +
LIVE_EXTERNAL_EVENTS=1 are set (the secret is minted per run by the
external-events workflow, never committed).
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import time
import urllib.error
import urllib.request
import uuid

import pytest

REMOTE_KEY = os.environ.get("REMOTE_INKBOX_API_KEY")
AUT_KEY = os.environ.get("CLAUDE_CODE_INKBOX_API_KEY")
GITHUB_SECRET = os.environ.get("INKBOX_WEBHOOK_SECRET_GITHUB")
BASE_URL = os.environ.get("INKBOX_BASE_URL", "https://inkbox.ai")
# The bridge's local webhook listener (INKBOX_BRIDGE_PORT defaults to 8767).
WEBHOOK_URL = os.environ.get("AUT_WEBHOOK_URL", "http://127.0.0.1:8767/webhook")
TIMEOUT_S = float(os.environ.get("LIVE_EXTERNAL_TIMEOUT", "300"))
# Second-chance window after a one-shot re-inject (see the valid-signature test).
# Every external event wakes a FRESH Claude Code session (chat_id is
# external:<source>:<event_key>, no resume), so the valid test is a cold-start +
# real-model call on a fixed budget — occasionally slow under CI load. A benign
# re-inject gets one more window before we call it a real failure.
RETRY_WINDOW_S = float(os.environ.get("LIVE_EXTERNAL_RETRY_WINDOW", "180"))
# How long to watch after the forged event to be confident nothing was dialed.
FORGED_QUIET_S = float(os.environ.get("LIVE_FORGED_QUIET", "40"))
POLL_EVERY_S = 6.0

pytestmark = pytest.mark.skipif(
    not (REMOTE_KEY and AUT_KEY and GITHUB_SECRET
         and os.environ.get("LIVE_REAL_MODEL") == "1"
         and os.environ.get("LIVE_EXTERNAL_EVENTS") == "1"),
    reason="github external-event suite: needs both keys + INKBOX_WEBHOOK_SECRET_GITHUB + "
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


def _sign_github(payload: bytes, secret: str) -> str:
    """GitHub's scheme: HMAC-SHA256 over the raw body, ``sha256=<hex>``."""
    return "sha256=" + hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()


def _post_github_event(envelope: dict, *, signature: str) -> tuple[int, str]:
    """POST a GitHub-style webhook with the given ``X-Hub-Signature-256``."""
    payload = json.dumps(envelope).encode()
    req = urllib.request.Request(
        WEBHOOK_URL,
        data=payload,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "User-Agent": "GitHub-Hookshot/live-test",
            "X-GitHub-Event": "workflow_run",
            "X-GitHub-Delivery": str(uuid.uuid4()),
            "X-Inkbox-Request-Id": str(uuid.uuid4()),  # the bridge dedups on this
            "X-Hub-Signature-256": signature,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:  # noqa: S310 — local bridge
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as exc:  # 401 on a forged signature
        return exc.code, exc.read().decode()


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


def _escalation_envelope(driver_name: str) -> dict:
    """A GitHub Actions failure asking the agent to phone the driver contact."""
    run_id = str(uuid.uuid4().int % 10**17)
    return {
        "event": "workflow_run",
        "action": "completed",
        "conclusion": "failure",
        "title": "CI failed on main",
        "severity": "prod",
        "summary": "A GitHub Action failed on the example-app repo; production deploy is blocked.",
        "requested_action": (
            f"Call {driver_name} immediately by phone (use inkbox_place_call) and tell "
            "them a GitHub Action failed and the deploy is blocked. This is urgent — "
            "place the call now."
        ),
        "repository": {"full_name": "example-org/example-app"},
        "workflow_run": {
            "id": run_id,
            "name": "CI",
            "html_url": f"https://github.com/example-org/example-app/actions/runs/{run_id}",
        },
    }


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


def test_forged_github_signature_is_dropped_and_agent_does_nothing(ctx):
    """A forged X-Hub-Signature-256 → 401 at the webhook, agent never dials."""
    aut, driver_phone = ctx["aut"], ctx["driver_phone"]
    before = {c.id for c in _outbound_calls_to(aut, driver_phone)}

    status, body = _post_github_event(_escalation_envelope(ctx["driver_name"]), signature="sha256=deadbeef")
    assert status == 401, f"forged signature should be rejected, got {status} {body!r}"

    # Watch briefly: a rejected event must not produce any call to the driver.
    deadline = time.monotonic() + FORGED_QUIET_S
    while time.monotonic() < deadline:
        fresh = [c for c in _outbound_calls_to(aut, driver_phone) if c.id not in before]
        assert not fresh, f"agent dialed on a FORGED event: {fresh}"
        time.sleep(POLL_EVERY_S)


def _post_valid_escalation(driver_name: str) -> None:
    """POST a validly-signed GitHub escalation; assert the bridge accepted it."""
    envelope = _escalation_envelope(driver_name)
    payload = json.dumps(envelope).encode()
    status, body = _post_github_event(envelope, signature=_sign_github(payload, GITHUB_SECRET))
    assert status == 200 and json.loads(body).get("ok") is True, \
        f"valid webhook not accepted: {status} {body!r}"


def _wait_for_call(aut, driver_phone: str, before: set, timeout_s: float) -> bool:
    """Poll for a new outbound call to the driver; True once one appears."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if [c for c in _outbound_calls_to(aut, driver_phone) if c.id not in before]:
            return True
        time.sleep(POLL_EVERY_S)
    return False


def test_valid_github_signature_makes_agent_call_driver(ctx):
    """A validly-signed GitHub failure → the agent places a call to the driver."""
    aut, driver_phone, driver_name = ctx["aut"], ctx["driver_phone"], ctx["driver_name"]
    before = {c.id for c in _outbound_calls_to(aut, driver_phone)}

    _post_valid_escalation(driver_name)
    if _wait_for_call(aut, driver_phone, before, TIMEOUT_S):
        return  # the agent escalated by phoning the driver — exactly what we monitor for

    # First window lapsed with no call. The event woke a cold-start fresh session
    # and a real-model call can occasionally run long (or the model hesitates) —
    # so re-inject once (a fresh event id → a fresh session) and give it one more
    # window before declaring a real failure. The forged-event test proves we
    # never dial spuriously, so a benign re-send is safe.
    _post_valid_escalation(driver_name)
    if _wait_for_call(aut, driver_phone, before, RETRY_WINDOW_S):
        return
    pytest.fail(
        f"agent never called {driver_name} within {TIMEOUT_S:.0f}s + a "
        f"{RETRY_WINDOW_S:.0f}s re-inject window"
    )
