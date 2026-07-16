"""Live end-to-end coverage for an Inkbox-signed external event.

The bridge accepts a correctly signed, non-Inkbox payload, creates the expected
external-event session, and completes a turn against the real model.  Outbound
call behavior is deliberately covered by ``test_voice.py`` instead: instructions
inside webhook payloads are data, and requiring a model to obey a synthetic
``requested_action`` field makes this provider-boundary test nondeterministic.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
import urllib.request
import uuid

import pytest

SIGNING_KEY = os.environ.get("CLAUDE_CODE_INKBOX_SIGNING_KEY") or os.environ.get(
    "INKBOX_SIGNING_KEY"
)
WEBHOOK_URL = os.environ.get("AUT_WEBHOOK_URL", "http://127.0.0.1:8767/webhook")
GATEWAY_LOG = os.environ.get("GATEWAY_LOG", "")
TIMEOUT_S = float(os.environ.get("LIVE_EXTERNAL_TIMEOUT", "200"))
POLL_EVERY_S = 0.5

pytestmark = pytest.mark.skipif(
    not (
        SIGNING_KEY
        and GATEWAY_LOG
        and os.environ.get("LIVE_REAL_MODEL") == "1"
        and os.environ.get("LIVE_EXTERNAL_EVENTS") == "1"
    ),
    reason="external-event intelligence suite: needs signing key + gateway log + "
    "LIVE_REAL_MODEL=1 + LIVE_EXTERNAL_EVENTS=1",
)


def _sign(payload: bytes, *, request_id: str, timestamp: str, secret: str) -> str:
    """Reproduce Inkbox's webhook HMAC over ``{request_id}.{timestamp}.`` + body."""
    key = secret.removeprefix("whsec_")
    message = f"{request_id}.{timestamp}.".encode() + payload
    return "sha256=" + hmac.new(key.encode(), message, hashlib.sha256).hexdigest()


def _post_external_event(envelope: dict) -> tuple[int, str]:
    """Sign and POST an external event as the registered Inkbox provider."""
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
            "X-Inkbox-Signature": _sign(
                payload,
                request_id=request_id,
                timestamp=timestamp,
                secret=SIGNING_KEY,
            ),
        },
    )
    with urllib.request.urlopen(req, timeout=15) as resp:  # noqa: S310 -- local bridge
        return resp.status, resp.read().decode()


def _gateway_log() -> str:
    try:
        with open(GATEWAY_LOG, encoding="utf-8") as handle:
            return handle.read()
    except FileNotFoundError:
        return ""


def _wait_for_log(marker: str) -> bool:
    deadline = time.monotonic() + TIMEOUT_S
    while time.monotonic() < deadline:
        if marker in _gateway_log():
            return True
        time.sleep(POLL_EVERY_S)
    return False


def test_signed_external_event_completes_agent_turn():
    """A verified external event reaches and completes a real-model turn."""
    event_id = str(uuid.uuid4().int % 10**17)
    envelope = {
        "id": event_id,
        "source": "live-e2e",
        "event": "deployment_completed",
        "title": "Live external-event delivery probe",
        "summary": "The synthetic deployment completed successfully.",
        "severity": "informational",
    }
    marker = f"[bridge] external-event turn done: external:live-e2e:{event_id}"

    status, body = _post_external_event(envelope)
    assert status == 200 and json.loads(body).get("ok") is True, \
        f"webhook not accepted: {status} {body!r}"
    assert _wait_for_log(marker), \
        f"signed external event never completed the expected agent turn: {marker}"
