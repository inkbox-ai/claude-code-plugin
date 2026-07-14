"""Live end-to-end coverage for GitHub-signed external webhooks.

This suite exercises the third-party provider boundary with a realistic
``workflow_run`` payload:

* a forged ``X-Hub-Signature-256`` is rejected before the agent wakes;
* a valid signature is accepted and starts an external-event agent session.

GitHub's signature authenticates delivery from GitHub; it does not turn
arbitrary, non-schema payload fields into operator instructions. Actual model
reasoning and outward call placement are covered separately by
``test_external_event_intelligence.py`` using the gateway's explicit signed
escalation schema.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
import urllib.error
import urllib.request
import uuid

import pytest

GITHUB_SECRET = os.environ.get("INKBOX_WEBHOOK_SECRET_GITHUB")
WEBHOOK_URL = os.environ.get("AUT_WEBHOOK_URL", "http://127.0.0.1:8767/webhook")
GATEWAY_LOG = os.environ.get("GATEWAY_LOG", "")
SESSION_START_TIMEOUT_S = float(os.environ.get("LIVE_GITHUB_SESSION_TIMEOUT", "45"))
POLL_EVERY_S = 0.5

pytestmark = pytest.mark.skipif(
    not (
        GITHUB_SECRET
        and GATEWAY_LOG
        and os.environ.get("LIVE_REAL_MODEL") == "1"
        and os.environ.get("LIVE_EXTERNAL_EVENTS") == "1"
    ),
    reason="github external-event suite: needs INKBOX_WEBHOOK_SECRET_GITHUB + "
    "GATEWAY_LOG + LIVE_REAL_MODEL=1 + LIVE_EXTERNAL_EVENTS=1",
)


def _sign_github(payload: bytes, secret: str) -> str:
    """Return GitHub's HMAC-SHA256 signature over the exact request body."""
    return "sha256=" + hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()


def _post_github_event(
    envelope: dict,
    *,
    secret: str | None = None,
    signature: str | None = None,
) -> tuple[int, str]:
    """POST a GitHub-style webhook with either a computed or explicit signature."""
    payload = json.dumps(envelope).encode()
    if secret is not None:
        signature = _sign_github(payload, secret)
    assert signature is not None

    req = urllib.request.Request(
        WEBHOOK_URL,
        data=payload,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "User-Agent": "GitHub-Hookshot/live-test",
            "X-GitHub-Event": "workflow_run",
            "X-GitHub-Delivery": str(uuid.uuid4()),
            "X-Inkbox-Request-Id": str(uuid.uuid4()),
            "X-Hub-Signature-256": signature,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:  # noqa: S310 — local bridge
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode()


def _workflow_run_envelope() -> dict:
    """Build the subset of a real GitHub ``workflow_run`` payload we consume."""
    repository = os.environ.get("GITHUB_REPOSITORY", "inkbox-ai/claude-code-plugin")
    server_url = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    run_id = str(uuid.uuid4().int % 10**17)
    return {
        "action": "completed",
        "workflow_run": {
            "id": run_id,
            "name": "CI",
            "event": "pull_request",
            "status": "completed",
            "conclusion": "failure",
            "head_branch": "main",
            "html_url": f"{server_url}/{repository}/actions/runs/{run_id}",
        },
        "repository": {
            "name": repository.rsplit("/", 1)[-1],
            "full_name": repository,
            "html_url": f"{server_url}/{repository}",
        },
    }


def _session_marker(envelope: dict) -> str:
    repository = envelope["repository"]["full_name"]
    run_id = envelope["workflow_run"]["id"]
    return f"[session external:{repository}:{run_id}] Claude Code session started"


def _gateway_log() -> str:
    try:
        with open(GATEWAY_LOG, encoding="utf-8") as handle:
            return handle.read()
    except FileNotFoundError:
        return ""


def _wait_for_log(marker: str, timeout_s: float) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if marker in _gateway_log():
            return True
        time.sleep(POLL_EVERY_S)
    return False


def test_forged_github_signature_is_rejected_before_agent_wakes():
    """A forged signature returns 401 and never creates an agent session."""
    envelope = _workflow_run_envelope()
    marker = _session_marker(envelope)

    status, body = _post_github_event(envelope, signature="sha256=deadbeef")
    assert status == 401, f"forged signature should be rejected, got {status} {body!r}"
    time.sleep(2)
    assert marker not in _gateway_log(), "forged GitHub event unexpectedly woke an agent session"


def test_valid_github_signature_wakes_agent_session():
    """A valid signature is accepted and reaches the real agent session layer."""
    envelope = _workflow_run_envelope()
    marker = _session_marker(envelope)

    status, body = _post_github_event(envelope, secret=GITHUB_SECRET)
    assert status == 200 and json.loads(body).get("ok") is True, \
        f"valid webhook not accepted: {status} {body!r}"
    assert _wait_for_log(marker, SESSION_START_TIMEOUT_S), \
        f"valid GitHub webhook never started the expected session: {marker}"
