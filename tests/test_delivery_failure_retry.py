"""Outbound delivery-failure feedback loop.

Covers both failure surfaces on every channel:
  - synchronous send rejections (server content policy 422, opt-out 402,
    email send errors, local too-long guards) → agent woken with the error;
  - asynchronous delivery-failure webhooks (text.delivery_failed,
    imessage.delivery_failed, message.bounced / message.failed) → same.

And the budget mechanics: max OUTBOUND_FAILURE_MAX_ATTEMPTS sends per logical
reply shared across both surfaces, reset on inbound / delivered / TTL,
replay-deduped webhooks. The wake-up is a run_consult side-turn (the agent
acts via its Inkbox tools), so evidence is the captured consult prompt.
"""

import asyncio
import json
import time
import types

import pytest

from inkbox_claude import gateway
from inkbox_claude.config import BridgeConfig


MAX = gateway.OUTBOUND_FAILURE_MAX_ATTEMPTS


@pytest.fixture(autouse=True)
def fake_web(monkeypatch):
    """aiohttp isn't installed in tests; stub the json_response the handlers use."""
    def json_response(payload):
        return types.SimpleNamespace(text=json.dumps(payload), payload=payload)
    monkeypatch.setattr(gateway, "web", types.SimpleNamespace(json_response=json_response))


# ── Test doubles ────────────────────────────────────────────────────────


class _FakeSession:
    def __init__(self):
        self.consulted = []

    async def run_consult(self, prompt):
        self.consulted.append(prompt)
        return ""


class _FakeSessions:
    def __init__(self):
        self.by_id = {}

    def get(self, chat_id, system_prompt_extra=""):
        return self.by_id.setdefault(chat_id, _FakeSession())


class SpamBlockError(Exception):
    """Shaped like the SDK error for the server's content-policy 422."""

    status_code = 422
    detail = {
        "error": "message_blocked_spam_filter",
        "rule": "markdown_artifacts",
        "text_message_id": "txt-blocked",
        "message": "Markdown formatting (headers/bold/code fences) reads as bot traffic in SMS.",
    }


class TransientError(Exception):
    """Shaped like a 503 a bare resend would clear on its own."""

    status_code = 503
    detail = {"error": "carrier_unavailable", "message": "upstream temporarily unavailable"}


class OptOutError(Exception):
    """Shaped like the iMessage-line 402 for an opted-out recipient."""

    status_code = 402
    detail = {
        "error": "recipient_opted_out",
        "message": "Recipient has opted out of messages from this line.",
    }


def _gw():
    gw = gateway.InkboxGateway(BridgeConfig(require_signature=False))
    gw.sessions = _FakeSessions()
    return gw


async def _drain():
    # Let the background _run_failure_turn task (run_consult) finish.
    await asyncio.sleep(0)
    await asyncio.sleep(0)


def _run(gw, coro):
    async def go():
        result = await coro
        await _drain()
        return result
    return asyncio.run(go())


def _consults(gw, chat_id):
    session = gw.sessions.by_id.get(chat_id)
    return session.consulted if session else []


# Standard SMS routing facts a sync rejection carries.
_SMS_META = {"to": "+15555550101", "conversation_id": "conv-123"}


def _delivery_failed_envelope(text_id="txt-out-1", conversation_id="conv-123"):
    return {
        "id": f"evt-{text_id}",
        "event_type": "text.delivery_failed",
        "data": {
            "text_message": {
                "id": text_id,
                "direction": "outbound",
                "local_phone_number": "+15555550100",
                "remote_phone_number": "+15555550101",
                "conversation_id": conversation_id,
                "text": "Sorry Kim — the site isn't built yet.",
                "delivery_status": "delivery_failed",
                "error_code": "40002",
                "error_detail": (
                    "The message was flagged by a SPAM filter and was not "
                    "delivered. This is a temporary condition."
                ),
            },
            "contacts": [{"id": "contact-123"}],
        },
    }


# ── Synchronous send rejections ─────────────────────────────────────────


def test_sms_spam_block_wakes_agent_with_rule():
    gw = _gw()
    _run(gw, gw._note_send_rejection(
        "contact-123", "sms", _SMS_META, "**Jane Doe** is on file.", SpamBlockError(),
    ))

    prompts = _consults(gw, "contact-123")
    assert len(prompts) == 1
    text = prompts[0]
    assert "channel=sms stage=send_rejected" in text
    assert f"attempt=1/{MAX}" in text
    assert "message_blocked_spam_filter rule=markdown_artifacts" in text
    assert "reads as bot traffic in SMS" in text
    assert "**Jane Doe** is on file." in text


def test_sms_retry_budget_caps_total_sends():
    gw = _gw()
    for _ in range(MAX + 1):
        _run(gw, gw._note_send_rejection(
            "contact-123", "sms", _SMS_META, "blocked body", SpamBlockError(),
        ))

    # Failures 1 and 2 wake the agent; failures 3+ stay quiet.
    prompts = _consults(gw, "contact-123")
    assert len(prompts) == MAX - 1
    assert f"attempt=1/{MAX}" in prompts[0]
    assert f"attempt=2/{MAX}" in prompts[1]


def test_transient_sms_error_does_not_wake_agent():
    gw = _gw()
    _run(gw, gw._note_send_rejection(
        "contact-123", "sms", _SMS_META, "hi", TransientError(),
    ))

    assert _consults(gw, "contact-123") == []
    assert gw._outbound_failure_state == {}
    assert gateway.InkboxGateway._send_is_retryable(TransientError()) is True


def test_non_retryable_status_is_not_transient():
    # A 4xx (content policy / opt-out / rate-limit) is the agent's to fix.
    assert gateway.InkboxGateway._send_is_retryable(SpamBlockError()) is False
    assert gateway.InkboxGateway._send_is_retryable(OptOutError()) is False


def test_sms_too_long_wakes_agent():
    gw = _gw()
    too_long = ValueError(gateway._message_too_long_reason("SMS", "x", gateway.SMS_MAX_LENGTH))
    _run(gw, gw._note_send_rejection("contact-123", "sms", _SMS_META, "x" * 5, too_long))

    prompts = _consults(gw, "contact-123")
    assert len(prompts) == 1
    assert "channel=sms stage=send_rejected" in prompts[0]
    assert "sms_too_long" in prompts[0]


def test_imessage_opt_out_wakes_agent():
    gw = _gw()
    meta = {"to": "+15555550101", "conversation_id": "imsg-conv-1"}
    _run(gw, gw._note_send_rejection("contact-123", "imessage", meta, "hello again", OptOutError()))

    prompts = _consults(gw, "contact-123")
    assert len(prompts) == 1
    assert "channel=imessage stage=send_rejected" in prompts[0]
    assert "recipient_opted_out" in prompts[0]
    assert "opted out" in prompts[0]


def test_email_send_failure_wakes_agent():
    gw = _gw()
    meta = {"to": "kim@example.com"}
    _run(gw, gw._note_send_rejection(
        "contact-123", "email", meta, "Here is the update.", Exception("550 mailbox unavailable"),
    ))

    prompts = _consults(gw, "contact-123")
    assert len(prompts) == 1
    assert "channel=email stage=send_rejected" in prompts[0]
    assert "550 mailbox unavailable" in prompts[0]
    assert "to=kim@example.com" in prompts[0]


# ── Asynchronous delivery-failure webhooks ──────────────────────────────


def test_carrier_delivery_failed_wakes_agent():
    gw = _gw()
    response = _run(gw, gw._on_text_delivery_failed(_delivery_failed_envelope(), "text.delivery_failed"))

    assert json.loads(response.text)["ok"] is True
    prompts = _consults(gw, "contact-123")
    assert len(prompts) == 1
    text = prompts[0]
    assert "channel=sms stage=delivery_failed" in text
    assert f"attempt=1/{MAX}" in text
    assert "[40002]" in text
    assert "flagged by a SPAM filter" in text
    assert "Sorry Kim — the site isn't built yet." in text


def test_carrier_delivery_failed_replay_is_deduped():
    gw = _gw()
    envelope = _delivery_failed_envelope()

    first = _run(gw, gw._on_text_delivery_failed(envelope, "text.delivery_failed"))
    second = _run(gw, gw._on_text_delivery_failed(envelope, "text.delivery_failed"))

    assert json.loads(first.text)["ok"] is True
    assert json.loads(second.text)["deduped"] is True
    assert len(_consults(gw, "contact-123")) == 1


def test_inbound_direction_delivery_event_does_not_wake():
    gw = _gw()
    envelope = _delivery_failed_envelope(text_id="txt-in")
    envelope["data"]["text_message"]["direction"] = "inbound"

    _run(gw, gw._on_text_delivery_failed(envelope, "text.delivery_failed"))

    assert _consults(gw, "contact-123") == []
    assert gw._outbound_failure_state == {}


def test_group_delivery_failed_reads_recipient_row():
    gw = _gw()
    envelope = _delivery_failed_envelope()
    msg = envelope["data"]["text_message"]
    msg["remote_phone_number"] = None
    msg["error_code"] = None
    msg["error_detail"] = None
    msg["recipients"] = [
        {
            "recipient_phone_number": "+15555550101",
            "delivery_status": "delivery_failed",
            "error_code": "40002",
            "error_detail": "Flagged by a SPAM filter.",
        },
    ]
    envelope["data"]["recipient_phone_number"] = "+15555550101"

    _run(gw, gw._on_text_delivery_failed(envelope, "text.delivery_failed"))

    prompts = _consults(gw, "contact-123")
    assert len(prompts) == 1
    assert "[40002]" in prompts[0]
    assert "Flagged by a SPAM filter." in prompts[0]


def test_imessage_delivery_failed_wakes_agent():
    gw = _gw()
    envelope = {
        "id": "evt-imsg-1",
        "event_type": "imessage.delivery_failed",
        "data": {
            "message": {
                "id": "imsg-out-1",
                "direction": "outbound",
                "remote_number": "+15555550101",
                "conversation_id": "imsg-conv-1",
                "content": "See you at 5!",
                "status": "delivery_failed",
                "error_code": "OPTED_OUT",
                "error_detail": "Recipient has opted out.",
            },
            "contacts": [{"id": "contact-123"}],
        },
    }
    response = _run(gw, gw._on_imessage_delivery_failed(envelope))

    assert json.loads(response.text)["ok"] is True
    prompts = _consults(gw, "contact-123")
    assert len(prompts) == 1
    assert "channel=imessage stage=delivery_failed" in prompts[0]
    assert "[OPTED_OUT]" in prompts[0]
    assert "See you at 5!" in prompts[0]


def test_mail_bounce_wakes_agent_and_failed_is_deduped():
    gw = _gw()
    envelope = {
        "id": "evt-mail-1",
        "event_type": "message.bounced",
        "data": {
            "message": {
                "id": "mail-out-1",
                "thread_id": "thread-1",
                "message_id": "<out-1@inkboxmail.com>",
                "from_address": "agent@inkboxmail.com",
                "to_addresses": ["kim@example.com"],
                "subject": "Your website",
                "snippet": "Here is the plan for the build.",
                "direction": "outbound",
                "status": "bounced",
            },
            "contacts": [{"id": "contact-123"}],
        },
    }

    first = _run(gw, gw._on_mail_delivery_failed(envelope, "message.bounced"))
    failed = dict(envelope, event_type="message.failed")
    second = _run(gw, gw._on_mail_delivery_failed(failed, "message.failed"))

    assert json.loads(first.text)["ok"] is True
    assert json.loads(second.text)["deduped"] is True
    prompts = _consults(gw, "contact-123")
    assert len(prompts) == 1
    assert "channel=email stage=bounced" in prompts[0]
    assert "kim@example.com" in prompts[0]
    assert "Here is the plan for the build." in prompts[0]


def test_mail_inbound_direction_never_wakes():
    gw = _gw()
    envelope = {
        "event_type": "message.bounced",
        "data": {
            "message": {
                "id": "mail-in-1",
                "direction": "inbound",
                "to_addresses": ["agent@inkboxmail.com"],
            },
        },
    }

    _run(gw, gw._on_mail_delivery_failed(envelope, "message.bounced"))

    assert gw._outbound_failure_state == {}


def test_mail_failure_events_are_subscribed():
    assert "message.bounced" in gateway.MAIL_EVENTS
    assert "message.failed" in gateway.MAIL_EVENTS
    assert "message.received" in gateway.MAIL_EVENTS


# ── Budget mechanics across surfaces ────────────────────────────────────


def test_sync_and_webhook_failures_share_one_budget():
    gw = _gw()

    # failure 1 (sync, keyed by conv + number)
    _run(gw, gw._note_send_rejection("contact-123", "sms", _SMS_META, "blocked body", SpamBlockError()))
    # failure 2 (webhook, same conv + number → same budget)
    _run(gw, gw._on_text_delivery_failed(_delivery_failed_envelope(), "text.delivery_failed"))

    prompts = _consults(gw, "contact-123")
    assert len(prompts) == 2
    assert f"attempt=1/{MAX}" in prompts[0]
    assert f"attempt=2/{MAX}" in prompts[1]


def test_inbound_reset_clears_budget():
    gw = _gw()
    _run(gw, gw._note_send_rejection("contact-123", "sms", _SMS_META, "b", SpamBlockError()))
    _run(gw, gw._note_send_rejection("contact-123", "sms", _SMS_META, "b", SpamBlockError()))
    # A fresh inbound clears the budget (what _on_text_received_once calls).
    gw._clear_outbound_failures("sms", "conv-123", "+15555550101", chat_id="contact-123")
    _run(gw, gw._note_send_rejection("contact-123", "sms", _SMS_META, "b", SpamBlockError()))

    prompts = _consults(gw, "contact-123")
    assert len(prompts) == 3
    assert f"attempt=1/{MAX}" in prompts[2]


def test_delivered_receipt_resets_budget():
    gw = _gw()
    _run(gw, gw._note_send_rejection("contact-123", "sms", _SMS_META, "b", SpamBlockError()))
    _run(gw, gw._note_send_rejection("contact-123", "sms", _SMS_META, "b", SpamBlockError()))
    delivered = {
        "event_type": "text.delivered",
        "data": {
            "text_message": {
                "id": "txt-ok",
                "direction": "outbound",
                "remote_phone_number": "+15555550101",
                "conversation_id": "conv-123",
                "delivery_status": "delivered",
            },
        },
    }
    _run(gw, gw._on_delivered_receipt(delivered, "sms"))
    _run(gw, gw._note_send_rejection("contact-123", "sms", _SMS_META, "b", SpamBlockError()))

    prompts = _consults(gw, "contact-123")
    assert len(prompts) == 3
    assert f"attempt=1/{MAX}" in prompts[2]


def test_budget_expires_after_ttl():
    gw = _gw()
    _run(gw, gw._note_send_rejection("contact-123", "sms", _SMS_META, "b", SpamBlockError()))
    # Age every counter entry past the TTL.
    for entry in gw._outbound_failure_state.values():
        entry["at"] = time.time() - gateway.OUTBOUND_FAILURE_STATE_TTL_SECONDS - 1
    _run(gw, gw._note_send_rejection("contact-123", "sms", _SMS_META, "b", SpamBlockError()))

    prompts = _consults(gw, "contact-123")
    assert len(prompts) == 2
    assert f"attempt=1/{MAX}" in prompts[1]
