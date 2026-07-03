"""Failed outbound delivery recovery (issue #19).

Covers the fleet-standard behavior: only hard failed-delivery events wake the
agent, they correlate back to the right session/thread, the recovery reply is
delivered on the SAME channel/thread by default, exact [SILENT] suppresses it,
telemetry (text.delivery_unconfirmed) never wakes the agent, and neither
duplicate webhooks nor a recovery-that-fails-again can loop the agent.
"""

import asyncio
import json
import types

import pytest

from inkbox_claude import gateway
from inkbox_claude import sessions as sessions_mod
from inkbox_claude.config import BridgeConfig


@pytest.fixture(autouse=True)
def fake_web(monkeypatch):
    """aiohttp isn't installed in tests; stub the json_response the handlers use."""
    def json_response(payload):
        return types.SimpleNamespace(text=json.dumps(payload), payload=payload)
    monkeypatch.setattr(gateway, "web", types.SimpleNamespace(json_response=json_response))


# ---------------------------------------------------------------------------
# Routing / correlation / dedup / loop protection (fake session manager)
# ---------------------------------------------------------------------------

class _FakeSession:
    """Captures the recovery turns the gateway asks for."""

    def __init__(self):
        self.recoveries = []  # list of (prompt, mode, meta)

    async def run_recovery(self, prompt, mode, meta):
        self.recoveries.append((prompt, mode, dict(meta)))


class _FakeSessions:
    def __init__(self):
        self.by_id = {}

    def get(self, chat_id):
        return self.by_id.setdefault(chat_id, _FakeSession())


def _gw():
    gw = gateway.InkboxGateway(BridgeConfig(require_signature=False))
    gw.sessions = _FakeSessions()
    return gw


async def _drain():
    # Let the background _run_failure_turn task finish.
    await asyncio.sleep(0)
    await asyncio.sleep(0)


def _dispatch(gw, envelope, event_type):
    """Drive one event through the real routing, then let the wake task run."""
    async def go():
        r = await gw._dispatch_event(envelope, event_type)
        await _drain()
        return r
    return asyncio.run(go())


class _RecordingSubscriptions:
    """Records subscription writes; list() is owner-scoped like the real API."""

    def __init__(self):
        self.created = {}  # owner-tuple -> event_types
        self._subs = []
        self._n = 0

    def list(self, **owner_kw):
        return [s for s in self._subs if s.owner == owner_kw]

    def create(self, url, event_types, **owner_kw):
        self._n += 1
        sub = types.SimpleNamespace(
            id=f"sub-{self._n}", url=url, event_types=list(event_types), owner=owner_kw
        )
        self._subs.append(sub)
        self.created[tuple(sorted(owner_kw.items()))] = list(event_types)
        return sub

    def delete(self, sub_id):
        self._subs = [s for s in self._subs if s.id != sub_id]


class _RecordingInkbox:
    def __init__(self, identity):
        self._identity = identity
        self.webhooks = types.SimpleNamespace(subscriptions=_RecordingSubscriptions())
        self.phone_numbers = types.SimpleNamespace(update=lambda *a, **k: None)

    def get_identity(self, handle):
        return self._identity


def _reconcile_gw():
    identity = types.SimpleNamespace(
        id="idn-1", agent_handle="agent",
        mailbox=types.SimpleNamespace(id="mbx-1", email_address="a@inkbox.ai"),
        phone_number=types.SimpleNamespace(id="ph-1", number="+15550100200"),
        imessage_enabled=True,
    )
    gw = gateway.InkboxGateway(BridgeConfig(identity="agent", require_signature=False))
    gw._inkbox = _RecordingInkbox(identity)
    gw._public_url = "https://agent.tunnel.inkbox.ai"
    gw._public_host = "agent.tunnel.inkbox.ai"
    return gw


def test_startup_subscribes_to_hard_failure_events():
    # Acceptance: the subscription path must register the hard-failure events.
    # This guards the original #19 bug — handlers existed but the events were
    # never subscribed, so a webhook never arrived. Drives the real startup
    # reconciliation, not just the event constants.
    gw = _reconcile_gw()
    gw._patch_identity_objects()
    created = gw._inkbox.webhooks.subscriptions.created

    mailbox = created[(("mailbox_id", "mbx-1"),)]
    phone = created[(("phone_number_id", "ph-1"),)]
    imessage = created[(("agent_identity_id", "idn-1"),)]

    assert "message.bounced" in mailbox and "message.failed" in mailbox
    assert "text.delivery_failed" in phone
    assert "imessage.delivery_failed" in imessage
    # Telemetry must NOT be subscribed — it is not a hard failure.
    everything = set(mailbox) | set(phone) | set(imessage)
    assert "text.delivery_unconfirmed" not in everything


def test_startup_reconcile_is_idempotent_across_restarts():
    # A bridge restart must not thrash subscriptions: the second reconcile sees
    # them already wired and issues no new writes.
    gw = _reconcile_gw()
    gw._patch_identity_objects()
    first = dict(gw._inkbox.webhooks.subscriptions.created)
    gw._inkbox.webhooks.subscriptions.created.clear()
    gw._patch_identity_objects()
    assert gw._inkbox.webhooks.subscriptions.created == {}  # no re-creates
    assert first  # sanity: the first run really did create them


def test_sms_delivery_failure_wakes_session_on_channel():
    gw = _gw()
    envelope = {"data": {"text_message": {
        "id": "m1", "remote_phone_number": "+15551234567",
        "text": "build passed", "error_detail": "Message filtered by carrier",
        "conversation_id": "sms-conv-1",
    }, "contacts": [{"id": "contact-9"}]}}
    _dispatch(gw, envelope, "text.delivery_failed")

    session = gw.sessions.by_id["contact-9"]
    assert len(session.recoveries) == 1
    prompt, mode, meta = session.recoveries[0]
    # Recovery routes back on SMS, in the same thread.
    assert mode == "sms"
    assert meta["conversation_id"] == "sms-conv-1"
    assert meta.get("recovery") is True
    # ...with the details the agent needs to recover.
    assert "SMS" in prompt and "+15551234567" in prompt
    assert "Message filtered by carrier" in prompt
    assert "build passed" in prompt


def test_imessage_delivery_failure_uses_error_reason():
    gw = _gw()
    envelope = {"data": {"message": {
        "id": "i1", "remote_number": "+15551112222", "content": "on it",
        "error_reason": "recipient_unavailable", "status": "error",
        "conversation_id": "imsg-1",
    }, "contacts": [{"id": "contact-3"}]}}
    _dispatch(gw, envelope, "imessage.delivery_failed")

    session = gw.sessions.by_id["contact-3"]
    prompt, mode, meta = session.recoveries[0]
    assert mode == "imessage"
    assert meta["conversation_id"] == "imsg-1"
    assert "iMessage" in prompt
    assert "recipient_unavailable" in prompt


def test_email_bounce_wakes_session():
    gw = _gw()
    envelope = {"data": {"message": {
        "id": "e1", "to_addresses": ["bob@example.com"], "subject": "Re: pricing",
        "thread_id": "thr-1",
    }, "contacts": [{"id": "contact-5"}]}}
    _dispatch(gw, envelope, "message.bounced")

    session = gw.sessions.by_id["contact-5"]
    prompt, mode, meta = session.recoveries[0]
    assert mode == "email"
    assert meta["to"] == "bob@example.com"
    assert meta["subject"] == "Re: pricing"
    assert "email" in prompt and "bounced" in prompt


def test_failure_correlated_by_outbound_id_finds_session_and_thread():
    # The webhook can't resolve a contact (no contacts, no number), but we
    # recorded what we sent — correlation by message id still routes recovery to
    # the right session/thread and enriches the missing details.
    gw = _gw()
    gw._record_outbound("out-42", gateway._OutboundContext(
        channel="SMS", mode="sms", session_key="contact-77",
        recipient="+15559998888", body="deploy done", conversation_id="conv-9",
    ))
    envelope = {"data": {"text_message": {"id": "out-42", "error_detail": "carrier down"}}}
    _dispatch(gw, envelope, "text.delivery_failed")

    session = gw.sessions.by_id["contact-77"]
    prompt, mode, meta = session.recoveries[0]
    assert mode == "sms"
    assert meta["conversation_id"] == "conv-9"
    assert meta["to"] == "+15559998888"
    assert "deploy done" in prompt  # body filled in from the outbound context
    assert "carrier down" in prompt


def test_unconfirmed_does_not_wake_agent():
    # text.delivery_unconfirmed is telemetry, not a hard failure — never wake.
    gw = _gw()
    envelope = {"data": {"text_message": {
        "id": "u1", "remote_phone_number": "+15550001111", "text": "hi",
    }, "contacts": [{"id": "contact-x"}]}}
    resp = _dispatch(gw, envelope, "text.delivery_unconfirmed")
    assert json.loads(resp.text)["ignored"] == "text.delivery_unconfirmed"
    assert gw.sessions.by_id == {}  # nobody woken


def test_duplicate_failure_webhook_does_not_double_notify():
    gw = _gw()
    envelope = {"data": {"text_message": {
        "id": "dup1", "remote_phone_number": "+15550000000", "text": "x",
        "error_detail": "boom",
    }}}
    _dispatch(gw, envelope, "text.delivery_failed")
    second = _dispatch(gw, envelope, "text.delivery_failed")
    assert json.loads(second.text)["deduped"] is True
    # Only the first webhook ran a recovery turn.
    assert len(gw.sessions.by_id["+15550000000"].recoveries) == 1


def test_recovery_send_that_fails_again_does_not_loop():
    # A recovery reply that itself fails to deliver must NOT trigger another
    # recovery — the outbound context flags it as a recovery send.
    gw = _gw()
    gw._record_outbound("rec-1", gateway._OutboundContext(
        channel="SMS", mode="sms", session_key="contact-1",
        recipient="+1555", conversation_id="c1", recovery=True,
    ))
    envelope = {"data": {"text_message": {"id": "rec-1", "error_detail": "still down"}}}
    resp = _dispatch(gw, envelope, "text.delivery_failed")
    assert json.loads(resp.text)["recovery_exhausted"] is True
    assert gw.sessions.by_id == {}  # not woken again


def test_recovery_cap_stops_after_repeated_failures(monkeypatch):
    # Even with fresh message ids each time, a persistently dead channel is
    # bounded: after the cap, the agent stops being woken.
    monkeypatch.setattr(gateway, "MAX_RECOVERIES_PER_WINDOW", 2)
    gw = _gw()

    def fail(i):
        env = {"data": {"text_message": {
            "id": f"c{i}", "remote_phone_number": "+15550000000", "text": "x",
            "error_detail": "boom",
        }}}
        return _dispatch(gw, env, "text.delivery_failed")

    fail(1)
    fail(2)
    third = fail(3)
    assert json.loads(third.text)["recovery_capped"] is True
    assert len(gw.sessions.by_id["+15550000000"].recoveries) == 2


def test_no_usable_session_is_dropped_not_woken():
    # No contact, no thread, no recorded send, no recipient → nothing to route to.
    gw = _gw()
    envelope = {"data": {"text_message": {"id": "", "error_detail": "boom"}}}
    resp = _dispatch(gw, envelope, "text.delivery_failed")
    assert json.loads(resp.text)["unresolved"] is True
    assert gw.sessions.by_id == {}


# ---------------------------------------------------------------------------
# On-channel delivery + [SILENT] suppression (real send path, fake Inkbox)
# ---------------------------------------------------------------------------

class _FakeIdentity:
    def __init__(self):
        self.sent = []  # list of (channel, kwargs)

    def send_text(self, **kwargs):
        self.sent.append(("sms", kwargs))
        return types.SimpleNamespace(id="new-sms-id")

    def send_imessage(self, **kwargs):
        self.sent.append(("imessage", kwargs))
        return types.SimpleNamespace(id="new-im-id")

    def send_email(self, **kwargs):
        self.sent.append(("email", kwargs))
        return types.SimpleNamespace(id="new-email-id")


class _FakeInkbox:
    def __init__(self, identity):
        self._identity = identity

    def get_identity(self, handle):
        return self._identity


def _send_gw(identity):
    gw = gateway.InkboxGateway(BridgeConfig(require_signature=False, identity="agent"))
    gw._inkbox = _FakeInkbox(identity)
    return gw


def test_send_to_contact_delivers_on_thread_and_records_context():
    ident = _FakeIdentity()
    gw = _send_gw(ident)
    asyncio.run(gw.send_to_contact(
        "contact-1", "here you go", "sms",
        {"conversation_id": "c1", "recovery": True},
    ))
    channel, kwargs = ident.sent[0]
    assert channel == "sms"
    assert kwargs["conversation_id"] == "c1"  # same thread
    assert kwargs["text"] == "here you go"
    # Outbound context recorded and tagged as a recovery send (loop guard).
    ctx = gw._outbound_by_id["new-sms-id"]
    assert ctx.session_key == "contact-1"
    assert ctx.recovery is True


def test_send_to_contact_silent_suppresses_delivery():
    ident = _FakeIdentity()
    gw = _send_gw(ident)
    asyncio.run(gw.send_to_contact(
        "contact-1", "[SILENT]", "sms", {"conversation_id": "c1"},
    ))
    assert ident.sent == []          # nothing left the bridge
    assert gw._outbound_by_id == {}  # and nothing was recorded


# ---------------------------------------------------------------------------
# The recovery turn itself routes its reply to the failed channel (real
# ContactSession, fake Claude client with stubbed SDK message types).
# ---------------------------------------------------------------------------

def test_run_recovery_routes_reply_to_failed_channel(monkeypatch):
    class FakeAssistant:
        def __init__(self, content):
            self.content = content

    class FakeTextBlock:
        def __init__(self, text):
            self.text = text

    class FakeResult:
        def __init__(self, result, session_id="s1"):
            self.result = result
            self.session_id = session_id

    monkeypatch.setattr(sessions_mod, "AssistantMessage", FakeAssistant)
    monkeypatch.setattr(sessions_mod, "TextBlock", FakeTextBlock)
    monkeypatch.setattr(sessions_mod, "ResultMessage", FakeResult)

    sent = []

    async def send_fn(chat_id, text, mode, meta):
        sent.append((chat_id, text, mode, dict(meta)))

    async def scenario():
        cfg = BridgeConfig(project_dir="/tmp")
        session = sessions_mod.ContactSession(
            chat_id="contact-1", cfg=cfg, send_fn=send_fn,
            mcp_server=None, mcp_tool_names=[],
            identity_info={"handle": "t", "email": "", "phone": ""},
        )

        class FakeClient:
            async def query(self, text):
                self.q = text

            async def receive_response(self):
                yield FakeAssistant([FakeTextBlock("retrying: build is green")])
                yield FakeResult("retrying: build is green")

        async def ensure():
            return FakeClient()

        session._ensure_client = ensure

        # The session was last used on email; a failed SMS must still route the
        # recovery reply back on SMS, not email.
        session.mode = "email"
        session.reply_meta = {"to": "someone@example.com"}

        await session.run_recovery(
            "recover this", "sms",
            {"conversation_id": "c1", "to": "+15551230000", "recovery": True},
        )
        await asyncio.wait_for(session._worker, timeout=1)

        assert len(sent) == 1
        chat_id, text, mode, meta = sent[0]
        assert mode == "sms"                    # routed to the failed channel
        assert meta["conversation_id"] == "c1"  # ...and the failed thread
        assert text == "retrying: build is green"

    asyncio.run(scenario())
