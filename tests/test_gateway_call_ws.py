import asyncio
import types

from inkbox_claude import gateway
from inkbox_claude.config import BridgeConfig


class _FakeWS:
    """Stand-in for aiohttp's WebSocketResponse.

    Captures the headers the handler sets before prepare() and yields no
    messages, so the handler arms the socket and then exits cleanly.
    """

    def __init__(self):
        self.headers = {}
        self.prepared = False

    async def prepare(self, _request):
        # Headers must already be set by the time the upgrade is committed.
        self.prepared = True

    def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration


class _FakeTextMsg:
    def __init__(self, data):
        self.type = "text"
        self.data = data


class _ScriptedWS(_FakeWS):
    def __init__(self, messages):
        super().__init__()
        self._messages = list(messages)
        self.sent = []

    async def __anext__(self):
        if not self._messages:
            raise StopAsyncIteration
        return self._messages.pop(0)

    async def send_str(self, data):
        self.sent.append(data)


class _FakeRequest:
    def __init__(self):
        self.headers = {}  # no X-Call-Context; signature check is off
        self.query = {}  # no context_token; inbound (no outbound place-call ctx)


class _NoDeliveryInkbox:
    def get_identity(self, _identity):
        raise AssertionError("send_to_contact must not reach Inkbox delivery")


class _FakeIdentity:
    def __init__(self):
        self.sent_texts = []
        self.sent_imessages = []

    def send_text(self, **kwargs):
        self.sent_texts.append(kwargs)

    def send_imessage(self, **kwargs):
        self.sent_imessages.append(kwargs)


class _DeliveryInkbox:
    def __init__(self, identity):
        self.identity = identity

    def get_identity(self, _identity):
        return self.identity


class _FakeContactSession:
    def __init__(self):
        self.inbound = []
        self.consults = []

    async def handle_inbound(self, text, mode, meta):
        self.inbound.append((text, mode, meta))

    async def run_consult(self, prompt):
        self.consults.append(prompt)
        return ""


class _FakeSessions:
    def __init__(self, session):
        self.session = session
        self.requested_ids = []

    def get(self, chat_id):
        self.requested_ids.append(chat_id)
        return self.session


def test_call_ws_declares_inkbox_stt_tts_headers(monkeypatch):
    """The WS upgrade must advertise platform-side STT/TTS so Inkbox sends us
    transcripts and speaks our text frames — without these it defaults to raw
    media and voice is silent both ways."""
    fake_ws = _FakeWS()
    # gateway.web is None when aiohttp isn't installed, so swap in a tiny
    # stand-in namespace rather than patching an attribute on it.
    monkeypatch.setattr(gateway, "web", types.SimpleNamespace(WebSocketResponse=lambda: fake_ws))

    cfg = BridgeConfig(require_signature=False)
    gw = gateway.InkboxGateway(cfg)

    asyncio.run(gw._handle_call_ws(_FakeRequest()))

    assert fake_ws.prepared is True
    assert fake_ws.headers.get("x-use-inkbox-speech-to-text") == "true"
    assert fake_ws.headers.get("x-use-inkbox-text-to-speech") == "true"


def test_send_to_contact_suppresses_exact_silent_reply():
    gw = gateway.InkboxGateway(BridgeConfig(require_signature=False, identity="claude"))
    gw._inkbox = _NoDeliveryInkbox()

    asyncio.run(gw.send_to_contact("contact-1", "[SILENT]", "sms", {"to": "+15551234567"}))


def test_send_to_contact_drops_late_voice_reply_without_channel_fallback():
    gw = gateway.InkboxGateway(BridgeConfig(require_signature=False, identity="claude"))
    gw._inkbox = _NoDeliveryInkbox()

    asyncio.run(
        gw.send_to_contact(
            "+15551234567",
            "This answer finished after hangup.",
            "voice",
            {"call_id": "call-1", "to": "+15551234567"},
        )
    )


def test_send_to_contact_rejects_over_limit_sms_without_delivery():
    gw = gateway.InkboxGateway(BridgeConfig(require_signature=False, identity="claude"))
    gw._inkbox = _NoDeliveryInkbox()

    try:
        asyncio.run(
            gw.send_to_contact(
                "+15551234567",
                "x" * (gateway.SMS_MAX_LENGTH + 1),
                "sms",
                {"to": "+15551234567"},
            )
        )
    except ValueError as exc:
        assert "SMS text is 1601 characters" in str(exc)
    else:
        raise AssertionError("expected over-limit SMS reply to be rejected")


def test_send_to_contact_rejects_over_limit_imessage_without_delivery():
    gw = gateway.InkboxGateway(BridgeConfig(require_signature=False, identity="claude"))
    gw._inkbox = _NoDeliveryInkbox()

    try:
        asyncio.run(
            gw.send_to_contact(
                "contact-1",
                "x" * (gateway.IMESSAGE_MAX_LENGTH + 1),
                "imessage",
                {"conversation_id": "imconv-123"},
            )
        )
    except ValueError as exc:
        assert "iMessage text is 18996 characters" in str(exc)
    else:
        raise AssertionError("expected over-limit iMessage reply to be rejected")


def test_send_to_contact_uses_prefixed_sms_conversation_chat_id():
    identity = _FakeIdentity()
    gw = gateway.InkboxGateway(BridgeConfig(require_signature=False, identity="claude"))
    gw._inkbox = _DeliveryInkbox(identity)

    asyncio.run(gw.send_to_contact("sms:conv-123", "reply", "sms", {}))

    assert identity.sent_texts == [{"text": "reply", "conversation_id": "conv-123"}]


def test_send_to_contact_uses_prefixed_imessage_conversation_chat_id():
    identity = _FakeIdentity()
    gw = gateway.InkboxGateway(BridgeConfig(require_signature=False, identity="claude"))
    gw._inkbox = _DeliveryInkbox(identity)

    asyncio.run(gw.send_to_contact("imessage:imconv-123", "reply", "imessage", {}))

    assert identity.sent_imessages == [{"conversation_id": "imconv-123", "text": "reply"}]


def test_call_ws_stt_tts_runs_call_ended_reflection(monkeypatch):
    fake_ws = _ScriptedWS([
        _FakeTextMsg('{"event":"start"}'),
        _FakeTextMsg('{"event":"transcript","text":"Please send the summary after this.","is_final":true}'),
        _FakeTextMsg('{"event":"stop"}'),
    ])
    monkeypatch.setattr(gateway, "web", types.SimpleNamespace(WebSocketResponse=lambda: fake_ws))
    monkeypatch.setattr(gateway, "WSMsgType", types.SimpleNamespace(TEXT="text"))

    session = _FakeContactSession()
    gw = gateway.InkboxGateway(BridgeConfig(require_signature=False))
    gw.sessions = _FakeSessions(session)

    asyncio.run(gw._handle_call_ws(_FakeRequest()))

    assert session.inbound == [
        (
            "Please send the summary after this.",
            "voice",
            {
                "call_id": "",
                "sender": "",
                "contact": None,
                "direction": "inbound",
            },
        )
    ]
    assert len(session.consults) == 1
    assert "[voice call ended]" in session.consults[0]
    assert "do not redo work that was already completed" in session.consults[0]
    assert "Please send the summary after this." in session.consults[0]


def test_call_ws_uses_stored_call_contact_session_for_stt_tts(monkeypatch):
    fake_ws = _ScriptedWS([
        _FakeTextMsg('{"event":"transcript","text":"Can you see my earlier texts?","is_final":true}'),
        _FakeTextMsg('{"event":"stop"}'),
    ])
    monkeypatch.setattr(gateway, "web", types.SimpleNamespace(WebSocketResponse=lambda: fake_ws))
    monkeypatch.setattr(gateway, "WSMsgType", types.SimpleNamespace(TEXT="text"))

    session = _FakeContactSession()
    sessions = _FakeSessions(session)
    gw = gateway.InkboxGateway(BridgeConfig(require_signature=False))
    gw.sessions = sessions
    gw._call_meta_by_id["call-1"] = {
        "id": "call-1",
        "direction": "inbound",
        "remotePhoneNumber": "+15551234567",
        "local_phone_number": "+15550001111",
        "contacts": [{"bucket": "from", "contactId": "contact-1", "name": "Ada Lovelace"}],
    }
    request = _FakeRequest()
    request.query = {"call_id": "call-1"}

    asyncio.run(gw._handle_call_ws(request))

    assert sessions.requested_ids == ["contact-1", "contact-1"]
    assert session.inbound[0][2]["sender"] == "+15551234567"
    assert session.inbound[0][2]["contact"]["id"] == "contact-1"
    assert session.inbound[0][2]["contact"]["name"] == "Ada Lovelace"
    assert "call-1" not in gw._call_meta_by_id


def test_call_ws_backfills_remote_via_identity_centered_call_read(monkeypatch):
    """When the upgrade carries no caller metadata (Inkbox accepted the call
    itself), a single call-id read resolves the remote party — including
    shared-line calls, which have no phone_number on the identity."""
    fake_ws = _ScriptedWS([
        _FakeTextMsg('{"event":"transcript","text":"hello","is_final":true}'),
        _FakeTextMsg('{"event":"stop"}'),
    ])
    monkeypatch.setattr(gateway, "web", types.SimpleNamespace(WebSocketResponse=lambda: fake_ws))
    monkeypatch.setattr(gateway, "WSMsgType", types.SimpleNamespace(TEXT="text"))

    class _FakeCalls:
        def __init__(self):
            self.requested = []

        def get(self, call_id):
            self.requested.append(call_id)
            return types.SimpleNamespace(
                remote_phone_number="+15559990000", direction="inbound"
            )

    class _FakeInkboxWithCalls:
        def __init__(self):
            self.calls = _FakeCalls()
            self.contacts = types.SimpleNamespace(lookup=lambda **_k: [])

    session = _FakeContactSession()
    gw = gateway.InkboxGateway(BridgeConfig(require_signature=False))
    gw.sessions = _FakeSessions(session)
    gw._inkbox = _FakeInkboxWithCalls()
    request = _FakeRequest()
    request.query = {"call_id": "call-9"}

    asyncio.run(gw._handle_call_ws(request))

    assert gw._inkbox.calls.requested == ["call-9"]
    assert session.inbound[0][2]["sender"] == "+15559990000"


class _FakeBridge:
    def __init__(self):
        self.ran = False
        self.closed = False

    async def run(self, *, inkbox_ws, on_agent_consult, on_post_call_actions, on_call_ended):
        self.ran = True

    async def close(self):
        self.closed = True


def test_call_ws_realtime_path_sets_rawmedia_headers_and_runs_bridge(monkeypatch):
    """With Realtime enabled and OpenAI reachable, accept in raw-media mode
    (STT/TTS off) and hand the call to the bridge."""
    fake_ws = _FakeWS()
    monkeypatch.setattr(gateway, "web", types.SimpleNamespace(WebSocketResponse=lambda: fake_ws))
    bridge = _FakeBridge()

    async def fake_open(*, config, meta):
        return bridge

    monkeypatch.setattr(gateway, "open_inkbox_realtime_bridge", fake_open)

    from inkbox_claude.realtime import RealtimeConfig
    cfg = BridgeConfig(require_signature=False, realtime=RealtimeConfig(enabled=True, api_key="sk-x"))
    gw = gateway.InkboxGateway(cfg)

    asyncio.run(gw._handle_call_ws(_FakeRequest()))

    assert fake_ws.headers.get("x-use-inkbox-speech-to-text") == "false"
    assert fake_ws.headers.get("x-use-inkbox-text-to-speech") == "false"
    assert bridge.ran is True and bridge.closed is True


def test_call_ws_passes_outbound_context_to_realtime(monkeypatch, tmp_path):
    fake_ws = _FakeWS()
    monkeypatch.setattr(gateway, "web", types.SimpleNamespace(WebSocketResponse=lambda: fake_ws))
    monkeypatch.setenv("INKBOX_CLAUDE_HOME", str(tmp_path))
    bridge = _FakeBridge()
    seen = {}

    context_dir = tmp_path / "call_contexts"
    context_dir.mkdir()
    (context_dir / "tok-123.json").write_text(
        '{"purpose":"tell them the deploy is fixed","opening_message":"Hi there",'
        '"context":"PR 12","to_number":"+15551234567"}'
    )

    async def fake_open(*, config, meta):
        seen["meta"] = meta
        return bridge

    monkeypatch.setattr(gateway, "open_inkbox_realtime_bridge", fake_open)

    from inkbox_claude.realtime import RealtimeConfig
    cfg = BridgeConfig(require_signature=False, realtime=RealtimeConfig(enabled=True, api_key="sk-x"))
    gw = gateway.InkboxGateway(cfg)
    request = _FakeRequest()
    request.query = {"context_token": "tok-123"}

    asyncio.run(gw._handle_call_ws(request))

    assert seen["meta"].direction == "outbound"
    assert seen["meta"].remote_phone_number == "+15551234567"
    assert seen["meta"].outbound_purpose == "tell them the deploy is fixed"
    assert seen["meta"].outbound_opening == "Hi there"
    assert seen["meta"].outbound_context == "PR 12"


def test_call_ws_passes_contact_and_identity_context_to_realtime(monkeypatch):
    fake_ws = _FakeWS()
    monkeypatch.setattr(gateway, "web", types.SimpleNamespace(WebSocketResponse=lambda: fake_ws))
    bridge = _FakeBridge()
    seen = {}

    async def fake_open(*, config, meta):
        seen["meta"] = meta
        return bridge

    monkeypatch.setattr(gateway, "open_inkbox_realtime_bridge", fake_open)

    from inkbox_claude.realtime import RealtimeConfig

    cfg = BridgeConfig(require_signature=False, realtime=RealtimeConfig(enabled=True, api_key="sk-x"))
    gw = gateway.InkboxGateway(cfg)
    gw._identity = types.SimpleNamespace(
        agent_handle="claude",
        mailbox=types.SimpleNamespace(email_address="claude@example.com"),
        phone_number=types.SimpleNamespace(number="+15550001111"),
    )
    request = _FakeRequest()
    request.headers = {
        "X-Call-Context": (
            '{"id":"call-1","remote_phone_number":"+15551234567",'
            '"contacts":[{"id":"contact-1","name":"Ada Lovelace"}]}'
        )
    }

    asyncio.run(gw._handle_call_ws(request))

    assert seen["meta"].agent_identity_handle == "claude"
    assert seen["meta"].agent_identity_email == "claude@example.com"
    assert seen["meta"].agent_identity_phone == "+15550001111"
    assert seen["meta"].contact_known is True
    assert seen["meta"].contact_id == "contact-1"
    assert seen["meta"].contact_name == "Ada Lovelace"


def test_call_ws_realtime_falls_back_to_stt_tts_on_connect_failure(monkeypatch):
    """If OpenAI can't be reached and fallback is allowed, accept the call on
    the Inkbox STT/TTS path (headers back to true) instead of dropping it."""
    fake_ws = _FakeWS()
    monkeypatch.setattr(gateway, "web", types.SimpleNamespace(WebSocketResponse=lambda: fake_ws))

    async def fake_open(*, config, meta):
        raise gateway.RealtimeBridgeConnectError("no key")

    monkeypatch.setattr(gateway, "open_inkbox_realtime_bridge", fake_open)

    from inkbox_claude.realtime import RealtimeConfig
    cfg = BridgeConfig(require_signature=False, realtime=RealtimeConfig(
        enabled=True, api_key="sk-x", fallback_to_inkbox_stt_tts=True,
    ))
    gw = gateway.InkboxGateway(cfg)

    asyncio.run(gw._handle_call_ws(_FakeRequest()))

    assert fake_ws.headers.get("x-use-inkbox-speech-to-text") == "true"
    assert fake_ws.headers.get("x-use-inkbox-text-to-speech") == "true"
