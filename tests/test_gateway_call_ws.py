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

    def get(self, _chat_id):
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
