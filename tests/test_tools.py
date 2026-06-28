import asyncio
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from urllib.parse import parse_qs, urlparse

import pytest

from inkbox_claude import tools as tools_mod


@pytest.fixture(autouse=True)
def _fake_claude_sdk(monkeypatch):
    async def immediate(func, /, *args, **kwargs):
        return func(*args, **kwargs)

    def fake_tool(name, _description, _schema):
        def decorator(func):
            func._inkbox_tool_name = name
            return func

        return decorator

    def fake_create_sdk_mcp_server(name, version, tools):
        return {"name": name, "version": version, "tools": tools}

    monkeypatch.setattr(tools_mod.asyncio, "to_thread", immediate)
    monkeypatch.setattr(tools_mod, "CLAUDE_SDK_AVAILABLE", True)
    monkeypatch.setattr(tools_mod, "tool", fake_tool)
    monkeypatch.setattr(tools_mod, "create_sdk_mcp_server", fake_create_sdk_mcp_server)


@dataclass
class _FakeCall:
    direction: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    local_phone_number: str = "+16614031457"
    remote_phone_number: str = "+15551112222"
    status: str = "completed"
    started_at: datetime = datetime(2026, 6, 18, 4, 0, 0)
    ended_at: datetime = datetime(2026, 6, 18, 4, 1, 0)


@dataclass
class _FakeTranscript:
    party: str
    text: str
    seq: int
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    ts_ms: int = 0


class _FakeIdentity:
    def __init__(self):
        self.phone_number = type(
            "Phone",
            (),
            {"client_websocket_url": "wss://agent.inkboxwire.com/phone/media/ws?existing=1"},
        )()
        self.tunnel = type("Tunnel", (), {"public_host": "agent.inkboxwire.com"})()
        self.place_call_kwargs = None
        self.list_calls_kwargs = None
        self.transcript_call_id = None
        self.sent_imessages = []

    def place_call(self, **kwargs):
        self.place_call_kwargs = kwargs
        return type("Call", (), {"id": "call-123", "status": "queued"})()

    def list_calls(self, **kwargs):
        self.list_calls_kwargs = kwargs
        return [_FakeCall("inbound"), _FakeCall("outbound")]

    def list_transcripts(self, call_id):
        self.transcript_call_id = call_id
        return [
            _FakeTranscript("remote", "hey can you check the build", 1),
            _FakeTranscript("local", "sure, it's green", 2),
        ]

    def send_imessage(self, **kwargs):
        self.sent_imessages.append(kwargs)
        return type("Message", (), {"id": "im-1"})()


class _FakeClient:
    def __init__(self):
        self.identity = _FakeIdentity()

    def get_identity(self, _handle):
        return self.identity


def _tool_map(client):
    server, tool_names = tools_mod.build_inkbox_mcp_server(client, "claude-agent")
    return {tool._inkbox_tool_name: tool for tool in server["tools"]}, tool_names


def _call(client, name, arguments):
    tools, _tool_names = _tool_map(client)
    result = asyncio.run(tools[name](arguments))
    return json.loads(result["content"][0]["text"])


def test_call_tools_are_registered():
    tools, tool_names = _tool_map(_FakeClient())

    assert "inkbox_place_call" in tools
    assert "inkbox_list_calls" in tools
    assert "inkbox_get_call_transcript" in tools
    assert "mcp__inkbox__inkbox_place_call" in tool_names
    assert "mcp__inkbox__inkbox_list_calls" in tool_names
    assert "mcp__inkbox__inkbox_get_call_transcript" in tool_names


def test_place_call_writes_context_and_tags_websocket_url(tmp_path, monkeypatch):
    monkeypatch.setenv("INKBOX_CLAUDE_HOME", str(tmp_path))
    client = _FakeClient()

    data = _call(
        client,
        "inkbox_place_call",
        {
            "to_number": "+15551112222",
            "purpose": "tell them the build is fixed",
            "opening_message": "Hi, this is Claude Code with the build update.",
            "context": "The fix landed in PR 12.",
        },
    )

    assert data["placed"] is True
    assert data["id"] == "call-123"
    assert data["to"] == "+15551112222"
    ws_url = client.identity.place_call_kwargs["client_websocket_url"]
    parsed = urlparse(ws_url)
    query = parse_qs(parsed.query)
    assert query["existing"] == ["1"]
    token = query["context_token"][0]
    payload = json.loads((tmp_path / "call_contexts" / f"{token}.json").read_text())
    assert payload["purpose"] == "tell them the build is fixed"
    assert payload["opening_message"] == "Hi, this is Claude Code with the build update."
    assert payload["context"] == "The fix landed in PR 12."


def test_place_call_requires_purpose():
    data = _call(
        _FakeClient(),
        "inkbox_place_call",
        {"to_number": "+15551112222", "purpose": "  "},
    )

    assert "purpose is required" in data["error"]


def test_list_calls_passes_pagination_and_returns_rows():
    client = _FakeClient()

    data = _call(client, "inkbox_list_calls", {"limit": 5, "offset": 10})

    assert client.identity.list_calls_kwargs == {"limit": 5, "offset": 10}
    assert [row["direction"] for row in data] == ["inbound", "outbound"]


def test_get_call_transcript_returns_segments():
    client = _FakeClient()

    data = _call(client, "inkbox_get_call_transcript", {"call_id": "call-123"})

    assert client.identity.transcript_call_id == "call-123"
    assert [(seg["party"], seg["text"]) for seg in data] == [
        ("remote", "hey can you check the build"),
        ("local", "sure, it's green"),
    ]


def test_get_call_transcript_requires_call_id():
    data = _call(_FakeClient(), "inkbox_get_call_transcript", {"call_id": "  "})

    assert "call_id is required" in data["error"]


def test_send_imessage_rejects_text_over_limit():
    client = _FakeClient()
    data = _call(
        client,
        "inkbox_send_imessage",
        {
            "conversation_id": "imconv-123",
            "text": "x" * (tools_mod.IMESSAGE_MAX_LENGTH + 1),
        },
    )

    assert data["error_code"] == "imessage_too_long"
    assert data["char_count"] == tools_mod.IMESSAGE_MAX_LENGTH + 1
    assert client.identity.sent_imessages == []
