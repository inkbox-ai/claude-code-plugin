import asyncio
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime

import mcp.types as mt
import pytest

from inkbox_claude import tools as tools_mod

if not tools_mod.CLAUDE_SDK_AVAILABLE:  # pragma: no cover - sdk is a hard dep here
    pytest.skip("claude-agent-sdk not installed", allow_module_level=True)


# Mirror the SDK shapes as dataclasses so _json_safe serializes them to dicts.
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
        self.list_calls_kwargs = None
        self.transcript_call_id = None

    def list_calls(self, **kwargs):
        self.list_calls_kwargs = kwargs
        return [_FakeCall("inbound"), _FakeCall("outbound")]

    def list_transcripts(self, call_id):
        self.transcript_call_id = call_id
        return [
            _FakeTranscript("remote", "hey can you check the build", 1),
            _FakeTranscript("local", "sure, it's green", 2),
        ]


class _FakeClient:
    def __init__(self):
        self.identity = _FakeIdentity()

    def get_identity(self, handle):
        return self.identity


def _call(server, name, arguments):
    """Invoke a tool by name through the MCP server and return parsed JSON."""
    handler = server["instance"].request_handlers[mt.CallToolRequest]
    req = mt.CallToolRequest(
        method="tools/call",
        params=mt.CallToolRequestParams(name=name, arguments=arguments),
    )
    result = asyncio.run(handler(req)).root
    return json.loads(result.content[0].text)


def test_call_read_tools_are_registered():
    server, names = tools_mod.build_inkbox_mcp_server(_FakeClient(), "claude-code-dima")
    assert "mcp__inkbox__inkbox_list_calls" in names
    assert "mcp__inkbox__inkbox_get_call_transcript" in names


def test_list_calls_passes_pagination_and_returns_rows():
    client = _FakeClient()
    server, _ = tools_mod.build_inkbox_mcp_server(client, "claude-code-dima")

    data = _call(server, "inkbox_list_calls", {"limit": 5, "offset": 10})

    assert client.identity.list_calls_kwargs == {"limit": 5, "offset": 10}
    assert [row["direction"] for row in data] == ["inbound", "outbound"]


def test_get_call_transcript_returns_segments():
    client = _FakeClient()
    server, _ = tools_mod.build_inkbox_mcp_server(client, "claude-code-dima")

    data = _call(server, "inkbox_get_call_transcript", {"call_id": "call-123"})

    assert client.identity.transcript_call_id == "call-123"
    assert [(seg["party"], seg["text"]) for seg in data] == [
        ("remote", "hey can you check the build"),
        ("local", "sure, it's green"),
    ]


def test_get_call_transcript_requires_call_id():
    server, _ = tools_mod.build_inkbox_mcp_server(_FakeClient(), "claude-code-dima")

    data = _call(server, "inkbox_get_call_transcript", {"call_id": "  "})

    assert "call_id is required" in data["error"]
