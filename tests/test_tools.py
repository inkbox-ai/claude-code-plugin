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
        self.agent_handle = "claude-agent"
        self.mailbox = type("Mailbox", (), {"email_address": "claude@inkbox.ai"})()
        self.phone_number = type(
            "Phone",
            (),
            {
                "number": "+15550001111",
                "client_websocket_url": "wss://agent.inkboxwire.com/phone/media/ws?existing=1",
            },
        )()
        self.imessage_enabled = False
        self.tunnel = type("Tunnel", (), {"public_host": "agent.inkboxwire.com"})()
        self.place_call_kwargs = None
        self.place_call_error = None
        self.list_calls_kwargs = None
        self.transcript_call_id = None
        self.sent_emails = []
        self.sent_texts = []
        self.sent_imessages = []
        self.a2a = _FakeA2AClient()

    def place_call(self, **kwargs):
        self.place_call_kwargs = kwargs
        if self.place_call_error is not None:
            raise self.place_call_error
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

    def send_email(self, **kwargs):
        self.sent_emails.append(kwargs)
        return type("Message", (), {"id": "email-1"})()

    def send_text(self, **kwargs):
        self.sent_texts.append(kwargs)
        return type("Message", (), {"id": "sms-1"})()

    def a2a_client(self):
        return self.a2a


class _FakeA2AClient:
    def __init__(self):
        self.calls = []
        self.closed = False

    def fetch_card(self, card_url):
        self.calls.append(("fetch_card", card_url))
        return {"rpc_url": "https://target.example/a2a"}

    def send(self, target, **kwargs):
        self.calls.append(("send", target, kwargs))
        return {"kind": "task", "task": {"id": "task-1"}}

    def get_task(self, target, task_id):
        self.calls.append(("get_task", target, task_id))
        return {"id": task_id, "state": "TASK_STATE_WORKING"}

    def wait(self, target, task_id):
        self.calls.append(("wait", target, task_id))
        return {"id": task_id, "state": "TASK_STATE_COMPLETED"}

    def close(self):
        self.closed = True


class _FakeContacts:
    def __init__(self):
        self.deleted = []

    def get(self, contact_id):
        return {"id": contact_id, "given_name": "Ada"}

    def delete(self, contact_id):
        self.deleted.append(contact_id)


class _FakeClient:
    def __init__(self):
        self.identity = _FakeIdentity()
        self.contacts = _FakeContacts()

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


def test_coding_agent_tool_tier_is_registered():
    tools, tool_names = _tool_map(_FakeClient())
    expected = {
        "inkbox_whoami",
        "inkbox_send_email",
        "inkbox_send_sms",
        "inkbox_send_imessage",
        "inkbox_place_call",
        "inkbox_list_calls",
        "inkbox_get_call_transcript",
        "inkbox_list_text_conversations",
        "inkbox_get_text_conversation",
        "inkbox_list_imessage_conversations",
        "inkbox_get_imessage_conversation",
        "inkbox_lookup_contact",
        "inkbox_list_contacts",
        "inkbox_get_contact",
        "inkbox_create_contact",
        "inkbox_update_contact",
        "inkbox_delete_contact",
        "inkbox_a2a_call",
        "inkbox_a2a_check",
        "inkbox_a2a_reply",
    }

    assert set(tools) == expected
    assert set(tool_names) == {f"mcp__inkbox__{name}" for name in expected}


def test_get_contact_and_delete_contact_tools():
    client = _FakeClient()

    contact = _call(client, "inkbox_get_contact", {"contact_id": "contact-1"})
    deleted = _call(client, "inkbox_delete_contact", {"contact_id": "contact-1"})

    assert contact["id"] == "contact-1"
    assert deleted["deleted"] == "contact-1"
    assert client.contacts.deleted == ["contact-1"]


def test_a2a_tools_send_check_and_reply():
    client = _FakeClient()
    card_url = "https://target.example/card"

    sent = _call(
        client,
        "inkbox_a2a_call",
        {"card_url": card_url, "text": "Investigate.", "message_id": "msg-1"},
    )
    checked = _call(
        client,
        "inkbox_a2a_check",
        {"card_url": card_url, "task_id": "task-1", "wait": True},
    )
    replied = _call(
        client,
        "inkbox_a2a_reply",
        {
            "card_url": card_url,
            "task_id": "task-1",
            "text": "More context.",
            "message_id": "msg-2",
        },
    )

    assert sent["task"]["id"] == "task-1"
    assert checked["state"] == "TASK_STATE_COMPLETED"
    assert replied["task"]["id"] == "task-1"
    assert (
        "wait",
        {"rpc_url": "https://target.example/a2a"},
        "task-1",
    ) in client.identity.a2a.calls
    assert client.identity.a2a.closed is True


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
    # Number-only identity → resolved to (and echoed as) the dedicated line.
    assert data["origination"] == "dedicated_number"
    assert client.identity.place_call_kwargs["origination"] == "dedicated_number"
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


def test_place_call_explicit_origination_wins():
    client = _FakeClient()
    client.identity.imessage_enabled = True

    data = _call(
        client,
        "inkbox_place_call",
        {
            "to_number": "+15551112222",
            "purpose": "check in",
            "origination": "shared_imessage_number",
        },
    )

    assert data["origination"] == "shared_imessage_number"
    assert client.identity.place_call_kwargs["origination"] == "shared_imessage_number"


def test_place_call_no_shared_connection_returns_legible_error():
    client = _FakeClient()
    client.identity.imessage_enabled = True
    client.identity.place_call_error = RuntimeError(
        "HTTP 409 {'error': 'no_shared_connection'}"
    )

    data = _call(
        client,
        "inkbox_place_call",
        {
            "to_number": "+15551112222",
            "purpose": "check in",
            "origination": "shared_imessage_number",
        },
    )

    # The agent gets an actionable message, not a raw 409.
    assert "isn't connected to you over iMessage" in data["error"]
    assert 'set origination to "dedicated_number"' in data["error"]
    assert "no_shared_connection" in data["detail"]


def test_place_call_without_any_line_tells_agent_how_to_fix():
    client = _FakeClient()
    client.identity.phone_number = None
    client.identity.imessage_enabled = False

    data = _call(
        client,
        "inkbox_place_call",
        {"to_number": "+15551112222", "purpose": "check in"},
    )

    assert "no dedicated phone number" in data["error"]
    assert "enable iMessage" in data["error"]
    assert client.identity.place_call_kwargs is None


def test_whoami_reports_the_two_lines():
    client = _FakeClient()
    client.identity.imessage_enabled = True

    data = _call(client, "inkbox_whoami", {})

    assert data["lines"]["dedicated_phone_line"] == "+15550001111"
    assert data["lines"]["shared_imessage_line"] == "enabled"
    # The shared line's number is managed by Inkbox and never surfaced.
    assert "origination=dedicated_number" in data["lines"]["dedicated_phone_line_note"]
    assert "origination=shared_imessage_number" in data["lines"]["shared_imessage_line_note"]
    assert "not shown" in data["lines"]["shared_imessage_line_note"]


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


def test_send_sms_rejects_text_over_limit():
    client = _FakeClient()
    data = _call(
        client,
        "inkbox_send_sms",
        {
            "to": "+15551112222",
            "text": "x" * (tools_mod.SMS_MAX_LENGTH + 1),
        },
    )

    assert data["error_code"] == "sms_too_long"
    assert data["char_count"] == tools_mod.SMS_MAX_LENGTH + 1
    assert client.identity.sent_texts == []


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


def test_successful_message_tool_notifies_current_session():
    client = _FakeClient()
    deliveries = []

    class _Session:
        def mark_tool_delivery(self, mode, target):
            deliveries.append((mode, target))

    token = tools_mod.CURRENT_SESSION.set(_Session())
    try:
        data = _call(
            client,
            "inkbox_send_email",
            {
                "to": "ada@example.com",
                "subject": "Details",
                "body": "Here they are.",
                "attachment_paths": [],
            },
        )
    finally:
        tools_mod.CURRENT_SESSION.reset(token)

    assert data["sent"] is True
    assert client.identity.sent_emails == [{
        "to": ["ada@example.com"],
        "subject": "Details",
        "body_text": "Here they are.",
        "attachments": None,
    }]
    assert deliveries == [("email", "ada@example.com")]
