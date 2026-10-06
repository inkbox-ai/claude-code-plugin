"""Exercise native iMessage tools and gateway through the real SDK HTTP stack."""

import asyncio
import json
from types import SimpleNamespace
from uuid import UUID

import httpx
import pytest

from inkbox import Inkbox
from inkbox import _config as sdk_config
from inkbox.agent_identity import AgentIdentity

from inkbox_claude.config import BridgeConfig, imessage_threading_capability
from inkbox_claude.gateway import InkboxGateway
from inkbox_claude.imessage import IMessageState, auto_reply_kwargs, source_metadata
from inkbox_claude.tools import build_inkbox_mcp_server, CURRENT_SESSION
from tests.native_mcp import call_mcp
from tests.test_sessions import make_session
from inkbox_claude.sessions import _Turn
import os


pytestmark = pytest.mark.skipif(
    not imessage_threading_capability(AgentIdentity)[0],
    reason="The installed SDK does not yet expose native iMessage reply APIs",
)

IDENTITY_ID = str(UUID(int=1))
CONVERSATION_ID = str(UUID(int=2))
SOURCE_ID = str(UUID(int=3))
OUTBOUND_ID = str(UUID(int=4))
THREAD_ID = str(UUID(int=5))
ROOT_ID = str(UUID(int=6))
PARENT_ID = str(UUID(int=7))
TIMESTAMP = "2026-01-01T00:00:00+00:00"


def message(**changes):
    return {
        "id": SOURCE_ID, "conversation_id": CONVERSATION_ID, "assignment_id": None,
        "direction": "inbound", "content": "Question", "message_type": "message",
        "service": "imessage", "is_read": False, "status": "received",
        "created_at": TIMESTAMP, "updated_at": TIMESTAMP,
        "reply_to_message_id": None, "thread_id": None, "thread_root_message_id": None,
        **changes,
    }


@pytest.fixture
def sdk(monkeypatch, tmp_path):
    """Use real SDK clients/serializers with synthetic credentials and no network."""
    monkeypatch.setenv("INKBOX_CLAUDE_HOME", str(tmp_path))
    token = CURRENT_SESSION.set(None)
    monkeypatch.setenv("INKBOX_IMESSAGE_THREADED_REPLIES", "true")
    monkeypatch.setenv("INKBOX_BASE_URL", "https://example.com")
    monkeypatch.setenv("INKBOX_VAULT_KEY", "")
    monkeypatch.delenv("INKBOX_CLAUDE_CHAT_ID", raising=False)
    monkeypatch.setattr(sdk_config, "_CONFIG_PATH", tmp_path / "unused-sdk-config")
    state = SimpleNamespace(
        requests=[], source=message(),
        outbound=message(id=OUTBOUND_ID, direction="outbound", content="Answer", status="pending"),
        thread_status=200, send_status=201,
        page={
            "conversation_id": CONVERSATION_ID, "thread_id": None, "thread_root_message_id": None,
            "messages": [message()], "next_cursor": "next:opaque+/=",
        },
    )

    def handle(request):
        assert request.url.host == "example.com"
        state.requests.append(request)
        path = request.url.path
        if request.method == "GET" and path == "/api/v1/identities/agent":
            return httpx.Response(200, json={
                "id": IDENTITY_ID, "organization_id": "org_example", "agent_handle": "agent",
                "imessage_enabled": True, "created_at": TIMESTAMP, "updated_at": TIMESTAMP,
            })
        if request.method == "GET" and path == f"/api/v1/imessage/messages/{SOURCE_ID}":
            return httpx.Response(200, json=state.source)
        if request.method == "GET" and path in {
            f"/api/v1/imessage/messages/{SOURCE_ID}/thread",
            f"/api/v1/imessage/conversations/{CONVERSATION_ID}/threads/{THREAD_ID}",
        }:
            if state.thread_status != 200:
                return httpx.Response(state.thread_status, json={"detail": "Thread endpoint unavailable"})
            return httpx.Response(200, json=state.page)
        if request.method == "POST" and path == "/api/v1/imessage/messages":
            if state.send_status != 201:
                return httpx.Response(state.send_status, json={"detail": {"error": "imessage_reply_target_unavailable"}})
            return httpx.Response(201, json={"message": state.outbound})
        raise AssertionError(f"Unexpected mocked request: {request.method} {path}")

    monkeypatch.setattr(httpx, "HTTPTransport", lambda **kwargs: httpx.MockTransport(handle))
    client = Inkbox(api_key="ApiKey_synthetic_test_only", base_url="https://example.com")
    state.client = client
    yield state
    client.close()
    CURRENT_SESSION.reset(token)


def active_source(monkeypatch):
    session = make_session([])
    session._turn_active = True
    session._current_turn = _Turn(text="Question", mode="imessage", reply_meta={
        "conversation_id": CONVERSATION_ID, **source_metadata(message()),
    })
    token = CURRENT_SESSION.set(session)
    return session, token


def tool(sdk, name="inkbox_send_imessage", **arguments):
    cfg = BridgeConfig(identity="agent", base_url="https://example.com", imessage_threaded_replies=os.environ.get("INKBOX_IMESSAGE_THREADED_REPLIES") == "true")
    config, _ = build_inkbox_mcp_server(sdk.client, "agent", cfg)
    result = asyncio.run(call_mcp(config, name, arguments))
    return result, json.loads(result["content"][0]["text"])


def posts(sdk):
    return [request for request in sdk.requests if request.method == "POST"]


def test_real_sdk_bridge_selected_target_policy_identity_and_key_reach_wire(sdk, monkeypatch):
    active_source(monkeypatch)
    result, payload = tool(
        sdk, conversation_id=CONVERSATION_ID, text="Answer",
    )
    assert not result.get("isError"), payload
    assert len(posts(sdk)) == 1
    request = posts(sdk)[0]
    assert json.loads(request.content) == {
        "conversation_id": CONVERSATION_ID, "text": "Answer", "reply_to_message_id": SOURCE_ID,
        "plain_reply_fallback": True,
    }
    assert request.url.params["agent_identity_id"] == IDENTITY_ID
    assert request.headers["Idempotency-Key"].startswith("claude:tool:")
    reads = [request for request in sdk.requests if "/imessage/" in request.url.path and request.method == "GET"]
    assert [request.url.path for request in reads] == [
        f"/api/v1/imessage/messages/{SOURCE_ID}", f"/api/v1/imessage/messages/{SOURCE_ID}/thread",
    ]
    assert all(request.url.params["agent_identity_id"] == IDENTITY_ID for request in reads)
    assert reads[-1].url.params["limit"] == "1"
    assert payload["id"] == OUTBOUND_ID
    assert payload["status"] == "pending"
    assert payload["reply_to_message_id"] is None
    assert payload["thread_id"] is None


def test_real_sdk_plain_fallback_preserves_actual_standalone_thread(sdk, monkeypatch):
    active_source(monkeypatch)
    sdk.outbound.update(thread_id=THREAD_ID, service="sms", was_downgraded=True)
    result, payload = tool(sdk, conversation_id=CONVERSATION_ID, text="Answer")
    assert not result.get("isError"), payload
    assert json.loads(posts(sdk)[0].content)["plain_reply_fallback"] is True
    assert payload["reply_to_message_id"] is None
    assert payload["thread_id"] == THREAD_ID
    assert payload["thread_root_message_id"] is None


@pytest.mark.parametrize("name,arguments,path", [
    ("inkbox_get_imessage_thread", {"message_id": SOURCE_ID}, f"/api/v1/imessage/messages/{SOURCE_ID}/thread"),
    ("inkbox_get_imessage_conversation_thread", {"conversation_id": CONVERSATION_ID, "thread_id": THREAD_ID},
     f"/api/v1/imessage/conversations/{CONVERSATION_ID}/threads/{THREAD_ID}"),
])
def test_real_sdk_thread_pages_keep_cursor_identity_and_null_metadata(sdk, name, arguments, path):
    result, payload = tool(sdk, name, **arguments, limit=2, cursor="prior:opaque+/=")
    assert not result.get("isError"), payload
    request = sdk.requests[-1]
    assert request.url.path == path
    assert dict(request.url.params) == {
        "agent_identity_id": IDENTITY_ID, "limit": "2", "cursor": "prior:opaque+/=",
    }
    assert payload["thread_id"] is None
    assert payload["thread_root_message_id"] is None
    assert payload["messages"][0]["id"] == SOURCE_ID
    assert payload["messages"][0]["reply_to_message_id"] is None
    assert payload["next_cursor"] == "next:opaque+/="


@pytest.mark.parametrize("ancestry,expected_target", [
    ({"reply_to_message_id": None, "thread_id": None, "thread_root_message_id": None}, SOURCE_ID),
    ({"reply_to_message_id": None, "thread_id": THREAD_ID, "thread_root_message_id": None}, SOURCE_ID),
    ({"reply_to_message_id": None, "thread_id": THREAD_ID, "thread_root_message_id": SOURCE_ID}, SOURCE_ID),
    ({"reply_to_message_id": None, "thread_id": THREAD_ID, "thread_root_message_id": ROOT_ID}, SOURCE_ID),
    ({"reply_to_message_id": PARENT_ID, "thread_id": None, "thread_root_message_id": None}, SOURCE_ID),
    ({"reply_to_message_id": PARENT_ID, "thread_id": THREAD_ID, "thread_root_message_id": ROOT_ID}, SOURCE_ID),
])
def test_real_sdk_trigger_controls_reply_route_without_inventing_ancestry(sdk, ancestry, expected_target):
    sdk.source.update(ancestry)
    identity = sdk.client.get_identity("agent")
    received = identity.get_imessage(SOURCE_ID)
    meta = {"conversation_id": CONVERSATION_ID, **source_metadata(received, "received-event")}

    assert {key: meta[key] for key in ancestry} == ancestry
    assert meta["imessage_reply_target"] == expected_target
    assert auto_reply_kwargs(meta) == (
        {"reply_to_message_id": SOURCE_ID, "plain_reply_fallback": True} if expected_target else {}
    )

    # Saved inputs must preserve the API projection, including unresolved ancestry.
    store = IMessageState(BridgeConfig(identity="agent", base_url="https://example.com"))
    assert store.admit("contact", received.content, meta)
    restored = store.replay_pending()[0]["meta"]
    assert {key: restored["imessage_sources"][0][key] for key in ancestry} == ancestry
    assert auto_reply_kwargs(restored) == auto_reply_kwargs(meta)
    assert not posts(sdk)


def test_real_sdk_absent_ancestry_stays_unknown(sdk):
    for field in ("reply_to_message_id", "thread_id", "thread_root_message_id"):
        sdk.source.pop(field)
    received = sdk.client.get_identity("agent").get_imessage(SOURCE_ID)
    meta = source_metadata(received, "received-event")
    assert meta["message_id"] == SOURCE_ID
    assert meta["reply_to_message_id"] is None
    assert meta["thread_id"] is None
    assert meta["thread_root_message_id"] is None
    assert auto_reply_kwargs(meta) == {"reply_to_message_id": SOURCE_ID, "plain_reply_fallback": True}


def test_real_sdk_unavailable_backend_prevents_targeted_send(sdk, monkeypatch):
    active_source(monkeypatch)
    sdk.thread_status = 404
    result, payload = tool(sdk, conversation_id=CONVERSATION_ID, text="Answer")
    assert result["isError"] is True
    assert payload["status_code"] == 404
    assert not posts(sdk)


def test_real_sdk_target_rejection_does_not_trigger_plain_resend(sdk, monkeypatch):
    active_source(monkeypatch)
    sdk.send_status = 422
    result, payload = tool(sdk, conversation_id=CONVERSATION_ID, text="Answer")
    assert result["isError"] is True
    assert payload["status_code"] == 422
    assert payload["error_code"] == "imessage_reply_target_unavailable"
    assert len(posts(sdk)) == 1
    assert json.loads(posts(sdk)[0].content)["reply_to_message_id"] == SOURCE_ID


def test_real_sdk_flag_off_keeps_ordinary_tool_wire_shape(sdk, monkeypatch):
    monkeypatch.setenv("INKBOX_IMESSAGE_THREADED_REPLIES", "false")
    result, payload = tool(sdk, conversation_id=CONVERSATION_ID, text="Answer")
    assert not result.get("isError"), payload
    assert json.loads(posts(sdk)[0].content) == {"conversation_id": CONVERSATION_ID, "text": "Answer"}
    assert not [request for request in sdk.requests if request.url.path.endswith("/thread")]
    assert payload == {"sent": True, "id": OUTBOUND_ID}


def test_real_sdk_gateway_routes_and_correlates_native_queued_reply(sdk):
    sdk.outbound.update(reply_to_message_id=SOURCE_ID, thread_id=THREAD_ID, thread_root_message_id=SOURCE_ID)
    gateway = InkboxGateway(BridgeConfig(
        identity="agent", base_url="https://example.com", imessage_threaded_replies=True,
    ))
    gateway._inkbox = sdk.client
    gateway._identity = sdk.client.get_identity("agent")
    meta = {
        "conversation_id": CONVERSATION_ID, "message_id": SOURCE_ID,
        "imessage_reply_target": SOURCE_ID, "imessage_sources": [{"id": SOURCE_ID}],
    }
    asyncio.run(gateway.send_to_contact("contact-1", "Answer", "imessage", meta))
    request = posts(sdk)[0]
    assert request.url.params["agent_identity_id"] == IDENTITY_ID
    assert request.headers["Idempotency-Key"].startswith("claude-imsg-")
    assert json.loads(request.content) == {
        "conversation_id": CONVERSATION_ID, "text": "Answer", "reply_to_message_id": SOURCE_ID,
        "plain_reply_fallback": True,
    }
    stored = gateway._channel_store("imessage").lookup_outbound(OUTBOUND_ID)
    assert stored["chat_id"] == "contact-1"
    assert stored["reply_to_message_id"] == SOURCE_ID
    assert stored["thread_id"] == THREAD_ID
    assert stored["status"] == "pending"
