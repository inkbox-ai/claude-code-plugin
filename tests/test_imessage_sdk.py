"""Exercise native iMessage tools and gateway through the real SDK HTTP stack."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
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
from tests.test_native_channel_sessions import Client
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
    from aiohttp import web
    monkeypatch.setattr("inkbox_claude.gateway.web", web)
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
        "conversation_id": CONVERSATION_ID, "imessage_threaded_replies": True, **source_metadata(message()),
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
        f"/api/v1/imessage/messages/{OUTBOUND_ID}",
    ]
    assert all(request.url.params["agent_identity_id"] == IDENTITY_ID for request in reads)
    assert reads[1].url.params["limit"] == "1"
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


@pytest.mark.parametrize("failure", ["endpoint", "source_conversation", "thread_conversation"])
def test_native_preflight_rejects_invalid_authority_before_media_upload(sdk, monkeypatch, failure):
    from unittest.mock import Mock

    active_source(monkeypatch)
    if failure == "endpoint":
        sdk.thread_status = 404
    elif failure == "source_conversation":
        sdk.source["conversation_id"] = str(UUID(int=20))
    else:
        sdk.page["conversation_id"] = str(UUID(int=20))
    upload = Mock()
    monkeypatch.setattr("inkbox_claude.tools._upload_media_url", upload)
    result, _ = tool(sdk, conversation_id=CONVERSATION_ID, text="Answer", media_path="must-not-read")
    assert result["isError"]
    upload.assert_not_called()
    assert not posts(sdk)
    probes = [request for request in sdk.requests if request.url.path.endswith("/thread")]
    assert len(probes) == 1 and probes[0].url.params["limit"] == "1"


@pytest.mark.parametrize("failure", ["endpoint", "source_conversation", "thread_conversation"])
def test_gateway_preflight_rejection_preserves_unsent_durable_answer(sdk, failure):
    cfg = BridgeConfig(identity="agent", base_url="https://example.com", imessage_threaded_replies=True)
    gateway = InkboxGateway(cfg)
    gateway._inkbox = sdk.client
    gateway._identity = sdk.client.get_identity("agent")
    meta = {"conversation_id": CONVERSATION_ID, "imessage_threaded_replies": True,
            **source_metadata(sdk.source, "original")}
    store = gateway._channel_store("imessage")
    assert store.admit("contact", "Question", meta)
    store.mark(meta, "reply_pending", reply="Saved answer")
    if failure == "endpoint":
        sdk.thread_status = 404
    elif failure == "source_conversation":
        sdk.source["conversation_id"] = str(UUID(int=20))
    else:
        sdk.page["conversation_id"] = str(UUID(int=20))
    try:
        with pytest.raises(Exception):
            asyncio.run(gateway.send_to_contact("contact", "Saved answer", "imessage", meta))
        assert store.summary()["reply_pending"] == 1
        assert store.summary()["sending"] == 0
        assert not posts(sdk)
    finally:
        store.close()


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
    assert payload["sent"] is True and payload["id"] == OUTBOUND_ID
    assert payload["delivery_final"] is False
    assert payload["service"] is None


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


@pytest.mark.parametrize("destination", [{"conversation_id": "other-conversation"}, {"to": ["+15550002222"]}])
def test_deliberate_separate_send_has_no_inherited_source_or_answer_suppression(sdk, monkeypatch, destination):
    from unittest.mock import Mock
    session, _ = active_source(monkeypatch)
    upload=Mock(return_value="https://example.com/synthetic-media")
    monkeypatch.setattr("inkbox_claude.tools._upload_media_url",upload)
    result, payload = tool(sdk, text="Separate instruction", media_path="synthetic-path", **destination)
    assert not result.get("isError") and payload["sent"] is True
    upload.assert_called_once()
    assert len(posts(sdk)) == 1
    body = json.loads(posts(sdk)[0].content)
    expected = {**destination, "text": "Separate instruction", "media_urls": ["https://example.com/synthetic-media"]}
    if "to" in expected:
        expected["to"] = expected["to"][0]
    assert body == expected
    assert session._imessage_tool_outputs == []
    assert [request.url.path for request in sdk.requests if "/imessage/" in request.url.path and request.method == "GET"] == [f"/api/v1/imessage/messages/{OUTBOUND_ID}"]
    assert posts(sdk)[0].url.params["agent_identity_id"] == IDENTITY_ID


@pytest.mark.parametrize("destination", [{"conversation_id": "other-conversation"}, {"to": ["+15550002222"]}])
@pytest.mark.parametrize("state", ["ended", "stopped", "disabled"])
def test_independent_send_still_requires_current_native_owner(sdk, monkeypatch, destination, state):
    from unittest.mock import Mock
    session, _ = active_source(monkeypatch)
    if state == "ended":
        session._current_turn = None
    elif state == "stopped":
        session._interrupting = True
    else:
        monkeypatch.setenv("INKBOX_IMESSAGE_THREADED_REPLIES", "false")
    upload = Mock()
    monkeypatch.setattr("inkbox_claude.tools._upload_media_url", upload)
    # The tool captures the original owner before the identity read yields.
    if state == "ended":
        session, _ = active_source(monkeypatch)
        original = sdk.client.get_identity
        def lose_owner(*args, **kwargs):
            result = original(*args, **kwargs)
            session._current_turn = None
            return result
        monkeypatch.setattr(sdk.client, "get_identity", lose_owner)
    result, _ = tool(sdk, text="Separate instruction", media_path="synthetic-path", **destination)
    assert result["isError"] and not posts(sdk)
    upload.assert_not_called()


@pytest.mark.parametrize("failure", ["correlation", "notice"])
def test_accepted_tool_send_is_not_rejected_by_local_tracking_failure(sdk, monkeypatch, failure):
    session, _ = active_source(monkeypatch)
    cfg = BridgeConfig(identity="agent", base_url="https://example.com")
    store = IMessageState(cfg)
    store.mark_delivery_failed(OUTBOUND_ID, {"chat_id": session.chat_id, "meta": {"conversation_id": CONVERSATION_ID}})
    if failure == "correlation":
        monkeypatch.setattr(IMessageState, "record_outbound", lambda *_args: (_ for _ in ()).throw(OSError("disk unavailable")))
    else:
        monkeypatch.setattr(session, "buffer_delivery_notice", lambda *_args: (_ for _ in ()).throw(OSError("disk unavailable")))
    result, payload = tool(sdk, conversation_id=CONVERSATION_ID, text="Answer")
    assert not result.get("isError") and payload["sent"] is True
    assert payload["id"] == OUTBOUND_ID and "Do not resend" in payload["warning"]
    assert len(posts(sdk)) == 1 and len(session._imessage_tool_outputs) == 1
    if failure == "notice":
        assert len(store.pending_failure_notices()) == 1
        assert store.lookup_outbound(OUTBOUND_ID)["status"] == "failed"


@pytest.mark.parametrize("when", ["before_upload", "after_upload"])
def test_active_native_cancellation_cannot_upload_or_send_after_ownership_is_lost(sdk, monkeypatch, when):
    from unittest.mock import Mock
    session,_=active_source(monkeypatch)
    def uploading(*args):
        session._interrupting=True
        return "https://example.com/synthetic-media"
    upload=Mock(side_effect=uploading)
    monkeypatch.setattr("inkbox_claude.tools._upload_media_url",upload)
    if when == "before_upload":session._interrupting=True
    result,_=tool(sdk,conversation_id=CONVERSATION_ID,text="Answer",media_path="synthetic-path")
    assert result["isError"] and not posts(sdk)
    assert upload.call_count == (0 if when == "before_upload" else 1)


def test_triggerless_proactive_imessage_keeps_ordinary_sdk_send(sdk):
    result,_=tool(sdk,to=["+15550002222"],text="Explicit proactive message")
    assert not result.get("isError")
    body=json.loads(posts(sdk)[0].content)
    assert body["to"]=="+15550002222"
    assert "reply_to_message_id" not in body


@pytest.mark.parametrize("name,args", [
    ("inkbox_get_imessage_thread", {"message_id": SOURCE_ID}),
    ("inkbox_get_imessage_conversation_thread", {"conversation_id": "other", "thread_id": THREAD_ID}),
])
def test_active_native_thread_reads_cannot_expand_to_another_conversation(sdk, monkeypatch, name, args):
    active_source(monkeypatch)
    sdk.source["conversation_id"]="other"
    result,_=tool(sdk,name,**args)
    assert result["isError"]
    assert not any("/thread" in request.url.path for request in sdk.requests)


def test_companion_cannot_expand_supplied_native_history(sdk, monkeypatch):
    session,_=active_source(monkeypatch)
    session._current_turn.reply_meta["companion"]=True
    result,_=tool(sdk,"inkbox_get_imessage_thread",message_id=SOURCE_ID)
    assert result["isError"]
    assert not any("/imessage/" in request.url.path for request in sdk.requests)

def reaction_gateway(sdk):
    cfg = BridgeConfig(identity="agent", base_url="https://example.com",
                       allow_all_users=True, imessage_threaded_replies=True)
    gw = InkboxGateway(cfg)
    gw._identity = sdk.client.get_identity("agent")
    gw._resolve_contact_full = AsyncMock(return_value={"id": "contact-1"})
    gw._lookup_imessage_conversation_summary = AsyncMock(return_value=None)
    session = make_session([])
    session.cfg, session.send_fn, session.receipt_fn = cfg, gw.send_to_contact, gw._channel_receipt
    host = session._client = Client()

    def get(chat_id):
        assert chat_id == session.chat_id
        return session

    gw.sessions = SimpleNamespace(sessions={session.chat_id: session}, get=get)
    return gw, session, host


def reaction_event(**changes):
    return {"id": "reaction-event", "data": {"reaction": {
        "id": "reaction-one", "direction": "inbound", "remote_number": "+15555550123",
        "conversation_id": CONVERSATION_ID, "target_message_id": SOURCE_ID,
        "reaction": "question", **changes,
    }}}


def close_channel_stores(gw):
    for store in gw._channel_stores.values():
        store.close()


def test_native_reaction_sends_to_target_once_across_restart(sdk):
    async def run():
        gw, session, host = reaction_gateway(sdk)
        try:
            await gw._on_imessage_reaction_received(reaction_event())
            await session._worker
            assert len(host.queries) == 1
            sends = [request for request in sdk.requests if request.method == "POST"]
            assert len(sends) == 1
            assert json.loads(sends[0].content)["reply_to_message_id"] == SOURCE_ID
            assert json.loads(sends[0].content)["text"] == "Answer"
            assert gw._channel_store("imessage").summary()["done"] == 1
            duplicate = await gw._on_imessage_reaction_received(reaction_event())
            assert json.loads(duplicate.text)["deduped"]
            assert len(host.queries) == 1
        finally:
            close_channel_stores(gw)
        restarted, fresh, fresh_host = reaction_gateway(sdk)
        try:
            duplicate = await restarted._on_imessage_reaction_received({**reaction_event(), "id": "redelivered-event"})
            assert json.loads(duplicate.text)["deduped"]
            assert fresh_host.queries == [] and fresh._worker is None
            assert len([request for request in sdk.requests if request.method == "POST"]) == 1
        finally:
            close_channel_stores(restarted)
    asyncio.run(run())


def test_native_reaction_admission_failure_can_retry_without_losing_target(sdk):
    async def run():
        gw, session, _host = reaction_gateway(sdk)
        session.handle_inbound = AsyncMock(side_effect=RuntimeError("Admission unavailable"))
        event = reaction_event()
        event.pop("id")  # The stable reaction ID is sufficient when the envelope omits one.
        try:
            with pytest.raises(RuntimeError, match="Admission unavailable"):
                await gw._on_imessage_reaction_received(event)
            assert gw._channel_store("imessage").replay_pending() == []
            session.handle_inbound.side_effect = None
            await gw._on_imessage_reaction_received(event)
            [receipt] = gw._channel_store("imessage").replay_pending()
            assert receipt["meta"]["imessage_reply_target"] == SOURCE_ID
            assert receipt["meta"]["imessage_event_id"] == "reaction:reaction-one"
            assert receipt["meta"]["reaction"] == "question"
        finally:
            close_channel_stores(gw)
    asyncio.run(run())


@pytest.mark.parametrize("missing", ["target_message_id", "receipt_id"])
def test_native_reaction_without_stable_source_does_not_start_work(sdk, missing):
    async def run():
        gw, session, host = reaction_gateway(sdk)
        event = reaction_event()
        if missing == "receipt_id":
            event.pop("id")
            event["data"]["reaction"].pop("id")
        else:
            event["data"]["reaction"].pop(missing)
        try:
            response = await gw._on_imessage_reaction_received(event)
            assert json.loads(response.text)["ignored"] == "reaction-without-source"
            assert session._worker is None and host.queries == []
            assert not any(request.method == "POST" for request in sdk.requests)
        finally:
            close_channel_stores(gw)
    asyncio.run(run())


def test_native_reaction_waits_behind_active_work_without_interrupt(sdk):
    async def run():
        gw, session, host = reaction_gateway(sdk)
        host.gate = asyncio.Event()
        meta = {"conversation_id": CONVERSATION_ID, "sender": "+15555550123",
                **source_metadata(message(), "original-event")}
        store = gw._channel_store("imessage")
        try:
            store.admit(session.chat_id, "Original request", meta)
            await session.handle_inbound("Original request", "imessage", meta)
            await asyncio.wait_for(host.started.wait(), 2)
            await gw._on_imessage_reaction_received(reaction_event())
            assert len(host.queries) == 1 and host.interrupts == 0
            assert store.summary()["pending"] == 1 and store.summary()["running"] == 1
            host.gate.set()
            await asyncio.wait_for(session._worker, 2)
            assert len(host.queries) == 2 and host.interrupts == 0
            assert store.summary()["done"] == 2
            sends = [request for request in sdk.requests if request.method == "POST"]
            assert len(sends) == 2
            assert all(json.loads(request.content)["reply_to_message_id"] == SOURCE_ID for request in sends)
        finally:
            host.gate.set()
            close_channel_stores(gw)
    asyncio.run(run())


def test_quiet_group_reactions_do_not_replace_or_duplicate_source_context(sdk):
    async def run():
        gw, session, host = reaction_gateway(sdk)
        session.cfg.group_reply_mode = "mention"
        session.chat_id = f"imessage:{CONVERSATION_ID}"
        gw.sessions.sessions = {session.chat_id: session}
        gw._lookup_imessage_conversation_summary.return_value = {"is_group": True}
        original = message(remote_number="+15555550123", content="Quiet conversation")
        try:
            await gw._on_imessage_received({"id": "source-event", "data": {"message": original}})
            await gw._on_imessage_reaction_received(reaction_event())
            assert len(session._context) == 2
            assert session._context[0]["id"] != session._context[1]["id"]
            await gw._on_imessage_reaction_received(reaction_event(id="another-reaction", reaction="like"))
            assert len(session._context) == 3
            assert gw._channel_store("imessage").summary()["done"] == 3
            assert host.queries == [] and session._worker is None
            assert not any(request.method == "POST" for request in sdk.requests)
        finally:
            close_channel_stores(gw)
    asyncio.run(run())
