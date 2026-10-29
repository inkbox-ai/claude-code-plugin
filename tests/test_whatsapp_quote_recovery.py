"""Actionable send recovery does not mistake the last rejection for send certainty."""

import asyncio
import json
import threading
from unittest.mock import Mock

import httpx
import pytest
from inkbox import InkboxAPIError

from inkbox_claude.channel_tools import build_channel_tools
from inkbox_claude.tools import CURRENT_SESSION
from inkbox_claude.whatsapp import ReplyPreflightRejected, send_reply
from tests.test_whatsapp_autoquote import admit, bind_tool
from tests.test_whatsapp_v2 import channel as channel, CONVERSATION, MESSAGE


@pytest.mark.parametrize("quoted", [False, True])
@pytest.mark.parametrize("pending", [False, True])
@pytest.mark.parametrize("blocked_operation", ["send", "react", "unreact"])
def test_registered_overlap_result_preserves_original_key_without_raw_error(channel, quoted, pending, blocked_operation):
    chat, _, meta = admit(channel)
    if not quoted:
        meta.pop("whatsapp_reply_source")
    channel.client.get_identity = Mock(return_value=channel.identity)
    session, _ = bind_tool(channel, meta, chat)
    tool = next(t for t in build_channel_tools(channel.client, "agent", channel.gateway.cfg)
                if t.name == "inkbox_send_whatsapp")
    entered, release = threading.Event(), threading.Event()

    def lose_response(**kwargs):
        entered.set()
        assert release.wait(5)
        raise httpx.ReadTimeout("private-response-marker")

    channel.identity.send_whatsapp = Mock(side_effect=lose_response)

    async def run():
        token = CURRENT_SESSION.set(session)
        first = None
        try:
            args = {"conversation_id": CONVERSATION, "text": "Original", "idempotency_key": "original-send"}
            if quoted:
                args["reply_to_message_id"] = MESSAGE
            first = asyncio.create_task(tool.handler(args))
            assert await asyncio.to_thread(entered.wait, 3)
            if not pending:
                release.set()
                await first
            blocked_tool = next(t for t in build_channel_tools(channel.client, "agent", channel.gateway.cfg)
                                if t.name == f"inkbox_{blocked_operation}_whatsapp")
            blocked_args = {
                "send": {"conversation_id": CONVERSATION, "text": "Replacement"},
                "react": {"message_id": MESSAGE, "reaction": "love", "idempotency_key": "new-reaction"},
                "unreact": {"reaction_id": MESSAGE, "idempotency_key": "new-removal"},
            }[blocked_operation]
            result = await blocked_tool.handler(blocked_args)
            body = json.loads(result["content"][0]["text"])
            assert body["error"]
            assert body["idempotency_key"] == "original-send"
            assert "original" in body["recovery"] and "new" in body["recovery"]
            assert "unavailable" not in body["error"].lower()
            assert "private-response-marker" not in json.dumps(result)
            assert channel.identity.send_whatsapp.call_count == 1
        finally:
            release.set()
            if first is not None:
                await first
            CURRENT_SESSION.reset(token)

    asyncio.run(run())


def test_public_rejection_after_unknown_quote_replay_never_permits_correction(channel):
    chat, _, meta = admit(channel)
    channel.client.get_identity = Mock(return_value=channel.identity)
    session, tool = bind_tool(channel, meta, chat)
    args = {"conversation_id": CONVERSATION, "reply_to_message_id": MESSAGE,
            "text": "Original", "idempotency_key": "original-quote"}
    token = CURRENT_SESSION.set(session)
    try:
        channel.identity.send_whatsapp = Mock(side_effect=httpx.ReadTimeout("lost response"))
        with pytest.raises(httpx.ReadTimeout):
            tool(dict(args), lambda: None)
        channel.identity.send_whatsapp = Mock(side_effect=InkboxAPIError(422, {"error": "whatsapp_text_too_long"}))
        with pytest.raises(InkboxAPIError):
            tool(dict(args), lambda: None)
        with pytest.raises(ReplyPreflightRejected):
            tool({**args, "text": "Short", "idempotency_key": "new-quote"}, lambda: None)
        with pytest.raises(ReplyPreflightRejected):
            asyncio.run(send_reply(channel.gateway, chat, "Final", meta))
        assert channel.identity.send_whatsapp.call_count == 1
    finally:
        CURRENT_SESSION.reset(token)


@pytest.mark.parametrize("first_failure", ["timeout", "decode", 502, 503, 504])
def test_actual_sdk_retry_then_public_rejection_keeps_quote_and_recovery_uncertain(channel, monkeypatch, first_failure):
    chat, _, meta = admit(channel)
    channel.client.get_identity = Mock(return_value=channel.identity)
    session, _ = bind_tool(channel, meta, chat)
    tool = next(t for t in build_channel_tools(channel.client, "agent", channel.gateway.cfg)
                if t.name == "inkbox_send_whatsapp")
    transport = channel.client.whatsapp._http._client._transport
    attempts = []

    def handle(request):
        if request.method != "POST" or request.url.path != "/api/v1/whatsapp/messages":
            return transport.handle_request(request)
        attempts.append(request)
        if len(attempts) == 1:
            if first_failure == "timeout":
                raise httpx.ReadTimeout("private-response-marker")
            if first_failure == "decode":
                return httpx.Response(201, content="private-response-marker")
            return httpx.Response(first_failure, json={"detail": "private-response-marker"})
        return httpx.Response(422, json={"detail": {"error": "whatsapp_text_too_long"}})

    monkeypatch.setattr(channel.client.whatsapp._http._client, "_transport", httpx.MockTransport(handle))
    monkeypatch.setattr("inkbox._message_requests.time.sleep", lambda _: None)

    async def run():
        token = CURRENT_SESSION.set(session)
        try:
            args = {"conversation_id": CONVERSATION, "reply_to_message_id": MESSAGE,
                    "text": "Original", "idempotency_key": "original-quote"}
            result = await tool.handler(args)
            body = json.loads(result["content"][0]["text"])
            assert result["is_error"]
            assert body["error"] == "whatsapp_text_too_long"
            assert body["idempotency_key"] == "original-quote"
            assert "do not send a new copy" in body["recovery"]
            assert "not accepted" not in body["recovery"].lower()
            assert "new key" not in body["recovery"].lower()
            assert "private-response-marker" not in json.dumps(result)
            assert len(attempts) == 2
            assert [r.headers["Idempotency-Key"] for r in attempts] == ["original-quote"] * 2
            assert attempts[0].content == attempts[1].content
            result = await tool.handler({**args, "text": "Short", "idempotency_key": "new-quote"})
            blocked = json.loads(result["content"][0]["text"])
            assert result["is_error"] and blocked["idempotency_key"] == "original-quote"
            with pytest.raises(ReplyPreflightRejected):
                await send_reply(channel.gateway, chat, "Final", meta)
            assert len(attempts) == 2 and session._current_channel_tool_delivery
        finally:
            CURRENT_SESSION.reset(token)

    asyncio.run(run())
