"""Fresh inputs use plain chat replies while explicit and saved quotes remain intact."""

import asyncio
import json
from copy import deepcopy
from types import SimpleNamespace as NS
from unittest.mock import Mock
from uuid import UUID

import pytest

from inkbox_claude.gateway import InkboxGateway
from inkbox_claude.imessage import IMessageState
from inkbox_claude.tools import CURRENT_SESSION
from inkbox_claude.whatsapp import receive, send_reply
from inkbox_claude.whatsapp_v2 import receive_reaction
from tests.test_whatsapp_autoquote import admit, bind_tool, final, posts
from tests.test_whatsapp_v2 import channel as channel, CONVERSATION, MESSAGE


@pytest.mark.parametrize("quoted_input", [False, True])
def test_fresh_final_is_plain_after_restart_even_for_quoted_input(channel, quoted_input):
    chat_id, _, meta = admit(channel, legacy=False,
        reply_to_message_id=str(UUID(int=99)) if quoted_input else None)
    store = channel.gateway._channel_store("whatsapp")
    store.mark(meta, "reply_pending", reply="Answer")
    pending = IMessageState(channel.gateway.cfg, channel="whatsapp").pending_replies()[0]
    assert pending["meta"]["whatsapp_reply_source"]["reply_style"] == "plain"
    asyncio.run(send_reply(channel.gateway, chat_id, pending["reply"],
        {**pending["meta"], "final_reply": True}))
    assert json.loads(posts(channel)[0].content) == {
        "conversation_id": CONVERSATION, "text": "Answer"}


@pytest.mark.parametrize("explicit", [False, True])
@pytest.mark.parametrize("implicit_destination", [False, True])
def test_fresh_tool_send_quotes_only_explicit_target(channel, explicit, implicit_destination):
    chat_id, _, meta = admit(channel, legacy=False)
    session, tool = bind_tool(channel, meta, chat_id)
    args = {"conversation_id": CONVERSATION, "text": "Answer"}
    if implicit_destination:
        args.pop("conversation_id")
    if explicit:
        args["reply_to_message_id"] = MESSAGE
    token = CURRENT_SESSION.set(session)
    try:
        tool(args, lambda: None)
    finally:
        CURRENT_SESSION.reset(token)
    expected = {"conversation_id": CONVERSATION, "text": "Answer"}
    if explicit:
        expected["reply_to_message_id"] = MESSAGE
    assert json.loads(posts(channel)[0].content) == expected
    assert session._current_channel_tool_delivery


def test_fresh_reaction_response_is_plain(channel):
    asyncio.run(receive_reaction(channel.gateway, {"data": {"reaction": deepcopy(channel.reaction)}}))
    _, _, meta = channel.inbound.call_args.args
    final(channel, channel.gateway.sessions.get.call_args.args[0], meta)
    assert "reply_to_message_id" not in json.loads(posts(channel)[0].content)


def test_captionless_images_use_distinct_native_attachment_paths(channel, monkeypatch, tmp_path):
    from inkbox_claude import gateway as gateway_module
    # Exercise the native attachment-note path rather than mocking the entire helper.
    channel.gateway._with_media = InkboxGateway._with_media.__get__(channel.gateway)
    channel.identity.get_whatsapp_media = Mock(side_effect=lambda media_id: NS(
        status="ready", url=f"https://example.test/{media_id}.png", content_type="image/png", size=5))
    async def download(items, *, prefix):
        path = tmp_path / f"{prefix}.png"
        path.write_bytes(b"image")
        return [{"path": str(path), "content_type": items[0]["content_type"]}]
    monkeypatch.setattr(gateway_module, "download_media", download)
    for index in (20, 21):
        message_id, media_id = str(UUID(int=index)), str(UUID(int=index + 100))
        message = {**channel.message, "id": message_id, "content": "",
            "media": [{"id": media_id, "status": "ready"}]}
        asyncio.run(receive(channel.gateway, {"id": f"event-{index}", "data": {"message": message}}))
        body, mode, meta = channel.inbound.call_args.args
        assert f"whatsapp-{message_id}.png" in body
        assert "Read tool" in body and "image/png" in body
        assert mode == "whatsapp" and meta["media"] is True
    assert channel.inbound.await_count == 2
    assert channel.identity.get_whatsapp_media.call_count == 2
