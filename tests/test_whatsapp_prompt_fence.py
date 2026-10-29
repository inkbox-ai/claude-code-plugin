"""Prompt dispatch observes the owning turn's current send outcome."""

import asyncio
from copy import deepcopy
import json
import threading
from unittest.mock import Mock

import httpx
import pytest

from inkbox_claude.channel_tools import build_channel_tools
from inkbox_claude.imessage import IMessageState
from inkbox_claude.tools import CURRENT_SESSION
from inkbox_claude.whatsapp import ReplyPreflightRejected, send_reply
from tests.test_whatsapp_autoquote import admit, bind_tool, posts
from tests.test_whatsapp_v2 import channel as channel, CONVERSATION


@pytest.mark.parametrize("destination", ["source", "group", "other"])
@pytest.mark.parametrize("kind", ["permission", "poll"])
@pytest.mark.parametrize("plain", [False, True])
@pytest.mark.parametrize("pause_at", ["source", "dispatch"])
def test_prompt_rechecks_live_send_outcome_and_allows_exact_recovery(channel, monkeypatch, kind, plain, pause_at, destination):
    chat, _, meta = admit(channel)
    channel.client.get_identity = Mock(return_value=channel.identity)
    session, _ = bind_tool(channel, meta, chat)
    owner = session._current_turn
    session.cfg.permission_timeout_s = 0.1
    tool = next(t for t in build_channel_tools(channel.client, "agent", channel.gateway.cfg)
                if t.name == "inkbox_send_whatsapp")
    release = threading.Event()

    async def run():
        entered = asyncio.Event()
        loop = asyncio.get_running_loop()
        snapshots = []

        async def send(chat_id, text, mode, route):
            snapshots.append(deepcopy(route))
            await send_reply(channel.gateway, chat_id, text, route)
            session.pending.future.set_result("yes")

        session.send_fn = send
        token = CURRENT_SESSION.set(session)
        prompt = None
        try:
            first = {"conversation_id": CONVERSATION, "text": "Starting work"}
            if plain:
                first["idempotency_key"] = "first-plain"
            assert "error" not in json.loads((await tool.handler(first))["content"][0]["text"])
            assert len(posts(channel)) == 1

            original_read = channel.identity.get_whatsapp_message
            original_post = channel.client.whatsapp._http.post
            if pause_at == "source":
                def read(message_id):
                    # Only the prompt read pauses; the overlapping tool can proceed.
                    channel.identity.get_whatsapp_message = original_read
                    loop.call_soon_threadsafe(entered.set)
                    assert release.wait(5)
                    return original_read(message_id)

                channel.identity.get_whatsapp_message = read
            else:
                from inkbox_claude import tools
                original_dispatch = tools._to_thread_drained

                async def dispatch(function, *args, **kwargs):
                    entered.set()
                    await asyncio.to_thread(release.wait, 5)
                    return await original_dispatch(function, *args, **kwargs)

                monkeypatch.setattr(tools, "_to_thread_drained", dispatch)

            prompt = asyncio.create_task(session._escalate(kind, "Allow action?"))
            await asyncio.wait_for(entered.wait(), 2)
            snapshot = snapshots[0]
            assert (snapshot["whatsapp_plain_send"]["outcome"] == "accepted" if plain
                    else snapshot["whatsapp_quote_accepted"] is True)

            def lost_response(*args, **kwargs):
                original_post(*args, **kwargs)
                raise httpx.ReadTimeout("Response unavailable")

            channel.client.whatsapp._http.post = lost_response
            uncertain = {"conversation_id": CONVERSATION, "text": "Additional requested message"}
            if destination == "group":
                uncertain = {"to": ["+15550000002", "+15550000004"], "text": "New group"}
            elif destination == "other":
                uncertain["conversation_id"] = "00000000-0000-0000-0000-000000000077"
            if plain:
                uncertain["idempotency_key"] = "second-plain"
            result = await tool.handler(uncertain)
            recovery = json.loads(result["content"][0]["text"])
            assert "error" in recovery
            request = posts(channel)[1]
            key = request.headers["Idempotency-Key"]
            # The SDK may exhaust same-key transport retries before returning.
            attempted = posts(channel)
            assert all(r.headers["Idempotency-Key"] == key and r.content == request.content
                       for r in attempted[1:])
            assert recovery["idempotency_key"] == key
            assert session._current_turn is owner and session._turn_active and not owner.cancelled
            if destination == "source":
                assert (meta["whatsapp_plain_send"]["outcome"] == "uncertain" if plain
                        else meta["whatsapp_quote_accepted"] is False)
            else:
                assert meta["whatsapp_recipient_sends"][key]["outcome"] == "uncertain"
            channel.client.whatsapp._http.post = original_post
            release.set()
            with pytest.raises(ReplyPreflightRejected):
                await prompt
            assert session.pending is None
            assert posts(channel) == attempted

            # Recovery retains the exact request, including after loading saved state.
            restarted = IMessageState(channel.gateway.cfg, channel="whatsapp")
            try:
                assert restarted.pending_replies() == []
                saved = restarted.interrupted()[0]["meta"]
            finally:
                restarted.close()
            if destination == "source":
                assert (saved["whatsapp_plain_send"]["key"] if plain
                        else saved["whatsapp_explicit_quote_key"]) == key
            else:
                assert saved["whatsapp_recipient_sends"][key]["key"] == key
            owner.reply_meta = saved
            with pytest.raises(ReplyPreflightRejected):
                await session._escalate(kind, "Allow action?")
            assert posts(channel) == attempted
            recovered = await tool.handler({**uncertain, "idempotency_key": key})
            assert "error" not in json.loads(recovered["content"][0]["text"])
            assert len(posts(channel)) == len(attempted) + 1
            replay = posts(channel)[-1]
            assert replay.headers["Idempotency-Key"] == key
            assert replay.content == request.content
            assert await session._escalate(kind, "Allow action?") == "yes"
            assert len(posts(channel)) == len(attempted) + 2
            assert posts(channel)[-1].headers["Idempotency-Key"] != key
            with pytest.raises(ReplyPreflightRejected):
                await send_reply(channel.gateway, chat, "Automatic final", {**saved, "final_reply": True})
            assert len(posts(channel)) == len(attempted) + 2
        finally:
            release.set()
            if prompt is not None:
                await asyncio.gather(prompt, return_exceptions=True)
            CURRENT_SESSION.reset(token)

    asyncio.run(run())
