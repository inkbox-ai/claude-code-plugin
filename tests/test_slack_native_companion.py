"""Signed channel-wide Slack Companion admission through the native Claude queue."""
import asyncio
from copy import deepcopy
from types import SimpleNamespace as NS
from unittest.mock import Mock
from uuid import UUID

import pytest
from tests.test_companion import harness, fixture, Request, drained, uid
from tests.test_slack_companion_adapter import client, receipt, SOURCE, CONNECTION, IDENTITY, TIMESTAMP


@pytest.fixture(autouse=True)
def real_http_responses(monkeypatch):
    from aiohttp import web
    from inkbox_claude import gateway
    monkeypatch.setattr(gateway, "web", web)


def test_current_slack_source_routes_timeline_and_thread_in_one_native_session(harness, client, receipt):
    async def run():
        envelope,pages=fixture()
        scope={**envelope["companion"],"channel":"slack"}
        for page in pages:
            page["channel"]="slack"
            page["reply_context"]={"channel":"slack","conversation_id":scope["conversation_id"],
                "connection_id":CONNECTION,"slack_conversation_id":"CEXAMPLE","thread_ts":None}
            for entry in page["items"]:
                entry["author"]="THOME:UALICE"
        gw,_=harness.build(pages,slack_enabled=True,group_reply_mode="mention")
        gw._identity.id=UUID(IDENTITY)
        gw._inkbox.slack=client.slack
        client.slack.send_message.return_value=NS(status="sent",id="action")
        harness.hooks.reply="Current answer"
        envelope=deepcopy(receipt)
        envelope["companion"]=scope
        envelope["data"].update(message_kinds=["channel","mention"],sender_access="direct")
        assert (await gw._handle_webhook(Request(envelope))).status==200
        record=(await drained(gw))[0]
        assert record["state"]=="initialized"
        assert len(harness.queries)==1
        assert client.slack.send_message.call_args.kwargs["thread_ts"] is None
        assert "Please help" in harness.queries[0]
        original_session=record["host_session_id"]
        # A subsequent source in another thread keeps channel-wide model context,
        # while delivery/status/controls bind to this exact current route.
        live=deepcopy(envelope)
        live["id"]="second-receipt"
        live["companion"].update(phase="live",sequence=2)
        live["data"].update(message_ts="1791000000.000004",thread_ts=TIMESTAMP)
        live["data"]["event"]["text"]="Second task"
        archived=client.slack.list_archived_messages.return_value.messages[0]
        archived.id=UUID(uid(13));archived.message_ts="1791000000.000004";archived.thread_ts=TIMESTAMP
        assert (await gw._handle_webhook(Request(live,request_id="request-two"))).status==200
        record=(await drained(gw))[0]
        assert len(harness.queries)==2
        assert record["host_session_id"]==original_session
        assert len(gw.sessions.sessions)==1
        assert client.slack.send_message.call_args.kwargs["thread_ts"]==TIMESTAMP
        assert len(client.slack.send_message.call_args_list)==2
        await gw._cleanup()
    asyncio.run(run())


def test_unsigned_slack_is_rejected_even_when_general_signature_setting_is_off(harness, receipt):
    async def run():
        _,pages=fixture()
        gw,_=harness.build(pages,slack_enabled=True,require_signature=False)
        assert (await gw._handle_webhook(Request(receipt,signed=False))).status==401
        assert harness.queries==[]
        await gw._cleanup()
    asyncio.run(run())


def test_mention_prefixed_companion_approval_answers_without_second_query(harness, client, receipt):
    from inkbox_claude.escalation import parse_permission_reply

    async def run():
        initial, pages = fixture()
        scope = {**initial["companion"], "channel": "slack"}
        for page in pages:
            page["channel"] = "slack"
            page["reply_context"] = {"channel": "slack", "conversation_id": scope["conversation_id"],
                "connection_id": CONNECTION, "slack_conversation_id": "CEXAMPLE", "thread_ts": None}
            for entry in page["items"]:
                entry["author"] = "THOME:UALICE"
        gw, _ = harness.build(pages, slack_enabled=True, group_reply_mode="mention")
        gw._identity.id = UUID(IDENTITY)
        gw._inkbox.slack = client.slack
        client.slack.send_message.return_value = NS(status="sent", id="action")
        envelope = deepcopy(receipt)
        envelope["companion"] = scope
        envelope["data"].update(message_kinds=["channel", "mention"], sender_access="direct")
        answered = []

        async def during_query(_client, _text):
            session = next(iter(gw.sessions.sessions.values()))
            task = asyncio.create_task(session._escalate("permission", "Allow once?"))
            while session.pending is None:
                await asyncio.sleep(0)
            assert "Mention the bot" in session.pending.prompt_text
            live = deepcopy(envelope)
            live["id"] = "approval-receipt"
            live["companion"].update(phase="live", sequence=2)
            live["data"].update(message_ts="1791000000.000004")
            live["data"]["event"]["text"] = "<@UAGENT> 1"
            archived = client.slack.list_archived_messages.return_value.messages[0]
            archived.id = UUID(uid(13))
            archived.message_ts = "1791000000.000004"
            assert (await gw._handle_webhook(Request(live, request_id="approval-request"))).status == 200
            answered.append(parse_permission_reply(await asyncio.wait_for(task, 2)))
            assert not session._interrupting

        harness.hooks.query = during_query
        assert (await gw._handle_webhook(Request(envelope))).status == 200
        record = (await drained(gw))[0]
        assert answered == ["allow"] and len(harness.queries) == 1
        assert record["events"][uid(13)]["state"] == "completed"
        await gw._cleanup()
    asyncio.run(run())


@pytest.mark.parametrize("allowed", ["UALICE", "TINSTALL:UALICE", "THOME:UALICE"])
def test_ordinary_slack_companion_accepts_documented_author_aliases(harness, client, receipt, allowed):
    async def run():
        initial, pages = fixture()
        gw, _ = harness.build(pages, slack_enabled=True, allowed_users=[allowed])
        gw._identity.id = UUID(IDENTITY)
        gw._inkbox.slack = client.slack
        envelope = deepcopy(receipt)
        envelope["companion"] = {**initial["companion"], "channel": "slack", "phase": "ordinary"}
        envelope["companion"].pop("activation_id")
        envelope["data"].update(message_kinds=["channel", "mention"], sender_access="direct")
        assert (await gw._handle_webhook(Request(envelope))).status == 200
        await drained(gw)
        assert len(harness.queries) == 1
        await gw._cleanup()
    asyncio.run(run())


@pytest.mark.parametrize("phase", ["pending", "generated", "submitted"])
def test_native_stop_snapshot_survives_companion_restart(harness, client, receipt, monkeypatch, phase):
    async def run():
        initial, pages = fixture()
        gw, _ = harness.build(pages, slack_enabled=True)
        gw._identity.id = UUID(IDENTITY)
        gw._inkbox.slack = client.slack
        receiver = gw._companion_receiver()
        monkeypatch.setattr(receiver, "schedule", Mock())
        envelope = deepcopy(receipt)
        envelope["companion"] = {**initial["companion"], "channel": "slack", "phase": "ordinary"}
        envelope["companion"].pop("activation_id")
        envelope["data"].update(message_kinds=["channel", "mention"], sender_access="direct")
        assert (await gw._handle_webhook(Request(envelope))).status == 200
        key, record = next(iter(receiver.records.items()))
        stored = record["events"][SOURCE]
        stored.update(state=phase, reply="Saved answer", host_fenced=True,
                      reply_meta=stored["message"]["slack_route"], submitted_meta=stored["message"]["slack_route"])
        receiver.save(key)
        gw._channel_store("slack").consume_control("native-stop", [{
            "chat_id": record["session_key"], "source_event_ids": [envelope["id"]],
        }])
        await gw._cleanup()
        restarted, _ = harness.build(pages, slack_enabled=True)
        restarted._identity.id = UUID(IDENTITY)
        restarted._inkbox.slack = client.slack
        monkeypatch.setattr("inkbox_claude.runtime.saved_answer", lambda *_args: pytest.fail("Stopped work must not recover a send"))
        restarted._companion_receiver().recover()
        [recovered] = await drained(restarted)
        assert recovered["events"][SOURCE]["state"] == ("unconfirmed" if phase == "submitted" else "discarded")
        assert harness.queries == []
        client.slack.send_message.assert_not_called()
        # The tombstone covers only its captured input, not later work in this scope.
        fresh = deepcopy(envelope)
        fresh["id"] = "fresh-event"
        fresh["companion"]["sequence"] = 2
        fresh["data"]["message_ts"] = "1791000000.000004"
        archived = client.slack.list_archived_messages.return_value.messages[0]
        archived.id = UUID(uid(13))
        archived.message_ts = fresh["data"]["message_ts"]
        assert (await restarted._handle_webhook(Request(fresh, request_id="fresh-request"))).status == 200
        await drained(restarted)
        assert len(harness.queries) == 1
        await restarted._cleanup()
    asyncio.run(run())


def test_active_native_stop_clears_companion_working_status(harness, client, receipt):
    async def run():
        initial, pages = fixture()
        gw, _ = harness.build(pages, slack_enabled=True)
        gw._identity.id = UUID(IDENTITY)
        gw._inkbox.slack = client.slack
        statuses = []

        async def activity(_chat, _mode, meta, state):
            statuses.append((meta["source_event_id"], state))

        gw.sessions.activity_fn = activity
        started, released = asyncio.Event(), asyncio.Event()

        async def connected(host):
            async def interrupt():
                released.set()
            host.interrupt = interrupt

        async def receive(_host):
            started.set()
            await released.wait()

        harness.hooks.connect, harness.hooks.receive = connected, receive
        envelope = deepcopy(receipt)
        envelope["companion"] = {**initial["companion"], "channel": "slack", "phase": "ordinary"}
        envelope["companion"].pop("activation_id")
        envelope["data"].update(message_kinds=["channel", "mention"], sender_access="direct", thread_ts="1791000000.000000")
        client.slack.list_archived_messages.return_value.messages[0].thread_ts = envelope["data"]["thread_ts"]
        try:
            assert (await gw._handle_webhook(Request(envelope))).status == 200
            await asyncio.wait_for(started.wait(), 2)
            stop = {"id": "active-stop", "event_type": "slack.session_stopped", "data": {
                **envelope["data"], "event": {"type": "agent_session_stopped"},
            }}
            assert (await gw._on_slack_received(stop)).status == 200
            [record] = await drained(gw)
            assert (envelope["id"], "accepted") in statuses
            assert statuses[-1] == (envelope["id"], "cancelled")
            assert record["events"][SOURCE]["state"] == "discarded"
            client.slack.send_message.assert_not_called()
        finally:
            released.set()
            await gw._cleanup()
    asyncio.run(run())
