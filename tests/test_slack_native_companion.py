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
