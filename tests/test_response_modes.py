"""Current-message response policy, durable quiet context, and host boundaries."""

import asyncio
import json
from copy import deepcopy
from types import SimpleNamespace

import pytest

from inkbox_claude import sessions as sessions_mod
from inkbox_claude.companion import same_author
from inkbox_claude.config import BridgeConfig, read_config
from inkbox_claude.gateway import InkboxGateway
from inkbox_claude.prompts import mentions_agent
from inkbox_claude.sessions import ContactSession, _Turn
from test_companion import Request, drained, event, fixture, harness, uid


@pytest.mark.parametrize("text,expected", [
    ("hi @agent", True), ("@HELPER, hi", True), ("(@agent)", True),
    ("Hi agent", False), ("@agents", False), ("@helper-two", False),
    ("mail@agent.com", False), ("https://example.com/@agent", False),
    ("www.example.com/@helper", False), ("x+@agent", False),
])
def test_explicit_mentions(text, expected):
    assert mentions_agent(text, "helper") is expected


@pytest.mark.parametrize("channel", ["mail", "phone", "imessage"])
@pytest.mark.parametrize("response", ["safe", "relaxed"])
@pytest.mark.parametrize("group", ["auto", "mention"])
@pytest.mark.parametrize("access", ["direct", "sponsored", None, "future"])
def test_current_receipt_gates_initialization(harness, channel, response, group, access):
    async def scenario():
        envelope, pages = fixture(channel)
        message = envelope["data"].get("text_message", envelope["data"].get("message"))
        message["sender_access"] = access
        gw, _ = harness.build(pages, companion_response_mode=response, group_reply_mode=group)
        await gw._handle_webhook(Request(envelope))
        record = (await drained(gw))[0]
        should_wake = (response == "relaxed" or access == "direct") and group == "auto"
        assert len(harness.queries) == int(should_wake)
        assert len(harness.clients) == int(should_wake)
        assert all(item["state"] == "completed" for item in record["events"].values())
        assert bool(record["context"]) is not should_wake
        await gw._cleanup()
    asyncio.run(scenario())


@pytest.mark.parametrize("recipients,text,access,wakes", [
    ({"to_addresses": ["Agent <HELPER@EXAMPLE.COM>"]}, "hello", "direct", True),
    ({"cc_addresses": ["helper@example.com"]}, "hello", "direct", False),
    ({"bcc_addresses": ["helper@example.com"]}, "hello", "direct", False),
    ({"to_addresses": ["someone@example.com"]}, "To: helper@example.com", "direct", False),
    ({"to_addresses": ["other@example.com"]}, "@agent hello", "direct", True),
    ({"to_addresses": ["helper@example.com"]}, "@agent hello", "sponsored", False),
])
def test_companion_mail_to_addressing(harness, recipients, text, access, wakes):
    async def scenario():
        envelope, pages = fixture()
        envelope["data"]["message"].update(recipients, body=text, sender_access=access)
        gw, _ = harness.build(pages, group_reply_mode="mention")
        gw._identity.email_address = "helper@example.com"
        await gw._handle_webhook(Request(envelope))
        await drained(gw)
        assert len(harness.queries) == int(wakes)
        await gw._cleanup()
    asyncio.run(scenario())


@pytest.mark.parametrize("channel", ["mail", "phone", "imessage"])
def test_quiet_live_first_persists_snapshot_and_only_current_mention_wakes(harness, channel):
    async def scenario():
        envelope, pages = fixture(channel)
        pages[1]["items"][-1]["text"] = "@agent welcome"
        scope = {**envelope["companion"], "phase": "live", "sequence": 2}
        sender = pages[0]["items"][0]["author"]
        live = event(scope, sender, 13, "quiet text")
        live["data"].get("text_message", live["data"].get("message"))["sender_access"] = "sponsored"
        gw, transport = harness.build(pages, group_reply_mode="mention")
        await gw._handle_webhook(Request(live))
        await drained(gw)
        assert not harness.queries and not harness.clients and not harness.outputs
        assert len(transport.calls) == 3
        await gw._cleanup()
        restarted, transport = harness.build(pages, group_reply_mode="mention")
        restarted._companion_receiver().recover()
        await drained(restarted)
        await restarted._handle_webhook(Request(envelope))
        await drained(restarted)
        assert not harness.queries
        awake = event({**scope, "sequence": 3}, pages[1]["items"][-1]["author"], 14, "@agent now")
        await restarted._handle_webhook(Request(awake))
        await drained(restarted)
        assert len(harness.queries) == 1
        assert "quiet text" in harness.queries[0] and "@agent welcome" in harness.queries[0]
        assert f"Current source_message_id: {uid(14)}" in harness.queries[0]
        assert not transport.calls
        await restarted._cleanup()
    asyncio.run(scenario())


@pytest.mark.parametrize("phase", ["initialization", "live"])
@pytest.mark.parametrize("author,accepted", [("OWNER@EXAMPLE.COM", True), ("other@example.com", False)])
def test_mail_snapshot_author_matching(harness, phase, author, accepted):
    async def scenario():
        envelope, pages = fixture()
        envelope["companion"]["phase"] = phase
        envelope["data"]["message"]["from_address"] = author
        gw, _ = harness.build(pages)
        await gw._handle_webhook(Request(envelope))
        record = (await drained(gw))[0]
        assert (record["state"] == "initialized") is accepted
        assert len(harness.queries) == int(accepted)
        await gw._cleanup()
    asyncio.run(scenario())


def test_phone_authors_are_not_casefolded():
    assert same_author("mail", "Alice@Example.com", "alice@example.com")
    assert not same_author("imessage", "+15550100001", "+15550100002")
    assert not same_author("phone", "SENDER", "sender")
    assert not same_author("mail", None, None)


@pytest.mark.parametrize("access,text,accepted", [
    ("direct", "@agent allow", True), ("direct", "allow", False),
    ("sponsored", "@agent allow", False), (None, "@agent allow", False),
    ("direct", "@agent unrelated", False),
])
def test_companion_approval_uses_current_gates(harness, access, text, accepted):
    async def scenario():
        envelope, pages = fixture()
        envelope["data"]["message"]["body"] = "@agent help"
        gw, _ = harness.build(pages, group_reply_mode="mention", permission_timeout_s=0.03)
        waiting, decisions = asyncio.Event(), []
        async def receive(_client):
            if len(harness.queries) == 1:
                session = next(iter(gw.sessions.sessions.values()))
                task = asyncio.create_task(session._escalate("permission", "Approve?"))
                while session.pending is None:
                    await asyncio.sleep(0)
                waiting.set()
                decisions.append(await task)
        harness.hooks.receive = receive
        await gw._handle_webhook(Request(envelope))
        await waiting.wait()
        reply = event({**envelope["companion"], "phase": "live", "sequence": 2}, "OWNER@EXAMPLE.COM", 13, text)
        reply["data"]["message"]["sender_access"] = access
        await gw._handle_webhook(Request(reply))
        await drained(gw)
        assert decisions == ["allow" if accepted else None]
        assert "Include @agent" in harness.outputs[0][2]["body_text"]
        await gw._cleanup()
    asyncio.run(scenario())


def test_completed_result_recovers_pre_send_failure_without_rerunning_model(harness, monkeypatch):
    async def scenario():
        envelope, pages = fixture()
        gw, _ = harness.build(pages)
        harness.hooks.reply = "Finished result"
        receiver = gw._companion_receiver()
        receiver.schedule = lambda _: None
        receiver.accept(envelope)
        key = next(iter(receiver.records))
        original = gw.send_to_contact
        async def unavailable(*_args):
            raise ConnectionError("Not submitted to messaging API")
        monkeypatch.setattr(gw, "send_to_contact", unavailable)
        assert await receiver.drain_once(key)
        assert receiver.records[key]["events"][uid(12)]["state"] == "generated"
        assert receiver.records[key]["events"][uid(12)]["reply"] == "Finished result"
        await gw._cleanup()
        restarted, _ = harness.build(pages)
        restarted._companion_receiver().recover()
        await drained(restarted)
        assert len(harness.queries) == 1
        assert harness.outputs == [("mail", uid(12), {"body_text": "Finished result"})]
        await restarted._cleanup()
    asyncio.run(scenario())


def test_ordinary_quiet_messages_persist_without_interrupting_or_retargeting(harness):
    async def scenario():
        _, pages = fixture()
        gw, _ = harness.build(pages, group_reply_mode="mention")
        session = gw.sessions.get("sms:group-one")
        original = {"conversation_kind": "group", "conversation_id": "group-one", "sender": "+15555550101"}
        session.mode, session.reply_meta = "sms", original
        active = _Turn(text="old", mode="sms", reply_meta=original)
        session._current_turn, session._turn_active = active, True
        interrupts = []
        async def interrupt():
            interrupts.append(True)
        session._client = SimpleNamespace(interrupt=interrupt)
        quiet = {**original, "sender": "+15555550102", "message_id": "quiet-one"}
        await session.handle_inbound("background", "sms", quiet)
        assert not interrupts and session.reply_meta == original
        assert session._queue.empty() and not harness.queries
        session._client = None
        session._current_turn, session._turn_active = None, False
        await gw._cleanup()
        restarted, _ = harness.build(pages, group_reply_mode="mention")
        resumed = restarted.sessions.get("sms:group-one")
        await resumed.handle_inbound("@agent respond", "sms", original)
        await resumed._worker
        assert len(harness.queries) == 1 and "background" in harness.queries[0]
        assert not resumed._context
        assert not restarted.sessions.get("sms:group-two")._context
        await restarted._cleanup()
    asyncio.run(scenario())


def test_ordinary_approval_is_raw_asked_sender_only_and_mention_exempt(harness):
    async def scenario():
        _, pages = fixture()
        gw, _ = harness.build(pages, group_reply_mode="mention")
        session = gw.sessions.get("sms:group")
        original = {"sender": "+15555550101", "conversation_id": "group", "conversation_kind": "group"}
        session.mode, session.reply_meta = "sms", original
        task = asyncio.create_task(session._escalate("permission", "Approve?"))
        while session.pending is None:
            await asyncio.sleep(0)
        for meta in ({**original, "sender": "+15555550102"}, {**original, "reaction": "like"}):
            await session.handle_inbound("framed allow", "sms", {**meta, "raw_text": "allow"})
            assert not session.pending.future.done()
        await session.handle_inbound("framed allow", "sms", {**original, "raw_text": "allow"})
        assert await task == "allow"
        assert not harness.queries
        await gw._cleanup()
    asyncio.run(scenario())


def test_reset_fences_late_host_startup_and_preserves_chosen_resume_on_failure(harness):
    async def scenario():
        _, pages = fixture()
        gw, _ = harness.build(pages)
        session = gw.sessions.get("contact")
        session.resume_session_id = "saved-session"
        entered, release = asyncio.Event(), asyncio.Event()
        async def connect(_client):
            entered.set()
            await release.wait()
        harness.hooks.connect = connect
        startup = asyncio.create_task(session._ensure_client())
        await entered.wait()
        await session.close()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await startup
        assert session._client is None and session.resume_session_id == "saved-session"
        assert not harness.queries
        await gw._cleanup()
    asyncio.run(scenario())


@pytest.mark.parametrize("name,value", [("INKBOX_GROUP_REPLY_MODE", "random"), ("INKBOX_COMPANION_RESPONSE_MODE", "random")])
def test_invalid_response_modes_fail_clearly(monkeypatch, name, value):
    monkeypatch.setenv(name, value)
    with pytest.raises(ValueError, match=name):
        read_config()
