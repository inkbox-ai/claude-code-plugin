"""Companion conformance at the gateway, SDK pagination, and Claude query boundary."""

import asyncio
import hashlib
import hmac
import json
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest

from inkbox_claude import sessions as sessions_mod
from inkbox_claude.config import BridgeConfig
from inkbox_claude.gateway import InkboxGateway
from inkbox_claude.sessions import SessionManager


def uid(number):
    return str(UUID(int=number))


def fixture(channel="mail", *, activation=2, scope=1, conversation=3):
    sponsor = "owner@example.com" if channel == "mail" else "+15555550101"
    fred = "fred@example.com" if channel == "mail" else "+15555550102"
    context = {"channel": channel, "conversation_id": uid(conversation)}
    if channel == "mail":
        context.update(reply_to_message_id=uid(12), to=[sponsor], cc=[fred])
    entries = [
        {"id": uid(10), "author": fred, "text": "/clear", "historical": True, "is_trigger": False},
        {"id": uid(11), "author": fred, "text": "YES", "historical": True, "is_trigger": False},
        {
            "id": uid(12),
            "author": sponsor,
            "text": "Welcome",
            "historical": False,
            "is_trigger": True,
        },
    ]
    for entry in entries:
        entry.update(occurred_at="2026-01-01T00:00:00Z", attachments=[])
    entries[0]["attachments"] = [{"id": uid(20), "filename": "notes.txt"}]
    base = {
        "scope_id": uid(scope),
        "activation_id": uid(activation),
        "conversation_id": uid(conversation),
        "channel": channel,
        "reply_context": context,
        "notices": [
            {
                "code": "history_gap",
                "level": "info",
                "message": "Some earlier messages are unavailable.",
            }
        ],
    }
    pages = [
        {
            **deepcopy(base),
            "items": entries[:2],
            "history_complete": False,
            "next_cursor": "page-two",
        },
        {**deepcopy(base), "items": entries[1:], "history_complete": True, "next_cursor": None},
    ]
    metadata = {
        **{k: base[k] for k in ("scope_id", "activation_id", "conversation_id", "channel")},
        "phase": "initialization",
        "sequence": 1,
    }
    envelope = event(metadata, sponsor, 12, "Welcome")
    return envelope, pages


def event(scope, sender, message_id, text):
    channel = scope["channel"]
    message = {"id": uid(message_id), "direction": "inbound", "sender_access": "direct"}
    if channel == "mail":
        message.update(
            from_address=sender,
            thread_id=scope["conversation_id"],
            body=text,
            body_state="complete",
        )
    elif channel == "phone":
        message.update(
            sender_phone_number=sender,
            conversation_id=scope["conversation_id"],
            text=text,
            recipients=[],
        )
    else:
        message.update(
            sender_number=sender,
            remote_number=None,
            conversation_id=scope["conversation_id"],
            content=text,
        )
    return {
        "event_type": {
            "mail": "message.received",
            "phone": "text.received",
            "imessage": "imessage.received",
        }[channel],
        "companion": deepcopy(scope),
        "data": {
            "text_message" if channel == "phone" else "message": message,
            "contact": {"id": "shared-contact"},
            "contact_memories": ["PRIVATE CONTACT MEMORY"],
        },
    }


class Request:
    def __init__(self, envelope, *, signed=True, request_id="request-one"):
        self.body = json.dumps(envelope).encode()
        self.url = "https://agent.example/webhook"
        self.headers = {"X-Inkbox-Request-Id": request_id, "X-Inkbox-Timestamp": "1700000000"}
        if signed:
            digest = hmac.new(
                b"test", f"{request_id}.1700000000.".encode() + self.body, hashlib.sha256
            ).hexdigest()
            self.headers["X-Inkbox-Signature"] = "sha256=" + digest

    async def read(self):
        return self.body


class Transport:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []
        self.error = None

    def get(self, path, *, params):
        self.calls.append((path, params))
        if self.error:
            raise self.error
        return deepcopy(self.pages[1 if params.get("cursor") else 0])


@pytest.fixture
def harness(tmp_path, monkeypatch):
    companion = pytest.importorskip("inkbox.companion")
    monkeypatch.setenv("INKBOX_CLAUDE_HOME", str(tmp_path))
    queries, outputs, clients = [], [], []
    hooks = SimpleNamespace(query=None, receive=None, connect=None, reply="[SILENT]")

    class Client:
        def __init__(self, *, options):
            self.options = options
            clients.append(self)

        async def connect(self):
            if hooks.connect:
                await hooks.connect(self)

        async def query(self, text):
            queries.append(text)
            if hooks.query:
                await hooks.query(self, text)

        async def receive_response(self):
            if hooks.receive:
                await hooks.receive(self)
            yield sessions_mod.ResultMessage(
                subtype="success",
                duration_ms=1,
                duration_api_ms=1,
                is_error=False,
                num_turns=1,
                session_id=f"host-{clients.index(self)}",
                result=hooks.reply,
            )

        async def disconnect(self):
            pass

    monkeypatch.setattr(sessions_mod, "ClaudeSDKClient", Client)

    def build(pages, **config):
        transport = Transport(pages)
        cfg = BridgeConfig(identity="agent", signing_key="whsec_test", **config)
        gateway = InkboxGateway(cfg)
        identity = SimpleNamespace(id="identity-one")
        identity.reply_all_email = lambda parent, **kwargs: outputs.append(("mail", parent, kwargs))
        identity.send_text = lambda **kwargs: outputs.append(("phone", kwargs))
        identity.send_imessage = lambda **kwargs: outputs.append(("imessage", kwargs))
        gateway._identity = identity
        gateway._inkbox = SimpleNamespace(
            companion=companion.CompanionResource(transport), get_identity=lambda _: identity
        )
        gateway.sessions = SessionManager(cfg, gateway.send_to_contact, None, [], {})
        gateway._resolve_contact_full = lambda **kwargs: pytest.fail("Contact routing must not run")
        return gateway, transport

    return SimpleNamespace(
        build=build, queries=queries, outputs=outputs, hooks=hooks, clients=clients, root=tmp_path
    )


async def drained(gateway):
    receiver = gateway._companion_receiver()
    await asyncio.gather(*list(receiver.jobs.values()))
    return list(receiver.records.values())


def test_shared_v1_fixture_reaches_claude_as_one_complete_input(harness):
    async def scenario():
        shared = json.loads((Path(__file__).parent / "fixtures" / "companion-v1.json").read_text())
        assert shared["version"] == 1
        pages = shared["pages"]
        trigger = pages[1]["items"][-1]
        scope = {
            key: pages[0][key]
            for key in ("scope_id", "activation_id", "conversation_id", "channel")
        }
        scope.update(phase="initialization", sequence=1)
        envelope = event(scope, trigger["author"], UUID(trigger["id"]).int, trigger["text"])
        gw, _ = harness.build(pages, allowed_users=[trigger["author"]])
        await gw._handle_webhook(Request(envelope))
        await drained(gw)
        assert len(harness.queries) == 1
        prompt = harness.queries[0]
        for entry in [*pages[0]["items"], trigger]:
            assert prompt.count(json.dumps(entry["text"], ensure_ascii=False)) == 1
        assert "future_history_notice" in prompt and "future_level" in prompt
        assert "source_message_id" in prompt
        await gw._cleanup()

    asyncio.run(scenario())


@pytest.mark.parametrize("channel", ["mail", "phone", "imessage"])
def test_complete_paginated_history_is_one_query_and_fixed_group_reply(harness, channel):
    async def scenario():
        envelope, pages = fixture(channel)
        gw, transport = harness.build(pages, allowed_users=[pages[1]["items"][-1]["author"]])
        harness.hooks.reply = "A group reply"
        response = await gw._handle_webhook(Request(envelope))
        assert response.status == 200
        assert harness.queries == []
        record = (await drained(gw))[0]
        assert record["state"] == "initialized"
        assert len(harness.queries) == 1
        prompt = harness.queries[0]
        assert prompt.count('"text":"/clear"') == 1
        assert prompt.count('"text":"YES"') == 1
        assert prompt.count('"text":"Welcome"') == 1
        assert '"filename":"notes.txt"' in prompt
        assert "history_gap" in prompt
        assert "PRIVATE CONTACT MEMORY" not in prompt
        assert any(params.get("cursor") == "page-two" for _, params in transport.calls)
        assert record["host_session_id"] == "host-0"
        if channel == "mail":
            assert harness.outputs == [("mail", uid(12), {"body_text": "A group reply"})]
        else:
            assert harness.outputs == [
                (channel, {"conversation_id": uid(3), "text": "A group reply"})
            ]
        assert all(
            path.stat().st_mode & 0o777 == 0o600 for path in harness.root.glob("companion/*/*.json")
        )
        await gw._cleanup()

    asyncio.run(scenario())


def test_duplicate_and_restart_never_repeat_initializer(harness):
    async def scenario():
        envelope, pages = fixture()
        gw, _ = harness.build(pages)
        await gw._handle_webhook(Request(envelope))
        await gw._handle_webhook(Request(envelope, request_id="retry"))
        await drained(gw)
        await gw._cleanup()
        restarted, _ = harness.build(pages)
        restarted._companion_receiver().recover()
        await restarted._handle_webhook(Request(envelope))
        await drained(restarted)
        assert len(harness.queries) == 1
        await restarted._cleanup()

    asyncio.run(scenario())


def test_live_after_restart_resumes_persisted_host_session(harness):
    async def scenario():
        envelope, pages = fixture()
        gw, _ = harness.build(pages)
        await gw._handle_webhook(Request(envelope))
        await drained(gw)
        await gw._cleanup()
        restarted, _ = harness.build(pages)
        scope = {**envelope["companion"], "phase": "live", "sequence": 2}
        await restarted._handle_webhook(
            Request(event(scope, "fred@example.com", 13, "After restart"))
        )
        await drained(restarted)
        assert len(harness.queries) == 2
        assert harness.clients[1].options.resume == "host-0"
        await restarted._cleanup()

    asyncio.run(scenario())


def test_restart_recovers_hydration_and_queued_live_event(harness):
    async def scenario():
        envelope, pages = fixture()
        gw, _ = harness.build(pages)
        receiver = gw._companion_receiver()
        receiver.schedule = lambda _: None
        await gw._handle_webhook(Request(envelope))
        scope = {**envelope["companion"], "phase": "live", "sequence": 2}
        await gw._handle_webhook(Request(event(scope, "fred@example.com", 13, "A follow-up")))
        assert len(list(harness.root.glob("companion/*/*.json"))) == 1
        await gw._cleanup()
        restarted, _ = harness.build(pages)
        restarted._companion_receiver().recover()
        record = (await drained(restarted))[0]
        assert len(harness.queries) == 2
        assert "Welcome" in harness.queries[0] and "A follow-up" in harness.queries[1]
        assert all(item["state"] == "completed" for item in record["events"].values())
        await restarted._cleanup()

    asyncio.run(scenario())


@pytest.mark.parametrize("checkpoint", ["pending", "ready", "submitting", "submitted"])
def test_recovery_only_retries_work_before_query(harness, checkpoint):
    async def scenario():
        envelope, pages = fixture()
        gw, _ = harness.build(pages)
        receiver = gw._companion_receiver()
        receiver.schedule = lambda _: None
        receiver.accept(envelope)
        key, record = next(iter(receiver.records.items()))
        record["state"] = checkpoint
        receiver.save(key)
        await gw._cleanup()
        restarted, _ = harness.build(pages)
        restarted._companion_receiver().recover()
        recovered = (await drained(restarted))[0]
        if checkpoint in {"submitting", "submitted"}:
            assert harness.queries == []
            assert recovered["state"] == "paused"
        else:
            assert len(harness.queries) == 1
            assert recovered["state"] == "initialized"
        await restarted._cleanup()

    asyncio.run(scenario())


def test_query_timeout_pauses_without_retry(harness):
    async def scenario():
        envelope, pages = fixture()
        gw, _ = harness.build(pages)

        async def fail(_client, _text):
            stored = json.loads(next(harness.root.glob("companion/*/*.json")).read_text())
            assert stored["events"][uid(12)]["state"] == "submitting"
            raise TimeoutError("Acceptance unknown")

        harness.hooks.query = fail
        await gw._handle_webhook(Request(envelope))
        assert (await drained(gw))[0]["state"] == "paused"
        await gw._handle_webhook(Request(envelope))
        assert len(harness.queries) == 1
        await gw._cleanup()
        restarted, _ = harness.build(pages)
        restarted._companion_receiver().recover()
        await drained(restarted)
        assert len(harness.queries) == 1
        await restarted._cleanup()

    asyncio.run(scenario())


def test_live_first_initializes_then_waits_for_completion(harness):
    async def scenario():
        envelope, pages = fixture()
        gw, _ = harness.build(pages)
        entered, release = asyncio.Event(), asyncio.Event()

        async def receive(_client):
            entered.set()
            await release.wait()

        harness.hooks.receive = receive
        scope = {**envelope["companion"], "phase": "live", "sequence": 2}
        await gw._handle_webhook(Request(event(scope, "fred@example.com", 13, "First live")))
        await entered.wait()
        scope["sequence"] = 3
        await gw._handle_webhook(
            Request(event(scope, "owner@example.com", 14, "Sponsor follow-up"))
        )
        assert len(harness.queries) == 1
        release.set()
        await drained(gw)
        assert len(harness.queries) == 2
        assert "Welcome" in harness.queries[0]
        assert "First live" in harness.queries[0]
        assert "Sponsor follow-up" in harness.queries[1]
        await gw._cleanup()

    asyncio.run(scenario())


@pytest.mark.parametrize("phase", ["live", "ordinary"])
def test_approval_requires_sponsor_or_current_ordinary_sender(harness, phase):
    async def scenario():
        envelope, pages = fixture()
        if phase == "ordinary":
            envelope["companion"].update(phase="ordinary")
            envelope["companion"].pop("activation_id")
        gw, _ = harness.build(pages)
        waiting, allowed = asyncio.Event(), []

        async def receive(client):
            if len(harness.queries) == 1:
                session = next(iter(gw.sessions.sessions.values()))
                pending_task = asyncio.create_task(session._escalate("permission", "Approve?"))
                await asyncio.sleep(0)
                waiting.set()
                allowed.append(await pending_task)

        harness.hooks.receive = receive
        await gw._handle_webhook(Request(envelope))
        await waiting.wait()
        scope = {**envelope["companion"], "phase": phase, "sequence": 2}
        await gw._handle_webhook(Request(event(scope, "fred@example.com", 13, "YES")))
        await asyncio.gather(*list(gw._companion.approval_jobs))
        assert not allowed
        scope["sequence"] = 3
        await gw._handle_webhook(Request(event(scope, "owner@example.com", 14, "YES")))
        await asyncio.gather(*list(gw._companion.approval_jobs))
        await drained(gw)
        assert allowed == ["YES"]
        assert len(harness.queries) == 2
        assert '"author": "fred@example.com"' in harness.queries[1]
        await gw._cleanup()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "failure", ["size", "sponsor", "revoked", "scope", "trigger", "incomplete"]
)
def test_invalid_initialization_never_submits_partial_turn(harness, failure):
    async def scenario():
        envelope, pages = fixture()
        settings = {"companion_max_bytes": 1000} if failure == "size" else {}
        if failure == "sponsor":
            settings["allowed_users"] = ["someone@example.com"]
        if failure == "scope":
            for page in pages:
                page["scope_id"] = uid(99)
        if failure == "trigger":
            pages[1]["items"][-1]["id"] = uid(99)
        if failure == "incomplete":
            pages[1]["history_complete"] = False
            pages[1]["next_cursor"] = "page-two"
        gw, transport = harness.build(pages, **settings)
        if failure == "revoked":
            from inkbox.exceptions import InkboxAPIError

            transport.error = InkboxAPIError(403, "Unavailable")
        await gw._handle_webhook(Request(envelope))
        assert (await drained(gw))[0]["state"] == "failed"
        assert harness.queries == []
        await gw._cleanup()

    asyncio.run(scenario())


def test_host_startup_does_not_repeat_snapshot_request(harness):
    async def scenario():
        envelope, pages = fixture()
        gw, transport = harness.build(pages)

        async def connect(_client):
            from inkbox.exceptions import InkboxAPIError

            transport.error = InkboxAPIError(403, "Unavailable")

        harness.hooks.connect = connect
        await gw._handle_webhook(Request(envelope))
        assert (await drained(gw))[0]["state"] == "initialized"
        assert len(harness.queries) == 1
        assert len(transport.calls) == 3
        await gw._cleanup()

    asyncio.run(scenario())


@pytest.mark.parametrize("channel", ["mail", "phone", "imessage"])
def test_ordinary_uses_separate_scope_without_loading_history(harness, channel):
    async def scenario():
        envelope, pages = fixture(channel)
        gw, transport = harness.build(pages)
        ordinary = deepcopy(envelope)
        ordinary["companion"].update(phase="ordinary")
        ordinary["companion"].pop("activation_id")
        await gw._handle_webhook(Request(ordinary))
        await drained(gw)
        assert transport.calls == []
        await gw._handle_webhook(Request(envelope))
        await drained(gw)
        assert len(harness.queries) == 2
        assert len(gw.sessions.sessions) == 2
        assert all(key.startswith("companion:") for key in gw.sessions.sessions)
        await gw._cleanup()

    asyncio.run(scenario())


@pytest.mark.parametrize("changed", ["activation", "scope", "conversation"])
def test_new_activation_cohort_or_conversation_has_new_session(harness, changed):
    async def scenario():
        envelope, pages = fixture()
        gw, transport = harness.build(pages)
        await gw._handle_webhook(Request(envelope))
        await drained(gw)
        envelope, pages = fixture(**{changed: 50})
        transport.pages = pages
        await gw._handle_webhook(Request(envelope))
        await drained(gw)
        assert len(harness.queries) == 2
        assert len(gw.sessions.sessions) == 2
        await gw._cleanup()

    asyncio.run(scenario())


def test_no_signature_no_companion_even_if_signature_option_disabled(harness):
    async def scenario():
        envelope, pages = fixture()
        gw, _ = harness.build(pages, require_signature=False)
        request = Request(envelope)
        request.headers["X-Inkbox-Signature"] = "sha256=invalid"
        response = await gw._handle_webhook(request)
        assert response.status == 401
        assert gw._companion is None
        await gw._handle_webhook(Request(envelope, signed=False))
        assert gw._companion is None

    asyncio.run(scenario())


def test_store_failure_never_acknowledges_or_submits(harness, monkeypatch):
    async def scenario():
        envelope, pages = fixture()
        gw, _ = harness.build(pages)

        def fail(*_args):
            raise OSError("Storage unavailable")

        monkeypatch.setattr("inkbox_claude.companion.os.replace", fail)
        with pytest.raises(OSError):
            await gw._handle_webhook(Request(envelope))
        assert (await gw._handle_webhook(Request(envelope))).status == 503
        assert harness.queries == []
        await gw._cleanup()

    asyncio.run(scenario())


def test_live_arrival_cannot_retarget_initializer_reply(harness):
    async def scenario():
        envelope, pages = fixture()
        gw, _ = harness.build(pages)
        entered, release = asyncio.Event(), asyncio.Event()
        harness.hooks.reply = "A reply"

        async def receive(_client):
            entered.set()
            await release.wait()

        harness.hooks.receive = receive
        await gw._handle_webhook(Request(envelope))
        await entered.wait()
        context = {**pages[0]["reply_context"], "reply_to_message_id": uid(13)}
        scope = {**envelope["companion"], "phase": "live", "sequence": 2, "reply_context": context}
        await gw._handle_webhook(Request(event(scope, "fred@example.com", 13, "Next")))
        release.set()
        await drained(gw)
        assert [output[1] for output in harness.outputs] == [uid(12), uid(12)]
        await gw._cleanup()

    asyncio.run(scenario())


def test_restart_during_hydration_recovers_once(harness):
    async def scenario():
        envelope, pages = fixture()
        gw, _ = harness.build(pages)
        receiver = gw._companion_receiver()
        started = asyncio.Event()

        async def slow_load(_record):
            started.set()
            await asyncio.Event().wait()

        receiver.load = slow_load
        await gw._handle_webhook(Request(envelope))
        await started.wait()
        await gw._cleanup()
        assert harness.queries == []
        restarted, _ = harness.build(pages)
        restarted._companion_receiver().recover()
        await drained(restarted)
        assert len(harness.queries) == 1
        await restarted._cleanup()

    asyncio.run(scenario())


def test_historical_and_live_control_text_do_not_clear_session(harness):
    async def scenario():
        envelope, pages = fixture()
        gw, _ = harness.build(pages)
        await gw._handle_webhook(Request(envelope))
        await drained(gw)
        scope = {**envelope["companion"], "phase": "live", "sequence": 2}
        await gw._handle_webhook(Request(event(scope, "fred@example.com", 13, "/clear")))
        await drained(gw)
        assert len(harness.queries) == 2
        assert len(harness.clients) == 1
        assert '"text": "/clear"' in harness.queries[1]
        await gw._cleanup()

    asyncio.run(scenario())


def test_ordinary_denial_and_invalid_metadata_are_not_acknowledged(harness):
    async def scenario():
        envelope, pages = fixture()
        gw, _ = harness.build(pages, allowed_users=["someone@example.com"])
        envelope["companion"]["phase"] = "ordinary"
        assert (await gw._handle_webhook(Request(envelope))).status == 400
        envelope["companion"].pop("activation_id")
        assert (await gw._handle_webhook(Request(envelope))).status == 403
        assert harness.queries == []
        assert gw._companion.records == {}
        await gw._cleanup()

    asyncio.run(scenario())


def test_admitted_sponsor_answer_does_not_reload_snapshot(harness):
    async def scenario():
        from inkbox.exceptions import InkboxAPIError

        envelope, pages = fixture()
        gw, transport = harness.build(pages, permission_timeout_s=0.05)
        waiting, decisions = asyncio.Event(), []

        async def receive(_client):
            if len(harness.queries) == 1:
                session = next(iter(gw.sessions.sessions.values()))
                task = asyncio.create_task(session._escalate("permission", "Approve?"))
                while not harness.outputs:
                    await asyncio.sleep(0)
                waiting.set()
                decisions.append(await task)

        harness.hooks.receive = receive
        await gw._handle_webhook(Request(envelope))
        await waiting.wait()
        transport.error = InkboxAPIError(403, "Unavailable")
        scope = {**envelope["companion"], "phase": "live", "sequence": 2}
        await gw._handle_webhook(Request(event(scope, "owner@example.com", 13, "YES")))
        await asyncio.gather(*list(gw._companion.approval_jobs))
        await drained(gw)
        assert decisions == ["YES"]
        assert len(transport.calls) == 3
        assert len(harness.queries) == 1
        await gw._cleanup()

    asyncio.run(scenario())


def test_exclusive_owner_is_checked_before_reading_journals(harness):
    async def scenario():
        _, pages = fixture()
        first, _ = harness.build(pages)
        receiver = first._companion_receiver()
        malformed = receiver.root / "malformed.json"
        malformed.write_text("not JSON")
        second, _ = harness.build(pages)
        with pytest.raises(RuntimeError, match="active owner"):
            second._companion_receiver()
        malformed.unlink()
        await first._cleanup()
        second._companion_receiver()
        await second._cleanup()

    asyncio.run(scenario())


def test_gateway_shutdown_drains_companion_and_a2a_progress(harness):
    async def scenario():
        envelope, pages = fixture()
        gw, _ = harness.build(pages)
        entered, stop_progress = asyncio.Event(), asyncio.Event()

        async def receive(_client):
            entered.set()
            await asyncio.Event().wait()

        harness.hooks.receive = receive
        await gw._handle_webhook(Request(envelope))
        await asyncio.wait_for(entered.wait(), 2)
        progress = asyncio.create_task(stop_progress.wait())
        acknowledgement = asyncio.create_task(asyncio.Event().wait())
        gw._a2a_progress_stop_events["task-one"] = stop_progress
        gw._a2a_progress_tasks["task-one"] = progress
        gw._a2a_ack_tasks["task-one:message-one"] = ("task-one", acknowledgement)

        await asyncio.wait_for(gw._cleanup(), 2)

        assert gw._closing and gw._companion._closed
        assert stop_progress.is_set() and progress.done()
        assert acknowledgement.cancelled()
        assert not gw._a2a_progress_tasks and not gw._a2a_ack_tasks
        assert all(session._worker.done() for session in gw.sessions.sessions.values())
        assert len(harness.queries) == 1
        assert not harness.outputs

    asyncio.run(scenario())


@pytest.mark.parametrize("stage", ["initialization", "live", "ordinary", "connect"])
def test_transient_failure_recovers_without_another_receipt(harness, monkeypatch, stage):
    async def scenario():
        from inkbox_claude import companion as receiver_mod

        monkeypatch.setattr(receiver_mod, "RETRY_INITIAL_DELAY", 0.001)
        monkeypatch.setattr(receiver_mod, "RETRY_MAX_DELAY", 0.002)
        envelope, pages = fixture()
        gw, transport = harness.build(pages)
        expected_queries = 1
        if stage == "live":
            await gw._handle_webhook(Request(envelope))
            await drained(gw)
            envelope = event(
                {**envelope["companion"], "phase": "live", "sequence": 2},
                "fred@example.com",
                13,
                "Recover this live message",
            )
            expected_queries = 2
        attempts = []

        def flaky(original):
            def call(*args, **kwargs):
                attempts.append(None)
                if len(attempts) <= 3:
                    raise ConnectionError("Temporary failure")
                return original(*args, **kwargs)

            return call

        if stage == "live":
            gw._fetch_mail_body = flaky(gw._fetch_mail_body)
        elif stage == "ordinary":
            envelope["companion"].update(phase="ordinary")
            envelope["companion"].pop("activation_id")
            gw._fetch_mail_body = flaky(gw._fetch_mail_body)
        elif stage == "connect":

            async def connect(_client):
                attempts.append(None)
                if len(attempts) <= 3:
                    raise ConnectionError("Temporary host connection failure")

            harness.hooks.connect = connect
        else:
            transport.get = flaky(transport.get)
        assert (await gw._handle_webhook(Request(envelope))).status == 200
        record = (await asyncio.wait_for(drained(gw), 2))[0]
        assert len(attempts) >= 4
        assert "error" not in record
        assert len(harness.queries) == expected_queries
        assert all(item["state"] == "completed" for item in record["events"].values())
        await gw._cleanup()

    asyncio.run(scenario())


@pytest.mark.parametrize("stage", ["hydration", "startup", "pre-send"])
@pytest.mark.parametrize("failure", ["sdk-transport", "http-429", "http-503", "runtime"])
def test_retry_budget_keeps_transport_recovery_and_pauses_deterministic_failures(
    harness, monkeypatch, stage, failure
):
    async def scenario():
        import httpx
        from inkbox.exceptions import InkboxAPIError

        envelope, pages = fixture()
        gw, transport = harness.build(pages)
        harness.hooks.reply = "Saved complete answer"
        receiver = gw._companion_receiver()
        receiver.schedule = lambda _: None
        receiver.accept(envelope)
        key = next(iter(receiver.records))
        record = receiver.records[key]
        error = (
            httpx.ConnectError("Connection unavailable") if failure == "sdk-transport"
            else InkboxAPIError(int(failure.removeprefix("http-")), "Unavailable")
            if failure.startswith("http-") else RuntimeError("Incompatible host response")
        )
        original_send = gw.send_to_contact

        async def fail(*_args):
            raise error

        if stage == "hydration":
            transport.error = error
        elif stage == "startup":
            harness.hooks.connect = fail
        else:
            monkeypatch.setattr(gw, "send_to_contact", fail)
        for attempt in range(1, 7):
            assert await receiver.drain_once(key) is (failure != "runtime" or attempt <= 5)
        assert record["retry_count"] == 6
        assert len(harness.queries) == (1 if stage == "pre-send" else 0)
        assert not harness.outputs
        if stage == "pre-send":
            assert record["events"][uid(12)]["state"] == "generated"
            assert record["events"][uid(12)]["reply"] == "Saved complete answer"
        if failure == "runtime":
            assert record["state"] == "paused"
            assert record["error"] == "retry_exhausted:RuntimeError"
            await gw._cleanup()
            restarted, _ = harness.build(pages)
            restarted._companion_receiver().recover()
            assert (await drained(restarted))[0]["state"] == "paused"
            assert len(harness.queries) == (1 if stage == "pre-send" else 0)
            assert not harness.outputs
            await restarted._cleanup()
        else:
            transport.error = None
            harness.hooks.connect = None
            monkeypatch.setattr(gw, "send_to_contact", original_send)
            assert await receiver.drain_once(key) is False
            assert len(harness.queries) == 1
            assert len(harness.outputs) == 1
            assert "error" not in record and "retry_count" not in record
            await gw._cleanup()

    asyncio.run(scenario())


@pytest.mark.parametrize("channel", ["mail", "phone", "imessage"])
@pytest.mark.parametrize("delivery", ["automatic", "tool"])
@pytest.mark.parametrize("revocation", ["activation", "sponsor"])
def test_admitted_turn_uses_saved_route_without_reloading_access(
    harness, monkeypatch, channel, delivery, revocation
):
    async def scenario():
        from inkbox.exceptions import InkboxAPIError
        from inkbox_claude import tools as tools_mod

        envelope, pages = fixture(channel)
        sponsor = pages[1]["items"][-1]["author"]
        gw, transport = harness.build(pages, allowed_users=[sponsor])
        harness.hooks.reply = "Automatic group response"
        monkeypatch.setattr(tools_mod, "create_sdk_mcp_server", lambda **kwargs: kwargs)
        server, _ = tools_mod.build_inkbox_mcp_server(gw._inkbox, "agent", gw.cfg)
        reply_tool = next(tool for tool in server["tools"] if tool.name == "inkbox_reply_companion")
        tool_results = []

        async def receive(_client):
            if revocation == "activation":
                transport.error = InkboxAPIError(403, "Unavailable")
            else:
                gw.cfg.allowed_users = ["someone-else@example.com"]
            if delivery == "tool":
                session = next(iter(gw.sessions.sessions.values()))
                token = tools_mod.CURRENT_SESSION.set(session)
                try:
                    tool_results.append(await reply_tool.handler({"text": "Tool group response"}))
                finally:
                    tools_mod.CURRENT_SESSION.reset(token)

        harness.hooks.receive = receive
        await gw._handle_webhook(Request(envelope))
        record = (await drained(gw))[0]
        assert len(transport.calls) == 3
        if revocation == "sponsor":
            assert record["state"] == "failed"
            assert record["revoked"] and not harness.outputs
        else:
            assert record["state"] == "initialized"
            assert len(harness.outputs) == 1
        assert len(harness.queries) == 1
        if delivery == "tool":
            assert bool(tool_results[0].get("is_error")) is (revocation == "sponsor")
        await gw._handle_webhook(Request(envelope))
        await drained(gw)
        assert len(harness.queries) == 1
        await gw._cleanup()

    asyncio.run(scenario())


def test_ordinary_reply_rechecks_local_sender_without_network(harness):
    async def scenario():
        envelope, pages = fixture()
        envelope["companion"].update(phase="ordinary")
        envelope["companion"].pop("activation_id")
        gw, transport = harness.build(pages)
        harness.hooks.reply = "Response"

        async def receive(_client):
            gw.cfg.allowed_users = ["someone-else@example.com"]

        harness.hooks.receive = receive
        await gw._handle_webhook(Request(envelope))
        record = (await drained(gw))[0]
        assert record["state"] == "failed"
        assert len(harness.queries) == 1
        assert not transport.calls
        assert not harness.outputs
        await gw._cleanup()

    asyncio.run(scenario())


def test_uncertain_send_never_repeats_host_input(harness):
    async def scenario():
        envelope, pages = fixture()
        gw, transport = harness.build(pages)
        harness.hooks.reply = "Response"

        async def receive(_client):
            def fail(*_args, **_kwargs):
                raise ConnectionError("Send outcome unknown")

            gw._identity.reply_all_email = fail

        harness.hooks.receive = receive
        await gw._handle_webhook(Request(envelope))
        assert (await drained(gw))[0]["state"] == "paused"
        transport.error = None
        await gw._handle_webhook(Request(envelope))
        await drained(gw)
        assert len(harness.queries) == 1
        await gw._cleanup()
        restarted, _ = harness.build(pages)
        restarted._companion_receiver().recover()
        await drained(restarted)
        assert len(harness.queries) == 1
        assert harness.outputs == []
        await restarted._cleanup()

    asyncio.run(scenario())


def test_shutdown_keeps_lock_until_host_stops_and_fences_late_checkpoint(harness):
    async def scenario():
        envelope, pages = fixture()
        gw, _ = harness.build(pages)
        entered, cancelled, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        checkpoints = []

        async def receive(_client):
            session = next(iter(gw.sessions.sessions.values()))
            checkpoints.append(session._current_turn.checkpoint)
            entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                await release.wait()
                checkpoints[0]("completed")
                raise

        harness.hooks.receive = receive
        await gw._handle_webhook(Request(envelope))
        await asyncio.wait_for(entered.wait(), 2)
        receiver = gw._companion
        closing = asyncio.create_task(receiver.close())
        await asyncio.wait_for(cancelled.wait(), 2)
        restarted, _ = harness.build(pages)
        with pytest.raises(RuntimeError, match="active owner"):
            restarted._companion_receiver()
        release.set()
        await asyncio.wait_for(closing, 2)
        assert all(session._worker.done() for session in gw.sessions.sessions.values())
        restarted._companion_receiver().recover()
        assert (await drained(restarted))[0]["state"] == "paused"
        path = next(receiver.root.glob("*.json"))
        saved = path.read_bytes()
        checkpoints[0]("completed")
        with pytest.raises(asyncio.CancelledError):
            checkpoints[0]("submitting")
        assert path.read_bytes() == saved
        with pytest.raises(RuntimeError):
            receiver.save(next(iter(receiver.records)))
        assert len(harness.queries) == 1
        await restarted._cleanup()
        await gw._cleanup()

    asyncio.run(scenario())


@pytest.mark.parametrize("status", [401, 403, 404, 409])
def test_revocation_discards_context_and_preserves_dedup_after_restart(harness, status):
    async def scenario():
        from inkbox.exceptions import InkboxAPIError

        envelope, pages = fixture()
        scope = {
            **envelope["companion"],
            "phase": "live",
            "sequence": 2,
            "reply_context": pages[0]["reply_context"],
            "history": [{"text": "captured-history-marker"}],
        }
        live = event(scope, "fred@example.com", 13, "captured-body-marker")
        live["data"]["message"]["attachments"] = [{"filename": "captured-attachment-marker"}]
        gw, transport = harness.build(pages)
        transport.error = InkboxAPIError(status, "Unavailable")
        receiver = gw._companion_receiver()
        receiver.accept(envelope)
        receiver.accept(live)
        record = (await drained(gw))[0]
        assert record["state"] == "failed"
        assert set(record["events"]) == {uid(12), uid(13)}
        serialized = next(receiver.root.glob("*.json")).read_text()
        assert "captured-" not in serialized
        assert "Welcome" not in serialized
        assert "reply_context" not in serialized
        assert all(item["state"] == "discarded" for item in record["events"].values())
        await gw._cleanup()
        restarted, transport = harness.build(pages)
        second = restarted._companion_receiver()
        assert second.accept(live)["deduped"]
        later = event({**scope, "sequence": 3}, "fred@example.com", 14, "captured-later-marker")
        second.accept(later)
        await drained(restarted)
        assert not harness.queries and not transport.calls
        assert "captured-" not in next(second.root.glob("*.json")).read_text()
        await restarted._cleanup()

    asyncio.run(scenario())


@pytest.mark.parametrize("channel", ["mail", "phone", "imessage"])
@pytest.mark.parametrize(
    "invalid",
    [
        "sequence-zero",
        "sequence-negative",
        "sequence-bool",
        "sequence-string",
        "scope-missing",
        "scope-malformed",
        "activation-missing",
        "activation-malformed",
        "conversation-missing",
        "conversation-malformed",
        "source-missing",
        "source-malformed",
        "source-conversation-missing",
        "source-conversation-mismatch",
    ],
)
def test_invalid_scope_source_or_sequence_never_persists(harness, channel, invalid):
    async def scenario():
        envelope, pages = fixture(channel)
        scope = envelope["companion"]
        message = envelope["data"]["text_message" if channel == "phone" else "message"]
        if invalid.startswith("sequence-"):
            scope["sequence"] = {"zero": 0, "negative": -1, "bool": True, "string": "1"}[
                invalid.split("-", 1)[1]
            ]
        else:
            field, change = invalid.rsplit("-", 1)
            target, name = {
                "scope": (scope, "scope_id"),
                "activation": (scope, "activation_id"),
                "conversation": (scope, "conversation_id"),
                "source": (message, "id"),
                "source-conversation": (
                    message,
                    "thread_id" if channel == "mail" else "conversation_id",
                ),
            }[field]
            if change == "missing":
                target.pop(name)
            else:
                target[name] = uid(99) if change == "mismatch" else "invalid-id"
        gw, _ = harness.build(pages)
        assert (await gw._handle_webhook(Request(envelope))).status == 400
        assert gw._companion.records == {}
        assert not list(gw._companion.root.glob("*.json"))
        assert not harness.queries
        await gw._cleanup()

    asyncio.run(scenario())


@pytest.mark.parametrize("ordinary", [False, True])
@pytest.mark.parametrize("conflict", ["sequence-source", "source-sequence", "sender", "recipients"])
def test_conflicting_delivery_authority_is_rejected(harness, ordinary, conflict):
    async def scenario():
        envelope, pages = fixture()
        if ordinary:
            envelope["companion"].update(phase="ordinary")
            envelope["companion"].pop("activation_id")
        gw, _ = harness.build(pages)
        await gw._handle_webhook(Request(envelope))
        await drained(gw)
        changed = deepcopy(envelope)
        if conflict == "sequence-source":
            changed["data"]["message"]["id"] = uid(99)
        elif conflict == "source-sequence":
            changed["companion"]["sequence"] = 2
        elif conflict == "sender":
            changed["data"]["message"]["from_address"] = "someone-else@example.com"
        else:
            changed["data"]["message"]["to_addresses"] = ["someone-else@example.com"]
        before = next(gw._companion.root.glob("*.json")).read_bytes()
        assert (await gw._handle_webhook(Request(changed))).status == 400
        assert next(gw._companion.root.glob("*.json")).read_bytes() == before
        assert len(harness.queries) == 1
        await gw._cleanup()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "corruption",
    [
        "sequence",
        "source",
        "scope",
        "state",
        "phase-state",
        "revoked-state",
        "duplicate-sequence",
        "authority",
    ],
)
def test_invalid_checkpoint_fails_before_recovery(harness, corruption):
    async def scenario():
        envelope, pages = fixture()
        gw, _ = harness.build(pages)
        receiver = gw._companion_receiver()
        receiver.schedule = lambda _: None
        receiver.accept(envelope)
        path = next(receiver.root.glob("*.json"))
        await gw._cleanup()
        record = json.loads(path.read_text())
        stored = record["events"][uid(12)]
        if corruption == "sequence":
            stored["scope"]["sequence"] = -1
        elif corruption == "source":
            stored["message"]["id"] = uid(99)
        elif corruption == "scope":
            record["scope"]["sequence"] = 0
        elif corruption == "state":
            stored["state"] = "unknown"
        elif corruption == "phase-state":
            record["state"] = "ordinary"
        elif corruption == "revoked-state":
            record["revoked"] = True
        elif corruption == "duplicate-sequence":
            second = deepcopy(stored)
            second["message"]["id"] = uid(13)
            record["events"][uid(13)] = second
        else:
            stored["message"]["from_address"] = "someone-else@example.com"
            stored["sender"] = "someone-else@example.com"
        path.write_text(json.dumps(record))
        restarted, _ = harness.build(pages)
        restored = restarted._companion_receiver()
        assert restored.records == {}
        assert path.with_suffix(".invalid").exists()
        with pytest.raises(RuntimeError, match="checkpoint is invalid"):
            restored.accept(envelope)
        assert not harness.queries
        await restarted._cleanup()

    asyncio.run(scenario())


def delivery_failure(channel, *, event_type=None, conversation=3):
    message = {"id": uid(40), "direction": "outbound"}
    if channel == "mail":
        message.update(
            thread_id=uid(conversation),
            to_addresses=["owner@example.com"],
            subject="Group response",
            snippet="failed-group-body-marker",
        )
    elif channel == "phone":
        message.update(
            conversation_id=uid(conversation),
            remote_phone_number="+15555550101",
            text="failed-group-body-marker",
            error_detail="Unavailable",
        )
    else:
        message.update(
            conversation_id=uid(conversation),
            remote_number="+15555550101",
            content="failed-group-body-marker",
            error_reason="Unavailable",
        )
    return {
        "event_type": event_type
        or {
            "mail": "message.bounced",
            "phone": "text.delivery_failed",
            "imessage": "imessage.delivery_failed",
        }[channel],
        "data": {
            "text_message" if channel == "phone" else "message": message,
            "contacts": [{"id": "private-contact"}],
        },
    }


@pytest.mark.parametrize(
    "event_type,channel",
    [
        ("message.bounced", "mail"),
        ("message.failed", "mail"),
        ("text.delivery_failed", "phone"),
        ("imessage.delivery_failed", "imessage"),
    ],
)
@pytest.mark.parametrize("phase", ["initialization", "ordinary"])
def test_delivery_failure_stays_scoped_without_private_routing_after_restart(
    harness, monkeypatch, event_type, channel, phase
):
    async def scenario():
        envelope, pages = fixture(channel)
        if phase == "ordinary":
            envelope["companion"].update(phase="ordinary")
            envelope["companion"].pop("activation_id")
        gw, _ = harness.build(pages)
        harness.hooks.reply = "Initial group response"
        await gw._handle_webhook(Request(envelope))
        record = (await drained(gw))[0]
        original_state = record["state"]
        assert len(harness.queries) == 1
        assert len(harness.outputs) == 1

        def forbidden(*_args, **_kwargs):
            pytest.fail("Delivery failures must stay out of contact routing and host sessions")

        for name in ("_chat_key", "_note_outbound_delivery_failure"):
            monkeypatch.setattr(gw, name, forbidden)
        monkeypatch.setattr(gw.sessions, "get", forbidden)
        failure = delivery_failure(channel, event_type=event_type)
        response = await gw._handle_webhook(Request(failure))
        assert response.status == 200
        assert json.loads(response.text)["companion"] == "delivery_failed"
        diagnostic = record["last_delivery_failure"]
        assert diagnostic == {
            "event_type": event_type,
            "message_id": uid(40),
            "channel": channel,
            "conversation_id": uid(3),
            "action": "operator_review",
        }
        assert record["state"] == original_state
        serialized = next(gw._companion.root.glob("*.json")).read_text()
        assert "failed-group-body-marker" not in serialized
        assert "private-contact" not in serialized
        await gw._cleanup()

        restarted, _ = harness.build(pages)
        restarted._companion_receiver().recover()
        recovered = (await drained(restarted))[0]
        assert recovered["last_delivery_failure"] == diagnostic
        for name in ("_chat_key", "_note_outbound_delivery_failure"):
            monkeypatch.setattr(restarted, name, forbidden)
        monkeypatch.setattr(restarted.sessions, "get", forbidden)
        assert (await restarted._handle_webhook(Request(failure))).status == 200
        assert recovered["last_delivery_failure"] == diagnostic
        assert len(harness.queries) == 1
        assert len(harness.outputs) == 1
        assert not restarted.sessions.sessions
        await restarted._cleanup()

    asyncio.run(scenario())


@pytest.mark.parametrize("channel", ["mail", "phone", "imessage"])
@pytest.mark.parametrize("corruption", ["event-state", "unreadable", "routing-mismatch"])
def test_quarantined_delivery_failure_never_wakes_private_recovery(
    harness, monkeypatch, channel, corruption
):
    async def scenario():
        envelope, pages = fixture(channel)
        gw, _ = harness.build(pages)
        harness.hooks.reply = "Initial group response"
        await gw._handle_webhook(Request(envelope))
        await drained(gw)
        path = next(gw._companion.root.glob("*.json"))
        await gw._cleanup()
        record = json.loads(path.read_text())
        if corruption == "event-state":
            record["events"][uid(12)]["state"] = "invalid"
        elif corruption == "routing-mismatch":
            record["scope"]["conversation_id"] = uid(99)
        original = "{broken json" if corruption == "unreadable" else json.dumps(record)
        path.write_text(original)

        def forbidden(*_args, **_kwargs):
            pytest.fail("Quarantined delivery failure reached private recovery")

        for _ in range(2):
            restarted, _ = harness.build(pages)
            restored = restarted._companion_receiver()
            for name in ("_chat_key", "_note_outbound_delivery_failure"):
                monkeypatch.setattr(restarted, name, forbidden)
            monkeypatch.setattr(restarted.sessions, "get", forbidden)
            failure = delivery_failure(channel)
            assert (await restarted._handle_webhook(Request(failure))).status == 503
            unrelated = delivery_failure(channel, conversation=98)
            other_channel = "phone" if channel == "mail" else "mail"
            for other in (unrelated, delivery_failure(other_channel)):
                if corruption == "event-state":
                    assert restored.delivery_failure_scopes(other) == []
                else:
                    assert (await restarted._handle_webhook(Request(other))).status == 503
            assert path.with_suffix(".invalid").read_text() == original
            assert not path.exists()
            assert len(harness.queries) == 1
            assert len(harness.outputs) == 1
            assert not restarted.sessions.sessions
            await restarted._cleanup()

    asyncio.run(scenario())


@pytest.mark.parametrize("channel", ["mail", "phone", "imessage"])
@pytest.mark.parametrize("case", ["mode-off", "unknown-conversation", "different-channel"])
def test_unknown_delivery_failure_keeps_ordinary_routing(harness, monkeypatch, channel, case):
    async def scenario():
        from unittest.mock import AsyncMock, Mock
        from inkbox_claude.gateway import web

        known_channel = "phone" if channel == "mail" else "mail"
        envelope, pages = fixture(known_channel if case == "different-channel" else channel)
        gw, _ = harness.build(pages)
        if case != "mode-off":
            await gw._handle_webhook(Request(envelope))
            await drained(gw)
        route = Mock(wraps=gw._chat_key)
        notify = AsyncMock(return_value=web.json_response({"ok": True}))
        monkeypatch.setattr(gw, "_chat_key", route)
        monkeypatch.setattr(gw, "_note_outbound_delivery_failure", notify)
        failure = delivery_failure(
            channel, conversation=99 if case == "unknown-conversation" else 3
        )
        response = await gw._handle_webhook(Request(failure, request_id="failure"))
        assert response.status == 200
        route.assert_called_once()
        notify.assert_awaited_once()
        assert notify.call_args.kwargs["chat_id"] == "private-contact"
        assert "failed-group-body-marker" in notify.call_args.kwargs["failed_body"]
        if gw._companion is not None:
            assert all(
                "last_delivery_failure" not in record for record in gw._companion.records.values()
            )
        await gw._cleanup()

    asyncio.run(scenario())


@pytest.mark.parametrize("signed", [True, False])
def test_companion_failure_requires_authentication_even_with_signature_option_off(
    harness, monkeypatch, signed
):
    async def scenario():
        envelope, pages = fixture()
        gw, _ = harness.build(pages, require_signature=False)
        await gw._handle_webhook(Request(envelope))
        record = (await drained(gw))[0]
        monkeypatch.setattr(
            gw, "_chat_key", lambda *_args: pytest.fail("Unverified failure routed")
        )
        request = Request(delivery_failure("mail"), signed=signed, request_id="failure")
        if signed:
            request.headers["X-Inkbox-Signature"] = "sha256=invalid"
        response = await gw._handle_webhook(request)
        assert response.status == (401 if signed else 200)
        assert "last_delivery_failure" not in record
        assert len(harness.queries) == 1
        await gw._cleanup()

    asyncio.run(scenario())


def test_delivery_failure_storage_error_is_not_acknowledged_or_routed(harness, monkeypatch):
    async def scenario():
        envelope, pages = fixture()
        gw, _ = harness.build(pages)
        await gw._handle_webhook(Request(envelope))
        await drained(gw)
        monkeypatch.setattr(gw, "_chat_key", lambda *_args: pytest.fail("Failure routed privately"))

        def fail(*_args):
            raise OSError("Storage unavailable")

        monkeypatch.setattr("inkbox_claude.companion.os.replace", fail)
        with pytest.raises(OSError):
            await gw._handle_webhook(Request(delivery_failure("mail"), request_id="failure"))
        assert (
            await gw._handle_webhook(Request(delivery_failure("mail"), request_id="failure"))
        ).status == 503
        assert len(harness.queries) == 1
        await gw._cleanup()

    asyncio.run(scenario())


@pytest.mark.parametrize("error,status", [(ValueError, 400), (PermissionError, 403), (RuntimeError, 503)])
def test_companion_http_errors_do_not_expose_exception_details(harness, monkeypatch, error, status):
    async def scenario():
        envelope, pages = fixture()
        gw, _ = harness.build(pages)
        receiver = gw._companion_receiver()

        def fail(_envelope):
            raise error("private database path and request credentials")

        monkeypatch.setattr(receiver, "accept", fail)
        response = await gw._handle_webhook(Request(envelope))
        assert response.status == status
        assert "private" not in response.text
        assert "credentials" not in response.text
        await gw._cleanup()

    asyncio.run(scenario())


def test_companion_without_inkbox_signature_cannot_fall_through_external_handler(harness, monkeypatch):
    async def scenario():
        envelope, pages = fixture()
        gw, _ = harness.build(pages)
        request = Request(envelope, signed=False)
        request.headers = {}
        response = await gw._handle_webhook(request)
        assert response.status == 401
        assert not harness.queries
        assert gw._companion is None
        await gw._cleanup()
    asyncio.run(scenario())


def test_invalid_checkpoint_only_pauses_its_scope_across_restarts(harness):
    async def scenario():
        envelope, pages = fixture()
        gw, _ = harness.build(pages)
        receiver = gw._companion_receiver()
        receiver.schedule = lambda _: None
        receiver.accept(envelope)
        key = next(iter(receiver.records))
        path = receiver.root / (key + ".json")
        await gw._cleanup()
        path.write_text("{broken json")
        for _ in range(2):
            restarted, _ = harness.build(pages)
            restored = restarted._companion_receiver()
            restored.schedule = lambda _: None
            with pytest.raises(RuntimeError, match="checkpoint is invalid"):
                restored.accept(envelope)
            healthy = deepcopy(envelope)
            healthy["companion"]["activation_id"] = uid(99)
            restored.accept(healthy)
            assert len(restored.records) == 1
            assert not harness.queries
            await restarted._cleanup()
    asyncio.run(scenario())
