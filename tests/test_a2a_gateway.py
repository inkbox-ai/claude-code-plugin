import asyncio
import json
import types

import pytest

from inkbox_claude import gateway as gateway_mod
from inkbox_claude import a2a_progress as progress_mod
from inkbox_claude.config import BridgeConfig
from inkbox_claude.gateway import InkboxGateway


@pytest.fixture(autouse=True)
def fake_web(monkeypatch):
    monkeypatch.setattr(
        gateway_mod,
        "web",
        types.SimpleNamespace(
            json_response=lambda payload: types.SimpleNamespace(
                status=200,
                text=json.dumps(payload),
            )
        ),
    )


class _Session:
    def __init__(self):
        self.calls = []
        self.inbound = []

    async def run_consult(self, prompt, *, a2a_context=None):
        self.calls.append((prompt, a2a_context))
        return "Completed."

    async def handle_inbound(self, prompt, mode, meta):
        self.inbound.append((prompt, mode, meta))


class _Sessions:
    def __init__(self):
        self.session = _Session()
        self.keys = []

    def get(self, key, system_prompt_extra=""):
        self.keys.append((key, system_prompt_extra))
        return self.session


def _gateway(tmp_path):
    gateway = object.__new__(InkboxGateway)
    gateway._a2a_registry_path = tmp_path / "a2a.json"
    gateway._a2a_jobs = {}
    gateway._a2a_progress_tasks = {}
    gateway._a2a_progress_stop_events = {}
    gateway._a2a_ingest_lock = asyncio.Lock()
    gateway.cfg = BridgeConfig(project_dir=str(tmp_path))
    task = types.SimpleNamespace(state="submitted", messages=[])

    def reply(task_id, **kwargs):
        gateway.replies.append((task_id, kwargs))
        if kwargs.get("intent") == "progress":
            task.state = "working"
        elif kwargs.get("intent") == "complete":
            task.state = "completed"
        task.messages.append(types.SimpleNamespace(parts=[{"text": kwargs["text"]}]))

    gateway._identity = types.SimpleNamespace(
        id="identity-1",
        a2a_task=lambda _task_id: task,
        a2a_reply=reply,
    )
    gateway._a2a_authoritative_task = task
    gateway.replies = []
    gateway.sessions = _Sessions()
    return gateway


def _event():
    return {
        "id": "evt-1",
        "event_type": "a2a.task.created",
        "data": {
            "task_id": "task-1",
            "context_id": "context-1",
            "message_id": "message-1",
            "caller": {
                "identity_id": "caller-1",
                "organization_id": "org-1",
                "handle": "caller",
            },
            "parts": [{"text": "Investigate."}],
        },
    }


def test_a2a_gateway_persists_dedupes_and_completes(tmp_path, monkeypatch):
    async def inline(function, *args, **kwargs):
        return function(*args, **kwargs)

    monkeypatch.setattr(gateway_mod.asyncio, "to_thread", inline)
    gateway = _gateway(tmp_path)

    async def scenario():
        first = await gateway._on_a2a_event(_event())
        await asyncio.gather(*gateway._a2a_jobs["task-1"])
        second = await gateway._on_a2a_event(_event())
        return first, second

    first, second = asyncio.run(scenario())
    registry = json.loads(gateway._a2a_registry_path.read_text())

    assert first.status == 200
    assert json.loads(second.text)["deduped"] is True
    assert registry["task-1:message-1"]["state"] == "finalized"
    assert gateway.sessions.keys[0][0] == "a2a:identity-1:context-1"
    assert gateway.replies == [
        (
            "task-1",
            {
                "intent": "progress",
                "text": (
                    "Task task-1 received. Work is queued and starting. "
                    "Expect progress updates about every 3 minutes."
                ),
            },
        ),
        ("task-1", {"intent": "complete", "text": "Completed."}),
    ]


def test_concurrent_duplicate_a2a_delivery_sends_one_acknowledgement(tmp_path):
    gateway = _gateway(tmp_path)

    async def scenario():
        responses = await asyncio.gather(
            gateway._on_a2a_event(_event()),
            gateway._on_a2a_event(_event()),
        )
        await asyncio.gather(*gateway._a2a_jobs["task-1"])
        return responses

    responses = asyncio.run(scenario())
    acknowledgements = [
        kwargs
        for _task_id, kwargs in gateway.replies
        if kwargs["text"].startswith("Task task-1 received.")
    ]

    assert len(acknowledgements) == 1
    assert sum("deduped" in response.text for response in responses) == 1


def test_a2a_gateway_resumes_nonfinal_registry_entries(tmp_path, monkeypatch):
    async def inline(function, *args, **kwargs):
        return function(*args, **kwargs)

    monkeypatch.setattr(gateway_mod.asyncio, "to_thread", inline)
    gateway = _gateway(tmp_path)
    task = types.SimpleNamespace(
        id="task-1",
        context_id="context-1",
        state="working",
        caller=types.SimpleNamespace(
            identity_id="caller-1",
            organization_id="org-1",
            handle="caller",
        ),
        messages=[
            types.SimpleNamespace(
                message_id="message-1",
                parts=[{"text": "Resume this."}],
            )
        ],
    )
    gateway._identity.a2a_task = lambda _task_id: task
    gateway._identity.iter_a2a_tasks = lambda **_kwargs: iter(())
    gateway._write_a2a_registry(
        "task-1:message-1",
        _event()["data"],
        "running",
    )

    async def scenario():
        await gateway._catch_up_a2a_tasks()
        await asyncio.gather(*gateway._a2a_jobs["task-1"])

    asyncio.run(scenario())
    registry = json.loads(gateway._a2a_registry_path.read_text())

    assert registry["task-1:message-1"]["state"] == "finalized"
    assert gateway.sessions.session.calls[0][0].endswith("Resume this.")


def test_a2a_sent_update_returns_to_the_delegating_session(
    tmp_path,
    monkeypatch,
):
    gateway = _gateway(tmp_path)
    monkeypatch.setattr(
        gateway_mod,
        "find_a2a_delegation",
        lambda _task_id: {
            "session_key": "contact-1",
            "card_url": "https://target.example/card",
        },
    )
    event = _event()
    event["event_type"] = "a2a.sent_task.updated"
    event["data"]["state"] = "input_required"
    event["data"]["parts"] = [{"text": "Which region?"}]

    asyncio.run(gateway._on_a2a_event(event))

    assert gateway.sessions.keys[0][0] == "contact-1"
    prompt, mode, meta = gateway.sessions.session.inbound[0]
    assert "Which region?" in prompt
    assert mode == "external"
    assert meta["a2a_task_id"] == "task-1"


def test_a2a_sent_progress_does_not_wake_delegating_session(tmp_path, monkeypatch):
    gateway = _gateway(tmp_path)
    monkeypatch.setattr(
        gateway_mod,
        "find_a2a_delegation",
        lambda _task_id: {
            "session_key": "contact-1",
            "card_url": "https://target.example/card",
        },
    )
    event = _event()
    event["event_type"] = "a2a.sent_task.updated"
    event["data"]["state"] = "working"
    event["data"]["parts"] = [{"text": "Still working."}]

    response = asyncio.run(gateway._on_a2a_event(event))

    assert json.loads(response.text)["ignored"] == "progress-only"
    assert gateway.sessions.session.inbound == []


@pytest.mark.parametrize(
    ("interval", "expectation"),
    [
        (180, "Expect progress updates about every 3 minutes."),
        (60, "Expect progress updates about every 1 minute."),
        (1, "Expect progress updates about every 1 second."),
        (0, "Periodic progress updates are disabled."),
    ],
)
def test_a2a_receipt_reports_configured_progress_frequency(
    tmp_path,
    interval,
    expectation,
):
    gateway = _gateway(tmp_path)
    gateway.cfg.a2a_progress_interval_seconds = interval

    asyncio.run(gateway._on_a2a_event(_event()))

    receipt = gateway.replies[0][1]["text"]
    assert receipt.startswith("Task task-1 received. Work is queued and starting.")
    assert receipt.endswith(expectation)


def test_a2a_acknowledgement_recovers_accepted_reply_without_duplicate(tmp_path):
    gateway = _gateway(tmp_path)
    key = "task-1:message-1"
    data = _event()["data"]
    gateway._write_a2a_registry(key, data, "queued")
    original_reply = gateway._identity.a2a_reply
    attempts = 0

    def accepted_then_lost(task_id, **kwargs):
        nonlocal attempts
        attempts += 1
        original_reply(task_id, **kwargs)
        raise OSError("response lost")

    gateway._identity.a2a_reply = accepted_then_lost

    with pytest.raises(OSError):
        asyncio.run(gateway._record_a2a_acknowledgement(key, data))
    asyncio.run(gateway._record_a2a_acknowledgement(key, data))

    assert attempts == 1
    registry = json.loads(gateway._a2a_registry_path.read_text())
    assert "pending_text" not in registry[key]["receipt"]
    assert registry[key]["receipt"]["delivered_text"].startswith("Task task-1")


def test_a2a_progress_summary_rejects_terminal_claim():
    update = progress_mod._clean_update(
        "Done — the task is complete.",
        ["validating the work"],
    )

    assert update == "I'm validating the work."


def test_a2a_progress_summary_uses_tool_free_side_turn(monkeypatch):
    captured = {}

    class FakeResult:
        def __init__(self):
            self.result = "I'm reviewing the requested calculation."

    class FakeClient:
        def __init__(self, *, options):
            captured["options"] = options

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def query(self, prompt):
            captured["prompt"] = prompt

        async def receive_response(self):
            yield FakeResult()

    def options(**kwargs):
        return kwargs

    monkeypatch.setattr(progress_mod, "CLAUDE_SDK_AVAILABLE", True)
    monkeypatch.setattr(progress_mod, "ClaudeAgentOptions", options)
    monkeypatch.setattr(progress_mod, "ClaudeSDKClient", FakeClient)
    monkeypatch.setattr(progress_mod, "ResultMessage", FakeResult)

    update = asyncio.run(progress_mod.build_a2a_progress_update(
        task_text="Inspect the calculation.",
        activities=["reviewing the relevant material"],
        previous_update="I'm checking the request.",
        project_dir="/tmp",
    ))

    assert update == "I'm reviewing the requested calculation."
    assert captured["options"]["tools"] == []
    assert captured["options"]["allowed_tools"] == []
    assert captured["options"]["max_turns"] == 1
    assert "Inspect the calculation." in captured["prompt"]
    assert "I'm checking the request." in captured["prompt"]


def test_a2a_progress_activity_is_short_and_does_not_retain_inputs():
    progress_mod.start_a2a_progress("task-1")

    progress_mod.observe_a2a_tool_start("task-1", "run_sql_query")
    progress_mod.observe_a2a_tool_start("task-1", "list_directory_users")

    snapshot = progress_mod.a2a_activity_snapshot("task-1")
    progress_mod.stop_a2a_progress("task-1")
    assert snapshot == [
        "checking the requested data",
        "reviewing the requested records",
    ]
    assert progress_mod._fallback_update(snapshot) == (
        "I'm checking the requested data and reviewing the requested records."
    )


def test_a2a_progress_update_is_durable_and_nonterminal(tmp_path, monkeypatch):
    gateway = _gateway(tmp_path)
    gateway._a2a_authoritative_task.state = "working"
    key = "task-1:message-1"
    data = _event()["data"]
    gateway._write_a2a_registry(key, data, "running", progress_started=True)
    progress_mod.start_a2a_progress("task-1")

    async def summary(**_kwargs):
        return "I'm checking the requested calculation."

    monkeypatch.setattr(gateway_mod, "build_a2a_progress_update", summary)
    keep_running = asyncio.run(gateway._emit_a2a_progress_update(
        task_id="task-1",
        registry_key=key,
        data=data,
        task_text="Calculate a result.",
    ))

    assert keep_running is True
    assert gateway.replies[-1][1]["intent"] == "progress"
    assert "checking the requested calculation" in gateway.replies[-1][1]["text"]
    registry = json.loads(gateway._a2a_registry_path.read_text())
    progress = registry[key]["progress"]
    assert progress["delivered_count"] == 1
    assert "pending" not in progress
    assert registry[key]["state"] == "running"


def test_a2a_progress_retry_recovers_accepted_reply_without_duplicate(tmp_path):
    gateway = _gateway(tmp_path)
    gateway._a2a_authoritative_task.state = "working"
    update = "I'm validating the work. (60s elapsed)"
    gateway._a2a_authoritative_task.messages.append(
        types.SimpleNamespace(parts=[{"text": update}])
    )
    key = "task-1:message-1"
    data = _event()["data"]
    gateway._write_a2a_registry(key, data, "running", progress_started=True)
    gateway._write_a2a_registry(key, data, "running", progress_text=update)
    receipt_count = len(gateway.replies)

    keep_running = asyncio.run(gateway._emit_a2a_progress_update(
        task_id="task-1",
        registry_key=key,
        data=data,
        task_text="Validate the work.",
    ))

    assert keep_running is True
    assert len(gateway.replies) == receipt_count
    progress = json.loads(gateway._a2a_registry_path.read_text())[key]["progress"]
    assert progress["last_delivered_text"] == update
    assert "pending" not in progress


def test_a2a_progress_elapsed_time_continues_across_caller_follow_up(tmp_path):
    gateway = _gateway(tmp_path)
    first_key = "task-1:message-1"
    gateway._write_a2a_registry(
        first_key,
        _event()["data"],
        "running",
        progress_started=True,
    )
    first = json.loads(gateway._a2a_registry_path.read_text())
    started_at = first[first_key]["progress"]["started_at"]
    follow_up = _event()["data"] | {"message_id": "message-2"}
    second_key = "task-1:message-2"

    gateway._write_a2a_registry(
        second_key,
        follow_up,
        "running",
        progress_started=True,
    )

    registry = json.loads(gateway._a2a_registry_path.read_text())
    assert registry[second_key]["progress"]["started_at"] == started_at


def test_a2a_progress_stops_for_terminal_task(tmp_path):
    gateway = _gateway(tmp_path)
    gateway._a2a_authoritative_task.state = "completed"
    key = "task-1:message-1"
    data = _event()["data"]
    gateway._write_a2a_registry(key, data, "running", progress_started=True)

    keep_running = asyncio.run(gateway._emit_a2a_progress_update(
        task_id="task-1",
        registry_key=key,
        data=data,
        task_text="Validate the work.",
    ))

    assert keep_running is False
    assert gateway.replies == []


def test_a2a_progress_rechecks_state_after_summary(tmp_path, monkeypatch):
    gateway = _gateway(tmp_path)
    gateway._a2a_authoritative_task.state = "working"
    key = "task-1:message-1"
    data = _event()["data"]
    gateway._write_a2a_registry(key, data, "running", progress_started=True)

    async def settle_during_summary(**_kwargs):
        gateway._a2a_authoritative_task.state = "canceled"
        return "I'm checking the request."

    monkeypatch.setattr(
        gateway_mod,
        "build_a2a_progress_update",
        settle_during_summary,
    )
    keep_running = asyncio.run(
        gateway._emit_a2a_progress_update(
            task_id="task-1",
            registry_key=key,
            data=data,
            task_text="Check the request.",
        )
    )

    assert keep_running is False
    assert gateway.replies == []


def test_a2a_progress_runner_waits_configured_interval(monkeypatch, tmp_path):
    gateway = _gateway(tmp_path)
    gateway.cfg.a2a_progress_interval_seconds = 60
    sleeps = []
    emissions = []

    async def fake_wait_for(awaitable, timeout):
        awaitable.close()
        sleeps.append(timeout)
        raise asyncio.TimeoutError

    async def stop_after_one(**kwargs):
        emissions.append(kwargs)
        return False

    monkeypatch.setattr(gateway_mod.asyncio, "wait_for", fake_wait_for)
    gateway._emit_a2a_progress_update = stop_after_one

    asyncio.run(gateway._run_a2a_progress_updates(
        task_id="task-1",
        registry_key="task-1:message-1",
        data=_event()["data"],
        task_text="Calculate.",
        stop_event=asyncio.Event(),
    ))

    assert sleeps == [60]
    assert emissions == [{
        "task_id": "task-1",
        "registry_key": "task-1:message-1",
        "data": _event()["data"],
        "task_text": "Calculate.",
    }]


def test_a2a_completion_cancels_progress_timer(tmp_path):
    gateway = _gateway(tmp_path)

    async def scenario():
        await gateway._on_a2a_event(_event())
        await asyncio.gather(*gateway._a2a_jobs["task-1"])
        assert gateway._a2a_progress_tasks == {}

    asyncio.run(scenario())


def test_a2a_cancellation_drains_worker_and_progress_tasks(tmp_path):
    gateway = _gateway(tmp_path)

    async def scenario():
        stop_event = asyncio.Event()
        progress_mod.start_a2a_progress("task-1")
        progress_task = asyncio.create_task(gateway._run_a2a_progress_updates(
            task_id="task-1",
            registry_key="task-1:message-1",
            data=_event()["data"],
            task_text="Calculate.",
            stop_event=stop_event,
        ))
        worker_task = asyncio.create_task(asyncio.sleep(30))
        gateway._a2a_progress_tasks["task-1"] = progress_task
        gateway._a2a_progress_stop_events["task-1"] = stop_event
        gateway._a2a_jobs["task-1"] = {worker_task}
        canceled = _event()
        canceled["event_type"] = "a2a.task.canceled"

        await gateway._on_a2a_event(canceled)

        assert progress_task.done()
        assert worker_task.cancelled()
        assert gateway._a2a_progress_tasks == {}
        assert gateway._a2a_progress_stop_events == {}
        assert gateway._a2a_jobs == {}
        assert progress_mod.a2a_activity_snapshot("task-1") == []

    asyncio.run(scenario())
