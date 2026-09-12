import asyncio
import json
import threading
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
            json_response=lambda payload, status=200: types.SimpleNamespace(
                status=status,
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
    gateway._a2a_ack_tasks = {}
    gateway._a2a_progress_tasks = {}
    gateway._a2a_progress_stop_events = {}
    gateway._a2a_progress_owners = {}
    gateway._a2a_progress_fences = {}
    gateway._a2a_canceled_tasks = {}
    gateway._a2a_ingest_lock = asyncio.Lock()
    gateway._closing = False
    gateway.cfg = BridgeConfig(project_dir=str(tmp_path))
    task = types.SimpleNamespace(
        id="task-1",
        context_id="context-1",
        state="submitted",
        caller=types.SimpleNamespace(
            identity_id="caller-1",
            organization_id="org-1",
            handle="caller",
        ),
        messages=[
            types.SimpleNamespace(
                role="ROLE_CALLER",
                message_id="message-1",
                parts=[{"text": "Investigate."}],
            )
        ],
    )

    def reply(task_id, **kwargs):
        gateway.replies.append((task_id, kwargs))
        if kwargs.get("intent") == "progress":
            task.state = "working"
        elif kwargs.get("intent") == "complete":
            task.state = "completed"
        task.messages.append(
            types.SimpleNamespace(role="agent", parts=[{"text": kwargs["text"]}])
        )

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
                role="ROLE_CALLER",
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


def test_a2a_catch_up_resumes_persisted_caller_data_not_worker_history(tmp_path):
    gateway = _gateway(tmp_path)
    data = _event()["data"] | {
        "parts": [{"text": "Original caller request."}],
    }
    key = "task-1:message-1"
    receipt = gateway_mod._a2a_receipt_text(
        "task-1",
        gateway.cfg.a2a_progress_interval_seconds,
    )
    progress = "I'm checking the original request. (180s elapsed)"
    gateway._write_a2a_registry(
        key,
        data,
        "running",
        receipt_text=receipt,
        progress_started=True,
    )
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
                role="ROLE_CALLER",
                message_id="message-1",
                parts=[{"text": "Original caller request."}],
            ),
            types.SimpleNamespace(
                role="ROLE_AGENT",
                message_id="message-ack",
                parts=[{"text": receipt}],
            ),
            types.SimpleNamespace(
                role="ROLE_AGENT",
                message_id="message-progress",
                parts=[{"text": progress}],
            ),
        ],
    )
    gateway._identity.a2a_task = lambda _task_id: task
    queried_states = []

    def iter_a2a_tasks(*, state):
        queried_states.append(state)
        return iter([task] if state == "working" else [])

    gateway._identity.iter_a2a_tasks = iter_a2a_tasks

    async def scenario():
        await gateway._catch_up_a2a_tasks()
        await asyncio.gather(*gateway._a2a_jobs["task-1"])

    asyncio.run(scenario())

    assert len(gateway.sessions.session.calls) == 1
    prompt, _context = gateway.sessions.session.calls[0]
    assert prompt.endswith("Original caller request.")
    assert receipt not in prompt
    assert progress not in prompt
    registry = json.loads(gateway._a2a_registry_path.read_text())
    assert list(registry) == [key]
    assert registry[key]["data"] == data | {"state": "working"}
    assert queried_states == ["submitted", "working"]


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


def test_a2a_acknowledgement_ignores_caller_spoof(tmp_path):
    gateway = _gateway(tmp_path)
    key = "task-1:message-1"
    data = _event()["data"]
    receipt = gateway_mod._a2a_receipt_text(
        "task-1",
        gateway.cfg.a2a_progress_interval_seconds,
    )
    gateway._a2a_authoritative_task.messages.append(
        types.SimpleNamespace(role="caller", parts=[{"text": receipt}])
    )
    gateway._write_a2a_registry(key, data, "queued")

    asyncio.run(gateway._record_a2a_acknowledgement(key, data))

    assert gateway.replies[-1][1]["text"] == receipt


@pytest.mark.parametrize(
    ("state", "canonical"),
    [
        ("completed", "completed"),
        ("TASK_STATE_FAILED", "failed"),
        ("A2ATaskState.CANCELED", "canceled"),
        ("rejected", "rejected"),
        ("TASK_STATE_INPUT_REQUIRED", "input_required"),
        ("auth_required", "auth_required"),
    ],
)
def test_delayed_a2a_webhook_stops_before_worker_turn(
    tmp_path,
    state,
    canonical,
):
    gateway = _gateway(tmp_path)
    gateway._a2a_authoritative_task.state = state

    response = asyncio.run(gateway._on_a2a_event(_event()))

    assert json.loads(response.text)["ignored"] == f"task-{canonical}"
    assert gateway.replies == []
    assert gateway.sessions.keys == []
    assert gateway.sessions.session.calls == []
    assert gateway._a2a_jobs == {}
    assert not gateway._a2a_registry_path.exists()


def test_delayed_duplicate_webhook_finalizes_acknowledged_task(tmp_path):
    gateway = _gateway(tmp_path)
    key = "task-1:message-1"
    data = _event()["data"]
    receipt = gateway_mod._a2a_receipt_text(
        "task-1",
        gateway.cfg.a2a_progress_interval_seconds,
    )
    gateway._write_a2a_registry(
        key,
        data,
        "running",
        receipt_text=receipt,
        receipt_delivered=True,
    )
    gateway._a2a_authoritative_task.state = "TASK_STATE_COMPLETED"

    response = asyncio.run(gateway._on_a2a_event(_event()))

    assert json.loads(response.text)["ignored"] == "task-completed"
    assert gateway.sessions.keys == []
    assert gateway._a2a_jobs == {}
    saved = json.loads(gateway._a2a_registry_path.read_text())[key]
    assert saved["state"] == "finalized"


def test_a2a_acknowledgement_accepts_raw_agent_role(tmp_path):
    gateway = _gateway(tmp_path)
    key = "task-1:message-1"
    data = _event()["data"]
    receipt = gateway_mod._a2a_receipt_text(
        "task-1",
        gateway.cfg.a2a_progress_interval_seconds,
    )
    gateway._a2a_authoritative_task.messages.append(
        types.SimpleNamespace(role="ROLE_AGENT", parts=[{"text": receipt}])
    )
    gateway._write_a2a_registry(key, data, "queued")
    reply_count = len(gateway.replies)

    asyncio.run(gateway._record_a2a_acknowledgement(key, data))

    assert len(gateway.replies) == reply_count


def test_failed_a2a_acknowledgement_keeps_referenced_background_retry(
    tmp_path,
    monkeypatch,
):
    gateway = _gateway(tmp_path)
    monkeypatch.setattr(gateway_mod, "_A2A_RETRY_INTERVAL_SECONDS", 0)
    attempts = 0
    original_reply = gateway._identity.a2a_reply

    def fail_once(task_id, **kwargs):
        nonlocal attempts
        if kwargs.get("intent") == "progress":
            attempts += 1
        if kwargs.get("intent") == "progress" and attempts == 1:
            raise OSError("temporarily unavailable")
        original_reply(task_id, **kwargs)

    gateway._identity.a2a_reply = fail_once

    async def stay_active(prompt, *, a2a_context=None):
        gateway.sessions.session.calls.append((prompt, a2a_context))
        return "[SILENT]"

    gateway.sessions.session.run_consult = stay_active

    async def scenario():
        response = await gateway._on_a2a_event(_event())
        assert response.status == 200
        assert "task-1:message-1" in gateway._a2a_ack_tasks
        pending = json.loads(gateway._a2a_registry_path.read_text())[
            "task-1:message-1"
        ]["receipt"]
        assert pending["pending_text"].startswith("Task task-1")
        retry = gateway._a2a_ack_tasks["task-1:message-1"][1]
        await retry
        await asyncio.gather(*gateway._a2a_jobs["task-1"])

    asyncio.run(scenario())

    assert attempts == 2
    receipt = json.loads(gateway._a2a_registry_path.read_text())[
        "task-1:message-1"
    ]["receipt"]
    assert "pending_text" not in receipt
    assert receipt["delivered_text"].startswith("Task task-1")


def test_a2a_catch_up_recovers_pending_ack_without_rerunning_finalized_turn(
    tmp_path,
):
    gateway = _gateway(tmp_path)
    key = "task-1:message-1"
    data = _event()["data"]
    receipt = gateway_mod._a2a_receipt_text(
        "task-1",
        gateway.cfg.a2a_progress_interval_seconds,
    )
    gateway._write_a2a_registry(key, data, "queued", receipt_text=receipt)
    gateway._write_a2a_registry(key, data, "finalized")
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
                role="caller",
                message_id="message-1",
                parts=[{"text": "Investigate."}],
            )
        ],
    )
    gateway._identity.a2a_task = lambda _task_id: task
    gateway._identity.iter_a2a_tasks = lambda **_kwargs: iter(())

    asyncio.run(gateway._catch_up_a2a_tasks())

    saved = json.loads(gateway._a2a_registry_path.read_text())[key]
    assert saved["state"] == "finalized"
    assert saved["receipt"]["delivered_text"] == receipt
    assert gateway._a2a_jobs == {}


def test_a2a_progress_summary_rejects_terminal_claim():
    terminal_updates = (
        "Done — the task is complete.",
        "The final answer is ready.",
        "I cannot continue without the records.",
        "I'm waiting for your input.",
    )
    for update in terminal_updates:
        assert progress_mod._clean_update(update, ["run_tests"]) == (
            "I'm continuing the requested work."
        )


def test_a2a_progress_summary_allows_nonterminal_status_words():
    updates = (
        "I'm ready to review the next records.",
        "The query succeeded and I'm checking the response.",
        "The issue appears resolved, so I'm validating related behavior.",
        "I'm finalizing the analysis now.",
    )
    for update in updates:
        assert progress_mod._clean_update(update, ["run_tests"]) == update


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
        tool_names=["read_file"],
        previous_update="I'm checking the request.",
        project_dir="/tmp",
    ))

    assert update == "I'm reviewing the requested calculation."
    assert captured["options"]["tools"] == []
    assert captured["options"]["allowed_tools"] == []
    assert captured["options"]["max_turns"] == 1
    assert "Inspect the calculation." in captured["prompt"]
    assert "read_file" in captured["prompt"]
    assert "I'm checking the request." in captured["prompt"]


def test_a2a_progress_summary_rejects_echoed_tool_identifier():
    for update in (
        "browser_search",
        "I'm using browser search to investigate.",
    ):
        assert progress_mod._clean_update(update, ["browser_search"]) == (
            "I'm continuing the requested work."
        )


def test_a2a_progress_summary_rejects_tool_identifier_across_separators():
    # The leak guard must suppress an echoed tool name regardless of the
    # surrounding punctuation, not only underscores. The remote agent controls
    # the task text (an untrusted prompt-injection surface), and the update is
    # sent on to that untrusted agent, so a tool name bordered by '-', '.', or
    # ':' must not slip through to them.
    for update in (
        "Running bash-based checks now",
        "Inspecting the bash.exe helper",
        "Using bash:mode for this step",
    ):
        assert progress_mod._clean_update(update, ["bash"]) == (
            "I'm continuing the requested work."
        )


def test_a2a_progress_tool_names_are_bounded_and_do_not_retain_inputs():
    progress_mod.start_a2a_progress("task-1")

    progress_mod.observe_a2a_tool_start("task-1", "run/sql query\n")
    for index in range(9):
        progress_mod.observe_a2a_tool_start(
            "task-1",
            f"Tool {index} {'x' * 100}",
        )

    snapshot = progress_mod.a2a_tool_snapshot("task-1")
    progress_mod.stop_a2a_progress("task-1")
    assert len(snapshot) == 8
    assert snapshot[0].startswith("tool_1_")
    assert all(len(tool_name) <= 80 for tool_name in snapshot)
    assert progress_mod._safe_tool_name("run/sql query\n") == "run_sql_query"
    assert progress_mod._fallback_update() == "I'm continuing the requested work."


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
        types.SimpleNamespace(role="ROLE_AGENT", parts=[{"text": update}])
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


def test_a2a_progress_retry_ignores_caller_spoof(tmp_path):
    gateway = _gateway(tmp_path)
    gateway._a2a_authoritative_task.state = "working"
    update = "I'm validating the work. (60s elapsed)"
    gateway._a2a_authoritative_task.messages.append(
        types.SimpleNamespace(role="caller", parts=[{"text": update}])
    )
    key = "task-1:message-1"
    data = _event()["data"]
    gateway._write_a2a_registry(key, data, "running", progress_started=True)
    gateway._write_a2a_registry(key, data, "running", progress_text=update)

    asyncio.run(gateway._emit_a2a_progress_update(
        task_id="task-1",
        registry_key=key,
        data=data,
        task_text="Validate the work.",
    ))

    assert gateway.replies[-1][1]["text"] == update


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


def test_a2a_progress_follow_up_preserves_near_boundary_cadence(
    tmp_path,
    monkeypatch,
):
    gateway = _gateway(tmp_path)
    gateway.cfg.a2a_progress_interval_seconds = 180
    now = [1_000.0]
    monkeypatch.setattr(gateway_mod.time, "time", lambda: now[0])
    first_key = "task-1:message-1"
    gateway._write_a2a_registry(
        first_key,
        _event()["data"],
        "running",
        progress_started=True,
    )
    now[0] = 1_179.0
    follow_up = _event()["data"] | {"message_id": "message-2"}
    second_key = "task-1:message-2"
    gateway._write_a2a_registry(
        second_key,
        follow_up,
        "running",
        progress_started=True,
    )
    sleeps = []

    async def fake_wait_for(awaitable, timeout):
        awaitable.close()
        sleeps.append(timeout)
        raise asyncio.TimeoutError

    async def stop_after_one(**_kwargs):
        return False

    monkeypatch.setattr(gateway_mod.asyncio, "wait_for", fake_wait_for)
    gateway._emit_a2a_progress_update = stop_after_one

    asyncio.run(gateway._run_a2a_progress_updates(
        task_id="task-1",
        registry_key=second_key,
        data=follow_up,
        task_text="Calculate.",
        stop_event=asyncio.Event(),
    ))

    assert sleeps == [1.0]
    registry = json.loads(gateway._a2a_registry_path.read_text())
    assert registry[second_key]["progress"]["next_due_at"] == 1_180.0


def test_a2a_pending_progress_retries_immediately_after_restart(
    tmp_path,
    monkeypatch,
):
    gateway = _gateway(tmp_path)
    gateway.cfg.a2a_progress_interval_seconds = 180
    key = "task-1:message-1"
    data = _event()["data"]
    gateway._write_a2a_registry(key, data, "running", progress_started=True)
    gateway._write_a2a_registry(
        key,
        data,
        "running",
        progress_text="I'm checking the calculation. (180s elapsed)",
    )
    sleeps = []

    async def fake_wait_for(awaitable, timeout):
        awaitable.close()
        sleeps.append(timeout)
        raise asyncio.TimeoutError

    async def stop_after_one(**_kwargs):
        return False

    monkeypatch.setattr(gateway_mod.asyncio, "wait_for", fake_wait_for)
    gateway._emit_a2a_progress_update = stop_after_one

    asyncio.run(gateway._run_a2a_progress_updates(
        task_id="task-1",
        registry_key=key,
        data=data,
        task_text="Calculate.",
        stop_event=asyncio.Event(),
    ))

    assert sleeps == [0]


@pytest.mark.parametrize(
    "state",
    ["completed", "input_required", "TASK_STATE_AUTH_REQUIRED"],
)
def test_a2a_progress_stops_for_terminal_or_waiting_task(tmp_path, state):
    gateway = _gateway(tmp_path)
    gateway._a2a_authoritative_task.state = state
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
    key = "task-1:message-1"
    gateway._write_a2a_registry(
        key,
        _event()["data"],
        "running",
        progress_started=True,
    )
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
        registry_key=key,
        data=_event()["data"],
        task_text="Calculate.",
        stop_event=asyncio.Event(),
    ))

    assert sleeps == [pytest.approx(60, abs=0.1)]
    assert emissions == [{
        "task_id": "task-1",
        "registry_key": "task-1:message-1",
        "data": _event()["data"],
        "task_text": "Calculate.",
    }]


def test_older_a2a_turn_cannot_stop_follow_up_progress_runner(tmp_path):
    gateway = _gateway(tmp_path)
    first = _event()["data"]
    second = first | {"message_id": "message-2"}

    async def scenario():
        await gateway._start_a2a_progress_updates(
            task_id="task-1",
            registry_key="task-1:message-1",
            data=first,
            task_text="First turn.",
        )
        await gateway._start_a2a_progress_updates(
            task_id="task-1",
            registry_key="task-1:message-2",
            data=second,
            task_text="Follow up.",
        )
        replacement = gateway._a2a_progress_tasks["task-1"]

        # The older worker turn reaches its finally block after the follow-up
        # already owns the task's progress runner.
        await gateway._stop_a2a_progress_updates(
            "task-1",
            owner="task-1:message-1",
        )

        assert gateway._a2a_progress_tasks["task-1"] is replacement
        assert not replacement.done()
        assert gateway._a2a_progress_owners["task-1"] == "task-1:message-2"
        await gateway._stop_a2a_progress_updates(
            "task-1",
            owner="task-1:message-2",
        )

    asyncio.run(scenario())


def test_terminal_fence_drains_inflight_progress_before_reply(tmp_path):
    gateway = _gateway(tmp_path)
    gateway._a2a_authoritative_task.state = "working"
    key = "task-1:message-1"
    data = _event()["data"]
    update = "I'm validating the work. (180s elapsed)"
    gateway._write_a2a_registry(key, data, "running", progress_started=True)
    gateway._write_a2a_registry(key, data, "running", progress_text=update)
    started = threading.Event()
    release = threading.Event()
    events = []
    original_reply = gateway._identity.a2a_reply

    def paused_reply(task_id, **kwargs):
        if kwargs.get("intent") == "progress" and kwargs.get("text") == update:
            events.append("progress-started")
            started.set()
            assert release.wait(timeout=5)
            events.append("progress-finished")
        return original_reply(task_id, **kwargs)

    gateway._identity.a2a_reply = paused_reply

    async def scenario():
        progress_task = asyncio.create_task(gateway._emit_a2a_progress_update(
            task_id="task-1",
            registry_key=key,
            data=data,
            task_text="Validate the work.",
        ))
        gateway._a2a_progress_tasks["task-1"] = progress_task
        gateway._a2a_progress_stop_events["task-1"] = asyncio.Event()
        gateway._a2a_progress_owners["task-1"] = key
        assert await asyncio.to_thread(started.wait, 5)

        fence_task = asyncio.create_task(
            gateway._fence_a2a_progress_updates("task-1", key, data)
        )
        await asyncio.sleep(0)
        assert not fence_task.done()
        events.append("terminal-waiting")
        release.set()
        await fence_task
        events.append("terminal-reply")

        assert await progress_task is True
        assert await gateway._emit_a2a_progress_update(
            task_id="task-1",
            registry_key=key,
            data=data,
            task_text="Validate the work.",
        ) is False

    asyncio.run(scenario())

    assert events == [
        "progress-started",
        "terminal-waiting",
        "progress-finished",
        "terminal-reply",
    ]
    saved = json.loads(gateway._a2a_registry_path.read_text())[key]
    assert saved["progress"]["fenced"] is True


def test_a2a_follow_up_reacquires_durably_fenced_progress(tmp_path):
    gateway = _gateway(tmp_path)
    first_key = "task-1:message-1"
    first = _event()["data"]
    second_key = "task-1:message-2"
    second = first | {
        "message_id": "message-2",
        "parts": [{"text": "The region is west."}],
    }

    async def scenario():
        gateway._write_a2a_registry(
            first_key,
            first,
            "running",
            progress_started=True,
        )
        await gateway._fence_a2a_progress_updates("task-1", first_key, first)

        # Restarting the same turn must retain an ambiguous terminal fence.
        await gateway._start_a2a_progress_updates(
            task_id="task-1",
            registry_key=first_key,
            data=first,
            task_text="Investigate.",
        )
        assert gateway._a2a_progress_tasks == {}

        # A genuine caller follow-up has a new message key and starts a new owner.
        gateway._write_a2a_registry(second_key, second, "running")
        await gateway._start_a2a_progress_updates(
            task_id="task-1",
            registry_key=second_key,
            data=second,
            task_text="The region is west.",
        )
        assert gateway._a2a_progress_owners["task-1"] == second_key
        assert "task-1" not in gateway._a2a_progress_fences
        await gateway._stop_a2a_progress_updates("task-1", owner=second_key)

    asyncio.run(scenario())

    saved = json.loads(gateway._a2a_registry_path.read_text())
    assert saved[first_key]["progress"]["fenced"] is True
    assert saved[second_key]["progress"].get("fenced") is not True


def test_a2a_completion_cancels_progress_timer(tmp_path):
    gateway = _gateway(tmp_path)

    async def scenario():
        await gateway._on_a2a_event(_event())
        await asyncio.gather(*gateway._a2a_jobs["task-1"])
        assert gateway._a2a_progress_tasks == {}

    asyncio.run(scenario())


def test_implicit_completion_response_loss_stays_fenced_across_restart(tmp_path):
    gateway = _gateway(tmp_path)
    original_reply = gateway._identity.a2a_reply

    def committed_then_lost(task_id, **kwargs):
        original_reply(task_id, **kwargs)
        if kwargs.get("intent") == "complete":
            raise OSError("response lost")

    gateway._identity.a2a_reply = committed_then_lost

    async def first_process():
        await gateway._on_a2a_event(_event())
        await asyncio.gather(*gateway._a2a_jobs["task-1"])

    asyncio.run(first_process())

    before_restart = json.loads(gateway._a2a_registry_path.read_text())[
        "task-1:message-1"
    ]
    assert before_restart["state"] == "running"
    assert before_restart["progress"]["fenced"] is True

    restarted = _gateway(tmp_path)
    receipt = gateway_mod._a2a_receipt_text(
        "task-1",
        restarted.cfg.a2a_progress_interval_seconds,
    )
    task = types.SimpleNamespace(
        id="task-1",
        context_id="context-1",
        state="completed",
        caller=types.SimpleNamespace(
            identity_id="caller-1",
            organization_id="org-1",
            handle="caller",
        ),
        messages=[
            types.SimpleNamespace(
                role="ROLE_CALLER",
                message_id="message-1",
                parts=[{"text": "Investigate."}],
            ),
            types.SimpleNamespace(
                role="ROLE_AGENT",
                message_id="message-ack",
                parts=[{"text": receipt}],
            ),
            types.SimpleNamespace(
                role="ROLE_AGENT",
                message_id="message-complete",
                parts=[{"text": "Completed."}],
            ),
        ],
    )
    restarted._identity.a2a_task = lambda _task_id: task
    restarted._identity.iter_a2a_tasks = lambda **_kwargs: iter(())

    asyncio.run(restarted._catch_up_a2a_tasks())

    recovered = json.loads(restarted._a2a_registry_path.read_text())[
        "task-1:message-1"
    ]
    assert recovered["state"] == "finalized"
    assert recovered["progress"]["fenced"] is True
    assert restarted.sessions.session.calls == []
    assert restarted.replies == []
    assert restarted._a2a_jobs == {}
    assert restarted._a2a_progress_tasks == {}


def test_implicit_completion_failure_does_not_rerun_fenced_turn(tmp_path):
    gateway = _gateway(tmp_path)
    original_reply = gateway._identity.a2a_reply

    def failed_without_commit(task_id, **kwargs):
        if kwargs.get("intent") == "complete":
            raise OSError("request outcome unknown")
        original_reply(task_id, **kwargs)

    gateway._identity.a2a_reply = failed_without_commit

    async def first_process():
        await gateway._on_a2a_event(_event())
        await asyncio.gather(*gateway._a2a_jobs["task-1"])

    asyncio.run(first_process())

    saved = json.loads(gateway._a2a_registry_path.read_text())[
        "task-1:message-1"
    ]
    assert saved["state"] == "running"
    assert saved["progress"]["fenced"] is True

    restarted = _gateway(tmp_path)
    receipt = gateway_mod._a2a_receipt_text(
        "task-1",
        restarted.cfg.a2a_progress_interval_seconds,
    )
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
                role="ROLE_CALLER",
                message_id="message-1",
                parts=[{"text": "Investigate."}],
            ),
            types.SimpleNamespace(
                role="ROLE_AGENT",
                message_id="message-ack",
                parts=[{"text": receipt}],
            ),
        ],
    )
    restarted._identity.a2a_task = lambda _task_id: task
    queried_states = []

    def iter_a2a_tasks(*, state):
        queried_states.append(state)
        return iter((task,)) if state == "working" else iter(())

    restarted._identity.iter_a2a_tasks = iter_a2a_tasks

    asyncio.run(restarted._catch_up_a2a_tasks())

    assert restarted.sessions.session.calls == []
    assert restarted.replies == []
    assert restarted._a2a_jobs == {}
    assert queried_states == ["submitted", "working"]

    async def follow_up():
        task.messages.append(types.SimpleNamespace(
            role="ROLE_CALLER",
            message_id="message-2",
            parts=[{"text": "Use the west region."}],
        ))
        event = _event()
        event["event_type"] = "a2a.task.message"
        event["data"] = event["data"] | {
            "message_id": "message-2",
            "parts": [{"text": "Use the west region."}],
        }
        await restarted._on_a2a_event(event)
        await asyncio.gather(*restarted._a2a_jobs["task-1"])

    asyncio.run(follow_up())

    assert len(restarted.sessions.session.calls) == 1
    assert restarted.sessions.session.calls[0][0].endswith("Use the west region.")
    registry = json.loads(restarted._a2a_registry_path.read_text())
    assert registry["task-1:message-2"]["state"] == "finalized"


@pytest.mark.parametrize("stopped_state", ["TASK_STATE_INPUT_REQUIRED", "auth_required"])
def test_waiting_state_restart_settles_and_new_caller_reacquires(
    tmp_path,
    stopped_state,
):
    gateway = _gateway(tmp_path)
    key = "task-1:message-1"
    data = _event()["data"]
    receipt = gateway_mod._a2a_receipt_text(
        "task-1",
        gateway.cfg.a2a_progress_interval_seconds,
    )
    gateway._write_a2a_registry(
        key,
        data,
        "running",
        receipt_text=receipt,
        progress_started=True,
        progress_fenced=True,
    )
    task = types.SimpleNamespace(
        id="task-1",
        context_id="context-1",
        state=stopped_state,
        caller=types.SimpleNamespace(
            identity_id="caller-1",
            organization_id="org-1",
            handle="caller",
        ),
        messages=[
            types.SimpleNamespace(
                role="ROLE_CALLER",
                message_id="message-1",
                parts=[{"text": "Investigate."}],
            ),
            types.SimpleNamespace(
                role="ROLE_AGENT",
                message_id="message-ack",
                parts=[{"text": receipt}],
            ),
            types.SimpleNamespace(
                role="ROLE_AGENT",
                message_id="message-question",
                parts=[{"text": "Which region?"}],
            ),
        ],
    )
    gateway._identity.a2a_task = lambda _task_id: task
    gateway._identity.iter_a2a_tasks = lambda **_kwargs: iter(())

    asyncio.run(gateway._catch_up_a2a_tasks())

    settled = json.loads(gateway._a2a_registry_path.read_text())[key]
    assert settled["state"] == "finalized"
    assert gateway.replies == []
    assert gateway.sessions.session.calls == []
    assert gateway._a2a_jobs == {}
    assert gateway._a2a_progress_tasks == {}

    async def follow_up():
        entered = asyncio.Event()
        release = asyncio.Event()

        async def wait_for_release(prompt, *, a2a_context=None):
            gateway.sessions.session.calls.append((prompt, a2a_context))
            entered.set()
            await release.wait()
            return "[SILENT]"

        gateway.sessions.session.run_consult = wait_for_release
        task.state = "working"
        task.messages.append(types.SimpleNamespace(
            role="ROLE_CALLER",
            message_id="message-2",
            parts=[{"text": "Use the west region."}],
        ))
        follow_up_event = _event()
        follow_up_event["event_type"] = "a2a.task.message"
        follow_up_event["data"] = data | {
            "message_id": "message-2",
            "parts": [{"text": "Use the west region."}],
        }

        await gateway._on_a2a_event(follow_up_event)
        await entered.wait()
        assert gateway._a2a_progress_owners["task-1"] == "task-1:message-2"
        assert "task-1" not in gateway._a2a_progress_fences
        release.set()
        await asyncio.gather(*gateway._a2a_jobs["task-1"])

    asyncio.run(follow_up())

    registry = json.loads(gateway._a2a_registry_path.read_text())
    assert registry["task-1:message-2"]["state"] == "finalized"


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
        acknowledgement_task = asyncio.create_task(asyncio.sleep(30))
        worker_task = asyncio.create_task(asyncio.sleep(30))
        gateway._a2a_ack_tasks["task-1:message-1"] = (
            "task-1",
            acknowledgement_task,
        )
        gateway._a2a_progress_tasks["task-1"] = progress_task
        gateway._a2a_progress_stop_events["task-1"] = stop_event
        gateway._a2a_progress_owners["task-1"] = "task-1:message-1"
        gateway._a2a_jobs["task-1"] = {worker_task}
        canceled = _event()
        canceled["event_type"] = "a2a.task.canceled"

        await gateway._on_a2a_event(canceled)

        assert progress_task.done()
        assert acknowledgement_task.cancelled()
        assert worker_task.cancelled()
        assert gateway._a2a_ack_tasks == {}
        assert gateway._a2a_progress_tasks == {}
        assert gateway._a2a_progress_stop_events == {}
        assert gateway._a2a_progress_owners == {}
        assert gateway._a2a_jobs == {}
        assert progress_mod.a2a_tool_snapshot("task-1") == []

    asyncio.run(scenario())


def test_a2a_cancellation_fences_webhook_blocked_in_acknowledgement(tmp_path):
    gateway = _gateway(tmp_path)
    task = gateway._a2a_authoritative_task
    original_reply = gateway._identity.a2a_reply
    acknowledgement_started = threading.Event()
    release_acknowledgement = threading.Event()

    def paused_reply(task_id, **kwargs):
        if kwargs.get("intent") == "progress":
            acknowledgement_started.set()
            assert release_acknowledgement.wait(5)
        return original_reply(task_id, **kwargs)

    gateway._identity.a2a_reply = paused_reply

    async def scenario():
        webhook = asyncio.create_task(gateway._on_a2a_event(_event()))
        assert await asyncio.to_thread(acknowledgement_started.wait, 5)
        task.state = "canceled"
        canceled = _event()
        canceled["event_type"] = "a2a.task.canceled"
        cancellation = asyncio.create_task(gateway._on_a2a_event(canceled))
        await asyncio.sleep(0)

        assert not cancellation.done()
        assert gateway.sessions.keys == []
        release_acknowledgement.set()
        response, _ = await asyncio.gather(webhook, cancellation)

        assert json.loads(response.text)["ignored"] == "task-canceled"
        assert gateway.sessions.keys == []
        assert gateway.sessions.session.calls == []
        assert gateway._a2a_jobs == {}
        saved = json.loads(gateway._a2a_registry_path.read_text())[
            "task-1:message-1"
        ]
        assert saved["state"] == "finalized"

        task.state = "working"
        task.messages.append(types.SimpleNamespace(
            role="ROLE_CALLER",
            message_id="message-2",
            parts=[{"text": "Use the west region."}],
        ))
        follow_up = _event()
        follow_up["event_type"] = "a2a.task.message"
        follow_up["data"] = follow_up["data"] | {
            "message_id": "message-2",
            "parts": [{"text": "Use the west region."}],
        }
        await gateway._on_a2a_event(follow_up)
        await asyncio.gather(*gateway._a2a_jobs["task-1"])

        assert len(gateway.sessions.session.calls) == 1
        assert gateway.sessions.session.calls[0][0].endswith(
            "Use the west region."
        )

    asyncio.run(scenario())


def test_a2a_cancel_before_admission_blocks_current_generation(tmp_path):
    gateway = _gateway(tmp_path)

    async def scenario():
        canceled = _event()
        canceled["event_type"] = "a2a.task.canceled"
        await gateway._on_a2a_event(canceled)

        gateway._a2a_authoritative_task.state = "working"
        current = await gateway._on_a2a_event(_event())
        assert json.loads(current.text)["ignored"] == "task-canceled"
        assert gateway.sessions.session.calls == []
        assert gateway._a2a_jobs == {}

        async def stay_active(prompt, *, a2a_context=None):
            gateway.sessions.session.calls.append((prompt, a2a_context))
            return "[SILENT]"

        gateway.sessions.session.run_consult = stay_active
        gateway._a2a_authoritative_task.messages.append(types.SimpleNamespace(
            role="ROLE_CALLER",
            message_id="message-2",
            parts=[{"text": "Use the west region."}],
        ))
        follow_up = _event()
        follow_up["event_type"] = "a2a.task.message"
        follow_up["data"] = follow_up["data"] | {
            "message_id": "message-2",
            "parts": [{"text": "Use the west region."}],
        }
        await gateway._on_a2a_event(follow_up)
        await asyncio.gather(*gateway._a2a_jobs["task-1"])
        duplicate = await gateway._on_a2a_event(follow_up)

        assert len(gateway.sessions.session.calls) == 1
        assert json.loads(duplicate.text)["deduped"] is True
        assert "task-1" not in gateway._a2a_canceled_tasks

    asyncio.run(scenario())


def test_a2a_cancel_without_message_id_uses_authoritative_caller(tmp_path):
    gateway = _gateway(tmp_path)
    task = gateway._a2a_authoritative_task
    task.messages.append(types.SimpleNamespace(
        role="ROLE_CALLER",
        message_id="message-1",
        parts=[{"text": "Investigate."}],
    ))
    canceled = _event()
    canceled["event_type"] = "a2a.task.canceled"
    canceled["data"] = dict(canceled["data"])
    canceled["data"].pop("message_id")

    asyncio.run(gateway._on_a2a_event(canceled))

    assert gateway._a2a_canceled_tasks["task-1"] == {"task-1:message-1"}
    task.state = "working"
    response = asyncio.run(gateway._on_a2a_event(_event()))
    assert json.loads(response.text)["ignored"] == "task-canceled"
    assert gateway.sessions.session.calls == []
    assert gateway._a2a_jobs == {}


@pytest.mark.parametrize(
    ("case", "expected"),
    [
        ("spoof-distinct", "task-canceled"),
        ("non-caller", "task-canceled"),
        ("wrong-context", "task-canceled"),
        ("stopped", "task-completed"),
    ],
)
def test_a2a_canceled_tombstone_requires_authoritative_caller(
    tmp_path,
    case,
    expected,
):
    gateway = _gateway(tmp_path)
    task = gateway._a2a_authoritative_task
    gateway._a2a_canceled_tasks["task-1"] = {"task-1:message-1"}
    task.messages.append(types.SimpleNamespace(
        role="ROLE_CALLER",
        message_id="message-1",
        parts=[{"text": "Investigate."}],
    ))
    if case in {"wrong-context", "stopped"}:
        task.messages.append(types.SimpleNamespace(
            role="ROLE_CALLER",
            message_id="message-2",
            parts=[{"text": "Use the west region."}],
        ))
    elif case == "non-caller":
        task.messages.append(types.SimpleNamespace(
            role="ROLE_AGENT",
            message_id="message-2",
            parts=[{"text": "Worker progress."}],
        ))
    if case == "stopped":
        task.state = "completed"

    event = _event()
    event["event_type"] = "a2a.task.message"
    event["data"] = event["data"] | {
        "context_id": "context-2" if case == "wrong-context" else "context-1",
        "message_id": "message-2",
        "parts": [{"text": "Use the west region."}],
    }
    response = asyncio.run(gateway._on_a2a_event(event))

    assert json.loads(response.text)["ignored"] == expected
    assert gateway.sessions.session.calls == []
    assert gateway._a2a_jobs == {}


def test_a2a_restart_rejects_canceled_generation_and_admits_latest_caller(tmp_path):
    original = _gateway(tmp_path)
    canceled = _event()
    canceled["event_type"] = "a2a.task.canceled"
    asyncio.run(original._on_a2a_event(canceled))

    restarted = _gateway(tmp_path)
    task = restarted._a2a_authoritative_task
    task.state = "working"
    task.messages.append(types.SimpleNamespace(
        role="ROLE_CALLER",
        message_id="message-2",
        parts=[{"text": "Use the authoritative request."}],
    ))

    async def scenario():
        delayed = await restarted._on_a2a_event(_event())
        assert json.loads(delayed.text)["ignored"] == "stale-a2a-event"
        assert restarted.replies == []
        assert restarted.sessions.session.calls == []

        follow_up = _event()
        follow_up["event_type"] = "a2a.task.message"
        follow_up["data"] = follow_up["data"] | {
            "message_id": "message-2",
            "parts": [{"text": "Untrusted webhook text."}],
        }
        accepted = await restarted._on_a2a_event(follow_up)
        await asyncio.gather(*restarted._a2a_jobs["task-1"])
        duplicate = await restarted._on_a2a_event(follow_up)
        return accepted, duplicate

    accepted, duplicate = asyncio.run(scenario())

    assert json.loads(accepted.text) == {"ok": True}
    assert json.loads(duplicate.text)["deduped"] is True
    assert len(restarted.sessions.session.calls) == 1
    prompt, _context = restarted.sessions.session.calls[0]
    assert prompt.endswith("Use the authoritative request.")
    assert "Untrusted webhook text." not in prompt


def test_a2a_admission_uses_authoritative_parts_and_caller_metadata(tmp_path):
    gateway = _gateway(tmp_path)
    task = gateway._a2a_authoritative_task
    task.messages[0].parts = [{"text": "Authoritative instructions."}]
    event = _event()
    event["data"] = event["data"] | {
        "caller": {
            "identity_id": "spoofed-caller",
            "organization_id": "spoofed-org",
            "handle": "spoofed",
        },
        "parts": [{"text": "Spoofed instructions."}],
    }

    async def scenario():
        await gateway._on_a2a_event(event)
        await asyncio.gather(*gateway._a2a_jobs["task-1"])

    asyncio.run(scenario())

    prompt, _context = gateway.sessions.session.calls[0]
    assert prompt.endswith("Authoritative instructions.")
    assert "Spoofed instructions." not in prompt
    assert "caller=@caller caller_org=org-1" in prompt
    assert "spoofed" not in prompt
    saved = json.loads(gateway._a2a_registry_path.read_text())[
        "task-1:message-1"
    ]["data"]
    assert saved["parts"] == [{"text": "Authoritative instructions."}]
    assert saved["caller"]["identity_id"] == "caller-1"


@pytest.mark.parametrize(
    ("case", "expected"),
    [
        ("task-identity", "stale-a2a-event"),
        ("context-identity", "stale-a2a-event"),
        ("inactive-state", "task-inactive"),
        ("non-caller-role", "stale-a2a-event"),
    ],
)
def test_a2a_admission_rejects_non_authoritative_task(case, expected, tmp_path):
    gateway = _gateway(tmp_path)
    task = gateway._a2a_authoritative_task
    if case == "task-identity":
        task.id = "task-2"
    elif case == "context-identity":
        task.context_id = "context-2"
    elif case == "inactive-state":
        task.state = "queued"
    elif case == "non-caller-role":
        task.messages[0].role = "ROLE_AGENT"

    response = asyncio.run(gateway._on_a2a_event(_event()))

    assert json.loads(response.text)["ignored"] == expected
    assert gateway.replies == []
    assert gateway.sessions.keys == []
    assert gateway.sessions.session.calls == []
    assert gateway._a2a_jobs == {}
    assert not gateway._a2a_registry_path.exists()


def test_a2a_pending_ack_duplicate_rejects_stale_spoofed_generation(tmp_path):
    gateway = _gateway(tmp_path)
    task = gateway._a2a_authoritative_task
    task.state = "working"
    task.messages.append(types.SimpleNamespace(
        role="ROLE_CALLER",
        message_id="message-2",
        parts=[{"text": "Current caller request."}],
    ))
    stale_data = _event()["data"] | {
        "caller": {"identity_id": "spoofed", "handle": "spoofed"},
        "parts": [{"text": "Spoofed stale request."}],
    }
    key = "task-1:message-1"
    gateway._write_a2a_registry(
        key,
        stale_data,
        "queued",
        receipt_text=gateway_mod._a2a_receipt_text(
            "task-1",
            gateway.cfg.a2a_progress_interval_seconds,
        ),
    )

    response = asyncio.run(gateway._on_a2a_event(_event()))

    assert json.loads(response.text)["ignored"] == "stale-a2a-event"
    assert gateway.replies == []
    assert gateway.sessions.session.calls == []
    saved = json.loads(gateway._a2a_registry_path.read_text())[key]
    assert saved["state"] == "finalized"
    assert "pending_text" not in saved.get("receipt", {})


def test_a2a_pending_ack_duplicate_uses_authoritative_payload(tmp_path):
    gateway = _gateway(tmp_path)
    task = gateway._a2a_authoritative_task
    task.messages[0].parts = [{"text": "Authoritative request."}]
    spoofed = _event()["data"] | {
        "caller": {"identity_id": "spoofed", "handle": "spoofed"},
        "parts": [{"text": "Spoofed request."}],
    }
    key = "task-1:message-1"
    gateway._write_a2a_registry(
        key,
        spoofed,
        "queued",
        receipt_text=gateway_mod._a2a_receipt_text(
            "task-1",
            gateway.cfg.a2a_progress_interval_seconds,
        ),
    )
    event = _event()
    event["data"] = spoofed

    response = asyncio.run(gateway._on_a2a_event(event))

    assert json.loads(response.text)["deduped"] is True
    assert len(gateway.replies) == 1
    assert gateway.sessions.session.calls == []
    saved = json.loads(gateway._a2a_registry_path.read_text())[key]
    assert saved["data"]["parts"] == [{"text": "Authoritative request."}]
    assert saved["data"]["caller"]["identity_id"] == "caller-1"
    assert "pending_text" not in saved["receipt"]


def test_a2a_ack_retry_rejects_stale_generation(tmp_path, monkeypatch):
    gateway = _gateway(tmp_path)
    monkeypatch.setattr(gateway_mod, "_A2A_RETRY_INTERVAL_SECONDS", 0)
    task = gateway._a2a_authoritative_task
    task.state = "working"
    task.messages.append(types.SimpleNamespace(
        role="ROLE_CALLER",
        message_id="message-2",
        parts=[{"text": "Current caller request."}],
    ))
    key = "task-1:message-1"
    data = _event()["data"]
    gateway._write_a2a_registry(
        key,
        data,
        "queued",
        receipt_text=gateway_mod._a2a_receipt_text(
            "task-1",
            gateway.cfg.a2a_progress_interval_seconds,
        ),
    )

    async def scenario():
        gateway._schedule_a2a_acknowledgement_retry(key, data)
        await gateway._a2a_ack_tasks[key][1]

    asyncio.run(scenario())

    assert gateway.replies == []
    saved = json.loads(gateway._a2a_registry_path.read_text())[key]
    assert saved["state"] == "finalized"
    assert "pending_text" not in saved.get("receipt", {})


def test_a2a_catch_up_rejects_persisted_stale_generation_and_runs_current(tmp_path):
    gateway = _gateway(tmp_path)
    stale_key = "task-1:message-1"
    gateway._write_a2a_registry(stale_key, _event()["data"], "running")
    task = gateway._a2a_authoritative_task
    task.state = "working"
    task.messages.append(types.SimpleNamespace(
        role="ROLE_CALLER",
        message_id="message-2",
        parts=[{"text": "Current caller request."}],
    ))
    queried_states = []

    def iter_a2a_tasks(*, state):
        queried_states.append(state)
        return iter([task] if state == "working" else [])

    gateway._identity.iter_a2a_tasks = iter_a2a_tasks

    async def stay_active(prompt, *, a2a_context=None):
        gateway.sessions.session.calls.append((prompt, a2a_context))
        return "[SILENT]"

    gateway.sessions.session.run_consult = stay_active

    async def scenario():
        await gateway._catch_up_a2a_tasks()
        await asyncio.gather(*gateway._a2a_jobs["task-1"])
        current = _event()
        current["event_type"] = "a2a.task.message"
        current["data"] = current["data"] | {
            "message_id": "message-2",
            "parts": [{"text": "Spoofed duplicate."}],
        }
        return await gateway._on_a2a_event(current)

    duplicate = asyncio.run(scenario())

    assert json.loads(duplicate.text)["deduped"] is True
    assert len(gateway.sessions.session.calls) == 1
    prompt, _context = gateway.sessions.session.calls[0]
    assert prompt.endswith("Current caller request.")
    assert "Spoofed duplicate." not in prompt
    assert len(gateway.replies) == 1
    registry = json.loads(gateway._a2a_registry_path.read_text())
    assert registry[stale_key]["state"] == "finalized"
    assert registry["task-1:message-2"]["data"]["parts"] == [
        {"text": "Current caller request."}
    ]
    assert queried_states == ["submitted", "working"]


def test_a2a_cleanup_drains_acknowledgement_retry(tmp_path):
    gateway = _gateway(tmp_path)

    async def scenario():
        acknowledgement_task = asyncio.create_task(asyncio.sleep(30))
        gateway._a2a_ack_tasks["task-1:message-1"] = (
            "task-1",
            acknowledgement_task,
        )
        gateway._hosted_call_jobs = {}
        gateway.sessions = None
        gateway._runner = None
        gateway._tunnel = None

        await gateway._cleanup()

        assert acknowledgement_task.cancelled()
        assert gateway._a2a_ack_tasks == {}

    asyncio.run(scenario())


@pytest.mark.parametrize("stop_reason", ["cancel", "shutdown"])
def test_a2a_acknowledgement_send_is_drained_before_stop(
    tmp_path,
    monkeypatch,
    stop_reason,
):
    gateway = _gateway(tmp_path)
    monkeypatch.setattr(gateway_mod, "_A2A_RETRY_INTERVAL_SECONDS", 0)
    key = "task-1:message-1"
    data = _event()["data"]
    gateway._write_a2a_registry(key, data, "queued")
    original_reply = gateway._identity.a2a_reply
    started = threading.Event()
    release = threading.Event()
    completed = threading.Event()

    def paused_reply(task_id, **kwargs):
        started.set()
        assert release.wait(5)
        original_reply(task_id, **kwargs)
        completed.set()

    gateway._identity.a2a_reply = paused_reply

    async def scenario():
        gateway._schedule_a2a_acknowledgement_retry(key, data)
        assert await asyncio.to_thread(started.wait, 5)

        if stop_reason == "cancel":
            event = _event()
            event["event_type"] = "a2a.task.canceled"
            stopping = asyncio.create_task(gateway._on_a2a_event(event))
        else:
            gateway._hosted_call_jobs = {}
            gateway.sessions = None
            gateway._runner = None
            gateway._tunnel = None
            stopping = asyncio.create_task(gateway._cleanup())

        await asyncio.sleep(0)
        was_pending = not stopping.done()
        release.set()
        await stopping
        completed_before_stop_returned = completed.is_set()
        assert await asyncio.to_thread(completed.wait, 5)

        assert was_pending
        assert completed_before_stop_returned
        assert gateway._a2a_ack_tasks == {}

    asyncio.run(scenario())


@pytest.mark.parametrize("stop_reason", ["cancel", "shutdown"])
def test_implicit_completion_send_is_drained_before_stop(
    tmp_path,
    stop_reason,
):
    gateway = _gateway(tmp_path)
    original_reply = gateway._identity.a2a_reply
    started = threading.Event()
    release = threading.Event()
    completed = threading.Event()

    def paused_reply(task_id, **kwargs):
        if kwargs.get("intent") != "complete":
            return original_reply(task_id, **kwargs)
        started.set()
        assert release.wait(5)
        original_reply(task_id, **kwargs)
        completed.set()

    gateway._identity.a2a_reply = paused_reply

    async def scenario():
        await gateway._on_a2a_event(_event())
        assert await asyncio.to_thread(started.wait, 5)

        if stop_reason == "cancel":
            event = _event()
            event["event_type"] = "a2a.task.canceled"
            stopping = asyncio.create_task(gateway._on_a2a_event(event))
        else:
            gateway._hosted_call_jobs = {}
            gateway.sessions = None
            gateway._runner = None
            gateway._tunnel = None
            stopping = asyncio.create_task(gateway._cleanup())

        await asyncio.sleep(0)
        was_pending = not stopping.done()
        release.set()
        await stopping
        completed_before_stop_returned = completed.is_set()
        assert await asyncio.to_thread(completed.wait, 5)

        assert was_pending
        assert completed_before_stop_returned
        assert gateway._a2a_jobs == {}

    asyncio.run(scenario())


def test_a2a_webhook_admission_is_closed_before_cleanup_drain(tmp_path):
    gateway = _gateway(tmp_path)

    async def scenario():
        gateway._hosted_call_jobs = {}
        gateway.sessions = None
        gateway._runner = None
        gateway._tunnel = None
        await gateway._a2a_ingest_lock.acquire()
        webhook = asyncio.create_task(gateway._on_a2a_event(_event()))
        await asyncio.sleep(0)
        cleanup = asyncio.create_task(gateway._cleanup())
        await asyncio.sleep(0)
        assert gateway._closing is True
        gateway._a2a_ingest_lock.release()

        response = await webhook
        assert response.status == 503
        assert json.loads(response.text)["retryable"] is True
        assert not gateway._a2a_registry_path.exists()
        await cleanup
        assert gateway._a2a_jobs == {}

    asyncio.run(scenario())


def test_a2a_webhook_before_ingest_returns_retryable_when_closing(tmp_path):
    gateway = _gateway(tmp_path)
    gateway._closing = True

    response = asyncio.run(gateway._on_a2a_event(_event()))

    assert response.status == 503
    assert json.loads(response.text) == {
        "ok": False,
        "error": "gateway-closing",
        "retryable": True,
    }
    assert not gateway._a2a_registry_path.exists()
