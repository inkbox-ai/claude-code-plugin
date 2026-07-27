import asyncio
import json
import types

import pytest

from inkbox_claude import gateway as gateway_mod
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
    gateway._identity = types.SimpleNamespace(
        id="identity-1",
        a2a_task=lambda _task_id: types.SimpleNamespace(state="submitted"),
        a2a_reply=lambda task_id, **kwargs: gateway.replies.append(
            (task_id, kwargs)
        ),
    )
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
        ("task-1", {"intent": "complete", "text": "Completed."})
    ]


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
