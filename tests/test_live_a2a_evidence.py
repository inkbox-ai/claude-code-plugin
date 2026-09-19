"""A caller's request cannot stand in for an agent's final answer."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest


path = Path(__file__).parent / "live" / "a2a_driver.py"
spec = importlib.util.spec_from_file_location("live_a2a_evidence", path)
assert spec is not None and spec.loader is not None
driver = importlib.util.module_from_spec(spec)
spec.loader.exec_module(driver)


def _message(role, text):
    return {"role": role, "parts": [{"text": text}]}


@pytest.mark.parametrize("role", ["agent", "ROLE_AGENT"])
def test_final_answer_excludes_caller_and_earlier_agent_messages(role):
    task = SimpleNamespace(raw={"history": [
        _message("user", "requested-token"),
        _message(role, "earlier-token"),
        _message(role, "wrong final answer"),
    ]})
    assert driver._wire_final_agent_text(task) == "wrong final answer"
    assert driver._wire_history_messages(task) == ["earlier-token", "wrong final answer"]


def test_caller_only_history_cannot_complete_answer_proof():
    task = SimpleNamespace(raw={"history": [_message("user", "requested-token")]})
    with pytest.raises(AssertionError, match="no agent answer"):
        driver._wire_final_agent_text(task)
