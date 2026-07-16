"""Offline coverage for failure handling in the real-call live-test helpers."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest


def _load_live_voice_module():
    path = Path(__file__).parent / "live" / "test_voice.py"
    spec = importlib.util.spec_from_file_location("live_voice_helpers", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Calls:
    def __init__(self, *, call, transcripts):
        self.call = call
        self._transcripts = transcripts

    def get(self, _call_id):
        return self.call

    def transcripts(self, _call_id):
        if isinstance(self._transcripts, Exception):
            raise self._transcripts
        return self._transcripts


def test_wait_for_two_way_call_fails_immediately_on_canceled_leg():
    voice = _load_live_voice_module()
    remote = SimpleNamespace(calls=_Calls(
        call=SimpleNamespace(
            status="canceled", reason=None, hangup_reason="remote",
            started_at=None, ended_at="now", is_blocked=False,
        ),
        transcripts=[],
    ))

    with pytest.raises(pytest.fail.Exception, match="call ended before a two-way conversation") as exc:
        voice._wait_for_two_way_call(remote, "unused-number-id", "call-id")

    message = str(exc.value)
    assert "status='canceled'" in message
    assert "hangup_reason='remote'" in message
    assert "is_blocked=False" in message


def test_wait_for_two_way_call_returns_remote_speech_when_both_parties_spoke():
    voice = _load_live_voice_module()
    segments = [
        SimpleNamespace(party="remote", text="hello"),
        SimpleNamespace(party="local", text="hi back"),
    ]
    remote = SimpleNamespace(calls=_Calls(
        call=SimpleNamespace(status="answered"),
        transcripts=segments,
    ))

    assert voice._wait_for_two_way_call(remote, "unused-number-id", "call-id") == "hello"


def test_wait_for_two_way_call_checks_terminal_state_while_transcripts_are_unavailable():
    voice = _load_live_voice_module()
    remote = SimpleNamespace(calls=_Calls(
        call=SimpleNamespace(
            status="failed", reason="upstream", hangup_reason=None,
            started_at=None, ended_at="now", is_blocked=False,
        ),
        transcripts=RuntimeError("404 Call not found"),
    ))

    with pytest.raises(pytest.fail.Exception, match="call ended before a two-way conversation") as exc:
        voice._wait_for_two_way_call(remote, "unused-number-id", "call-id")

    assert "transcripts not ready" in str(exc.value)
    assert "status='failed'" in str(exc.value)
