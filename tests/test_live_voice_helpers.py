"""Offline coverage for failure handling in the real-call live-test helpers."""

from __future__ import annotations

import importlib.util
import time
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
    assert " reason=" not in message


def test_wait_for_two_way_call_returns_aut_local_speech_when_both_parties_spoke():
    voice = _load_live_voice_module()
    segments = [
        SimpleNamespace(party="remote", text="hello"),
        SimpleNamespace(party="local", text="hi back"),
    ]
    remote = SimpleNamespace(calls=_Calls(
        call=SimpleNamespace(status="answered"),
        transcripts=segments,
    ))

    assert voice._wait_for_two_way_call(remote, "unused-number-id", "call-id") == "hi back"


def test_driver_leg_requires_only_driver_local_speech():
    voice = _load_live_voice_module()
    remote = SimpleNamespace(calls=_Calls(
        call=SimpleNamespace(status="answered"),
        transcripts=[SimpleNamespace(party="local", text="scripted caller line")],
    ))

    assert voice._wait_for_driver_local_speech(
        remote,
        "unused-number-id",
        "call-id",
        deadline=time.monotonic() + 1,
    ) == "scripted caller line"


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


@pytest.mark.parametrize("missing_readback", [None, "driver", "aut"])
def test_wait_for_persisted_hosted_request_requires_both_transcripts_and_action(monkeypatch, missing_readback):
    voice = _load_live_voice_module()
    marker = "victor echo juliet"
    remote = SimpleNamespace(calls=_Calls(
        call=SimpleNamespace(
            status="answered", reason="test", hangup_reason=None,
            started_at="before", ended_at=None, is_blocked=False,
        ),
        transcripts=[SimpleNamespace(
            party="local",
            text=f"After this call ends, send one SMS containing {marker}.",
        )],
    ))
    aut = SimpleNamespace(calls=_Calls(
        call=SimpleNamespace(post_call_action_items=[{
            "status": "open",
            "action": "send_sms",
            "details": f"Send {marker} to the caller.",
        }]),
        transcripts=[SimpleNamespace(
            party="remote",
            text=f"After this call ends, send one SMS containing {marker}.",
        )],
    ))

    if missing_readback != "driver":
        remote.calls._transcripts.append(SimpleNamespace(party="remote", text=marker))
    if missing_readback != "aut":
        aut.calls._transcripts.append(SimpleNamespace(party="local", text=marker))
    monkeypatch.setattr(voice, "POLL_EVERY_S", 0)

    def wait():
        return voice._wait_for_persisted_hosted_request(
            remote, "unused-number-id", "driver-call-id", aut, "aut-call-id", marker,
            deadline=time.monotonic() + 0.01,
        )

    if missing_readback:
        with pytest.raises(pytest.fail.Exception, match=f"{missing_readback}_readback_ready=False"):
            wait()
    else:
        assert wait() is None


def test_wait_for_persisted_hosted_request_requires_aut_transcript(monkeypatch):
    voice = _load_live_voice_module()
    marker = "victor echo juliet"
    transcript = [SimpleNamespace(
        party="local",
        text=f"After this call ends, send one SMS containing {marker}.",
    )]
    remote = SimpleNamespace(calls=_Calls(call=SimpleNamespace(), transcripts=transcript))
    aut = SimpleNamespace(calls=_Calls(
        call=SimpleNamespace(post_call_action_items=[{
            "status": "open",
            "action": "send_sms",
            "details": f"Send {marker} to the caller.",
        }]),
        transcripts=[],
    ))
    monkeypatch.setattr(voice, "POLL_EVERY_S", 0)

    with pytest.raises(pytest.fail.Exception, match="aut_transcript_ready=False"):
        voice._wait_for_persisted_hosted_request(
            remote,
            "unused-number-id",
            "driver-call-id",
            aut,
            "aut-call-id",
            marker,
            deadline=time.monotonic() + 0.01,
        )


def test_action_diagnostic_counts_missing_words_without_accepting_partial_marker():
    voice = _load_live_voice_module()
    call = SimpleNamespace(post_call_action_items=[{
        "status": "open", "action": "Send SMS", "details": "private recipient: banana umbrella",
    }])
    marker = "banana umbrella calendar"
    diagnostic = voice._post_call_action_diagnostic(call, marker)
    assert diagnostic == {
        "item_count": 1, "inspected_count": 1, "open_count": 1,
        "marker_count": 0, "sms_count": 1, "max_marker_words": 2,
        "matching_action": False,
    }
    assert voice._matching_post_call_action(call, marker) is None
    assert "private recipient" not in str(diagnostic)
    assert "banana" not in str(diagnostic)
