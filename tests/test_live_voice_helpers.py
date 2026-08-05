"""Offline coverage for failure handling in the real-call live-test helpers."""

from __future__ import annotations

import time
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest


from tests import live_voice_proof as proof


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


def test_agent_two_way_proof_fails_immediately_on_canceled_leg():
    remote = SimpleNamespace(calls=_Calls(
        call=SimpleNamespace(
            status="canceled", reason=None, hangup_reason="remote",
            started_at=None, ended_at="now", is_blocked=False,
        ),
        transcripts=[],
    ))

    with pytest.raises(AssertionError, match="agent leg ended before transcript proof"):
        proof.wait_for_agent_two_way_conversation(
            remote,
            "call-id",
            deadline=time.monotonic() + 1,
            poll_every=0,
        )

def test_agent_two_way_proof_returns_agent_local_speech():
    segments = [
        SimpleNamespace(party="remote", text="hello"),
        SimpleNamespace(party="local", text="hi back"),
    ]
    remote = SimpleNamespace(calls=_Calls(
        call=SimpleNamespace(status="answered"),
        transcripts=segments,
    ))

    assert proof.wait_for_agent_two_way_conversation(
        remote,
        "call-id",
        deadline=time.monotonic() + 1,
        poll_every=0,
    ) == "hi back"


def test_driver_proof_requires_only_driver_local_speech():
    remote = SimpleNamespace(calls=_Calls(
        call=SimpleNamespace(status="answered"),
        transcripts=[SimpleNamespace(party="local", text="scripted driver line")],
    ))

    assert proof.wait_for_driver_local_speech(
        remote,
        "call-id",
        deadline=time.monotonic() + 1,
        poll_every=0,
    ) == "scripted driver line"


def test_agent_two_way_proof_checks_terminal_state_when_transcript_unavailable():
    remote = SimpleNamespace(calls=_Calls(
        call=SimpleNamespace(
            status="failed", reason="upstream", hangup_reason=None,
            started_at=None, ended_at="now", is_blocked=False,
        ),
        transcripts=RuntimeError("404 Call not found"),
    ))

    with pytest.raises(AssertionError, match="agent leg ended before transcript proof"):
        proof.wait_for_agent_two_way_conversation(
            remote,
            "call-id",
            deadline=time.monotonic() + 1,
            poll_every=0,
        )


def test_agent_leg_uses_pair_filter_under_agent_identity():
    pair_id = "33333333-3333-3333-3333-333333333333"
    driver_call = SimpleNamespace(id="driver", paired_call_id=pair_id)
    agent_call = SimpleNamespace(id="agent", direction="inbound")
    driver_calls = SimpleNamespace(get=lambda _call_id: driver_call)

    class AgentCalls:
        def __init__(self):
            self.kwargs = None

        def list(self, **kwargs):
            self.kwargs = kwargs
            return [agent_call]

    agent_calls = AgentCalls()

    result = proof.wait_for_agent_leg(
        SimpleNamespace(calls=driver_calls),
        SimpleNamespace(calls=agent_calls),
        "driver",
        direction="inbound",
        driver_number="+15555550123",
        before_agent_ids=set(),
        started_at=datetime.now(UTC),
        deadline=time.monotonic() + 1,
        poll_every=0,
    )

    assert result is agent_call
    assert agent_calls.kwargs == {"limit": 2, "paired_call_id": pair_id}


def test_wait_for_persisted_hosted_request_requires_transcript_and_action():
    voice = __import__("tests.live.test_voice", fromlist=["test_voice"])
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
        transcripts=[],
    ))

    assert voice._wait_for_persisted_hosted_request(
        remote,
        "unused-number-id",
        "driver-call-id",
        aut,
        "aut-call-id",
        marker,
        deadline=time.monotonic() + 1,
    ) is None
