"""Deterministic proof for cross-channel call-leg correlation."""

from __future__ import annotations

import importlib.util
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest


def _load_cross_channel_module():
    path = Path(__file__).parent / "live" / "test_cross_channel.py"
    spec = importlib.util.spec_from_file_location("claude_live_cross_channel", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


cross = _load_cross_channel_module()


class _Calls:
    def __init__(self, rows):
        self.rows = rows

    def list(self, **_kwargs):
        return list(self.rows)


def _record(record_id, direction, remote, created_at, voicemail="enabled"):
    return SimpleNamespace(
        id=record_id,
        direction=direction,
        remote_phone_number=remote,
        created_at=created_at,
        voicemail_detection=voicemail,
    )


def test_wait_for_call_pair_returns_aut_outbound_not_driver_inbound(monkeypatch):
    stamp = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    driver = _record("driver-1", "inbound", "+12025550111", stamp, "enabled")
    wrong_aut = _record("aut-wrong", "outbound", "+12025550199", stamp)
    aut = _record("aut-1", "outbound", "+12025550110", stamp, "disabled")
    sleeps = []
    monkeypatch.setattr(cross.time, "sleep", sleeps.append)

    selected = cross._wait_for_new_call_pair(
        SimpleNamespace(calls=_Calls([driver])),
        SimpleNamespace(calls=_Calls([wrong_aut, aut])),
        remote_pid="remote-number-id",
        remote_phone="+12025550110",
        aut_phone="+12025550111",
        before_driver=set(),
        before_aut=set(),
        driver_watermark=stamp - timedelta(seconds=1),
        aut_watermark=stamp - timedelta(seconds=1),
    )

    assert selected is aut
    assert cross._voicemail_detection_value(selected) == "disabled"
    assert sleeps == [cross.CALL_DUPLICATE_GRACE_S]


def test_wait_for_call_pair_rejects_duplicate_aut_legs(monkeypatch):
    stamp = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    driver = _record("driver-1", "inbound", "+12025550111", stamp)
    aut_calls = [
        _record("aut-1", "outbound", "+12025550110", stamp),
        _record("aut-2", "outbound", "+12025550110", stamp),
    ]
    monkeypatch.setattr(cross.time, "sleep", lambda _seconds: None)

    with pytest.raises(AssertionError, match="duplicate AUT call legs"):
        cross._wait_for_new_call_pair(
            SimpleNamespace(calls=_Calls([driver])),
            SimpleNamespace(calls=_Calls(aut_calls)),
            remote_pid="remote-number-id",
            remote_phone="+12025550110",
            aut_phone="+12025550111",
            before_driver=set(),
            before_aut=set(),
            driver_watermark=stamp - timedelta(seconds=1),
            aut_watermark=stamp - timedelta(seconds=1),
        )
