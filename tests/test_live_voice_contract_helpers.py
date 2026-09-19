"""Deterministic checks for the hosted-call live-test evidence helpers."""

from __future__ import annotations

import importlib.util
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest


def _load_voice_module():
    path = Path(__file__).parent / "live" / "test_voice.py"
    spec = importlib.util.spec_from_file_location("claude_live_voice_helpers", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


voice = _load_voice_module()


def _load_hosted_script():
    path = Path(__file__).parent / "live" / "hosted_voice_script.py"
    spec = importlib.util.spec_from_file_location("hosted_voice_script", path)
    assert spec is not None and spec.loader is not None
    script = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(script)
    return script


def test_hosted_caller_requests_outcome_without_prescribing_tools():
    workflow = (Path(__file__).parent.parent / ".github/workflows/live-voice.yml").read_text()
    script = _load_hosted_script()
    marker = "victor echo juliet"
    stages = script.hosted_sms_stages(marker)

    assert len(stages) == 2
    assert voice._has_after_call_sms_intent(stages[1]["text"])
    assert voice._spoken_key(marker) in voice._spoken_key(stages[1]["text"])
    assert stages[1]["expected_reply"] == marker
    caller_text = " ".join(stage["text"] for stage in stages).lower()
    for internal in ("post-call action", "tool", "title", "details", "register", "save"):
        assert internal not in caller_text
    assert 'python3 tests/live/hosted_voice_script.py "$marker_file" "$driver_stages_file"' in workflow
    assert "VOICE_DRIVER_STAGES_FILE=$driver_stages_file" in workflow
    assert "VOICE_DRIVER_WAIT_FOR_PEER=1" in workflow
    assert "Upload logs on failure" not in workflow
    assert "Dump logs on failure" not in workflow
    assert "candidates={current_candidates" not in (
        Path(__file__).parent / "live" / "test_voice.py"
    ).read_text()


def test_stage_diagnostics_are_bounded_and_exclude_content():
    script = _load_hosted_script()
    log = "\n".join([
        "INFO driver identity private-handle number private-number",
        "INFO driver heard (final): private-message",
        "INFO driver spoke stage=1 chars=42 private-tail",
        *["INFO driver heard final chars=12 active_stage=True"] * 80,
        "INFO driver reask stage=1 total_reasks=2",
        "INFO driver peer interrupted tts=True",
    ])
    lines = script.driver_diagnostic_lines(log)
    assert len(lines) == 60
    assert lines[-2:] == ["reask stage=1 total_reasks=2", "peer interrupted tts=True"]
    assert "private" not in repr(lines)


def test_spoken_marker_normalizes_punctuation_and_case():
    assert voice._spoken_key("Victor-Echo, JULIET!") == " victor echo juliet "
    assert voice._spoken_key(None) == ""


@pytest.mark.parametrize("observed", [
    "victorechojuliet", "unvictor echo juliet", "victor echo julietextra",
    "victor juliet echo", "victor another echo juliet",
])
def test_spoken_marker_rejects_merged_partial_or_changed_words(observed):
    assert voice._spoken_key("victor echo juliet") not in voice._spoken_key(observed)


def test_spoken_marker_does_not_alias_different_words():
    assert voice._spoken_key("cloud papa") != voice._spoken_key("Claude Papa")


def test_hosted_call_request_does_not_supply_the_spoken_task_or_solution():
    hosted = voice._call_me_text(hosted=True)
    assert "over the phone" in hosted
    for text in (hosted, voice._call_me_text()):
        for internal in ("post-call action", "tool", "voicemail_detection", "SMS"):
            assert internal not in text


def test_after_call_sms_intent_requires_after_call_language():
    assert voice._has_after_call_sms_intent(
        "After we hang up, send an S.M.S. containing Victor Echo."
    )
    assert not voice._has_after_call_sms_intent(
        "Send an SMS containing Victor Echo during this call."
    )


def test_sms_targets_include_recipient_rows():
    message = SimpleNamespace(
        remote_phone_number=None,
        recipients=[SimpleNamespace(recipient_phone_number="+1 (516) 555-0101")],
    )
    assert voice._sms_target_numbers(message) == {"15165550101"}


def _hosted_sms_row(**changes):
    fields = {
        "id": "new", "text": "victor echo juliet",
        "remote_phone_number": "+15165550101", "recipients": [],
        "created_at": datetime(2026, 9, 19, 12, 0, 1, tzinfo=UTC),
    }
    fields.update(changes)
    return SimpleNamespace(**fields)


def _check_hosted_rows(rows, ended_at=datetime(2026, 9, 19, 12, tzinfo=UTC)):
    return voice._assert_hosted_sms_rows(
        rows, {"baseline"}, "victor echo juliet", "+15165550101", ended_at,
    )


@pytest.mark.parametrize("changes", [
    {"text": "Here is victor echo juliet"},
    {"text": "victorechojuliet"},
    {"text": "victor echo"},
    {"remote_phone_number": "+15165550102"},
    {"recipients": [SimpleNamespace(recipient_phone_number="+15165550102")]},
    {"created_at": None},
    {"created_at": datetime(2026, 9, 19, 11, 59, 59, tzinfo=UTC)},
])
def test_hosted_sms_rejects_wrong_content_target_or_timing(changes):
    with pytest.raises(AssertionError):
        _check_hosted_rows([_hosted_sms_row(**changes)])


def test_hosted_sms_rejects_non_marker_duplicate():
    with pytest.raises(AssertionError, match="duplicate"):
        _check_hosted_rows([
            _hosted_sms_row(), _hosted_sms_row(id="extra", text="All done"),
        ])


def test_hosted_sms_rejects_any_send_before_recorded_hangup():
    with pytest.raises(AssertionError, match="before a recorded call end"):
        _check_hosted_rows([_hosted_sms_row()], ended_at=None)


def test_hosted_sms_accepts_one_exact_post_call_row_but_not_old_baseline():
    message = _hosted_sms_row()
    assert _check_hosted_rows([
        _hosted_sms_row(id="baseline", text="old unrelated body"), message,
    ]) == [message]
    assert _check_hosted_rows([], ended_at=None) == []


def test_record_timestamp_accepts_datetime_and_iso_z():
    stamp = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    assert voice._message_created_at(SimpleNamespace(created_at=stamp)) == stamp
    assert voice._message_created_at(
        SimpleNamespace(created_at="2026-08-01T12:00:00Z")
    ) == stamp


def test_voicemail_detection_value_accepts_sdk_enum_or_wire_string():
    enum_like = SimpleNamespace(value="disabled")
    assert voice._voicemail_detection_value(
        SimpleNamespace(voicemail_detection=enum_like)
    ) == "disabled"
    assert voice._voicemail_detection_value(
        SimpleNamespace(voicemail_detection="disabled")
    ) == "disabled"


def test_call_pair_correlation_keeps_driver_and_aut_ownership(monkeypatch):
    stamp = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    driver = SimpleNamespace(
        id="driver-1", created_at=stamp, voicemail_detection="enabled"
    )
    aut = SimpleNamespace(
        id="aut-1", created_at=stamp, voicemail_detection="disabled"
    )

    monkeypatch.setattr(voice, "POLL_EVERY_S", 0)

    assert voice._wait_for_fresh_call_pair(
        lambda: [driver],
        lambda: [aut],
        set(),
        set(),
        not_before=stamp,
        deadline=time.monotonic() + 1,
        label="test",
    ) == (driver, aut)


def test_call_pair_duplicate_diagnostic_names_owner(monkeypatch):
    stamp = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    driver = SimpleNamespace(id="driver-1", created_at=stamp)
    aut = [
        SimpleNamespace(id="aut-1", created_at=stamp),
        SimpleNamespace(id="aut-2", created_at=stamp),
    ]

    monkeypatch.setattr(voice, "POLL_EVERY_S", 0)

    with pytest.raises(AssertionError, match="test created duplicate AUT records"):
        voice._wait_for_fresh_call_pair(
            lambda: [driver],
            lambda: aut,
            set(),
            set(),
            not_before=stamp,
            deadline=time.monotonic() + 1,
            label="test",
        )


def test_delayed_old_call_is_ignored_after_snapshot(monkeypatch):
    now = datetime(2026, 8, 7, 12, 0, tzinfo=UTC)
    old = SimpleNamespace(id="old", created_at=now - timedelta(minutes=2))
    driver = SimpleNamespace(id="driver", created_at=now)
    aut = SimpleNamespace(id="aut", created_at=now + timedelta(seconds=1))
    monkeypatch.setattr(voice, "POLL_EVERY_S", 0)

    assert voice._wait_for_fresh_call_pair(
        lambda: [old, driver],
        lambda: [aut],
        set(),
        set(),
        not_before=now - timedelta(seconds=10),
        deadline=time.monotonic() + 1,
        label="test",
    ) == (driver, aut)


def test_fresh_call_cleanup_ends_every_post_baseline_record(monkeypatch):
    calls = [
        SimpleNamespace(id="baseline"),
        SimpleNamespace(id="current-1"),
        SimpleNamespace(id="current-2"),
    ]
    ended = []
    monkeypatch.setattr(voice, "_hangup_call", lambda _client, call_id: ended.append(call_id))

    voice._hangup_fresh_calls(
        SimpleNamespace(), lambda: calls, {"baseline"}
    )

    assert ended == ["current-1", "current-2"]


def test_pre_sweep_ends_active_calls_and_preserves_terminal_history(monkeypatch):
    active = SimpleNamespace(id="active", status="answered")
    terminal = SimpleNamespace(id="terminal", status="completed")
    statuses = {"active": active, "terminal": terminal}
    ended = []

    def hangup(_client, call_id):
        ended.append(call_id)
        statuses[call_id].status = "completed"

    client = SimpleNamespace(
        calls=SimpleNamespace(get=lambda call_id: statuses[call_id])
    )
    monkeypatch.setattr(voice, "_hangup_call", hangup)

    voice._sweep_matching_calls(client, lambda: [active, terminal])

    assert ended == ["active"]


def test_aut_speech_mode_reads_the_exact_call_id():
    seen = []
    aut = SimpleNamespace(calls=SimpleNamespace(get=lambda call_id: (
        seen.append(call_id)
        or SimpleNamespace(use_inkbox_tts=False, use_inkbox_stt=False)
    )))

    assert voice._aut_speech_mode(aut, "aut-current") == (False, False)
    assert seen == ["aut-current"]


def test_matching_post_call_action_requires_open_current_marker_sms():
    marker = "victor echo juliet"
    matching = {
        "status": "open",
        "action": "send_sms",
        "details": "After the call, send Victor-Echo, Juliet to the caller.",
    }
    assert voice._matching_post_call_action(
        SimpleNamespace(post_call_action_items=[matching]), marker
    ) is matching

    for item in (
        {**matching, "status": "canceled"},
        {**matching, "details": "Send a different marker."},
        {**matching, "details": "Send victorechojuliet."},
        {**matching, "details": "Send unvictor echo juliet."},
        {**matching, "action": "create_note", "details": marker},
    ):
        assert voice._matching_post_call_action(
            SimpleNamespace(post_call_action_items=[item]), marker
        ) is None


def test_action_gate_diagnostic_is_bounded_and_content_redacted():
    secret = "customer-secret-" * 10_000
    call = SimpleNamespace(
        post_call_action_items=[
            {
                "status": "open",
                "action": "send_sms",
                "details": f"Send Victor Echo Juliet {secret}",
            },
            *[
                {"status": "closed", "action": secret, "details": secret}
                for _ in range(15)
            ],
        ]
    )

    diagnostic = voice._post_call_action_diagnostic(
        call,
        "Victor Echo Juliet",
    )

    assert diagnostic == {
        "item_count": 16,
        "inspected_count": 10,
        "open_count": 1,
        "marker_count": 1,
        "sms_count": 1,
        "max_marker_words": 3,
        "matching_action": True,
    }
    assert "customer-secret" not in repr(diagnostic)
