"""Deterministic checks for the hosted-call live-test evidence helpers."""

from __future__ import annotations

import importlib.util
from datetime import UTC, datetime
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


def test_voice_driver_allows_realtime_response_latency():
    driver = (Path(__file__).parent / "live/voice_driver.py").read_text()
    assert 'VOICE_DRIVER_LISTEN", "30"' in driver


def test_workflow_requires_action_first_exact_body_and_readback():
    workflow = (Path(__file__).parent.parent / ".github/workflows/live-voice.yml").read_text()

    assert (
        "Create one post-call action now with both its title and details exactly: Send SMS "
        "$marker. Then list the actions. If either field lacks that exact phrase, edit that "
        "same action until both match. Only then read the five-word body back. After we hang "
        "up, send me one SMS containing exactly $marker. Do not send it during the call."
        in workflow
    )


def test_spoken_marker_normalizes_punctuation_and_case():
    assert voice._spoken_key("Victor-Echo, JULIET!") == "victorechojuliet"
    assert voice._spoken_key("cloudpapa") == voice._spoken_key("Claude Papa")


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


def test_call_pair_correlation_keeps_driver_and_aut_ownership():
    stamp = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    driver = SimpleNamespace(
        id="driver-1", created_at=stamp, voicemail_detection="enabled"
    )
    aut = SimpleNamespace(
        id="aut-1", created_at=stamp, voicemail_detection="disabled"
    )

    assert voice._correlate_fresh_call_pair(
        [driver],
        [aut],
        before_driver=set(),
        before_aut=set(),
        driver_watermark=stamp,
        aut_watermark=stamp,
    ) == (driver, aut)


def test_call_pair_duplicate_diagnostic_names_owner_and_ids():
    stamp = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    driver = SimpleNamespace(id="driver-1", created_at=stamp)
    aut = [
        SimpleNamespace(id="aut-1", created_at=stamp),
        SimpleNamespace(id="aut-2", created_at=stamp),
    ]

    with pytest.raises(
        AssertionError,
        match=r"phase=call_pairing .*aut-1.*aut-2.*duplicate AUT legs",
    ):
        voice._correlate_fresh_call_pair(
            [driver],
            aut,
            before_driver=set(),
            before_aut=set(),
            driver_watermark=stamp,
            aut_watermark=stamp,
        )


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
        "matching_action": True,
    }
    assert "customer-secret" not in repr(diagnostic)
