"""Outbound-call line resolution: explicit choice, capability fallback, and
channel-aware defaulting when the identity has BOTH a dedicated number and
iMessage enabled.

Guards the case where an agent on an iMessage conversation asked to "call me"
and the call would otherwise go out over the dedicated number instead of the
shared iMessage line.
"""

import types

import pytest

from inkbox_claude import tools


def _identity(has_number: bool, imessage: bool):
    return types.SimpleNamespace(
        phone_number=types.SimpleNamespace(number="+15550000000") if has_number else None,
        imessage_enabled=imessage,
    )


@pytest.fixture(autouse=True)
def _clear_session():
    # Each test starts outside any bound session (as a bare tool call would).
    token = tools.CURRENT_SESSION.set(None)
    yield
    tools.CURRENT_SESSION.reset(token)


def _set_channel(mode):
    # The gateway session tracks the last inbound modality on ``mode``; bind a
    # stand-in session the way ``ContactSession._ensure_client`` does.
    tools.CURRENT_SESSION.set(types.SimpleNamespace(mode=mode) if mode else None)


def test_single_line_resolves_unambiguously():
    _set_channel(None)
    assert tools._resolve_call_origination(_identity(True, False), "") == "dedicated_number"
    assert tools._resolve_call_origination(_identity(False, True), "") == "shared_imessage_number"
    assert tools._resolve_call_origination(_identity(False, False), "") is None


def test_explicit_choice_wins_over_channel():
    _set_channel("imessage")
    assert tools._resolve_call_origination(_identity(True, True), "dedicated_number") == "dedicated_number"
    _set_channel("sms")
    assert tools._resolve_call_origination(_identity(True, True), "shared_imessage_number") == "shared_imessage_number"


def test_both_lines_follow_conversation_channel():
    both = _identity(True, True)
    _set_channel("imessage")
    assert tools._resolve_call_origination(both, "") == "shared_imessage_number"
    _set_channel("sms")
    assert tools._resolve_call_origination(both, "") == "dedicated_number"
    _set_channel("voice")
    assert tools._resolve_call_origination(both, "") == "dedicated_number"


def test_both_lines_unknown_channel_defaults_dedicated():
    _set_channel(None)
    assert tools._resolve_call_origination(_identity(True, True), "") == "dedicated_number"
    # Email is neither phone line — same dedicated default.
    _set_channel("email")
    assert tools._resolve_call_origination(_identity(True, True), "") == "dedicated_number"


def test_channel_only_breaks_ties():
    # An iMessage-only identity stays shared even on an SMS-looking turn.
    _set_channel("sms")
    assert tools._resolve_call_origination(_identity(False, True), "") == "shared_imessage_number"
