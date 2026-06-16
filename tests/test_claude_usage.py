from datetime import datetime, timezone

from inkbox_claude import claude_usage


def test_format_usage_matches_claude_code_shape():
    now = datetime(2026, 6, 16, 12, 0, tzinfo=timezone.utc)
    # utilization is a 0-100 percentage, as the live endpoint returns it.
    data = {
        "five_hour": {"utilization": 42, "resets_at": "2026-06-16T14:30:00Z"},
        "seven_day": {"utilization": 10, "resets_at": "2026-06-19T12:00:00Z"},
        "seven_day_opus": {"utilization": 0, "resets_at": "2026-06-19T12:00:00Z"},
    }
    out = claude_usage.format_usage(data, now=now)
    assert "5-hour session: 42% used, resets in 2h 30m" in out
    assert "This week (all models): 10% used, resets in 3d 0h" in out
    assert "This week (Opus): 0% used" in out


def test_format_usage_skips_missing_windows():
    out = claude_usage.format_usage({"five_hour": {"utilization": 50, "resets_at": None}})
    assert out == "5-hour session: 50% used"  # no reset suffix when unknown


def test_format_usage_empty_payload():
    assert claude_usage.format_usage({}) == "No usage windows reported."


def test_usage_report_handles_no_subscription(monkeypatch):
    # No token → friendly message, no crash.
    monkeypatch.setattr(claude_usage, "_read_oauth_token", lambda: None)
    msg = claude_usage.usage_report()
    assert "no subscription login" in msg.lower()
