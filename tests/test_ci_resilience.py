from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_dependency_installs_use_the_bounded_setup_retry():
    workflows = (
        "canary.yml",
        "tests.yml",
        "live-a2a.yml",
        "live-channels.yml",
        "live-external-events.yml",
        "live-voice.yml",
    )

    for name in workflows:
        text = (ROOT / ".github" / "workflows" / name).read_text()
        assert "tests/ci/retry_install.sh" in text
        for line in text.splitlines():
            stripped = line.strip()
            if "pip install" in stripped or "npm install" in stripped:
                assert "tests/ci/retry_install.sh" in stripped


def test_setup_retry_is_bounded_and_backed_off():
    helper = (ROOT / "tests" / "ci" / "retry_install.sh").read_text()

    assert "for attempt in 1 2 3 4" in helper
    assert 'sleep "$((attempt * 5))"' in helper
    assert "dependency installation failed after 4 attempts" in helper


def test_live_workflows_do_not_publish_raw_gateway_artifacts():
    workflows = (
        "live-a2a.yml",
        "live-channels.yml",
        "live-external-events.yml",
        "live-voice.yml",
    )

    for name in workflows:
        text = (ROOT / ".github" / "workflows" / name).read_text()
        assert "actions/upload-artifact" not in text
        assert not any(
            "tail" in line and ".log" in line
            for line in text.splitlines()
        )


def test_live_failure_diagnostics_do_not_render_message_or_call_content():
    paths = (
        "conftest.py",
        "test_cross_channel.py",
        "test_email_intelligence.py",
        "test_email_reply.py",
        "test_external_event_github.py",
        "test_external_event_intelligence.py",
        "test_sms.py",
        "test_voice.py",
    )
    forbidden = (
        "body[:",
        "question!r",
        "token!r",
        "driver_calls!r",
        "aut_calls!r",
        "exc!r",
        "get_exc!r",
        "current_candidates!r",
    )

    for name in paths:
        text = (ROOT / "tests" / "live" / name).read_text()
        assert not any(value in text for value in forbidden)
