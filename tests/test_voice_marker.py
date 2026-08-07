import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MARKER_PATH = ROOT / "tests" / "live" / "voice_marker.py"
SPEC = importlib.util.spec_from_file_location("claude_voice_marker", MARKER_PATH)
assert SPEC is not None and SPEC.loader is not None
MARKER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MARKER)


def _marker(token: str) -> list[str]:
    return MARKER.marker_from_token(token).split()


def test_live_voice_marker_is_deterministic_distinct_and_speech_safe():
    for run_number in range(100):
        token = f"run-{run_number}-attempt-1"
        words = _marker(token)

        assert words == _marker(token)
        assert len(words) == 3
        assert len(set(words)) == 3
        assert set(words) <= set(MARKER.SPEECH_WORDS)


def test_live_voice_marker_uses_the_full_run_token_entropy():
    markers = {
        tuple(_marker(f"run-{run_number:04d}-attempt-1"))
        for run_number in range(1_000)
    }

    assert len(markers) >= 850


def test_hosted_workflow_uses_three_word_full_run_marker():
    workflow = (ROOT / ".github" / "workflows" / "live-voice.yml").read_text()
    driver = (ROOT / "tests" / "live" / "voice_driver.py").read_text()
    live_test = (ROOT / "tests" / "live" / "test_voice.py").read_text()

    assert 'RUN_TOKEN="${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"' in workflow
    assert 'python3 tests/live/voice_marker.py "$RUN_TOKEN"' in workflow
    assert "exact three-word body" in workflow
    assert "HOSTED_POST_CALL_MARKER=$marker" not in workflow
    assert "VOICE_DRIVER_LINE=" not in workflow
    assert "HOSTED_POST_CALL_MARKER_FILE=$marker_file" in workflow
    assert "VOICE_DRIVER_LINE_FILE=$driver_line_file" in workflow
    assert 'os.environ.get("VOICE_DRIVER_LINE_FILE", "")' in driver
    assert 'os.environ.get("HOSTED_POST_CALL_MARKER_FILE", "")' in live_test
