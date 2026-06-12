from inkbox_claude.escalation import (
    format_permission_request,
    format_poll,
    parse_permission_reply,
    parse_poll_reply,
    summarize_tool_call,
)


def test_summarize_bash():
    line = summarize_tool_call("Bash", {"command": "npm test", "description": "Run tests"})
    assert "npm test" in line
    assert "Run tests" in line


def test_summarize_edit_truncates():
    line = summarize_tool_call("Edit", {"file_path": "/x/" + "a" * 400})
    assert len(line) < 220


def test_permission_request_mentions_options():
    text = format_permission_request("Bash", {"command": "rm -rf build"})
    assert "rm -rf build" in text
    assert "ALWAYS" in text


def test_parse_permission_replies():
    assert parse_permission_reply("yes") == "allow"
    assert parse_permission_reply("1") == "allow"
    assert parse_permission_reply("OK!") == "allow"
    assert parse_permission_reply("Always") == "always"
    assert parse_permission_reply("2") == "always"
    assert parse_permission_reply("no") == "deny"
    assert parse_permission_reply("3") == "deny"
    assert parse_permission_reply("hmm what does it do") is None


QUESTIONS = [
    {
        "question": "Which framework?",
        "options": [{"label": "React", "description": "SPA"}, {"label": "Vue", "description": ""}],
        "multiSelect": False,
    },
    {
        "question": "Which features?",
        "options": [{"label": "Auth", "description": ""}, {"label": "Billing", "description": ""}],
        "multiSelect": True,
    },
]


def test_format_poll_numbers_options():
    text = format_poll(QUESTIONS)
    assert "Q1: Which framework?" in text
    assert "1. React" in text
    assert "2. Vue" in text


def test_parse_poll_numeric():
    answers = parse_poll_reply("2, 1 2", QUESTIONS)
    assert answers["Which framework?"] == "Vue"
    assert answers["Which features?"] == ["Auth", "Billing"]


def test_parse_poll_label_match():
    answers = parse_poll_reply("react", QUESTIONS[:1])
    assert answers["Which framework?"] == "React"


def test_parse_poll_free_text_passthrough():
    answers = parse_poll_reply("use svelte instead", QUESTIONS[:1])
    assert answers["Which framework?"] == "use svelte instead"
