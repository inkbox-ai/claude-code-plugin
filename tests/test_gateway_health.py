from inkbox_claude import gateway


def test_claude_health_reports_api_key(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    assert "API key" in gateway._claude_health()


def test_claude_health_reports_subscription(tmp_path, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    home = tmp_path
    (home / ".claude").mkdir()
    (home / ".claude" / ".credentials.json").write_text("{}")
    monkeypatch.setattr(gateway.Path, "home", classmethod(lambda cls: home))
    assert "subscription" in gateway._claude_health()


def test_claude_health_reports_missing_auth(tmp_path, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setattr(gateway.Path, "home", classmethod(lambda cls: tmp_path))
    assert "NOT authenticated" in gateway._claude_health()
