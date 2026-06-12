from inkbox_claude.config import read_config


def test_read_config_defaults(monkeypatch):
    for var in (
        "INKBOX_API_KEY", "INKBOX_IDENTITY", "INKBOX_ALLOW_ALL_USERS",
        "INKBOX_ALLOWED_USERS", "INKBOX_AUTO_ALLOWED_TOOLS",
    ):
        monkeypatch.delenv(var, raising=False)
    cfg = read_config()
    assert cfg.base_url == "https://inkbox.ai"
    assert cfg.require_signature is True
    assert "Read" in cfg.auto_allowed_tools
    assert "Bash" not in cfg.auto_allowed_tools


def test_read_config_env(monkeypatch):
    monkeypatch.setenv("INKBOX_API_KEY", "ApiKey_test")
    monkeypatch.setenv("INKBOX_IDENTITY", "code-agent")
    monkeypatch.setenv("INKBOX_ALLOWED_USERS", "+15551234567, me@example.com")
    monkeypatch.setenv("INKBOX_AUTO_ALLOWED_TOOLS", "Read,Grep")
    cfg = read_config()
    assert cfg.api_key == "ApiKey_test"
    assert cfg.allowed_users == ["+15551234567", "me@example.com"]
    assert cfg.auto_allowed_tools == ["Read", "Grep"]
