from inkbox_claude.config import VoiceStack, read_config, resolve_voice_stack


def test_read_config_defaults(monkeypatch):
    for var in (
        "INKBOX_API_KEY", "INKBOX_IDENTITY", "INKBOX_ALLOW_ALL_USERS",
        "INKBOX_ALLOWED_USERS", "INKBOX_AUTO_ALLOWED_TOOLS", "INKBOX_BASE_URL",
        "INKBOX_CONTACT_MEMORIES_ENABLED",
    ):
        monkeypatch.delenv(var, raising=False)
    cfg = read_config()
    assert cfg.base_url == ""
    assert cfg.require_signature is True
    assert cfg.contact_memories_enabled is True
    assert "Read" in cfg.auto_allowed_tools
    assert "Bash" not in cfg.auto_allowed_tools


def test_read_config_env(monkeypatch):
    monkeypatch.setenv("INKBOX_API_KEY", "ApiKey_test")
    monkeypatch.setenv("INKBOX_IDENTITY", "code-agent")
    monkeypatch.setenv("INKBOX_BASE_URL", "https://proxy.example")
    monkeypatch.setenv("INKBOX_ALLOWED_USERS", "+15551234567, me@example.com")
    monkeypatch.setenv("INKBOX_AUTO_ALLOWED_TOOLS", "Read,Grep")
    cfg = read_config()
    assert cfg.api_key == "ApiKey_test"
    assert cfg.base_url == "https://proxy.example"
    assert cfg.allowed_users == ["+15551234567", "me@example.com"]
    assert cfg.auto_allowed_tools == ["Read", "Grep"]


def test_contact_memories_can_be_disabled(monkeypatch):
    monkeypatch.setenv("INKBOX_CONTACT_MEMORIES_ENABLED", "false")
    assert read_config().contact_memories_enabled is False


def _clear_realtime_env(monkeypatch):
    for var in (
        "INKBOX_REALTIME_ENABLED", "INKBOX_REALTIME_API_KEY", "OPENAI_API_KEY",
        "INKBOX_REALTIME_MODEL", "INKBOX_REALTIME_VOICE",
        "INKBOX_REALTIME_FALLBACK_TO_INKBOX_STT_TTS",
    ):
        monkeypatch.delenv(var, raising=False)


def test_realtime_disabled_by_default(monkeypatch):
    _clear_realtime_env(monkeypatch)
    assert read_config().realtime.enabled is False


def test_realtime_needs_both_flag_and_key(monkeypatch):
    # Flag on but no key → still disabled (gateway would have nothing to dial).
    _clear_realtime_env(monkeypatch)
    monkeypatch.setenv("INKBOX_REALTIME_ENABLED", "true")
    assert read_config().realtime.enabled is False

    # Flag on + key → enabled.
    monkeypatch.setenv("INKBOX_REALTIME_API_KEY", "sk-rt")
    cfg = read_config()
    assert cfg.realtime.enabled is True
    assert cfg.realtime.api_key == "sk-rt"


def test_realtime_key_falls_back_to_openai_env(monkeypatch):
    _clear_realtime_env(monkeypatch)
    monkeypatch.setenv("INKBOX_REALTIME_ENABLED", "1")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-openai")
    cfg = read_config()
    assert cfg.realtime.enabled is True
    assert cfg.realtime.api_key == "sk-openai"


def test_canonical_voice_stack_wins_over_legacy_realtime(monkeypatch):
    _clear_realtime_env(monkeypatch)
    monkeypatch.setenv("INKBOX_REALTIME_ENABLED", "true")
    monkeypatch.setenv("INKBOX_REALTIME_API_KEY", "sk-old")
    monkeypatch.setenv("INKBOX_VOICE_STACK", "inkbox_voice_ai")
    cfg = read_config()
    assert cfg.voice_stack is VoiceStack.INKBOX_VOICE_AI


def test_legacy_realtime_install_resolves_to_realtime(monkeypatch):
    _clear_realtime_env(monkeypatch)
    monkeypatch.delenv("INKBOX_VOICE_STACK", raising=False)
    monkeypatch.setenv("INKBOX_REALTIME_ENABLED", "true")
    monkeypatch.setenv("INKBOX_REALTIME_API_KEY", "sk-old")
    assert read_config().voice_stack is VoiceStack.OPENAI_REALTIME


def test_invalid_voice_stack_fails_closed_to_local_and_is_reported(monkeypatch):
    monkeypatch.setenv("INKBOX_VOICE_STACK", "magic")
    cfg = read_config()
    assert cfg.voice_stack is VoiceStack.INKBOX_TTS_STT
    assert cfg.voice_stack_invalid_value == "magic"


def test_voice_authority_and_voicemail_config(monkeypatch):
    monkeypatch.setenv("INKBOX_VOICE_AI_AUTHORITY_MODE", "yolo")
    monkeypatch.setenv("INKBOX_VOICEMAIL_DETECTION", "disabled")
    cfg = read_config()
    assert cfg.voice_ai_authority_mode == "yolo"
    assert cfg.voicemail_detection == "disabled"


def test_resolve_voice_stack_defaults_to_tts_stt():
    assert resolve_voice_stack(None) == (VoiceStack.INKBOX_TTS_STT, "")
