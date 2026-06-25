import types

import pytest

from inkbox_claude import setup_wizard


# ----------------------------------------------------------------------
# .env persistence
# ----------------------------------------------------------------------


def test_show_qr_renders_block_chars():
    # segno is a declared dependency, so a QR should render to the terminal.
    import io
    import contextlib

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        ok = setup_wizard._show_qr("sms:+15550009999&body=connect @agent")
    out = buf.getvalue()
    assert ok is True
    assert "█" in out or "▀" in out  # QR modules rendered as block glyphs


def test_save_and_env_roundtrip(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    monkeypatch.setenv("INKBOX_CLAUDE_ENV_FILE", str(env_file))
    monkeypatch.delenv("INKBOX_IDENTITY", raising=False)

    setup_wizard._save("INKBOX_IDENTITY", "dev-agent")

    # Persisted to disk and mirrored into the live env for an immediate doctor.
    assert "INKBOX_IDENTITY=dev-agent" in env_file.read_text()
    assert setup_wizard._env("INKBOX_IDENTITY") == "dev-agent"


def test_save_upserts_existing_key(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("export INKBOX_IDENTITY=old\nINKBOX_BRIDGE_PORT=8767\n")
    monkeypatch.setenv("INKBOX_CLAUDE_ENV_FILE", str(env_file))
    monkeypatch.delenv("INKBOX_IDENTITY", raising=False)

    setup_wizard._save("INKBOX_IDENTITY", "new")

    text = env_file.read_text()
    assert "INKBOX_IDENTITY=new" in text
    assert "old" not in text
    # An unrelated line is left intact.
    assert "INKBOX_BRIDGE_PORT=8767" in text


def test_save_skips_empty_value(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    monkeypatch.setenv("INKBOX_CLAUDE_ENV_FILE", str(env_file))

    setup_wizard._save("INKBOX_SIGNING_KEY", "")

    assert not env_file.exists()


def test_env_reads_quoted_value_from_file(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text('INKBOX_API_KEY="ApiKey_abc"\n')
    monkeypatch.setenv("INKBOX_CLAUDE_ENV_FILE", str(env_file))
    monkeypatch.delenv("INKBOX_API_KEY", raising=False)

    assert setup_wizard._env("INKBOX_API_KEY") == "ApiKey_abc"


# ----------------------------------------------------------------------
# SDK install bootstrap
# ----------------------------------------------------------------------


def test_install_command_prefers_uv_when_available(monkeypatch):
    monkeypatch.setattr(setup_wizard.sys, "executable", "/tmp/venv/bin/python")
    monkeypatch.setattr(setup_wizard.shutil, "which", lambda name: "/bin/uv" if name == "uv" else None)

    assert setup_wizard._install_commands()[0] == [[
        "/bin/uv",
        "pip",
        "install",
        "--python",
        "/tmp/venv/bin/python",
        "inkbox>=0.4.9",
        "aiohttp>=3.9",
    ]]


def test_install_command_falls_back_to_pip_and_ensurepip(monkeypatch):
    monkeypatch.setattr(setup_wizard.sys, "executable", "/tmp/venv/bin/python")
    monkeypatch.setattr(setup_wizard.shutil, "which", lambda _name: None)

    assert setup_wizard._install_commands() == [
        [["/tmp/venv/bin/python", "-m", "pip", "install", "inkbox>=0.4.9", "aiohttp>=3.9"]],
        [
            ["/tmp/venv/bin/python", "-m", "ensurepip", "--upgrade"],
            ["/tmp/venv/bin/python", "-m", "pip", "install", "inkbox>=0.4.9", "aiohttp>=3.9"],
        ],
    ]


def test_missing_sdk_guidance_prints_interpreter(monkeypatch, capsys):
    def fail_import():
        raise ImportError("No module named 'inkbox'")

    monkeypatch.setattr(setup_wizard, "_load_inkbox_symbols", fail_import)
    monkeypatch.setattr(setup_wizard, "_is_interactive_stdin", lambda: False)
    monkeypatch.setattr(setup_wizard.sys, "executable", "/tmp/venv/bin/python")
    monkeypatch.setattr(setup_wizard.shutil, "which", lambda name: "/bin/uv" if name == "uv" else None)

    assert setup_wizard._ensure_inkbox_sdk() is None

    out = capsys.readouterr().out
    assert "/tmp/venv/bin/python" in out
    assert "uv pip install --python" in out
    assert "inkbox>=0.4.9" in out


# ----------------------------------------------------------------------
# Project directory
# ----------------------------------------------------------------------


def test_configure_project_dir_persists_choice(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    monkeypatch.setenv("INKBOX_CLAUDE_ENV_FILE", str(env_file))
    monkeypatch.delenv("CLAUDE_PROJECT_DIR", raising=False)
    monkeypatch.setattr(setup_wizard, "prompt", lambda *_a, **_k: str(tmp_path))

    setup_wizard._configure_project_dir()

    assert setup_wizard._env("CLAUDE_PROJECT_DIR") == str(tmp_path)


# ----------------------------------------------------------------------
# Signing key
# ----------------------------------------------------------------------


def test_setup_signing_key_mints_new(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    monkeypatch.setenv("INKBOX_CLAUDE_ENV_FILE", str(env_file))
    # First yes/no = "have a key?" -> no; second = "generate now?" -> yes.
    answers = iter([False, True])
    monkeypatch.setattr(setup_wizard, "prompt_yes_no", lambda *_a, **_k: next(answers))

    class FakeClient:
        def __init__(self, **_kwargs):
            pass

        def create_signing_key(self):
            return types.SimpleNamespace(signing_key="whsec_minted", created_at=None)

    setup_wizard._setup_signing_key("ApiKey_x", "https://inkbox.ai", FakeClient)

    text = env_file.read_text()
    assert "INKBOX_SIGNING_KEY=whsec_minted" in text
    assert "INKBOX_REQUIRE_SIGNATURE=true" in text


def test_setup_signing_key_decline_aborts(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    monkeypatch.setenv("INKBOX_CLAUDE_ENV_FILE", str(env_file))
    # "have a key?" -> no; "generate now?" -> no. A signing key is required, so
    # declining must abort setup rather than disable signature verification.
    answers = iter([False, False])
    monkeypatch.setattr(setup_wizard, "prompt_yes_no", lambda *_a, **_k: next(answers))

    with pytest.raises(SystemExit):
        setup_wizard._setup_signing_key("ApiKey_x", "https://inkbox.ai", lambda **_k: None)


# ----------------------------------------------------------------------
# iMessage walkthrough (mirrors the hermes-agent-plugin fakes)
# ----------------------------------------------------------------------


class _FakeIMessageIdentity:
    def __init__(self, enabled=False):
        self.imessage_enabled = enabled
        self.updates = []
        self.sent = []
        self.marked_read = []
        self._inbox = []

    def update(self, **kwargs):
        self.updates.append(kwargs)
        if "imessage_enabled" in kwargs:
            self.imessage_enabled = kwargs["imessage_enabled"]
        return self

    def list_imessages(self, **_kwargs):
        return list(self._inbox)

    def send_imessage(self, **kwargs):
        self.sent.append(kwargs)
        return types.SimpleNamespace(id="im-1")

    def mark_imessage_conversation_read(self, conversation_id):
        self.marked_read.append(conversation_id)


class _FakeIMessageClient:
    def __init__(self, identity):
        self._identity = identity
        self.imessages = types.SimpleNamespace(
            get_triage_number=lambda: types.SimpleNamespace(
                number="+15550009999",
                connect_command="connect @agent",
            ),
        )

    def get_identity(self, _handle):
        return self._identity


def test_configure_imessage_enables_and_offers_connect(monkeypatch):
    identity = _FakeIMessageIdentity(enabled=False)
    client = _FakeIMessageClient(identity)
    walked = []

    monkeypatch.setattr(setup_wizard, "prompt_yes_no", lambda *_a, **_k: True)
    monkeypatch.setattr(
        setup_wizard,
        "_wait_for_imessage_first_message",
        lambda _client, _identity, handle: walked.append(handle),
    )

    setup_wizard._configure_imessage(
        "ApiKey_test", "https://inkbox.ai", "agent", lambda **_kwargs: client,
    )

    assert identity.updates == [{"imessage_enabled": True}]
    assert walked == ["agent"]


def test_configure_imessage_declined_leaves_identity_untouched(monkeypatch):
    identity = _FakeIMessageIdentity(enabled=False)
    client = _FakeIMessageClient(identity)

    monkeypatch.setattr(setup_wizard, "prompt_yes_no", lambda *_a, **_k: False)
    monkeypatch.setattr(
        setup_wizard,
        "_wait_for_imessage_first_message",
        lambda *_a: (_ for _ in ()).throw(AssertionError("should not walk through connect")),
    )

    setup_wizard._configure_imessage(
        "ApiKey_test", "https://inkbox.ai", "agent", lambda **_kwargs: client,
    )

    assert identity.updates == []


def test_wait_for_imessage_first_message_greets_back(monkeypatch):
    from datetime import datetime, timedelta, timezone

    identity = _FakeIMessageIdentity(enabled=True)
    client = _FakeIMessageClient(identity)
    identity._inbox = [
        types.SimpleNamespace(
            id="im-old",
            direction="inbound",
            conversation_id="imconv-old",
            remote_number="+15555550101",
            created_at=datetime.now(timezone.utc) - timedelta(days=1),
        ),
        types.SimpleNamespace(
            id="im-new",
            direction="inbound",
            conversation_id="imconv-123",
            remote_number="+15555550101",
            created_at=datetime.now(timezone.utc) + timedelta(seconds=5),
        ),
    ]

    monkeypatch.setattr(setup_wizard.time, "sleep", lambda _s: None)

    setup_wizard._wait_for_imessage_first_message(client, identity, "agent")

    assert len(identity.sent) == 1
    assert identity.sent[0]["conversation_id"] == "imconv-123"
    assert "@agent" in identity.sent[0]["text"]
    assert identity.marked_read == ["imconv-123"]


def test_sms_opt_in_qr_uses_smsto_scheme(monkeypatch):
    """The summary's SMS opt-in QR encodes SMSTO:<number>:START — scanning it
    drafts the START text that unlocks outbound SMS in one tap."""
    identity = types.SimpleNamespace(
        agent_handle="agent",
        email_address="agent@inkbox.ai",
        mailbox=None,
        phone_number=types.SimpleNamespace(
            number="+16614031457",
            type="local",
            sms_status=None,
        ),
    )

    captured = {}
    # capture the payload handed to the QR renderer; return True so the
    # plain-text fallback line is skipped
    monkeypatch.setattr(setup_wizard, "_show_qr",
                        lambda data: captured.update(payload=data) or True)

    setup_wizard._print_agent_summary(identity)

    assert captured["payload"] == "SMSTO:+16614031457:START"


def test_connect_qr_uses_smsto_scheme(monkeypatch):
    """The scan-to-connect QR encodes SMSTO:<number>:<command> (servers PR #234) —
    scanners draft that far more reliably than a raw sms: link."""
    from datetime import datetime, timedelta, timezone

    identity = _FakeIMessageIdentity(enabled=True)
    client = _FakeIMessageClient(identity)
    identity._inbox = [
        types.SimpleNamespace(
            id="im-1",
            direction="inbound",
            conversation_id="imconv-1",
            remote_number="+15555550101",
            created_at=datetime.now(timezone.utc) + timedelta(seconds=5),
        ),
    ]

    captured = {}
    # capture the payload handed to the QR renderer; return True so the
    # plain-text fallback line is skipped
    monkeypatch.setattr(setup_wizard, "_show_qr",
                        lambda data: captured.update(payload=data) or True)
    monkeypatch.setattr(setup_wizard.time, "sleep", lambda _s: None)

    setup_wizard._wait_for_imessage_first_message(client, identity, "agent")

    assert captured["payload"] == "SMSTO:+15550009999:connect @agent"


# ----------------------------------------------------------------------
# OpenAI Realtime configuration
# ----------------------------------------------------------------------


def test_detect_realtime_key_prefers_plugin_var(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    monkeypatch.setenv("INKBOX_CLAUDE_ENV_FILE", str(env_file))
    monkeypatch.setenv("INKBOX_REALTIME_API_KEY", "sk-plugin")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-generic")
    assert setup_wizard._detect_openai_realtime_key() == ("INKBOX_REALTIME_API_KEY", "sk-plugin")


def test_detect_realtime_key_falls_back_to_openai(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    monkeypatch.setenv("INKBOX_CLAUDE_ENV_FILE", str(env_file))
    monkeypatch.delenv("INKBOX_REALTIME_API_KEY", raising=False)
    monkeypatch.setenv("OPENAI_API_KEY", "sk-generic")
    assert setup_wizard._detect_openai_realtime_key() == ("OPENAI_API_KEY", "sk-generic")


def test_detect_realtime_key_none_when_unset(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    monkeypatch.setenv("INKBOX_CLAUDE_ENV_FILE", str(env_file))
    monkeypatch.delenv("INKBOX_REALTIME_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    assert setup_wizard._detect_openai_realtime_key() is None


def test_configure_realtime_declined_writes_disabled(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    monkeypatch.setenv("INKBOX_CLAUDE_ENV_FILE", str(env_file))
    monkeypatch.delenv("INKBOX_REALTIME_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setattr(setup_wizard, "prompt_yes_no", lambda *a, **k: False)

    identity = types.SimpleNamespace(phone_number=types.SimpleNamespace(number="+16614031457"))
    setup_wizard._configure_realtime_calls(identity)
    assert setup_wizard._env("INKBOX_REALTIME_ENABLED") == "false"


def test_configure_realtime_enables_on_valid_key(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    monkeypatch.setenv("INKBOX_CLAUDE_ENV_FILE", str(env_file))
    monkeypatch.setenv("INKBOX_REALTIME_API_KEY", "sk-rt")
    monkeypatch.setattr(setup_wizard, "prompt_yes_no", lambda *a, **k: True)
    # Validation passes without hitting the network.
    monkeypatch.setattr(setup_wizard, "_test_openai_realtime_api_key", lambda *a, **k: (True, "ok"))

    identity = types.SimpleNamespace(phone_number=types.SimpleNamespace(number="+16614031457"))
    setup_wizard._configure_realtime_calls(identity)
    assert setup_wizard._env("INKBOX_REALTIME_ENABLED") == "true"
    assert setup_wizard._env("INKBOX_REALTIME_API_KEY") == "sk-rt"


def test_configure_realtime_skips_without_phone(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    monkeypatch.setenv("INKBOX_CLAUDE_ENV_FILE", str(env_file))
    setup_wizard._configure_realtime_calls(types.SimpleNamespace(phone_number=None))
    # No phone → returns before writing anything to this run's .env file.
    assert not env_file.exists()


# ----------------------------------------------------------------------
# Agent avatar
# ----------------------------------------------------------------------


def test_avatar_auto_attached_on_signup(monkeypatch):
    # Self-signup agents get the avatar with no prompt.
    uploaded = {}
    monkeypatch.setattr(setup_wizard, "_upload_avatar",
                        lambda b, k, h, img: uploaded.update(handle=h, n=len(img)) or (True, "ok"))
    # Must not prompt or probe for an existing avatar on the signup path.
    monkeypatch.setattr(setup_wizard, "prompt_yes_no",
                        lambda *a, **k: (_ for _ in ()).throw(AssertionError("no prompt on signup")))
    monkeypatch.setattr(setup_wizard, "_identity_has_avatar",
                        lambda *a, **k: (_ for _ in ()).throw(AssertionError("no probe on signup")))

    identity = types.SimpleNamespace(agent_handle="dev-agent")
    setup_wizard._configure_avatar("https://inkbox.ai", "ApiKey_x", identity, is_signup=True)
    assert uploaded["handle"] == "dev-agent" and uploaded["n"] > 0


def test_avatar_skipped_when_existing_agent_already_has_one(monkeypatch):
    monkeypatch.setattr(setup_wizard, "_identity_has_avatar", lambda *a, **k: True)
    monkeypatch.setattr(setup_wizard, "_upload_avatar",
                        lambda *a, **k: (_ for _ in ()).throw(AssertionError("must not upload")))
    monkeypatch.setattr(setup_wizard, "prompt_yes_no",
                        lambda *a, **k: (_ for _ in ()).throw(AssertionError("must not prompt")))
    identity = types.SimpleNamespace(agent_handle="dev-agent")
    setup_wizard._configure_avatar("https://inkbox.ai", "ApiKey_x", identity, is_signup=False)


def test_avatar_offered_and_uploaded_for_existing_agent_without_one(monkeypatch):
    uploaded = {}
    monkeypatch.setattr(setup_wizard, "_identity_has_avatar", lambda *a, **k: False)
    monkeypatch.setattr(setup_wizard, "prompt_yes_no", lambda *a, **k: True)
    monkeypatch.setattr(setup_wizard, "_upload_avatar",
                        lambda b, k, h, img: uploaded.update(handle=h) or (True, "ok"))
    identity = types.SimpleNamespace(agent_handle="dev-agent")
    setup_wizard._configure_avatar("https://inkbox.ai", "ApiKey_x", identity, is_signup=False)
    assert uploaded["handle"] == "dev-agent"


def test_avatar_declined_for_existing_agent(monkeypatch):
    monkeypatch.setattr(setup_wizard, "_identity_has_avatar", lambda *a, **k: False)
    monkeypatch.setattr(setup_wizard, "prompt_yes_no", lambda *a, **k: False)
    monkeypatch.setattr(setup_wizard, "_upload_avatar",
                        lambda *a, **k: (_ for _ in ()).throw(AssertionError("declined → no upload")))
    identity = types.SimpleNamespace(agent_handle="dev-agent")
    setup_wizard._configure_avatar("https://inkbox.ai", "ApiKey_x", identity, is_signup=False)
