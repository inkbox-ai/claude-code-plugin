import types

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
        "inkbox>=0.4.7",
        "aiohttp>=3.9",
    ]]


def test_install_command_falls_back_to_pip_and_ensurepip(monkeypatch):
    monkeypatch.setattr(setup_wizard.sys, "executable", "/tmp/venv/bin/python")
    monkeypatch.setattr(setup_wizard.shutil, "which", lambda _name: None)

    assert setup_wizard._install_commands() == [
        [["/tmp/venv/bin/python", "-m", "pip", "install", "inkbox>=0.4.7", "aiohttp>=3.9"]],
        [
            ["/tmp/venv/bin/python", "-m", "ensurepip", "--upgrade"],
            ["/tmp/venv/bin/python", "-m", "pip", "install", "inkbox>=0.4.7", "aiohttp>=3.9"],
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
    assert "inkbox>=0.4.7" in out


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


def test_setup_signing_key_decline_disables_signature(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    monkeypatch.setenv("INKBOX_CLAUDE_ENV_FILE", str(env_file))
    # "have a key?" -> no; "generate now?" -> no.
    answers = iter([False, False])
    monkeypatch.setattr(setup_wizard, "prompt_yes_no", lambda *_a, **_k: next(answers))

    setup_wizard._setup_signing_key("ApiKey_x", "https://inkbox.ai", lambda **_k: None)

    assert "INKBOX_REQUIRE_SIGNATURE=false" in env_file.read_text()


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
