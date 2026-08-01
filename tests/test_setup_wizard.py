import os
import sys
import types

import pytest

from inkbox_claude import setup_wizard


# ----------------------------------------------------------------------
# .env persistence
# ----------------------------------------------------------------------


def test_avatar_base_url_defaults_to_public_api():
    assert setup_wizard._avatar_base_url("") == "https://inkbox.ai"
    assert setup_wizard._avatar_base_url("https://proxy.example/") == "https://proxy.example"


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
        "inkbox>=0.5.9,<1.0.0",
        "aiohttp>=3.9",
    ]]


def test_install_command_falls_back_to_pip_and_ensurepip(monkeypatch):
    monkeypatch.setattr(setup_wizard.sys, "executable", "/tmp/venv/bin/python")
    monkeypatch.setattr(setup_wizard.shutil, "which", lambda _name: None)

    assert setup_wizard._install_commands() == [
        [["/tmp/venv/bin/python", "-m", "pip", "install", "inkbox>=0.5.9,<1.0.0", "aiohttp>=3.9"]],
        [
            ["/tmp/venv/bin/python", "-m", "ensurepip", "--upgrade"],
            ["/tmp/venv/bin/python", "-m", "pip", "install", "inkbox>=0.5.9,<1.0.0", "aiohttp>=3.9"],
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
    assert "inkbox>=0.5.9,<1.0.0" in out


# ----------------------------------------------------------------------
# API key scope handling
# ----------------------------------------------------------------------


def test_api_key_flow_rejects_unknown_auth_subtype(monkeypatch, capsys):
    class FakeWhoamiApiKeyResponse:
        auth_subtype = "future_scope"
        organization_id = "org_123"

    class FakeInkbox:
        def __init__(self, **_kwargs):
            pass

        def whoami(self):
            return FakeWhoamiApiKeyResponse()

        def list_identities(self):
            raise AssertionError("unknown subtypes must not fall back to identity listing")

    monkeypatch.setattr(setup_wizard, "prompt", lambda *_args, **_kwargs: "ApiKey_test")

    result = setup_wizard._api_key_flow(
        "https://inkbox.ai",
        FakeInkbox,
        Exception,
        FakeWhoamiApiKeyResponse,
        "admin_scoped",
        "agent_scoped_claimed",
        "agent_scoped_unclaimed",
        object,
    )

    assert result == (None, "", False, None)
    assert "Unsupported API-key subtype" in capsys.readouterr().out


def test_admin_api_key_flow_selects_existing_identity_and_mints_agent_key(monkeypatch):
    class FakeWhoamiApiKeyResponse:
        auth_subtype = "admin_scoped"
        organization_id = "org_123"

    class FakeApiKeys:
        def __init__(self):
            self.created = []

        def create(self, **kwargs):
            self.created.append(kwargs)
            return types.SimpleNamespace(api_key="ApiKey_agent_selected")

    class FakeInkbox:
        instance = None

        def __init__(self, **_kwargs):
            self.api_keys = FakeApiKeys()
            self.phone_numbers = types.SimpleNamespace()
            self.identities = [
                types.SimpleNamespace(agent_handle="first-agent", email_address=None),
                types.SimpleNamespace(agent_handle="selected-agent", email_address=None),
            ]
            self.details = {
                "first-agent": types.SimpleNamespace(
                    id="identity-1",
                    agent_handle="first-agent",
                    email_address="first@example.com",
                    phone_number=types.SimpleNamespace(number="+15550000001", type="local"),
                ),
                "selected-agent": types.SimpleNamespace(
                    id="identity-2",
                    agent_handle="selected-agent",
                    email_address="selected@example.com",
                    phone_number=types.SimpleNamespace(number="+15550000002", type="local"),
                ),
            }
            FakeInkbox.instance = self

        def whoami(self):
            return FakeWhoamiApiKeyResponse()

        def list_identities(self):
            return self.identities

        def get_identity(self, handle):
            return self.details[handle]

    monkeypatch.setattr(setup_wizard, "prompt", lambda *_args, **_kwargs: "ApiKey_admin")
    monkeypatch.setattr(setup_wizard, "prompt_choice", lambda *_args, **_kwargs: 1)

    identity, agent_key, did_provision_phone, authority_identity = setup_wizard._api_key_flow(
        "https://inkbox.ai",
        FakeInkbox,
        Exception,
        FakeWhoamiApiKeyResponse,
        "admin_scoped",
        "agent_scoped_claimed",
        "agent_scoped_unclaimed",
        object,
    )

    assert identity.agent_handle == "selected-agent"
    assert agent_key == "ApiKey_agent_selected"
    assert did_provision_phone is False
    assert authority_identity is identity
    assert FakeInkbox.instance.api_keys.created == [
        {
            "label": "Claude Code bridge - selected-agent",
            "description": (
                "Auto-minted by inkbox-claude setup. Scoped to one agent "
                "identity so the bridge never stores the admin key."
            ),
            "scoped_identity_id": "identity-2",
        }
    ]


def test_admin_api_key_flow_can_create_identity_and_mint_agent_key(monkeypatch):
    class FakeWhoamiApiKeyResponse:
        auth_subtype = "admin_scoped"
        organization_id = "org_123"

    class FakeApiKeys:
        def __init__(self):
            self.created = []

        def create(self, **kwargs):
            self.created.append(kwargs)
            return types.SimpleNamespace(api_key="ApiKey_agent_new")

    class FakeInkbox:
        instance = None

        def __init__(self, **_kwargs):
            self.api_keys = FakeApiKeys()
            self.phone_numbers = types.SimpleNamespace()
            self.created_identities = []
            FakeInkbox.instance = self

        def whoami(self):
            return FakeWhoamiApiKeyResponse()

        def list_identities(self):
            return []

        def create_identity(self, handle, **kwargs):
            self.created_identities.append((handle, kwargs))
            return types.SimpleNamespace(
                id="identity-new",
                agent_handle=handle,
                email_address=f"{handle}@example.com",
                phone_number=None,
            )

    answers = iter(["ApiKey_admin", "new-agent", "New Agent"])
    monkeypatch.setattr(setup_wizard, "prompt", lambda *_args, **_kwargs: next(answers))
    monkeypatch.setattr(setup_wizard, "prompt_yes_no", lambda *_args, **_kwargs: False)

    identity, agent_key, did_provision_phone, authority_identity = setup_wizard._api_key_flow(
        "https://inkbox.ai",
        FakeInkbox,
        Exception,
        FakeWhoamiApiKeyResponse,
        "admin_scoped",
        "agent_scoped_claimed",
        "agent_scoped_unclaimed",
        object,
    )

    assert identity.agent_handle == "new-agent"
    assert agent_key == "ApiKey_agent_new"
    assert did_provision_phone is False
    assert authority_identity is identity
    assert FakeInkbox.instance.created_identities == [
        ("new-agent", {"display_name": "New Agent", "phone_number": None})
    ]
    assert FakeInkbox.instance.api_keys.created == [
        {
            "label": "Claude Code bridge - new-agent",
            "description": (
                "Auto-minted by inkbox-claude setup. Scoped to one agent "
                "identity so the bridge never stores the admin key."
            ),
            "scoped_identity_id": "identity-new",
        }
    ]


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
# iMessage walkthrough
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

    enabled = setup_wizard._configure_imessage(
        "ApiKey_test", "https://inkbox.ai", "agent", lambda **_kwargs: client,
    )

    assert enabled is True
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

    enabled = setup_wizard._configure_imessage(
        "ApiKey_test", "https://inkbox.ai", "agent", lambda **_kwargs: client,
    )

    assert enabled is False
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
    # No phone and no iMessage → returns before writing anything to .env.
    assert not env_file.exists()


def test_configure_realtime_offered_for_imessage_only_identity(tmp_path, monkeypatch):
    # Calls can arrive over the shared iMessage line, so realtime is offered
    # even without a dedicated number when the threaded flag says enabled
    # (the local identity object may be stale, hence the explicit bool).
    env_file = tmp_path / ".env"
    monkeypatch.setenv("INKBOX_CLAUDE_ENV_FILE", str(env_file))
    monkeypatch.setenv("INKBOX_REALTIME_API_KEY", "sk-rt")
    monkeypatch.setattr(setup_wizard, "prompt_yes_no", lambda *a, **k: True)
    monkeypatch.setattr(setup_wizard, "_test_openai_realtime_api_key", lambda *a, **k: (True, "ok"))

    setup_wizard._configure_realtime_calls(
        types.SimpleNamespace(phone_number=None), imessage_enabled=True
    )


class _VoiceIdentity:
    def __init__(self, authority="contact_scoped"):
        self.agent_handle = "agent"
        self.phone_number = types.SimpleNamespace(
            client_websocket_url="wss://agent.example/phone/media/ws"
        )
        self.authority = authority
        self.hosted_updates = []
        self.incoming_updates = []
        self.authority_updates = []
        self.incoming_before = types.SimpleNamespace(
            incoming_call_action="auto_accept",
            client_websocket_url="wss://old.example/phone/media/ws",
            incoming_call_webhook_url="https://old.example/webhook",
        )

    def get_hosted_agent_config(self):
        return types.SimpleNamespace(authority_mode=self.authority)

    def set_hosted_agent_config(self, **kwargs):
        self.hosted_updates.append(kwargs)

    def get_incoming_call_action(self):
        return self.incoming_before

    def set_incoming_call_action(self, **kwargs):
        self.incoming_updates.append(kwargs)

    def set_hosted_agent_authority_mode(self, authority):
        self.authority_updates.append(authority)


def _voice_setup_kwargs():
    return {
        "base_url": "",
        "Inkbox": object,
        "InkboxAPIError": Exception,
        "WhoamiApiKeyResponse": object,
        "ADMIN_SCOPED": "admin_scoped",
    }


def test_phone_voice_stack_configures_tts_stt(tmp_path, monkeypatch):
    monkeypatch.setenv("INKBOX_CLAUDE_ENV_FILE", str(tmp_path / ".env"))
    questions = []
    monkeypatch.setattr(
        setup_wizard,
        "prompt",
        lambda question, *a, **k: questions.append(question) or "",
    )
    monkeypatch.setattr(setup_wizard, "prompt_choice", lambda *a, **k: 2)
    identity = _VoiceIdentity()

    setup_wizard._configure_phone_call_voice_stack(identity, **_voice_setup_kwargs())

    assert setup_wizard._env("INKBOX_VOICE_STACK") == "inkbox_tts_stt"
    assert questions == ["  Press Enter to continue and set up phone call handling"]
    assert setup_wizard._env("INKBOX_REALTIME_ENABLED") == "false"
    assert identity.incoming_updates[-1]["incoming_call_action"] == "auto_accept"


def test_local_voice_stack_rebuilds_canonical_tunnel_url_after_hosted_mode(
    tmp_path, monkeypatch,
):
    monkeypatch.setenv("INKBOX_CLAUDE_ENV_FILE", str(tmp_path / ".env"))
    monkeypatch.setattr(setup_wizard, "prompt", lambda *a, **k: "")
    monkeypatch.setattr(setup_wizard, "prompt_choice", lambda *a, **k: 2)
    identity = _VoiceIdentity()
    identity.phone_number.client_websocket_url = None

    setup_wizard._configure_phone_call_voice_stack(
        identity,
        **_voice_setup_kwargs(),
    )

    assert identity.incoming_updates[-1] == {
        "incoming_call_action": "auto_accept",
        "client_websocket_url": "wss://agent.inkboxwire.com/phone/media/ws",
        "incoming_call_webhook_url": None,
    }


@pytest.mark.parametrize(
    ("saved_stack", "expected_index"),
    [
        ("inkbox_voice_ai", 0),
        ("openai_realtime", 1),
        ("inkbox_tts_stt", 2),
    ],
)
def test_voice_stack_rerun_defaults_to_saved_selection(
    saved_stack, expected_index, monkeypatch,
):
    monkeypatch.setattr(
        setup_wizard,
        "_env",
        lambda name: saved_stack if name == "INKBOX_VOICE_STACK" else "",
    )
    assert setup_wizard._voice_stack_default_index("") == expected_index


def test_prompt_choice_reprompts_invalid_input_and_honors_default(monkeypatch):
    answers = iter(["not-a-number", "9", ""])
    monkeypatch.setattr("builtins.input", lambda _prompt: next(answers))
    assert setup_wizard.prompt_choice("choose", ["one", "two", "three"], 1) == 1


def test_prompt_choice_cancellation_exits(monkeypatch):
    monkeypatch.setattr(
        "builtins.input",
        lambda _prompt: (_ for _ in ()).throw(KeyboardInterrupt()),
    )
    with pytest.raises(SystemExit):
        setup_wizard.prompt_choice("choose", ["one", "two"], 0)


def test_phone_voice_stack_configures_valid_realtime(tmp_path, monkeypatch):
    monkeypatch.setenv("INKBOX_CLAUDE_ENV_FILE", str(tmp_path / ".env"))
    monkeypatch.setenv("INKBOX_REALTIME_API_KEY", "sk-valid")
    monkeypatch.setattr(setup_wizard, "prompt", lambda *a, **k: "")
    monkeypatch.setattr(setup_wizard, "prompt_choice", lambda *a, **k: 1)
    monkeypatch.setattr(
        setup_wizard,
        "_test_openai_realtime_api_key",
        lambda *a, **k: (True, "ok"),
    )
    identity = _VoiceIdentity()

    setup_wizard._configure_phone_call_voice_stack(
        identity,
        **_voice_setup_kwargs(),
    )

    assert setup_wizard._env("INKBOX_VOICE_STACK") == "openai_realtime"
    assert setup_wizard._env("INKBOX_REALTIME_API_KEY") == "sk-valid"
    assert identity.incoming_updates[-1]["incoming_call_action"] == "auto_accept"


def test_voice_ai_contact_scope_does_not_prompt_for_admin(tmp_path, monkeypatch):
    monkeypatch.setenv("INKBOX_CLAUDE_ENV_FILE", str(tmp_path / ".env"))
    choices = iter([0, 0])
    monkeypatch.setattr(setup_wizard, "prompt_choice", lambda *a, **k: next(choices))
    monkeypatch.setattr(
        setup_wizard,
        "prompt",
        lambda question, *a, **k: (
            ""
            if "Press Enter" in question
            else (_ for _ in ()).throw(AssertionError("admin prompted"))
        ),
    )
    identity = _VoiceIdentity(authority="contact_scoped")

    setup_wizard._configure_phone_call_voice_stack(
        identity,
        **_voice_setup_kwargs(),
    )

    assert identity.authority_updates == []
    assert setup_wizard._env("INKBOX_VOICE_AI_AUTHORITY_MODE") == "contact_scoped"
    assert setup_wizard._env("INKBOX_VOICE_STACK") == "inkbox_voice_ai"


def test_realtime_validation_failure_loops_back_to_all_choices(tmp_path, monkeypatch):
    monkeypatch.setenv("INKBOX_CLAUDE_ENV_FILE", str(tmp_path / ".env"))
    choices = iter([1, 2])
    monkeypatch.setattr(setup_wizard, "prompt_choice", lambda *a, **k: next(choices))
    monkeypatch.setattr(setup_wizard, "prompt", lambda *a, **k: "sk-invalid")
    monkeypatch.setattr(
        setup_wizard,
        "_test_openai_realtime_api_key",
        lambda *a, **k: (False, "invalid key"),
    )
    identity = _VoiceIdentity()

    setup_wizard._configure_phone_call_voice_stack(identity, **_voice_setup_kwargs())

    assert setup_wizard._env("INKBOX_VOICE_STACK") == "inkbox_tts_stt"
    assert setup_wizard._env("INKBOX_REALTIME_ENABLED") == "false"


def test_voice_ai_reuses_admin_identity_without_persisting_admin_key(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    monkeypatch.setenv("INKBOX_CLAUDE_ENV_FILE", str(env_file))
    choices = iter([0, 1])
    monkeypatch.setattr(setup_wizard, "prompt_choice", lambda *a, **k: next(choices))
    monkeypatch.setattr(
        setup_wizard,
        "prompt",
        lambda question, *a, **k: (
            ""
            if "Press Enter" in question
            else (_ for _ in ()).throw(AssertionError("admin key must be reused"))
        ),
    )
    identity = _VoiceIdentity(authority="contact_scoped")
    admin_identity = _VoiceIdentity(authority="contact_scoped")

    setup_wizard._configure_phone_call_voice_stack(
        identity,
        authority_identity=admin_identity,
        **_voice_setup_kwargs(),
    )

    assert admin_identity.authority_updates == ["yolo"]
    assert identity.hosted_updates == [{"voice": None, "model": None, "instructions": None}]
    assert identity.incoming_updates == [{
        "incoming_call_action": "hosted_agent",
        "client_websocket_url": None,
        "incoming_call_webhook_url": None,
    }]
    assert setup_wizard._env("INKBOX_VOICE_STACK") == "inkbox_voice_ai"
    assert setup_wizard._env("INKBOX_VOICE_AI_AUTHORITY_MODE") == "yolo"
    assert "admin" not in env_file.read_text().lower()


def test_voice_ai_failure_restores_all_prior_config(tmp_path, monkeypatch):
    monkeypatch.setenv("INKBOX_CLAUDE_ENV_FILE", str(tmp_path / ".env"))
    choices = iter([0, 1, 2])
    monkeypatch.setattr(setup_wizard, "prompt_choice", lambda *a, **k: next(choices))
    monkeypatch.setattr(setup_wizard, "prompt", lambda *a, **k: "")
    identity = _VoiceIdentity(authority="contact_scoped")
    identity.get_hosted_agent_config = lambda: types.SimpleNamespace(
        authority_mode="contact_scoped",
        voice="old-voice",
        model="old-model",
        instructions="old instructions",
    )
    incoming_calls = 0

    def fail_then_restore(**kwargs):
        nonlocal incoming_calls
        incoming_calls += 1
        identity.incoming_updates.append(kwargs)
        if incoming_calls == 1:
            raise RuntimeError("incoming update failed")

    identity.set_incoming_call_action = fail_then_restore
    admin_identity = _VoiceIdentity()
    # First selection attempts Voice AI and fails; second returns to TTS/STT.
    setup_wizard._configure_phone_call_voice_stack(
        identity,
        authority_identity=admin_identity,
        **_voice_setup_kwargs(),
    )

    assert admin_identity.authority_updates == ["yolo", "contact_scoped"]
    assert identity.hosted_updates == [
        {"voice": "old-voice", "model": "old-model", "instructions": "old instructions"},
        {"voice": "old-voice", "model": "old-model", "instructions": "old instructions"},
    ]
    assert identity.incoming_updates[1] == {
        "incoming_call_action": "auto_accept",
        "client_websocket_url": "wss://old.example/phone/media/ws",
        "incoming_call_webhook_url": "https://old.example/webhook",
    }
    assert identity.incoming_updates[-1]["client_websocket_url"] == (
        "wss://agent.example/phone/media/ws"
    )
    assert setup_wizard._env("INKBOX_VOICE_STACK") == "inkbox_tts_stt"


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


# ----------------------------------------------------------------------
# Dedicated phone number (standalone step, after iMessage)
# ----------------------------------------------------------------------


def test_offer_dedicated_number_reports_existing(monkeypatch, capsys):
    identity = types.SimpleNamespace(
        agent_handle="agent",
        phone_number=types.SimpleNamespace(number="+15550001111", type="local"),
    )
    monkeypatch.setattr(setup_wizard, "prompt_yes_no",
                        lambda *a, **k: (_ for _ in ()).throw(AssertionError("must not prompt")))

    result, provisioned = setup_wizard._offer_dedicated_number(object(), identity)

    assert result is identity and provisioned is False
    assert "Already provisioned: +15550001111" in capsys.readouterr().out


def test_offer_dedicated_number_provisions_and_refetches(monkeypatch):
    class FakePhones:
        def provision(self, *, agent_handle, type):
            assert (agent_handle, type) == ("agent", "local")
            return types.SimpleNamespace(number="+15550002222", type="local")

    refreshed = types.SimpleNamespace(
        agent_handle="agent",
        phone_number=types.SimpleNamespace(number="+15550002222", type="local"),
    )

    class FakeClient:
        phone_numbers = FakePhones()

        def get_identity(self, _handle):
            return refreshed

    identity = types.SimpleNamespace(agent_handle="agent", phone_number=None)
    monkeypatch.setattr(setup_wizard, "prompt_yes_no", lambda *a, **k: True)

    result, provisioned = setup_wizard._offer_dedicated_number(FakeClient(), identity)

    assert result is refreshed and provisioned is True


def test_offer_dedicated_number_failure_points_at_paid_tiers(monkeypatch, capsys):
    class FakePhones:
        def provision(self, **_kwargs):
            raise RuntimeError("HTTP 402 plan does not include phone numbers")

    class FakeClient:
        phone_numbers = FakePhones()

    identity = types.SimpleNamespace(agent_handle="agent", phone_number=None)
    monkeypatch.setattr(setup_wizard, "prompt_yes_no", lambda *a, **k: True)

    result, provisioned = setup_wizard._offer_dedicated_number(FakeClient(), identity)

    out = capsys.readouterr().out
    assert result is identity and provisioned is False
    # Plan-gating fallback: point at pricing, echo the raw error, keep moving.
    assert "Dedicated phone numbers are available on Inkbox paid tiers" in out
    assert "https://inkbox.ai/pricing" in out
    assert "HTTP 402 plan does not include phone numbers" in out


def test_offer_dedicated_number_declined_skips(monkeypatch, capsys):
    class FakeClient:
        phone_numbers = types.SimpleNamespace(
            provision=lambda **_k: (_ for _ in ()).throw(AssertionError("declined → no provision"))
        )

    identity = types.SimpleNamespace(agent_handle="agent", phone_number=None)
    monkeypatch.setattr(setup_wizard, "prompt_yes_no", lambda *a, **k: False)

    result, provisioned = setup_wizard._offer_dedicated_number(FakeClient(), identity)

    assert result is identity and provisioned is False
    assert "Skipped" in capsys.readouterr().out


# ----------------------------------------------------------------------
# Wizard step ordering
# ----------------------------------------------------------------------


def test_wizard_walks_imessage_before_dedicated_number(tmp_path, monkeypatch):
    """iMessage comes FIRST, then the standalone dedicated-number step, then
    the summary and the realtime offer — with the iMessage bool threaded into
    the realtime gate (the local identity object may be stale)."""
    env_file = tmp_path / ".env"
    monkeypatch.setenv("INKBOX_CLAUDE_ENV_FILE", str(env_file))
    for var in ("INKBOX_API_KEY", "INKBOX_IDENTITY", "INKBOX_BASE_URL"):
        monkeypatch.delenv(var, raising=False)

    identity = types.SimpleNamespace(agent_handle="agent", phone_number=None, mailbox=None)
    order = []
    seen = {}

    fake_symbols = {
        "Inkbox": lambda **_k: types.SimpleNamespace(),
        "InkboxAPIError": Exception,
        "IdentityPhoneNumberCreateOptions": object,
        "WhoamiApiKeyResponse": object,
        "ADMIN_SCOPED": "admin_scoped",
        "AGENT_CLAIMED": "agent_scoped_claimed",
        "AGENT_UNCLAIMED": "agent_scoped_unclaimed",
    }
    monkeypatch.setattr(setup_wizard, "_ensure_inkbox_sdk", lambda: fake_symbols)
    monkeypatch.setattr(setup_wizard, "prompt_yes_no", lambda *a, **k: True)
    monkeypatch.setattr(
        setup_wizard, "_api_key_flow", lambda *a, **k: (identity, "ApiKey_x", False, None)
    )
    monkeypatch.setattr(setup_wizard, "_configure_avatar", lambda *a, **k: order.append("avatar"))
    monkeypatch.setattr(
        setup_wizard, "_configure_imessage",
        lambda *a, **k: order.append("imessage") or True,
    )
    monkeypatch.setattr(
        setup_wizard, "_offer_dedicated_number",
        lambda _client, ident: order.append("dedicated_number") or (ident, False),
    )
    monkeypatch.setattr(setup_wizard, "_print_agent_summary", lambda *a, **k: order.append("summary"))
    monkeypatch.setattr(
        setup_wizard, "_wait_for_sms_opt_in",
        lambda *a, **k: order.append("sms_opt_in"),
    )

    def fake_voice_stack(_identity, *, imessage_enabled=False, **_kwargs):
        order.append("voice_stack")
        seen["imessage_enabled"] = imessage_enabled

    monkeypatch.setattr(setup_wizard, "_configure_phone_call_voice_stack", fake_voice_stack)
    monkeypatch.setattr(setup_wizard, "_setup_signing_key", lambda *a, **k: order.append("signing_key"))
    monkeypatch.setattr(setup_wizard, "_configure_project_dir", lambda: order.append("project_dir"))
    monkeypatch.setattr(setup_wizard, "_configure_autostart", lambda: order.append("autostart"))

    setup_wizard.interactive_setup()

    assert order == [
        "avatar",
        "imessage",
        "dedicated_number",
        "summary",
        "voice_stack",
        "signing_key",
        "project_dir",
        "autostart",
    ]
    # No number was provisioned this run, so the START wait never blocks.
    assert "sms_opt_in" not in order
    assert seen["imessage_enabled"] is True


# ----------------------------------------------------------------------
# Keeping the bridge running
# ----------------------------------------------------------------------


def _patch_daemon(monkeypatch, *, pid, calls):
    """Stub the daemon module the autostart step imports lazily."""
    daemon = types.ModuleType("daemon")
    daemon.running_pid = lambda: pid
    daemon.start = lambda: (calls.append("start"), 0)[1]
    daemon.restart = lambda: (calls.append("restart"), 0)[1]
    daemon.install_autostart = lambda _env_file: calls.append("install_autostart") or False
    daemon.state_dir = lambda: __import__("pathlib").Path(os.environ.get("INKBOX_CLAUDE_HOME", "/tmp"))
    monkeypatch.setitem(sys.modules, "inkbox_claude.daemon", daemon)
    monkeypatch.setattr(setup_wizard, "_confirm_bridge_running", lambda *_a, **_k: True)
    return daemon


def test_background_start_restarts_an_already_running_bridge(monkeypatch, capsys):
    calls = []
    _patch_daemon(monkeypatch, pid=4242, calls=calls)
    # Decline boot autostart, accept the background start.
    answers = iter([False, True])
    monkeypatch.setattr(setup_wizard, "prompt_yes_no", lambda *_a, **_k: next(answers))

    setup_wizard._configure_autostart()

    # A live bridge is still on the old .env — starting it again would no-op.
    assert calls == ["restart"]
    assert "pid 4242" in capsys.readouterr().out


def test_background_start_starts_when_nothing_is_running(monkeypatch):
    calls = []
    _patch_daemon(monkeypatch, pid=None, calls=calls)
    answers = iter([False, True])
    monkeypatch.setattr(setup_wizard, "prompt_yes_no", lambda *_a, **_k: next(answers))

    setup_wizard._configure_autostart()

    assert calls == ["start"]


def test_autostart_fallback_also_restarts_a_live_bridge(monkeypatch):
    # install_autostart() fails, so the step falls back to a background run —
    # which must reload a bridge that is already up, not no-op on it.
    calls = []
    _patch_daemon(monkeypatch, pid=99, calls=calls)
    monkeypatch.setattr(setup_wizard, "prompt_yes_no", lambda *_a, **_k: True)

    setup_wizard._configure_autostart()

    assert calls == ["install_autostart", "restart"]


def test_declining_both_offers_starts_nothing(monkeypatch, capsys):
    calls = []
    _patch_daemon(monkeypatch, pid=None, calls=calls)
    monkeypatch.setattr(setup_wizard, "prompt_yes_no", lambda *_a, **_k: False)

    setup_wizard._configure_autostart()

    assert calls == []
    assert "inkbox-claude start" in capsys.readouterr().out


def test_ready_banner_names_the_identity_and_the_health_command(capsys):
    setup_wizard._print_ready_banner("dev-agent")

    lines = [line for line in capsys.readouterr().out.splitlines() if line]
    assert any("dev-agent" in line for line in lines)
    assert any("inkbox-claude doctor" in line for line in lines)
    # Every row is the same width, so the box closes cleanly.
    assert len({len(line) for line in lines}) == 1


def test_ready_banner_box_fits_a_long_handle(capsys):
    setup_wizard._print_ready_banner("a-very-long-agent-handle-that-sets-the-width")

    lines = [line for line in capsys.readouterr().out.splitlines() if line]
    assert len({len(line) for line in lines}) == 1


def test_autostart_reports_a_live_bridge(monkeypatch):
    calls = []
    _patch_daemon(monkeypatch, pid=None, calls=calls)
    answers = iter([False, True])
    monkeypatch.setattr(setup_wizard, "prompt_yes_no", lambda *_a, **_k: next(answers))

    assert setup_wizard._configure_autostart() is True


def test_autostart_reports_no_bridge_when_both_offers_declined(monkeypatch):
    calls = []
    _patch_daemon(monkeypatch, pid=None, calls=calls)
    monkeypatch.setattr(setup_wizard, "prompt_yes_no", lambda *_a, **_k: False)

    assert setup_wizard._configure_autostart() is False


def test_autostart_reports_failure_when_the_daemon_will_not_start(monkeypatch):
    calls = []
    daemon = _patch_daemon(monkeypatch, pid=None, calls=calls)
    daemon.start = lambda: 1  # e.g. bad config, or the port is taken
    answers = iter([False, True])
    monkeypatch.setattr(setup_wizard, "prompt_yes_no", lambda *_a, **_k: next(answers))

    assert setup_wizard._configure_autostart() is False


def test_confirm_bridge_running_reports_a_pid_that_stays_up(monkeypatch):
    monkeypatch.setattr(setup_wizard.time, "sleep", lambda _s: None)

    assert setup_wizard._confirm_bridge_running(lambda: 4242, timeout=0.0) is True


def test_confirm_bridge_running_catches_a_bridge_that_dies(monkeypatch, capsys):
    # daemon.start() only watches the child for ~1.5s; a bridge that exits on
    # bad config just after that still leaves the operator a pid and a banner.
    pids = iter([4242, 4242, None])
    monkeypatch.setattr(setup_wizard.time, "sleep", lambda _s: None)

    assert setup_wizard._confirm_bridge_running(lambda: next(pids), timeout=30.0) is False

    out = capsys.readouterr().out
    assert "exited right after starting" in out
    assert "gateway.log" in out


def test_env_file_prefers_an_existing_local_dotenv(monkeypatch, tmp_path):
    monkeypatch.delenv("INKBOX_CLAUDE_ENV_FILE", raising=False)
    (tmp_path / ".env").write_text("INKBOX_IDENTITY=local\n")
    monkeypatch.chdir(tmp_path)

    assert setup_wizard._env_file_path() == tmp_path / ".env"


def test_env_file_falls_back_to_the_state_dir_not_cwd(monkeypatch, tmp_path):
    # Running setup from a home directory used to drop an API key into ~/.env,
    # which a globally installed bridge never reads.
    monkeypatch.delenv("INKBOX_CLAUDE_ENV_FILE", raising=False)
    state = tmp_path / "state"
    monkeypatch.setenv("INKBOX_CLAUDE_HOME", str(state))
    monkeypatch.chdir(tmp_path)

    assert setup_wizard._env_file_path() == state / ".env"


def test_env_file_honours_the_explicit_override(monkeypatch, tmp_path):
    monkeypatch.setenv("INKBOX_CLAUDE_ENV_FILE", str(tmp_path / "custom.env"))

    assert setup_wizard._env_file_path() == tmp_path / "custom.env"
