from __future__ import annotations

import json
import types

from inkbox_claude import bootstrap as subject
from inkbox_claude import cli


class Identity:
    id = "identity-1"
    agent_handle = "helper"
    mailbox = types.SimpleNamespace(email_address="helper@example.com")
    phone_number = types.SimpleNamespace(number="+15551234567")
    tunnel = types.SimpleNamespace(public_host="helper.example.com")
    imessage_enabled = False
    imessage_number = None

    def __init__(self, signing=False):
        self.signing = signing
        self.hosted = types.SimpleNamespace(voice="cedar", model="voice", instructions=None, authority_mode="contact_scoped")
        self.incoming = types.SimpleNamespace(incoming_call_action="auto_accept", client_websocket_url="wss://old", incoming_call_webhook_url=None)
        self.signing_creations = 0

    def get_hosted_agent_config(self): return self.hosted
    def set_hosted_agent_config(self, **values): self.hosted = types.SimpleNamespace(authority_mode="contact_scoped", **values)
    def get_incoming_call_action(self): return self.incoming
    def set_incoming_call_action(self, **values): self.incoming = types.SimpleNamespace(**values)
    def get_signing_key_status(self): return types.SimpleNamespace(configured=self.signing)
    def create_signing_key(self):
        self.signing = True
        self.signing_creations += 1
        return types.SimpleNamespace(signing_key="signing-secret")


class Client:
    def __init__(self, key, identity):
        self.key = key
        self.identity = identity
        self.api_keys = types.SimpleNamespace(create=lambda **_kwargs: types.SimpleNamespace(api_key="agent-secret"))
        self.imessages = types.SimpleNamespace()

    def whoami(self): return types.SimpleNamespace(auth_type="api_key", auth_subtype="api_key.agent_scoped.claimed")
    def list_identities(self): return [self.identity]
    def get_identity(self, handle):
        if handle != self.identity.agent_handle: raise RuntimeError("not found")
        return self.identity


def install(monkeypatch, identity):
    saved = {}
    class Inkbox:
        def __new__(cls, *, api_key, **_kwargs): return Client(api_key, identity)
    monkeypatch.setattr(subject, "_load_inkbox_symbols", lambda: {
        "Inkbox": Inkbox,
        "ADMIN_SCOPED": "api_key.admin_scoped",
        "AGENT_CLAIMED": "api_key.agent_scoped.claimed",
        "AGENT_UNCLAIMED": "api_key.agent_scoped.unclaimed",
    })
    monkeypatch.setattr(subject, "_save", lambda name, value: saved.__setitem__(name, value))
    monkeypatch.setattr(subject, "_env", lambda name: saved.get(name, ""))
    return saved


def test_bootstrap_configures_voice_signing_and_gateway(monkeypatch):
    identity = Identity()
    saved = install(monkeypatch, identity)
    monkeypatch.setattr(subject, "_start_gateway", lambda actions: actions.append("started_gateway_process") or True)
    result = subject.bootstrap(identity_handle="@helper", api_key="agent-secret", project_dir="/work", voice_ai=True, rotate_signing_key=True, start_gateway=True)
    assert result["status"] == "configured"
    assert result["gateway_running"] is True
    assert saved["INKBOX_API_KEY"] == "agent-secret"
    assert saved["CLAUDE_PROJECT_DIR"] == "/work"
    assert saved["INKBOX_VOICE_STACK"] == "inkbox_voice_ai"
    assert saved["INKBOX_SIGNING_KEY"] == "signing-secret"


def test_bootstrap_requires_explicit_signing_rotation(monkeypatch):
    identity = Identity(signing=True)
    install(monkeypatch, identity)
    result = subject.bootstrap(identity_handle="helper", api_key="agent-secret")
    assert result["status"] == "requires_human"
    assert "--rotate-signing-key" in result["human_actions"][0]
    assert identity.signing_creations == 0


def test_cli_never_prints_key(monkeypatch, capsys):
    monkeypatch.setenv("INKBOX_API_KEY", "top-secret")
    monkeypatch.setattr(cli, "bootstrap", lambda **kwargs: {"status": "configured", "identity": kwargs["identity_handle"]})
    assert cli.main(["bootstrap", "--identity", "helper"]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "configured"
    assert "top-secret" not in capsys.readouterr().out
