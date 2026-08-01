"""Doctor keeps identity reachability separate from remote voice config."""

from __future__ import annotations

import sys
import types

from inkbox_claude import daemon, doctor
from inkbox_claude.config import BridgeConfig, VoiceStack


def test_voice_config_probe_failures_do_not_mark_identity_unreachable(
    monkeypatch, tmp_path,
):
    identity = types.SimpleNamespace(
        mailbox=types.SimpleNamespace(email_address="agent@example.com"),
        phone_number=types.SimpleNamespace(number="+15551234567"),
        imessage_enabled=False,
        get_incoming_call_action=lambda: (_ for _ in ()).throw(
            RuntimeError("incoming config unavailable")
        ),
        get_hosted_agent_config=lambda: (_ for _ in ()).throw(
            RuntimeError("authority config unavailable")
        ),
    )
    client = types.SimpleNamespace(get_identity=lambda _handle: identity)
    inkbox_module = types.ModuleType("inkbox")
    inkbox_module.Inkbox = lambda **_kwargs: client
    monkeypatch.setitem(sys.modules, "inkbox", inkbox_module)
    monkeypatch.setattr(daemon, "_maybe_load_env_file", lambda: None)
    monkeypatch.setattr(doctor.shutil, "which", lambda _name: "/usr/bin/claude")
    monkeypatch.setattr(
        doctor,
        "read_config",
        lambda: BridgeConfig(
            api_key="ApiKey_agent",
            identity="agent",
            signing_key="whsec_test",
            project_dir=str(tmp_path),
            voice_stack=VoiceStack.INKBOX_VOICE_AI,
            voice_ai_authority_mode="yolo",
        ),
    )

    checks = doctor.run_doctor()
    by_name = {name: (ok, detail) for name, ok, detail in checks}
    reachable = [row for row in checks if row[0] == "identity reachable"]

    assert reachable == [("identity reachable", True, "agent@example.com, +15551234567")]
    assert by_name["incoming call action"] == (
        False,
        "incoming config unavailable",
    )
    assert by_name["Voice AI authority"] == (
        False,
        "authority config unavailable",
    )


def test_doctor_reports_remote_routing_mismatch(monkeypatch, tmp_path):
    identity = types.SimpleNamespace(
        mailbox=types.SimpleNamespace(email_address="agent@example.com"),
        phone_number=types.SimpleNamespace(number="+15551234567"),
        imessage_enabled=False,
        get_incoming_call_action=lambda: types.SimpleNamespace(
            incoming_call_action="hosted_agent"
        ),
    )
    client = types.SimpleNamespace(get_identity=lambda _handle: identity)
    inkbox_module = types.ModuleType("inkbox")
    inkbox_module.Inkbox = lambda **_kwargs: client
    monkeypatch.setitem(sys.modules, "inkbox", inkbox_module)
    monkeypatch.setattr(daemon, "_maybe_load_env_file", lambda: None)
    monkeypatch.setattr(doctor.shutil, "which", lambda _name: "/usr/bin/claude")
    monkeypatch.setattr(
        doctor,
        "read_config",
        lambda: BridgeConfig(
            api_key="ApiKey_agent",
            identity="agent",
            signing_key="whsec_test",
            project_dir=str(tmp_path),
            voice_stack=VoiceStack.INKBOX_TTS_STT,
        ),
    )

    by_name = {
        name: (ok, detail)
        for name, ok, detail in doctor.run_doctor()
    }
    assert by_name["incoming call action"] == (
        False,
        "hosted_agent (expected auto_accept)",
    )
