import asyncio

import pytest

from inkbox_claude import gateway as gateway_module
from inkbox_claude.config import BridgeConfig, RealtimeConfig, VoiceStack
from inkbox_claude.gateway import InkboxGateway


def _enable_gateway_dependencies(monkeypatch):
    monkeypatch.setattr(gateway_module, "AIOHTTP_AVAILABLE", True)
    monkeypatch.setattr(gateway_module, "INKBOX_AVAILABLE", True)


def test_gateway_rejects_realtime_stack_without_api_key(monkeypatch):
    _enable_gateway_dependencies(monkeypatch)
    gateway = InkboxGateway(BridgeConfig(
        api_key="ApiKey_test",
        identity="agent",
        voice_stack=VoiceStack.OPENAI_REALTIME,
        realtime=RealtimeConfig(enabled=False, api_key=""),
    ))

    with pytest.raises(
        RuntimeError,
        match="openai_realtime requires INKBOX_REALTIME_API_KEY",
    ):
        asyncio.run(gateway.run())


def test_gateway_rejects_invalid_voice_ai_authority(monkeypatch):
    _enable_gateway_dependencies(monkeypatch)
    gateway = InkboxGateway(BridgeConfig(
        api_key="ApiKey_test",
        identity="agent",
        voice_stack=VoiceStack.INKBOX_VOICE_AI,
        voice_ai_authority_mode="unbounded",
    ))

    with pytest.raises(
        RuntimeError,
        match="INKBOX_VOICE_AI_AUTHORITY_MODE must be contact_scoped or yolo",
    ):
        asyncio.run(gateway.run())


def test_gateway_preserves_display_name_in_session_identity(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    _enable_gateway_dependencies(monkeypatch)
    identity = SimpleNamespace(
        agent_handle="example-agent", display_name="Example Assistant",
        mailbox=SimpleNamespace(email_address="agent@example.test"),
        phone_number=SimpleNamespace(number="+15165550101"),
    )
    monkeypatch.setattr(gateway_module, "Inkbox", lambda **kwargs: SimpleNamespace(
        get_identity=lambda handle: identity,
    ))
    gateway = InkboxGateway(BridgeConfig(
        api_key="ApiKey_test", identity="example-agent",
        public_url="https://example.test", project_dir="/tmp",
    ))
    monkeypatch.setattr(gateway, "_start_http_server", AsyncMock())
    monkeypatch.setattr(gateway, "_patch_identity_objects", lambda: None)
    monkeypatch.setattr(gateway_module, "build_inkbox_mcp_server", lambda *args: (None, []))
    captured = {}

    class StartupCaptured(Exception):
        pass

    def capture_sessions(**kwargs):
        captured.update(kwargs["identity_info"])
        raise StartupCaptured

    monkeypatch.setattr(gateway_module, "SessionManager", capture_sessions)
    with pytest.raises(StartupCaptured):
        asyncio.run(gateway.run())
    assert captured["display_name"] == identity.display_name
