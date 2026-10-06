"""Real Claude reasoning → native in-process MCP → synthetic encrypted SDK API."""
import asyncio
import json
import os

import httpx
import pytest
from inkbox import Inkbox
from inkbox_claude.config import BridgeConfig
from inkbox_claude.sessions import ContactSession
from inkbox_claude.tools import build_inkbox_mcp_server
from tests.fixtures.vault_api import VaultAPI, VAULT_KEY, TOTP_SEED, LOGIN_ID

pytestmark = pytest.mark.skipif(os.getenv("INKBOX_RUN_NATIVE_HOST_TESTS") != "1",
                                reason="real Claude model contract runs in the authorized real-model E2E lane")


def test_real_host_retrieves_current_synthetic_two_factor_code(monkeypatch,tmp_path):
    api=VaultAPI()
    monkeypatch.setenv('INKBOX_CLAUDE_HOME',str(tmp_path/'state'))
    monkeypatch.setenv('INKBOX_CLAUDE_VAULT_KEY',VAULT_KEY)
    monkeypatch.delenv('INKBOX_VAULT_KEY',raising=False)
    monkeypatch.setattr('inkbox._config._CONFIG_PATH',tmp_path/'no-sdk-config')
    monkeypatch.setattr('inkbox._http.httpx.HTTPTransport',lambda **kwargs:httpx.MockTransport(api.respond))
    client=Inkbox(api_key='synthetic-agent-key',base_url='https://api.example.com')
    cfg=BridgeConfig(identity='example-agent',project_dir=str(tmp_path),permission_timeout_s=2)
    server,names=build_inkbox_mcp_server(client,cfg.identity,cfg)
    outputs=[]
    async def send(_chat,text,_mode,_meta):outputs.append(text)
    session=ContactSession('contract',cfg,send,server,names,{'handle':'example-agent'})
    # Observe the actual fresh SDK code, including a period-boundary change.
    codes=[]
    from inkbox.vault.resources.vault import UnlockedVault
    original=UnlockedVault.get_totp_code
    def observe(vault,secret_id):
        result=original(vault,secret_id);codes.append(result.code);return result
    monkeypatch.setattr(UnlockedVault,'get_totp_code',observe)
    async def run():
        try:
            await session.handle_inbound('Find the Example login in your Vault and tell me its current two-factor login code. Do not include its password or other credentials.',
                                         'email',{'sender':'operator@example.com','message_id':'local-only'})
            await asyncio.wait_for(session._worker,120)
        finally:await session.stop_companion()
    try:asyncio.run(run())
    finally:client.close()
    assert codes, 'Claude must actually obtain the code, not describe the tool'
    assert any(code in '\n'.join(outputs) for code in codes)
    assert all(secret not in '\n'.join(outputs) for secret in (VAULT_KEY,TOTP_SEED,'synthetic-password'))
    paths=[request.url.path for request in api.requests]
    assert '/api/v1/vault/secrets' in paths
    assert f'/api/v1/vault/secrets/{LOGIN_ID}' in paths
