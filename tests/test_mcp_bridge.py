import asyncio
from types import SimpleNamespace

import httpx
import pytest
from claude_agent_sdk import create_sdk_mcp_server, tool
from inkbox.remote_mcp import RemoteToolError

from inkbox_claude.mcp_bridge import McpBridge


def bridge(tmp_path, monkeypatch):
    monkeypatch.setenv("INKBOX_CLAUDE_HOME", str(tmp_path))

    @tool("local_example", "Local example", {})
    async def example(arguments):
        from inkbox_claude.tools import CURRENT_SESSION

        assert CURRENT_SESSION.get() is session
        return {"content": [{"type": "text", "text": "local"}]}

    session = SimpleNamespace(
        _current_turn=object(),
        _turn_active=True,
        _interrupting=False,
        _client_generation=0,
    )
    client = SimpleNamespace(
        whoami=lambda: SimpleNamespace(auth_subtype="api_key.agent_scoped.claimed"),
        get_identity=lambda name: SimpleNamespace(id="identity-example"),
        _base_url="https://inkbox.ai",
    )
    config = SimpleNamespace(api_key="synthetic", identity="example")
    return McpBridge(
        create_sdk_mcp_server("inkbox", tools=[example]), client, config, session
    )


def test_loopback_transport_requires_token_and_exact_route(tmp_path, monkeypatch):
    async def run():
        owner = bridge(tmp_path, monkeypatch)
        config = await owner.start()
        try:
            async with httpx.AsyncClient() as client:
                assert (await client.post(config["url"], json={})).status_code == 401
                assert (
                    await client.get(config["url"], headers=config["headers"])
                ).status_code == 405
                assert (
                    await client.post(
                        config["url"] + "/other", headers=config["headers"], json={}
                    )
                ).status_code == 404
                response = await client.post(
                    config["url"],
                    headers=config["headers"],
                    json={
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "tools/call",
                        "params": {"name": "local_example", "arguments": {}},
                    },
                )
                assert response.json()["result"]["content"][0]["text"] == "local"
                assert "Access-Control-Allow-Origin" not in response.headers
        finally:
            await owner.close()
        assert owner.runner is None

    asyncio.run(run())


def test_remote_calls_bind_native_identity_and_fence_old_turns(tmp_path, monkeypatch):
    async def run():
        owner = bridge(tmp_path, monkeypatch)
        received = []

        async def call(name, args, **kwargs):
            kwargs["before_execute"]()
            received.append((name, args, kwargs))
            return {
                "content": [],
                "structuredContent": {"accepted": True},
                "_meta": {"receipt": "example"},
            }

        owner.remote = SimpleNamespace(call=call)
        owner.remote_names = {"remote_example"}
        params = {
            "name": "remote_example",
            "arguments": {},
            "_meta": {"claudecode/toolUseId": "toolu_example"},
        }
        with pytest.raises(RemoteToolError, match="not bound"):
            await owner.call(params)
        owner.bind(
            {
                "session_id": "session-one",
                "prompt_id": "prompt-one",
                "tool_name": "mcp__inkbox__remote_example",
            },
            "toolu_example",
        )
        first = await owner.call(params)
        await owner.call(params)
        assert first["structuredContent"] == {"accepted": True}
        assert first["_meta"] == {"receipt": "example"}
        assert received[0][2]["call_identity"] == received[1][2]["call_identity"]
        owner.session._current_turn = object()
        with pytest.raises(RemoteToolError, match="ended"):
            await owner.call(params)
        with pytest.raises(RemoteToolError, match="stable"):
            owner.bind({}, {})

    asyncio.run(run())


def test_catalog_publication_preserves_previous_generation_on_collision(
    tmp_path, monkeypatch
):
    async def run():
        owner = bridge(tmp_path, monkeypatch)
        tools = [
            {
                "name": "remote_example",
                "inputSchema": {"type": "object"},
                "annotations": {"readOnlyHint": True},
            }
        ]

        class Remote:
            def __init__(self, **kwargs):
                pass

            async def refresh(self):
                return tools

        monkeypatch.setattr("inkbox_claude.mcp_bridge.RemoteMcp", Remote)
        assert [tool["name"] for tool in await owner.catalog()] == [
            "remote_example",
            "local_example",
        ]
        published = owner.remote
        tools[:] = [{"name": "local_example"}]
        with pytest.raises(RemoteToolError, match="conflicts"):
            await owner.refresh()
        assert owner.remote is published
        assert owner.remote_names == {"remote_example"}
        owner.client.whoami = lambda: SimpleNamespace(
            auth_subtype="api_key.admin_scoped"
        )
        assert [tool["name"] for tool in await owner.refresh()] == ["local_example"]
        assert owner.remote is None

    asyncio.run(run())
