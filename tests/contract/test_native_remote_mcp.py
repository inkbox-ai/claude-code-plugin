import asyncio
import json
import threading
from http.server import ThreadingHTTPServer
from types import SimpleNamespace
from typing import ClassVar

import pytest
from claude_agent_sdk import (
    ClaudeAgentOptions,
    ClaudeSDKClient,
    HookMatcher,
    PermissionResultAllow,
    PermissionResultDeny,
    ResultMessage,
    create_sdk_mcp_server,
)

from inkbox_claude.mcp_bridge import McpBridge
from tests.contract.test_native_bash_wait import Mock


@pytest.mark.parametrize("approve", [True, False])
def test_native_http_preserves_call_identity_and_approval(
    tmp_path, monkeypatch, approve
):
    async def run():
        monkeypatch.setenv("INKBOX_CLAUDE_HOME", str(tmp_path))

        class Model(Mock):
            results: ClassVar[list] = []
            captured_results: ClassVar[list] = []
            captured_tools: ClassVar[list] = []
            tool_name = "mcp__inkbox__example"
            tool_input: ClassVar[dict] = {"value": "synthetic"}

        model = ThreadingHTTPServer(("127.0.0.1", 0), Model)
        worker = threading.Thread(target=model.serve_forever, daemon=True)
        worker.start()
        session = SimpleNamespace(
            _current_turn=object(),
            _turn_active=True,
            _interrupting=False,
            _client_generation=0,
        )
        owner = McpBridge(
            create_sdk_mcp_server("inkbox", tools=[]), None, None, session
        )
        calls, hooks, approvals = [], [], []

        async def catalog():
            return descriptors

        descriptors = [
            {
                "name": "example",
                "description": "Synthetic example",
                "inputSchema": {
                    "type": "object",
                    "properties": {"value": {"type": "string"}},
                    "required": ["value"],
                },
                "outputSchema": {
                    "type": "object",
                    "properties": {"accepted": {"type": "boolean"}},
                },
                "annotations": {"readOnlyHint": False},
            }
        ]

        async def call(name, args, **kwargs):
            kwargs["before_execute"]()
            calls.append((name, args, kwargs["call_identity"]))
            return {
                "content": [{"type": "text", "text": "WAIT_FINISHED"}],
                "structuredContent": {"accepted": True},
            }

        async def hook(value, call_id, context):
            hooks.append((value, call_id))
            owner.bind(value, call_id)
            return {}

        async def permission(name, args, context):
            approvals.append(name)
            return (
                PermissionResultAllow(updated_input=args)
                if approve
                else PermissionResultDeny(message="Declined synthetic operation")
            )

        owner.catalog = catalog
        owner.remote = SimpleNamespace(call=call)
        owner.remote_names = {"example"}
        config = await owner.start()
        client = ClaudeSDKClient(
            options=ClaudeAgentOptions(
                cwd=str(tmp_path),
                setting_sources=[],
                mcp_servers={"inkbox": config},
                can_use_tool=permission,
                hooks={"PreToolUse": [HookMatcher(hooks=[hook])]},
                env={
                    "CLAUDE_CONFIG_DIR": str(tmp_path / "config"),
                    "ANTHROPIC_API_KEY": "synthetic-local-only",
                    "ANTHROPIC_AUTH_TOKEN": "",
                    "ANTHROPIC_BASE_URL": f"http://127.0.0.1:{model.server_port}",
                    "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
                    "DISABLE_AUTOUPDATER": "1",
                },
            )
        )
        try:
            await client.connect()
            await client.query("Run the synthetic example tool.")
            async for msg in client.receive_response():
                if isinstance(msg, ResultMessage):
                    assert not msg.is_error
                    break
            assert hooks[0][1] == "toolu_wait"
            assert hooks[0][0]["session_id"] and hooks[0][0]["prompt_id"]
            assert approvals == ["mcp__inkbox__example"]
            assert bool(calls) is approve
            if approve:
                assert calls[0][:2] == ("example", {"value": "synthetic"})
                assert json.loads(Model.captured_results[0]["content"]) == {
                    "accepted": True
                }
                descriptors[:] = [
                    {
                        "name": "changed",
                        "description": "Changed example",
                        "inputSchema": {"type": "object"},
                    }
                ]
                await client.reconnect_mcp_server("inkbox")
                await client.query(
                    "Inspect the changed catalog without executing anything."
                )
                async for msg in client.receive_response():
                    if isinstance(msg, ResultMessage):
                        assert not msg.is_error
                        break
                tools = {tool["name"]: tool for tool in Model.captured_tools[-1]}
                assert "mcp__inkbox__example" not in tools
                assert tools["mcp__inkbox__changed"]["description"] == "Changed example"
                descriptors.clear()
                await client.reconnect_mcp_server("inkbox")
                await client.query("Inspect the empty remote catalog.")
                async for msg in client.receive_response():
                    if isinstance(msg, ResultMessage):
                        assert not msg.is_error
                        break
                assert not any(
                    tool["name"].startswith("mcp__inkbox__")
                    for tool in Model.captured_tools[-1]
                )
        finally:
            await client.disconnect()
            await owner.close()
            model.shutdown()
            model.server_close()
            worker.join(timeout=2)

    asyncio.run(asyncio.wait_for(run(), 50))
