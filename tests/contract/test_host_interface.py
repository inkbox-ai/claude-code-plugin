"""Contract tests: the host interface this bridge depends on, against the
INSTALLED claude-agent-sdk + Claude Code CLI.

Run in CI with the latest published SDK/CLI (not the pinned dev versions) so an
upstream rename, signature change, or removal fails HERE — before it takes down
a live gateway. Everything asserted is something the bridge actually imports,
constructs, or invokes.
"""

from __future__ import annotations

import shutil
import subprocess
from unittest.mock import MagicMock

import pytest


def test_sdk_exports_every_symbol_the_bridge_imports():
    # Mirrors the imports in sessions.py and tools.py, 1:1.
    from claude_agent_sdk import (  # noqa: F401
        AssistantMessage,
        ClaudeAgentOptions,
        ClaudeSDKClient,
        HookMatcher,
        PermissionResultAllow,
        PermissionResultDeny,
        ResultMessage,
        TextBlock,
        create_sdk_mcp_server,
        tool,
    )


def test_options_accept_the_kwargs_the_bridge_passes():
    """Constructing ClaudeAgentOptions with the exact kwargs sessions.py uses
    fails loudly if the SDK renames or drops any of them."""
    from claude_agent_sdk import ClaudeAgentOptions, ClaudeSDKClient, HookMatcher

    async def _can_use_tool(tool_name, input_data, context):  # signature stand-in
        raise NotImplementedError

    async def _pre_tool_use(hook_input, tool_use_id, context):
        return {}

    options = ClaudeAgentOptions(
        cwd="/tmp",
        model=None,
        system_prompt={"type": "preset", "preset": "claude_code", "append": "extra"},
        setting_sources=["user", "project"],
        allowed_tools=["Read", "mcp__inkbox__inkbox_whoami"],
        mcp_servers={},
        can_use_tool=_can_use_tool,
        hooks={"PreToolUse": [HookMatcher(hooks=[_pre_tool_use])]},
        resume=None,
    )
    # The client must construct from those options without connecting.
    assert ClaudeSDKClient(options=options) is not None


def test_permission_results_construct_like_the_bridge_uses_them():
    from claude_agent_sdk import PermissionResultAllow, PermissionResultDeny

    PermissionResultAllow()
    PermissionResultAllow(updated_input={"answers": {}})
    PermissionResultDeny(message="not approved")


def test_inkbox_mcp_server_builds_against_installed_sdk():
    """build_inkbox_mcp_server exercises the SDK's ``tool`` decorator and
    ``create_sdk_mcp_server`` for every tool the bridge registers."""
    from inkbox_claude.tools import build_inkbox_mcp_server

    server, tool_names = build_inkbox_mcp_server(MagicMock(), "contract-test")
    assert server is not None
    expected = {
        "mcp__inkbox__inkbox_whoami",
        "mcp__inkbox__inkbox_send_email",
        "mcp__inkbox__inkbox_send_sms",
        "mcp__inkbox__inkbox_send_imessage",
        "mcp__inkbox__inkbox_place_call",
        "mcp__inkbox__inkbox_list_calls",
        "mcp__inkbox__inkbox_get_call_transcript",
        "mcp__inkbox__inkbox_list_text_conversations",
        "mcp__inkbox__inkbox_get_text_conversation",
        "mcp__inkbox__inkbox_list_imessage_conversations",
        "mcp__inkbox__inkbox_get_imessage_conversation",
        "mcp__inkbox__inkbox_lookup_contact",
        "mcp__inkbox__inkbox_list_contacts",
        "mcp__inkbox__inkbox_get_contact",
        "mcp__inkbox__inkbox_create_contact",
        "mcp__inkbox__inkbox_update_contact",
        "mcp__inkbox__inkbox_delete_contact",
    }
    assert expected <= set(tool_names)


@pytest.mark.parametrize("field,value", [("notes", "Updated note"), ("given_name", "Ada")])
def test_contact_update_accepts_partial_fields_through_real_mcp_schema(field, value):
    import asyncio
    import anyio
    from mcp import ClientSession
    from mcp.shared.memory import create_client_server_memory_streams
    from inkbox_claude.tools import build_inkbox_mcp_server

    async def scenario():
        client = MagicMock()
        client.contacts.update.return_value = {"id": "contact-1", field: value}
        config, _ = build_inkbox_mcp_server(client, "contract-test")
        server = config["instance"]
        async with create_client_server_memory_streams() as (client_streams, server_streams):
            async with anyio.create_task_group() as tasks:
                tasks.start_soon(server.run, *server_streams, server.create_initialization_options())
                async with ClientSession(*client_streams) as session:
                    await session.initialize()
                    listed = await session.list_tools()
                    update = next(tool for tool in listed.tools if tool.name == "inkbox_update_contact")
                    assert update.model_dump(by_alias=True)["inputSchema"]["required"] == ["contact_id"]
                    result = await session.call_tool(
                        "inkbox_update_contact", {"contact_id": "contact-1", field: value}
                    )
                    assert not result.model_dump(by_alias=True).get("isError"), result.content
                tasks.cancel_scope.cancel()
        client.contacts.update.assert_called_once_with("contact-1", **{field: value})

    asyncio.run(scenario())


def test_claude_cli_installed_and_answers_version():
    """The SDK drives a ``claude`` subprocess; the CLI must be present and sane."""
    claude = shutil.which("claude")
    if claude is None:
        pytest.fail("claude CLI not on PATH — the bridge cannot start sessions without it")
    out = subprocess.run([claude, "--version"], capture_output=True, text=True, timeout=60)
    assert out.returncode == 0, f"claude --version failed: {out.stderr[:300]}"
    assert out.stdout.strip(), "claude --version printed nothing"
