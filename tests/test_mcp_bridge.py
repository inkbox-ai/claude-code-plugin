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


def test_native_bridge_validates_full_remote_schema(tmp_path, monkeypatch):
    from inkbox.remote_mcp import RemoteMcp

    async def run():
        owner = bridge(tmp_path, monkeypatch)
        remote = RemoteMcp(
            api_key="synthetic",
            base_url="https://example.test",
            identity="identity",
            journal_path=tmp_path / "schema.sqlite",
        )
        remote._catalog = (
            {
                "name": "remote_example",
                "inputSchema": {
                    "type": "object",
                    "properties": {"text": {"type": "string", "minLength": 2}},
                    "required": ["text"],
                    "additionalProperties": False,
                    "allOf": [{"properties": {"text": {"const": "accepted"}}}],
                },
            },
        )

        async def unexpected(*args, **kwargs):
            pytest.fail("Invalid arguments must not reach the transport")

        remote.request = unexpected
        owner.remote = remote
        owner.remote_names = {"remote_example"}
        owner.bind(
            {
                "session_id": "session",
                "prompt_id": "prompt",
                "tool_name": "mcp__inkbox__remote_example",
            },
            "toolu_invalid",
        )
        with pytest.raises(RemoteToolError, match="remote schema"):
            await owner.call(
                {
                    "name": "remote_example",
                    "arguments": {"text": "otherwise valid"},
                    "_meta": {"claudecode/toolUseId": "toolu_invalid"},
                }
            )

    asyncio.run(run())


@pytest.mark.parametrize(
    "failure,expected",
    [
        ("missing", "stage=binding category=remote_tool binding=missing schema=unchecked"),
        ("mismatch", "stage=binding category=remote_tool binding=mismatch schema=unchecked"),
        ("conversion", "stage=conversion category=type binding=matched schema=unchecked"),
        ("schema", "stage=remote_preflight category=schema binding=matched schema=failed"),
        ("journal", "stage=remote_preflight category=journal binding=matched schema=unchecked"),
        ("stale", "stage=turn_validation category=remote_tool binding=matched schema=passed"),
        ("transport", "stage=remote_request category=transport binding=matched schema=passed"),
    ],
)
def test_failure_diagnostics_identify_stage_without_payloads(tmp_path, monkeypatch, caplog, failure, expected):
    import sqlite3
    from inkbox.remote_mcp import CallJournal, RemoteMcp

    secret = "synthetic-sensitive-value@example.invalid"

    async def run():
        owner = bridge(tmp_path, monkeypatch)
        remote = RemoteMcp(
            api_key=secret, base_url="https://example.invalid", identity="example",
            journal_path=tmp_path / "catalog.sqlite",
        )
        remote._catalog = ({
            "name": "inkbox_contact_create",
            "inputSchema": {
                "type": "object", "required": ["contact"],
                "properties": {"contact": {"type": "object", "required": ["name"]}},
            },
        },)
        owner.remote = remote
        owner.remote_names = {"inkbox_contact_create"}
        name = "inkbox_create_contact"
        params = {"name": name, "arguments": {"name": secret}, "_meta": {"claudecode/toolUseId": "call"}}
        if failure != "missing":
            owner.bind({"session_id": "session", "prompt_id": "prompt", "tool_name": "mcp__inkbox__" + name}, "call")
        if failure == "mismatch":
            params["name"] = "inkbox_contact_create"
        if failure == "conversion":
            params["arguments"]["emails"] = None
        if failure == "schema":
            params["arguments"] = {"notes": secret}
        if failure == "journal":
            def fail_journal(*args):
                raise sqlite3.OperationalError(secret)
            monkeypatch.setattr(CallJournal, "begin", fail_journal)
        if failure == "stale":
            owner.session._current_turn = object()

        async def request(*args):
            assert failure == "transport", "failure must occur before network access"
            raise httpx.ConnectError(secret)

        monkeypatch.setattr(remote, "request", request)

        async def body():
            return {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": params}

        response = await owner.handle(SimpleNamespace(headers={"Authorization": "Bearer " + owner.token}, json=body))
        import json
        error = json.loads(response.text)["error"]
        assert error["code"] == -32000
        if failure == "schema":
            assert "field=contact, rule=required, actual=object" in error["message"]
            assert "No request was sent" in error["message"]
        else:
            assert error["message"] == "Tool request unavailable or no longer authorized"
        assert secret not in response.text
        assert secret not in caplog.text
        assert expected in caplog.text

    asyncio.run(run())


def test_contact_create_catalog_accepts_minimal_fields_and_string_lists(tmp_path, monkeypatch):
    from jsonschema import Draft202012Validator, ValidationError
    from mcp.types import ListToolsRequest
    from inkbox_claude.tools import build_inkbox_mcp_server
    from inkbox_claude.remote_compat import compatibility_arguments

    async def run():
        owner = bridge(tmp_path, monkeypatch)
        local, _ = build_inkbox_mcp_server(owner.client, "example")
        owner.local = local["instance"]
        catalog = await owner.local_request(ListToolsRequest(method="tools/list"))
        schema = next(row.inputSchema for row in catalog.tools if row.name == "inkbox_create_contact")
        validator = Draft202012Validator(schema)
        arguments = {"preferred_name": "Example", "emails": ["sample@example.com"]}
        validator.validate(arguments)
        for field in ("emails", "phones"):
            with pytest.raises(ValidationError):
                validator.validate({"preferred_name": "Example", field: [{"value": "invalid shape"}]})
        with pytest.raises(ValidationError):
            validator.validate({"name": "unsupported field"})
        converted = compatibility_arguments("inkbox_create_contact", arguments, "operation")
        assert converted == {
            "contact": {"preferred_name": "Example", "emails": [
                {"value": "sample@example.com", "label": None, "is_primary": True},
            ]}, "idempotency_key": "operation",
        }

    asyncio.run(run())


def test_schema_failure_detail_excludes_payloads_and_unknown_property_names():
    from jsonschema import Draft202012Validator, ValidationError
    from inkbox_claude.mcp_bridge import schema_failure_detail

    secret = "sensitive@example.invalid"
    try:
        try:
            Draft202012Validator({"type": "object", "additionalProperties": {"type": "string"}}).validate({secret: {"value": secret}})
        except ValidationError:
            raise RemoteToolError("Tool arguments do not match the remote schema") from None
    except RemoteToolError as error:
        assert schema_failure_detail(error) == ("other", "type", "object")
