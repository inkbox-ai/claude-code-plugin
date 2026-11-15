"""Session-bound native MCP transport preserving remote metadata and results."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import secrets
import sqlite3
import time
from pathlib import Path

import httpx
from aiohttp import web
from inkbox.remote_mcp import RemoteMcp, RemoteToolError
from mcp.types import CallToolRequest, CallToolRequestParams, ListToolsRequest

from . import terminal_reply
from .remote_compat import COMPATIBILITY_NAMES, compatibility_arguments

logger = logging.getLogger(__name__)


def failure_category(error):
    if isinstance(error, RemoteToolError):
        if str(error) == "Tool arguments do not match the remote schema":
            return "schema"
        return "remote_tool"
    for kind, category in (
        (sqlite3.Error, "journal"), (httpx.TimeoutException, "timeout"),
        (httpx.HTTPError, "transport"), (TimeoutError, "timeout"),
        (OSError, "io"), (KeyError, "missing_field"),
        (TypeError, "type"), (ValueError, "value"),
    ):
        if isinstance(error, kind):
            return category
    return "other"


def schema_failure_detail(error):
    from jsonschema.exceptions import ValidationError

    cause = error.__context__
    if not isinstance(cause, ValidationError):
        return "unknown", "unknown", "unknown"
    fields = {
        "contact", "patch", "contact_id", "idempotency_key", "given_name", "family_name",
        "preferred_name", "company_name", "job_title", "notes", "emails", "phones",
        "value", "value_e164", "label", "is_primary",
    }
    field = ".".join(
        "item" if isinstance(part, int) else part if part in fields else "other"
        for part in list(cause.absolute_path)[:6]
    ) or "root"
    rule = cause.validator if cause.validator in {
        "type", "required", "additionalProperties", "anyOf", "oneOf", "allOf",
        "format", "pattern", "enum", "maxLength", "minLength", "maxItems", "minItems",
    } else "other"
    actual = {
        str: "string", dict: "object", list: "array", bool: "boolean",
        int: "number", float: "number", type(None): "null",
    }.get(type(cause.instance), "other")
    return field, rule, actual


class McpBridge:
    def __init__(self, local_server, client, cfg, session):
        self.local = local_server["instance"]
        self.client, self.cfg, self.session = client, cfg, session
        self.remote = None
        self.remote_names = frozenset()
        self.tools = None
        self.refreshed_at = 0.0
        self.calls = {}
        self.runner = None
        self.token = secrets.token_urlsafe(32)
        self.root = (
            Path(os.getenv("INKBOX_CLAUDE_HOME") or Path.home() / ".inkbox-claude")
            / "remote_calls"
        )

    async def start(self):
        app = web.Application(client_max_size=1_048_576)
        app.router.add_post("/mcp", self.handle)
        self.runner = web.AppRunner(app, access_log=None, shutdown_timeout=1)
        await self.runner.setup()
        try:
            site = web.TCPSite(self.runner, "127.0.0.1", 0)
            await site.start()
            port = self.runner.addresses[0][1]
            return {
                "type": "http",
                "url": f"http://127.0.0.1:{port}/mcp",
                "headers": {"Authorization": "Bearer " + self.token},
            }
        except BaseException:
            await self.close()
            raise

    async def close(self):
        self.calls.clear()
        if self.runner is not None:
            await self.runner.cleanup()
            self.runner = None

    def bind(self, hook, call_id):
        values = [hook.get("session_id"), hook.get("prompt_id"), call_id]
        if not all(
            isinstance(value, str) and 1 <= len(value) <= 256 for value in values
        ):
            raise RemoteToolError(
                "The native host did not supply a stable tool-call identity"
            )
        if len(self.calls) >= 512 and call_id not in self.calls:
            raise RemoteToolError("The native turn has too many pending tool calls")
        turn = self.session._current_turn
        generation = self.session._client_generation

        def validate():
            if (
                turn is None
                or self.session._current_turn is not turn
                or not self.session._turn_active
                or self.session._interrupting
                or self.session._client_generation != generation
            ):
                raise RemoteToolError(
                    "The native turn ended before this tool operation"
                )

        validate()
        identity = hashlib.sha256(
            json.dumps(values, separators=(",", ":")).encode()
        ).hexdigest()
        self.calls[call_id] = (
            identity,
            values[0],
            str(hook.get("tool_name") or ""),
            validate,
        )

    async def catalog(self):
        if self.tools is None:
            await self.refresh()
        return self.tools

    async def local_request(self, request):
        lookup = getattr(self.local, "get_request_handler", None)
        if lookup is not None:
            entry = lookup(request.method)
            if entry is None:
                raise RemoteToolError("The local tool method is unavailable")
            params = request.params.model_dump(by_alias=True) if request.params else {}
            # SDK-created local handlers do not use an MCP connection context.
            return await entry.handler(None, entry.params_type.model_validate(params))
        result = await self.local.request_handlers[type(request)](request)
        return result.root

    async def refresh(self):
        result = await self.local_request(
            ListToolsRequest(method="tools/list")
        )
        local = [
            tool.model_dump(mode="json", by_alias=True, exclude_none=True)
            for tool in result.tools
        ]
        try:
            account = await asyncio.wait_for(asyncio.to_thread(self.client.whoami), 5)
        except Exception:  # noqa: BLE001
            raise RemoteToolError(
                "Tool discovery could not verify current authority"
            ) from None
        if account.auth_subtype in {
            "api_key.admin_scoped",
            "api_key.agent_scoped.unclaimed",
        }:
            self.remote, self.remote_names = None, frozenset()
            self.tools = local
            self.refreshed_at = time.monotonic()
            return self.tools
        if account.auth_subtype != "api_key.agent_scoped.claimed":
            raise RemoteToolError("This credential type does not support remote tools")
        try:
            identity = await asyncio.wait_for(
                asyncio.to_thread(self.client.get_identity, self.cfg.identity), 5
            )
        except Exception:  # noqa: BLE001
            raise RemoteToolError(
                "Tool discovery could not verify the configured identity"
            ) from None
        remote = RemoteMcp(
            api_key=self.cfg.api_key,
            base_url=self.client._base_url,
            identity=str(identity.id),
            journal_path=self.root / "catalog.sqlite",
        )
        catalog = await remote.refresh()
        names = {tool["name"] for tool in catalog}
        if terminal_reply.NAME in names:
            raise RemoteToolError("A remote tool conflicts with a local delivery tool")
        if names & {tool["name"] for tool in local}:
            raise RemoteToolError("A remote tool conflicts with a local tool")
        self.remote, self.remote_names = remote, frozenset(names)
        retained = []
        for tool in local:
            replacement = COMPATIBILITY_NAMES.get(tool["name"])
            if replacement is not None:
                if replacement not in names:
                    continue
                tool = {
                    **tool,
                    "description": tool.get("description", "")
                    + f" Compatibility name for {replacement}.",
                }
            retained.append(tool)
        helpers = (
            [terminal_reply.DESCRIPTOR]
            if names & {"inkbox_email_reply", "inkbox_text_send"}
            else []
        )
        self.tools = catalog + retained + helpers
        self.refreshed_at = time.monotonic()
        return self.tools

    async def call(self, params, *, diagnostics=None):
        diagnostics = diagnostics if diagnostics is not None else {}
        diagnostics["stage"] = "arguments"
        name, arguments = params.get("name"), params.get("arguments", {})
        if not isinstance(name, str) or not isinstance(arguments, dict):
            raise TypeError("Invalid tool arguments")
        if self.remote is not None and (
            name in self.remote_names
            or name in COMPATIBILITY_NAMES
            or name == terminal_reply.NAME
        ):
            diagnostics["stage"] = "binding"
            metadata = params.get("_meta") or {}
            call_id = (
                metadata.get("claudecode/toolUseId")
                if isinstance(metadata, dict)
                else None
            )
            entry = self.calls.get(call_id) if isinstance(call_id, str) else None
            diagnostics["binding"] = (
                "missing" if entry is None else
                "matched" if entry[2] == "mcp__inkbox__" + name else "mismatch"
            )
            if entry is None or entry[2] != "mcp__inkbox__" + name:
                raise RemoteToolError(
                    "The tool call is not bound to an active native operation"
                )
            operation_id, session_id, _, validate = entry
            if name == terminal_reply.NAME:
                diagnostics["stage"] = "terminal_reply"
                turn = self.session._current_turn
                journal = self.root / (
                    "reply-"
                    + hashlib.sha256(self.session.chat_id.encode()).hexdigest()
                    + ".sqlite"
                )
                return await terminal_reply.send(
                    self.remote,
                    self.client,
                    self.cfg.identity,
                    terminal_reply.source(turn.mode, turn.reply_meta),
                    arguments,
                    journal,
                    validate,
                )
            journal = self.root / (
                hashlib.sha256(session_id.encode()).hexdigest() + ".sqlite"
            )
            diagnostics["stage"] = "conversion"
            converted = compatibility_arguments(name, arguments)

            def before_execute():
                diagnostics["schema"] = "passed"
                diagnostics["stage"] = "turn_validation"
                validate()
                diagnostics["stage"] = "remote_request"

            diagnostics["stage"] = "remote_preflight"
            return await self.remote.call(
                COMPATIBILITY_NAMES.get(name, name),
                converted,
                call_identity=operation_id,
                journal_path=journal,
                before_execute=before_execute,
            )
        diagnostics["stage"] = "local_request"
        from .tools import A2A_TURN_CONTEXT, CURRENT_SESSION

        turn = self.session._current_turn
        session_token = CURRENT_SESSION.set(self.session)
        a2a_token = A2A_TURN_CONTEXT.set(getattr(turn, "a2a_context", None))
        try:
            result = await self.local_request(
                CallToolRequest(
                    method="tools/call",
                    params=CallToolRequestParams(name=name, arguments=arguments),
                )
            )
            return result.model_dump(mode="json", by_alias=True, exclude_none=True)
        finally:
            A2A_TURN_CONTEXT.reset(a2a_token)
            CURRENT_SESSION.reset(session_token)

    async def handle(self, request):
        if not secrets.compare_digest(
            request.headers.get("Authorization", ""), "Bearer " + self.token
        ):
            return web.Response(status=401)
        identifier = None
        diagnostics = {"stage": "decode", "binding": "unchecked", "schema": "unchecked"}
        try:
            body = await request.json()
            if not isinstance(body, dict) or body.get("jsonrpc") != "2.0":
                raise ValueError("Invalid request")
            identifier = body.get("id")
            method, params = body.get("method"), body.get("params", {})
            diagnostics["stage"] = "dispatch"
            if not isinstance(params, dict):
                raise TypeError("Invalid parameters")
            if method == "initialize":
                result = {
                    "protocolVersion": params.get("protocolVersion", "2025-11-25"),
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "inkbox", "version": "1"},
                }
            elif method == "notifications/initialized":
                return web.Response(status=202)
            elif method == "ping":
                result = {}
            elif method == "tools/list":
                # The native host rejects root combinators; RemoteMcp validates the full schema.
                result = {
                    "tools": [
                        {
                            **tool,
                            "inputSchema": {
                                key: value for key, value in tool["inputSchema"].items()
                                if key not in {"allOf", "anyOf", "oneOf"}
                            },
                        } if tool["name"] in self.remote_names else tool
                        for tool in await self.catalog()
                    ]
                }
            elif method == "tools/call":
                result = await self.call(params, diagnostics=diagnostics)
            else:
                raise ValueError("Unsupported method")
            payload = {"jsonrpc": "2.0", "id": identifier, "result": result}
        except Exception as error:  # noqa: BLE001
            category = failure_category(error)
            message = "Tool request unavailable or no longer authorized"
            if category == "schema":
                diagnostics["schema"] = "failed"
                field, rule, actual = schema_failure_detail(error)
                logger.warning("MCP schema failure field=%s rule=%s actual=%s", field, rule, actual)
                message = (
                    f"Tool arguments do not match the schema: field={field}, rule={rule}, actual={actual}. "
                    "No request was sent. Correct the arguments using the tool schema."
                )
            logger.warning(
                "MCP request failed stage=%s category=%s binding=%s schema=%s",
                diagnostics["stage"], category, diagnostics["binding"], diagnostics["schema"],
            )
            payload = {
                "jsonrpc": "2.0",
                "id": identifier,
                "error": {
                    "code": -32000,
                    "message": message,
                },
            }
        encoded = json.dumps(payload, separators=(",", ":"))
        if len(encoded.encode()) > 1_048_576:
            return web.Response(status=413)
        return web.Response(text=encoded, content_type="application/json")
