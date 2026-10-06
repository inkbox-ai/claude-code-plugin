"""Optional channel and credential tools for the native in-process MCP server."""

from __future__ import annotations

import os
from uuid import UUID


def build_channel_tools(client, identity_handle, cfg):
    from .tools import tool, _result, _error, _json_safe, CURRENT_SESSION, _to_thread_drained
    from .slack import SLACK_TOOLS, run_tool

    registered = []

    def register(name, description, properties, required, operation):
        async def handler(args):
            if set(args) - set(properties):
                return _error("Unsupported tool arguments")
            try:
                return _result(await _to_thread_drained(operation, args))
            except Exception as exc:
                # Classify failures without exposing provider bodies or credentials.
                message = "Operation unavailable. Check access and local configuration."
                safe = ("Vault is locked", "secret_id must be a UUID", "only login", "no TOTP",
                        "No vault key matched", "not been initialized", "timed out",
                        "The originating turn has ended", "originating conversation", "supplied Companion history")
                for marker in safe:
                    if marker.lower() in str(exc).lower():
                        message = marker
                        break
                if message == "Vault is locked":
                    message = "Vault is locked. Set INKBOX_CLAUDE_VAULT_KEY locally and restart. Never send the key in chat."
                status = getattr(exc, "status_code", None)
                return _error(message, **({"status_code": status} if isinstance(status, int) else {}))
        registered.append(tool(name, description, {
            "type": "object", "properties": properties, "required": required,
            "additionalProperties": False,
        })(handler))

    if cfg.slack_enabled:
        for spec in SLACK_TOOLS:
            def slack(args, name=spec["name"]):
                if not cfg.slack_enabled:
                    raise ValueError("Slack is disabled")
                return run_tool(client, identity_handle, name, args)
            schema = spec["inputSchema"]
            register(spec["name"], spec["description"], schema["properties"], schema["required"], slack)

    def list_secrets(args):
        identity_id = str(client.get_identity(identity_handle).id)
        return [secret for secret in client.vault.list_secrets(secret_type=args.get("secret_type"))
                if any(str(rule.identity_id) == identity_id and str(rule.vault_secret_id) == str(secret.id) for rule in secret.access)]

    register("inkbox_list_vault_secrets", "List this identity's Vault metadata before selecting a credential; values are not decrypted.",
             {"secret_type": {"type": "string", "enum": ["login", "api_key", "key_pair", "ssh_key", "other"]}}, [],
             list_secrets)

    def read_secret(args, *, totp=False):
        try:
            secret_id = str(UUID(str(args.get("secret_id") or "")))
        except ValueError:
            raise ValueError("secret_id must be a UUID") from None
        key = os.getenv("INKBOX_CLAUDE_VAULT_KEY")
        if not key:
            raise ValueError("Vault is locked")
        identity_id = str(client.get_identity(identity_handle).id)
        def authorize():
            if os.getenv("INKBOX_CLAUDE_VAULT_KEY") != key:
                raise ValueError("Vault is locked")
            rules = client.vault.list_access_rules(secret_id)
            if not any(str(rule.identity_id) == identity_id and str(rule.vault_secret_id) == secret_id for rule in rules):
                raise PermissionError("The configured identity cannot access this secret")
        authorize()
        # Never inherit an SDK-global or previously cached unlock: every secret
        # invocation is controlled by the current bridge-local key.
        vault = client.vault.unlock(key, identity_id=identity_id)
        if totp:
            result = vault.get_totp_code(secret_id)
            authorize()
            return result
        secret = _json_safe(vault.get_secret(secret_id))
        authorize()
        if secret.get("secret_type") == "login":
            secret["has_totp"] = secret["payload"].pop("totp", None) is not None
        return secret

    secret_schema = {"secret_id": {"type": "string", "format": "uuid"}}
    register("inkbox_get_vault_secret", "Retrieve one current credential selected from Vault metadata. Requires local INKBOX_CLAUDE_VAULT_KEY; never request that key in chat.",
             secret_schema, ["secret_id"], read_secret)
    register("inkbox_get_totp_code", "Get only a login's current 2FA code and expiry, never its seed. Requires local Vault configuration.",
             secret_schema, ["secret_id"], lambda args: read_secret(args, totp=True))

    if cfg.imessage_threaded_replies:
        for name, identifiers in (("get_imessage_thread", ["message_id"]),
                                  ("get_imessage_conversation_thread", ["conversation_id", "thread_id"])):
            def read_thread(args, method=name, keys=identifiers):
                if not cfg.imessage_threaded_replies:
                    raise ValueError("Native iMessage is disabled")
                session = CURRENT_SESSION.get()
                turn = getattr(session, "_current_turn", None)
                route = dict(turn.reply_meta or {}) if turn is not None else {}
                if route.get("companion"):
                    raise ValueError("Use the supplied Companion history")
                native = bool(turn is not None and turn.mode == "imessage" and route)
                def validate(conversation=None):
                    if not native:
                        return
                    if session._current_turn is not turn or not session._turn_active or session._interrupting:
                        raise ValueError("The originating turn has ended")
                    if conversation is not None and conversation != route.get("conversation_id"):
                        raise ValueError("Native thread reads must stay in the originating conversation")
                validate()
                limit = args.get("limit", 50)
                if type(limit) is not int or not 1 <= limit <= 100:
                    raise ValueError("Invalid page limit")
                identity = client.get_identity(identity_handle)
                validate()
                if native:
                    conversation = (str(identity.get_imessage(args["message_id"]).conversation_id or "")
                                    if method == "get_imessage_thread" else str(args["conversation_id"]))
                    validate(conversation)
                result = getattr(identity, method)(*(args[key] for key in keys), limit=limit, cursor=args.get("cursor"))
                validate()
                return result
            register("inkbox_" + name, "Read one native iMessage thread with bounded pagination. Thread identifiers are opaque; do not substitute message identifiers.",
                     {**{key: {"type": "string", "minLength": 1} for key in identifiers},
                      "limit": {"type": "integer", "minimum": 1, "maximum": 100}, "cursor": {"type": "string"}},
                     identifiers, read_thread)
    return registered
