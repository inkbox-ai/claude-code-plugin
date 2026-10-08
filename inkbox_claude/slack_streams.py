"""Optional task streams through the public authenticated SDK surface."""

from dataclasses import dataclass
from uuid import UUID


@dataclass(frozen=True)
class StreamOperation:
    id: str
    status: str
    message_ts: str | None
    error_code: str | None
    retry_after: int | None


class SlackTaskStreams:
    def __init__(self, resource):
        self.resource = resource
        methods = ("start_stream", "append_stream", "stop_stream", "get_operation_by_key")
        self.supported = all(callable(getattr(type(resource), name, None)) for name in methods)
        self.lookup_supported = callable(getattr(type(resource), "get_operation_by_key", None))

    def capable(self, route):
        # Inline conversations stay inline, including when streams are available.
        if not self.supported or not all(route.get(k) for k in ("thread_ts", "actor_id", "workspace_id", "recipient_team_id")):
            return False
        try:
            feature = self.resource.capabilities(route["connection_id"]).capabilities.get("task_streaming")
            return feature is not None and feature.scopes_satisfied is True
        except Exception:
            return False  # No write occurred, so ordinary progress remains safe.

    @staticmethod
    def _parse(raw, route, kind):
        if not isinstance(raw, dict):
            raw = vars(raw)
        if (raw.get("operation") != kind
                or str(raw.get("connection_id")) != str(route["connection_id"])
                or raw.get("conversation_id") != route["conversation_id"]
                or raw.get("thread_ts") is not None and raw["thread_ts"] != route.get("thread_ts")
                or raw.get("status") not in {"in_progress", "succeeded", "failed", "unknown"}):
            raise ValueError("Invalid task-stream operation")
        timestamp = raw.get("message_ts")
        if raw["status"] == "succeeded" and (not isinstance(timestamp, str) or not timestamp):
            raise ValueError("Unconfirmed task-stream message")
        return StreamOperation(str(UUID(str(raw.get("id")))), raw["status"], timestamp,
                               raw.get("error_code"), raw.get("retry_after"))

    def write(self, route, *, kind, key, chunks, stream_id=None):
        if not self.supported:
            raise RuntimeError("Task streams require an SDK with the complete stream interface")
        args = (route["connection_id"], route["conversation_id"])
        kwargs = {"chunks": chunks, "idempotency_key": key}
        if kind == "stream_start":
            result = self.resource.start_stream(*args, **kwargs, thread_ts=route["thread_ts"],
                recipient_user_id=route["actor_id"], recipient_team_id=route["recipient_team_id"],
                task_display_mode="timeline")
        elif kind in {"stream_append", "stream_stop"}:
            method = self.resource.append_stream if kind == "stream_append" else self.resource.stop_stream
            result = method(*args, str(UUID(str(stream_id))), **kwargs)
        else:
            raise ValueError("Invalid task-stream operation kind")
        return self._parse(result, route, kind)

    def lookup(self, route, *, kind, key):
        return self._parse(self.resource.get_operation_by_key(route["connection_id"],
            idempotency_key=key), route, kind)


def tool_progress(tool_name):
    """Use fixed labels only; never expose arguments, paths, or worker IDs."""
    name = str(tool_name).removeprefix("mcp__inkbox__")
    return {
        "Read": "Reading information…", "Glob": "Finding files…", "Grep": "Searching files…",
        "Bash": "Running a command…", "Edit": "Updating files…", "Write": "Writing a file…",
        "WebSearch": "Searching the web…", "WebFetch": "Reading a web page…",
        "Task": "Delegating work…", "Agent": "Delegating work…",
        "inkbox_a2a_call": "Delegating work…",
        "inkbox_a2a_check": "Checking delegated work…",
        "inkbox_list_a2a_tasks": "Checking delegated work…",
        "inkbox_list_a2a_messages": "Reading delegated updates…",
        "inkbox_a2a_reply": "Updating delegated work…",
        "inkbox_slack_upload_file": "Uploading an attachment…",
        "inkbox_slack_list_messages": "Reading Slack messages…",
        "inkbox_slack_search": "Searching Slack…",
    }.get(name, "Using a tool…")
