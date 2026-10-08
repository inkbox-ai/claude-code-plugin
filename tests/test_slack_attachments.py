"""Local file delivery uses the SDK, bounded reads, and the original tool owner."""

import asyncio
import base64
import json
import os
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest

from inkbox_claude.channel_tools import build_channel_tools
from inkbox_claude.config import BridgeConfig
from inkbox_claude.slack import file_payload, run_tool, SLACK_MAX_UPLOAD_BYTES
from inkbox_claude.tools import CURRENT_SESSION
from tests.test_slack import CONNECTION, IDENTITY, connected_client
from tests.test_sessions import make_session, _Turn


def client():
    sdk = connected_client()
    sdk.get_identity.return_value.id = IDENTITY
    sdk.slack.upload_file.return_value = NS(id="operation-example", connection_id=CONNECTION,
        operation="file_upload", status="succeeded", file_id="F_TEST", conversation_id="C_TEST")
    return sdk


def args(path):
    return {"connection_id": CONNECTION, "conversation_id": "C_TEST", "thread_ts": "1234567890.000001",
            "file_path": str(path), "idempotency_key": "file-example"}


def test_upload_preserves_bytes_and_explicit_route_and_returns_only_receipt(tmp_path):
    path = tmp_path / "chart.png"
    data = b"\x89PNG\r\n\x00example"
    path.write_bytes(data)
    sdk = client()
    result = run_tool(sdk, "example", "inkbox_slack_upload_file", {
        **args("chart.png"), "title": "Chart", "initial_comment": "*Results*"}, local_root=tmp_path)
    sdk.slack.upload_file.assert_called_once_with(CONNECTION, conversation_id="C_TEST",
        thread_ts="1234567890.000001", idempotency_key="file-example", filename="chart.png",
        content_base64=base64.b64encode(data).decode(), title="Chart", initial_comment="*Results*")
    assert result["file_id"] == "F_TEST" and result["status"] == "succeeded"
    assert "content_base64" not in result and "file_path" not in result


@pytest.mark.parametrize("filename", ["../secret", "bad/name", "bad\\name", "bad\nname", "..", "x" * 256])
def test_invalid_filename_never_reads_or_uploads(tmp_path, filename):
    sdk = client()
    with pytest.raises(ValueError):
        run_tool(sdk, "example", "inkbox_slack_upload_file", {**args(tmp_path / "absent"), "filename": filename})
    sdk.slack.upload_file.assert_not_called()


@pytest.mark.parametrize("size", [0, SLACK_MAX_UPLOAD_BYTES + 1])
def test_empty_or_large_file_is_rejected(tmp_path, size):
    path = tmp_path / "file.bin"
    with path.open("wb") as stream:
        stream.truncate(size)
    with pytest.raises(ValueError):
        file_payload(str(path))


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="Named pipes unavailable")
def test_special_file_is_rejected_without_waiting_for_a_writer(tmp_path):
    path = tmp_path / "pipe"
    os.mkfifo(path)
    with pytest.raises(ValueError):
        file_payload(str(path))


@pytest.mark.parametrize("status", ["in_progress", "unknown", "failed"])
def test_unconfirmed_upload_returns_inspection_coordinates_without_retry(tmp_path, status):
    path = tmp_path / "file.txt"
    path.write_text("example")
    sdk = client()
    sdk.slack.upload_file.return_value.status = status
    result = run_tool(sdk, "example", "inkbox_slack_upload_file", args(path))
    assert result["status"] == status and result["id"] == "operation-example"
    sdk.slack.upload_file.assert_called_once()


def test_disconnected_duplicate_or_cross_identity_connection_cannot_upload(tmp_path):
    path = tmp_path / "file.txt"
    path.write_text("example")
    for variation in ("disconnected", "duplicate", "identity"):
        sdk = client()
        connection = sdk.slack.list_connections.return_value.connections[0]
        if variation == "disconnected":
            connection.status = "disconnected"
        elif variation == "duplicate":
            sdk.slack.list_connections.return_value.connections.append(connection)
        else:
            connection.identity_id = "other-identity"
        with pytest.raises(ValueError):
            run_tool(sdk, "example", "inkbox_slack_upload_file", args(path))
        sdk.slack.upload_file.assert_not_called()


def test_cancelled_owner_cannot_upload_after_file_preparation(tmp_path, monkeypatch):
    from inkbox_claude import slack
    async def scenario():
        sdk = client()
        session = make_session([])
        owner = _Turn("Upload", mode="slack", reply_meta={"conversation_id": "C_TEST"})
        session._current_turn = owner
        session._turn_active = True
        token = CURRENT_SESSION.set(session)
        original = slack.file_payload
        def prepare(*values, **options):
            payload = original(*values, **options)
            owner.cancelled = True
            session._current_turn = _Turn("New request", mode="slack")
            return payload
        monkeypatch.setattr(slack, "file_payload", prepare)
        path = tmp_path / "file.txt"
        path.write_text("example")
        try:
            tool = next(item for item in build_channel_tools(sdk, "example", BridgeConfig(slack_enabled=True))
                        if item.name == "inkbox_slack_upload_file")
            result = await tool.handler(args(path))
            body = json.loads(result["content"][0]["text"])
            assert "originating turn has ended" in body["error"].lower()
            sdk.slack.upload_file.assert_not_called()
        finally:
            CURRENT_SESSION.reset(token)
    asyncio.run(scenario())


def test_connection_is_rechecked_after_file_preparation(tmp_path, monkeypatch):
    from inkbox_claude import slack
    sdk = client()
    original = slack.file_payload
    def prepare(*values, **options):
        payload = original(*values, **options)
        sdk.slack.list_connections.return_value.connections[0].status = "disconnected"
        return payload
    monkeypatch.setattr(slack, "file_payload", prepare)
    path = tmp_path / "file.txt"
    path.write_text("example")
    with pytest.raises(PermissionError):
        run_tool(sdk, "example", "inkbox_slack_upload_file", args(path))
    sdk.slack.upload_file.assert_not_called()


@pytest.mark.parametrize("status", ["disconnected", "reauthorization_required"])
def test_owned_operation_remains_inspectable_after_disconnect(status):
    sdk = client()
    sdk.slack.list_connections.return_value.connections[0].status = status
    sdk.slack.get_operation.return_value = NS(id="operation-example", status="unknown", operation="file_upload")
    receipt = run_tool(sdk, "example", "inkbox_slack_get_operation",
                       {"connection_id": CONNECTION, "operation_id": "operation-example"})
    assert receipt["status"] == "unknown"
    sdk.slack.get_operation.assert_called_once_with(CONNECTION, "operation-example")


def test_real_published_sdk_upload_and_operation_lookup(tmp_path):
    import httpx
    from inkbox import Inkbox
    sdk = Inkbox(api_key="synthetic-key", base_url="https://api.example")
    requests = []
    def handle(request):
        requests.append(request)
        if request.url.path.endswith("/connections"):
            return httpx.Response(200, json={"connections": [{"id": CONNECTION, "identity_id": IDENTITY,
                "workspace_id": "T_TEST", "workspace_name": "Example", "bot_user_id": "U_BOT", "scopes": [],
                "status": "connected", "created_at": "2026-01-01T00:00:00Z"}], "installation_available": False})
        return httpx.Response(200, json={"id": "00000000-0000-4000-8000-000000000003",
            "connection_id": CONNECTION, "operation": "file_upload", "status": "succeeded",
            "conversation_id": "C_TEST", "file_id": "F_TEST"})
    sdk._api_http._client.close()
    sdk._api_http._client = httpx.Client(base_url="https://api.example/api/v1", transport=httpx.MockTransport(handle))
    sdk.get_identity = Mock(return_value=NS(id=IDENTITY))
    path = tmp_path / "file.txt"
    path.write_text("example")
    try:
        result = run_tool(sdk, "example", "inkbox_slack_upload_file", args(path))
        assert result["status"] == "succeeded"
        result = run_tool(sdk, "example", "inkbox_slack_get_operation",
                          {"connection_id": CONNECTION, "operation_id": result["id"]})
        assert result["file_id"] == "F_TEST"
        uploads = [request for request in requests if request.method == "POST"]
        assert len(uploads) == 1 and uploads[0].headers["Idempotency-Key"] == "file-example"
        assert json.loads(uploads[0].content)["content_base64"] == base64.b64encode(b"example").decode()
    finally:
        sdk.close()
