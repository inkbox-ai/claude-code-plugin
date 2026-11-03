"""Native Bash foreground/background semantics against a localhost-only model."""

import asyncio
import json
import threading
import time

import pytest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from claude_agent_sdk import ClaudeAgentOptions, ClaudeSDKClient, ResultMessage


class Mock(BaseHTTPRequestHandler):
    background = False
    results = []

    def log_message(self, *args):
        pass

    def do_POST(self):
        req = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))))
        if self.path.rstrip("/").endswith("/count_tokens"):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"input_tokens":1}')
            return
        seen = []
        for msg in req.get("messages", []):
            for b in (
                msg.get("content", []) if isinstance(msg.get("content"), list) else []
            ):
                if (
                    b.get("type") == "tool_result"
                    and b.get("tool_use_id") == "toolu_wait"
                ):
                    seen.append(b)
        if seen:
            txt = json.dumps(seen[-1].get("content", "")).lower()
            self.results.append(
                {
                    "is_error": bool(seen[-1].get("is_error")),
                    "background_notice": "background" in txt,
                    "finished_marker": "wait_finished" in txt,
                }
            )
            block = {"type": "text", "text": "Synthetic final result."}
            stop = "end_turn"
        else:
            block = {
                "type": "tool_use",
                "id": "toolu_wait",
                "name": "Bash",
                "input": {
                    "command": "python3 -c 'import time; time.sleep(3); print(\"WAIT_FINISHED\")'",
                    "description": "Run isolated three-second timing probe",
                    "timeout": 10000,
                    "run_in_background": self.background,
                },
            }
            stop = "tool_use"
        msg = {
            "id": "msg_synthetic",
            "type": "message",
            "role": "assistant",
            "model": req.get("model", "mock"),
            "content": [block],
            "stop_reason": stop,
            "stop_sequence": None,
            "usage": {"input_tokens": 1, "output_tokens": 1},
        }
        self.send_response(200)
        if req.get("stream"):
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()

            def event(n, d):
                self.wfile.write(
                    ("event: " + n + "\ndata: " + json.dumps(d) + "\n\n").encode()
                )

            event(
                "message_start",
                {
                    "type": "message_start",
                    "message": {**msg, "content": [], "stop_reason": None},
                },
            )
            start = dict(block)
            if block["type"] == "tool_use":
                start["input"] = {}
            else:
                start["text"] = ""
            event(
                "content_block_start",
                {"type": "content_block_start", "index": 0, "content_block": start},
            )
            delta = (
                {"type": "input_json_delta", "partial_json": json.dumps(block["input"])}
                if block["type"] == "tool_use"
                else {"type": "text_delta", "text": block["text"]}
            )
            event(
                "content_block_delta",
                {"type": "content_block_delta", "index": 0, "delta": delta},
            )
            event("content_block_stop", {"type": "content_block_stop", "index": 0})
            event(
                "message_delta",
                {
                    "type": "message_delta",
                    "delta": {"stop_reason": stop, "stop_sequence": None},
                    "usage": {"output_tokens": 1},
                },
            )
            event("message_stop", {"type": "message_stop"})
            self.wfile.flush()
        else:
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(msg).encode())


async def run(background, tmp_path):
    class Scenario(Mock):
        pass

    Scenario.background = background
    Scenario.results = []
    srv = ThreadingHTTPServer(("127.0.0.1", 0), Scenario)
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    tmp = str(tmp_path)
    client = ClaudeSDKClient(
        options=ClaudeAgentOptions(
            cwd=tmp,
            setting_sources=[],
            allowed_tools=["Bash"],
            system_prompt={"type": "preset", "preset": "claude_code"},
            env={
                "CLAUDE_CONFIG_DIR": tmp + "/config",
                "ANTHROPIC_API_KEY": "synthetic-local-only",
                "ANTHROPIC_AUTH_TOKEN": "",
                "ANTHROPIC_BASE_URL": f"http://127.0.0.1:{srv.server_port}",
                "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
                "DISABLE_AUTOUPDATER": "1",
            },
        )
    )
    try:
        await client.connect()
        start = time.monotonic()
        await client.query("Run the synthetic isolated wait probe.")
        async for msg in client.receive_response():
            if isinstance(msg, ResultMessage):
                assert not msg.is_error
                assert Scenario.results == [
                    {
                        "is_error": False,
                        "background_notice": background,
                        "finished_marker": not background,
                    }
                ]
                if not background:
                    assert time.monotonic() - start >= 3
                return
        raise AssertionError("Native host ended without a terminal result")
    finally:
        try:
            await client.disconnect()
        finally:
            srv.shutdown()
            srv.server_close()
            thread.join(timeout=2)


@pytest.mark.parametrize("background", [False, True])
def test_native_bash_foreground_waits_for_result_while_background_can_finish_turn(
    tmp_path, background
):
    asyncio.run(asyncio.wait_for(run(background, tmp_path), 40))
