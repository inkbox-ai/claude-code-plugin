"""Deterministic Anthropic-API mock for live agent tests.

Claude Code honours ``ANTHROPIC_BASE_URL``, so pointing the bridged sessions at
this server makes the agent "think" here instead of against the real API: no
real key, no tokens, no flakiness, fully deterministic. We still exercise the
entire real pipeline (bridge, tunnel, inbound routing, Claude Code session,
Inkbox send + delivery) — only the LLM brain is faked.

Every reply contains ``REPLY_OK`` plus, when present, the inbound's smoke nonce,
so a live test can assert the canned content travelled inbound → model → reply →
delivery end to end (and that the agent did NOT fall back to an error message).

Serves the Messages API (``POST /v1/messages``, streaming and not) and the
token-count endpoint. Run: ``python mock_anthropic.py [port]`` (default 8089).
Stdlib only.
"""

from __future__ import annotations

import json
import re
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

_NONCE = re.compile(r"smoke-[0-9a-f]{6,}")


def _reply_text(req: dict) -> str:
    m = _NONCE.search(json.dumps(req))
    tag = m.group(0) if m else "no-nonce"
    return f"REPLY_OK {tag} — automated reachability reply from the agent."


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_args):  # quiet
        pass

    def _send_json(self, code: int, obj: dict) -> None:
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802  (health / probes)
        self._send_json(200, {"ok": True})

    def _sse(self, event: str, data: dict) -> None:
        self.wfile.write(f"event: {event}\ndata: {json.dumps(data)}\n\n".encode())

    def do_POST(self):  # noqa: N802
        n = int(self.headers.get("Content-Length") or 0)
        try:
            req = json.loads(self.rfile.read(n) or b"{}")
        except ValueError:
            req = {}

        if self.path.rstrip("/").endswith("/count_tokens"):
            self._send_json(200, {"input_tokens": 1})
            return

        text = _reply_text(req)
        model = req.get("model", "mock-model")
        usage = {"input_tokens": 1, "output_tokens": 1}
        if req.get("stream"):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            self._sse("message_start", {"type": "message_start", "message": {
                "id": "msg_mock", "type": "message", "role": "assistant", "model": model,
                "content": [], "stop_reason": None, "stop_sequence": None, "usage": usage,
            }})
            self._sse("content_block_start", {"type": "content_block_start", "index": 0,
                                              "content_block": {"type": "text", "text": ""}})
            self._sse("content_block_delta", {"type": "content_block_delta", "index": 0,
                                              "delta": {"type": "text_delta", "text": text}})
            self._sse("content_block_stop", {"type": "content_block_stop", "index": 0})
            self._sse("message_delta", {"type": "message_delta",
                                        "delta": {"stop_reason": "end_turn", "stop_sequence": None},
                                        "usage": {"output_tokens": 1}})
            self._sse("message_stop", {"type": "message_stop"})
            self.wfile.flush()
        else:
            self._send_json(200, {
                "id": "msg_mock", "type": "message", "role": "assistant", "model": model,
                "content": [{"type": "text", "text": text}],
                "stop_reason": "end_turn", "stop_sequence": None, "usage": usage,
            })


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8089
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
