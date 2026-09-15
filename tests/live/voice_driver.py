"""Live voice-call driver: the peer on the other end of a real phone call.

Opens an Inkbox tunnel for the driver identity, serves the call-media WebSocket
behind it, and bridges audio in Inkbox STT/TTS mode (text frames only — no local
model). It speaks one scripted line so the agent under test gets a turn, and the
call transcript (read separately by the test) proves the agent replied.

Run as a standalone process alongside the gateway. On startup it writes a small
JSON state file (its public WS URL + phone-number id) that the test reads to place
or expect a call. Two call directions are supported by the same bridge:
  * the test places a call to the agent and passes this driver's WS URL, or
  * the agent calls this driver's number, which is set to auto-accept onto the
    same WS URL.

Env:
  REMOTE_INKBOX_API_KEY   driver identity key (identity-scoped)
  INKBOX_BASE_URL         API root (default https://inkbox.ai)
  VOICE_DRIVER_PORT       local port the tunnel forwards to (default 8090)
  VOICE_DRIVER_STATE      path to write the JSON state file
  VOICE_DRIVER_LINE       the one line the driver speaks (default below)
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from pathlib import Path

import uvicorn
from fastapi import FastAPI, WebSocket
from starlette.websockets import WebSocketState

from inkbox import Inkbox
from inkbox.tunnels.client import connect as tunnel_connect

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s driver %(message)s")
log = logging.getLogger("voice_driver")

API_KEY = os.environ["REMOTE_INKBOX_API_KEY"]
BASE_URL = os.environ.get("INKBOX_BASE_URL", "https://inkbox.ai")
PORT = int(os.environ.get("VOICE_DRIVER_PORT", "8090"))
STATE_FILE = os.environ.get("VOICE_DRIVER_STATE", "/tmp/voice_driver_state.json")
LINE_FILE = os.environ.get("VOICE_DRIVER_LINE_FILE", "")
LINE = (
    Path(LINE_FILE).read_text(encoding="utf-8").strip()
    if LINE_FILE
    else os.environ.get(
        "VOICE_DRIVER_LINE",
        "Hi, this is a quick test call. Please reply out loud with one short sentence, then say goodbye.",
    )
)
# Answering-machine detection scores whoever answers: a greeting longer than the
# carrier's `greeting_duration_millis` (3.5s) reads as a voicemail announcement
# and the call is hung up before the agent ever speaks. Answer the way a person
# does - one word, then silence - and hold the prompt until that window closes.
GREETING = os.environ.get("VOICE_DRIVER_GREETING", "Hello?")
# Delay before the first ask. A greeting arrives as several final transcripts
# 1.5-5s apart, so no silence threshold tells "between greeting sentences" from
# "greeting over" — the first ask is simply allowed to land wherever it lands, and
# _run_turn re-asks once the agent is actually idle.
SPEAK_AFTER_S = float(os.environ.get("VOICE_DRIVER_SPEAK_AFTER", "5"))
# Then give the agent a turn and hang up — a dropped WS does NOT end the call, so we
# must send an explicit stop or the leg lingers until the server max-duration cap.
LISTEN_S = float(os.environ.get("VOICE_DRIVER_LISTEN", "12"))
# Re-ask the question this often while the agent is idle. An ask the greeting
# talked over is otherwise never repeated and the call idles out with the agent
# still waiting for a request. 0 disables re-asking.
REASK_EVERY_S = float(os.environ.get("VOICE_DRIVER_REASK", "20"))
# Never re-ask until the agent has been silent this long, so a reply or a tool
# round-trip in progress is never talked over.
QUIET_GAP_S = float(os.environ.get("VOICE_DRIVER_QUIET_GAP", "6"))
MAX_REASKS = int(os.environ.get("VOICE_DRIVER_MAX_REASKS", "2"))
# The agent saying this back means the question landed; stop re-asking so a
# question that already took effect never turns into a second one.
ANSWER_CONTAINS = os.environ.get("VOICE_DRIVER_ANSWER_CONTAINS", "")


def _speech_key(text: str) -> str:
    """Compare speech ignoring ASR casing, spacing and punctuation."""
    return "".join(char for char in text.casefold() if char.isalnum())

app = FastAPI()


@app.get("/health")
async def health() -> dict:
    return {"ok": True}


@app.websocket("/phone/media/ws")
async def phone_media_ws(ws: WebSocket) -> None:
    """Accept the call-media WS in Inkbox STT/TTS mode and run one scripted turn."""
    import asyncio

    # Opt into Inkbox-managed speech both ways → we exchange text, not audio.
    await ws.accept(headers=[
        (b"x-use-inkbox-text-to-speech", b"true"),
        (b"x-use-inkbox-speech-to-text", b"true"),
    ])
    log.info("call WS accepted")
    loop = asyncio.get_event_loop()
    answered = asyncio.Event()        # agent said the expected answer back
    state = {"last_heard": 0.0}       # monotonic ts of the agent's most recent turn
    answer_key = _speech_key(ANSWER_CONTAINS)
    convo: asyncio.Task | None = None

    async def _say(text: str) -> None:
        await ws.send_text(json.dumps({"event": "text", "delta": text}))
        await ws.send_text(json.dumps({"event": "text", "done": True}))
        log.info("spoke: %s", text)

    async def _run_turn() -> None:
        # Speak one line, give the agent a turn, then hang up so the call ends fast.
        await _say(GREETING)
        await asyncio.sleep(SPEAK_AFTER_S)
        await _say(LINE)
        asked_at = loop.time()
        state["last_heard"] = asked_at
        # Re-ask if the agent never got the question: the greeting routinely runs
        # several seconds past our first ask, and a lost ask leaves the agent
        # waiting while the call idles out. Re-ask ONLY once the agent has gone
        # quiet and has not already answered, so neither an in-progress reply nor
        # a question that already landed is spoken over or repeated.
        started = loop.time()
        reasks = 0
        while loop.time() - started < LISTEN_S:
            await asyncio.sleep(1.0)
            if (
                REASK_EVERY_S > 0
                and not answered.is_set()
                and reasks < MAX_REASKS
                and loop.time() - asked_at >= REASK_EVERY_S
                and loop.time() - state["last_heard"] >= QUIET_GAP_S
            ):
                await _say(LINE)
                asked_at = loop.time()
                reasks += 1
        try:
            await ws.send_text(json.dumps({"event": "stop"}))
            log.info("sent stop (hangup)")
        except Exception:
            pass

    try:
        while True:
            raw = await ws.receive_text()
            ev = json.loads(raw)
            kind = ev.get("event")
            if kind == "start":
                log.info("call start: %s", ev.get("stream_id"))
                convo = asyncio.create_task(_run_turn())
            elif kind == "transcript" and ev.get("is_final"):
                text = ev.get("text") or ""
                log.info("heard (final): %s", text)
                state["last_heard"] = loop.time()  # agent is actively talking
                if answer_key and answer_key in _speech_key(text):
                    answered.set()
            elif kind == "stop":
                log.info("call stop: %s", ev.get("reason"))
                break
    except Exception as exc:  # noqa: BLE001 — never let the bridge crash the process
        log.info("WS loop ended: %r", exc)
    finally:
        if convo:
            convo.cancel()
        if ws.client_state != WebSocketState.DISCONNECTED:
            try:
                await ws.close()
            except Exception:
                pass


def _run_uvicorn() -> uvicorn.Server:
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=PORT, log_level="warning"))
    threading.Thread(target=server.run, name="uvicorn", daemon=True).start()
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if server.started:
            return server
        time.sleep(0.05)
    raise RuntimeError("uvicorn did not start")


def main() -> None:
    client = Inkbox(api_key=API_KEY, base_url=BASE_URL)
    handle = client.mailboxes.list()[0].email_address.split("@", 1)[0]   # tunnel name = handle
    num = client.phone_numbers.list()[0]
    log.info("driver identity %s number %s", handle, num.number)

    server = _run_uvicorn()

    listener = tunnel_connect(
        client, name=handle, forward_to=f"http://127.0.0.1:{PORT}",
        state_dir=f"/tmp/inkbox-tunnel-{handle}",
    )
    public_host = listener.tunnel.public_host
    ws_url = f"wss://{public_host}/phone/media/ws"
    log.info("tunnel ready: %s", ws_url)

    # Auto-accept inbound calls (agent → driver) straight onto this WS.
    client.phone_numbers.update(num.id, incoming_call_action="auto_accept", client_websocket_url=ws_url)

    Path(STATE_FILE).write_text(json.dumps({
        "ws_url": ws_url, "number": num.number, "number_id": str(num.id), "handle": handle,
    }))
    log.info("state written to %s", STATE_FILE)

    try:
        listener.wait()
    finally:
        # auto_reject is the driver's known idle baseline. Restoring an
        # arbitrary previous value can preserve a stale auto-accept setting,
        # causing earlier call-placement probes to remain active and collide
        # with the next voice run.
        try:
            client.phone_numbers.update(num.id, incoming_call_action="auto_reject")
        except Exception as exc:  # noqa: BLE001
            log.info("number revert failed: %r", exc)
        listener.close()
        server.should_exit = True


if __name__ == "__main__":
    main()
