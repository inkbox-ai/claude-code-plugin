import asyncio
import json
import types

from inkbox_claude import gateway as gateway_module
from inkbox_claude.gateway import InkboxGateway
from inkbox_claude.config import BridgeConfig, VoiceStack


def _payload(call_id="call-1"):
    return {
        "id": f"event-{call_id}",
        "event_type": "call.ended",
        "data": {
            "call": {
                "id": call_id,
                "mode": "hosted_agent",
                "direction": "outbound",
                "remote_phone_number": "+15551112222",
                "reason": "Check the release",
                "status": "completed",
            },
            "contacts": [{"id": "contact-1", "preferred_name": "Dima"}],
            "outcome": "completed",
            "post_call_action_items": [
                {"action": "Send the summary", "status": "open"},
            ],
            "transcript": {
                "entries": [
                    {"party": "remote", "text": "Please send the summary."},
                    {"party": "local", "text": "I will."},
                ],
            },
        },
    }


class _Session:
    def __init__(self, prompts, error=None):
        self.prompts = prompts
        self.error = error

    async def run_consult(self, prompt):
        self.prompts.append(prompt)
        if self.error:
            raise self.error
        return "This plain text must not be delivered"


class _Sessions:
    def __init__(self, prompts, error=None):
        self.prompts = prompts
        self.error = error
        self.chat_ids = []

    def get(self, chat_id):
        self.chat_ids.append(chat_id)
        return _Session(self.prompts, self.error)


class _Inkbox:
    def get_identity(self, _handle):
        return types.SimpleNamespace(list_transcripts=lambda _call_id: [])


class _TranscriptFailureInkbox:
    def get_identity(self, _handle):
        def fail(_call_id):
            raise RuntimeError("transcript endpoint not settled")

        return types.SimpleNamespace(list_transcripts=fail)


def _gateway(tmp_path, *, error=None):
    gateway_module.web = types.SimpleNamespace(json_response=lambda value: value)
    cfg = BridgeConfig(identity="claude", voice_stack=VoiceStack.INKBOX_VOICE_AI)
    gateway = InkboxGateway(cfg)
    gateway._hosted_call_registry_path = tmp_path / "hosted.json"
    gateway._inkbox = _Inkbox()
    prompts = []
    gateway.sessions = _Sessions(prompts, error)
    return gateway, prompts


async def _drain(gateway):
    await asyncio.sleep(0)
    tasks = list(gateway._hosted_call_jobs.values())
    if tasks:
        await asyncio.gather(*tasks)


def test_hosted_completion_runs_once_and_suppresses_plain_text(tmp_path):
    async def scenario():
        gateway, prompts = _gateway(tmp_path)
        await gateway._on_hosted_call_ended(_payload())
        await _drain(gateway)
        assert len(prompts) == 1
        assert "Remote party phone number: +15551112222" in prompts[0]
        assert "Contact memories are background only" in prompts[0]
        assert "inkbox_send_sms" in prompts[0]
        assert "`to` set to that exact remote number" in prompts[0]
        assert "Do not finish until each required tool reports success" in prompts[0]
        assert "Send the summary" in prompts[0]
        assert gateway.sessions.chat_ids == ["contact-1"]
        assert json.loads(gateway._hosted_call_registry_path.read_text())["call-1"]["state"] == "completed"

        await gateway._on_hosted_call_ended(_payload())
        await _drain(gateway)
        assert len(prompts) == 1

    asyncio.run(scenario())


def test_hosted_completion_registry_is_private_bounded_and_drops_completed_payload(tmp_path):
    async def scenario():
        gateway, _ = _gateway(tmp_path)
        payload = _payload()
        payload["data"]["contacts"][0]["memories"] = ["private" * 20_000]
        payload["data"]["transcript"]["entries"][0]["text"] = "secret" * 100_000
        payload["data"]["post_call_action_items"] = [
            {
                "id": f"action-{index}",
                "action": "a" * 10_000,
                "details": "d" * 20_000,
                "status": "open",
            }
            for index in range(150)
        ]
        await gateway._on_hosted_call_ended(payload)
        queued = json.loads(
            gateway._hosted_call_registry_path.read_text()
        )["call-1"]
        replay = queued["payload"]["data"]
        assert "transcript" not in replay
        assert "memories" not in replay["contacts"][0]
        assert len(replay["post_call_action_items"]) == 100
        assert len(replay["post_call_action_items"][0]["action"]) == 4_000
        assert len(replay["post_call_action_items"][0]["details"]) == 8_000
        assert gateway._hosted_call_registry_path.stat().st_mode & 0o777 == 0o600

        await _drain(gateway)
        entry = json.loads(gateway._hosted_call_registry_path.read_text())["call-1"]
        assert entry["state"] == "completed"
        assert "payload" not in entry

    asyncio.run(scenario())


def test_full_transcript_failure_uses_inline_webhook_transcript(tmp_path):
    async def scenario():
        gateway, prompts = _gateway(tmp_path)
        gateway._inkbox = _TranscriptFailureInkbox()
        await gateway._on_hosted_call_ended(_payload())
        await _drain(gateway)
        assert "Please send the summary." in prompts[0]
        assert json.loads(gateway._hosted_call_registry_path.read_text())["call-1"]["state"] == "completed"

    asyncio.run(scenario())


def test_failed_completion_is_recovered_after_restart(tmp_path):
    async def scenario():
        first, _ = _gateway(tmp_path, error=RuntimeError("Claude unavailable"))
        await first._on_hosted_call_ended(_payload())
        await _drain(first)
        assert json.loads(first._hosted_call_registry_path.read_text())["call-1"]["state"] == "failed"

        restarted, prompts = _gateway(tmp_path)
        await restarted._recover_hosted_call_completions()
        await _drain(restarted)
        assert len(prompts) == 1
        assert json.loads(restarted._hosted_call_registry_path.read_text())["call-1"]["state"] == "completed"

    asyncio.run(scenario())


def test_recovery_remains_retryable_when_authoritative_transcript_fails(tmp_path):
    async def scenario():
        first, _ = _gateway(tmp_path, error=RuntimeError("Claude unavailable"))
        await first._on_hosted_call_ended(_payload())
        await _drain(first)

        unsettled, prompts = _gateway(tmp_path)
        unsettled._inkbox = _TranscriptFailureInkbox()
        await unsettled._recover_hosted_call_completions()
        await _drain(unsettled)
        entry = json.loads(
            unsettled._hosted_call_registry_path.read_text()
        )["call-1"]
        assert entry["state"] == "failed"
        assert prompts == []

        settled, prompts = _gateway(tmp_path)
        await settled._recover_hosted_call_completions()
        await _drain(settled)
        entry = json.loads(settled._hosted_call_registry_path.read_text())["call-1"]
        assert entry["state"] == "completed"
        assert len(prompts) == 1

    asyncio.run(scenario())


def test_non_hosted_call_does_not_start_completion_turn(tmp_path):
    async def scenario():
        gateway, prompts = _gateway(tmp_path)
        payload = _payload()
        payload["data"]["call"]["mode"] = "client_websocket"
        await gateway._on_hosted_call_ended(payload)
        await _drain(gateway)
        assert prompts == []
        assert not gateway._hosted_call_registry_path.exists()

    asyncio.run(scenario())
