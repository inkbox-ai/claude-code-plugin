import importlib.util
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace


def _live_email_module():
    path = Path(__file__).parent / "live" / "test_email_intelligence.py"
    spec = importlib.util.spec_from_file_location("live_email_intelligence_helper", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Messages:
    def __init__(self):
        self.sent = False
        self.bodies = {
            "confirmation": "Sent — emailed you everything on file.",
            "details": "Ada Lovelace, ada@example.com, +1 555 111 2222",
        }

    def list(self, _mailbox, direction=None):
        if not self.sent:
            return []
        return [
            SimpleNamespace(
                id="confirmation",
                thread_id="request-thread",
                from_address="agent@example.com",
                subject="Re: request",
            ),
            SimpleNamespace(
                id="details",
                thread_id="tool-thread",
                from_address="agent@example.com",
                subject="Your details",
            ),
        ]

    def send(self, _mailbox, **kwargs):
        self.sent = True
        return SimpleNamespace(thread_id="request-thread")

    def get(self, _mailbox, message_id):
        return SimpleNamespace(body_text=self.bodies[message_id])


def test_ask_accepts_separate_tool_email_after_generic_confirmation(monkeypatch):
    live_email = _live_email_module()
    monkeypatch.setattr(live_email, "POLL_EVERY_S", 0)
    inkbox = ModuleType("inkbox")
    mail = ModuleType("inkbox.mail")
    mail_types = ModuleType("inkbox.mail.types")
    mail_types.MessageDirection = SimpleNamespace(INBOUND="inbound")
    monkeypatch.setitem(sys.modules, "inkbox", inkbox)
    monkeypatch.setitem(sys.modules, "inkbox.mail", mail)
    monkeypatch.setitem(sys.modules, "inkbox.mail.types", mail_types)
    remote = SimpleNamespace(messages=_Messages())

    body = live_email._ask(
        remote,
        "agent@example.com",
        "driver@example.com",
        "Who am I?",
        accept=lambda candidate: (
            "ada lovelace" in candidate and "+1 555 111 2222" in candidate
        ),
    )

    assert body == "ada lovelace, ada@example.com, +1 555 111 2222"
