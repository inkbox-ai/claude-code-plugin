import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest


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


def test_candidate_observer_cannot_change_acceptance_or_expose_content(monkeypatch, capsys):
    module = _live_email_module()
    names = ("inkbox_lookup_contact", "inkbox_list_contacts", "inkbox_get_contact",
             "inkbox_create_contact", "inkbox_update_contact", "inkbox_delete_contact")
    secret = "synthetic-private-body-key-address@example.invalid"

    class Messages(_Messages):
        def __init__(self):
            super().__init__()
            self.bodies = {str(i): secret for i in range(12)}
            self.bodies["complete"] = "\n".join(names) + "\n" + secret

        def list(self, _mailbox, direction=None):
            return [SimpleNamespace(id=key, from_address="agent@example.com") for key in self.bodies] if self.sent else []

    observed = []

    def observe(candidate):
        observed.append(candidate)
        module._observe_contact_tool_presence(candidate)

    result = module._ask(SimpleNamespace(messages=Messages()), "agent@example.com", "driver@example.com",
                         "List the exact names of all the Inkbox tools you have access to, one per line.",
                         accept=lambda candidate: all(name in candidate for name in names), observe=observe)
    assert all(name in result for name in names)
    assert len(observed) == 8
    output = capsys.readouterr().out
    assert secret not in output
    rows = [json.loads(line) for line in output.splitlines()]
    assert len(rows) == 8
    assert all(row == {"contact_tool_presence": dict.fromkeys(names, False)} for row in rows)


@pytest.mark.parametrize("accepted", [False, True])
def test_observer_exception_preserves_exact_predicate_and_timeout(monkeypatch, accepted):
    module = _live_email_module()
    clock = SimpleNamespace(now=0.0)
    monkeypatch.setattr(module, "TIMEOUT_S", 2)
    monkeypatch.setattr(module, "POLL_EVERY_S", 1)
    monkeypatch.setattr(module.time, "monotonic", lambda: clock.now)
    monkeypatch.setattr(module.time, "sleep", lambda n: setattr(clock, "now", clock.now + n))
    values = []

    def broken_observer(_candidate):
        raise OSError("synthetic-private-diagnostic-error")

    def accept(candidate):
        values.append(candidate)
        return accepted

    args = (SimpleNamespace(messages=_Messages()), "agent@example.com", "driver@example.com", "Unchanged request")
    if accepted:
        assert module._ask(*args, accept=accept, observe=broken_observer) == "sent — emailed you everything on file."
        assert len(values) == 1 and clock.now == 0
    else:
        with pytest.raises(pytest.fail.Exception, match="no acceptable reply within 2s \\(candidate_count=2\\)"):
            module._ask(*args, accept=accept, observe=broken_observer)
        assert len(values) == 2 and clock.now == 2


def test_presence_vector_uses_only_six_fixed_names(capsys):
    module = _live_email_module()
    module._observe_contact_tool_presence("inkbox_get_contact synthetic-private-key@example.invalid")
    output = capsys.readouterr().out
    assert "synthetic-private" not in output
    fields = json.loads(output)["contact_tool_presence"]
    assert len(fields) == 6 and sum(fields.values()) == 1 and fields["inkbox_get_contact"] is True
