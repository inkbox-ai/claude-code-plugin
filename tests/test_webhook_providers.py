import asyncio
import hashlib
import hmac
import json
import types

import pytest

from inkbox_claude import gateway as gateway_mod
from inkbox_claude import webhook_providers as wp
from inkbox_claude.webhook_providers import inkbox as inkbox_provider_mod
from inkbox_claude.config import BridgeConfig
from inkbox_claude.gateway import InkboxGateway


class _FakeResponse:
    def __init__(self, *, status=200, text=""):
        self.status = status
        self.text = text


class _FakeRequest:
    def __init__(self, body, headers=None, *, request_id="req-wp-1"):
        self._body = body
        self.headers = {"X-Inkbox-Request-Id": request_id, **(headers or {})}
        self.url = "https://agent.example/webhook"

    async def read(self):
        return self._body


@pytest.fixture(autouse=True)
def fake_web(monkeypatch):
    def json_response(payload):
        return _FakeResponse(status=200, text=json.dumps(payload))

    monkeypatch.setattr(
        gateway_mod,
        "web",
        types.SimpleNamespace(Response=_FakeResponse, json_response=json_response),
    )


def _gateway(*, require_signature, external_events_enabled, monkeypatch=None):
    gw = InkboxGateway(BridgeConfig(
        require_signature=require_signature,
        external_events_enabled=external_events_enabled,
        signing_key="whsec_test",
    ))
    gw._external_wakes = []

    async def _capture(envelope, request_id="", verified=False):
        gw._external_wakes.append((envelope, verified))
        return gateway_mod.web.json_response({"ok": True})

    if monkeypatch is not None:
        monkeypatch.setattr(gw, "_on_external_event", _capture)
    else:
        gw._on_external_event = _capture
    return gw


# --- registry ------------------------------------------------------------

def test_providers_are_auto_discovered():
    # Importing the package alone registers every provider module (the drop-in
    # contract): the Inkbox provider is present without being imported by hand.
    assert "inkbox" in {p.name for p in wp.base._REGISTRY}


def test_match_provider_identifies_inkbox_by_header():
    provider = wp.match_provider({"X-Inkbox-Signature": "sha256=abc"})
    assert provider is not None and provider.name == "inkbox"


def test_match_provider_is_case_insensitive():
    provider = wp.match_provider({"x-inkbox-signature": "sha256=abc"})
    assert provider is not None and provider.name == "inkbox"


def test_match_provider_returns_none_for_unknown_source():
    # A third-party source we have not onboarded a verifier for.
    assert wp.match_provider({"X-Stripe-Signature": "t=1,v1=abc"}) is None


def test_github_provider_registered_and_matches():
    provider = wp.match_provider({"X-Hub-Signature-256": "sha256=abc"})
    assert provider is not None and provider.name == "github"


def test_github_provider_verifies_real_hmac():
    from inkbox_claude.webhook_providers.github import GithubProvider

    provider = GithubProvider()
    body = b'{"action":"completed","conclusion":"failure"}'
    secret = "gh_webhook_secret"
    good = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()

    hdr = {"X-Hub-Signature-256": good}
    assert provider.verify(body=body, headers=hdr, url="u", secret=secret) is True
    # Tamper / wrong secret / no secret → all reject.
    assert provider.verify(body=body + b"x", headers=hdr, url="u", secret=secret) is False
    assert provider.verify(body=body, headers=hdr, url="u", secret="wrong") is False
    assert provider.verify(body=body, headers=hdr, url="u", secret="") is False
    assert provider.verify(
        body=body, headers={"X-Hub-Signature-256": "nope"}, url="u", secret=secret
    ) is False


def test_inkbox_provider_delegates_to_sdk(monkeypatch):
    seen = {}

    def _fake_verify(*, payload, headers, secret):
        seen.update(payload=payload, secret=secret)
        return True

    monkeypatch.setattr(inkbox_provider_mod, "verify_webhook", _fake_verify)
    provider = inkbox_provider_mod.InkboxProvider()
    ok = provider.verify(body=b"raw", headers={}, url="u", secret="whsec_test")
    assert ok is True
    assert seen == {"payload": b"raw", "secret": "whsec_test"}


def test_inkbox_provider_fails_closed_without_sdk(monkeypatch):
    # SDK absent → cannot verify → must reject, never accept.
    monkeypatch.setattr(inkbox_provider_mod, "verify_webhook", None)
    provider = inkbox_provider_mod.InkboxProvider()
    ok = provider.verify(
        body=b"x", headers={"X-Inkbox-Signature": "sha256=abc"}, url="u", secret="s"
    )
    assert ok is False


def test_inkbox_provider_real_signature_roundtrip():
    # Exercise the real SDK HMAC path (not mocked): good sig verifies, and any
    # tamper — body or secret — fails.
    if inkbox_provider_mod.verify_webhook is None:
        pytest.skip("inkbox SDK not installed")
    provider = inkbox_provider_mod.InkboxProvider()
    body = b'{"event_type":"message.received","data":{"id":"abc"}}'
    secret = "whsec_secret"
    request_id, timestamp = "rid-1", "1700000000"
    message = f"{request_id}.{timestamp}.".encode() + body
    digest = hmac.new(secret.removeprefix("whsec_").encode(), message, hashlib.sha256).hexdigest()
    headers = {
        "X-Inkbox-Signature": "sha256=" + digest,
        "X-Inkbox-Request-Id": request_id,
        "X-Inkbox-Timestamp": timestamp,
    }

    assert provider.verify(body=body, headers=headers, url="u", secret=secret) is True
    assert provider.verify(body=body + b" ", headers=headers, url="u", secret=secret) is False
    assert provider.verify(body=body, headers=headers, url="u", secret="whsec_wrong") is False


# --- gateway integration ---------------------------------------------------

def test_unsigned_inkbox_typed_event_is_not_trusted_as_inkbox(monkeypatch):
    # We route on the authenticated source, not the body's claim. An unsigned
    # payload claiming "message.received" must NOT reach the Inkbox mail handler
    # — with pass-through off it is simply ignored.
    hit = {"mail": 0}

    async def _mail(_envelope):
        hit["mail"] += 1

    gw = _gateway(require_signature=True, external_events_enabled=False, monkeypatch=monkeypatch)
    monkeypatch.setattr(gw, "_on_mail_received", _mail)
    resp = asyncio.run(
        gw._handle_webhook(_FakeRequest(b'{"event_type":"message.received"}'))
    )
    assert resp.status == 200 and json.loads(resp.text)["ignored"] == "message.received"
    assert hit["mail"] == 0
    assert gw._external_wakes == []


def test_inkbox_event_with_valid_signature_passes(monkeypatch):
    monkeypatch.setattr(inkbox_provider_mod, "verify_webhook", lambda **k: True)
    gw = _gateway(require_signature=True, external_events_enabled=False, monkeypatch=monkeypatch)
    resp = asyncio.run(
        gw._handle_webhook(
            _FakeRequest(
                b'{"event_type":"message.delivered"}',
                headers={"X-Inkbox-Signature": "sha256=good"},
            )
        )
    )
    # message.* lifecycle is a log-only 200 — proves it passed auth.
    assert resp.status == 200 and json.loads(resp.text)["ignored"] == "message.delivered"


def test_inkbox_event_with_bad_signature_is_rejected(monkeypatch):
    monkeypatch.setattr(inkbox_provider_mod, "verify_webhook", lambda **k: False)
    gw = _gateway(require_signature=True, external_events_enabled=False, monkeypatch=monkeypatch)
    resp = asyncio.run(
        gw._handle_webhook(
            _FakeRequest(
                b'{"event_type":"message.delivered"}',
                headers={"X-Inkbox-Signature": "sha256=bad"},
            )
        )
    )
    assert resp.status == 401


def test_unknown_source_passthrough_is_unverified_when_enabled(monkeypatch):
    # No registered verifier + pass-through on → wake the agent even with
    # require_signature True (we cannot verify an unknown source).
    gw = _gateway(require_signature=True, external_events_enabled=True, monkeypatch=monkeypatch)
    resp = asyncio.run(
        gw._handle_webhook(_FakeRequest(b'{"event":"prod_on_fire"}'))
    )
    assert resp.status == 200
    assert gw._external_wakes == [({"event": "prod_on_fire"}, False)]


def test_unknown_source_dropped_when_passthrough_disabled(monkeypatch):
    gw = _gateway(require_signature=True, external_events_enabled=False, monkeypatch=monkeypatch)
    resp = asyncio.run(
        gw._handle_webhook(_FakeRequest(b'{"event":"prod_on_fire"}'))
    )
    assert resp.status == 200 and json.loads(resp.text)["ignored"] == "unknown"
    assert gw._external_wakes == []


def test_registered_third_party_is_verified(monkeypatch):
    # Simulate a future onboarded third-party verifier that rejects the request.
    fake = types.SimpleNamespace(name="acme", verify=lambda **k: False)
    monkeypatch.setattr(gateway_mod, "match_provider", lambda headers: fake)
    monkeypatch.setenv("INKBOX_WEBHOOK_SECRET_ACME", "s3cret")
    gw = _gateway(require_signature=True, external_events_enabled=True, monkeypatch=monkeypatch)
    resp = asyncio.run(
        gw._handle_webhook(
            _FakeRequest(b'{"event":"charge"}', headers={"X-Acme-Signature": "bad"})
        )
    )
    assert resp.status == 401
    assert gw._external_wakes == []


def test_third_party_valid_signature_proceeds(monkeypatch):
    # Matched third-party + good signature → the event reaches the agent, and
    # the raw body, url, and env-resolved secret are all passed to verify().
    captured = {}

    def _verify(**kwargs):
        captured.update(kwargs)
        return True

    fake = types.SimpleNamespace(name="acme", verify=_verify)
    monkeypatch.setattr(gateway_mod, "match_provider", lambda headers: fake)
    monkeypatch.setenv("INKBOX_WEBHOOK_SECRET_ACME", "s3cret")
    gw = _gateway(require_signature=True, external_events_enabled=True, monkeypatch=monkeypatch)
    resp = asyncio.run(
        gw._handle_webhook(
            _FakeRequest(b'{"event":"charge"}', headers={"X-Acme-Signature": "good"})
        )
    )
    assert resp.status == 200
    assert gw._external_wakes == [({"event": "charge"}, True)]
    assert captured["secret"] == "s3cret"             # env secret reached the verifier
    assert captured["body"] == b'{"event":"charge"}'  # raw body, unparsed
    assert captured["url"] == "https://agent.example/webhook"


def test_verified_third_party_bypasses_passthrough_flag(monkeypatch):
    # A source we deliberately onboarded (provider + secret) is trusted, so its
    # events reach the agent even with external pass-through OFF — the flag only
    # gates *unverified* unknown sources.
    fake = types.SimpleNamespace(name="acme", verify=lambda **k: True)
    monkeypatch.setattr(gateway_mod, "match_provider", lambda headers: fake)
    monkeypatch.setenv("INKBOX_WEBHOOK_SECRET_ACME", "s3cret")
    gw = _gateway(require_signature=True, external_events_enabled=False, monkeypatch=monkeypatch)
    resp = asyncio.run(
        gw._handle_webhook(
            _FakeRequest(b'{"event":"charge"}', headers={"X-Acme-Signature": "good"})
        )
    )
    assert resp.status == 200
    assert gw._external_wakes == [({"event": "charge"}, True)]


def test_inkbox_signed_external_shaped_event_routes_external(monkeypatch):
    # An Inkbox *signature* only means Inkbox vouched for delivery — a forwarded
    # external event (e.g. a CI escalation) is Inkbox-signed but is NOT a known
    # Inkbox event shape. It must reach the agent via the external path, not get
    # swallowed by an Inkbox handler branch.
    hit = {"mail": 0}

    async def _mail(_e):
        hit["mail"] += 1

    monkeypatch.setattr(inkbox_provider_mod, "verify_webhook", lambda **k: True)
    gw = _gateway(require_signature=True, external_events_enabled=True, monkeypatch=monkeypatch)
    monkeypatch.setattr(gw, "_on_mail_received", _mail)
    resp = asyncio.run(
        gw._handle_webhook(
            _FakeRequest(
                b'{"event":"agent_escalation_demo","title":"prod down"}',
                headers={"X-Inkbox-Signature": "sha256=good"},
            )
        )
    )
    assert resp.status == 200
    assert hit["mail"] == 0                        # not routed to any Inkbox handler
    assert gw._external_wakes[0][1] is True        # woke the agent as VERIFIED external


def test_inkbox_signed_unknown_dropped_when_external_events_off(monkeypatch):
    # An Inkbox-signed payload with no handler (e.g. a future Inkbox event
    # family) must NOT wake a session when external events are off — it's gated
    # by the flag, same as an unknown source. Only registered third parties
    # bypass the flag.
    monkeypatch.setattr(inkbox_provider_mod, "verify_webhook", lambda **k: True)
    gw = _gateway(require_signature=True, external_events_enabled=False, monkeypatch=monkeypatch)
    resp = asyncio.run(
        gw._handle_webhook(
            _FakeRequest(
                b'{"event_type":"contact.updated","data":{}}',
                headers={"X-Inkbox-Signature": "sha256=good"},
            )
        )
    )
    assert resp.status == 200 and json.loads(resp.text)["ignored"] == "contact.updated"
    assert gw._external_wakes == []


def test_other_provider_claiming_inkbox_type_routes_external_not_mail(monkeypatch):
    # A non-Inkbox source signs a payload that *claims* "message.received".
    # Routing on the authenticated source means it goes to the external path
    # (source=github), never to the Inkbox mail handler — no spoof possible.
    hit = {"mail": 0}

    async def _mail(_envelope):
        hit["mail"] += 1

    other = types.SimpleNamespace(name="github", verify=lambda **k: True)
    monkeypatch.setattr(gateway_mod, "match_provider", lambda headers: other)
    gw = _gateway(require_signature=True, external_events_enabled=True, monkeypatch=monkeypatch)
    monkeypatch.setattr(gw, "_on_mail_received", _mail)
    resp = asyncio.run(
        gw._handle_webhook(_FakeRequest(b'{"event_type":"message.received"}'))
    )
    assert resp.status == 200
    assert hit["mail"] == 0                  # never reached the Inkbox mail handler
    assert gw._external_wakes[0][1] is True  # handled as a verified external event


def test_github_valid_signature_reaches_agent(monkeypatch):
    # A GitHub-signed escalation with a VALID signature is verified and handed
    # to the agent as an external event (source=github, not a known Inkbox shape).
    monkeypatch.setenv("INKBOX_WEBHOOK_SECRET_GITHUB", "gh_secret")
    body = b'{"event":"workflow_run","conclusion":"failure","summary":"call the operator now"}'
    sig = "sha256=" + hmac.new(b"gh_secret", body, hashlib.sha256).hexdigest()
    gw = _gateway(require_signature=True, external_events_enabled=True, monkeypatch=monkeypatch)
    resp = asyncio.run(
        gw._handle_webhook(_FakeRequest(body, headers={"X-Hub-Signature-256": sig}))
    )
    assert resp.status == 200
    assert len(gw._external_wakes) == 1  # verified → agent woken


def test_github_forged_signature_is_dropped(monkeypatch):
    # Same event, a FORGED signature → rejected before the agent sees anything.
    monkeypatch.setenv("INKBOX_WEBHOOK_SECRET_GITHUB", "gh_secret")
    body = b'{"event":"workflow_run","conclusion":"failure","summary":"call the operator now"}'
    gw = _gateway(require_signature=True, external_events_enabled=True, monkeypatch=monkeypatch)
    resp = asyncio.run(
        gw._handle_webhook(
            _FakeRequest(body, headers={"X-Hub-Signature-256": "sha256=deadbeef"})
        )
    )
    assert resp.status == 401
    assert gw._external_wakes == []  # forged → agent never woken


def test_require_signature_false_bypasses_verify(monkeypatch):
    # Local-testing escape hatch: the source is still identified by its header
    # (real Inkbox traffic always carries it), but the signature is not checked.
    gw = _gateway(require_signature=False, external_events_enabled=False, monkeypatch=monkeypatch)
    resp = asyncio.run(
        gw._handle_webhook(
            _FakeRequest(
                b'{"event_type":"message.delivered"}',
                headers={"X-Inkbox-Signature": "sha256=unchecked"},
            )
        )
    )
    assert resp.status == 200 and json.loads(resp.text)["ignored"] == "message.delivered"


# --- external turn content -------------------------------------------------

def test_unknown_source_turn_carries_unverified_directive():
    chat_id, prompt = InkboxGateway._build_external_event_turn(
        {"event": "maybe_prod_fire"}, "rid-1", verified=False
    )
    assert chat_id == "external:external"
    assert prompt.startswith(gateway_mod.EXTERNAL_EVENT_UNVERIFIED_DIRECTIVE)


def test_verified_turn_carries_action_directive_and_fields():
    envelope = {
        "source": "ci",
        "event": "workflow_failed",
        "title": "Deploy failed",
        "summary": "Prod deploy is red",
        "severity": "high",
        "requested_action": "call the operator",
        "url": "https://ci.example/run/7",
        "id": "run-7",
    }
    chat_id, prompt = InkboxGateway._build_external_event_turn(envelope, "", verified=True)
    assert chat_id == "external:ci"
    assert prompt.startswith(gateway_mod.EXTERNAL_EVENT_DIRECTIVE)
    assert "[inkbox:external source=ci event=workflow_failed event_key=run-7" in prompt
    assert "severity=high" in prompt
    assert "Deploy failed" in prompt
    assert "Requested action: call the operator" in prompt
    assert "Link: https://ci.example/run/7" in prompt
    assert "Raw event payload:" in prompt


def test_external_turn_bounds_untrusted_text():
    envelope = {"source": "x[evil]\nsource", "title": "t" * 500, "summary": "s" * 5000}
    chat_id, prompt = InkboxGateway._build_external_event_turn(envelope, "rid", verified=False)
    # Marker-breaking characters are stripped and free text is bounded.
    assert chat_id == "external:xevil source"
    assert "t" * 201 not in prompt.split("Raw event payload:")[0]


def test_on_external_event_runs_capture_turn_in_source_session():
    class _FakeSession:
        def __init__(self):
            self.consults = []

        async def run_consult(self, prompt):
            self.consults.append(prompt)
            return ""

    class _FakeSessions:
        def __init__(self, session):
            self.session = session
            self.requested = []

        def get(self, chat_id):
            self.requested.append(chat_id)
            return self.session

    session = _FakeSession()
    gw = InkboxGateway(BridgeConfig(require_signature=False, external_events_enabled=True))
    gw.sessions = _FakeSessions(session)

    async def main():
        resp = await gw._on_external_event({"source": "ci", "event": "boom"}, "rid", verified=True)
        await asyncio.sleep(0)  # let the background turn task run
        return resp

    resp = asyncio.run(main())
    assert resp.status == 200
    assert gw.sessions.requested == ["external:ci"]
    assert len(session.consults) == 1
    assert session.consults[0].startswith(gateway_mod.EXTERNAL_EVENT_DIRECTIVE)


# --- secret resolution -------------------------------------------------------

def test_provider_secret_inkbox_uses_signing_key():
    gw = _gateway(require_signature=True, external_events_enabled=False)
    assert gw._provider_secret("inkbox") == "whsec_test"


def test_provider_secret_third_party_reads_env(monkeypatch):
    monkeypatch.setenv("INKBOX_WEBHOOK_SECRET_ACME", "from-env")
    gw = _gateway(require_signature=True, external_events_enabled=False)
    assert gw._provider_secret("acme") == "from-env"


def test_provider_secret_missing_env_is_empty(monkeypatch):
    monkeypatch.delenv("INKBOX_WEBHOOK_SECRET_NOPE", raising=False)
    gw = _gateway(require_signature=True, external_events_enabled=False)
    assert gw._provider_secret("nope") == ""


# --- provider unit edges -------------------------------------------------

def test_register_provider_returns_class_and_registers(monkeypatch):
    monkeypatch.setattr(wp.base, "_REGISTRY", [])

    @wp.register_provider
    class _Tmp(wp.WebhookProvider):
        name = "tmp"
        provider_header = "X-Tmp"

    assert _Tmp.__name__ == "_Tmp"  # decorator is transparent
    assert [p.name for p in wp.base._REGISTRY] == ["tmp"]


def test_match_provider_first_match_wins(monkeypatch):
    a = types.SimpleNamespace(name="a", matches=lambda h: True)
    b = types.SimpleNamespace(name="b", matches=lambda h: True)
    monkeypatch.setattr(wp.base, "_REGISTRY", [a, b])
    assert wp.match_provider({}).name == "a"


def test_base_matches_false_without_provider_header():
    assert wp.WebhookProvider().matches({"X-Anything": "1"}) is False


def test_base_verify_is_abstract():
    with pytest.raises(NotImplementedError):
        wp.WebhookProvider().verify(body=b"", headers={}, url="", secret="")
