"""Identity-scoped inbound-call configuration (SDK 0.4.15+).

One identity-level row covers calls arriving on the dedicated number AND the
shared iMessage line; the number-scoped update is only a legacy fallback for
SDKs that predate ``set_incoming_call_action``.
"""

import types

from inkbox_claude import gateway
from inkbox_claude.config import BridgeConfig, VoiceStack


class _FakeSubscriptions:
    def __init__(self, existing=()):
        self.created = []
        self.existing = list(existing)
        self.deleted = []

    def list(self, **_owner):
        return list(self.existing)

    def create(self, **kwargs):
        self.created.append(kwargs)

    def delete(self, sub_id):
        self.deleted.append(sub_id)


class _UnsupportedA2ASubscriptions(_FakeSubscriptions):
    def __init__(self):
        super().__init__()
        self.attempted = []

    def create(self, **kwargs):
        self.attempted.append(kwargs)
        if any(event.startswith("a2a.") for event in kwargs["event_types"]):
            raise ValueError(
                "event_type 'a2a.task.created' does not belong to any known channel"
            )
        return super().create(**kwargs)


class _FakePhoneNumbers:
    def __init__(self):
        self.updated = []

    def update(self, phone_id, **kwargs):
        self.updated.append((phone_id, kwargs))


class _FakeInkbox:
    def __init__(self, identity, subscriptions=None):
        self._identity = identity
        self.webhooks = types.SimpleNamespace(
            subscriptions=subscriptions or _FakeSubscriptions()
        )
        self.phone_numbers = _FakePhoneNumbers()

    def get_identity(self, _handle):
        return self._identity


class _Identity:
    """Identity WITH the identity-scoped call-config method."""

    def __init__(self, *, phone=None, imessage_enabled=False):
        self.id = "identity-1"
        self.agent_handle = "claude"
        self.mailbox = None
        self.phone_number = phone
        self.imessage_enabled = imessage_enabled
        self.incoming_call_kwargs = None

    def set_incoming_call_action(self, **kwargs):
        self.incoming_call_kwargs = kwargs


class _LegacyIdentity:
    """Identity WITHOUT set_incoming_call_action (pre-0.4.15 SDK surface)."""

    def __init__(self, *, phone=None, imessage_enabled=False):
        self.id = "identity-1"
        self.agent_handle = "claude"
        self.mailbox = None
        self.phone_number = phone
        self.imessage_enabled = imessage_enabled


def _phone():
    return types.SimpleNamespace(id="phone-1", number="+15550001111")


def _patched_gateway(identity, subscriptions=None, *, voice_stack=VoiceStack.INKBOX_TTS_STT):
    gw = gateway.InkboxGateway(BridgeConfig(
        identity="claude", require_signature=False, voice_stack=voice_stack
    ))
    gw._inkbox = _FakeInkbox(identity, subscriptions)
    gw._public_url = "https://agent.example"
    gw._public_host = "agent.example"
    gw._patch_identity_objects()
    return gw


def test_incoming_call_config_is_identity_scoped_with_number():
    identity = _Identity(phone=_phone())
    gw = _patched_gateway(identity)

    assert identity.incoming_call_kwargs == {
        "incoming_call_action": "auto_accept",
        "client_websocket_url": "wss://agent.example/phone/media/ws",
        "incoming_call_webhook_url": "https://agent.example/webhook",
    }
    # The identity-scoped write replaces the legacy number-scoped one.
    assert gw._inkbox.phone_numbers.updated == []


def test_voice_ai_reconciles_hosted_incoming_action_without_local_urls():
    identity = _Identity(phone=_phone())
    _patched_gateway(identity, voice_stack=VoiceStack.INKBOX_VOICE_AI)

    assert identity.incoming_call_kwargs == {
        "incoming_call_action": "hosted_agent",
        "client_websocket_url": None,
        "incoming_call_webhook_url": None,
    }


def test_incoming_call_config_registered_for_imessage_only_identity():
    # No dedicated number, but the shared iMessage line can still receive
    # calls — the identity-level row must be written.
    identity = _Identity(phone=None, imessage_enabled=True)
    gw = _patched_gateway(identity)

    assert identity.incoming_call_kwargs is not None
    assert identity.incoming_call_kwargs["incoming_call_action"] == "auto_accept"
    assert gw._inkbox.phone_numbers.updated == []


def test_incoming_call_config_skipped_when_no_line_can_ring():
    identity = _Identity(phone=None, imessage_enabled=False)
    gw = _patched_gateway(identity)

    assert identity.incoming_call_kwargs is None
    assert gw._inkbox.phone_numbers.updated == []


def test_legacy_sdk_falls_back_to_number_scoped_update():
    identity = _LegacyIdentity(phone=_phone())
    gw = _patched_gateway(identity)

    assert gw._inkbox.phone_numbers.updated == [(
        "phone-1",
        {
            "incoming_call_webhook_url": "https://agent.example/webhook",
            "incoming_call_action": "auto_accept",
            "client_websocket_url": "wss://agent.example/phone/media/ws",
        },
    )]


def test_legacy_sdk_cannot_configure_imessage_only_identity():
    # The number-scoped shim has nothing to update without a number; the
    # gateway must not crash, and no write happens.
    identity = _LegacyIdentity(phone=None, imessage_enabled=True)
    gw = _patched_gateway(identity)

    assert gw._inkbox.phone_numbers.updated == []


def test_a2a_subscription_falls_back_to_imessage_on_older_api():
    subscriptions = _UnsupportedA2ASubscriptions()
    _patched_gateway(
        _Identity(phone=None, imessage_enabled=True),
        subscriptions=subscriptions,
    )

    assert subscriptions.created[-1]["event_types"] == gateway.IMESSAGE_EVENTS


def test_a2a_and_imessage_use_channel_coherent_subscriptions():
    subscriptions = _FakeSubscriptions()
    _patched_gateway(
        _Identity(phone=None, imessage_enabled=True),
        subscriptions=subscriptions,
    )

    assert [created["event_types"] for created in subscriptions.created] == [
        gateway.A2A_EVENTS,
        gateway.CALL_EVENTS,
        gateway.IMESSAGE_EVENTS,
    ]
    assert [created["url"] for created in subscriptions.created] == [
        "https://agent.example/webhook",
        "https://agent.example/webhook",
        "https://agent.example/webhook",
    ]


def test_imessage_reconcile_preserves_existing_a2a_channel_subscription():
    a2a = types.SimpleNamespace(
        id="sub-a2a",
        url="https://agent.example/webhook",
        event_types=gateway.A2A_EVENTS,
    )
    subscriptions = _FakeSubscriptions([a2a])
    _patched_gateway(
        _Identity(phone=None, imessage_enabled=True),
        subscriptions=subscriptions,
    )

    assert subscriptions.deleted == []
    assert [created["event_types"] for created in subscriptions.created] == [
        gateway.CALL_EVENTS,
        gateway.IMESSAGE_EVENTS,
    ]


def test_a2a_only_subscription_is_skipped_on_older_api():
    subscriptions = _UnsupportedA2ASubscriptions()
    _patched_gateway(
        _Identity(phone=None, imessage_enabled=False),
        subscriptions=subscriptions,
    )

    assert [created["event_types"] for created in subscriptions.created] == [gateway.CALL_EVENTS]
