"""Identity-scoped inbound-call configuration (SDK 0.4.15+).

One identity-level row covers calls arriving on the dedicated number AND the
shared iMessage line; the number-scoped update is only a legacy fallback for
SDKs that predate ``set_incoming_call_action``.
"""

import types

from inkbox_claude import gateway
from inkbox_claude.config import BridgeConfig


class _FakeSubscriptions:
    def __init__(self):
        self.created = []

    def list(self, **_owner):
        return []

    def create(self, **kwargs):
        self.created.append(kwargs)


class _FakePhoneNumbers:
    def __init__(self):
        self.updated = []

    def update(self, phone_id, **kwargs):
        self.updated.append((phone_id, kwargs))


class _FakeInkbox:
    def __init__(self, identity):
        self._identity = identity
        self.webhooks = types.SimpleNamespace(subscriptions=_FakeSubscriptions())
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


def _patched_gateway(identity):
    gw = gateway.InkboxGateway(BridgeConfig(identity="claude", require_signature=False))
    gw._inkbox = _FakeInkbox(identity)
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
