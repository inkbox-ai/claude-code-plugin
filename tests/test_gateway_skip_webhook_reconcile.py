"""Startup normally installs webhook subscriptions pointing at whatever URL the
bridge just came up on, which is right when the bridge owns its ingress.

Deployments that provision subscriptions ahead of time need the opposite: the
destination is already fixed, and the API key may not be permitted to change
it, so writing on every start is redundant at best and fatal to startup at
worst. INKBOX_SKIP_WEBHOOK_RECONCILE turns that write off and leaves the rest
of startup alone."""

import pytest

from inkbox_claude.config import BridgeConfig, env_flag
from inkbox_claude.gateway import InkboxGateway


class _ExplodingSubscriptions:
    """Any call here means the skip did not take effect."""

    def list(self, **_kwargs):
        raise AssertionError("listed subscriptions despite the skip flag")

    def create(self, **_kwargs):
        raise AssertionError("created a subscription despite the skip flag")

    def delete(self, _sub_id):
        raise AssertionError("deleted a subscription despite the skip flag")


class _ExplodingInkbox:
    def __init__(self):
        self.webhooks = type("_W", (), {"subscriptions": _ExplodingSubscriptions()})()

    def get_identity(self, _handle):
        raise AssertionError("read the identity despite the skip flag")


def _gateway(*, skip: bool) -> InkboxGateway:
    gw = InkboxGateway(
        BridgeConfig(
            identity="claude-agent",
            allow_all_users=True,
            skip_webhook_reconcile=skip,
        )
    )
    gw._inkbox = _ExplodingInkbox()
    gw._public_url = "https://agent.inkboxwire.com"
    gw._public_host = "agent.inkboxwire.com"
    return gw


def test_skipping_touches_no_subscriptions() -> None:
    """The point of the flag: startup proceeds without writing anything."""
    _gateway(skip=True)._patch_identity_objects()


def test_not_skipping_still_reconciles() -> None:
    """Default behavior is unchanged; the fake asserts by exploding."""
    with pytest.raises(AssertionError):
        _gateway(skip=False)._patch_identity_objects()


def test_default_config_reconciles() -> None:
    """A config that never mentions the flag must keep the old behavior."""
    assert BridgeConfig().skip_webhook_reconcile is False


@pytest.mark.parametrize("raw", ["true", "True", "1", "yes", "on", "ON"])
def test_truthy_spellings_enable_the_skip(monkeypatch, raw: str) -> None:
    """Operators set this by hand, so accept the obvious spellings."""
    monkeypatch.setenv("INKBOX_SKIP_WEBHOOK_RECONCILE", raw)

    assert env_flag("INKBOX_SKIP_WEBHOOK_RECONCILE", False) is True


@pytest.mark.parametrize("raw", ["false", "False", "0", "no", "off", ""])
def test_everything_else_leaves_reconcile_on(monkeypatch, raw: str) -> None:
    """Unrecognized must not silently disable subscription setup."""
    monkeypatch.setenv("INKBOX_SKIP_WEBHOOK_RECONCILE", raw)

    assert env_flag("INKBOX_SKIP_WEBHOOK_RECONCILE", False) is False


def test_unset_leaves_reconcile_on(monkeypatch) -> None:
    """The absent case, which is what almost every deployment has."""
    monkeypatch.delenv("INKBOX_SKIP_WEBHOOK_RECONCILE", raising=False)

    assert env_flag("INKBOX_SKIP_WEBHOOK_RECONCILE", False) is False
