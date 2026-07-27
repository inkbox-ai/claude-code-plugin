import types

import pytest

from inkbox_claude import gateway
from inkbox_claude.config import BridgeConfig


class Conflict(Exception):
    status_code = 409


class Subscriptions:
    def __init__(self, rows=None, conflict=False, rows_after_conflict=None):
        self.rows = list(rows or [])
        self.conflict = conflict
        self.rows_after_conflict = rows_after_conflict
        self.created = []
        self.updated = []
        self.deleted = []
        self.list_count = 0

    def list(self, **_owner):
        self.list_count += 1
        if self.list_count > 1 and self.rows_after_conflict is not None:
            return list(self.rows_after_conflict)
        return list(self.rows)

    def create(self, **kwargs):
        self.created.append(kwargs)
        if self.conflict:
            raise Conflict()

    def update(self, sub_id, **kwargs):
        self.updated.append((sub_id, kwargs))

    def delete(self, sub_id):
        self.deleted.append(sub_id)


def _sub(*, sub_id="sub-1", url="https://agent.example/webhook", events=None, context=None):
    return types.SimpleNamespace(
        id=sub_id, url=url, event_types=events or list(gateway.MAIL_EVENTS),
        context_config=context,
    )


def _patch(subscriptions, *, previous_webhook_url=""):
    mailbox = types.SimpleNamespace(id="mailbox-1", email_address="agent@example.com")
    identity = types.SimpleNamespace(
        id="identity-1", agent_handle="claude", mailbox=mailbox,
        phone_number=None, imessage_enabled=False,
    )
    client = types.SimpleNamespace(
        get_identity=lambda _handle: identity,
        webhooks=types.SimpleNamespace(subscriptions=subscriptions),
        phone_numbers=types.SimpleNamespace(update=lambda *_args, **_kwargs: None),
    )
    gw = gateway.InkboxGateway(BridgeConfig(identity="claude", require_signature=False))
    gw._inkbox = client
    gw._public_url = "https://agent.example"
    gw._public_host = "agent.example"
    gw._patch_identity_objects(previous_webhook_url=previous_webhook_url)
    return subscriptions


def _desired_update():
    return {
        "url": "https://agent.example/webhook",
        "event_types": gateway.MAIL_EVENTS,
        "context_config": gateway._WEBHOOK_CONTEXT_CONFIG,
    }


def test_creation_passes_context_config():
    subscriptions = _patch(Subscriptions())
    assert subscriptions.created == [{**_desired_update(), "mailbox_id": "mailbox-1"}]
    assert subscriptions.updated == []


def test_unrelated_same_channel_subscription_is_preserved_and_bridge_is_created():
    unrelated = _sub(sub_id="crm", url="https://crm.example/inbound", context=None)
    subscriptions = _patch(Subscriptions([unrelated]))

    assert subscriptions.created == [{**_desired_update(), "mailbox_id": "mailbox-1"}]
    assert subscriptions.updated == [] and subscriptions.deleted == []


def test_exact_desired_url_subscription_is_adopted():
    subscriptions = _patch(Subscriptions([_sub(context=gateway._WEBHOOK_CONTEXT_CONFIG)]))

    assert subscriptions.created == []
    assert subscriptions.updated == [] and subscriptions.deleted == []


@pytest.mark.parametrize("row", [
    _sub(events=["message.received"], context=gateway._WEBHOOK_CONTEXT_CONFIG),
    _sub(context=None),
])
def test_exact_desired_url_with_event_or_context_drift_is_updated(row):
    subscriptions = _patch(Subscriptions([row]))
    assert subscriptions.updated == [("sub-1", _desired_update())]
    assert subscriptions.created == []


def test_known_previous_bridge_url_is_migrated_safely():
    previous = "https://old-agent.example/webhook"
    subscriptions = _patch(
        Subscriptions([_sub(url=previous, context=gateway._WEBHOOK_CONTEXT_CONFIG)]),
        previous_webhook_url=previous,
    )

    assert subscriptions.updated == [("sub-1", _desired_update())]
    assert subscriptions.created == [] and subscriptions.deleted == []


def test_multiple_unrelated_subscriptions_are_preserved():
    rows = [
        _sub(sub_id="crm", url="https://crm.example/inbound"),
        _sub(sub_id="analytics", url="https://analytics.example/events"),
    ]
    subscriptions = _patch(Subscriptions(rows))

    assert subscriptions.created == [{**_desired_update(), "mailbox_id": "mailbox-1"}]
    assert subscriptions.updated == [] and subscriptions.deleted == []


def test_matching_subscription_performs_zero_writes():
    subscriptions = _patch(Subscriptions([_sub(context=gateway._WEBHOOK_CONTEXT_CONFIG)]))
    assert subscriptions.created == [] and subscriptions.updated == [] and subscriptions.deleted == []


def test_409_repair_adopts_matching_context():
    matching = _sub(context=gateway._WEBHOOK_CONTEXT_CONFIG)
    subscriptions = _patch(Subscriptions(conflict=True, rows_after_conflict=[matching]))
    assert len(subscriptions.created) == 1
    assert subscriptions.updated == []


def test_409_repair_updates_drifted_context():
    subscriptions = _patch(Subscriptions(conflict=True, rows_after_conflict=[_sub(context=None)]))
    assert subscriptions.updated == [("sub-1", _desired_update())]


def test_409_repair_never_updates_a_different_url():
    row = _sub(url="https://crm.example/inbound", context=None)
    subscriptions = Subscriptions(conflict=True, rows_after_conflict=[row])

    with pytest.raises(Conflict):
        _patch(subscriptions)

    assert len(subscriptions.created) == 1
    assert subscriptions.updated == [] and subscriptions.deleted == []
