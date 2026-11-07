"""WhatsApp Companion exercises paged SDK history and native Claude checkpoints."""
import asyncio
from copy import deepcopy
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest
from inkbox import InkboxAPIError
from inkbox_claude import whatsapp
from tests import test_companion as companion_harness
from tests.test_companion import fixture, Request, drained, uid

harness = companion_harness.harness

pytestmark = pytest.mark.skipif(not whatsapp.group_sdk_available(), reason='Compatible SDK group/Companion models are tested in the source-SDK lane')


@pytest.fixture(autouse=True)
def real_web_responses(monkeypatch):
    from aiohttp import web
    from inkbox_claude import gateway
    monkeypatch.setattr(gateway, "web", web)


def source_event(message_id=12, **changes):
    env, pages = fixture('imessage')
    env['event_type'] = 'whatsapp.received'
    env['companion']['channel'] = 'whatsapp'
    message = env['data']['message']
    message.update(id=uid(message_id), agent_identity_id=uid(100), local_number='+15555550100', remote_number=None,
                   is_group=True, group_name='Shared project', unresolved_participant_count=0,
                   participants=['+15555550101', '+15555550102'], content='Welcome',
                   mentions=[], agent_mentioned=False, media=[], is_blocked=False)
    message.update(changes)
    for page in pages:
        page['channel'] = page['reply_context']['channel'] = 'whatsapp'
    return env, pages


def build(harness, pages, **config):
    gw, transport = harness.build(pages, **config)
    gw._identity.id = uid(100)
    gw._identity.agent_handle = 'agent'
    gw._identity.get_whatsapp_conversation = lambda conversation: NS(id=conversation, agent_identity_id=uid(100),
        is_group=True, group_name='Shared project', unresolved_participant_count=0, remote_number=None,
        local_number='+15555550100', participants=['+15555550101', '+15555550102'])
    gw._identity.get_whatsapp_message = lambda message: NS(**source_event()[0]['data']['message'])
    gw._identity.send_whatsapp = lambda **kwargs: (harness.outputs.append(('whatsapp', kwargs)) or NS(id=uid(90), status='queued'))
    gw._with_media = AsyncMock(side_effect=lambda text, media, **kwargs: text)
    return gw, transport


async def close(gw):
    if gw._companion:
        await gw._companion.close()
    await gw.sessions.close_all()
    for store in gw._channel_stores.values():
        store.close()


def test_signed_initialization_keeps_history_group_route_and_no_contact_memories(harness):
    async def run():
        env, pages = source_event()
        gw, transport = build(harness, pages, allowed_users=['+15555550101'])
        harness.hooks.reply = 'Group answer'
        try:
            response = await gw._handle_webhook(Request(env))
            assert response.status == 200
            rows = await drained(gw)
            assert len(harness.queries) == 1
            prompt = harness.queries[0]
            assert '/clear' in prompt and 'YES' in prompt and 'PRIVATE CONTACT MEMORY' not in prompt
            assert any(params.get('cursor') for _, params in transport.calls)
            assert len(harness.outputs) == 1 and harness.outputs[0][0] == 'whatsapp', rows
            assert harness.outputs[0][1]['conversation_id'] == uid(3)
            assert 'to' not in harness.outputs[0][1]
            assert rows[0]['events'][uid(12)]['state'] == 'completed'
        finally:
            await close(gw)
    asyncio.run(run())


@pytest.mark.parametrize('native,access,mode,expected', [(False,'direct','safe',0),(True,'direct','safe',1),
    (True,'sponsored','safe',0),(True,'sponsored','relaxed',1)])
def test_only_current_native_agent_mention_wakes_and_safe_mode_remains_independent(harness, native, access, mode, expected):
    async def run():
        env, pages = source_event(content='@123 please help', agent_mentioned=native, sender_access=access,
            mentions=[{'text':'@123','phone_number':None,'is_agent':native}])
        gw, _ = build(harness, pages, allow_all_users=True, group_reply_mode='mention', companion_response_mode=mode)
        try:
            assert (await gw._handle_webhook(Request(env))).status == 200
            await drained(gw)
            assert len(harness.queries) == expected
            if expected:
                assert '@agent please help' in harness.queries[0]
                assert '"original_text": "@123 please help"' in harness.queries[0]
        finally:
            await close(gw)
    asyncio.run(run())


def test_mutable_retry_projection_keeps_first_receipt_and_rejects_changed_body(harness):
    async def run():
        env, pages = source_event()
        gw, _ = build(harness, pages, allow_all_users=True)
        receiver = gw._companion_receiver()
        receiver.schedule = lambda key: None
        try:
            receiver.accept(env)
            retry = deepcopy(env)
            retry['data']['message'].update(group_name='Renamed', mentions=[{'text':'@123','phone_number':None,'is_agent':True}],
                agent_mentioned=True, media=[{'id':uid(25)}], thread_id=uid(26), sender_access='sponsored')
            assert receiver.accept(retry)['deduped']
            saved = next(iter(receiver.records.values()))['events'][uid(12)]['message']
            assert saved['mentions'] == [] and not saved['agent_mentioned']
            retry['data']['message']['content'] = 'different source body'
            with pytest.raises(ValueError, match='authority'):
                receiver.accept(retry)
        finally:
            await close(gw)
    asyncio.run(run())


def test_readiness_conflict_retains_unsubmitted_receipt_for_recovery(harness):
    async def run():
        env, pages = source_event()
        gw, transport = build(harness, pages, allow_all_users=True)
        receiver = gw._companion_receiver()
        receiver.schedule = lambda key: None
        try:
            receiver.accept(env)
            key = next(iter(receiver.records))
            transport.error = InkboxAPIError(409, {'error':'group_not_ready'})
            assert await receiver.drain_once(key)
            assert receiver.records[key]['events'][uid(12)]['state'] == 'pending'
            assert not harness.queries
            transport.error = None
            await receiver.drain_once(key)
            assert len(harness.queries) == 1
        finally:
            await close(gw)
    asyncio.run(run())


def test_signed_ordinary_downgrade_cannot_repeat_previous_grant(harness):
    async def run():
        env, pages = source_event()
        gw, _ = build(harness, pages, allow_all_users=True)
        receiver = gw._companion_receiver()
        receiver.schedule = lambda key: None
        try:
            receiver.accept(env)
            ordinary = deepcopy(env)
            ordinary['companion'].pop('activation_id')
            ordinary['companion']['phase'] = 'ordinary'
            assert receiver.accept(ordinary)['deduped']
            row = next(iter(receiver.records.values()))
            assert row['events'][uid(12)]['state'] == 'discarded'
            assert len(receiver.records) == 1 and not harness.queries
        finally:
            await close(gw)
    asyncio.run(run())


@pytest.mark.parametrize('failure', ['preflight', 'rejected', 'uncertain'])
def test_saved_answer_retries_only_preflight_without_replaying_model(harness, failure):
    async def run():
        env, pages = source_event()
        gw, transport = build(harness, pages, allow_all_users=True)
        harness.hooks.reply = 'One saved answer'
        receiver = gw._companion_receiver()
        receiver.schedule = lambda key: None
        writes = []
        original = gw.send_to_contact
        async def send(chat, text, mode, meta):
            if failure == 'preflight':
                from inkbox_claude.imessage import SafeReplyPreflightError
                raise SafeReplyPreflightError('Current membership proof unavailable')
            return await original(chat, text, mode, meta)
        def effect(**kwargs):
            writes.append(kwargs)
            if failure == 'rejected':
                raise InkboxAPIError(422, {'error': 'whatsapp_text_too_long'})
            raise TimeoutError('Unknown POST outcome')
        gw.send_to_contact = send
        gw._identity.send_whatsapp = effect
        try:
            receiver.accept(env)
            key = next(iter(receiver.records))
            await receiver.drain_once(key)
            row = receiver.records[key]
            saved = row['events'][uid(12)]
            assert len(harness.queries) == 1
            if failure == 'preflight':
                assert saved['state'] == 'generated' and not writes
                gw.send_to_contact = original
                gw._identity.send_whatsapp = lambda **kwargs: writes.append(kwargs) or NS(id=uid(90))
                await receiver.drain_once(key)
                assert saved['state'] == 'completed' and len(writes) == 1
            elif failure == 'rejected':
                assert saved['state'] == 'completed' and saved['reply_rejected']
                await receiver.drain_once(key)
                assert len(writes) == 1
            else:
                assert saved['state'] == 'unconfirmed'
                assert 'unconfirmed outcome' in ' '.join(row['context'])
                await receiver.drain_once(key)
                assert len(writes) == 1
            assert len(harness.queries) == 1
        finally:
            await close(gw)
    asyncio.run(run())


def test_current_grant_revocation_prevents_host_submission(harness):
    async def run():
        env, pages = source_event()
        gw, transport = build(harness, pages, allow_all_users=True)
        receiver = gw._companion_receiver()
        receiver.schedule = lambda key: None
        try:
            receiver.accept(env)
            key = next(iter(receiver.records))
            transport.error = InkboxAPIError(403, {'error': 'activation_revoked'})
            await receiver.drain_once(key)
            assert not harness.queries and not harness.outputs
            assert receiver.records[key].get('revoked')
        finally:
            await close(gw)
    asyncio.run(run())
