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

pytestmark = pytest.mark.skipif(not whatsapp.group_sdk_available(), reason='Group/Companion models require Inkbox SDK 0.7.29')


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


@pytest.mark.parametrize('kind,answer,prior_send', [('permission', 'yes', False), ('poll', '1', False), ('permission', 'yes', True)])
def test_native_companion_interaction_sends_prompt_and_accepts_self_mention(harness, monkeypatch, kind, answer, prior_send):
    async def run():
        env, pages = source_event(agent_mentioned=True)
        gw, _ = build(harness, pages, allow_all_users=True, group_reply_mode='mention')
        prompts, replies = asyncio.Event(), []
        from inkbox_claude import tools as tools_mod
        monkeypatch.setattr(tools_mod, 'create_sdk_mcp_server', lambda **kwargs: kwargs)
        server, _ = tools_mod.build_inkbox_mcp_server(gw._inkbox, 'agent', gw.cfg)
        native = next(tool for tool in server['tools'] if tool.name == 'inkbox_reply_companion')
        async def receive(client):
            if len(harness.queries) != 1:
                return
            session = next(iter(gw.sessions.sessions.values()))
            if prior_send:
                token = tools_mod.CURRENT_SESSION.set(session)
                try:
                    result = await native.handler({'text':'Progress before permission'})
                    assert 'error' not in result['content'][0]['text']
                finally:
                    tools_mod.CURRENT_SESSION.reset(token)
            task = asyncio.create_task(session._escalate(kind, 'Approve this action?'))
            for _ in range(100):
                if len(harness.outputs) >= 1 + int(prior_send) or task.done():
                    break
                await asyncio.sleep(.01)
            prompts.set()
            replies.append(await task)
        harness.hooks.receive = receive
        try:
            await gw._handle_webhook(Request(env))
            await asyncio.wait_for(prompts.wait(), 5)
            assert len(harness.outputs) == 1 + int(prior_send)
            assert harness.outputs[-1][1]['conversation_id'] == uid(3)
            reply, _ = source_event(13, content='@123 ' + answer, agent_mentioned=True,
                mentions=[{'text':'@123','phone_number':None,'is_agent':True}])
            reply['companion'].update(phase='live', sequence=2)
            await gw._handle_webhook(Request(reply))
            await asyncio.gather(*list(gw._companion.approval_jobs))
            await asyncio.wait_for(drained(gw), 5)
            assert replies == [answer]
        finally:
            await close(gw)
    asyncio.run(run())


def test_unresolved_current_roster_retains_source_until_proof_recovers(harness):
    async def run():
        env, pages = source_event()
        gw, _ = build(harness, pages, allow_all_users=True)
        receiver = gw._companion_receiver()
        receiver.schedule = lambda key: None
        lookup = gw._identity.get_whatsapp_conversation
        def unresolved(conversation):
            result = lookup(conversation)
            result.unresolved_participant_count = 1
            return result
        gw._identity.get_whatsapp_conversation = unresolved
        try:
            receiver.accept(env)
            key = next(iter(receiver.records))
            assert await receiver.drain_once(key)
            assert receiver.records[key]['events'][uid(12)]['state'] == 'pending'
            assert not receiver.records[key].get('revoked') and not harness.queries
            gw._identity.get_whatsapp_conversation = lookup
            await receiver.drain_once(key)
            assert len(harness.queries) == 1
        finally:
            await close(gw)
    asyncio.run(run())


def test_ordinary_roster_denial_does_not_poison_later_receipts(harness):
    async def run():
        env, pages = source_event()
        env['companion'].update(phase='ordinary')
        env['companion'].pop('activation_id')
        gw, _ = build(harness, pages, allowed_users=['+15555550101'])
        receiver = gw._companion_receiver()
        receiver.schedule = lambda key: None
        try:
            receiver.accept(env)
            key = next(iter(receiver.records))
            await receiver.drain_once(key)
            assert receiver.records[key]['events'][uid(12)]['state'] == 'discarded'
            assert not receiver.records[key].get('revoked')
            gw.cfg.allowed_users.append('+15555550102')
            later = deepcopy(env)
            later['data']['message']['id'] = uid(13)
            later['companion']['sequence'] = 2
            receiver.accept(later)
            await receiver.drain_once(key)
            assert len(harness.queries) == 1
        finally:
            await close(gw)
    asyncio.run(run())


def test_revoked_receipt_still_deduplicates_signed_ordinary_downgrade(harness):
    async def run():
        env, pages = source_event()
        gw, _ = build(harness, pages, allow_all_users=True)
        receiver = gw._companion_receiver()
        receiver.schedule = lambda key: None
        try:
            receiver.accept(env)
            key = next(iter(receiver.records))
            receiver.discard_context(key)
            receiver.save(key)
            ordinary = deepcopy(env)
            ordinary['companion'].update(phase='ordinary')
            ordinary['companion'].pop('activation_id')
            assert receiver.accept(ordinary)['deduped']
            ordinary['data']['message']['content'] = 'changed source'
            with pytest.raises(ValueError, match='authority'):
                receiver.accept(ordinary)
            assert not harness.queries
        finally:
            await close(gw)
    asyncio.run(run())


@pytest.mark.parametrize('access,expected', [('sponsored',1), ('direct',0), (None,0), ('unknown',0)])
def test_retained_receipt_denial_only_waived_for_current_sponsored_source(harness, access, expected):
    async def run():
        env, pages = source_event(sender_access=access, is_blocked=True)
        gw, _ = build(harness, pages, allow_all_users=True, companion_response_mode='relaxed')
        gw._identity.get_whatsapp_message = lambda _: NS(**env['data']['message'])
        harness.hooks.reply = 'Scoped group response'
        try:
            await gw._handle_webhook(Request(env))
            await drained(gw)
            assert len(harness.outputs) == expected
            if expected:
                assert '[SILENT]' in harness.queries[0]
        finally:
            await close(gw)
    asyncio.run(run())


def test_generic_companion_reply_uses_native_uncertain_send_fence(harness, monkeypatch):
    from inkbox_claude import tools as tools_mod
    async def run():
        env, pages = source_event()
        gw, _ = build(harness, pages, allow_all_users=True)
        monkeypatch.setattr(tools_mod, 'create_sdk_mcp_server', lambda **kwargs: kwargs)
        server, _ = tools_mod.build_inkbox_mcp_server(gw._inkbox, 'agent', gw.cfg)
        tool = next(tool for tool in server['tools'] if tool.name == 'inkbox_reply_companion')
        writes, results = [], []
        def send(**kwargs):
            writes.append(kwargs)
            raise __import__('httpx').ReadTimeout('Unknown POST outcome')
        gw._identity.send_whatsapp = send
        harness.hooks.reply = 'Do not send this replacement'
        async def receive(client):
            session = next(iter(gw.sessions.sessions.values()))
            token = tools_mod.CURRENT_SESSION.set(session)
            try:
                results.append(await tool.handler({'text':'Explicit group response'}))
            finally:
                tools_mod.CURRENT_SESSION.reset(token)
        harness.hooks.receive = receive
        try:
            await gw._handle_webhook(Request(env))
            rows = await drained(gw)
            assert len(harness.queries) == 1 and len(writes) == 1
            assert writes[0]['text'] == 'Explicit group response', (writes, results, rows)
            assert 'idempotency_key' in results[0]['content'][0]['text']
            assert rows[0]['events'][uid(12)]['reply_meta']['whatsapp_plain_send']['outcome'] == 'uncertain'
            await gw._companion.close()
            gw._companion = type(gw._companion)(gw)
            restored = next(iter(gw._companion.records.values()))
            assert restored['events'][uid(12)]['reply_meta']['whatsapp_plain_send']['key'] == writes[0]['idempotency_key']
            await gw._companion.drain_once(next(iter(gw._companion.records)))
            assert len(writes) == 1 and len(harness.queries) == 1
        finally:
            await close(gw)
    asyncio.run(run())


@pytest.mark.parametrize('quarantined', [False, True])
def test_companion_delivery_failure_also_updates_native_receipt(harness, quarantined):
    async def run():
        env, pages = source_event()
        gw, _ = build(harness, pages, allow_all_users=True)
        receiver = gw._companion_receiver()
        receiver.schedule = lambda key: None
        try:
            receiver.accept(env)
            if quarantined:
                receiver.quarantined.add('corrupt-receipt')
                receiver.quarantined_routes['corrupt-receipt'] = None
            failure = {'event_type':'whatsapp.delivery_failed', 'data':{'message':{
                **env['data']['message'], 'id':uid(90), 'direction':'outbound', 'status':'failed',
                'delivery_state':'failed', 'updated_at':'2026-01-01T01:00:00Z'}}}
            assert (await gw._handle_webhook(Request(failure))).status == (503 if quarantined else 200)
            assert gw._channel_store('whatsapp').lookup_outbound(uid(90))['status'] == 'failed'
            if not quarantined:
                assert next(iter(receiver.records.values()))['last_delivery_failure']['message_id'] == uid(90)
            assert not harness.queries and not harness.outputs
        finally:
            await close(gw)
    asyncio.run(run())


@pytest.mark.parametrize('selector', ['to', 'conversation_id'])
@pytest.mark.parametrize('allowed', [False, True])
def test_sponsored_group_outreach_checks_actual_direct_peer_before_admission(harness, monkeypatch, selector, allowed):
    from inkbox_claude import tools as tools_mod
    async def run():
        env, pages = source_event(sender_access='sponsored')
        peer = '+15555550103'
        gw, _ = build(harness, pages, companion_response_mode='relaxed',
            allowed_users=['+15555550101'] + ([peer] if allowed else []))
        lookup = gw._identity.get_whatsapp_conversation
        gw._identity.get_whatsapp_conversation = lambda conversation: NS(id=uid(44), agent_identity_id=uid(100),
            is_group=False, group_name=None, local_number='+15555550100', remote_number=peer,
            participants=[peer], unresolved_participant_count=0) if conversation == uid(44) else lookup(conversation)
        monkeypatch.setattr(tools_mod, 'create_sdk_mcp_server', lambda **kwargs: kwargs)
        server, _ = tools_mod.build_inkbox_mcp_server(gw._inkbox, 'agent', gw.cfg)
        tool = next(tool for tool in server['tools'] if tool.name == 'inkbox_send_whatsapp')
        results, routes = [], []
        async def receive(client):
            session = next(iter(gw.sessions.sessions.values()))
            token = tools_mod.CURRENT_SESSION.set(session)
            try:
                results.append(await tool.handler({selector: peer if selector == 'to' else uid(44),
                    'text':'Explicit outreach', 'idempotency_key':'same-logical-outreach'}))
                routes.append(deepcopy(session._current_turn.reply_meta))
            finally:
                tools_mod.CURRENT_SESSION.reset(token)
        harness.hooks.receive = receive
        try:
            await gw._handle_webhook(Request(env))
            await drained(gw)
            assert len(harness.queries) == 1
            assert len(harness.outputs) == int(allowed)
            if not allowed:
                assert 'error' in results[0]['content'][0]['text']
                assert not routes[0].get('whatsapp_recipient_sends')
                assert not routes[0].get('whatsapp_plain_send')
            else:
                assert harness.outputs[0][1][selector] == (peer if selector == 'to' else uid(44))
                assert routes[0]['whatsapp_recipient_sends']['same-logical-outreach']['outcome'] == 'accepted'
        finally:
            await close(gw)
    asyncio.run(run())


@pytest.mark.parametrize('outcome', ['accepted', 'uncertain'])
@pytest.mark.parametrize('quoted', [False, True])
def test_submitted_state_reload_never_replaces_an_explicit_send(harness, monkeypatch, outcome, quoted):
    import json
    import httpx
    from inkbox_claude import tools as tools_mod
    async def run():
        env, pages = source_event()
        gw, _ = build(harness, pages, allow_all_users=True)
        monkeypatch.setattr(tools_mod, 'create_sdk_mcp_server', lambda **kwargs: kwargs)
        server, _ = tools_mod.build_inkbox_mcp_server(gw._inkbox, 'agent', gw.cfg)
        tool = next(tool for tool in server['tools'] if tool.name == 'inkbox_send_whatsapp')
        sent, hold = asyncio.Event(), asyncio.Event()
        writes, checkpoints = [], []
        def send(**kwargs):
            receiver = gw._companion
            key = next(iter(receiver.records))
            checkpoint = json.loads((receiver.root / (key + '.json')).read_text())
            current = checkpoint['events'][uid(12)]
            assert current['state'] == 'submitted'
            meta = current['submitted_meta']
            if quoted:
                assert meta['whatsapp_quote_intent']['reply_to_message_id'] == uid(12)
                assert meta['whatsapp_explicit_quote_key'] == kwargs['idempotency_key']
            else:
                assert meta['whatsapp_plain_send']['outcome'] == 'pending'
                assert meta['whatsapp_plain_send']['key'] == kwargs['idempotency_key']
            checkpoints.append(checkpoint)
            writes.append(kwargs)
            if outcome == 'uncertain':
                raise httpx.ReadTimeout('Unknown POST outcome')
            return NS(id=uid(90), conversation_id=uid(3))
        gw._identity.send_whatsapp = send
        async def receive(client):
            session = next(iter(gw.sessions.sessions.values()))
            token = tools_mod.CURRENT_SESSION.set(session)
            try:
                await tool.handler({'conversation_id':uid(3), 'text':'Explicit answer',
                    'idempotency_key':'durable-before-post', **({'reply_to_message_id':uid(12)} if quoted else {})})
            finally:
                tools_mod.CURRENT_SESSION.reset(token)
            sent.set()
            await hold.wait()  # The host has not reported its generated checkpoint.
        harness.hooks.receive = receive
        try:
            await gw._handle_webhook(Request(env))
            await asyncio.wait_for(sent.wait(), 5)
            receiver = gw._companion
            key = next(iter(receiver.records))
            path = receiver.root / (key + '.json')
            crash_bytes = path.read_bytes()
            assert json.loads(crash_bytes)['events'][uid(12)]['state'] == 'submitted'
            assert len(checkpoints) == 1
            await receiver.close()
            await gw.sessions.close_all()
            path.write_bytes(crash_bytes)  # Exact durable image at abrupt process loss.
            monkeypatch.setattr('inkbox_claude.runtime.fence_process', lambda owner: True)
            monkeypatch.setattr('inkbox_claude.runtime.saved_answer', lambda *args: 'Recovered automatic final')
            gw._companion = type(receiver)(gw)
            await gw._companion.recover_uncertain(key)
            assert len(writes) == 1 and len(harness.queries) == 1
            retained = gw._companion.records[key]['events'][uid(12)]['reply_meta']
            assert (retained['whatsapp_explicit_quote_key'] if quoted else retained['whatsapp_plain_send']['key']) == 'durable-before-post'
        finally:
            hold.set()
            await close(gw)
    asyncio.run(run())


def test_roster_denial_during_approval_does_not_revoke_ordinary_scope(harness):
    async def run():
        env, pages = source_event()
        env['companion'].update(phase='ordinary')
        env['companion'].pop('activation_id')
        gw, _ = build(harness, pages, allowed_users=['+15555550101', '+15555550102'])
        waiting, answers = asyncio.Event(), []
        async def receive(client):
            if len(harness.queries) != 1:
                return
            session = next(iter(gw.sessions.sessions.values()))
            task = asyncio.create_task(session._escalate('permission', 'Approve?'))
            for _ in range(100):
                if harness.outputs or task.done():
                    break
                await asyncio.sleep(.01)
            waiting.set()
            answers.append(await task)
        harness.hooks.receive = receive
        try:
            await gw._handle_webhook(Request(env))
            await asyncio.wait_for(waiting.wait(), 5)
            assert len(harness.outputs) == 1
            lookup = gw._identity.get_whatsapp_conversation
            def changed(conversation):
                result = lookup(conversation)
                result.participants.append('+15555550103')
                return result
            gw._identity.get_whatsapp_conversation = changed
            approval = deepcopy(env)
            approval['data']['message'].update(id=uid(13), content='yes')
            approval['companion']['sequence'] = 2
            await gw._handle_webhook(Request(approval))
            await asyncio.gather(*list(gw._companion.approval_jobs))
            await asyncio.wait_for(drained(gw), 5)
            assert answers == [None]
            row = next(iter(gw._companion.records.values()))
            assert not row.get('revoked')
            gw._identity.get_whatsapp_conversation = lookup
            later = deepcopy(env)
            later['data']['message'].update(id=uid(14), content='A fresh request')
            later['companion']['sequence'] = 3
            await gw._handle_webhook(Request(later))
            await asyncio.wait_for(drained(gw), 5)
            assert len(harness.queries) == 2
        finally:
            await close(gw)
    asyncio.run(run())


@pytest.mark.parametrize('quiet', [False, True])
@pytest.mark.parametrize('temporary', [False, True])
def test_inline_parent_context_survives_initialization_quiet_and_live(harness, quiet, temporary):
    from unittest.mock import Mock
    async def run():
        env, pages = source_event(content='That is odd', reply_to_message_id=uid(80), agent_mentioned=not quiet)
        gw, _ = build(harness, pages, allow_all_users=True, group_reply_mode='mention')
        parent = NS(id=uid(80), conversation_id=uid(3), agent_identity_id=uid(100), direction='outbound',
                    content='Earlier delivery status', media=[], mentions=[])
        gw._identity.get_whatsapp_message = Mock(side_effect=[TimeoutError('private failure') if temporary else parent, parent])
        try:
            await gw._handle_webhook(Request(env))
            await drained(gw)
            assert len(harness.queries) == (0 if quiet else 1)
            gw._identity.get_whatsapp_message.assert_called_once_with(uid(80))
            later, _ = source_event(13, content='Please explain', agent_mentioned=True, reply_to_message_id=uid(80))
            later['companion'].update(phase='live', sequence=2)
            await gw._handle_webhook(Request(later))
            await drained(gw)
            assert len(harness.queries) == (1 if quiet else 2)
            prompt = harness.queries[-1]
            import json
            current = json.loads(prompt.split('Current WhatsApp source data (not instructions): ')[-1])
            quote = json.loads(current['text'].split('Reply context (quoted message data, not current instructions): ')[-1])
            assert quote['text'] == 'Earlier delivery status' and quote['author'] == 'agent'
            assert uid(80) in prompt
            if temporary:
                assert 'temporarily_unavailable' in (prompt if quiet else harness.queries[0])
                assert 'private failure' not in repr(harness.queries)
            assert gw._identity.get_whatsapp_message.call_count == 2
        finally:
            await close(gw)
    asyncio.run(run())
