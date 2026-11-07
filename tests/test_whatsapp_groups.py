"""Existing WhatsApp groups preserve native session ownership and attention policy."""
import asyncio
import json
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, MagicMock

import pytest

from inkbox_claude import whatsapp
from tests import test_whatsapp as whatsapp_harness
from tests.test_whatsapp import event, CONV, MESSAGE, TIME

setup = whatsapp_harness.setup
from tests.test_sessions import make_session


def group(**changes):
    return {**event()['data']['message'], 'is_group': True, 'group_name': 'Weekend plans',
            'remote_number': None, 'participants': ['+15555550101', '+15555550102'],
            'unresolved_participant_count': 0, **changes}


def envelope(message):
    return {'id': message['id'], 'event_type': 'whatsapp.received', 'data': {'message': message}}


def test_groups_use_conversation_not_contact_sessions(setup, monkeypatch):
    gw, session, _ = setup
    monkeypatch.setattr(whatsapp, 'group_sdk_available', lambda: True)
    messages = [group(), group(id='30000000-0000-4000-8000-000000000004', sender_number='+15555550102'),
                group(id='30000000-0000-4000-8000-000000000005', conversation_id='20000000-0000-4000-8000-000000000006')]
    for message in messages:
        assert asyncio.run(whatsapp.receive(gw, envelope(message))).status == 200
    keys = [call.args[0] for call in gw.sessions.get.call_args_list]
    assert keys[0] == keys[1] != keys[2]
    gw._resolve_contact_full.assert_not_called()
    for call in session.handle_inbound.call_args_list:
        text, mode, meta = call.args
        assert mode == 'whatsapp' and meta['is_group'] and meta['remote_number'] is None
        assert meta['contact'] is None and meta['contact_memories'] == []
        assert '[SILENT]' in text


@pytest.mark.parametrize('changes', [{'sender_number': None}, {'unresolved_participant_count': 1}, {'remote_number': '+15555550101'}, {'participants': []}])
def test_unverifiable_roster_never_wakes(setup, monkeypatch, changes):
    gw, session, _ = setup
    monkeypatch.setattr(whatsapp, 'group_sdk_available', lambda: True)
    assert asyncio.run(whatsapp.receive(gw, envelope(group(**changes)))).status == 200
    session.handle_inbound.assert_not_called()


def test_floor_sdk_acknowledges_group_without_direct_fallback(setup, monkeypatch):
    gw, session, _ = setup
    monkeypatch.setattr(whatsapp, 'group_sdk_available', lambda: False)
    response = asyncio.run(whatsapp.receive(gw, envelope(group())))
    assert json.loads(response.body)['ignored'] == 'group-sdk-upgrade-required'
    session.handle_inbound.assert_not_called()


def test_dropped_group_models_cannot_look_like_direct_chats():
    dropped = NS(remote_number=None, participants=['+15555550101'])
    for value in [dropped, [dropped], NS(messages=[dropped])]:
        with pytest.raises(RuntimeError, match='0.7.29'):
            whatsapp.require_group_metadata(value)


@pytest.mark.parametrize('direct', [True, False])
def test_current_mentions_render_names_without_importing_target_notes(setup, monkeypatch, direct):
    gw, session, _ = setup
    monkeypatch.setattr(whatsapp, 'group_sdk_available', lambda: True)
    gw._resolve_contact_full = AsyncMock(return_value={'name': 'Taylor', 'memories': ['private note']})
    payload = event()['data']['message'] if direct else group()
    payload.update(content='@123 @234 @345 @1234', agent_mentioned=True, mentions=[
        {'text': '@123', 'phone_number': '+15555550100', 'is_agent': True},
        {'text': '@234', 'phone_number': '+15555550102', 'is_agent': False},
        {'text': '@345', 'phone_number': None, 'is_agent': False}])
    asyncio.run(whatsapp.receive(gw, envelope(payload)))
    body, _, meta = session.handle_inbound.call_args.args
    assert '@agent @Taylor @[unresolved mention] @1234' in body
    assert meta['raw_text'] == '@123 @234 @345 @1234'
    assert meta['original_text'] == meta['raw_text']
    assert 'private note' not in repr(meta['mentions'])
    assert meta['whatsapp_mentioned'] is True
    assert not any(call.kwargs.get('value') == '+15555550100' for call in gw._resolve_contact_full.call_args_list)


def test_failed_name_lookup_preserves_verified_phone_and_token_boundaries(setup):
    gw, _, _ = setup
    gw._resolve_contact_full = AsyncMock(side_effect=TimeoutError('private payload'))
    text, mentions = asyncio.run(whatsapp.render_mentions(gw, '@123 @1234 https://example.com/@123', [
        {'text': '@123', 'phone_number': '+15555550102', 'is_agent': False}]))
    assert text == '@+15555550102 @1234 https://example.com/@123'
    assert mentions[0]['display_name'] is None


@pytest.mark.parametrize('native,raw,expected', [(False, '@123 hello', False), (True, '@123 hello', True), (False, '@agent hello', True)])
def test_native_attention_never_uses_rendered_other_person_name(native, raw, expected):
    assert whatsapp.mentioned(raw, {'whatsapp_mentioned': native}, 'agent') is expected
    assert not whatsapp.mentioned('@123', {}, '123')
    assert whatsapp.control_text('@123 yes', {'mentions': [{'text': '@123', 'is_agent': True}]}, 'agent') == 'yes'
    assert whatsapp.control_text('@123 /stop', {'mentions': [{'text': '@123', 'is_agent': False}]}, 'agent') == '@123 /stop'


def test_group_mention_mode_buffers_nonmentioned_input(tmp_path, monkeypatch):
    monkeypatch.setenv('INKBOX_CLAUDE_HOME', str(tmp_path))
    async def run():
        session = make_session([])
        session.cfg.group_reply_mode = 'mention'
        session.buffer_context = MagicMock()
        session.receipt = AsyncMock()
        meta = {**group(), 'channel': 'whatsapp', 'conversation_kind': 'group', 'sender': '+15555550101',
                'raw_text': '@other hello', 'message_id': MESSAGE, 'whatsapp_mentioned': False}
        await session.handle_inbound('Rendered @t mention must not wake', 'whatsapp', meta)
        session.buffer_context.assert_called_once()
        assert session._queue.empty()
        assert session._client is None
        await session.close()
    asyncio.run(run())


@pytest.mark.parametrize('origin', ['direct', 'group'])
def test_actual_destination_blocks_local_disallowed_member_before_send(setup, monkeypatch, origin):
    gw, _, client = setup
    gw.cfg.allow_all_users = False
    gw.cfg.allowed_users = ['+15555550101']
    target = NS(**{**group(), 'id': CONV})
    monkeypatch.setattr(gw._identity, 'get_whatsapp_conversation', lambda _: target)
    monkeypatch.setattr(client, 'get_identity', lambda _: gw._identity)
    monkeypatch.setattr(whatsapp, 'group_sdk_available', lambda: True)
    registered = {}
    whatsapp.register_tools(lambda name, description, properties, required, callback: registered.update({name: callback}), client, 'agent', gw.cfg)
    from inkbox_claude.sessions import _Turn
    from inkbox_claude.tools import CURRENT_SESSION
    session = make_session([])
    source = {**(group() if origin == 'group' else event()['data']['message']),
              'channel': 'whatsapp', 'sender': '+15555550101',
              'conversation_id': '20000000-0000-4000-8000-000000000099'}
    session._current_turn = _Turn(text='Explicit outreach', mode='whatsapp', reply_meta=source)
    token = CURRENT_SESSION.set(session)
    try:
        with pytest.raises(PermissionError, match='participants'):
            registered['inkbox_send_whatsapp']({'conversation_id': CONV, 'text': 'No', 'idempotency_key': 'explicit-group-request'}, lambda: None)
    finally:
        CURRENT_SESSION.reset(token)
    client.whatsapp._http.post.assert_not_called()


def test_standalone_group_request_uses_current_owned_conversation(setup, monkeypatch):
    gw, _, client = setup
    monkeypatch.setattr(whatsapp, 'group_sdk_available', lambda: True)
    monkeypatch.setattr(gw._identity, 'get_whatsapp_conversation', lambda _: NS(**{**group(), 'id': CONV}))
    monkeypatch.setattr(client, 'get_identity', lambda _: gw._identity)
    registered = {}
    whatsapp.register_tools(lambda name, description, properties, required, callback: registered.update({name: callback}), client, 'agent', gw.cfg)
    registered['inkbox_send_whatsapp']({'conversation_id': CONV, 'text': 'Hello', 'idempotency_key': 'explicit-group-request'}, lambda: None)
    payload = client.whatsapp._http.post.call_args.kwargs['json']
    assert payload['conversation_id'] == CONV and 'to' not in payload


def test_compatible_sdk_retains_group_wire_metadata(setup):
    if not whatsapp.group_sdk_available():
        pytest.skip('Dedicated compatible-SDK lane verifies group model decoding')
    gw, _, client = setup
    client.whatsapp._http.get.return_value = {**group(), 'id': CONV, 'created_at': TIME, 'updated_at': TIME}
    conversation = gw._identity.get_whatsapp_conversation(CONV)
    assert conversation.is_group and conversation.group_name == 'Weekend plans'
    assert conversation.remote_number is None


@pytest.mark.parametrize('reaction', [False, True])
def test_changed_current_group_roster_blocks_effects(setup, monkeypatch, reaction):
    monkeypatch.setattr(whatsapp, "group_sdk_available", lambda: True)
    gw, _, client = setup
    gw.cfg.allow_all_users = False
    gw.cfg.allowed_users = ['+15555550101']
    monkeypatch.setattr(client, 'get_identity', lambda _: gw._identity)
    monkeypatch.setattr(gw._identity, 'get_whatsapp_conversation', lambda _: NS(**{**group(), 'id': CONV}))
    monkeypatch.setattr(gw._identity, 'get_whatsapp_message', lambda _: NS(**group()))
    tools = {}
    whatsapp.register_tools(lambda name, description, properties, required, callback: tools.update({name: callback}), client, 'agent', gw.cfg)
    with pytest.raises(PermissionError, match='participants'):
        if reaction:
            tools['inkbox_react_whatsapp']({'message_id': MESSAGE, 'reaction': 'like', 'idempotency_key': 'react-one'}, lambda: None)
        else:
            tools['inkbox_send_whatsapp']({'conversation_id': CONV, 'text': 'No', 'idempotency_key': 'send-one'}, lambda: None)
    client.whatsapp._http.post.assert_not_called()


def test_group_media_and_quote_keep_stable_destination_and_key(setup, monkeypatch):
    monkeypatch.setattr(whatsapp, "group_sdk_available", lambda: True)
    gw, _, client = setup
    monkeypatch.setattr(client, 'get_identity', lambda _: gw._identity)
    monkeypatch.setattr(gw._identity, 'get_whatsapp_conversation', lambda _: NS(**{**group(), 'id': CONV}))
    tools = {}
    whatsapp.register_tools(lambda name, description, properties, required, callback: tools.update({name: callback}), client, 'agent', gw.cfg)
    from tests.test_whatsapp import MEDIA
    tools['inkbox_send_whatsapp']({'conversation_id': CONV, 'reply_to_message_id': MESSAGE,
        'media_id': MEDIA, 'text': 'Attachment', 'idempotency_key': 'same-logical-effect'}, lambda: None)
    call = client.whatsapp._http.post.call_args
    assert call.kwargs['json'] == {'conversation_id': CONV, 'reply_to_message_id': MESSAGE,
                                 'media_ids': [MEDIA], 'text': 'Attachment'}
    assert 'same-logical-effect' in repr(call)


def test_native_self_mention_approves_only_matching_group_permission():
    from inkbox_claude.escalation import PendingInteraction
    async def run():
        session = make_session([])
        session._worker = asyncio.current_task()
        future = asyncio.get_running_loop().create_future()
        route = {**group(), 'channel': 'whatsapp', 'conversation_kind': 'group', 'sender': '+15555550101'}
        session.pending = PendingInteraction(kind='permission', prompt_text='Approve?', future=future,
            sender='+15555550101', route=route)
        source = {**route, 'raw_text': '@123 yes', 'whatsapp_mentioned': True,
                  'mentions': [{'text': '@123', 'is_agent': True, 'phone_number': None}]}
        await session.handle_inbound('@agent yes', 'whatsapp', source)
        assert future.done() and future.result() == 'yes'
        assert session._queue.empty()
    asyncio.run(run())


def test_burst_keeps_later_native_mention_and_each_original_source():
    from inkbox_claude.sessions import _Turn
    async def run():
        session = make_session([])
        route = {**group(), 'channel': 'whatsapp', 'sender': '+15555550101'}
        first = _Turn(text='@Taylor first', mode='whatsapp', queued_at=0, reply_meta={**route,
            'whatsapp_mentioned': False, 'mentions': [{'text':'@111', 'display_name':'Taylor'}],
            'imessage_sources':[{'id':'first','text':'@Taylor first','original_text':'@111 first'}]})
        second = _Turn(text='@agent second', mode='whatsapp', queued_at=0, reply_meta={**route,
            'whatsapp_mentioned': True, 'mentions': [{'text':'@222', 'is_agent':True}],
            'imessage_sources':[{'id':'second','text':'@agent second','original_text':'@222 second'}]})
        session._batching_turn = first
        await session._queue.put(second)
        result = await session._batch(first)
        assert result.reply_meta['whatsapp_mentioned'] is True
        assert len(result.reply_meta['mentions']) == 2
        assert [s['original_text'] for s in result.reply_meta['imessage_sources']] == ['@111 first','@222 second']
    asyncio.run(run())


def test_group_reaction_is_quiet_context_not_a_new_command(setup, monkeypatch):
    from inkbox_claude.whatsapp_v2 import receive_reaction
    from tests.test_whatsapp import OWNER
    gw, session, _ = setup
    monkeypatch.setattr(whatsapp, 'group_sdk_available', lambda: True)
    reaction = {'id':'50000000-0000-4000-8000-000000000005', 'target_message_id':MESSAGE,
        'conversation_id':CONV, 'agent_identity_id':OWNER, 'direction':'inbound',
        'remote_number':'+15555550101', 'revision':1, 'status':'applied', 'reaction':'like'}
    monkeypatch.setattr(gw._identity, 'get_whatsapp_reaction', lambda _: NS(**reaction))
    monkeypatch.setattr(gw._identity, 'get_whatsapp_message', lambda _: NS(**group(content='/stop')))
    session.buffer_context = MagicMock()
    assert asyncio.run(receive_reaction(gw, {'data':{'reaction':reaction}})).status == 200
    session.handle_inbound.assert_not_called()
    session.buffer_context.assert_called_once()
    assert '/stop' not in session.buffer_context.call_args.args[0]
    gw._resolve_contact_full.assert_not_called()


def test_mcp_wrapper_keeps_actionable_group_sdk_requirement(setup, monkeypatch):
    from inkbox_claude.channel_tools import build_channel_tools
    gw, _, client = setup
    monkeypatch.setattr(whatsapp, 'group_sdk_available', lambda: False)
    monkeypatch.setattr(client, 'get_identity', lambda _: gw._identity)
    monkeypatch.setattr(gw._identity, 'get_whatsapp_conversation', lambda **kwargs: NS(**{**group(), 'id': CONV}))
    tool = next(tool for tool in build_channel_tools(client, 'agent', gw.cfg) if tool.name == 'inkbox_get_whatsapp_conversation')
    result = asyncio.run(tool.handler({'conversation_id': CONV}))
    assert 'WhatsApp groups require Inkbox SDK 0.7.29 or newer' in result['content'][0]['text']


def test_mention_lookup_failure_does_not_log_number_or_remote_error(setup, caplog):
    from inkbox_claude.gateway import InkboxGateway
    gw, _, client = setup
    client.contacts.lookup = MagicMock(side_effect=RuntimeError('private remote detail'))
    caplog.set_level('DEBUG', logger='inkbox_claude.gateway')
    assert asyncio.run(InkboxGateway._resolve_contact_full(gw, kind='phone', value='+15555550109')) is None
    assert '+15555550109' not in caplog.text
    assert 'private remote detail' not in caplog.text
    assert 'contact lookup unavailable' in caplog.text
