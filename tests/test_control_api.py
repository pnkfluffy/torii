import asyncio
from argparse import Namespace
from contextlib import redirect_stdout
from io import StringIO
import itertools
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch

from coordinator import control_api, mcp
from coordinator.controls import handle_control
from coordinator.service import Service
from coordinator.store import Store


ORIGIN_MAIN_PARENT_API = [{'params': {'provider': 'codex', 'model': 'gpt-6-luna'},
  'ok': True,
  'text': 'Parent chats: codex gpt-6-luna. Active turns and native background tasks settle before a change. '
          'Worker settings stay unchanged.',
  'data': {'parent_selection': {'provider': 'codex', 'model': 'gpt-6-luna'}},
  'state': 'done',
  'settings': [['parent_selection', '{"provider": "codex", "model": "gpt-6-luna"}'],
               ['parent_selection_revision', '1']]},
 {'params': {'provider': 'codex'},
  'ok': False,
  'text': 'Supply both provider and model, or omit both for automatic parent selection.',
  'data': None,
  'state': 'refused',
  'settings': [['parent_selection', '{"provider": "codex", "model": "gpt-6-luna"}'],
               ['parent_selection_revision', '1']]},
 {'params': {},
  'ok': True,
  'text': 'Parent chats: automatic provider and model selection. Active turns and native background tasks settle '
          'before a change. Worker settings stay unchanged.',
  'data': {'parent_selection': None},
  'state': 'done',
  'settings': [['parent_selection', 'null'], ['parent_selection_revision', '2']]},
 {'params': {'topic': '1:2', 'provider': 'codex', 'model': 'gpt-6-luna', 'effort': 'low'},
  'ok': True,
  'text': 'Chat 1:2 agent: codex gpt-6-luna · low thinking. Active turns and native background tasks settle '
          'before a change. Worker settings stay unchanged.',
  'data': {'topic': '1:2',
           'selection': {'provider': 'codex', 'model': 'gpt-6-luna', 'effort': 'low', 'revision': 1}},
  'state': 'done',
  'settings': [['coordinator_selection',
                '{"provider": "codex", "model": "gpt-6-luna", "effort": "low", "revision": 1}'],
               ['parent_selection', 'null'],
               ['parent_selection_revision', '2']]},
 {'params': {'topic': '1:2', 'provider': 'codex', 'model': 'gpt-6-luna', 'effort': 'low'},
  'ok': True,
  'text': 'Chat 1:2 agent is already set to this provider, model and thinking.',
  'data': {'topic': '1:2',
           'selection': {'provider': 'codex', 'model': 'gpt-6-luna', 'effort': 'low', 'revision': 1}},
  'state': 'done',
  'settings': [['coordinator_selection',
                '{"provider": "codex", "model": "gpt-6-luna", "effort": "low", "revision": 1}'],
               ['parent_selection', 'null'],
               ['parent_selection_revision', '2']]},
 {'params': {'topic': '1:2', 'provider': 'claude'},
  'ok': True,
  'text': 'Chat 1:2 agent: claude provider default · default thinking. Active turns and native background tasks '
          'settle before a change. Worker settings stay unchanged.',
  'data': {'topic': '1:2', 'selection': {'provider': 'claude', 'model': None, 'effort': None, 'revision': 2}},
  'state': 'done',
  'settings': [['coordinator_selection', '{"provider": "claude", "model": null, "effort": null, "revision": 2}'],
               ['parent_selection', 'null'],
               ['parent_selection_revision', '2']]},
 {'params': {'topic': '1:2', 'effort': 'low'},
  'ok': False,
  'text': 'Supply provider with model or effort, or supply topic alone to clear the chat pick.',
  'data': None,
  'state': 'refused',
  'settings': [['coordinator_selection', '{"provider": "claude", "model": null, "effort": null, "revision": 2}'],
               ['parent_selection', 'null'],
               ['parent_selection_revision', '2']]},
 {'params': {'topic': '1:2'},
  'ok': True,
  'text': 'Chat 1:2 agent: default provider, model and thinking. Active turns and native background tasks settle '
          'before a change. Worker settings stay unchanged.',
  'data': {'topic': '1:2', 'selection': None},
  'state': 'done',
  'settings': [['coordinator_selection', 'null'],
               ['coordinator_selection_cleared', 'true'],
               ['parent_selection', 'null'],
               ['parent_selection_revision', '2']]}]


ORIGIN_MAIN_VALID_PARENT_PICKS = [{'params': {'topic': '1:2', 'provider': 'claude', 'model': 'claude-opus-5-5'},
  'ok': True,
  'text': 'Chat 1:2 agent: claude claude-opus-5-5 · default thinking. Active turns and native background '
          'tasks settle before a change. Worker settings stay unchanged.',
  'data': {'topic': '1:2',
           'selection': {'provider': 'claude', 'model': 'claude-opus-5-5', 'effort': None, 'revision': 1}},
  'state': 'done',
  'settings': [['coordinator_selection',
                '{"provider": "claude", "model": "claude-opus-5-5", "effort": null, "revision": 1}']],
  'outbox': [],
  'requests': []},
 {'params': {'topic': '1:2', 'provider': 'claude', 'model': 'opus'},
  'ok': True,
  'text': 'Chat 1:2 agent: claude opus · default thinking. Active turns and native background tasks settle '
          'before a change. Worker settings stay unchanged.',
  'data': {'topic': '1:2',
           'selection': {'provider': 'claude', 'model': 'opus', 'effort': None, 'revision': 2}},
  'state': 'done',
  'settings': [['coordinator_selection',
                '{"provider": "claude", "model": "opus", "effort": null, "revision": 2}']],
  'outbox': [],
  'requests': []},
 {'params': {'topic': '1:2', 'provider': 'claude', 'model': 'fable'},
  'ok': True,
  'text': 'Chat 1:2 agent: claude fable · default thinking. Active turns and native background tasks settle '
          'before a change. Worker settings stay unchanged.',
  'data': {'topic': '1:2',
           'selection': {'provider': 'claude', 'model': 'fable', 'effort': None, 'revision': 3}},
  'state': 'done',
  'settings': [['coordinator_selection',
                '{"provider": "claude", "model": "fable", "effort": null, "revision": 3}']],
  'outbox': [],
  'requests': []},
 {'params': {'topic': '1:2', 'provider': 'codex', 'model': 'gpt-6-luna'},
  'ok': True,
  'text': 'Chat 1:2 agent: codex gpt-6-luna · default thinking. Active turns and native background tasks '
          'settle before a change. Worker settings stay unchanged.',
  'data': {'topic': '1:2',
           'selection': {'provider': 'codex', 'model': 'gpt-6-luna', 'effort': None, 'revision': 4}},
  'state': 'done',
  'settings': [['coordinator_selection',
                '{"provider": "codex", "model": "gpt-6-luna", "effort": null, "revision": 4}']],
  'outbox': [],
  'requests': []},
 {'params': {'topic': '1:2', 'provider': 'codex'},
  'ok': True,
  'text': 'Chat 1:2 agent: codex provider default · default thinking. Active turns and native background '
          'tasks settle before a change. Worker settings stay unchanged.',
  'data': {'topic': '1:2', 'selection': {'provider': 'codex', 'model': None, 'effort': None, 'revision': 5}},
  'state': 'done',
  'settings': [['coordinator_selection',
                '{"provider": "codex", "model": null, "effort": null, "revision": 5}']],
  'outbox': [],
  'requests': []},
 {'params': {'topic': '1:2'},
  'ok': True,
  'text': 'Chat 1:2 agent: default provider, model and thinking. Active turns and native background tasks '
          'settle before a change. Worker settings stay unchanged.',
  'data': {'topic': '1:2', 'selection': None},
  'state': 'done',
  'settings': [['coordinator_selection', 'null'], ['coordinator_selection_cleared', 'true']],
  'outbox': [],
  'requests': []},
 {'params': {'provider': 'claude', 'model': 'claude-opus-5-5'},
  'ok': True,
  'text': 'Parent chats: claude claude-opus-5-5. Active turns and native background tasks settle before a '
          'change. Worker settings stay unchanged.',
  'data': {'parent_selection': {'provider': 'claude', 'model': 'claude-opus-5-5'}},
  'state': 'done',
  'settings': [['coordinator_selection', 'null'],
               ['coordinator_selection_cleared', 'true'],
               ['parent_selection', '{"provider": "claude", "model": "claude-opus-5-5"}'],
               ['parent_selection_revision', '1']],
  'outbox': [],
  'requests': []},
 {'params': {},
  'ok': True,
  'text': 'Parent chats: automatic provider and model selection. Active turns and native background tasks '
          'settle before a change. Worker settings stay unchanged.',
  'data': {'parent_selection': None},
  'state': 'done',
  'settings': [['coordinator_selection', 'null'],
               ['coordinator_selection_cleared', 'true'],
               ['parent_selection', 'null'],
               ['parent_selection_revision', '2']],
  'outbox': [],
  'requests': []}]


ORIGIN_MAIN_CHAT_CONTROL = {
    ('policy.chat', 'set'): {
        'text': 'Chat 1:3 delegation instructions saved (23 characters). Its agent follows them from its next message. Worker settings stay unchanged.',
        'data': {'topic': '1:3', 'delegation': {'text': 'Use Codex for research.', 'revision': 1}},
        'setting': ('coordinator:1:3_delegation', '{"text": "Use Codex for research.", "revision": 1}'),
        'note': 'Delegation updated.',
    },
    ('policy.chat', 'clear'): {
        'text': 'Chat 1:3 delegation instructions cleared. Its agent follows the usage policy from its next message.',
        'data': {'topic': '1:3', 'delegation': None},
        'setting': ('coordinator:1:3_delegation', 'null'),
        'note': 'Delegation cleared.',
    },
    ('model.parent', 'set'): {
        'text': 'Chat 1:3 agent: codex gpt-6-luna · default thinking. Active turns and native background tasks settle before a change. Worker settings stay unchanged.',
        'data': {'topic': '1:3', 'selection': {'provider': 'codex', 'model': 'gpt-6-luna', 'effort': None, 'revision': 1}},
        'setting': ('coordinator:1:3_selection', '{"provider": "codex", "model": "gpt-6-luna", "effort": null, "revision": 1}'),
        'note': None,
    },
    ('model.parent', 'clear'): {
        'text': 'Chat 1:3 agent: default provider, model and thinking. Active turns and native background tasks settle before a change. Worker settings stay unchanged.',
        'data': {'topic': '1:3', 'selection': None},
        'setting': ('coordinator:1:3_selection', 'null'),
        'cleared': ('coordinator:1:3_selection_cleared', 'true'),
        'note': None,
    },
}


CLOSING_CHAT_REFUSAL = 'This chat is closing or closed.'


class ControlApiTests(unittest.TestCase):
    def test_invalid_parent_provider_names_all_accepted_providers(self):
        result = self.call('model.parent', {'topic': '1:2', 'provider': 'invalid', 'model': 'test-model'})
        self.assertFalse(result.ok, result.text)
        self.assertEqual(result.text, 'Send claude, codex or opencode for provider.')

    def test_parent_provider_kind_accepts_opencode_topic_bind_does_not(self):
        self.store.put('opencode_enabled', True)
        self.store.put('opencode_accounts', {'example-host': {'model': 'example-model'}})
        self.store.put('opencode_active_account', 'example-host')
        ctx = control_api.Context('cli', '1:2')
        for op, params in (
                ('model.parent', {'topic': '1:2', 'provider': 'opencode'}),
                ('topic.create', {'name': 'OpenCode', 'provider': 'opencode'})):
            with self.subTest(op=op):
                clean, problem = control_api.validate(self.store, control_api.BY_ID[op], params, ctx)
                self.assertIsNone(problem)
                self.assertEqual(clean['provider'], 'opencode')
        before = list(self.store.db.execute('SELECT * FROM topics'))
        result = self.call('topic.bind', {'topic': '1:2', 'cwd': self.temp.name, 'name': 'OpenCode',
                                         'provider': 'opencode', 'session': 'ses_imported'})
        self.assertFalse(result.ok, result.text)
        self.assertEqual(result.text, 'Send claude or codex for provider.')
        self.assertEqual(list(self.store.db.execute('SELECT * FROM topics')), before)

    def test_moved_home_chat_refuses_opencode_without_saving_or_queuing(self):
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES('1:3',1,3,'new home',?,1)",
                                  (self.temp.name,))
            self.store.put('coordinator_session_topic', '1:2')
            self.store.put('coordinator_home_topic', '1:3')
            self.store.put('opencode_enabled', True)
            self.store.put('opencode_accounts', {'example-host': {'model': 'example-model'}})
            self.store.put('opencode_active_account', 'example-host')
        key = 'coordinator:1:3'
        queued_before = self.store.db.execute('SELECT count(*) FROM service_requests').fetchone()[0]
        result = self.call('model.parent', {'topic': '1:3', 'provider': 'opencode'})
        self.assertFalse(result.ok, result.text)
        self.assertEqual(result.text, 'The home chat stays on Claude or ChatGPT.')
        self.assertIsNone(self.store.get(key + '_selection'))
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM service_requests').fetchone()[0], queued_before)

    def test_chat_policy_refuses_surrogates_and_controls_without_writes_on_cli_and_mcp(self):
        from coordinator.__main__ import ctl
        self.store.put('coordinator_home_topic', '1:2')
        message = self.store.message_save('1:2', 'owner', 'Set delegation.')['id']
        self.assertTrue(self.call('policy.chat', {'topic': '1:2', 'text': 'Existing instructions.'}).ok)
        cases = [('\ud800', 'surrogate characters'), ('\udfff', 'surrogate characters')]
        cases += [(chr(code), 'control characters other than newline and tab')
                  for code in (*range(32), *range(127, 160)) if code not in (9, 10)]
        for source in ('cli', 'mcp'):
            for character, character_class in cases:
                for text in ('Use Sol.' + character + 'Use Claude.', character + 'Use Sol.' + character):
                    with self.subTest(source=source, text=repr(text)):
                        before = self.pick_snapshot()
                        changes = self.store.db.total_changes
                        refusal = 'Chat delegation instructions cannot contain ' + character_class + '.'
                        if source == 'cli':
                            output = StringIO()
                            args = Namespace(operation='call', op='policy.chat', topic=None,
                                             pairs=['topic=1:2', 'text=' + text])
                            with redirect_stdout(output):
                                status = ctl(args, self.store)
                            result = json.loads(output.getvalue())
                            self.assertEqual((status, result['ok'], result['state'], result['text']),
                                             (3, False, 'refused', refusal))
                        else:
                            response = mcp.dispatch(self.store, {'method': 'tools/call', 'params': {
                                'name': 'policy_chat', 'arguments': {
                                    'topic': '1:2', 'message': message, 'text': text}}}, topic='1:2')
                            self.assertTrue(response['isError'], response)
                            self.assertEqual(response['content'][0]['text'], refusal + '\nnull')
                        self.assertEqual(self.pick_snapshot(), before)
                        self.assertEqual(self.store.db.total_changes, changes + (source == 'cli'))
                        if source == 'cli':
                            self.assertEqual(tuple(self.store.db.execute(
                                'SELECT op,state FROM cli_calls ORDER BY id DESC LIMIT 1').fetchone()),
                                             ('policy.chat', 'refused'))

    def test_chat_policy_accepts_newline_and_tab_on_cli_and_mcp(self):
        from coordinator.__main__ import ctl
        self.store.put('coordinator_home_topic', '1:2')
        message = self.store.message_save('1:2', 'owner', 'Set delegation.')['id']
        for source in ('cli', 'mcp'):
            text = 'Use Sol.\n\tUse Claude for UI. ' + source
            if source == 'cli':
                output = StringIO()
                args = Namespace(operation='call', op='policy.chat', topic=None,
                                 pairs=['topic=1:2', 'text=' + text])
                with redirect_stdout(output):
                    status = ctl(args, self.store)
                self.assertEqual(status, 0, output.getvalue())
            else:
                response = mcp.dispatch(self.store, {'method': 'tools/call', 'params': {
                    'name': 'policy_chat', 'arguments': {
                        'topic': '1:2', 'message': message, 'text': text}}}, topic='1:2')
                self.assertFalse(response['isError'], response)
            self.assertEqual(self.store.get('coordinator_delegation')['text'], text)
            self.assertEqual(self.store.db.execute('SELECT text FROM outbox ORDER BY id DESC LIMIT 1')
                             .fetchone()[0], 'Delegation updated.')

    def test_d5_cli_chat_policy_replies_revisions_notes_and_refusals(self):
        self.store.put('coordinator_home_topic', '1:2')
        def notes():
            return [tuple(row) for row in self.store.db.execute('SELECT topic,text FROM outbox ORDER BY id')]
        cases = [
            ({'text': '  Use Sol.  '}, {'text': 'Use Sol.', 'revision': 1},
             'Chat 1:2 delegation instructions saved (8 characters). Its agent follows them from its next message. Worker settings stay unchanged.',
             'Delegation updated.'),
            ({'text': 'Use Sol.'}, {'text': 'Use Sol.', 'revision': 1},
             'Chat 1:2 delegation instructions are already set to this text.', None),
            ({'text': 'Use Luna.'}, {'text': 'Use Luna.', 'revision': 2},
             'Chat 1:2 delegation instructions saved (9 characters). Its agent follows them from its next message. Worker settings stay unchanged.',
             'Delegation updated.'),
            ({}, None, 'Chat 1:2 delegation instructions cleared. Its agent follows the usage policy from its next message.',
             'Delegation cleared.'),
            ({}, None, 'Chat 1:2 has no delegation instructions of its own. It follows the usage policy.', None)]
        expected = []
        for params, saved, reply, note in cases:
            with self.subTest(params=params):
                before = self.store.db.total_changes
                result = self.call('policy.chat', dict(params, topic='1:2'))
                self.assertTrue(result.ok, result.text)
                self.assertEqual(result.text, reply)
                self.assertEqual(result.data, {'topic': '1:2', 'delegation': saved})
                self.assertEqual(self.store.get('coordinator_delegation'), saved)
                if note:
                    expected.append(('1:2', note))
                else:
                    self.assertEqual(self.store.db.total_changes, before)
                self.assertEqual(notes(), expected)
        refusals = [({'topic': '1:2', 'text': 'x' * 2001},
                     'Keep chat delegation instructions to 2000 characters or fewer (this text has 2001).'),
                    ({'topic': '1:2', 'text': '  '}, 'Send text of 1 to 16000 characters for text.'),
                    ({'topic': '1:2', 'text': 'x' * 16001}, 'Send text of at most 16000 characters (this one has 16001) for text.'),
                    ({}, 'policy.chat needs topic.'),
                    ({'topic': 'missing'}, 'Send a known channel ID for topic.')]
        self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd) VALUES ('1:8',1,8,'unlinked','')")
        refusals.append(({'topic': '1:8'}, 'Choose a linked channel first.'))
        for params, reply in refusals:
            with self.subTest(reply=reply):
                before = self.store.db.total_changes
                result = self.call('policy.chat', params)
                self.assertEqual((result.ok, result.state, result.text), (False, 'refused', reply))
                self.assertEqual(self.store.db.total_changes, before)
                self.assertEqual(notes(), expected)
        self.assertTrue(self.call('policy.chat', {'topic': '1:2', 'text': 'x' * 2000}).ok)
        self.assertEqual(self.store.get('coordinator_delegation')['revision'], 1)

    def test_d6_mcp_chat_policy_uses_shared_owner_gate(self):
        self.store.put('coordinator_home_topic', '1:2')
        for target, chat in (('1:3', 1), ('2:4', 2)):
            self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,3,?,?,1)',
                                  (target, chat, target, self.temp.name))
        owner = self.store.message_save('1:2', 'owner', 'Set chat instructions.')['id']
        other = self.store.message_save('1:3', 'owner', 'Another chat.')['id']
        event = self.store.message_save('1:2', 'event', 'Event.')['id']
        cases = [({'topic': '1:3'}, '1:2', 'Pass message=N for the owner message that asked.'),
                 ({'topic': '1:3', 'message': event}, '1:2', 'message=N must identify an owner message.'),
                 ({'topic': '1:3', 'message': other}, '1:2', 'The owner message came from another channel.'),
                 ({'message': owner}, '1:2', 'policy.chat needs topic.'),
                 ({'topic': '2:4', 'message': owner}, '1:2', 'Choose a linked channel in the current Telegram group.'),
                 ({'topic': '1:3', 'message': owner}, None,
                  'This tool needs the asking parent channel. Restart the parent to load its scoped MCP tools.')]
        for params, asking, reply in cases:
            with self.subTest(reply=reply):
                before = self.store.db.total_changes
                response = mcp.dispatch(self.store, {'method': 'tools/call', 'params': {
                    'name': 'policy_chat', 'arguments': dict(params, text='Use Sol.')}}, topic=asking)
                self.assertTrue(response['isError'], response)
                self.assertEqual(response['content'][0]['text'], reply + '\nnull')
                self.assertEqual(self.store.db.total_changes, before)
        cli = self.call('policy.chat', {'topic': '1:3', 'text': 'Use Sol.'})
        self.store.put('coordinator:1:3_delegation', None)
        before = self.store.db.execute('SELECT count(*) FROM outbox').fetchone()[0]
        response = mcp.dispatch(self.store, {'method': 'tools/call', 'params': {
            'name': 'policy_chat', 'arguments': {'topic': '1:3', 'text': 'Use Sol.', 'message': owner}}}, topic='1:2')
        self.assertFalse(response['isError'], response)
        self.assertEqual(response['content'][0]['text'], cli.text + '\n' + json.dumps(cli.data, separators=(',', ':')))
        self.assertEqual(self.store.get('coordinator:1:3_delegation'), cli.data['delegation'])
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM outbox').fetchone()[0], before + 1)

    def test_c1_chat_picks_and_instructions_refuse_closing_or_closed_targets(self):
        self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:3',1,3,'target',?,0)",
                              (self.temp.name,))
        message = self.store.message_save('1:2', 'owner', 'Change the target chat setting.')['id']
        for lifecycle_state in ('closing', 'closed'):
            for op in ('policy.chat', 'model.parent'):
                for action in ('set', 'clear'):
                    for source in ('cli', 'mcp'):
                        with self.subTest(lifecycle=lifecycle_state, op=op, action=action, source=source):
                            params = {'topic': '1:3'}
                            if action == 'set' and op == 'policy.chat':
                                params['text'] = 'Use Codex for research.'
                            elif action == 'set':
                                params.update(provider='codex', model='gpt-6-luna')
                            self._reset_chat_control(op, action, queue_existing=True)
                            self.store.put('topic_lifecycle:1:3', {'state': lifecycle_state, 'request': 1})
                            changes = self.store.db.total_changes
                            settings = [tuple(row) for row in self.store.db.execute(
                                'SELECT * FROM settings ORDER BY key')]
                            outbox = [dict(row) for row in self.store.db.execute('SELECT * FROM outbox ORDER BY id')]
                            result = self._chat_control_call(source, op, params, message)
                            if source == 'cli':
                                status, payload = result
                                self.assertEqual(status, 3)
                                self.assertEqual((payload['ok'], payload['op'], payload['kind'], payload['state'],
                                                  payload['text'], payload['data']),
                                                 (False, op, 'write', 'refused', CLOSING_CHAT_REFUSAL, None))
                            else:
                                self.assertTrue(result['isError'], result)
                                self.assertEqual(result['content'][0]['text'], CLOSING_CHAT_REFUSAL + '\nnull')
                            self.assertEqual(self.store.db.total_changes, changes + (source == 'cli'))
                            if source == 'cli':
                                self.assertEqual(tuple(self.store.db.execute(
                                    'SELECT op,state FROM cli_calls ORDER BY id DESC LIMIT 1').fetchone()),
                                                 (op, 'refused'))
                            self.assertEqual([tuple(row) for row in self.store.db.execute(
                                'SELECT * FROM settings ORDER BY key')], settings)
                            self.assertEqual([dict(row) for row in self.store.db.execute(
                                'SELECT * FROM outbox ORDER BY id')], outbox)

    def test_c2_disabled_chat_without_lifecycle_keeps_chat_controls(self):
        self._assert_chat_controls_match_origin(enabled=0)

    def test_c3_linked_chat_keeps_chat_controls(self):
        self._assert_chat_controls_match_origin(enabled=1)

    def _assert_chat_controls_match_origin(self, enabled):
        self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:3',1,3,'target',?,?)",
                              (self.temp.name, enabled))
        message = self.store.message_save('1:2', 'owner', 'Change the target chat setting.')['id']
        for op in ('policy.chat', 'model.parent'):
            for action in ('set', 'clear'):
                for source in ('cli', 'mcp'):
                    with self.subTest(enabled=enabled, op=op, action=action, source=source):
                        params = {'topic': '1:3'}
                        if action == 'set' and op == 'policy.chat':
                            params['text'] = 'Use Codex for research.'
                        elif action == 'set':
                            params.update(provider='codex', model='gpt-6-luna')
                        self._reset_chat_control(op, action)
                        expected = ORIGIN_MAIN_CHAT_CONTROL[(op, action)]
                        result = self._chat_control_call(source, op, params, message)
                        if source == 'cli':
                            status, payload = result
                            self.assertEqual(status, 0)
                            self.assertEqual((payload['ok'], payload['op'], payload['kind'], payload['state'],
                                              payload['text'], payload['data']),
                                             (True, op, 'write', 'done', expected['text'], expected['data']))
                        else:
                            self.assertFalse(result['isError'], result)
                            text, data = result['content'][0]['text'].split('\n', 1)
                            self.assertEqual(text, expected['text'])
                            self.assertEqual(json.loads(data), expected['data'])
                        expected_settings = [expected['setting']]
                        if 'cleared' in expected:
                            expected_settings.append(expected['cleared'])
                        self.assertEqual([tuple(row) for row in self.store.db.execute(
                            "SELECT key,value FROM settings WHERE key IN "
                            "('coordinator:1:3_delegation','coordinator:1:3_selection',"
                            "'coordinator:1:3_selection_cleared') ORDER BY key")], sorted(expected_settings))
                        expected_notes = [] if expected['note'] is None else [('1:3', expected['note'], 0)]
                        self.assertEqual([tuple(row) for row in self.store.db.execute(
                            'SELECT topic,text,retired FROM outbox ORDER BY id')], expected_notes)

    def _reset_chat_control(self, op, action, queue_existing=False):
        with self.store.db:
            self.store.db.execute("DELETE FROM settings WHERE key IN "
                                  "('coordinator:1:3_delegation','coordinator:1:3_selection',"
                                  "'coordinator:1:3_selection_cleared')")
            self.store.db.execute('DELETE FROM outbox')
            if action == 'clear' and op == 'policy.chat':
                self.store.put('coordinator:1:3_delegation', {'text': 'Existing instructions.', 'revision': 1})
            elif action == 'clear':
                self.store.put('coordinator:1:3_selection', {
                    'provider': 'codex', 'model': 'gpt-6-luna', 'effort': None, 'revision': 1})
            if queue_existing:
                self.store.enqueue_report('1:2', 'Existing asking chat note.')

    def _chat_control_call(self, source, op, params, message):
        if source == 'cli':
            from coordinator.__main__ import ctl
            output = StringIO()
            args = Namespace(operation='call', op=op, topic=None,
                             pairs=[key + '=' + str(value) for key, value in params.items()])
            with redirect_stdout(output):
                status = ctl(args, self.store)
            return status, json.loads(output.getvalue())
        name = 'policy_chat' if op == 'policy.chat' else 'model_parent'
        return mcp.dispatch(self.store, {'method': 'tools/call', 'params': {
            'name': name, 'arguments': dict(params, message=message)}}, topic='1:2')

    def test_f1_cli_list_and_mcp_describe_global_default_and_model_family(self):
        expected = (
            'Pick or clear one chat agent. Over MCP, topic and message=N for the owner message in the asking channel are required. '
            'Use provider=claude (Claude), codex (ChatGPT) or opencode (OpenCode); omit provider, model and effort to clear the pick. '
            'Claude and ChatGPT accept optional model and effort. OpenCode uses the selected account model, has no thinking setting and cannot run the home chat. '
            'The chat moves to the new agent once its current turn and background work settle. Worker settings do not change. '
            "The default for all chats is the owner's to set through the local CLI: without topic, provider and model set it, "
            'and omitting both restores automatic selection. A model must belong to its provider, '
            'for example claude-opus-5-5 for claude and gpt-6.1-sol for codex.')
        completed = subprocess.run([sys.executable, '-m', 'coordinator', '--state-dir', self.temp.name,
                                    '--token-file', str(Path(self.temp.name) / 'no-token'), 'ctl', 'list'],
                                   capture_output=True, text=True, timeout=30)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        listed = {op['id']: op for op in json.loads(completed.stdout)}
        self.assertEqual(listed['model.parent']['description'], expected)
        tools = mcp.dispatch(self.store, {'method': 'tools/list'})['tools']
        self.assertEqual(next(tool['description'] for tool in tools if tool['name'] == 'model_parent'), expected)

    def pick_snapshot(self):
        return {table: [tuple(row) for row in self.store.db.execute('SELECT * FROM ' + table + ' ORDER BY 1')]
                for table in ('settings', 'outbox', 'service_requests')}

    def cli_pick(self, params):
        from coordinator.__main__ import ctl
        output = StringIO()
        args = Namespace(operation='call', op='model.parent', topic=None,
                         pairs=[key + '=' + str(value) for key, value in params.items()])
        with redirect_stdout(output):
            status = ctl(args, self.store)
        return status, json.loads(output.getvalue())

    def test_f2ab_cli_and_mcp_mismatches_refuse_without_any_mutation(self):
        self.store.put('coordinator_home_topic', '1:2')
        self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:3',1,3,'target',?,1)",
                              (self.temp.name,))
        message = self.store.message_save('1:2', 'owner', 'Pick the target chat agent.')['id']
        self.assertTrue(self.call('model.parent', dict(topic='1:3', provider='codex', model='gpt-6-luna')).ok)
        self.store.enqueue_report('1:2', 'Existing queued note.')
        self.store.service_request('workers.stop', {'worker': 123})
        cases = [('claude', 'gpt-6-luna', 'Send a Claude model with provider=claude, for example claude-opus-5-5.'),
                 ('codex', 'claude-opus-5-5', 'Send a ChatGPT model with provider=codex, for example gpt-6.1-sol.'),
                 ('codex', 'opus', 'Send a ChatGPT model with provider=codex, for example gpt-6.1-sol.')]
        for source in ('cli', 'mcp'):
            for provider, model, text in cases:
                with self.subTest(source=source, provider=provider, model=model):
                    before = self.pick_snapshot()
                    fields = dict(topic='1:3', provider=provider, model=model)
                    if source == 'cli':
                        status, result = self.cli_pick(fields)
                        self.assertEqual((status, result['ok'], result['state'], result['text']),
                                         (3, False, 'refused', text))
                    else:
                        response = mcp.dispatch(self.store, {'method': 'tools/call', 'params': {
                            'name': 'model_parent', 'arguments': dict(fields, message=message)}}, topic='1:2')
                        self.assertTrue(response['isError'], response)
                        self.assertEqual(response['content'][0]['text'], text + '\nnull')
                    self.assertEqual(self.pick_snapshot(), before)

    def test_f2c_cli_mismatched_global_default_keeps_selection_and_revision(self):
        self.assertTrue(self.call('model.parent', dict(provider='codex', model='gpt-6-luna')).ok)
        before = self.pick_snapshot()
        status, result = self.cli_pick(dict(provider='claude', model='gpt-6-luna'))
        self.assertEqual((status, result['ok'], result['state'], result['text']),
                         (3, False, 'refused',
                          'Send a Claude model with provider=claude, for example claude-opus-5-5.'))
        self.assertEqual(self.pick_snapshot(), before)

    def test_f2e_valid_parent_picks_match_computed_origin_main(self):
        self.store.put('coordinator_home_topic', '1:2')
        message = self.store.message_save('1:2', 'owner', 'Pick then clear the chat agent.')['id']
        for expected in ORIGIN_MAIN_VALID_PARENT_PICKS:
            with self.subTest(params=expected['params']):
                result = self.call('model.parent', expected['params'])
                actual = dict(params=expected['params'], ok=result.ok, text=result.text, data=result.data,
                              state=result.state, settings=[list(row) for row in self.store.db.execute(
                                  "SELECT key,value FROM settings WHERE key LIKE '%selection%' ORDER BY key")],
                              outbox=[dict(row) for row in self.store.db.execute('SELECT * FROM outbox ORDER BY id')],
                              requests=[dict(row) for row in self.store.db.execute('SELECT * FROM service_requests ORDER BY id')])
                self.assertEqual(actual, expected)
        self.store.db.execute("DELETE FROM settings WHERE key LIKE '%selection%'")
        for expected in ORIGIN_MAIN_VALID_PARENT_PICKS:
            if 'topic' not in expected['params']:
                continue
            with self.subTest(source='mcp', params=expected['params']):
                response = mcp.dispatch(self.store, {'method': 'tools/call', 'params': {
                    'name': 'model_parent', 'arguments': dict(expected['params'], message=message)}}, topic='1:2')
                self.assertEqual(response['isError'], not expected['ok'])
                text, data = response['content'][0]['text'].split('\n', 1)
                self.assertEqual((text, json.loads(data)), (expected['text'], expected['data']))
                self.assertEqual([list(row) for row in self.store.db.execute(
                    "SELECT key,value FROM settings WHERE key LIKE '%selection%' ORDER BY key")], expected['settings'])
                self.assertEqual([dict(row) for row in self.store.db.execute('SELECT * FROM outbox ORDER BY id')], expected['outbox'])
                self.assertEqual([dict(row) for row in self.store.db.execute('SELECT * FROM service_requests ORDER BY id')], expected['requests'])

    def test_claude_model_aliases_and_prefix_are_case_sensitive(self):
        for model in ('opus', 'sonnet', 'haiku', 'fable', 'claude-future-model'):
            with self.subTest(model=model):
                self.assertTrue(self.call('model.parent', dict(topic='1:2', provider='claude', model=model)).ok)
                self.assertFalse(self.call('model.parent', dict(topic='1:2', provider='codex', model=model)).ok)
        for model in ('Opus', 'FABLE', 'Claude-opus-5-5', 'opus-next'):
            with self.subTest(model=model):
                self.assertFalse(self.call('model.parent', dict(topic='1:2', provider='claude', model=model)).ok)
                self.assertTrue(self.call('model.parent', dict(topic='1:2', provider='codex', model=model)).ok)

    def test_c5_topic_tools_require_owner_message_in_scoped_parent_channel(self):
        with self.store.db:
            self.store.put('coordinator_home_topic', '1:2')
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:3',1,3,'other',?,1)",
                                  (self.temp.name,))
        owner = self.store.message_save('1:2', 'owner', 'Make a chat and close other.')
        other = self.store.message_save('1:3', 'owner', 'Other channel request.')
        event = self.store.message_save('1:2', 'event', 'Untrusted event.')
        for op, params in (('topic.create', {'name': 'New'}), ('topic.close', {'topic': '1:3'})):
            for message in (None, other['id'], event['id'], 99999):
                with self.subTest(op=op, message=message):
                    before = self.store.db.execute('SELECT count(*) FROM service_requests').fetchone()[0]
                    with self.store.db:
                        result = control_api.call(self.store, op, dict(params, message=message),
                                                  topic='1:2', source='mcp')
                    self.assertFalse(result.ok, result.text)
                    self.assertEqual(self.store.db.execute('SELECT count(*) FROM service_requests').fetchone()[0], before)
            response = mcp.dispatch(self.store, {'method': 'tools/call', 'params': {
                'name': op.replace('.', '_'), 'arguments': dict(params, message=owner['id'])}}, topic='1:2')
            self.assertFalse(response['isError'], response)

    def test_c5_topic_tools_are_deferred_parent_tools(self):
        listed = {tool['name']: tool for tool in mcp.tools_list()}
        for name in ('topic_create', 'topic_close'):
            self.assertIn(name, listed)
            self.assertIn('message', listed[name]['inputSchema']['required'])
            self.assertNotIn(name, mcp.ALWAYS_LOADED_TOOLS)
            self.assertNotIn('_meta', listed[name])

    def test_K3_removed_close_operation_is_unknown_and_settings_omit_it(self):
        command = [sys.executable, '-m', 'coordinator', '--state-dir', self.temp.name,
                   '--token-file', str(Path(self.temp.name) / 'no-token'), 'ctl', 'call']
        removed = subprocess.run(command + ['topic.close_enabled', 'enabled=true'],
                                 capture_output=True, text=True, timeout=10)
        self.assertEqual(removed.returncode, 3, removed.stderr or removed.stdout)
        expected = {'data': None, 'kind': None, 'ok': False, 'op': 'topic.close_enabled',
                    'state': 'refused', 'text': 'Unknown operation: topic.close_enabled. Use ctl list.'}
        self.assertEqual(json.loads(removed.stdout), expected)
        settings = subprocess.run(command + ['settings.get'], capture_output=True, text=True, timeout=10)
        self.assertEqual(settings.returncode, 0, settings.stderr or settings.stdout)
        actual = json.loads(settings.stdout)
        self.assertEqual((actual['ok'], actual['op'], actual['state']), (True, 'settings.get', 'done'))

    def test_c5_native_parents_receive_separate_scoped_mcp_configs(self):
        from coordinator.session import CoordinatorSession
        from coordinator.codex_session import CodexCoordinatorSession
        one = CoordinatorSession.write_mcp_config(self.store, '-10042:2')
        two = CoordinatorSession.write_mcp_config(self.store, '-10042:3')
        self.assertNotEqual(one, two)
        for config, topic in ((one, '-10042:2'), (two, '-10042:3')):
            values = CodexCoordinatorSession.overrides(self.store, topic)
            codex_args = next(json.loads(value.split('=', 1)[1]) for value in values
                              if value.startswith('mcp_servers.torii.args='))
            for args in (json.loads(config.read_text())['mcpServers']['torii']['args'], codex_args):
                with self.subTest(topic=topic, args=args):
                    completed = subprocess.run([sys.executable, *args],
                        cwd=Path(__file__).resolve().parents[1], capture_output=True, text=True, timeout=30,
                        input=json.dumps({'jsonrpc': '2.0', 'id': 1, 'method': 'tools/list'}) + '\n')
                    self.assertEqual(completed.returncode, 0, completed.stderr)
                    tools = {tool['name']: tool for tool in json.loads(completed.stdout)['result']['tools']}
                    self.assertTrue({'topic_create', 'topic_close'} <= tools.keys())
                    schema = tools['tasks_update']['inputSchema']
                    self.assertEqual(schema['properties'].get('note'), {'type': 'string', 'maxLength': 16000})
                    self.assertNotIn('note', schema['required'])

    def test_coordinator_claude_pick_carries_origin_topic_to_fresh_check(self):
        profile = Path(self.temp.name) / 'work'
        profile.mkdir()
        self.store.put('accounts', {'work': {'config_dir': str(profile), 'enabled': True}})
        self.store.put('account_status', {'work': {'identity': {'logged_in': True, 'email': 'work@example.com'}}})
        reply = mcp.dispatch(self.store, {'method': 'tools/call', 'params': {'name': 'account_use',
                             'arguments': {'alias': 'work', 'topic': '1:2'}}})
        self.assertEqual(reply['content'][0]['text'], 'work@example.com is picked for Claude; its usage is being checked.\nnull')
        self.assertEqual(self.store.get('claude_pick_refresh')['topic'], '1:2')
        self.assertEqual(self.store.get('claude_account_pick'), 'work')

    def test_p1_mcp_chat_pick_matches_cli_value_revision_and_reply(self):
        self.store.put('coordinator_home_topic', '1:2')
        self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:3',1,3,'asking',?,1)",
                              (self.temp.name,))
        message = self.store.message_save('1:3', 'owner', 'Pick the agent for these chats.')['id']
        for target in ('1:2', '1:3'):
            for provider in ('claude', 'codex'):
                fields = dict(topic=target, provider=provider,
                              model='claude-opus-5-5' if provider == 'claude' else 'test-model', effort='low')
                key = 'coordinator' if target == '1:2' else 'coordinator:' + target
                self.store.db.execute('DELETE FROM settings WHERE key LIKE ?', (key + '_selection%',))
                cli = self.call('model.parent', fields)
                saved = self.store.get(key + '_selection')
                self.store.db.execute('DELETE FROM settings WHERE key LIKE ?', (key + '_selection%',))
                response = mcp.dispatch(self.store, {'method': 'tools/call', 'params': {
                    'name': 'model_parent', 'arguments': dict(fields, message=message)}}, topic='1:3')
                self.assertFalse(response['isError'], response)
                self.assertEqual(response['content'][0]['text'].split('\n', 1)[0], cli.text)
                self.assertEqual(self.store.get(key + '_selection'), saved)
                self.assertEqual(saved['revision'], 1)
                again = mcp.dispatch(self.store, {'method': 'tools/call', 'params': {
                    'name': 'model_parent', 'arguments': dict(fields, message=message)}}, topic='1:3')
                self.assertFalse(again['isError'], again)
                self.assertEqual(self.store.get(key + '_selection'), saved)
                self.store.db.execute('DELETE FROM settings WHERE key LIKE ?', (key + '_selection%',))

    def test_p2_mcp_refusals_store_nothing(self):
        self.store.put('coordinator_home_topic', '1:2')
        for target, chat in (('1:3', 1), ('2:4', 2)):
            self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,3,?,?,1)',
                                  (target, chat, target, self.temp.name))
        owner = self.store.message_save('1:2', 'owner', 'Pick a chat agent.')['id']
        other = self.store.message_save('1:3', 'owner', 'Another channel.')['id']
        event = self.store.message_save('1:2', 'event', 'Not the owner.')['id']
        variants = [dict(topic='1:3', provider='codex'),
                    dict(topic='1:3', provider='codex', message=other),
                    dict(topic='1:3', provider='codex', message=event),
                    dict(topic='1:3', provider='codex', message=99999),
                    dict(provider='codex', model='gpt-6-luna', message=owner),
                    dict(message=owner),
                    dict(topic='1:3', model='gpt-6-luna', message=owner),
                    dict(topic='1:3', effort='low', message=owner),
                    dict(topic='2:4', provider='codex', message=owner),
                    dict(topic='1:3', provider='opencode', message=owner)]
        before = list(self.store.db.execute('SELECT * FROM settings ORDER BY key'))
        for fields in variants:
            with self.subTest(fields=fields):
                response = mcp.dispatch(self.store, {'method': 'tools/call', 'params': {
                    'name': 'model_parent', 'arguments': fields}}, topic='1:2')
                self.assertTrue(response['isError'], response)
                if 'topic' not in fields:
                    self.assertIn("default for all chats is the owner's to set", response['content'][0]['text'])
                self.assertEqual(list(self.store.db.execute('SELECT * FROM settings ORDER BY key')), before)
        unscoped = mcp.dispatch(self.store, {'method': 'tools/call', 'params': {
            'name': 'model_parent', 'arguments': dict(topic='1:3', provider='codex', message=owner)}})
        self.assertTrue(unscoped['isError'], unscoped)
        self.assertEqual(list(self.store.db.execute('SELECT * FROM settings ORDER BY key')), before)

    def test_p3_mcp_clear_returns_chat_to_default(self):
        from coordinator.parent_state import effective_agent
        self.store.put('coordinator_home_topic', '1:2')
        self.store.put('parent_selection', dict(provider='claude', model='default-model'))
        message = self.store.message_save('1:2', 'owner', 'Set then clear the chat pick.')['id']
        for fields in (dict(provider='codex', model='gpt-6-luna', effort='low'), {}):
            response = mcp.dispatch(self.store, {'method': 'tools/call', 'params': {
                'name': 'model_parent', 'arguments': dict(fields, topic='1:2', message=message)}}, topic='1:2')
            self.assertFalse(response['isError'], response)
        self.assertIsNone(self.store.get('coordinator_selection'))
        agent = effective_agent(self.store)
        self.assertEqual((agent['provider'], agent['model'], agent['picked']), ('claude', 'default-model', False))

    def test_p4_model_parent_is_deferred_with_scoped_required_inputs(self):
        tools = {tool['name']: tool for tool in mcp.tools_list()}
        tool = tools['model_parent']
        self.assertEqual(set(tool['inputSchema']['required']), {'topic', 'message'})
        self.assertNotIn('_meta', tool)
        self.assertNotIn('model_parent', mcp.ALWAYS_LOADED_TOOLS)
        self.assertNotIn('effort_parent', tools)
        for text in ('topic', 'message=N', 'claude', 'codex', 'current turn', 'background work settle',
                     'Worker settings do not change'):
            self.assertIn(text, tool['description'])

    def test_p5_cli_parent_picks_match_computed_origin_main(self):
        self.store.put('coordinator_home_topic', '1:2')
        for expected in ORIGIN_MAIN_PARENT_API:
            with self.subTest(params=expected['params']):
                result = self.call('model.parent', dict(expected['params'], message=99999))
                actual = dict(params=expected['params'], ok=result.ok, text=result.text, data=result.data,
                              state=result.state, settings=[list(row) for row in self.store.db.execute(
                                  "SELECT key,value FROM settings WHERE key LIKE '%selection%' ORDER BY key")])
                self.assertEqual(actual, expected)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name))
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:2',1,2,'test',?,1)",
                                  (self.temp.name,))
            self.store.put('owner', 1)
            self.store.put('mode', 'group')
            self.store.put('group', 1)
            self.store.put('accounts', {'work': {'config_dir': None, 'enabled': True}})

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def call(self, op, params=None):
        with self.store.db:
            return control_api.call(self.store, op, params or {}, topic='1:2')

    def test_stage_uses_server_time_and_keeps_repeats(self):
        task = self.store.task_create('1:2', 'Measure')
        for now in (100.25, 101.75):
            with patch('coordinator.store.time.time', return_value=now):
                result = self.call('tasks.update', {'task': task['id'], 'stage': 'ci'})
            self.assertTrue(result.ok, result.text)
        rows = self.store.db.execute('SELECT task,stage,created FROM task_stamps ORDER BY id').fetchall()
        self.assertEqual([tuple(row) for row in rows], [(task['id'], 'ci', 100.25), (task['id'], 'ci', 101.75)])
        self.assertEqual(self.store.task_get(task['id'])['updated'], 101.75)

    def test_tasks_update_note_appends_with_other_fields(self):
        task = self.store.task_create('1:2', 'Measure', notes='a')
        result = self.call('tasks.update', {'task': task['id'], 'note': ' b ', 'status': 'done',
                                          'title': 'Measured', 'stage': 'ready'})
        self.assertTrue(result.ok, result.text)
        self.assertEqual((result.data['notes'], result.data['status'], result.data['title']),
                         ('a\nb', 'done', 'Measured'))
        fetched = self.call('tasks.get', {'task': task['id']})
        self.assertEqual(fetched.data['notes'], 'a\nb')
        self.assertEqual([stamp['stage'] for stamp in fetched.data['stamps']], ['ready'])

    def test_stage_and_other_fields_commit_together(self):
        task = self.store.task_create('1:2', 'Measure')
        result = self.call('tasks.update', {'task': task['id'], 'stage': 'ready', 'notes': 'Built', 'status': 'done'})
        self.assertTrue(result.ok, result.text)
        self.assertEqual((result.data['notes'], result.data['status']), ('Built', 'done'))
        self.assertEqual(self.store.db.execute('SELECT stage FROM task_stamps').fetchone()[0], 'ready')

    def test_unknown_stage_and_client_time_are_refused_without_mutation(self):
        task = self.store.task_create('1:2', 'Measure', notes='Original')
        self.assertTrue(self.call('tasks.update', {'task': task['id'], 'stage': 'ready'}).ok)
        for params in ({'stage': 'unknown', 'notes': 'Changed'}, {'stage': 'ci', 'created': 1}):
            result = self.call('tasks.update', dict(params, task=task['id']))
            self.assertEqual((result.ok, result.state), (False, 'refused'))
        with self.assertRaisesRegex(ValueError, 'stage'):
            self.store.task_update(task['id'], stage='unknown', notes='Changed')
        self.assertEqual(self.store.task_get(task['id'])['notes'], 'Original')
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM task_stamps').fetchone()[0], 1)

    def test_tasks_get_lists_stamps_in_append_order_and_list_is_unchanged(self):
        task = self.store.task_create('1:2', 'Measure')
        for stage, now in (('ready', 200), ('ci', 100), ('ci', 100)):
            with patch('coordinator.store.time.time', return_value=now):
                self.assertTrue(self.call('tasks.update', {'task': task['id'], 'stage': stage}).ok)
        result = self.call('tasks.get', {'task': task['id']})
        self.assertTrue(result.ok, result.text)
        self.assertEqual([(row['stage'], row['created']) for row in result.data['stamps']],
                         [('ready', 200), ('ci', 100), ('ci', 100)])
        self.assertIn('ready', result.text)
        self.assertNotIn('stamps', self.call('tasks.list').data[0])

    def test_mcp_stage_enum_and_tool_list(self):
        tools = mcp.dispatch(self.store, {'method': 'tools/list'})['tools']
        update = next(tool for tool in tools if tool['name'] == 'tasks_update')
        self.assertEqual(update['inputSchema']['properties']['stage'],
                         {'type': 'string', 'enum': ['ready', 'reviewed', 'ci', 'merged', 'live']})
        self.assertNotIn('stage', update['inputSchema']['required'])
        self.assertEqual({tool['name'] for tool in tools}, {
            'telegram_send', 'tasks_create', 'tasks_update', 'tasks_get', 'tasks_list', 'worktree_create',
            'workers_spawn', 'workers_steer', 'workers_stop', 'workers_goal', 'workers_list', 'workers_get',
            'service_restart', 'secret_ask', 'secret_list', 'secret_rotate', 'secret_revoke', 'account_use',
            'account_redeem', 'accounts_claude_auto', 'settings_show', 'topics_list', 'topic_show', 'accounts_list', 'account_current',
            'account_show', 'policy_show', 'projects_list', 'problems_list', 'problems_summary', 'tasks_stale',
            'topic_create', 'topic_close', 'model_parent', 'policy_chat',
            'tasks_wait', 'pending_list', 'pending_fire', 'pending_cancel'})
        listed = next(tool for tool in tools if tool['name'] == 'tasks_list')
        self.assertEqual(listed['inputSchema']['properties']['status'],
                         {'type': 'string', 'enum': ['open', 'interrupted', 'waiting', 'done', 'dropped']})
        waiting = next(tool for tool in tools if tool['name'] == 'tasks_wait')
        self.assertEqual(waiting['inputSchema']['properties']['kind'],
                         {'type': 'string', 'enum': ['owner', 'time', 'job', 'pr', 'torii_update', 'signal']})

    def test_new_tables_preserve_existing_database_and_stamps_on_reopen(self):
        task = self.store.task_create('1:2', 'Measure')
        self.assertTrue(self.call('tasks.update', {'task': task['id'], 'stage': 'ready'}).ok)
        self.store.close()
        self.store = Store(Path(self.temp.name))
        result = self.call('tasks.get', {'task': task['id']})
        self.assertEqual((result.data['title'], result.data['stamps'][0]['stage']), ('Measure', 'ready'))

    def test_events_post_is_a_cli_store_operation(self):
        op = control_api.find('events.post')
        self.assertIsNotNone(op)
        self.assertEqual((op.kind, op.transport, op.params),
                         (control_api.WRITE, 'store', {'topic': 'topic', 'text': 'text'}))
        result = self.call('events.post', {'topic': '1:2', 'text': 'Approval needed.'})
        self.assertTrue(result.ok, result.text)
        event, = self.store.messages_pending()
        self.assertEqual((event['topic'], event['kind'], event['text']), ('1:2', 'event', 'Approval needed.'))
        self.assertEqual(result.data, {'message': event['id']})
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM service_requests').fetchone()[0], 0)

    def test_events_post_refuses_disabled_topic_without_storing_message(self):
        self.store.db.execute("UPDATE topics SET enabled=0 WHERE id='1:2'")

        result = self.call('events.post', {'topic': '1:2', 'text': 'Lane stopped.'})

        self.assertEqual((result.ok, result.state), (False, 'refused'))
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM messages').fetchone()[0], 0)

    def test_events_post_refuses_unlinked_topic_without_storing_message(self):
        self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:3',0,3,'unlinked',?,1)",
                              (self.temp.name,))

        result = self.call('events.post', {'topic': '1:3', 'text': 'Lane stopped.'})

        self.assertEqual((result.ok, result.state), (False, 'refused'))
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM messages').fetchone()[0], 0)


    def test_parent_model_control_keeps_worker_settings_accounts_and_history(self):
        self.store.put('codex_model', 'gpt-6.1-sol')
        self.store.put('worker_model', 'opus')
        self.store.put('coordinator_model', 'claude-opus-5-5')
        self.store.put('codex_active_account', 'personal')
        self.store.put('coordinator_session', 'native-history')
        host = {'id': 'running-parent', 'provider': 'codex', 'model': 'gpt-6.1-sol'}
        self.store.put('coordinator_host', host)
        selection = {'provider': 'codex', 'model': 'gpt-6-astra'}
        result = self.call('model.parent', selection)
        self.assertTrue(result.ok, result.text)
        self.assertEqual(self.call('settings.get').data['parent_selection'], selection)
        self.assertEqual((self.store.get('codex_model'), self.store.get('worker_model'),
                          self.store.get('coordinator_model'), self.store.get('codex_active_account'),
                          self.store.get('coordinator_session'), self.store.get('coordinator_host')),
                         ('gpt-6.1-sol', 'opus', 'claude-opus-5-5', 'personal', 'native-history', host))
        for params in ({'provider': 'claude'}, {'model': 'gpt-6-astra'},
                       {'provider': 'other', 'model': 'x'}):
            self.assertFalse(self.call('model.parent', params).ok)
            self.assertEqual(self.store.get('parent_selection'), selection)
        self.assertTrue(self.call('model.parent').ok)
        self.assertIsNone(self.store.get('parent_selection'))

    def test_parent_effort_control_sets_and_clears_each_provider_independently(self):
        self.store.put('coordinator_session', 'native-history')
        self.store.put('coordinator_host', {'id': 'running-parent'})
        for provider, other in (('claude', 'codex'), ('codex', 'claude')):
            for effort in ('low', 'medium', 'high', 'max'):
                with self.subTest(provider=provider, effort=effort):
                    before = self.store.get(other + '_parent_effort')
                    result = self.call('effort.parent', {'provider': provider, 'effort': effort})
                    self.assertTrue(result.ok, result.text)
                    self.assertEqual(self.store.get(provider + '_parent_effort'), effort)
                    self.assertEqual(self.store.get(other + '_parent_effort'), before)
            for clear in ({'provider': provider}, {'provider': provider, 'effort': None}):
                self.assertTrue(self.call('effort.parent', {'provider': provider, 'effort': 'low'}).ok)
                self.assertTrue(self.call('effort.parent', clear).ok)
                self.assertIsNone(self.store.get(provider + '_parent_effort'))
        self.assertEqual(self.store.get('coordinator_session'), 'native-history')
        self.assertEqual(self.store.get('coordinator_host'), {'id': 'running-parent'})

    def test_t6_chat_pick_sets_defaults_bumps_revision_and_clears_without_global_changes(self):
        self.store.put('coordinator_home_topic', '1:2')
        self.store.put('parent_selection', {'provider': 'claude', 'model': 'claude-opus-5-5'})
        for revision, params in enumerate(({'provider': 'codex', 'model': 'gpt-6-luna', 'effort': 'low'},
                                          {'provider': 'codex', 'model': 'gpt-6-luna', 'effort': 'high'},
                                          {'provider': 'claude'}), 1):
            result = self.call('model.parent', dict(params, topic='1:2'))
            self.assertTrue(result.ok, result.text)
            self.assertEqual(self.store.get('coordinator_selection'), {
                'provider': params['provider'], 'model': params.get('model'),
                'effort': params.get('effort'), 'revision': revision})
        self.assertEqual(self.store.get('parent_selection'), {'provider': 'claude', 'model': 'claude-opus-5-5'})
        self.assertIsNone(self.store.get('parent_selection_revision'))
        self.assertTrue(self.call('model.parent', {'topic': '1:2', 'provider': 'claude'}).ok)
        for _ in range(2):
            self.assertTrue(self.call('model.parent', {'topic': '1:2'}).ok)
            self.assertIsNone(self.store.get('coordinator_selection'))

    def test_t6_chat_pick_refuses_unknown_unlinked_invalid_provider_and_global_effort(self):
        self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:3',0,3,'unlinked','',1)")
        for params in ({'topic': 'unknown', 'provider': 'codex'}, {'topic': '1:3', 'provider': 'codex'},
                       {'topic': '1:2', 'provider': 'opencode'}, {'topic': '1:2', 'provider': 'unknown'},
                       {'effort': 'low'}, {'provider': 'codex', 'model': 'gpt-6-luna', 'effort': 'low'},
                       {'topic': '1:2', 'provider': 'codex', 'effort': 'xhigh'}):
            with self.subTest(params=params):
                self.assertFalse(self.call('model.parent', params).ok)
                self.assertIsNone(self.store.get('coordinator_selection'))
                self.assertIsNone(self.store.get('coordinator:1:2_selection'))
                self.assertIsNone(self.store.get('parent_selection'))
        self.assertIn('model_parent', {tool['name'] for tool in mcp.tools_list()})

    def test_t8_status_card_shows_chat_pick_and_provider_defaults_with_thinking(self):
        from coordinator.controls import _status_card
        self.store.put('coordinator_home_topic', '1:2')
        self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:4',1,4,'other',?,1)",
                              (self.temp.name,))
        self.store.put('parent_selection', {'provider': 'claude', 'model': 'claude-opus-5-5'})
        self.assertTrue(self.call('model.parent', {'topic': '1:2', 'provider': 'codex',
                                                  'model': 'gpt-6-luna', 'effort': 'low'}).ok)
        self.assertIn('Chat agent: ChatGPT gpt-6-luna · low thinking · picked for this chat',
                      _status_card(self.store, '1:2'))

        default = _status_card(self.store, '1:4')
        self.assertIn('Chat agent: Claude claude-opus-5-5 · medium thinking · default', default)
        self.assertIn('Parent selection: claude claude-opus-5-5 after active work settles.', default)
        self.assertTrue(self.call('model.parent', {'topic': '1:2', 'provider': 'claude'}).ok)
        self.store.put('coordinator_model', 'claude-sonnet-5')
        self.store.put('claude_parent_effort', 'medium')
        self.assertIn('Chat agent: Claude claude-sonnet-5 · medium thinking · picked for this chat',
                      _status_card(self.store, '1:2'))

    def test_f3_chat_model_or_effort_without_provider_refuses_without_clearing(self):
        self.store.put('coordinator_home_topic', '1:2')
        self.assertTrue(self.call('model.parent', {'topic': '1:2', 'provider': 'codex',
                                                  'model': 'gpt-6-luna', 'effort': 'low'}).ok)
        before = self.store.get('coordinator_selection')
        for fields in ({'model': 'gpt-6-astra'}, {'effort': 'high'}, {'model': 'x', 'effort': 'low'}):
            with self.subTest(fields=fields):
                result = self.call('model.parent', dict(fields, topic='1:2'))
                self.assertFalse(result.ok)
                self.assertIn('provider', result.text)
                self.assertIn('topic alone', result.text)
                self.assertEqual(self.store.get('coordinator_selection'), before)
        self.assertTrue(self.call('model.parent', {'topic': '1:2'}).ok)
        self.assertIsNone(self.store.get('coordinator_selection'))

    def test_parent_effort_refuses_invalid_values_without_storing_anything(self):
        for provider in ('claude', 'codex'):
            for effort in ('xhigh', 'ultra', 'none', 'minimal', '', 1, True, []):
                with self.subTest(provider=provider, effort=effort):
                    before = list(self.store.db.execute('SELECT * FROM settings ORDER BY key'))
                    result = self.call('effort.parent', {'provider': provider, 'effort': effort})
                    self.assertEqual((result.ok, result.state), (False, 'refused'))
                    self.assertEqual(list(self.store.db.execute('SELECT * FROM settings ORDER BY key')), before)
            self.assertTrue(self.call('effort.parent', {'provider': provider, 'effort': 'high'}).ok)
            self.assertFalse(self.call('effort.parent', {'provider': provider, 'effort': 'xhigh'}).ok)
            self.assertEqual(self.store.get(provider + '_parent_effort'), 'high')
        for params in ({}, {'effort': 'low'}, {'provider': 'unknown', 'effort': 'high'}):
            self.assertFalse(self.call('effort.parent', params).ok)

    def test_events_post_refuses_every_non_cli_source_and_is_not_an_mcp_tool(self):
        for source in ('mcp', 'telegram', 'service', 'other'):
            with self.subTest(source=source):
                with self.store.db:
                    result = control_api.call(self.store, 'events.post',
                                              {'topic': '1:2', 'text': 'Lane stopped.'}, source=source)
                self.assertEqual((result.ok, result.state, result.text),
                                 (False, 'refused', 'events.post is available only from the local CLI.'))
        self.assertEqual(len(mcp.tools_list()), 39)
        self.assertNotIn('events_post', {tool['name'] for tool in mcp.tools_list()})
        with self.assertRaisesRegex(ValueError, 'Unknown tool: events_post'):
            mcp.dispatch(self.store, {'method': 'tools/call', 'params': {
                'name': 'events_post', 'arguments': {'topic': '1:2', 'text': 'Lane stopped.'}}})
        self.assertEqual(self.store.messages_pending(), [])

    def test_events_post_validates_text_and_topic_before_storing(self):
        for params in ({'topic': '1:2', 'text': 'x' * (control_api.TEXT_LIMIT + 1)},
                       {'topic': '1:2', 'text': '  '}, {'topic': 'unknown', 'text': 'Lane stopped.'},
                       {'topic': '1:2'}, {'text': 'Lane stopped.'}):
            with self.subTest(params={key: len(value) for key, value in params.items()}):
                with self.store.db:
                    result = control_api.call(self.store, 'events.post', params)
                self.assertEqual((result.ok, result.state), (False, 'refused'))
                self.assertEqual(self.store.messages_pending(), [])
        text = 'x' * control_api.TEXT_LIMIT
        self.assertTrue(self.call('events.post', {'topic': '1:2', 'text': text}).ok)
        self.assertEqual(self.store.messages_pending()[0]['text'], text)

    def test_telegram_send_sends_same_channel_notices_without_quote(self):
        event = self.store.message_event('1:2', 'Lane stopped.')
        worker_result = self.store.message_save('1:2', 'worker_result', 'Build finished.')
        for message in (event, worker_result):
            with self.subTest(kind=message['kind']):
                result = self.call('telegram.send', {'text': 'Acknowledged.', 'reply_to': message['id']})
                self.assertTrue(result.ok, result.text)
                row = self.store.db.execute('SELECT reply_to FROM outbox WHERE id=?',
                                            (result.data['outbox'],)).fetchone()
                self.assertEqual(result.text, 'Telegram message queued. Sent without a quote.')
                self.assertFalse(result.data['quoted'])
                self.assertIsNone(row['reply_to'])

    def test_home_control_preserves_native_session_ownership(self):
        self.store.put('coordinator_home_topic', '1:2')
        self.store.put('coordinator_session', 'original-native-session')
        self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:3',1,3,'other',?,1)",
                              (self.temp.name,))
        self.assertTrue(self.call('topic.home', {'topic': '1:3'}).ok)
        self.assertEqual(self.store.get('coordinator_home_topic'), '1:3')
        self.assertEqual(self.store.get('coordinator_session_topic'), '1:2')
        self.assertEqual(self.store.get('coordinator_session'), 'original-native-session')
        service = Service(self.store, None, None, Path(self.temp.name))
        self.assertEqual(service.home_topic(), '1:3')
        self.assertEqual(service.session_key('1:2'), 'coordinator')
        self.assertEqual(service.session_key('1:3'), 'coordinator:1:3')
        self.assertTrue(self.call('topic.home', {'topic': '1:2'}).ok)
        self.assertEqual(self.store.get('coordinator_session_topic'), '1:2')
        self.assertFalse(self.call('topic.home', {'topic': '1:9'}).ok)
        self.store.db.execute("UPDATE topics SET enabled=0 WHERE id='1:3'")
        self.assertFalse(self.call('topic.home', {'topic': '1:3'}).ok)

    def test_claude_worker_is_refused_when_no_claude_account_is_configured(self):
        self.store.put('accounts', {})
        task = self.store.task_create('1:2', 'Work', worktree=self.temp.name)
        result = self.call('workers.spawn', {'task': task['id'], 'provider': 'claude', 'prompt': 'Go'})
        self.assertFalse(result.ok)
        self.assertIn('No Claude account is connected. Use provider codex.', result.text)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM workers').fetchone()[0], 0)

    def test_first_bind_selects_home_and_later_bind_preserves_it(self):
        self.assertIsNone(Service(self.store, None, None, Path(self.temp.name)).home_topic())
        self.store.bind('1:2', self.temp.name, 'test', enabled=True)
        self.assertEqual(self.store.get('coordinator_home_topic'), '1:2')
        self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd) VALUES ('1:3',1,3,'other','')")
        self.store.bind('1:3', self.temp.name, 'other', enabled=True)
        self.assertEqual(self.store.get('coordinator_home_topic'), '1:2')

    def test_disabled_home_can_be_cleared_without_changing_session_ownership(self):
        self.store.put('coordinator_home_topic', '1:2')
        self.store.put('coordinator_session', 'native-session')
        self.store.put('coordinator_host', {'id': 'original'})
        self.assertTrue(self.call('topic.disable').ok)
        self.assertTrue(self.call('topic.home', {'topic': '1:2', 'clear': True}).ok)
        self.assertIsNone(self.store.get('coordinator_home_topic'))
        self.assertEqual(self.store.get('coordinator_session_topic'), '1:2')
        self.assertEqual(self.store.get('coordinator_session'), 'native-session')
        self.assertEqual(self.store.get('coordinator_host'), {'id': 'original'})
        self.assertEqual(Service(self.store, None, None, Path(self.temp.name)).session_key('1:2'), 'coordinator')
        self.assertFalse(self.call('topic.home', {'topic': '1:2', 'clear': True}).ok)

    def test_clearing_another_channel_refuses_and_preserves_home(self):
        self.store.put('coordinator_home_topic', '1:2')
        self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:3',1,3,'other',?,1)",
                              (self.temp.name,))
        self.assertFalse(self.call('topic.home', {'topic': '1:3', 'clear': True}).ok)
        self.assertEqual(self.store.get('coordinator_home_topic'), '1:2')

    def test_disabled_bind_does_not_select_home(self):
        self.assertTrue(self.call('topic.bind', {'topic': '1:2', 'cwd': self.temp.name,
                                               'name': 'test', 'enable': False}).ok)
        self.assertIsNone(self.store.get('coordinator_home_topic'))
        self.assertTrue(self.call('topic.bind', {'topic': '1:2', 'cwd': self.temp.name,
                                               'name': 'test', 'enable': True}).ok)
        self.assertEqual(self.store.get('coordinator_home_topic'), '1:2')

    def test_manifest_exposes_v2_and_menu_ops_without_removed_ops(self):
        ids = [op.id for op in control_api.OPS]
        self.assertEqual(len(ids), len(set(ids)))
        for required in ('telegram.send', 'tasks.create', 'tasks.update', 'tasks.get', 'tasks.list', 'tasks.wait',
                         'pending.list', 'pending.fire', 'pending.cancel',
                         'worktree.create', 'workers.spawn', 'workers.steer', 'workers.stop',
                         'workers.list', 'workers.goal', 'service.restart', 'settings.show',
                         'account.enable', 'account.disable', 'topic.setup_new'):
            self.assertIn(required, ids)
        for removed in ('audit.list', 'held.list', 'topic.resume', 'service.restart_request',
                        'telegram.token', 'worker.stop', 'worker.steer', 'ops.list', 'workers.limit',
                        'model.planning', 'topic.provider', 'account.select', 'account.rename',
                        'account.selection_auto', 'account.access', 'account.threshold'):
            self.assertNotIn(removed, ids)
        for op in control_api.OPS:
            self.assertIn(op.kind, (control_api.READ, control_api.WRITE))
            self.assertIn(op.transport, ('store', control_api.SERVICE))
            for kind in op.params.values():
                self.assertIn(kind.rstrip('?'), control_api.TYPES)
            if op.transport == 'store':
                self.assertTrue(callable(control_api._handler(op)))

    def test_account_remove_and_targeted_add_are_in_control_manifest(self):
        remove = control_api.find('account.remove')
        self.assertEqual((remove.kind, remove.transport, remove.params),
                         (control_api.WRITE, 'store', {'alias': 'alias'}))
        self.assertEqual(mcp.input_schema(remove)['required'], ['alias'])
        schema = mcp.input_schema(control_api.find('account.add'))
        self.assertEqual(schema['properties']['alias'], {'type': 'string'})
        self.assertNotIn('alias', schema['required'])
        self.assertNotIn('account_remove', {tool['name'] for tool in mcp.tools_list()})
        self.assertIn('95%', control_api.find('account.codex_reset').description)

    def account_fixture(self, provider, logged_in=False):
        directory = Path(self.temp.name) / provider
        previous = Path(self.temp.name) / (provider + '-previous')
        directory.mkdir()
        previous.mkdir()
        for folder in (directory, previous):
            (folder / 'history.jsonl').write_text('native history\n')
        key, status_key, blocks_key = (('codex_accounts', 'codex_account_status', 'codex_account_blocks')
                                        if provider == 'codex' else ('accounts', 'account_status', 'account_blocks'))
        self.store.put(key, {'work': {'config_dir': str(directory), 'enabled': False,
                                     'previous_config_dirs': [str(previous), str(directory)]},
                             'other': {'config_dir': None, 'enabled': False}})
        self.store.put(status_key, {'work': {'identity': {'email': 'work@example.com', 'logged_in': logged_in}},
                                    'other': {'identity': {'logged_in': False}}})
        self.store.put(blocks_key, {'work': {'reason': 'quota', 'until': None},
                                    'other': {'reason': 'quota', 'until': None}})
        return directory, previous, key, status_key, blocks_key

    def test_targeted_account_add_infers_provider_and_uses_fresh_folder(self):
        self.store.put('bot_username', 'torii_test_bot')
        for provider in ('claude', 'codex'):
            with self.subTest(provider=provider):
                self.store.put('accounts', {})
                self.store.put('codex_accounts', {})
                directory, _, _, _, _ = self.account_fixture(provider)
                self.store.put('account_signin', None)
                with patch('coordinator.signin.accounts_root', return_value=Path(self.temp.name) / 'claude-new'), \
                     patch('coordinator.codex_accounts.accounts_root', return_value=Path(self.temp.name) / 'codex-new'):
                    result = self.call('account.add', {'alias': 'work',
                                                       'provider': 'claude' if provider == 'codex' else 'codex'})
                self.assertTrue(result.ok, result.text)
                record = self.store.get('account_signin')
                self.assertEqual(record['target'], 'work')
                self.assertEqual(record.get('provider', 'claude'), provider)
                new = Path(record['config_dir'])
                self.assertEqual(new.parent, Path(self.temp.name) / (provider + '-new'))
                self.assertTrue(new.name.startswith('.torii-'))
                self.assertFalse(new.exists())
                self.assertEqual((directory / 'history.jsonl').read_text(), 'native history\n')

    def test_account_add_and_remove_refuse_authenticated_accounts_even_when_disabled(self):
        self.store.put('bot_username', 'torii_test_bot')
        for provider in ('claude', 'codex'):
            with self.subTest(provider=provider):
                self.store.put('accounts', {})
                self.store.put('codex_accounts', {})
                self.account_fixture(provider, logged_in=True)
                before = list(self.store.db.execute('SELECT key,value FROM settings ORDER BY key'))
                added = self.call('account.add', {'alias': 'work'})
                self.assertEqual((added.state, added.text), ('refused', 'work@example.com is already signed in.'))
                removed = self.call('account.remove', {'alias': 'work'})
                self.assertEqual((removed.state, removed.text),
                                 ('refused', 'work@example.com is signed in. Only signed-out accounts can be removed.'))
                self.assertEqual(list(self.store.db.execute('SELECT key,value FROM settings ORDER BY key')), before)

    def test_account_remove_refuses_pending_target_without_mutation(self):
        for provider in ('claude', 'codex'):
            with self.subTest(provider=provider):
                self.store.put('accounts', {})
                self.store.put('codex_accounts', {})
                self.account_fixture(provider)
                self.store.put('account_signin', {'target': 'work', 'provider': provider, 'state': 'cancelled'})
                before = list(self.store.db.execute('SELECT key,value FROM settings ORDER BY key'))
                result = self.call('account.remove', {'alias': 'work'})
                self.assertEqual((result.state, result.text),
                                 ('refused', 'A sign-in for this account is open. Finish or cancel it first.'))
                self.assertEqual(list(self.store.db.execute('SELECT key,value FROM settings ORDER BY key')), before)

    def test_account_remove_clears_provider_records_and_keeps_all_folders(self):
        for provider in ('claude', 'codex'):
            with self.subTest(provider=provider):
                self.store.put('accounts', {})
                self.store.put('codex_accounts', {})
                directory, previous, key, status_key, blocks_key = self.account_fixture(provider)
                self.store.put('account_dirs_removed', [str(previous / '..' / previous.name)])
                self.store.put('codex_active_account', 'work')
                self.store.put('account_signin', {'target': 'other', 'state': 'starting'})
                result = self.call('account.remove', {'alias': 'work'})
                self.assertEqual((result.ok, result.text), (True, 'Removed work@example.com.'))
                self.assertEqual(self.store.get(key), {'other': {'config_dir': None, 'enabled': False}})
                self.assertEqual(self.store.get(status_key), {'other': {'identity': {'logged_in': False}}})
                self.assertEqual(self.store.get(blocks_key), {'other': {'reason': 'quota', 'until': None}})
                self.assertEqual(self.store.get('account_dirs_removed'), [str(previous.resolve()),
                                                                          str(directory.resolve())])
                self.assertEqual(self.store.get('codex_active_account'), None if provider == 'codex' else 'work')
                for folder in (directory, previous):
                    self.assertEqual((folder / 'history.jsonl').read_text(), 'native history\n')
                self.assertEqual(self.store.db.execute('SELECT count(*) FROM service_requests').fetchone()[0], 0)

    def test_unknown_account_target_and_remove_refuse_without_mutation(self):
        before = list(self.store.db.execute('SELECT key,value FROM settings ORDER BY key'))
        for op in ('account.add', 'account.remove'):
            with self.subTest(op=op):
                result = self.call(op, {'alias': 'missing'})
                self.assertEqual((result.state, result.text),
                                 ('refused', 'Account is not registered. Use /accounts to list registered aliases.'))
                self.assertEqual(list(self.store.db.execute('SELECT key,value FROM settings ORDER BY key')), before)

    def test_local_task_create_without_a_channel_names_the_topic_parameter(self):
        result = control_api.call(self.store, 'tasks.create', {'title': 'Ship unit 2'})
        self.assertEqual((result.state, result.text), ('refused', 'tasks.create needs topic (a channel ID) or message.'))

    def test_reports_require_current_binding_even_for_disabled_topics(self):
        self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd) VALUES ('8:0',8,0,'stale','')")
        for mode, chat in (('group', -10042),):
            with self.subTest(mode=mode):
                self.store.put('mode', mode)
                self.store.put('group', -10042)
                self.store.db.execute("UPDATE topics SET chat=?,enabled=0 WHERE id='1:2'", (chat,))
                stale = self.call('telegram.send', {'topic': '8:0', 'text': 'Private report'})
                self.assertFalse(stale.ok)
                self.assertIn('current Telegram binding', stale.text)
                self.assertTrue(self.call('telegram.send', {'text': 'Status'}).ok)
                row = self.store.pending_delivery()
                self.assertEqual(row['chat'], chat)
                self.store.delivered(row['id'], 100)
        self.store.put('owner', None)
        self.assertFalse(self.call('telegram.send', {'text': 'Unpaired'}).ok)
        self.assertIsNone(self.store.pending_delivery())

    def test_tasks_and_telegram_send_use_store(self):
        created = self.call('tasks.create', {'title': 'Ship unit 2', 'notes': 'Test it'})
        self.assertTrue(created.ok)
        task = created.data
        self.assertEqual((task['topic'], task['number'], task['status']), ('1:2', 1, 'open'))
        updated = self.call('tasks.update', {'task': task['id'], 'status': 'done'})
        self.assertEqual(updated.data['status'], 'done')
        self.assertEqual(self.call('tasks.get', {'task': task['id']}).data['title'], 'Ship unit 2')
        self.assertEqual(len(self.call('tasks.list', {'status': 'done'}).data), 1)
        self.assertFalse(self.call('tasks.update', {'task': task['id'], 'status': 'waiting'}).ok)
        sent = self.call('telegram.send', {'text': 'Done', 'buttons': [[{'text': 'Open', 'callback_data': 'open'}]]})
        row = self.store.db.execute('SELECT * FROM outbox WHERE id=?', (sent.data['outbox'],)).fetchone()
        self.assertEqual(row['text'], 'Done')
        self.assertEqual(json.loads(row['reply_markup'])['inline_keyboard'][0][0]['text'], 'Open')

    def test_telegram_send_refuses_invalid_buttons_without_queueing(self):
        for text, callback in (('ok', ''), ('ok', 'x' * 65), ('ok', 'é' * 33), ('', 'ok')):
            for encode in (lambda value: value, json.dumps):
                with self.subTest(text=text, callback=callback, encode=encode):
                    result = self.call('telegram.send', {'text': 'Report', 'buttons': encode([
                        [{'text': text, 'callback_data': callback}]])})
                    self.assertFalse(result.ok)
                    self.assertIn('buttons', result.text)
                    self.assertEqual(self.store.db.execute('SELECT count(*) FROM outbox').fetchone()[0], 0)

    def test_telegram_send_accepts_callback_byte_limits(self):
        for callback in ('x', 'x' * 64, 'é' * 32):
            with self.subTest(callback=callback):
                result = self.call('telegram.send', {'text': 'Report', 'buttons': [
                    [{'text': 'ok', 'callback_data': callback}]]})
                self.assertTrue(result.ok, result.text)
                row = self.store.db.execute('SELECT reply_markup FROM outbox WHERE id=?',
                                            (result.data['outbox'],)).fetchone()
                self.assertEqual(json.loads(row['reply_markup'])['inline_keyboard'][0][0]['callback_data'], callback)

    def test_completion_evidence_remains_readable_after_job_closes(self):
        created = self.call('tasks.create', {'title': 'Completion evidence'})
        self.assertTrue(created.ok, created.text)
        task = created.data
        self.store.task_update(task['id'], worktree=self.temp.name)
        ready = self.call('tasks.update', {'task': task['id'], 'stage': 'ready'})
        self.assertTrue(ready.ok, ready.text)
        ci = self.call('tasks.update', {'task': task['id'], 'stage': 'ci', 'notes': 'PR 12 sha abc run 34'})
        self.assertTrue(ci.ok, ci.text)
        worker = self.call('workers.spawn', {'task': task['id'], 'provider': 'claude', 'prompt': 'Run checks'})
        self.assertTrue(worker.ok, worker.text)
        result_text = 'Ran ./scripts/verify-local.py: 812 tests OK'
        completed = self.store.worker_complete(worker.data['worker'], {'success': True, 'text': result_text})
        self.assertEqual(completed['kind'], 'worker_result')
        closed = self.call('tasks.update', {'task': task['id'], 'status': 'done'})
        self.assertTrue(closed.ok, closed.text)
        fetched = self.call('tasks.get', {'task': task['id']})
        self.assertTrue(fetched.ok, fetched.text)
        self.assertEqual([stamp['stage'] for stamp in fetched.data['stamps']], ['ready', 'ci'])
        self.assertEqual([line.split(':', 1)[0] for line in fetched.text.splitlines()[1:]], ['ready', 'ci'])
        self.assertEqual(fetched.data['notes'], 'PR 12 sha abc run 34')
        full_worker = self.call('workers.get', {'worker': worker.data['worker']})
        self.assertTrue(full_worker.ok, full_worker.text)
        self.assertEqual(full_worker.data['result']['text'], result_text)

    def test_telegram_send_translates_same_channel_message_number(self):
        message = self.store.message_save('1:2', 'owner', 'Question', telegram_message=5006)
        sent = self.call('telegram.send', {'text': 'Answer', 'reply_to': message['id']})
        row = self.store.db.execute('SELECT reply_to FROM outbox WHERE id=?', (sent.data['outbox'],)).fetchone()
        self.assertTrue(sent.ok)
        self.assertEqual(row['reply_to'], 5006)

    def test_telegram_send_reply_to_notice_overrides_task_origin(self):
        origin = self.store.message_save('1:2', 'owner', 'Build this', telegram_message=5006)
        task = self.call('tasks.create', {'message': origin['id'], 'title': 'Build'})
        event = self.store.message_event('1:2', 'Lane stopped.')
        sent = self.call('telegram.send', {'text': 'Acknowledged.', 'reply_to': event['id'],
                                           'task': task.data['number']})
        self.assertTrue(sent.ok, sent.text)
        row = self.store.db.execute('SELECT reply_to FROM outbox WHERE id=?', (sent.data['outbox'],)).fetchone()
        self.assertEqual(sent.text, 'Telegram message queued. Sent without a quote.')
        self.assertFalse(sent.data['quoted'])
        self.assertIsNone(row['reply_to'])

    def test_telegram_send_refuses_invalid_reply_targets(self):
        self.two_topics()
        cross_channel = self.store.message_save('1:3', 'owner', 'Other channel', telegram_message=7001)
        for message in (cross_channel['id'], 999999):
            result = self.call('telegram.send', {'text': 'Answer', 'reply_to': message})
            self.assertEqual((result.state, result.text), (
                'refused', 'reply_to must be the message=N number of a Telegram message in this channel; '
                'send without reply_to or use another number.'))

    def test_telegram_send_without_reply_target_keeps_none(self):
        sent = self.call('telegram.send', {'text': 'Broadcast'})
        row = self.store.db.execute('SELECT reply_to FROM outbox WHERE id=?', (sent.data['outbox'],)).fetchone()
        self.assertTrue(sent.ok)
        self.assertIsNone(row['reply_to'])

    def test_telegram_send_image_and_multipart_replies_use_telegram_id(self):
        message = self.store.message_save('1:2', 'owner', 'Question', telegram_message=5006)
        image = Path(self.store.directory) / 'reply.png'
        image.write_bytes(b'image')
        sent = self.call('telegram.send', {'text': 'A' * 5000, 'reply_to': message['id'], 'image': str(image)})
        self.assertTrue(sent.ok, sent.text)
        rows = self.store.db.execute('SELECT reply_to,image FROM outbox WHERE topic=? ORDER BY id',
                                     ('1:2',)).fetchall()
        self.assertGreater(len(rows), 1)
        self.assertEqual([row['reply_to'] for row in rows], [5006] * len(rows))
        self.assertEqual(rows[0]['image'], str(image))
        self.assertTrue(all(row['image'] is None for row in rows[1:]))

    def test_telegram_send_as_document_persists_on_image_part_only(self):
        image = Path(self.store.directory) / 'original.jpg'
        image.write_bytes(b'jpeg')
        sent = self.call('telegram.send', {'text': 'A' * 5000, 'image': str(image), 'as_document': True})
        self.assertTrue(sent.ok, sent.text)
        rows = self.store.db.execute('SELECT image,as_document FROM outbox WHERE topic=? ORDER BY id',
                                     ('1:2',)).fetchall()
        self.assertGreater(len(rows), 1)
        self.assertEqual(rows[0]['as_document'], 1)
        self.assertTrue(all(row['as_document'] == 0 for row in rows[1:]))
        plain = self.call('telegram.send', {'text': 'Photo', 'image': str(image)})
        row = self.store.db.execute('SELECT as_document FROM outbox WHERE id=?', (plain.data['outbox'],)).fetchone()
        self.assertEqual(row['as_document'], 0)
        refused = self.call('telegram.send', {'text': 'No file', 'as_document': True})
        self.assertFalse(refused.ok)
        self.assertIn('as_document needs image', refused.text)

    def test_telegram_send_description_documents_message_tag_reply_to(self):
        description = control_api.BY_ID['telegram.send'].description
        self.assertIn('message=N number from a message tag in the same channel', description)

    def two_topics(self):
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:3',1,3,'other',?,1)",
                                  (self.temp.name,))
        return (self.store.message_save('1:2', 'owner', 'build it')['id'],
                self.store.message_save('1:3', 'owner', 'think')['id'])

    def mcp(self, op, params):
        with self.store.db:
            return control_api.call(self.store, op, params, source='mcp')

    def test_coordinator_task_is_bound_to_the_topic_of_the_message_that_asked(self):
        asked, other = self.two_topics()
        missing = self.mcp('tasks.create', {'topic': '1:3', 'title': 'Guess'})
        self.assertEqual(missing.state, 'refused')
        self.assertIn('Pass the ID of the owner message', missing.text)
        self.assertEqual(self.mcp('tasks.create', {'message': 999, 'title': 'Lost'}).state, 'refused')
        created = self.mcp('tasks.create', {'message': other, 'title': 'Brainstorm'})
        self.assertTrue(created.ok, created.text)
        self.assertEqual((created.data['topic'], created.data['number']), ('1:3', 1))
        crossed = self.mcp('tasks.create', {'message': other, 'topic': '1:2', 'title': 'Wrong list'})
        self.assertEqual(crossed.state, 'refused')
        self.assertIn('came from channel 1:3, not 1:2', crossed.text)
        self.assertEqual(self.store.tasks_list('1:2'), [])
        allowed = self.mcp('tasks.create', {'message': other, 'topic': '1:2', 'cross_topic': True, 'title': 'Asked'})
        self.assertEqual(allowed.data['topic'], '1:2')
        same = self.mcp('tasks.create', {'message': asked, 'topic': '1:2', 'title': 'Build'})
        self.assertEqual((same.data['topic'], same.data['number']), ('1:2', 2))

    def test_stale_tasks_are_open_tasks_idle_past_the_hours_with_no_active_worker(self):
        now = time.time()
        titles = ('Idle', 'Recent', 'Running', 'Done', 'Idle worker', 'Waiting for quota')
        tasks = {title: self.store.task_create('1:2', title) for title in titles}
        with self.store.db:
            for title, updated in (('Idle', now - 7 * 3600), ('Recent', now - 3600), ('Running', now - 9 * 3600),
                                   ('Done', now - 9 * 3600), ('Idle worker', now - 10 * 3600),
                                   ('Waiting for quota', now - 9 * 3600)):
                self.store.db.execute('UPDATE tasks SET updated=?,status=? WHERE id=?',
                                      (updated, 'done' if title == 'Done' else 'open', tasks[title]['id']))
            for title, status, updated in (('Running', 'running', now - 9 * 3600),
                                           ('Idle worker', 'done', now - 8 * 3600),
                                           ('Waiting for quota', 'waiting_for_quota', now - 9 * 3600)):
                self.store.db.execute('''INSERT INTO workers(task,topic,provider,prompt,status,created,updated)
                    VALUES (?,'1:2','claude','p',?,?,?)''', (tasks[title]['id'], status, updated, updated))
        stale = self.call('tasks.stale')
        self.assertEqual([row['title'] for row in stale.data['tasks']], ['Idle worker', 'Idle'])
        self.assertEqual(stale.data['tasks'][0]['workers'], 1)
        self.assertEqual([row['title'] for row in self.call('tasks.stale', {'hours': 1}).data['tasks']],
                         ['Idle worker', 'Idle', 'Recent'])
        self.assertIn('stale', {name.split('_')[1] for name in mcp.TOOLS if name.startswith('tasks_')})

    def test_task_reads_for_a_topic_show_only_that_topic(self):
        self.two_topics()
        mine = self.store.task_create('1:2', 'Mine')
        theirs = self.store.task_create('1:3', 'Theirs')
        self.assertEqual([row['title'] for row in self.mcp('tasks.list', {'topic': '1:3'}).data], ['Theirs'])
        self.assertEqual([row['title'] for row in self.call('tasks.list').data], ['Mine'])
        refused = self.mcp('tasks.get', {'task': mine['id'], 'topic': '1:3'})
        self.assertEqual(refused.state, 'refused')
        self.assertIsNone(refused.data)
        self.assertEqual(self.mcp('tasks.get', {'task': theirs['id'], 'topic': '1:3'}).data['title'], 'Theirs')
        self.assertEqual(self.mcp('tasks.get', {'task': mine['id']}).data['title'], 'Mine')

    def test_topic_names_are_display_only_and_ids_resolve_even_when_labels_repeat(self):
        self.two_topics()
        self.assertIsNone(control_api.resolve_topic(self.store, 'other'))
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:4',1,4,'Other',?,1)",
                                  (self.temp.name,))
        self.assertIsNone(control_api.resolve_topic(self.store, 'OTHER'))
        self.assertEqual(self.mcp('tasks.list', {'topic': 'other'}).state, 'refused')
        self.assertEqual(control_api.resolve_topic(self.store, '1:4'), '1:4')

    def test_topic_rename_queues_duplicate_labels_and_telegram_title_wins_later(self):
        self.two_topics()
        self.store.put('owner', 7)
        self.store.put('group', 1)
        result = self.call('topic.rename', {'name': 'other'})
        self.assertEqual((result.ok, result.state), (True, 'queued'))
        self.assertEqual(self.store.topic('1:2')['name'], 'test')
        request = self.store.db.execute('SELECT op,params FROM service_requests WHERE id=?',
                                        (result.data['request'],)).fetchone()
        self.assertEqual(request['op'], 'topic.rename')
        self.assertEqual(json.loads(request['params'])['name'], 'other')
        edited = {'update_id': 8, 'message': {'message_id': 8, 'from': {'id': 7}, 'message_thread_id': 2,
                                              'chat': {'id': 1, 'type': 'supergroup'},
                                              'forum_topic_edited': {'name': 'Telegram title'}}}
        self.assertEqual(self.store.accept(edited), 'service_event')
        self.assertEqual(self.store.topic('1:2')['name'], 'Telegram title')

    def test_task_secret_declarations_are_sorted_mapped_and_validated(self):
        created = self.call('tasks.create', {'title': 'Release', 'secrets': 'B_TOKEN,A_KEY,A_KEY'})
        self.assertTrue(created.ok, created.text)
        self.assertEqual(created.data['secrets'], ['A_KEY', 'B_TOKEN'])
        task = created.data['id']
        updated = self.call('tasks.update', {'task': task, 'secrets': ' GH_TOKEN=GITHUB_TOKEN_ORG , A_KEY=A_KEY'})
        self.assertEqual(updated.data['secrets'], ['A_KEY', 'GH_TOKEN=GITHUB_TOKEN_ORG'])
        self.assertEqual(self.call('tasks.update', {'task': task, 'secrets': ['NPM_TOKEN']}).data['secrets'],
                         ['NPM_TOKEN'])
        for invalid in ('PATH', 'NODE_REPL_AUTH_TOKEN', 'RUST_LOG', 'GIT_TOKEN=GITHUB_TOKEN', 'GH_TOKEN=lower', 'A_KEY=B_KEY,A_KEY=C_KEY', 'A_KEY,,B_KEY',
                        'GH_TOKEN=', ['A_KEY', 3]):
            refused = self.call('tasks.update', {'task': task, 'secrets': invalid})
            self.assertEqual(refused.state, 'refused', invalid)
            self.assertIn('secret names', refused.text)
        self.assertEqual(self.store.task_get(task)['secrets'], ['NPM_TOKEN'])
        cleared = self.call('tasks.update', {'task': task, 'secrets': ''})
        self.assertEqual(cleared.data['secrets'], [])
        self.assertIsNone(self.store.db.execute('SELECT secrets FROM tasks WHERE id=?', (task,)).fetchone()[0])
        self.assertEqual(self.call('tasks.get', {'task': task}).data['secrets'], [])

    def test_both_providers_accept_tasks_with_secrets(self):
        task = self.store.task_create('1:2', 'Build', worktree=self.temp.name, secrets=['GITHUB_TOKEN'])
        spawned = self.call('workers.spawn', {'task': task['id'], 'provider': 'codex', 'prompt': 'Build it'})
        self.assertTrue(spawned.ok, spawned.text)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM workers').fetchone()[0], 1)
        self.assertTrue(self.call('workers.spawn', {'task': task['id'], 'provider': 'claude',
                                                    'prompt': 'Build it'}).ok)

    def test_telegram_image_path_roots_and_refusals(self):
        root = Path(self.temp.name)
        project = root / 'project'
        project.mkdir()
        with self.store.db:
            self.store.db.execute("UPDATE topics SET cwd=? WHERE id='1:2'", (str(project),))
        topic_file = project / 'topic.png'
        topic_file.write_bytes(b'photo')
        task_root = root.parent / (root.name + '-task')
        outside = root.parent / (root.name + '-outside.png')
        task_root.mkdir()
        try:
            worktree_file = task_root / 'worktree.pdf'
            worktree_file.write_bytes(b'document')
            state_file = self.store.directory / 'state-image.png'
            state_file.write_bytes(b'state')
            task = self.store.task_create('1:2', 'Image task', worktree=str(task_root))
            for path in (topic_file, worktree_file, state_file):
                result = self.call('telegram.send', {'text': 'Attached', 'image': str(path)})
                self.assertTrue(result.ok, result.text)
                row = self.store.db.execute('SELECT image FROM outbox WHERE id=?',
                                            (result.data['outbox'],)).fetchone()
                self.assertEqual(row['image'], str(path.resolve()))
            outside.write_bytes(b'outside')
            link = root / 'escape.png'
            link.symlink_to(outside)
            missing = root / 'missing.png'
            too_large = root / 'large.pdf'
            with too_large.open('wb') as output:
                output.truncate(50 * 1024 * 1024 + 1)
            refusals = ((outside, 'under'), (link, 'under'), (missing, 'does not exist'),
                        (too_large, 'too large'))
            for path, phrase in refusals:
                result = self.call('telegram.send', {'text': 'Attached', 'image': str(path)})
                self.assertFalse(result.ok)
                self.assertIn(phrase, result.text)
            relative = self.call('telegram.send', {'text': 'Attached', 'image': 'topic.png'})
            self.assertFalse(relative.ok)
            self.assertIn('absolute', relative.text)
            self.store.task_update(task['id'], status='done')
        finally:
            outside.unlink(missing_ok=True)
            worktree_file.unlink(missing_ok=True)
            task_root.rmdir()

    def test_telegram_image_refuses_token_and_agent_history(self):
        root = Path(self.temp.name)
        token = root / 'bot-token'
        token.write_text('dummy')
        self.store.token_file = token.resolve()
        history = root / '.claude' / 'history.txt'
        history.parent.mkdir()
        history.write_text('dummy')
        codex_history = root / '.codex' / 'history.txt'
        codex_history.parent.mkdir()
        codex_history.write_text('dummy')
        with patch('coordinator.control_api.Path.home', return_value=root):
            for path in (token, history, codex_history):
                result = self.call('telegram.send', {'text': 'Attached', 'image': str(path)})
                self.assertFalse(result.ok)

    def test_workers_require_worktree_and_both_providers_accept_goals(self):
        task = self.store.task_create('1:2', 'Build')
        self.assertFalse(self.call('workers.spawn', {'task': task['id'], 'provider': 'claude',
                                                       'prompt': 'Build it'}).ok)
        self.store.task_update(task['id'], worktree=self.temp.name)
        codex = self.call('workers.spawn', {'task': task['id'], 'provider': 'codex',
                                           'prompt': 'Build it', 'goal': 'Tests pass'})
        self.assertTrue(codex.ok, codex.text)
        self.assertTrue(self.call('workers.goal', {'worker': codex.data['worker'], 'condition': 'New goal'}).ok)
        claude = self.call('workers.spawn', {'task': task['id'], 'provider': 'claude',
                                            'prompt': 'Build it', 'goal': 'Tests pass'})
        self.assertTrue(claude.ok)
        worker = claude.data['worker']
        self.assertEqual(self.store.db.execute('SELECT task,goal FROM workers WHERE id=?',
                                               (worker,)).fetchone()[:], (task['id'], 'Tests pass'))
        self.assertFalse(self.call('workers.goal', {'worker': worker, 'condition': 'x' * (control_api.TEXT_LIMIT + 1)}).ok)
        self.assertTrue(self.call('workers.goal', {'worker': worker, 'condition': 'clear'}).ok)
        self.assertIsNone(self.store.db.execute('SELECT goal FROM workers WHERE id=?', (worker,)).fetchone()[0])
        self.assertEqual(self.store.db.execute('SELECT op FROM service_requests').fetchone()[0], 'workers.goal')
        self.assertEqual(len(self.call('workers.list', {'task': task['id']}).data['workers']), 2)

    def test_workers_list_pages_short_summaries_and_workers_get_returns_the_full_record(self):
        task = self.store.task_create('1:2', 'Build', worktree=self.temp.name)
        ids = [self.call('workers.spawn', {'task': task['id'], 'provider': 'claude',
                                           'prompt': 'P%d ' % number + 'x' * 30000}).data['worker']
               for number in range(5)]
        self.store.worker_complete(ids[0], {'success': True, 'text': 'R' * 30000})
        first = self.call('workers.list', {'limit': 2})
        self.assertEqual([row['id'] for row in first.data['workers']], [ids[4], ids[3]])
        self.assertEqual(first.data['next_before'], ids[3])
        self.assertLess(len(json.dumps(first.data)), 2000)
        self.assertEqual(set(first.data['workers'][0]), {
            'id', 'task', 'topic', 'provider', 'model', 'effort', 'work', 'status', 'pid', 'created', 'updated',
            'prompt_head', 'result_head', 'iteration'})
        self.assertTrue(first.data['workers'][0]['prompt_head'].startswith('P4 xxx'))
        rest = self.call('workers.list', {'limit': 2, 'before': ids[1] + 1})
        self.assertEqual([row['id'] for row in rest.data['workers']], [ids[1], ids[0]])
        self.assertIsNone(rest.data['next_before'])
        self.assertEqual(rest.data['workers'][1]['result_head'], 'R' * control_api.HEAD + '…')
        done = self.call('workers.list', {'status': 'done'})
        self.assertEqual([row['id'] for row in done.data['workers']], [ids[0]])
        self.assertFalse(self.call('workers.list', {'limit': 0}).ok)
        self.assertFalse(self.call('workers.list', {'limit': control_api.WORKER_PAGE_LIMIT + 1}).ok)
        self.assertFalse(self.call('workers.list', {'status': 'lost'}).ok)
        full = self.call('workers.get', {'worker': ids[0]})
        self.assertEqual(len(full.data['prompt']), 30003)
        self.assertEqual(full.data['result']['text'], 'R' * 30000)
        self.assertFalse(self.call('workers.get', {'worker': 999}).ok)

    def workers_list_lease_fixture(self):
        job = self.store.task_create('1:2', 'Build', worktree=self.temp.name)
        other = self.store.task_create('1:2', 'Other', worktree=self.temp.name)
        third = self.store.task_create('1:2', 'Third', worktree=self.temp.name)
        numbers = itertools.count()

        def worker(task, work='dev', status='queued'):
            result = self.call('workers.spawn', {'task': task['id'], 'provider': 'codex',
                                               'prompt': 'Build it %d' % next(numbers), 'work': work})
            self.assertTrue(result.ok, result.text)
            worker_id = result.data['worker']
            self.store.db.execute('UPDATE workers SET status=? WHERE id=?', (status, worker_id))
            return worker_id

        holder = worker(job, status='running')
        worker(job, status='running')
        held = [worker(job), worker(job)]
        research = worker(job, work='research')
        for status in ('waiting_for_secret', 'waiting_for_quota', 'done', 'interrupted'):
            worker(job, status=status)
        worker(other, work='research', status='running')
        worker(other)
        other_holder = worker(third, status='running')
        other_held = worker(third)
        return {held[0]: holder, held[1]: holder, other_held: other_holder}, research

    def test_workers_list_write_lease_names_running_holder(self):
        waits, research = self.workers_list_lease_fixture()
        full = self.call('workers.get', {'worker': min(waits)})
        statements = []
        self.store.db.set_trace_callback(statements.append)
        try:
            result = self.call('workers.list')
        finally:
            self.store.db.set_trace_callback(None)
        self.assertTrue(result.ok, result.text)
        for row in result.data['workers']:
            if row['id'] in waits:
                self.assertEqual(row.get('waits_for'), waits[row['id']])
            else:
                self.assertNotIn('waits_for', row)
        self.assertEqual(result.text, '\n'.join(
            'Worker %d: %s, job %s, effort %s' % (row['id'], row['status'], row['task'], row['effort']) +
            (', waits for worker %d' % waits[row['id']] if row['id'] in waits else '')
            for row in result.data['workers']))
        self.assertEqual(len(statements), 3, statements)
        for params, expected_ids in (({'limit': 1}, [max(waits)]),
                                     ({'before': research, 'limit': 2}, sorted(waits)[:2][::-1])):
            with self.subTest(params=params):
                statements = []
                self.store.db.set_trace_callback(statements.append)
                try:
                    page = self.call('workers.list', params)
                finally:
                    self.store.db.set_trace_callback(None)
                self.assertEqual([row['id'] for row in page.data['workers']], expected_ids)
                self.assertEqual([row['waits_for'] for row in page.data['workers']],
                                 [waits[worker] for worker in expected_ids])
                self.assertEqual(len(statements), 3, statements)
        for params in ([{'status': status} for status in control_api.WORKER_STATUSES if status != 'queued'] +
                       [{'before': research + 1, 'limit': 1}, {'before': 1}]):
            with self.subTest(params=params):
                statements = []
                self.store.db.set_trace_callback(statements.append)
                try:
                    page = self.call('workers.list', params)
                finally:
                    self.store.db.set_trace_callback(None)
                self.assertTrue(all('waits_for' not in row for row in page.data['workers']))
                self.assertNotIn('waits for worker', page.text)
                self.assertEqual(len(statements), 2, statements)
        self.assertEqual(self.call('workers.get', {'worker': min(waits)}), full)

    def test_spawning_worker_preserves_finished_workers_on_the_task(self):
        task = self.store.task_create('1:2', 'Build', worktree=self.temp.name)
        older = self.call('workers.spawn', {'task': task['id'], 'provider': 'claude',
                                           'prompt': 'Question'}).data['worker']
        self.store.db.execute("UPDATE workers SET status='done' WHERE id=?", (older,))
        newer = self.call('workers.spawn', {'task': task['id'], 'provider': 'claude', 'prompt': 'Continue'}).data['worker']
        self.assertEqual(self.store.db.execute('SELECT status FROM workers WHERE id=?', (older,)).fetchone()[0],
                         'done')
        self.assertEqual([row['id'] for row in self.call(
            'workers.list', {'task': task['id'], 'status': 'done'}).data['workers']], [older])
        self.assertEqual(self.call('workers.get', {'worker': older}).data['id'], older)
        self.assertEqual(self.store.db.execute('SELECT status FROM workers WHERE id=?', (newer,)).fetchone()[0],
                         'queued')

    def expected_spawn(self, worker, existing=False):
        task = self.store.db.execute('SELECT task FROM workers WHERE id=?', (worker,)).fetchone()[0]
        data = {'worker': worker, 'results': str(Path(self.temp.name).resolve() / 'results' / ('task-' + str(task)))}
        if existing:
            data['existing'] = True
        return data

    def test_spawn_dedupe_reuses_queued_worker(self):
        task = self.store.task_create('1:2', 'Build', worktree=self.temp.name)
        params = {'task': task['id'], 'provider': 'codex', 'prompt': 'Build it'}
        first = self.call('workers.spawn', params)
        before = [dict(row) for row in self.store.db.execute('SELECT * FROM workers')]
        second = self.call('workers.spawn', params)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM workers').fetchone()[0], 1)
        self.assertEqual((first.ok, first.text, first.data),
                         (True, 'Worker %d queued.' % first.data['worker'], self.expected_spawn(first.data['worker'])))
        self.assertEqual((second.ok, second.text, second.data),
                         (True, 'Worker %d is already queued for this request.' % first.data['worker'],
                          self.expected_spawn(first.data['worker'], existing=True)))
        self.assertEqual([dict(row) for row in self.store.db.execute('SELECT * FROM workers')], before)

    def test_spawn_dedupe_matches_only_active_statuses(self):
        for status in ('queued', 'waiting_for_secret', 'waiting_for_quota', 'running', 'done', 'interrupted'):
            with self.subTest(status=status):
                task = self.store.task_create('1:2', status, worktree=self.temp.name)
                params = {'task': task['id'], 'provider': 'codex', 'prompt': 'Build it'}
                first = self.call('workers.spawn', params).data['worker']
                self.store.db.execute('UPDATE workers SET status=? WHERE id=?', (status, first))
                second = self.call('workers.spawn', params)
                rows = self.store.db.execute('SELECT id,status FROM workers WHERE task=? ORDER BY id',
                                             (task['id'],)).fetchall()
                if status in ('done', 'interrupted'):
                    self.assertEqual(len(rows), 2)
                    self.assertEqual([tuple(row) for row in rows], [(first, status), (second.data['worker'], 'queued')])
                    self.assertNotEqual(second.data['worker'], first)
                    self.assertEqual((second.ok, second.text, second.data),
                                     (True, 'Worker %d queued.' % second.data['worker'],
                                      self.expected_spawn(second.data['worker'])))
                else:
                    self.assertEqual([tuple(row) for row in rows], [(first, status)])
                    self.assertEqual((second.ok, second.text, second.data),
                                     (True, 'Worker %d is already %s for this request.' % (first, status),
                                      self.expected_spawn(first, existing=True)))

    def test_spawn_dedupe_excludes_worker_with_pending_stop(self):
        for state in ('queued', 'sending'):
            with self.subTest(stop_state=state):
                task = self.store.task_create('1:2', state, worktree=self.temp.name)
                params = {'task': task['id'], 'provider': 'codex', 'prompt': 'Build it'}
                first = self.call('workers.spawn', params).data['worker']
                with self.store.db:
                    self.store.db.execute("UPDATE workers SET status='running' WHERE id=?", (first,))
                stop = self.call('workers.stop', {'worker': first})
                self.assertTrue(stop.ok, stop.text)
                self.assertEqual(stop.state, 'queued')
                if state == 'sending':
                    self.store.service_request_settle(stop.data['request'], 'sending')
                self.assertEqual(self.store.db.execute('SELECT state FROM service_requests WHERE id=?',
                    (stop.data['request'],)).fetchone()[0], state)

                second = self.call('workers.spawn', params)

                self.assertTrue(second.ok, second.text)
                self.assertNotEqual(second.data['worker'], first)
                self.assertEqual((second.text, second.data),
                                 ('Worker %d queued.' % second.data['worker'], self.expected_spawn(second.data['worker'])))
                self.assertEqual([tuple(row) for row in self.store.db.execute(
                    'SELECT id,status FROM workers WHERE task=? ORDER BY id', (task['id'],))],
                    [(first, 'running'), (second.data['worker'], 'queued')])

    def test_spawn_dedupe_excludes_interrupted_worker_after_settled_stop(self):
        task = self.store.task_create('1:2', 'Build', worktree=self.temp.name)
        params = {'task': task['id'], 'provider': 'codex', 'prompt': 'Build it'}
        first = self.call('workers.spawn', params).data['worker']
        with self.store.db:
            self.store.db.execute("UPDATE workers SET status='running' WHERE id=?", (first,))
        stop = self.call('workers.stop', {'worker': first})
        self.store.worker_complete(first, {'success': False, 'failure_code': 'owner_stopped'}, 'interrupted')
        self.store.service_request_settle(stop.data['request'], 'done', {'stop': 'stopped'})

        second = self.call('workers.spawn', params)

        self.assertTrue(second.ok, second.text)
        self.assertNotEqual(second.data['worker'], first)
        self.assertEqual((second.text, second.data),
                         ('Worker %d queued.' % second.data['worker'], self.expected_spawn(second.data['worker'])))
        self.assertEqual(self.store.db.execute('SELECT status FROM workers WHERE id=?', (first,)).fetchone()[0],
                         'interrupted')

    def test_spawn_dedupe_reuses_running_worker_after_settled_stop(self):
        for state in ('done', 'refused', 'uncertain'):
            with self.subTest(stop_state=state):
                task = self.store.task_create('1:2', state, worktree=self.temp.name)
                params = {'task': task['id'], 'provider': 'codex', 'prompt': 'Build it'}
                first = self.call('workers.spawn', params).data['worker']
                with self.store.db:
                    self.store.db.execute("UPDATE workers SET status='running' WHERE id=?", (first,))
                stop = self.call('workers.stop', {'worker': first})
                self.store.service_request_settle(stop.data['request'], state)

                second = self.call('workers.spawn', params)

                self.assertEqual((second.ok, second.text, second.data),
                                 (True, 'Worker %d is already running for this request.' % first,
                                  self.expected_spawn(first, existing=True)))
                self.assertEqual(self.store.db.execute('SELECT count(*) FROM workers WHERE task=?',
                                                       (task['id'],)).fetchone()[0], 1)

    def test_spawn_dedupe_excludes_worker_resumed_by_steer(self):
        for status in ('queued', 'waiting_for_secret', 'waiting_for_quota', 'running'):
            with self.subTest(status=status):
                task = self.store.task_create('1:2', status, worktree=self.temp.name)
                params = {'task': task['id'], 'provider': 'codex', 'prompt': 'Build it'}
                first = self.call('workers.spawn', params).data['worker']
                completed = self.store.worker_complete(first, {'success': True, 'session_id': 'session-1'})
                self.assertEqual((completed['kind'], completed['source_worker']), ('worker_result', first))
                steer = self.call('workers.steer', {'worker': first, 'prompt': 'Check the tests'})
                self.assertTrue(steer.ok, steer.text)
                service = Service(self.store, object(), object(), Path(self.temp.name))
                self.assertTrue(asyncio.run(service.controls_once()))
                resumed = service.workers.get(first)
                self.assertEqual((resumed['status'], resumed['prompt'], resumed['fresh'], resumed['session'],
                                  resumed['host']),
                                 ('queued', 'Check the tests', 0, 'session-1',
                                  'worker-%d-resume-%d' % (first, steer.data['request'])))
                with self.store.db:
                    self.store.db.execute('UPDATE workers SET status=? WHERE id=?', (status, first))

                second = self.call('workers.spawn', dict(params, prompt='Check the tests'))

                self.assertTrue(second.ok, second.text)
                self.assertNotEqual(second.data['worker'], first)
                self.assertEqual((second.text, second.data),
                                 ('Worker %d queued.' % second.data['worker'], self.expected_spawn(second.data['worker'])))
                fresh = service.workers.get(second.data['worker'])
                self.assertEqual((fresh['status'], fresh['prompt'], fresh['session'], fresh['fresh']),
                                 ('queued', 'Check the tests', None, 1))

    def test_spawn_dedupe_reuses_launched_worker_without_a_result(self):
        for status in ('running', 'waiting_for_quota'):
            with self.subTest(status=status):
                task = self.store.task_create('1:2', status, worktree=self.temp.name)
                params = {'task': task['id'], 'provider': 'codex', 'prompt': 'Build it'}
                first = self.call('workers.spawn', params).data['worker']
                with self.store.db:
                    self.store.db.execute('UPDATE workers SET status=?,session=?,fresh=0 WHERE id=?',
                                          (status, 'session-1', first))

                second = self.call('workers.spawn', params)

                self.assertEqual((second.ok, second.text, second.data),
                                 (True, 'Worker %d is already %s for this request.' % (first, status),
                                  self.expected_spawn(first, existing=True)))
                self.assertEqual(self.store.db.execute('SELECT count(*) FROM workers WHERE task=?',
                                                       (task['id'],)).fetchone()[0], 1)

    def test_spawn_dedupe_ignores_other_workers_stop_and_result(self):
        task = self.store.task_create('1:2', 'Build', worktree=self.temp.name)
        params = {'task': task['id'], 'provider': 'codex', 'prompt': 'Build it'}
        other = self.call('workers.spawn', dict(params, prompt='Other work')).data['worker']
        self.store.worker_complete(other, {'success': True, 'session_id': 'other-session'})
        with self.store.db:
            self.store.db.execute("UPDATE workers SET status='running' WHERE id=?", (other,))
        self.call('workers.stop', {'worker': other})
        first = self.call('workers.spawn', params).data['worker']
        self.call('workers.steer', {'worker': first, 'prompt': 'More input'})

        second = self.call('workers.spawn', params)

        self.assertEqual((second.ok, second.text, second.data),
                         (True, 'Worker %d is already queued for this request.' % first,
                          self.expected_spawn(first, existing=True)))
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM workers').fetchone()[0], 2)

    def test_spawn_dedupe_distinguishes_every_request_field(self):
        changes = {'provider': 'claude', 'prompt': 'Build something else', 'work': 'dev',
                   'model': 'gpt-6.1-sol', 'goal': 'Tests pass', 'effort': 'high', 'task': None}
        for field, value in changes.items():
            with self.subTest(field=field):
                task = self.store.task_create('1:2', field, worktree=self.temp.name)
                params = {'task': task['id'], 'provider': 'codex', 'prompt': 'Build it'}
                first = self.call('workers.spawn', params).data['worker']
                if field == 'task':
                    value = self.store.task_create('1:2', 'Other job', worktree=self.temp.name)['id']
                second = self.call('workers.spawn', dict(params, **{field: value}))
                self.assertTrue(second.ok, second.text)
                self.assertNotEqual(second.data['worker'], first)
                self.assertEqual(second.data, self.expected_spawn(second.data['worker']))
                self.assertEqual(second.text, 'Worker %d queued.' % second.data['worker'])
                self.assertEqual(self.store.db.execute('SELECT count(*) FROM workers WHERE id IN (?,?)',
                                                       (first, second.data['worker'])).fetchone()[0], 2)

    def test_spawn_dedupe_normalizes_missing_effort_to_medium(self):
        for first_effort, second_effort in (({}, {'effort': 'medium'}), ({'effort': 'medium'}, {})):
            with self.subTest(first=first_effort):
                task = self.store.task_create('1:2', 'Build', worktree=self.temp.name)
                params = {'task': task['id'], 'provider': 'codex', 'prompt': 'Build it'}
                first = self.call('workers.spawn', dict(params, **first_effort)).data['worker']
                second = self.call('workers.spawn', dict(params, **second_effort))
                self.assertEqual(second.data, self.expected_spawn(first, existing=True))
                self.assertEqual(self.store.db.execute('SELECT effort FROM workers WHERE task=?',
                                                       (task['id'],)).fetchall()[0][0], 'medium')
                self.assertEqual(self.store.db.execute('SELECT count(*) FROM workers WHERE task=?',
                                                       (task['id'],)).fetchone()[0], 1)

    def test_spawn_writes_the_worker_row(self):
        task = self.store.task_create('1:2', 'Build', worktree=self.temp.name)
        params = {'task': task['id'], 'provider': 'codex', 'prompt': 'Build it'}
        with patch('coordinator.control_api.time.time', return_value=123.0):
            result = self.call('workers.spawn', params)
        rows = [dict(row) for row in self.store.db.execute('SELECT * FROM workers WHERE task=?', (task['id'],))]
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual((result.ok, result.text, result.data),
                         (True, 'Worker %d queued.' % row['id'], self.expected_spawn(row['id'])))
        self.assertEqual((row['task'], row['topic'], row['provider'], row['prompt'], row['cwd'],
                          json.loads(row['workspace']), row['goal'], row['model'], row['effort'],
                          row['work'], row['status'], row['created'], row['updated']),
                         (task['id'], '1:2', 'codex', 'Build it', self.temp.name,
                          {'cwd': self.temp.name, 'isolated': True}, None, None, 'medium',
                          'research', 'queued', 123.0, 123.0))

    def test_spawn_dedupe_keeps_refusals_before_existing_worker(self):
        for reason in ('unknown', 'done', 'dropped', 'unset_worktree', 'missing_worktree', 'accounts', 'spawn_refusal'):
            with self.subTest(reason=reason):
                task = self.store.task_create('1:2', reason, worktree=self.temp.name)
                params = {'task': task['id'], 'provider': 'claude', 'prompt': 'Build it'}
                first = self.call('workers.spawn', params).data['worker']
                self.store.db.execute("UPDATE workers SET status='running' WHERE id=?", (first,))
                if reason == 'unknown':
                    params['task'] = 999999
                    expected = 'Unknown job.'
                elif reason in ('done', 'dropped'):
                    self.store.task_update(task['id'], status=reason)
                    expected = 'Workers require an open job.'
                elif reason in ('unset_worktree', 'missing_worktree'):
                    self.store.task_update(task['id'], worktree=None if reason == 'unset_worktree'
                                           else str(Path(self.temp.name) / 'missing'))
                    expected = 'Create the job worktree first.'
                elif reason == 'accounts':
                    self.store.put('accounts', {})
                    expected = 'No Claude account is connected. Use provider codex.'
                else:
                    expected = 'Provider refuses this spawn.'
                with patch('coordinator.workers.spawn_refusal',
                           return_value=expected if reason == 'spawn_refusal' else None):
                    result = self.call('workers.spawn', params)
                self.assertEqual((result.ok, result.state, result.text), (False, 'refused', expected))
                self.assertEqual(self.store.db.execute('SELECT count(*) FROM workers WHERE id>=?',
                                                       (first,)).fetchone()[0], 1)
                self.store.put('accounts', {'work': {'config_dir': None, 'enabled': True}})

    def test_spawn_dedupe_chooses_lowest_id(self):
        task = self.store.task_create('1:2', 'Build', worktree=self.temp.name)
        params = {'task': task['id'], 'provider': 'codex', 'prompt': 'Build it'}
        first = self.call('workers.spawn', params).data['worker']
        self.store.db.execute('INSERT INTO workers (task,topic,provider,prompt,cwd,workspace,goal,model,effort,work,'
                              'created,updated) SELECT task,topic,provider,prompt,cwd,workspace,goal,model,effort,'
                              'work,created,updated FROM workers WHERE id=?', (first,))
        self.store.db.execute("UPDATE workers SET status='running' WHERE id=?", (first,))
        result = self.call('workers.spawn', params)
        self.assertEqual((result.ok, result.text, result.data),
                         (True, 'Worker %d is already running for this request.' % first,
                          self.expected_spawn(first, existing=True)))
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM workers').fetchone()[0], 2)

    def test_spawn_dedupe_locks_before_lookup(self):
        task = self.store.task_create('1:2', 'Build', worktree=self.temp.name)
        self.store.db.commit()
        statements = []
        self.store.db.set_trace_callback(statements.append)
        try:
            result = self.call('workers.spawn', {'task': task['id'], 'provider': 'codex', 'prompt': 'Build it'})
        finally:
            self.store.db.set_trace_callback(None)
        self.assertTrue(result.ok, result.text)
        normalized = [' '.join(sql.split()) for sql in statements]
        self.assertIn('BEGIN IMMEDIATE', normalized)
        begin = normalized.index('BEGIN IMMEDIATE')
        lookup = next(index for index, sql in enumerate(normalized) if sql.startswith('SELECT') and 'FROM workers' in sql)
        insert = next(index for index, sql in enumerate(normalized) if sql.startswith('INSERT INTO workers'))
        self.assertLess(begin, lookup)
        self.assertLess(lookup, insert)
        self.assertEqual(normalized[-1], 'COMMIT')

    def test_spawn_dedupe_reuses_worker_across_processes(self):
        task = self.store.task_create('1:2', 'Build', worktree=self.temp.name)
        self.store.db.commit()
        code = """
import json
from pathlib import Path
import sys
from coordinator import control_api
from coordinator.store import Store
store = Store(Path(sys.argv[1]))
print('ready', flush=True)
sys.stdin.readline()
try:
    with store.db:
        result = control_api.call(store, 'workers.spawn',
                                  {'task': int(sys.argv[2]), 'provider': 'codex', 'prompt': 'Build it'}, topic='1:2')
    print(json.dumps({'ok': result.ok, 'text': result.text, 'data': result.data}), flush=True)
finally:
    store.close()
"""
        children = []
        try:
            for _ in range(2):
                child = subprocess.Popen([sys.executable, '-c', code, self.temp.name, str(task['id'])],
                                         stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                children.append(child)
                self.assertEqual(child.stdout.readline(), 'ready\n')
            for child in children:
                child.stdin.write('spawn\n')
                child.stdin.flush()
            replies = []
            for child in children:
                stdout, stderr = child.communicate(timeout=30)
                self.assertEqual(child.returncode, 0, stderr)
                replies.append(json.loads(stdout))
            rows = self.store.db.execute('SELECT id,status FROM workers').fetchall()
            self.assertEqual(len(rows), 1)
            worker_id = rows[0]['id']
            self.assertEqual(rows[0]['status'], 'queued')
            self.assertTrue(all(reply['ok'] for reply in replies), replies)
            self.assertCountEqual([reply['data'] for reply in replies],
                                  [self.expected_spawn(worker_id), self.expected_spawn(worker_id, existing=True)])
            self.assertCountEqual([reply['text'] for reply in replies],
                                  ['Worker %d queued.' % worker_id,
                                   'Worker %d is already queued for this request.' % worker_id])
        finally:
            for child in children:
                if child.poll() is None:
                    child.kill()
                child.communicate(timeout=30)

    def test_worker_briefs_have_their_own_limit_and_other_text_keeps_16000(self):
        task = self.store.task_create('1:2', 'Build', worktree=self.temp.name)
        spawn = {'task': task['id'], 'provider': 'claude'}
        self.assertTrue(self.call('workers.spawn', dict(spawn, prompt='b' * 13634)).ok)
        worker = self.call('workers.spawn', dict(spawn, prompt='b' * control_api.BRIEF_LIMIT)).data['worker']
        refused = self.call('workers.spawn', dict(spawn, prompt='b' * (control_api.BRIEF_LIMIT + 1)))
        self.assertFalse(refused.ok)
        self.assertIn('at most %d characters' % control_api.BRIEF_LIMIT, refused.text)
        self.assertEqual(self.call('workers.steer', {'worker': worker, 'prompt': 's' * 20000}).state, 'queued')
        self.assertFalse(self.call('workers.spawn', dict(spawn, prompt='Go', goal='g' * 16001)).ok)
        self.assertFalse(self.call('telegram.send', {'text': 't' * 16001}).ok)
        self.assertFalse(self.call('tasks.update', {'task': task['id'], 'notes': 'n' * 16001}).ok)
        self.assertTrue(self.call('tasks.update', {'task': task['id'], 'notes': 'n' * 16000}).ok)
        sent = self.call('telegram.send', {'text': ('word ' * 3200).strip()})
        self.assertTrue(sent.ok)
        parts = [row['text'] for row in self.store.db.execute('SELECT text FROM outbox ORDER BY id')]
        self.assertEqual(len(parts), 4)
        self.assertTrue(all(len(part) <= 4096 for part in parts))

    def test_mcp_schema_advertises_the_brief_and_text_limits(self):
        from coordinator import mcp
        spawn = mcp.input_schema(control_api.BY_ID['workers.spawn'])['properties']
        self.assertEqual(spawn['prompt']['maxLength'], control_api.BRIEF_LIMIT)
        self.assertEqual(spawn['goal']['maxLength'], control_api.TEXT_LIMIT)
        send = mcp.input_schema(control_api.BY_ID['telegram.send'])['properties']
        self.assertEqual(send['text']['maxLength'], control_api.TEXT_LIMIT)

    def test_service_operations_are_durable_requests(self):
        task = self.store.task_create('1:2', 'Build', worktree=self.temp.name)
        worker = self.call('workers.spawn', {'task': task['id'], 'provider': 'codex', 'prompt': 'Build'}).data['worker']
        self.assertEqual(self.call('workers.steer', {'worker': worker, 'prompt': 'Check tests'}).state, 'queued')
        self.assertEqual(self.call('workers.stop', {'worker': worker}).state, 'queued')
        self.assertEqual(self.call('service.restart', {'reason': 'New version'}).state, 'queued')
        self.assertEqual([row[0] for row in self.store.db.execute('SELECT op FROM service_requests ORDER BY id')],
                     ['workers.steer', 'workers.stop', 'service.restart'])
        self.assertIsNone(self.store.get('restart_requested_v2'))

    def test_mcp_steer_to_done_worker_returns_error_without_queueing(self):
        task = self.store.task_create('1:2', 'Build', worktree=self.temp.name)
        worker = self.call('workers.spawn', {'task': task['id'], 'provider': 'codex', 'prompt': 'Build'}).data['worker']
        for status in ('done', 'interrupted'):
            with self.subTest(status=status):
                with self.store.db:
                    self.store.db.execute('UPDATE workers SET status=?,session=?,fresh=? WHERE id=?',
                                          (status, 'saved-session' if status == 'interrupted' else None,
                                           0 if status == 'interrupted' else 1, worker))
                reply = mcp.dispatch(self.store, {'method': 'tools/call', 'params': {
                    'name': 'workers_steer', 'arguments': {'worker': worker, 'prompt': 'Continue'}}})
                self.assertTrue(reply['isError'])
                self.assertIn('Worker cannot receive input.', reply['content'][0]['text'])
                self.assertIn(status, reply['content'][0]['text'])
                self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM service_requests').fetchone()[0], 0)

    def test_mcp_stop_to_inactive_worker_returns_error_without_queueing(self):
        task = self.store.task_create('1:2', 'Build', worktree=self.temp.name)
        worker = self.call('workers.spawn', {'task': task['id'], 'provider': 'codex', 'prompt': 'Build'}).data['worker']
        for status in ('done', 'interrupted'):
            with self.subTest(status=status):
                with self.store.db:
                    self.store.db.execute('UPDATE workers SET status=? WHERE id=?', (status, worker))
                reply = mcp.dispatch(self.store, {'method': 'tools/call', 'params': {
                    'name': 'workers_stop', 'arguments': {'worker': worker}}})
                self.assertTrue(reply['isError'])
                self.assertIn('Worker is not running.', reply['content'][0]['text'])
                self.assertIn(status, reply['content'][0]['text'])
                self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM service_requests').fetchone()[0], 0)

    def test_worktree_handler_records_reused_path(self):
        task = self.store.task_create('1:2', 'Build')
        workspace = {'cwd': self.temp.name, 'branch': 'torii/task-1', 'isolated': True}
        with patch('coordinator.workspaces.prepare_task_workspace', new=AsyncMock(return_value=workspace)) as create:
            first = self.call('worktree.create', {'task': task['id']})
            second = self.call('worktree.create', {'task': task['id']})
        self.assertEqual(first.data['cwd'], self.temp.name)
        self.assertEqual(second.data['worktree'], self.temp.name)
        create.assert_awaited_once()

    def test_worktree_create_passes_base_and_returns_fetched_start(self):
        root = Path(self.temp.name)
        repository = root / 'repository'
        origin = root / 'origin.git'
        subprocess.run(('git', 'init', '-q', '--bare', str(origin)), check=True)
        subprocess.run(('git', 'init', '-q', str(repository)), check=True)

        def git(*args):
            return subprocess.run(('git', '-C', str(repository)) + args, check=True,
                                  text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout.strip()

        git('-c', 'user.email=t@example.com', '-c', 'user.name=T',
            'commit', '-q', '--allow-empty', '-m', 'base')
        git('remote', 'add', 'origin', str(origin))
        git('checkout', '-qb', 'other')
        git('-c', 'user.email=t@example.com', '-c', 'user.name=T',
            'commit', '-q', '--allow-empty', '-m', 'other')
        tip = git('rev-parse', 'HEAD')
        git('push', '-q', 'origin', 'other')
        git('checkout', '-q', '-')
        with self.store.db:
            self.store.db.execute("UPDATE topics SET cwd=? WHERE id='1:2'", (str(repository),))
        task = self.store.task_create('1:2', 'Build')
        result = self.call('worktree.create', {'task': task['id'], 'base': 'other'})
        self.assertTrue(result.ok, result.text)
        self.assertEqual((result.data['base_ref'], result.data['base_commit'], result.data['fetched']),
                         ('origin/other', tip, True))
        self.assertEqual(self.store.task_get(task['id'])['worktree'], result.data['cwd'])

    def test_saved_worktree_reports_that_base_was_not_applied_without_fetch(self):
        task = self.store.task_create('1:2', 'Build', worktree=self.temp.name)
        with patch('coordinator.workspaces.prepare_task_workspace', new=AsyncMock()) as create:
            result = self.call('worktree.create', {'task': task['id'], 'base': 'missing'})
        self.assertTrue(result.ok, result.text)
        self.assertIn('not applied', result.text)
        self.assertEqual(result.data['worktree'], self.temp.name)
        self.assertEqual(result.data['base_ref'], 'HEAD')
        self.assertIs(result.data['fetched'], False)
        create.assert_not_awaited()

    def test_worktree_create_reports_ignored_base_when_unsaved_worktree_exists(self):
        task = self.store.task_create('1:2', 'Build')
        workspace = {'cwd': self.temp.name, 'branch': 'torii/task-1', 'isolated': True,
                     'base_ref': 'HEAD', 'base_commit': 'abc', 'fetched': False}
        with patch('coordinator.workspaces.prepare_task_workspace', new=AsyncMock(return_value=workspace)) as create:
            result = self.call('worktree.create', {'task': task['id'], 'base': 'missing'})
        self.assertTrue(result.ok, result.text)
        self.assertIn('not applied', result.text)
        create.assert_awaited_once_with(Path(self.temp.name), self.store.directory, task['id'], base='missing')

    def test_two_topics_on_one_repository_create_worktrees_for_their_first_tasks(self):
        repository = Path(self.temp.name) / 'repository'
        repository.mkdir()
        for args in (('init', '-q'), ('-c', 'user.email=t@example.com', '-c', 'user.name=T',
                                      'commit', '-q', '--allow-empty', '-m', 'base')):
            subprocess.run(('git', '-C', str(repository)) + args, check=True)
        with self.store.db:
            self.store.db.execute("UPDATE topics SET cwd=? WHERE id='1:2'", (str(repository),))
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:3',1,3,'twin',?,1)",
                                  (str(repository),))
        first = self.store.task_create('1:2', 'One')
        second = self.store.task_create('1:3', 'Two')
        self.assertEqual((first['number'], second['number']), (1, 1))
        made = [control_api.call(self.store, 'worktree.create', {'task': task['id']}) for task in (first, second)]
        self.assertTrue(all(result.ok for result in made), [result.text for result in made])
        self.assertEqual([result.data['branch'] for result in made],
                         ['torii/task-%d' % first['id'], 'torii/task-%d' % second['id']])

    def test_task_with_an_old_style_branch_keeps_its_saved_worktree(self):
        repository = Path(self.temp.name) / 'repository'
        repository.mkdir()
        subprocess.run(('git', '-C', str(repository), 'init', '-q'), check=True)
        subprocess.run(('git', '-C', str(repository), '-c', 'user.email=t@example.com', '-c', 'user.name=T',
                        'commit', '-q', '--allow-empty', '-m', 'base'), check=True)
        for number in range(4):
            task = self.store.task_create('1:2', 'Earlier %d' % number)
        legacy = Path(self.temp.name) / 'worktrees' / ('task-%d' % task['id'])
        subprocess.run(('git', '-C', str(repository), 'worktree', 'add', '-q', '-b',
                        'torii/task-%d' % task['number'], str(legacy)), check=True)
        self.store.task_update(task['id'], worktree=str(legacy))
        reused = self.call('worktree.create', {'task': task['id']})
        self.assertEqual(reused.data['worktree'], str(legacy))
        self.assertTrue(self.call('workers.spawn', {'task': task['id'], 'provider': 'claude', 'prompt': 'Go'}).ok)

    def test_account_menu_still_uses_available_ops(self):
        with self.store.db:
            self.assertTrue(handle_control(self.store, '1:2', 20, '/accounts'))
            self.assertTrue(control_api.call(self.store, 'model.worker', {'model': 'opus'}, topic='1:2').ok)
            self.assertTrue(handle_control(self.store, '1:2', 22, '/accounts'))
        self.assertEqual(self.store.get('worker_model'), 'opus')
        self.assertGreater(self.store.db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0], 0)


class RestartTests(unittest.IsolatedAsyncioTestCase):
    async def test_restart_waits_for_outbox_and_clears_durable_request(self):
        with tempfile.TemporaryDirectory() as root:
            store = Store(Path(root))
            try:
                with store.db:
                    store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:2',1,2,'test','',1)")
                    store.put('owner', 1)
                    store.put('mode', 'group')
                    store.put('group', 1)
                    control_api.call(store, 'service.restart', {'reason': 'New build'})
                    outbox = store.enqueue_report('1:2', 'Last reply')
                service = Service(store, object(), object(), Path(root))
                await service.controls_once()
                with patch.object(service, 'exit_for_restart') as exit_service:
                    self.assertFalse(await service.restart_once())
                    exit_service.assert_not_called()
                    store.delivered(outbox, 10)
                    self.assertTrue(await service.restart_once())
                    exit_service.assert_called_once()
                self.assertIsNone(store.get('restart_requested_v2'))
            finally:
                store.close()

    async def test_service_runs_queued_worker_controls_and_steers_running_goal(self):
        with tempfile.TemporaryDirectory() as root:
            store = Store(Path(root))
            store.put('accounts', {'work': {'config_dir': None, 'enabled': True}})
            try:
                with store.db:
                    store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:2',1,2,'test','',1)")
                task = store.task_create('1:2', 'Build', worktree=root)
                spawned = control_api.call(store, 'workers.spawn',
                                           {'task': task['id'], 'provider': 'claude', 'prompt': 'Build'})
                worker = spawned.data['worker']
                control_api.call(store, 'workers.steer', {'worker': worker, 'prompt': 'Check tests'})
                control_api.call(store, 'workers.goal', {'worker': worker, 'condition': 'All tests pass'})
                service = Service(store, object(), object(), Path(root))
                self.assertTrue(await service.controls_once())
                row = store.db.execute('SELECT prompt,goal FROM workers WHERE id=?', (worker,)).fetchone()
                self.assertIn('Check tests', row['prompt'])
                self.assertEqual(row['goal'], 'All tests pass')
                self.assertEqual([row[0] for row in store.db.execute(
                    "SELECT state FROM service_requests WHERE op IN ('workers.steer','workers.goal')")],
                                 ['done', 'done'])
                with store.db:
                    store.db.execute("UPDATE workers SET status='running' WHERE id=?", (worker,))
                control = AsyncMock()
                control.steer.return_value = 'received'
                service.workers.controls[worker] = control
                control_api.call(store, 'workers.goal', {'worker': worker, 'condition': 'clear'})
                self.assertTrue(await service.controls_once())
                self.assertEqual(control.steer.await_args.args[1], '/goal clear')
                self.assertEqual(store.db.execute("SELECT state FROM service_requests WHERE op='workers.goal' "
                                                  'ORDER BY id DESC LIMIT 1').fetchone()[0],
                                 'done')
            finally:
                store.close()


    async def test_running_codex_goals_use_the_native_control_and_wait_for_receipts(self):
        from coordinator.service import Service
        from tests.support import until
        from unittest.mock import AsyncMock
        with tempfile.TemporaryDirectory() as root:
            store = Store(Path(root))
            self.addCleanup(store.close)
            with store.db:
                store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:2',1,2,'test','',1)")
            task = store.task_create('1:2', 'Build', worktree=root)
            worker = control_api.call(store, 'workers.spawn',
                {'task': task['id'], 'provider': 'codex', 'prompt': 'Build'}).data['worker']
            store.db.execute("UPDATE workers SET status='running' WHERE id=?", (worker,))
            service = Service(store, object(), object(), Path(root))
            control = AsyncMock()
            control.goal.return_value = 'received'
            service.workers.controls[worker] = control
            for condition in ('Tests pass', 'clear'):
                result = control_api.call(store, 'workers.goal', {'worker': worker, 'condition': condition})
                self.assertTrue(result.ok, result.text)
                self.assertTrue(await service.controls_once())
                await until(lambda: store.db.execute("SELECT state FROM service_requests WHERE op='workers.goal' "
                                                     'ORDER BY id DESC LIMIT 1')
                            .fetchone()[0] == 'done')
                self.assertEqual(control.goal.await_args.args[1], condition)
                self.assertEqual(store.db.execute("SELECT state FROM service_requests WHERE op='workers.goal' "
                                                  'ORDER BY id DESC LIMIT 1')
                                 .fetchone()[0], 'done')
            control.steer.assert_not_awaited()

    async def test_steering_a_queued_worker_cannot_grow_its_brief_past_the_limit(self):
        with tempfile.TemporaryDirectory() as root:
            store = Store(Path(root))
            store.put('accounts', {'work': {'config_dir': None, 'enabled': True}})
            try:
                with store.db:
                    store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:2',1,2,'test','',1)")
                task = store.task_create('1:2', 'Build', worktree=root)
                brief = 'b' * (control_api.BRIEF_LIMIT - 100)
                worker = control_api.call(store, 'workers.spawn', {'task': task['id'], 'provider': 'claude',
                                                                   'prompt': brief}).data['worker']
                control_api.call(store, 'workers.steer', {'worker': worker, 'prompt': 'short follow-up'})
                control_api.call(store, 'workers.steer', {'worker': worker, 'prompt': 'x' * 200})
                service = Service(store, object(), object(), Path(root))
                self.assertTrue(await service.controls_once())
                prompt = store.db.execute('SELECT prompt FROM workers WHERE id=?', (worker,)).fetchone()[0]
                self.assertEqual(prompt, brief + '\n\nFollow-up:\nshort follow-up')
                states = [tuple(row) for row in store.db.execute('SELECT state,result FROM service_requests ORDER BY id')]
                self.assertEqual(states[0], ('done', None))
                self.assertEqual(states[1][0], 'refused')
                self.assertIn('would exceed', states[1][1])
            finally:
                store.close()

if __name__ == '__main__':
    unittest.main()
