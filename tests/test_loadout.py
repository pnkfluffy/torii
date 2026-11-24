import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from coordinator import control_api, loadout, mcp, parent_state
from coordinator.policy import chat_policy
from coordinator.store import Store


class LoadoutTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = Store(Path(self.temp.name))
        self.addCleanup(self.store.close)
        with self.store.db:
            for topic, thread in (('1:2', 2), ('1:3', 3)):
                self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                                      (topic, 1, thread, topic, self.temp.name))
            self.store.put('group', 1)
            self.store.put('mode', 'group')
            self.store.put('coordinator_home_topic', '1:2')
        self.owner = self.store.message_save('1:2', 'owner', 'Use a temporary model', telegram_message=42)['id']
        self.task = self.store.task_create('1:2', 'Task', worktree=self.temp.name)

    def call(self, op, **params):
        with self.store.db:
            return control_api.call(self.store, op, params, topic='1:2', source='cli')

    def swap(self, key='workers.codex.effort', value='low', until='3d', **params):
        return self.call('loadout.swap', key=key, value=value, until=until, reason='Owner requested',
                         message=self.owner, **params)

    def test_resolution_order_and_explicit_spawn(self):
        self.store.put('coordinator_model', 'claude-custom')
        self.assertEqual(loadout.effective(self.store, '1:2')[0]['coordinators']['claude']['model'], 'claude-custom')
        self.store.put('codex_worker_effort', 'medium')
        self.assertTrue(self.swap(value='low').ok)
        self.assertTrue(self.swap(value='max', topic='1:2').ok)
        self.assertTrue(self.swap(value='xhigh').ok)
        self.assertEqual(loadout.effective(self.store, '1:2')[0]['workers']['codex']['effort'], 'max')
        self.assertEqual(loadout.effective(self.store, '1:3')[0]['workers']['codex']['effort'], 'xhigh')
        self.assertEqual(loadout.effective(self.store, '1:2', {'workers.codex.effort': 'high'})[0]['workers']['codex']['effort'], 'high')
        result = self.call('workers.spawn', task=self.task['id'], provider='codex', prompt='Explicit', effort='high')
        self.assertTrue(result.ok, result.text)
        self.assertEqual(self.store.db.execute('SELECT effort FROM workers WHERE id=?', (result.data['worker'],)).fetchone()[0], 'high')
        self.assertTrue(self.swap(key='coordinators.codex.effort', value='low', topic='1:2').ok)
        self.store.put('coordinator_selection', {'provider': 'codex', 'effort': 'high', 'revision': 1})
        self.assertEqual(parent_state.effective_agent(self.store, 'coordinator', 'codex')['effort'], 'low')

    def test_expiry_pruning_and_unswap(self):
        with patch('coordinator.loadout.time.time', return_value=1000000):
            result = self.swap(until='1d')
        ident = result.data['id']
        with patch('coordinator.loadout.time.time', return_value=1000000 + 86401):
            self.assertEqual(loadout.effective(self.store, '1:2')[0]['workers']['codex']['effort'], 'high')
            self.assertFalse(self.call('loadout.unswap', id=ident).ok)
        with patch('coordinator.loadout.time.time', return_value=1000000 + 8 * 86400 + 1):
            loadout.effective(self.store, '1:2')
            self.assertEqual([row['id'] for row in self.store.get('loadout.swaps')], [ident])
            fresh = self.swap()
            self.assertEqual([row['id'] for row in self.store.get('loadout.swaps')], [fresh.data['id']])
        fresh = self.swap()
        self.assertTrue(self.call('loadout.unswap', id=fresh.data['id']).ok)
        self.assertEqual(loadout.effective(self.store, '1:2')[0]['workers']['codex']['effort'], 'high')

    def test_limits_and_owner_message(self):
        self.assertFalse(self.swap(until='31d').ok)
        self.assertTrue(self.swap(until='30d').ok)
        self.assertFalse(self.swap(key='workers.codex.model', value='a' * 81).ok)
        self.assertTrue(self.swap(key='workers.codex.model', value='a' * 80).ok)
        self.assertFalse(self.swap(key='route.claude', value='browser use').ok)
        self.assertFalse(self.swap(value='invalid').ok)
        self.assertFalse(self.call('loadout.swap', key='workers.codex.model', value='test', until='1d', reason='test').ok)
        forwarded = self.store.message_save('1:2', 'forwarded', 'test', telegram_message=43)['id']
        self.assertFalse(self.call('loadout.swap', key='workers.codex.model', value='test', until='1d', reason='test', message=forwarded).ok)

    def test_tools_prompt_and_spawn_defaults(self):
        names = {tool['name'] for tool in mcp.tools_list(self.store)}
        self.assertTrue({'loadout_swap', 'loadout_unswap', 'loadout_show'} <= names)
        new = self.call('workers.spawn', task=self.task['id'], provider='codex', prompt='New')
        self.assertTrue(new.ok, new.text)
        self.assertEqual(tuple(self.store.db.execute('SELECT model,effort FROM workers WHERE id=?', (new.data['worker'],)).fetchone()), ('gpt-6.2-sol', 'high'))
        self.assertTrue(self.swap(key='workers.claude.model', value='claude-sonnet-5-5').ok)
        self.assertIn('set workers.claude.model to "claude-sonnet-5-5"', chat_policy(self.store, 'coordinator'))

    def test_mcp_tools_list_matches_dispatch_and_hash(self):
        baseline = mcp.tools_list()
        baseline_hash = mcp.tools_hash()
        baseline_names = {tool['name'] for tool in baseline}
        self.assertIn('events_post', baseline_names)
        self.assertTrue({'loadout_show', 'loadout_swap', 'loadout_unswap'} <= baseline_names)
        self.assertEqual(mcp.dispatch(self.store, {'method': 'tools/list'})['tools'], baseline)
        self.assertEqual(mcp.tools_hash(self.store), baseline_hash)

    def test_mcp_scoping_and_tool_descriptions(self):
        tools = {tool['name']: tool for tool in mcp.tools_list(self.store)}
        for tool in tools.values():
            self.assertLessEqual(len(tool['description']), 300, tool['name'])
        def invoke(name, **arguments):
            request = {'method': 'tools/call', 'params': {'name': name, 'arguments': arguments}}
            return mcp.dispatch(self.store, request, topic='1:2')
        result = invoke('loadout_swap', key='workers.codex.model', value='tests', until='1d',
                        reason='Owner asked', message=self.owner, topic='1:3')
        self.assertTrue(result['isError'], result)
        result = invoke('loadout_swap', key='workers.codex.model', value='tests', until='1d',
                        reason='Owner asked', message=self.owner, topic='1:2')
        self.assertFalse(result['isError'], result)
        self.assertIn('set workers.codex.model to "tests"', chat_policy(self.store, 'coordinator'))
        self.assertNotIn('set workers.codex.model to "tests"', chat_policy(self.store, 'coordinator:1:3'))
        self.assertTrue(invoke('loadout_show', topic='1:3')['isError'])
        self.assertTrue(invoke('loadout_swap', key='workers.codex.model', value='test', until='1d',
                               reason='test')['isError'])
        self.assertTrue(control_api.call(self.store, 'effort.worker', {'provider': 'codex', 'effort': 'low'},
                                         source='mcp').state == 'refused')
        self.assertTrue(self.call('effort.worker', provider='codex', effort='low').ok)
        self.assertEqual(loadout.effective(self.store, '1:2')[0]['workers']['codex']['effort'], 'low')
        self.assertTrue(self.call('effort.worker', provider='codex').ok)
        self.assertEqual(loadout.effective(self.store, '1:2')[0]['workers']['codex']['effort'], 'high')

    def test_mcp_swap_and_unswap_stay_in_calling_chat(self):
        other_owner = self.store.message_save('1:3', 'owner', 'Use another temporary model')['id']
        def invoke(op, origin='1:3', **params):
            return control_api.call(self.store, op, params, topic=origin, source='mcp')
        params = dict(key='workers.codex.model', value='tests', until='1d', reason='Owner asked', message=other_owner)
        self.assertFalse(invoke('loadout.swap', **params).ok)
        self.assertFalse(invoke('loadout.swap', topic='1:2', **params).ok)
        own = invoke('loadout.swap', topic='1:3', **params)
        self.assertTrue(own.ok, own.text)
        self.assertFalse(invoke('loadout.unswap', origin='1:2', id=own.data['id']).ok)
        global_swap = self.swap(key='workers.codex.model', value='global')
        self.assertTrue(global_swap.ok, global_swap.text)
        self.assertFalse(invoke('loadout.unswap', id=global_swap.data['id']).ok)
        self.assertTrue(invoke('loadout.unswap', id=own.data['id']).ok)
        self.assertTrue(invoke('loadout.unswap', origin='1:2', id=global_swap.data['id']).ok)

    def test_mcp_show_defaults_to_calling_chat_and_refuses_other_chat(self):
        global_swap = self.swap(key='workers.codex.model', value='global')
        home = self.swap(key='workers.codex.model', value='home', topic='1:2')
        own = self.swap(key='workers.codex.model', value='own', topic='1:3')
        shown = control_api.call(self.store, 'loadout.show', {}, topic='1:3', source='mcp')
        self.assertTrue(shown.ok, shown.text)
        self.assertEqual({row['id'] for row in shown.data['swaps']}, {global_swap.data['id'], own.data['id']})
        shown = control_api.call(self.store, 'loadout.show', {}, topic='1:2', source='mcp')
        self.assertEqual({row['id'] for row in shown.data['swaps']}, {global_swap.data['id'], home.data['id']})
        for origin, target in (('1:2', '1:3'), ('1:3', '1:2')):
            self.assertFalse(control_api.call(self.store, 'loadout.show', {'topic': target}, topic=origin, source='mcp').ok)
        self.assertFalse(control_api.call(self.store, 'loadout.show', {}, source='mcp').ok)

    def test_swap_values_refuse_controls_and_line_separators(self):
        for char in [*(chr(n) for n in range(32)), *(chr(n) for n in range(127, 160)), '\u2028', '\u2029']:
            with self.subTest(codepoint=ord(char)):
                self.assertFalse(self.swap(key='workers.codex.model', value='tests' + char + 'now').ok)

    def test_prompt_quotes_values_and_keeps_reasons_only_in_show(self):
        reason = 'Only show this reason\u2028Keep it private to the tool result'
        route = self.call('loadout.swap', key='workers.codex.model', value='tests.carefully', until='1d', reason=reason, message=self.owner)
        self.assertTrue(route.ok, route.text)
        model = self.call('loadout.swap', key='coordinators.claude.model', value='claude-sonnet-5', until='1d', reason=reason, message=self.owner)
        self.assertTrue(model.ok, model.text)
        rendered = loadout.render(self.store, '1:2')
        self.assertIn(json.dumps('tests.carefully'), rendered)
        self.assertIn(json.dumps('claude-sonnet-5'), rendered)
        self.assertEqual(len(rendered.splitlines()), 2)
        self.assertNotIn('Only show this reason', rendered)
        self.assertEqual(self.call('loadout.show').data['swaps'][0]['reason'], reason)

    def test_defaults_preserve_existing_models_and_claude_effort(self):
        values = self.call('loadout.show').data['values']
        self.assertEqual(values, {
            'coordinators': {'claude': {'model': 'sonnet', 'effort': 'medium'},
                             'codex': {'model': 'gpt-6.2-sol', 'effort': 'medium'}},
            'workers': {'claude': {'model': 'opus', 'effort': 'medium'},
                        'codex': {'model': 'gpt-6.2-sol', 'effort': 'high'}}})
        self.assertEqual(parent_state.effective_agent(self.store, provider='claude')['model'], 'sonnet')
        self.assertEqual(parent_state.effective_agent(self.store, provider='claude', model='service-model')['model'], 'service-model')

    def test_read_paths_do_not_prune_and_next_write_prunes(self):
        old = self.swap().data
        old['until'] = time.time() - 8 * 86400
        self.store.put('loadout.swaps', [old])
        changes = self.store.db.total_changes
        for unused in range(3):
            self.assertEqual(loadout.active(self.store, '1:2'), [])
            loadout.effective(self.store, '1:2')
            self.assertEqual(loadout.render(self.store, '1:2'), '')
        self.assertEqual(self.store.db.total_changes, changes)
        self.assertEqual(self.store.get('loadout.swaps'), [old])
        fresh = self.swap()
        self.assertTrue(fresh.ok, fresh.text)
        self.assertEqual([row['id'] for row in self.store.get('loadout.swaps')], [fresh.data['id']])

    def test_template_cache_reloads_only_when_mtime_changes(self):
        template = Path(self.temp.name) / 'template' / 'defaults' / 'loadout.json'
        template.parent.mkdir(parents=True)
        source = (Path(loadout.__file__).parent / 'defaults' / 'loadout.json').read_text()
        template.write_text(source)
        read = Path.read_text
        reads = []
        def observed(path, *args, **kwargs):
            if path == template:
                reads.append(path)
            return read(path, *args, **kwargs)
        with patch('coordinator.loadout.__file__', str(template.parent.parent / 'loadout.py')), patch.object(Path, 'read_text', observed):
            first = loadout.effective(self.store)[0]
            first['workers']['claude']['model'] = 'changed-copy'
            self.assertEqual(loadout.effective(self.store)[0]['workers']['claude']['model'], 'opus')
            self.assertEqual(len(reads), 1)
            mtime = template.stat().st_mtime_ns
            changed = json.loads(source)
            changed['workers']['claude']['model'] = 'changed-template'
            template.write_text(json.dumps(changed))
            os.utime(template, ns=(mtime + 1000000000, mtime + 1000000000))
            self.assertEqual(loadout.effective(self.store)[0]['workers']['claude']['model'], 'changed-template')
            self.assertEqual(len(reads), 2)
