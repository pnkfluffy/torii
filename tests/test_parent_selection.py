import asyncio
import hashlib
import json
import logging
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch
import uuid

from coordinator import control_api, parent, parent_state
from coordinator.policy import REPLY_FIRST
from coordinator.providers import ProviderRunner
from coordinator.service import Service
from coordinator.store import Store
from tests.support import expected_codex_skills_pointer, isolate_shared_mcp_home, origin_codex_skills_pointer, read_jsonl, settle, stop_test_hosts, until
from tests.test_codex_parent import APP_SERVER
from tests.test_parent_turns import FAKE


ORIGIN_MAIN_DEFAULTS = {'controls': [{'data': {'parent_selection': {'model': 'test-astra', 'provider': 'codex'}},
               'ok': True,
               'params': {'model': 'test-astra', 'provider': 'codex'},
               'revision': 1,
               'state': 'done',
               'stored': {'model': 'test-astra', 'provider': 'codex'},
               'text': 'Parent chats: codex test-astra. Active turns and native background tasks settle '
                       'before a change. Worker settings stay unchanged.'},
              {'data': None,
               'ok': False,
               'params': {'provider': 'codex'},
               'revision': 1,
               'state': 'refused',
               'stored': {'model': 'test-astra', 'provider': 'codex'},
               'text': 'Supply both provider and model, or omit both for automatic parent selection.'},
              {'data': None,
               'ok': False,
               'params': {'model': 'test-astra'},
               'revision': 1,
               'state': 'refused',
               'stored': {'model': 'test-astra', 'provider': 'codex'},
               'text': 'Supply both provider and model, or omit both for automatic parent selection.'},
              {'data': None,
               'ok': False,
               'params': {'model': 'x', 'provider': 'other'},
               'revision': 1,
               'state': 'refused',
               'stored': {'model': 'test-astra', 'provider': 'codex'},
               'text': 'Send claude, codex or opencode for provider.'},
              {'data': {'parent_selection': {'model': 'claude-opus-5-5', 'provider': 'claude'}},
               'ok': True,
               'params': {'model': 'claude-opus-5-5', 'provider': 'claude'},
               'revision': 2,
               'state': 'done',
               'stored': {'model': 'claude-opus-5-5', 'provider': 'claude'},
               'text': 'Parent chats: claude claude-opus-5-5. Active turns and native background tasks '
                       'settle before a change. Worker settings stay unchanged.'},
              {'data': {'parent_selection': None},
               'ok': True,
               'params': {},
               'revision': 3,
               'state': 'done',
               'stored': None,
               'text': 'Parent chats: automatic provider and model selection. Active turns and native '
                       'background tasks settle before a change. Worker settings stay unchanged.'}],
 'claude': {'argv': ['@ROOT@/claude',
                     '--print',
                     '--verbose',
                     '--output-format',
                     'stream-json',
                     '--input-format',
                     'stream-json',
                     '--replay-user-messages',
                     '--dangerously-skip-permissions',
                     '--model',
                     'claude-opus-5-5',
                     '--effort',
                     'max',
                     '--settings',
                     '@SETTINGS@',
                     '--session-id',
                     '@SESSION@',
                     '--append-system-prompt',
                     'policy\nCurrent usage policy:\nowner policy',
                     '--system-prompt-snapshot',
                     'off',
                     '--mcp-config',
                     '@ROOT@/state/coordinator-mcp-@HOME_SCOPE@.json'],
            'content': {'cli': {'binary': 'fixture-cli'},
                        'effort': 'max',
                        'flags': ['--system-prompt-snapshot', 'off'],
                        'instructions': 'policy',
                        'mcp_configs': ['@MCP_HASH@'],
                        'model': 'claude-opus-5-5',
                        'settings': '@SETTINGS@',
                        'tools': 'fixture-tools'},
            'mcp': {'mcpServers': {'torii': {'args': ['-m',
                                                      'coordinator',
                                                      '--state-dir',
                                                      '@ROOT@/state',
                                                      'mcp', '--topic=home'],
                                             'command': '@PYTHON@',
                                             'env': {'PYTHONPATH': '@REPO@'}}}},
            'mcp_raw': '{"mcpServers": {"torii": {"command": "@PYTHON@", "args": ["-m", "coordinator", '
                       '"--state-dir", "@ROOT@/state", "mcp", "--topic=home"], "env": {"PYTHONPATH": "@REPO@"}}}}',
            'selection_changed': [False, False, True, False, True, False, True, True],
            'settings': {'ultracode': False,
                         'autoMemoryDirectory': '@ROOT@/state/coordinator-memory',
                         'claudeMdExcludes': '@EXCLUDES@'}},
 'codex': {'argv': ['@ROOT@/codex',
                    '-c',
                    'model_reasoning_effort="xhigh"',
                    '-c',
                    'mcp_servers.torii.command="@PYTHON@"',
                    '-c',
                    'mcp_servers.torii.args=["-m", "coordinator", "--state-dir", "@ROOT@/state", "mcp", "--topic=home"]',
                    '-c',
                    'mcp_servers.torii.env={"PYTHONPATH" = "@REPO@"}',
                    '-c',
                    'mcp_servers.torii.required=true',
                    '-c',
                    'mcp_servers.torii.startup_timeout_sec=30',
                    '-c',
                    'mcp_servers.torii.default_tools_approval_mode="approve"',
                    '-c',
                    'mcp_servers.torii.omit_tools_from=["deferred", "code_mode"]',
                    '-c',
                    'skills.include_instructions=false',
                    'app-server',
                    '--listen',
                    'stdio://'],
           'content': {'binary': '@ROOT@/codex',
                       'cli': {'binary': 'fixture-cli'},
                       'effort': 'xhigh',
                       'instructions': (
                           'policy\nCurrent usage policy:\nowner policy\n## Codex skills\n\nSkills: this launch has no list'
                           " of installed skills, so read a skill's SKILL.md before you use it. If CODEX_HOME is "
                           'unset, use ~/.codex for the paths below. A skill named <plugin>:<name> is normally at '
                           '$CODEX_HOME/plugins/cache/<source>/<plugin>/<version>/skills/<name>/SKILL.md; use local if'
                           ' present, otherwise the newest version (semantic versions, else lexical order). For '
                           'pstack, <source>/<plugin> is pstack-claude/pstack. Only enabled plugins supply skills; '
                           'their manifests can set other skill folders. Legacy plugins can also use '
                           '<version>/.codex-plugin/migrated-command-skills/. Before you start, list existing '
                           '.agents/skills/ and .codex/skills/ from your project root to your working folder, '
                           '~/.agents/skills/, $CODEX_HOME/skills/, $CODEX_HOME/skills/.system/, and '
                           '/etc/codex/skills/. Read the SKILL.md of each skill there that fits your task.'
                       ),
                       'model': 'test-codex',
                       'overrides': ['-c',
                                     'model_reasoning_effort="xhigh"',
                                     '-c',
                                     'mcp_servers.torii.command="@PYTHON@"',
                                     '-c',
                                     'mcp_servers.torii.args=["-m", "coordinator", "--state-dir", '
                                     '"@ROOT@/state", "mcp", "--topic=home"]',
                                     '-c',
                                     'mcp_servers.torii.env={"PYTHONPATH" = "@REPO@"}',
                                     '-c',
                                     'mcp_servers.torii.required=true',
                                     '-c',
                                     'mcp_servers.torii.startup_timeout_sec=30',
                                     '-c',
                                     'mcp_servers.torii.default_tools_approval_mode="approve"',
                                     '-c',
                                     'mcp_servers.torii.omit_tools_from=["deferred", "code_mode"]',
                                     '-c',
                                     'skills.include_instructions=false'],
                       'tools': 'fixture-tools'},
           'mcp': None,
           'mcp_raw': None,
           'selection_changed': [False, False, True, False, True, False, True, True]}}


class ParentSelectionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.addCleanup(self.temporary.cleanup)
        self.addCleanup(stop_test_hosts, self.root)
        isolate_shared_mcp_home(self, self.root)
        self.store = Store(self.root / 'state')
        self.addCleanup(self.store.close)
        claude, codex = self.root / 'claude', self.root / 'codex'
        claude.write_text(FAKE)
        codex.write_text(APP_SERVER.replace(
            "assert 'policy' in params['developerInstructions'] and params['model']=='test-codex'",
            "assert params['model'] in ('test-codex','test-astra')"))
        claude.chmod(0o700)
        codex.chmod(0o700)
        for profile in ('claude-home', 'codex-home'):
            (self.root / profile).mkdir()
        now = time.time()
        with self.store.db:
            self.store.put('coordinator_model', 'claude-opus-5-5')
            self.store.put('claude_parent_effort', 'max')
            self.store.put('codex_parent_effort', 'max')
            self.store.put('owner', 1)
            self.store.put('group', 1)
            self.store.put('accounts', {'c': {'config_dir': str(self.root / 'claude-home'), 'enabled': True}})
            self.store.put('account_status', {'c': {'identity': {'logged_in': True, 'email': 'claude@example.test'}, 'observed_at': now,
                           'usage': {'five_hour': {'utilization': 0},
                                     'seven_day': {'utilization': 0, 'resets_at': now + 3600}}}})
            self.store.put('codex_accounts', {'g': {'config_dir': str(self.root / 'codex-home'), 'enabled': True}})
            self.store.put('codex_account_status', {'g': {'identity': {'logged_in': True, 'email': 'codex@example.test'}}})
            self.store.put('codex_active_account', 'g')
            self.store.put('codex_model', 'test-codex')
            self.store.put('coordinator_home_topic', 'home')
            for topic in ('home', 'other'):
                self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES(?,?,?,?,?,1)',
                                      (topic, 1, 1 if topic == 'home' else 2, topic, str(self.root)))
        self.runner = ProviderRunner(self.store.directory, {'claude': str(claude), 'codex': str(codex)})
        self.service = Service(self.store, None, self.runner, self.root)
        self.parents = []

    async def asyncTearDown(self):
        for task in self.service.feed_tasks.values():
            task.cancel()
        await asyncio.gather(*self.service.feed_tasks.values(), return_exceptions=True)
        for session in self.parents:
            await session.stop()

    def select(self, provider, model):
        with self.store.db:
            result = control_api.call(self.store, 'model.parent', {'provider': provider, 'model': model})
        self.assertTrue(result.ok, result.text)

    async def test_t2_created_chat_first_host_uses_pick_without_switch(self):
        from tests.test_group_setup import GroupTelegram
        projects = self.root / 'projects'
        projects.mkdir()
        self.store.put('projects_root', str(projects))
        self.service.telegram = GroupTelegram()
        for provider, model, effort in (('claude', 'claude-opus-5-5', 'high'),
                                        ('codex', 'test-astra', 'low')):
            with self.subTest(provider=provider):
                self.select('codex' if provider == 'claude' else 'claude',
                            'default-model' if provider == 'claude' else 'claude-opus-5-5')
                queued = control_api.call(self.store, 'topic.create', {
                    'name': 'Picked ' + provider, 'provider': provider, 'model': model, 'effort': effort,
                    'message': self.store.message_save('home', 'owner', 'Create a picked chat.')['id']},
                    topic='home', source='mcp')
                self.assertTrue(queued.ok, queued.text)
                hosts = []
                self.store.db.set_trace_callback(hosts.append)
                try:
                    for unused in range(4):
                        await self.service.controls_once()
                    target = '1:' + str(self.service.telegram.thread)
                    session = await self.service.start_session(target)
                    self.assertIsNotNone(session)
                    self.parents.append(session)
                    host = self.store.get(session.key + '_host')
                    self.assertEqual({field: host[field] for field in ('provider', 'model', 'effort', 'picked')},
                                     dict(provider=provider, model=model, effort=effort, picked=True))
                    self.assertEqual(self.store.get(session.key + '_selection_revision'), 1)
                    self.assertIsNone(self.store.get(session.key + '_switch'))
                    self.assertFalse(session.switched)
                    await session.send(str(uuid.uuid4()), 'ready')
                    await until(session.idle)
                    self.assertEqual(session.model, model)
                    spec = json.loads((self.store.directory / 'hosts' / session.host_id / 'spec.json').read_text())
                    if provider == 'claude':
                        self.assertEqual(spec['argv'][spec['argv'].index('--model') + 1], model)
                        self.assertEqual(spec['argv'][spec['argv'].index('--effort') + 1], effort)
                    else:
                        self.assertIn('model_reasoning_effort=' + json.dumps(effort), spec['argv'])
                        request = next(row for row in reversed(read_jsonl(self.root / 'requests'))
                                       if row.get('method') == 'thread/start')
                        self.assertEqual(request['params']['model'], model)
                finally:
                    self.store.db.set_trace_callback(None)
                host_writes = [sql for sql in hosts if sql.startswith('INSERT OR REPLACE INTO settings') and
                               session.key + '_host\'' in sql]
                self.assertEqual(len(host_writes), 1, host_writes)
                self.assertFalse(any('This chat now runs on' in row[0] for row in self.store.db.execute(
                    'SELECT text FROM outbox WHERE topic=?', (target,))))
                await session.stop()

    def pick(self, topic, provider=None, model=None, effort=None):
        params = {'topic': topic, 'provider': provider, 'model': model, 'effort': effort}
        with self.store.db:
            result = control_api.call(self.store, 'model.parent', params)
        self.assertTrue(result.ok, result.text)
        return result.data['selection']

    async def test_t1_no_chat_pick_matches_origin_main_launch_and_selection_checks(self):
        """Compare default launch values with main's per-chat tool-server scope."""
        from coordinator.session import CoordinatorSession
        for provider in ('claude', 'codex'):
            for selected, other_provider in ((False, 'codex'), (True, 'codex'), (False, 'opencode'), (True, 'opencode')):
                with self.subTest(provider=provider, global_pick=selected, other=other_provider):
                    self.store.put('coordinator_session', None)
                    self.store.put('coordinator_provider', provider)
                    self.store.put('parent_selection', {'provider': provider, 'model': (
                        'test-codex' if provider == 'codex' else 'claude-opus-5-5')} if selected else None)
                    self.store.put('account_blocks', {'c': {'reason': 'quota', 'until': time.time() + 3600}}
                                   if provider == 'codex' else {})
                    if other_provider == 'opencode':
                        self.store.put('opencode_enabled', True)
                        self.store.put('opencode_accounts', {'other': {'model': 'other'}})
                        self.store.put('opencode_active_account', 'other')
                        self.pick('other', 'opencode')
                    else:
                        self.pick('other', 'codex', 'test-astra', 'low')
                    self.store.put('coordinator:other_host', {'id': 'other-live', 'provider': 'opencode',
                                   'account': 'other', 'model': 'other'} if other_provider == 'opencode' else None)
                    (self.store.directory / 'USAGE.md').write_text('owner policy')
                    with patch('coordinator.cli_identity.identity', return_value={'binary': 'fixture-cli'}), \
                            patch('coordinator.mcp.tools_hash', return_value='fixture-tools'):
                        session = await parent.start(self.store, self.runner, self.root, 'claude-opus-5-5',
                                                     'policy', topic='home')
                    self.parents.append(session)
                    if provider == 'claude':
                        self.assertEqual(await session.send(str(uuid.uuid4()), 'ready'), 'received')
                    await until(session.idle)
                    reference = ORIGIN_MAIN_DEFAULTS[provider]
                    def expand(value):
                        text = json.dumps(value)
                        for marker, replacement in (('@ROOT@', str(self.root)), ('@REPO@', str(Path(__file__).resolve().parents[1])),
                                                    ('@PYTHON@', sys.executable), ('@SESSION@', session.session_id),
                                                    ('@HOME_SCOPE@', hashlib.sha256(b'home').hexdigest()[:16])):
                            text = text.replace(marker, replacement)
                        return json.loads(text)
                    content = expand(reference['content'])
                    argv = expand(reference['argv'])
                    if provider == 'codex':
                        original_note = origin_codex_skills_pointer()
                        self.assertEqual(content['instructions'].count(original_note), 1)
                        content['instructions'] = content['instructions'].replace(original_note,
                            expected_codex_skills_pointer(Path.home(), self.root / 'codex-home'))
                    if provider == 'claude':
                        settings = expand(reference['settings'])
                        settings['claudeMdExcludes'] = [str(directory / name) for directory in Path.home().parents
                            for name in ('CLAUDE.md', 'CLAUDE.local.md', '.claude/CLAUDE.md')]
                        settings = json.dumps(settings)
                        content['settings'] = settings
                        argv = [settings if item == '@SETTINGS@' else item for item in argv]
                        expected_mcp = expand(reference['mcp'])
                        mcp_path = self.store.directory / ('coordinator-mcp-' + hashlib.sha256(b'home').hexdigest()[:16] + '.json')
                        expected_mcp_raw = expand(reference['mcp_raw'])
                        self.assertEqual(mcp_path.read_bytes(), expected_mcp_raw.encode())
                        self.assertEqual(json.loads(expected_mcp_raw), expected_mcp)
                        content['mcp_configs'] = [hashlib.sha256(expected_mcp_raw.encode()).hexdigest()]
                        with patch('coordinator.cli_identity.identity', return_value={'binary': 'fixture-cli'}), \
                                patch('coordinator.mcp.tools_hash', return_value='fixture-tools'):
                            inputs = CoordinatorSession._launch_inputs(self.store, session.model, 'policy', None,
                                self.runner.extension.fingerprint(), self.runner.binaries['claude'],
                                topic='home', key='coordinator')
                        self.assertEqual((inputs.model, inputs.effort, inputs.settings, inputs.prompt),
                                         (content['model'], content['effort'], content['settings'],
                                          'policy\nCurrent usage policy:\nowner policy'))
                    expected_fingerprint = hashlib.sha256(json.dumps(
                        content, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
                    host = self.store.get('coordinator_host')
                    self.assertEqual(host['fingerprint'], expected_fingerprint)
                    spec = json.loads((self.store.directory / 'hosts' / session.host_id / 'spec.json').read_text())
                    self.assertEqual(spec['argv'], argv)
                    if provider == 'codex':
                        request = next(row for row in reversed(read_jsonl(self.root / 'requests'))
                                       if row.get('method') == 'thread/start')
                        self.assertEqual(request['params']['developerInstructions'], content['instructions'])
                        self.assertEqual(request['params']['model'], content['model'])
                    await session.stop()
                    results = []
                    for selection, model, actual, available in (
                        (None, 'old', 'claude', True),
                        ({'provider': 'claude', 'model': 'same'}, 'same', 'claude', True),
                        ({'provider': 'claude', 'model': 'new'}, 'old', 'claude', True),
                        ({'provider': 'claude', 'model': 'new'}, 'old', 'codex', False),
                        ({'provider': 'claude', 'model': 'new'}, 'old', 'codex', True),
                        ({'provider': 'codex', 'model': 'same'}, 'same', 'codex', True),
                        ({'provider': 'codex', 'model': 'new'}, 'old', 'codex', True),
                        ({'provider': 'codex', 'model': 'new'}, 'old', 'claude', True)):
                        self.store.put('parent_selection', selection)
                        self.store.put('probe_host', {'provider': actual})
                        with patch('coordinator.parent_state.AccountBroker.select', return_value='c' if available else None):
                            results.append(parent_state.selection_changed(SimpleNamespace(store=self.store, key='probe', model=model)))
                    self.assertEqual(results, reference['selection_changed'])

    async def test_t1_default_service_cycle_ignores_another_chats_pick(self):
        await self.start()
        self.pick('other', 'codex', 'test-astra', 'low')
        with patch('coordinator.parent_state.selection_changed', side_effect=AssertionError('No pick to apply')):
            await self.service.feed_once()

    async def test_disabled_chat_with_exhausted_pick_has_no_pause_or_retry_effects(self):
        self.pick('other', 'codex', 'test-astra', 'low')
        self.store.put('codex_account_blocks', {'g': {'reason': 'quota', 'until': time.time() + 3600}})
        self.store.db.execute("UPDATE topics SET enabled=0 WHERE id='other'")
        self.assertIsNone(await self.service.start_session('other'))
        self.assertNotIn('other', self.service.sessions)
        self.assertNotIn('other', self.service.parent_retry)
        for key in ('coordinator:other_selection_retry_at', 'coordinator:other_selection_notice',
                    'coordinator_account_retry_at', 'coordinator_account_notice'):
            self.assertIsNone(self.store.get(key), key)
        self.assertEqual(list(self.store.db.execute('SELECT * FROM problems')), [])
        self.assertEqual(list(self.store.db.execute('SELECT * FROM outbox')), [])
        home = await self.start('home')
        self.assertEqual(home.provider, 'claude')

    async def test_disabled_chat_reattachment_ignores_its_cached_pick_pause(self):
        self.pick('other', 'codex', 'test-astra', 'low')
        reset = time.time() + 3600
        self.store.put('codex_account_blocks', {'g': {'reason': 'quota', 'until': reset}})
        self.store.db.execute("UPDATE topics SET enabled=0 WHERE id='other'")
        notice = {'text': 'This chat is paused.', 'topics': []}
        self.store.put('coordinator:other_host', {'id': 'disabled-fixture', 'provider': 'codex'})
        self.store.put('coordinator:other_selection_retry_at', reset)
        self.store.put('coordinator:other_selection_notice', notice)
        with patch.object(self.service, 'session_factory', AsyncMock(return_value=None)) as reattach:
            self.assertIsNone(await self.service.start_session('other', reattach_only=True))
        reattach.assert_awaited_once()
        self.assertNotIn('other', self.service.sessions)
        self.assertNotIn('other', self.service.parent_retry)
        self.assertEqual(self.store.get('coordinator:other_selection_notice'), notice)
        self.assertEqual(self.store.get('coordinator:other_selection_retry_at'), reset)
        self.assertIsNone(self.store.get('coordinator_account_retry_at'))
        self.assertIsNone(self.store.get('coordinator_account_notice'))
        self.assertEqual(list(self.store.db.execute('SELECT * FROM problems')), [])
        self.assertEqual(list(self.store.db.execute('SELECT * FROM outbox')), [])

    async def test_disabled_home_with_exhausted_pick_still_gets_its_own_pause(self):
        self.pick('home', 'codex', 'test-astra', 'low')
        reset = time.time() + 3600
        self.store.put('codex_account_blocks', {'g': {'reason': 'quota', 'until': reset}})
        self.store.db.execute("UPDATE topics SET enabled=0 WHERE id='home'")
        self.assertIsNone(await self.service.start_session('home'))
        self.assertNotIn('home', self.service.sessions)
        self.assertEqual(self.store.get('coordinator_selection_retry_at'), reset)
        notice = self.store.get('coordinator_selection_notice')
        self.assertEqual(notice['topics'], [])
        self.assertIn('out of usage', notice['text'])
        self.assertIsNone(self.store.get('coordinator_account_retry_at'))
        self.assertIsNone(self.store.get('coordinator_account_notice'))
        self.assertEqual([tuple(row) for row in self.store.db.execute('SELECT topic,area,code FROM problems')],
                         [('home', 'accounts', 'unavailable')])
        self.assertEqual(list(self.store.db.execute('SELECT topic,text FROM outbox')), [])
        self.store.db.execute("UPDATE topics SET enabled=1 WHERE id='home'")
        self.assertIsNone(await self.service.start_session('home'))
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM outbox').fetchone()[0], 1)
        self.assertEqual((await self.start('other')).provider, 'claude')

    async def test_claude_fingerprints_follow_only_own_pick_and_tool_config(self):
        from coordinator.session import CoordinatorSession
        self.pick('home', 'claude', 'claude-opus-5-5', 'low')
        self.pick('other', 'claude', 'claude-sonnet-5', 'high')

        def inputs(topic, config=None):
            return CoordinatorSession._launch_inputs(self.store, 'claude-opus-5-5', 'policy', config,
                self.runner.extension.fingerprint(), self.runner.binaries['claude'],
                topic=topic, key=self.service.session_key(topic))

        home, other = inputs('home'), inputs('other')
        self.assertEqual((home.model, home.effort, other.model, other.effort),
                         ('claude-opus-5-5', 'low', 'claude-sonnet-5', 'high'))
        self.assertNotEqual(home.fingerprint, other.fingerprint)
        for topic, value in (('home', home), ('other', other)):
            config = json.loads(value.configs[0].read_text())
            self.assertEqual(config['mcpServers']['torii']['args'][-1], '--topic=' + topic)
            session = await parent.start(self.store, self.runner, self.root, 'claude-opus-5-5', 'policy', topic=topic)
            self.parents.append(session)
            self.assertEqual(self.store.get(self.service.session_key(topic) + '_host')['fingerprint'], value.fingerprint)
            self.assertEqual(await session.send(str(uuid.uuid4()), 'ready'), 'received')
            await until(session.idle)
        self.pick('other', 'claude', 'claude-opus-5-5', 'max')
        changed_other = inputs('other')
        self.assertNotEqual(changed_other.fingerprint, other.fingerprint)
        self.assertEqual(inputs('home').fingerprint, home.fingerprint)
        self.pick('home', 'claude', 'claude-sonnet-5', 'medium')
        changed_home = inputs('home')
        self.assertNotEqual(changed_home.fingerprint, home.fingerprint)
        config = changed_home.configs[0]
        data = json.loads(config.read_text())
        data['mcpServers']['torii']['args'].append('--fixture-tool-config')
        config.write_text(json.dumps(data))
        self.assertNotEqual(inputs('home', config).fingerprint, changed_home.fingerprint)
        self.assertEqual(inputs('other').fingerprint, changed_other.fingerprint)

    async def test_codex_fingerprints_and_overrides_follow_only_own_pick_and_tool_scope(self):
        from coordinator.codex_session import CodexCoordinatorSession
        self.pick('home', 'codex', 'test-codex', 'low')
        self.pick('other', 'codex', 'test-astra', 'high')

        async def launch(topic):
            session = await self.start(topic)
            fingerprint = self.store.get(self.service.session_key(topic) + '_host')['fingerprint']
            values = CodexCoordinatorSession.overrides(self.store, topic=topic, key=self.service.session_key(topic))
            self.assertIn('mcp_servers.torii.args=' + json.dumps(
                ['-m', 'coordinator', '--state-dir', str(self.store.directory), 'mcp', '--topic=' + topic]), values)
            await session.stop()
            return fingerprint, values

        home, other = await launch('home'), await launch('other')
        self.assertNotEqual(home[0], other[0])
        self.assertIn('model_reasoning_effort="low"', home[1])
        self.assertIn('model_reasoning_effort="high"', other[1])
        self.pick('other', 'codex', 'test-codex', 'max')
        changed_other = await launch('other')
        self.assertNotEqual(changed_other[0], other[0])
        self.assertNotEqual(changed_other[1], other[1])
        self.assertEqual(await launch('home'), home)
        self.pick('home', 'codex', 'test-astra', 'medium')
        changed_home = await launch('home')
        self.assertNotEqual(changed_home[0], home[0])
        self.assertNotEqual(changed_home[1], home[1])
        original = CodexCoordinatorSession.overrides
        with patch.object(CodexCoordinatorSession, 'overrides', side_effect=lambda store, topic=None, key='coordinator':
                          original(store, topic=topic + '-tools' if topic == 'home' else topic, key=key)):
            session = await self.start('home')
            self.assertNotEqual(self.store.get('coordinator_host')['fingerprint'], changed_home[0])
            await session.stop()
        self.assertEqual(await launch('other'), changed_other)

    async def test_f1_default_host_records_keep_origin_main_fields(self):
        for provider in ('claude', 'codex'):
            for selected in (False, True):
                with self.subTest(provider=provider, selected=selected):
                    self.store.put('parent_selection', {'provider': provider, 'model': (
                        'test-codex' if provider == 'codex' else 'claude-opus-5-5')} if selected else None)
                    self.store.put('account_blocks', {'c': {'reason': 'quota', 'until': time.time() + 3600}}
                                   if provider == 'codex' else {})
                    session = await self.start()
                    self.assertEqual(set(self.store.get('coordinator_host')),
                                     {'id', 'account', 'model', 'provider', 'fingerprint'})
                    await session.stop()

    async def test_b1_chat_pick_pause_keeps_other_chats_at_normal_callback_pace(self):
        for index, topic in enumerate(('third', 'fourth'), 3):
            self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES(?,?,?,?,?,1)',
                                  (topic, 1, index, topic, str(self.root)))
        home, third = await self.start('home'), await self.start('third')
        self.assertEqual((home.provider, third.provider), ('claude', 'claude'))
        self.pick('other', 'codex', 'test-astra', 'low')
        reset = time.time() + 3600
        self.store.put('codex_account_blocks', {'g': {'reason': 'quota', 'until': reset}})
        waiting = self.store.message_save('other', 'owner', 'wait for my picked agent')
        await self.service.feed_once()
        with self.subTest(scope='pause state'):
            self.assertIsNone(self.store.get('coordinator_account_retry_at'))
            self.assertIsNone(self.store.get('coordinator_account_notice'))
            self.assertEqual(self.store.get('coordinator:other_selection_retry_at'), reset)
        with self.subTest(scope='home notices'):
            self.assertEqual(self.store.db.execute("SELECT count(*) FROM outbox WHERE topic='home'").fetchone()[0], 1)
            self.assertEqual(list(self.store.db.execute("SELECT text FROM outbox WHERE topic='other'")), [])
        with self.subTest(scope='problems'):
            self.assertEqual([row[0] for row in self.store.db.execute(
                "SELECT topic FROM problems WHERE area='accounts' AND code='unavailable'")], ['other'])
        feed = asyncio.create_task(self.service.feed())
        try:
            await asyncio.sleep(.2)
            for topic in ('home', 'third'):
                callback = self.store.message_save(topic, 'callback', 'worker finished in ' + topic)
                with self.subTest(callback=topic):
                    await until(lambda: self.store.db.execute('SELECT delivered FROM messages WHERE id=?',
                        (callback['id'],)).fetchone()[0] != 'pending', timeout=3)
                await asyncio.sleep(.2)
        finally:
            feed.cancel()
            await asyncio.gather(feed, return_exceptions=True)
        notice = self.store.get('coordinator:other_selection_notice')
        await self.start('fourth')
        for _ in range(2):
            await self.service.feed_once()
        self.assertEqual(self.store.get('coordinator:other_selection_notice'), notice)
        self.assertEqual(self.store.get('coordinator:other_selection_retry_at'), reset)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM outbox WHERE topic='other'").fetchone()[0], 0)
        self.assertEqual(self.store.db.execute(
            "SELECT count(*) FROM problems WHERE area='accounts' AND code='unavailable'").fetchone()[0], 1)
        self.assertEqual(self.store.db.execute('SELECT delivered FROM messages WHERE id=?',
                                              (waiting['id'],)).fetchone()[0], 'pending')
        self.assertIsNone(self.store.get('coordinator:other_host'))

    async def test_f4_identical_chat_pick_keeps_revision_and_running_host(self):
        for provider, model in (('claude', 'claude-opus-5-5'), ('codex', 'test-codex')):
            with self.subTest(provider=provider):
                self.pick('home', provider, model, 'low')
                session = await self.start()
                before = self.store.get('coordinator_selection')
                with self.store.db:
                    result = control_api.call(self.store, 'model.parent', {
                        'topic': 'home', 'provider': provider, 'model': model, 'effort': 'low'})
                self.assertTrue(result.ok, result.text)
                self.assertIn('already set', result.text)
                self.assertEqual(self.store.get('coordinator_selection'), before)
                await self.service.feed_once()
                self.assertFalse(session.rejected)
                self.assertIs(self.service.sessions['home'], session)
                await session.stop()

    async def test_f2_status_names_running_agent_during_fallback_and_pending_pick(self):
        from coordinator.controls import _status_card
        self.select('claude', 'claude-sonnet-5')
        self.limit_claude(time.time() + 3600)
        fallback = await self.start()
        self.assertIn('Chat agent: ChatGPT test-codex · xhigh thinking · default', _status_card(self.store, 'home'))
        await fallback.stop()
        self.pick('home', 'claude', 'claude-sonnet-5', 'low')
        picked = await self.start()
        self.assertIn('Chat agent: ChatGPT test-codex · xhigh thinking · picked for this chat',
                      _status_card(self.store, 'home'))
        self.pick('home', 'codex', 'test-astra', 'high')
        line = next(line for line in _status_card(self.store, 'home').splitlines() if line.startswith('Chat agent:'))
        self.assertTrue(line.startswith('Chat agent: ChatGPT test-codex · xhigh thinking'), line)
        self.assertIn('waiting to apply', line)
        self.assertIn('ChatGPT test-astra · high thinking · picked for this chat', line)
        self.assertFalse(picked.closed)

    async def test_f5_switch_notes_name_own_pick_and_return_to_default(self):
        before = await self.start()
        for params, reason in ((('codex', 'test-astra', 'low'), "this chat's own pick"),
                               ((None, None, None), 'back on the default')):
            self.pick('home', *params)
            await self.service.feed_once()
            await until(lambda: before.closed)
            before = await self.start()
            note = self.store.db.execute('SELECT text FROM outbox ORDER BY id DESC LIMIT 1').fetchone()[0]
            self.assertIn(reason, note)

    async def test_f5_pending_clear_keeps_default_note_after_reattachment(self):
        self.pick('home', 'codex', 'test-codex', 'low')
        before = await self.start()
        self.pick('home')
        await before.detach()
        self.parents.remove(before)
        self.service.sessions.pop('home')
        attached = await self.start()
        self.assertEqual(attached.host_id, before.host_id)
        self.assertTrue(self.store.get('coordinator_selection_cleared'))
        await self.service.feed_once()
        await until(lambda: attached.closed)
        await self.start()
        note = self.store.db.execute('SELECT text FROM outbox ORDER BY id DESC LIMIT 1').fetchone()[0]
        self.assertIn('back on the default', note)

    async def test_t2_chat_pick_wins_only_in_that_chat(self):
        for global_pick in (False, True):
            with self.subTest(global_pick=global_pick):
                if global_pick:
                    self.select('claude', 'claude-opus-5-5')
                self.pick('other', 'codex', 'test-astra', 'low')
                picked, default = await self.start('other'), await self.start('home')
                self.assertEqual((picked.provider, picked.model), ('codex', 'test-astra'))
                self.assertEqual((default.provider, default.model), ('claude', 'claude-opus-5-5'))
                self.assertIn('model_reasoning_effort="low"', read_jsonl(self.root / 'launches')[-1])
                self.assertEqual(self.store.get('coordinator:other_selection')['effort'], 'low')
                self.assertIsNone(self.store.get('coordinator_selection'))
                await picked.stop()
                await default.stop()

    async def test_t2_provider_only_pick_uses_that_providers_defaults(self):
        self.select('claude', 'claude-sonnet-5')
        self.store.put('codex_parent_effort', 'high')
        self.pick('home', 'codex')
        session = await self.start()
        self.assertEqual((session.provider, session.model), ('codex', 'test-codex'))
        self.assertIn('model_reasoning_effort="high"', read_jsonl(self.root / 'launches')[-1])

    async def test_t2_provider_only_claude_pick_keeps_service_default_without_relaunching(self):
        self.store.put('coordinator_model', None)
        self.service.coordinator_model = 'claude-sonnet-5'
        self.pick('home', 'claude')
        session = await self.start()
        self.assertEqual(session.model, 'claude-sonnet-5')
        self.assertFalse(parent_state.selection_changed(session))
        await self.service.feed_once()
        self.assertFalse(session.rejected)

    async def test_t3_model_and_thinking_changes_wait_and_resume_same_native_session(self):
        for provider in ('claude', 'codex'):
            with self.subTest(provider=provider):
                model = 'test-codex' if provider == 'codex' else 'claude-opus-5-5'
                self.pick('home', provider, model, 'low')
                before = await self.start()
                native = before.session_id
                next_model, effort = ('test-astra' if provider == 'codex' else 'claude-sonnet-5'), 'high'
                fingerprint = self.store.get('coordinator_host')['fingerprint']
                self.assertEqual(await before.send(str(uuid.uuid4()), 'hold' if provider == 'codex' else 'slow'), 'received')
                await until(lambda: before.busy)
                self.pick('home', provider, next_model, effort)
                await self.service.feed_once()
                self.assertFalse(before.rejected)
                if provider == 'codex':
                    self.assertEqual(await before.send(str(uuid.uuid4()), 'done'), 'received')
                else:
                    (self.root / 'slow.go').touch()
                with patch('coordinator.host.HostClient.stop', AsyncMock(side_effect=AssertionError('No signal stop'))):
                    await until(lambda: before.idle() or before.closed)
                    await self.service.feed_once()
                    await until(lambda: before.closed)
                after = await self.start()
                self.assertEqual(after.session_id, native)
                self.assertEqual(after.model, next_model)
                self.assertNotEqual(self.store.get('coordinator_host')['fingerprint'], fingerprint)
                spec = json.loads((self.store.directory / 'hosts' / after.host_id / 'spec.json').read_text())
                if provider == 'claude':
                    self.assertIn('--resume', spec['argv'])
                    self.assertEqual(spec['argv'][spec['argv'].index('--effort') + 1], effort)
                else:
                    self.assertIn('model_reasoning_effort=' + json.dumps('xhigh' if effort == 'max' else effort), spec['argv'])
                    self.assertEqual(next(row for row in reversed(read_jsonl(self.root / 'requests'))
                        if row.get('method') == 'thread/resume')['params']['threadId'], native)
                self.pick('home', provider, next_model, 'max')
                await self.service.feed_once()
                self.assertTrue(after.rejected)
                await until(lambda: after.closed)
                restored = await self.start()
                self.assertEqual(restored.session_id, native)
                spec = json.loads((self.store.directory / 'hosts' / restored.host_id / 'spec.json').read_text())
                if provider == 'codex':
                    self.assertIn('model_reasoning_effort="xhigh"', spec['argv'])
                else:
                    self.assertEqual(spec['argv'][spec['argv'].index('--effort') + 1], 'max')
                await restored.stop()

    async def test_t3_background_work_delays_a_chat_pick_change_without_global_selection(self):
        self.pick('home', 'claude', 'claude-opus-5-5', 'low')
        before = await self.start()
        self.assertEqual(await before.send(str(uuid.uuid4()), 'bg-work'), 'received')
        await until(lambda: before.protocol.background and not before.busy)
        self.pick('home', 'claude', 'claude-opus-5-5', 'high')
        await self.service.feed_once()
        self.assertFalse(before.rejected)
        (self.root / 'bg-finish').touch()
        await until(lambda: before.closed)
        after = await self.start()
        self.assertEqual(after.session_id, before.session_id)
        self.assertTrue((self.root / 'auto.finished').exists())

    async def test_t3_chat_revision_relaunches_only_its_idle_host(self):
        self.pick('other', 'codex', 'test-codex', 'low')
        before, default = await self.start('other'), await self.start('home')
        first = self.store.get('coordinator:other_selection')['revision']
        self.pick('other', 'codex', 'test-codex', 'high')
        self.assertGreater(self.store.get('coordinator:other_selection')['revision'], first)
        self.assertTrue(parent_state.selection_changed(before))
        await self.service.feed_once()
        self.assertTrue(before.rejected)
        self.assertFalse(default.rejected)
        await until(lambda: before.closed)
        after = await self.start('other')
        self.assertEqual(after.session_id, before.session_id)
        self.assertEqual(self.store.get('coordinator:other_selection_revision'),
                         self.store.get('coordinator:other_selection')['revision'])

    async def test_t4_chat_provider_switches_are_fresh_with_continuity_and_telegram_notes(self):
        self.pick('home', 'claude', 'claude-opus-5-5')
        before = await self.start()
        message = self.store.message_save('home', 'owner', 'remember this chat')
        self.store.message_delivered(message['id'], 'received')
        for provider, model, label in (('codex', 'test-astra', 'ChatGPT'), ('claude', 'claude-opus-5-5', 'Claude')):
            self.pick('home', provider, model)
            await self.service.feed_once()
            self.assertTrue(before.rejected)
            await until(lambda: before.closed)
            after = await self.start()
            self.assertTrue(after.switched)
            self.assertNotEqual(after.session_id, before.session_id)
            notice = self.store.db.execute("SELECT text FROM messages WHERE kind='restarted' ORDER BY id DESC LIMIT 1").fetchone()[0]
            self.assertIn('Channel continuity: Open jobs:', notice)
            self.assertIn('remember this chat', notice)
            self.assertIn('moved from', notice)
            report = self.store.db.execute('SELECT text FROM outbox ORDER BY id DESC LIMIT 1').fetchone()[0]
            self.assertIn('This chat now runs on ' + label, report)
            self.assertIn('owner selected', report)
            before = after

    async def test_t5_picked_chatgpt_waits_even_with_available_claude_and_other_chat_runs(self):
        self.pick('other', 'codex', 'test-astra', 'low')
        reset = time.time() + 3600
        self.store.put('codex_account_blocks', {'g': {'reason': 'quota', 'until': reset}})
        self.assertIsNone(await self.service.start_session('other'))
        self.assertIsNone(self.store.get('coordinator:other_host'))
        self.assertFalse(self.service.parent_available('other'))
        default = await self.start('home')
        self.assertEqual(default.provider, 'claude')
        self.assertTrue(self.service.parent_available('home'))
        notice = self.store.db.execute("SELECT text FROM outbox WHERE topic='home' ORDER BY id DESC LIMIT 1").fetchone()[0]
        self.assertIn('ChatGPT', notice)
        self.assertEqual(list(self.store.db.execute("SELECT text FROM outbox WHERE topic='other'")), [])

    async def test_t5_picked_claude_falls_back_silently_with_default_codex_effort(self):
        self.select('codex', 'test-astra')
        self.pick('other', 'claude', 'claude-sonnet-5', 'low')
        self.store.put('coordinator:other_provider', 'claude')
        self.store.put('codex_parent_effort', 'high')
        self.limit_claude(time.time() + 3600)
        fallback = await self.start('other')
        self.assertEqual((fallback.provider, fallback.model), ('codex', 'test-codex'))
        self.assertIn('model_reasoning_effort="high"', read_jsonl(self.root / 'launches')[-1])
        self.assertFalse(parent_state.selection_changed(fallback))
        self.assertEqual(list(self.store.db.execute("SELECT text FROM outbox WHERE topic='other'")), [])
        self.store.put('account_status', {alias: {**snapshot, 'usage': {'five_hour': {'utilization': 0}}}
                                        for alias, snapshot in self.store.get('account_status').items()})
        await self.service.feed_once()
        await until(lambda: fallback.closed)
        restored = await self.start('other')
        self.assertEqual((restored.provider, restored.model), ('claude', 'claude-sonnet-5'))
        self.assertEqual(restored.switch['reason'], 'Claude can take work again')

    async def test_t6_global_control_matches_origin_main_replies_refusals_and_values(self):
        for reference in ORIGIN_MAIN_DEFAULTS['controls']:
            with self.store.db:
                result = control_api.call(self.store, 'model.parent', reference['params'], topic='home')
            observed = {'params': reference['params'], 'ok': result.ok, 'text': result.text,
                        'state': result.state, 'data': result.data, 'stored': self.store.get('parent_selection'),
                        'revision': self.store.get('parent_selection_revision', 0)}
            self.assertEqual(observed, reference)

    async def check_clearing_chat_pick(self, global_pick):
        self.store.put('parent_selection', {'provider': 'claude', 'model': 'claude-opus-5-5'} if global_pick else None)
        self.pick('home', 'claude', 'claude-sonnet-5', 'low')
        before = await self.start()
        self.assertEqual(await before.send(str(uuid.uuid4()), 'slow'), 'received')
        await until(lambda: before.busy)
        self.pick('home')
        self.assertIsNone(self.store.get('coordinator_selection'))
        await self.service.feed_once()
        self.assertFalse(before.rejected)
        (self.root / 'slow.go').touch()
        await until(lambda: before.closed)
        after = await self.start()
        self.assertEqual(after.session_id, before.session_id)
        self.assertEqual(after.model, 'claude-opus-5-5')
        self.assertNotIn('effort', self.store.get('coordinator_host'))
        spec = json.loads((self.store.directory / 'hosts' / after.host_id / 'spec.json').read_text())
        self.assertEqual(spec['argv'][spec['argv'].index('--effort') + 1], 'max')
        self.assertFalse(parent_state.selection_changed(after))
        await after.stop()

    async def test_t7_clearing_chat_pick_waits_for_turn_then_resumes_automatic_default(self):
        await self.check_clearing_chat_pick(False)

    async def test_t7_clearing_chat_pick_waits_for_turn_then_resumes_global_default(self):
        await self.check_clearing_chat_pick(True)

    async def start(self, topic=None):
        topic = topic or self.service.home_topic()
        session = await self.service.start_session(topic)
        self.assertIsNotNone(session)
        self.parents.append(session)
        if session.provider == 'claude':
            self.assertEqual(await session.send(str(uuid.uuid4()), 'ready'), 'received')
        await until(lambda: session.idle() or session.closed)
        return session

    def prepare_selection_scenario(self):
        with self.store.db:
            self.store.put('accounts', {'personal': next(iter(self.store.get('accounts').values()))})
            self.store.put('account_status', {'personal': {
                **next(iter(self.store.get('account_status').values())),
                'identity': {'logged_in': True, 'email': 'personal@example.test'}}})
            self.store.put('codex_accounts', {'team': next(iter(self.store.get('codex_accounts').values()))})
            self.store.put('codex_account_status', {'team': {
                'identity': {'logged_in': True, 'email': 'team@example.test'}}})
            self.store.put('codex_active_account', 'team')
            self.store.put('coordinator_home_topic', 'Atlas')
            self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES(?,?,?,?,?,1)',
                                  ('Atlas', 1, 3, 'Atlas', str(self.root)))

    def limit_claude(self, reset):
        status = self.store.get('account_status')
        for snapshot in status.values():
            snapshot['usage']['five_hour'] = {'utilization': 98, 'resets_at': reset}
        with self.store.db:
            self.store.put('account_status', status)

    def prepare_owner_pick_scenario(self):
        self.prepare_selection_scenario()
        profile = self.root / 'work-claude'
        profile.mkdir()
        now = time.time()
        with self.store.db:
            self.store.put('accounts', {**self.store.get('accounts'), 'work': {
                'config_dir': str(profile), 'enabled': True}})
            self.store.put('account_status', {**self.store.get('account_status'), 'work': {
                'identity': {'logged_in': True, 'email': 'work@example.test'}, 'observed_at': now,
                'usage': {'five_hour': {'utilization': 0},
                          'seven_day': {'utilization': 0, 'resets_at': now + 7200}}}})
            result = control_api.call(self.store, 'account.use', {'alias': 'work'}, topic='Atlas')
        self.assertTrue(result.ok, result.text)
        self.assertEqual(self.service.accounts.select(), 'work')

    async def feed_owner(self, session, text, telegram_message):
        completed = asyncio.Event()
        session.on_turn = lambda reply: completed.set()
        row = self.store.message_save('Atlas', 'owner', text, telegram_message=telegram_message)
        await self.service.feed_once()
        await settle(asyncio.gather(*self.service.feed_tasks.values()))
        await settle(completed.wait())
        await until(lambda: session.idle() and self.store.db.execute(
            'SELECT receipt FROM messages WHERE id=?', (row['id'],)).fetchone()[0] == 'replayed',
                    'the owner turn to finish with a replayed receipt')
        self.assertEqual(tuple(self.store.db.execute('SELECT delivered,receipt FROM messages WHERE id=?',
                                                   (row['id'],)).fetchone()), ('received', 'replayed'))
        return row

    async def assert_documented_overrides_feed_is_quiet(self, accounts):
        self.prepare_selection_scenario()
        override = self.root / 'local-overrides.json'
        override.write_text(json.dumps({'shared_mcp_servers': []}))
        listed, status = self.store.get('accounts'), self.store.get('account_status')
        for index in range(1, accounts):
            alias = 'extra%d' % index
            profile = self.root / alias
            profile.mkdir()
            listed[alias] = {'config_dir': str(profile), 'enabled': True}
            status[alias] = json.loads(json.dumps(status['personal']))
            status[alias]['identity']['email'] = alias + '@example.test'
        with self.store.db:
            self.store.put('accounts', listed)
            self.store.put('account_status', status)
        self.limit_claude(time.time() + 7200)
        self.select('claude', 'claude-opus-5-5')
        with patch('coordinator.accounts.LOCAL_OVERRIDES_PATH', override), \
                patch.object(logging.getLogger('coordinator.accounts'), 'warning') as warning:
            session = await self.start()
            self.assertEqual(session.provider, 'codex')
            with patch.object(self.service, 'feed_once', wraps=self.service.feed_once) as ticks:
                feed = asyncio.create_task(self.service.feed())
                try:
                    await until(lambda: ticks.call_count >= 20)
                finally:
                    feed.cancel()
                    await asyncio.gather(feed, return_exceptions=True)
            self.assertIs(self.service.sessions.get('Atlas'), session)
            self.assertFalse(session.rejected)
            warning.assert_not_called()

    async def test_documented_overrides_fallback_feed_logs_no_warning_with_one_account(self):
        await self.assert_documented_overrides_feed_is_quiet(1)

    async def test_documented_overrides_fallback_feed_logs_no_warning_with_three_accounts(self):
        await self.assert_documented_overrides_feed_is_quiet(3)

    async def test_return_notice_names_claude_recovery_when_selection_is_unchanged(self):
        self.prepare_selection_scenario()
        self.select('claude', 'claude-opus-5-5')
        self.limit_claude(time.time() + 7200)
        before = [await self.start(topic) for topic in ('Atlas', 'other')]
        self.assertTrue(all(session.provider == 'codex' for session in before))
        self.select('claude', 'claude-opus-5-5')
        status = self.store.get('account_status')
        status['personal']['usage']['five_hour']['utilization'] = 0
        with self.store.db:
            self.store.put('account_status', status)
        with patch('coordinator.host.HostClient.stop', AsyncMock(side_effect=AssertionError('No signal stop'))):
            await self.service.feed_once()
            await settle(asyncio.gather(*(session.wait_closed() for session in before)))
        after = [await self.start(topic) for topic in ('Atlas', 'other')]
        self.assertTrue(all(session.provider == 'claude' for session in after))
        for topic in ('Atlas', 'other'):
            note = self.store.db.execute("SELECT text FROM outbox WHERE topic=? AND text LIKE "
                                         "'This chat now runs on%' ORDER BY id DESC LIMIT 1", (topic,)).fetchone()
            self.assertIsNone(note)

    async def wait_for_parent(self, session, predicate, description):
        def ready():
            for task in [*self.service.feed_tasks.values(), *session.tasks, session._monitor_task]:
                if task is not None and task.done() and not task.cancelled():
                    task.result()
            self.assertIsNone(session._failure, session._failure)
            return predicate()

        try:
            await until(ready, description)
        except Exception as error:
            state = {'provider': session.provider, 'closed': session.closed, 'busy': session.busy,
                     'host': self.store.get(session.key + '_host'),
                     'previous_provider': self.store.get(session.key + '_provider'),
                     'selected': parent_state.effective_agent(self.store, session.key),
                     'applied_revision': self.store.get(session.key + '_selection_revision'),
                     'consumed_sequence': self.store.get(session.key + '_host_seq'),
                     'receipts': [tuple(row) for row in self.store.db.execute(
                         'SELECT id,delivered,receipt FROM messages ORDER BY id')]}
            raise AssertionError(description + ': ' + repr(state)) from error

    async def wait_for_receipt(self, session, row, *, feed=False):
        feeder = asyncio.create_task(self.service.feed()) if feed else None

        def saved():
            return tuple(self.store.db.execute('SELECT delivered,receipt FROM messages WHERE id=?',
                                               (row['id'],)).fetchone())

        def replayed():
            if feeder is not None and feeder.done():
                feeder.result()
            return saved() == ('received', 'replayed')

        try:
            await self.wait_for_parent(session, replayed,
                                       'message %d to have a persisted replayed receipt' % row['id'])
            self.assertEqual(saved(), ('received', 'replayed'))
        finally:
            if feeder is not None:
                feeder.cancel()
                try:
                    await feeder
                except asyncio.CancelledError:
                    pass

    async def wait_for_journal(self, session, expected):
        def consumed():
            for record in read_jsonl(session.process.directory / 'events.jsonl'):
                event = json.loads(record['data'])
                if all(event.get(key) == value for key, value in expected.items()):
                    if self.store.get(session.key + '_host_seq', 0) >= record['seq']:
                        return True
            return False

        await self.wait_for_parent(session, consumed, 'the consumed journal event ' + repr(expected))

    async def test_provider_change_notice_names_owner_selection(self):
        self.select('codex', 'test-codex')
        before = await self.start()
        retiring, release = asyncio.Event(), asyncio.Event()
        retire = parent_state.retire

        async def gated_retire(session):
            retiring.set()
            await settle(release.wait())
            await retire(session)

        self.select('claude', 'claude-opus-5-5')
        with patch('coordinator.host.HostClient.stop', AsyncMock(side_effect=AssertionError('No signal stop'))), \
                patch('coordinator.parent_state.retire', side_effect=gated_retire):
            try:
                await self.service.feed_once()
                await self.wait_for_parent(before, retiring.is_set, 'the previous provider to reach retirement')
                state = {'provider': before.provider, 'previous': self.store.get(before.key + '_provider'),
                         'selected': parent_state.effective_agent(self.store, before.key),
                         'applied_revision': self.store.get(before.key + '_selection_revision')}
                self.assertEqual((state['provider'], state['previous'], state['applied_revision'],
                                  state['selected']['revision'], state['selected']['provider']),
                                 ('codex', 'codex', 1, 2, 'claude'), state)
                self.assertTrue(before.rejected, state)
                self.assertFalse(before.closed, state)
                self.assertIsNone(await self.service.start_session(), state)
            finally:
                release.set()
            await settle(before.wait_closed())
        self.assertIsNone(self.store.get(before.key + '_host'), state)
        self.assertEqual(self.store.get(before.key + '_provider'), 'codex', state)
        self.assertEqual(self.store.get(before.key + '_selection_revision'), 1, state)
        after = await self.start()
        self.assertEqual(after.provider, 'claude')
        self.assertEqual(self.store.get(after.key + '_selection_revision'), 2, state)
        note = self.store.db.execute("SELECT text FROM outbox WHERE topic='home' AND text LIKE "
                                     "'This chat now runs on%' ORDER BY id DESC LIMIT 1").fetchone()
        self.assertIsNotNone(note, state)
        self.assertEqual(note['text'], 'This chat now runs on Claude (the owner selected claude parent chats).', state)

    async def test_owner_provider_change_during_fallback_keeps_owner_reason(self):
        self.prepare_selection_scenario()
        self.select('claude', 'claude-opus-5-5')
        self.limit_claude(time.time() + 7200)
        before = await self.start()
        self.assertEqual(before.provider, 'codex')
        self.select('codex', 'test-codex')
        await self.service.feed_once()
        self.assertIs(self.service.sessions.get('Atlas'), before)
        self.assertFalse(before.rejected)
        status = self.store.get('account_status')
        status['personal']['usage']['five_hour']['utilization'] = 0
        with self.store.db:
            self.store.put('account_status', status)
        self.select('claude', 'claude-opus-5-5')
        with patch('coordinator.host.HostClient.stop', AsyncMock(side_effect=AssertionError('No signal stop'))):
            await self.service.feed_once()
            await settle(before.wait_closed())
        after = await self.start()
        self.assertEqual(after.provider, 'claude')
        note = self.store.db.execute("SELECT text FROM outbox WHERE topic='Atlas' AND text LIKE "
                                     "'This chat now runs on%' ORDER BY id DESC LIMIT 1").fetchone()
        self.assertEqual(note['text'], 'This chat now runs on Claude (the owner selected claude parent chats).')

    async def test_claude_selection_falls_back_to_active_chatgpt_with_its_model_and_quota_notice(self):
        self.prepare_selection_scenario()
        self.select('claude', 'claude-opus-5-5')
        profile = self.root / 'work-claude'
        profile.mkdir()
        with self.store.db:
            self.store.put('accounts', {**self.store.get('accounts'), 'work': {
                'config_dir': str(profile), 'enabled': True}})
            self.store.put('account_status', {**self.store.get('account_status'),
                           'work': self.store.get('account_status')['personal']})
        reset = time.time() + 7200
        self.limit_claude(reset)
        with self.store.db:
            self.store.put('coordinator_provider', 'claude')
        self.service.pause_parent('Atlas', parent.unavailable(
            self.store, self.service.accounts, self.service.codex_accounts))
        before = await self.start()
        self.assertEqual((before.provider, before.account_alias, before.model), ('codex', 'team', 'test-codex'))
        self.assertFalse(before.rejected)
        self.assertEqual(before.switch['reason'], 'every Claude account is out of usage')
        self.assertIsNone(self.store.get('coordinator_account_retry_at'))
        notice = self.store.db.execute("SELECT text FROM outbox WHERE topic='Atlas' AND text LIKE 'This chat now runs on%'").fetchone()
        self.assertIsNone(notice)
        requests = read_jsonl(self.root / 'requests')
        launches = [request['params']['model'] for request in requests if request.get('method') == 'thread/start']
        self.assertEqual(launches, ['test-codex'])
        self.assertEqual(self.store.get('parent_selection'), {'provider': 'claude', 'model': 'claude-opus-5-5'})

    async def test_fallback_returns_to_selected_claude_only_after_turn_and_background_settle(self):
        self.prepare_selection_scenario()
        binary = self.root / 'claude'
        binary.write_text(binary.read_text().replace("    text=event['message']['content']", """    text=event['message']['content']
    with (root/'claude-inputs').open('a') as log:
        log.write(json.dumps(event)+'\\n')"""))
        self.select('claude', 'claude-opus-5-5')
        self.limit_claude(time.time() + 7200)
        before = await self.start()
        row = self.store.message_save('Atlas', 'owner', 'hold', telegram_message=10)
        await self.service.feed_once()
        await settle(asyncio.gather(*self.service.feed_tasks.values()))
        await self.wait_for_receipt(before, row)
        self.assertTrue(before.busy)
        status = self.store.get('account_status')
        status['personal']['usage']['five_hour']['utilization'] = 0
        with self.store.db:
            self.store.put('account_status', status)
        before.protocol.background = [{'task_id': 'settling-work'}]
        await self.service.feed_once()
        self.assertFalse(before.rejected)
        completed = asyncio.Event()
        before.on_turn = lambda reply: completed.set()
        self.assertEqual(await before.send(str(uuid.uuid4()), 'release'), 'received')
        await settle(completed.wait())
        await self.wait_for_journal(before, {'method': 'turn/completed'})
        self.assertFalse(before.busy)
        await self.service.feed_once()
        self.assertFalse(before.rejected)
        text = 'Pending owner request ' + 'x' * 600 + '\nKeep the full final instruction.'
        pending = self.store.message_save('Atlas', 'owner', text, telegram_message=11)
        old_path = Path(before.transcript_path)
        with patch('coordinator.host.HostClient.stop', AsyncMock(side_effect=AssertionError('No signal stop'))):
            before.protocol.background = []
            await self.service.feed_once()
            await settle(before.wait_closed())
        after = await self.start()
        self.assertEqual((after.provider, after.model), ('claude', 'claude-opus-5-5'))
        self.assertTrue(after.switched)
        self.assertEqual(self.store.get(after.key + '_selection_revision'),
                         parent_state.effective_agent(self.store, after.key)['revision'])
        self.assertNotEqual(after.session_id, before.session_id)
        self.assertTrue(old_path.exists())
        completed = asyncio.Event()
        after.on_turn = lambda reply: completed.set() if reply.endswith(text) else None
        await self.service.feed_once()
        await settle(asyncio.gather(*self.service.feed_tasks.values()))
        await settle(completed.wait())
        await self.wait_for_journal(after, {'type': 'result', 'result':
            'done:[topic=Atlas name="Atlas" message=%d kind=owner] ' % pending['id'] + REPLY_FIRST + text})
        events = read_jsonl(self.root / 'claude-inputs')
        prompts = [event['message']['content'] for event in events if 'kind=owner]' in event['message']['content']]
        self.assertEqual(prompts, ['[topic=Atlas name="Atlas" message=%d kind=owner] ' % pending['id'] + REPLY_FIRST + text])
        for message in (row, pending):
            await self.wait_for_receipt(after, message)
            self.assertEqual(tuple(self.store.db.execute('SELECT delivered,receipt FROM messages WHERE id=?',
                                                       (message['id'],)).fetchone()), ('received', 'replayed'))

    async def test_fallback_idle_and_send_cycles_keep_the_same_host_while_claude_is_limited(self):
        self.prepare_selection_scenario()
        self.limit_claude(time.time() + 7200)
        before = await self.start()
        self.select('claude', 'claude-opus-5-5')
        identity = before.host_id, before.session_id, before.process.pid
        with patch('coordinator.host.HostClient.stop', AsyncMock(side_effect=AssertionError('No signal stop'))), \
                patch('coordinator.parent_state.retire', AsyncMock()) as retire:
            for index in range(3):
                await self.service.feed_once()
                self.assertFalse(before.rejected)
                self.assertIs(await self.service.start_session('Atlas'), before)
                await self.feed_owner(before, 'owner cycle %d' % index, 100 + index)
                self.assertTrue(before.idle())
                self.assertFalse(before.movable())
                self.assertEqual((before.host_id, before.session_id, before.process.pid), identity)
            retire.assert_not_called()
        self.assertEqual(len(read_jsonl(self.root / 'launches')), 1)

    async def test_chatgpt_fallback_returns_to_recovered_owner_pick_after_work_settles_and_keeps_history(self):
        self.prepare_owner_pick_scenario()
        self.select('claude', 'claude-opus-5-5')
        self.limit_claude(time.time() + 7200)
        before = await self.start()
        self.assertEqual((before.provider, before.account_alias), ('codex', 'team'))
        self.assertEqual(await before.send(str(uuid.uuid4()), 'hold'), 'received')
        self.assertTrue(before.busy)
        status = self.store.get('account_status')
        for snapshot in status.values():
            snapshot['usage']['five_hour']['utilization'] = 0
        with self.store.db:
            self.store.put('account_status', status)
        before.protocol.background = [{'task_id': 'settling-work'}]
        await self.service.feed_once()
        self.assertFalse(before.rejected)
        self.assertIs(await self.service.start_session('Atlas'), before)
        completed = asyncio.Event()
        before.on_turn = lambda reply: completed.set()
        self.assertEqual(await before.send(str(uuid.uuid4()), 'release'), 'received')
        await settle(completed.wait())
        await self.service.feed_once()
        self.assertFalse(before.busy)
        self.assertFalse(before.rejected)
        native = before.session_id
        history = Path(before.transcript_path)
        content = history.read_bytes()
        self.assertIn(native, history.name)
        self.assertIn(b'hold', content)
        with patch('coordinator.host.HostClient.stop', AsyncMock(side_effect=AssertionError('No signal stop'))):
            before.protocol.background = []
            await self.service.feed_once()
            await settle(before.wait_closed())
        after = await self.start()
        self.assertEqual((after.provider, after.account_alias, after.model),
                         ('claude', 'work', 'claude-opus-5-5'))
        self.assertNotEqual(after.session_id, native)
        self.assertEqual(history.read_bytes(), content)
        identity = after.host_id, after.session_id, after.process.pid
        for index in range(3):
            await self.service.feed_once()
            self.assertIs(await self.service.start_session('Atlas'), after)
            await self.feed_owner(after, 'picked account cycle %d' % index, 200 + index)
            self.assertFalse(after.movable())
            self.assertEqual((after.host_id, after.session_id, after.process.pid), identity)
        self.assertEqual(history.read_bytes(), content)
        self.assertEqual(len(read_jsonl(self.root / 'launches')), 1)
        self.assertEqual(len(read_jsonl(self.root / 'launches.jsonl')), 1)

    async def test_paused_owner_pick_uses_other_claude_then_returns_once_with_same_native_session(self):
        self.prepare_owner_pick_scenario()
        self.select('claude', 'claude-opus-5-5')
        status = self.store.get('account_status')
        status['work']['usage']['five_hour'] = {'utilization': 98, 'resets_at': time.time() + 7200}
        with self.store.db:
            self.store.put('account_status', status)
        before = await self.start()
        self.assertEqual((before.provider, before.account_alias), ('claude', 'personal'))
        completed = asyncio.Event()
        before.on_turn = lambda reply: completed.set()
        self.assertEqual(await before.send(str(uuid.uuid4()), 'slow'), 'received')
        await until(lambda: (self.root / 'slow.started').exists())
        status['work']['usage']['five_hour']['utilization'] = 0
        with self.store.db:
            self.store.put('account_status', status)
        before.protocol.background = [{'task_id': 'settling-work'}]
        await self.service.feed_once()
        self.assertTrue(before.busy)
        self.assertFalse(before.rejected)
        self.assertIs(await self.service.start_session('Atlas'), before)
        (self.root / 'slow.go').touch()
        await settle(completed.wait())
        await self.wait_for_journal(before, {'type': 'result', 'result': 'done:slow'})
        await self.service.feed_once()
        self.assertFalse(before.busy)
        self.assertFalse(before.rejected)
        native = before.session_id
        history = Path(before.router.transcript(native))
        content = history.read_bytes()
        before.protocol.background = []
        pending = self.store.message_save('Atlas', 'owner', 'resume on the recovered pick', telegram_message=299)
        await self.service.feed_once()
        await settle(asyncio.gather(*self.service.feed_tasks.values()))
        await settle(before.wait_closed())
        after = await self.start()
        self.assertEqual((after.provider, after.account_alias, after.session_id), ('claude', 'work', native))
        self.assertEqual(self.store.get(after.key + '_selection_revision'),
                         parent_state.effective_agent(self.store, after.key)['revision'])
        spec = json.loads((self.runner.state_dir / 'hosts' / after.host_id / 'spec.json').read_text())
        self.assertTrue(Path(spec['argv'][spec['argv'].index('--resume') + 1]).samefile(history))
        self.assertEqual(history.read_bytes(), content)
        await self.wait_for_receipt(after, pending, feed=True)
        await self.wait_for_journal(after, {'type': 'result', 'result':
            'done:[topic=Atlas name="Atlas" message=%d kind=owner] ' % pending['id'] + REPLY_FIRST + pending['text']})
        await self.wait_for_parent(after, after.idle, 'the resumed parent to finish the pending owner turn')
        self.assertEqual(tuple(self.store.db.execute('SELECT delivered,receipt FROM messages WHERE id=?',
                                                   (pending['id'],)).fetchone()), ('received', 'replayed'))
        identity = after.host_id, after.session_id, after.process.pid
        for index in range(3):
            await self.service.feed_once()
            self.assertIs(await self.service.start_session('Atlas'), after)
            await self.feed_owner(after, 'recovered pick cycle %d' % index, 300 + index)
            self.assertFalse(after.movable())
            self.assertEqual((after.host_id, after.session_id, after.process.pid), identity)
        self.assertEqual(len(read_jsonl(self.root / 'launches.jsonl')), 2)
        self.assertFalse((self.root / 'launches').exists())

    async def test_paused_owner_pick_does_not_ping_pong_the_chatgpt_fallback(self):
        self.prepare_owner_pick_scenario()
        self.select('claude', 'claude-opus-5-5')
        self.limit_claude(time.time() + 7200)
        session = await self.start()
        self.assertEqual((session.provider, session.account_alias), ('codex', 'team'))
        identity = session.host_id, session.session_id, session.process.pid
        for index in range(3):
            await self.service.feed_once()
            self.assertIs(await self.service.start_session('Atlas'), session)
            await self.feed_owner(session, 'paused pick cycle %d' % index, 400 + index)
            self.assertEqual(self.store.get('claude_account_pick'), 'work')
            self.assertFalse(session.movable())
            self.assertEqual((session.host_id, session.session_id, session.process.pid), identity)
        self.assertEqual(len(read_jsonl(self.root / 'launches')), 1)
        self.assertFalse((self.root / 'launches.jsonl').exists())

    async def test_claude_selection_with_both_providers_limited_pauses_at_the_earliest_reset(self):
        self.prepare_selection_scenario()
        self.select('claude', 'claude-opus-5-5')
        claude_reset, codex_reset = time.time() + 7200, time.time() + 3600
        self.limit_claude(claude_reset)
        profile = self.root / 'work-chatgpt'
        profile.mkdir()
        with self.store.db:
            self.store.put('codex_accounts', {**self.store.get('codex_accounts'), 'work': {
                'config_dir': str(profile), 'enabled': True}})
            self.store.put('codex_account_status', {**self.store.get('codex_account_status'), 'work': {
                'identity': {'logged_in': True, 'email': 'work@example.test'}}})
            self.store.put('codex_auto_switch', True)
            self.store.put('codex_account_blocks', {'team': {'reason': 'quota', 'until': codex_reset}})
        row = self.store.message_save('Atlas', 'owner', 'saved while both are limited', telegram_message=1)
        self.assertIsNone(await self.service.start_session('Atlas'))
        self.assertEqual(self.store.get('coordinator_account_retry_at'), codex_reset)
        self.assertEqual(self.store.get('coordinator_account_notice')['key'][:2], ['both', 'team'])
        for _ in range(3):
            await self.service.feed_once()
        notices = list(self.store.db.execute("SELECT text FROM outbox WHERE topic='Atlas'"))
        self.assertEqual(len(notices), 1)
        self.assertIn('no Claude or ChatGPT account can take work now', notices[0]['text'])
        self.assertIn('Your messages are saved.', notices[0]['text'])
        self.assertEqual(self.store.messages_pending()[0]['id'], row['id'])
        self.assertEqual(self.store.get('codex_active_account'), 'team')
        self.assertFalse((self.root / 'launches').exists())

    async def test_claude_selection_falls_back_when_usage_changes_during_launch(self):
        self.prepare_selection_scenario()
        self.select('claude', 'claude-opus-5-5')
        with self.store.db:
            self.store.put('coordinator_provider', 'claude')
        launch = parent.CoordinatorSession.start

        async def unavailable_at_launch(*args, **kwargs):
            self.limit_claude(time.time() + 7200)
            return await launch(*args, **kwargs)

        with patch.object(parent.CoordinatorSession, 'start', side_effect=unavailable_at_launch):
            session = await self.start()
        self.assertEqual((session.provider, session.account_alias, session.model), ('codex', 'team', 'test-codex'))
        self.assertEqual(session.switch['reason'], 'every Claude account is out of usage')
        self.assertFalse(session.rejected)

    async def test_codex_selection_does_not_fall_back_to_available_claude(self):
        self.prepare_selection_scenario()
        self.select('codex', 'test-astra')
        reset = time.time() + 3600
        with self.store.db:
            self.store.put('codex_account_blocks', {'team': {'reason': 'quota', 'until': reset}})
        self.assertIsNone(await self.service.start_session('Atlas'))
        self.assertEqual(self.store.get('coordinator_account_retry_at'), reset)
        self.assertEqual(self.store.get('coordinator_account_notice')['key'][:2], ['codex', 'team'])
        self.assertFalse((self.root / 'launches.jsonl').exists())

    async def assert_quota_handoff(self, provider, failed_close=False, same_provider_first=False):
        claude = self.root / 'claude'
        source = claude.read_text().replace("    text=event['message']['content']", """    text=event['message']['content']
    with (root/'claude-inputs').open('a') as log:
        log.write(json.dumps(dict(event,session_id=sid))+'\\n')
    transcript=Path(profile)/'projects'/'test'/(sid+'.jsonl')
    with transcript.open('a') as log:
        log.write(json.dumps({'type':'user','uuid':event['uuid']})+'\\n')""")
        if provider == 'claude':
            source = source.replace("    if text=='bg-work':", """    if (text.endswith('quota') and not (root/'claude-rejected').exists()) or ('kind=callback]' in text and profile.endswith('claude-next')):
        (root/'claude-rejected').touch()
        emit({'type':'assistant','message':{'role':'assistant','content':[{'type':'text','text':'working'}]}})
        emit({'type':'rate_limit_event','rate_limit_info':{'status':'rejected','resetsAt':time.time()+3600}})
        emit({'type':'result','subtype':'error','is_error':True,'result':"You're out of usage credits"})
        continue
    if text=='bg-work':""")
            codex = self.root / 'codex'
            codex.write_text(codex.read_text().replace("elif text.endswith('quota') or text.endswith('quota-hold'):",
                                                     "elif text.endswith('quota-hold'):"))
        else:
            self.store.put('account_blocks', {'c': {'reason': 'quota', 'until': time.time() + 3600}})
        if same_provider_first:
            profile = self.root / 'claude-next'
            profile.mkdir()
            self.store.put('accounts', {**self.store.get('accounts'), 'd': {
                'config_dir': str(profile), 'enabled': True}})
            self.store.put('account_status', {**self.store.get('account_status'), 'd': {
                'identity': {'logged_in': True, 'email': 'next@example.test'}, 'observed_at': time.time(),
                'usage': {'five_hour': {'utilization': 0},
                          'seven_day': {'utilization': 0, 'resets_at': time.time() + 7200}}}})
        claude.write_text(source)
        before = await self.start()
        self.assertEqual(before.provider, provider)
        text = 'Owner request ' + 'x' * 600 + '\nRequired final instruction beyond the excerpt. quota'
        row = self.store.message_save('home', 'owner', text, telegram_message=10)
        send = before.send

        async def delayed_receipt(message_id, prompt, row_id=None):
            receipt = await send(message_id, prompt, row_id=row_id)
            await before.wait_closed()
            return receipt

        failure = sqlite3.OperationalError('deterministic close settlement failure') if failed_close else None
        with patch.object(before, '_settle', wraps=before._settle, side_effect=failure), \
                patch.object(before, 'send', side_effect=delayed_receipt):
            await self.service.feed_once()
            await settle(asyncio.gather(*self.service.feed_tasks.values()))
        self.assertTrue(before.rejected)
        path = before.transcript_path or before.router.transcript(before.session_id)
        self.assertEqual(before.transcript_receipts(path, [row['id']]), {row['id']})
        saved = self.store.db.execute('SELECT delivered,receipt FROM messages WHERE id=?', (row['id'],)).fetchone()
        self.assertEqual(tuple(saved), ('received', 'replayed'))
        self.assertEqual(self.store.get(before.key + '_lost'), [row['id']] if failed_close else None)
        if provider == 'codex':
            self.store.put('account_blocks', {})
        if same_provider_first:
            rotated = await self.start()
            self.assertEqual((rotated.provider, rotated.account_alias, rotated.session_id),
                             ('claude', 'd', before.session_id))
            send = rotated.send

            async def rotated_receipt(message_id, prompt, row_id=None):
                receipt = await send(message_id, prompt, row_id=row_id)
                await rotated.wait_closed()
                return receipt

            with patch.object(rotated, 'send', side_effect=rotated_receipt):
                await self.service.feed_once()
                await settle(asyncio.gather(*self.service.feed_tasks.values()))
            events = read_jsonl(self.root / 'claude-inputs')
            callbacks = [event['message']['content'] for event in events if 'kind=callback]' in event['message']['content']]
            self.assertEqual(callbacks, ['[topic=home name="home" message=2 kind=callback] ' +
                                        parent_state.LOST_TURN % 'message=1'])
        after = await self.start()
        self.assertEqual(after.provider, 'codex' if provider == 'claude' else 'claude')
        self.assertTrue(after.switched)
        self.assertNotEqual(after.session_id, before.session_id)
        self.assertEqual(tuple(self.store.db.execute('SELECT delivered,receipt FROM messages WHERE id=?',
                                                   (row['id'],)).fetchone()), ('pending', None))
        self.assertIsNone(self.store.get(before.key + '_lost'))
        for _ in range(3):
            self.assertIs(await self.service.start_session('home'), after)
            await self.service.feed_once()
            await settle(asyncio.gather(*self.service.feed_tasks.values()))
            await until(after.idle)
        native = after.session_id
        await after.detach()
        self.parents.remove(after)
        self.service.sessions.pop('home')
        after = await self.start()
        self.assertEqual(after.session_id, native)
        await self.service.feed_once()
        await settle(asyncio.gather(*self.service.feed_tasks.values()))
        await until(after.idle)
        if after.provider == 'codex':
            requests = read_jsonl(self.root / 'requests')
            prompts = [request['params']['input'][0]['text'] for request in requests
                       if request.get('method') in ('turn/start', 'turn/steer')]
        else:
            events = read_jsonl(self.root / 'claude-inputs')
            prompts = [event['message']['content'] for event in events if event['session_id'] == after.session_id]
        expected = '[topic=home name="home" message=1 kind=owner] ' + REPLY_FIRST + text
        self.assertEqual([prompt for prompt in prompts if 'kind=owner]' in prompt], [expected])
        self.assertFalse(any('already in this conversation' in prompt for prompt in prompts), prompts)
        self.assertEqual(tuple(self.store.db.execute('SELECT delivered,receipt FROM messages WHERE id=?',
                                                   (row['id'],)).fetchone()), ('received', 'replayed'))
        self.assertEqual(before.transcript_receipts(path, [row['id']]), {row['id']})

    async def test_claude_quota_handoff_to_codex_delivers_the_full_owner_message_once(self):
        await self.assert_quota_handoff('claude')

    async def test_codex_quota_handoff_to_claude_delivers_the_full_owner_message_once(self):
        await self.assert_quota_handoff('codex')

    async def test_claude_unsettled_quota_handoff_to_codex_delivers_the_full_owner_message_once(self):
        await self.assert_quota_handoff('claude', failed_close=True)

    async def test_codex_unsettled_quota_handoff_to_claude_delivers_the_full_owner_message_once(self):
        await self.assert_quota_handoff('codex', failed_close=True)

    async def test_rejected_same_provider_recovery_then_switch_delivers_the_full_owner_message_once(self):
        await self.assert_quota_handoff('claude', same_provider_first=True)

    async def test_silent_capacity_recovery_then_rotation_delivers_the_owner_message_once(self):
        claude = self.root / 'claude'
        source = claude.read_text().replace("    text=event['message']['content']", """    text=event['message']['content']
    with (root/'claude-inputs').open('a') as log:
        log.write(json.dumps(dict(event,session_id=sid))+'\\n')
    transcript=Path(profile)/'projects'/'test'/(sid+'.jsonl')
    with transcript.open('a') as log:
        log.write(json.dumps({'type':'user','uuid':event['uuid']})+'\\n')""")
        source = source.replace("    if text=='bg-work':", """    if text.endswith('claude-quota') and profile.endswith('claude-home'):
        emit({'type':'assistant','message':{'role':'assistant','content':[{'type':'text','text':'working'}]}})
        emit({'type':'rate_limit_event','rate_limit_info':{'status':'rejected','resetsAt':time.time()+3600}})
        emit({'type':'result','subtype':'error','is_error':True,'result':"You're out of usage credits"})
        continue
    if text=='bg-work':""")
        claude.write_text(source)
        profile = self.root / 'claude-next'
        profile.mkdir()
        with self.store.db:
            self.store.put('accounts', {**self.store.get('accounts'), 'd': {
                'config_dir': str(profile), 'enabled': True}})
            self.store.put('account_status', {**self.store.get('account_status'), 'd': {
                'identity': {'logged_in': True, 'email': 'next@example.test'}, 'observed_at': time.time(),
                'usage': {'five_hour': {'utilization': 0},
                          'seven_day': {'utilization': 0, 'resets_at': time.time() + 7200}}}})
            self.store.put('account_blocks', {alias: {'reason': 'quota', 'until': time.time() + 3600}
                                              for alias in ('c', 'd')})
        first = await self.start()
        self.assertEqual(first.provider, 'codex')
        await first.stop()
        with self.store.db:
            self.store.put('account_blocks', {})
        with patch.object(self.store, 'enqueue_report', side_effect=AssertionError('Capacity recovery is silent')):
            await self.service.start_session('home')
        mid = self.service.sessions['home']
        self.parents.append(mid)
        self.assertEqual((mid.provider, mid.account_alias), ('claude', 'c'))
        self.assertIsNone(self.store.get(mid.key + '_switch'))
        self.assertIs(await self.service.start_session('home'), mid)
        self.assertEqual(await mid.send(str(uuid.uuid4()), 'ready'), 'received')
        await until(lambda: mid.idle() or mid.closed)
        await self.service.feed_once()
        await settle(asyncio.gather(*self.service.feed_tasks.values()))
        await until(lambda: mid.idle() or mid.closed)
        text = 'Second owner request ' + 'y' * 40 + ' claude-quota'
        row = self.store.message_save('home', 'owner', text, telegram_message=20)
        send = mid.send

        async def delayed_receipt(message_id, prompt, row_id=None):
            receipt = await send(message_id, prompt, row_id=row_id)
            await mid.wait_closed()
            return receipt

        with patch.object(mid, 'send', side_effect=delayed_receipt):
            await self.service.feed_once()
            await settle(asyncio.gather(*self.service.feed_tasks.values()))
        self.assertTrue(mid.rejected)
        callback = self.store.db.execute("SELECT id,delivered,receipt FROM messages WHERE kind='callback'").fetchone()
        self.assertEqual(tuple(callback)[1:], ('pending', None))
        self.assertEqual(self.store.get(mid.key + '_lost_turns'), {str(callback['id']): [row['id']]})
        rotated = await self.start()
        self.assertEqual((rotated.provider, rotated.account_alias, rotated.session_id), ('claude', 'd', mid.session_id))
        for _ in range(2):
            await self.service.feed_once()
            await settle(asyncio.gather(*self.service.feed_tasks.values()))
            await until(lambda: rotated.idle() or rotated.closed)
        events = read_jsonl(self.root / 'claude-inputs')
        prompts = [event['message']['content'] for event in events if event['session_id'] == mid.session_id]
        owners = [prompt for prompt in prompts if 'message=%d kind=owner]' % row['id'] in prompt]
        self.assertEqual(len(owners), 1, 'the resumed conversation received the owner message twice')
        self.assertEqual(owners, ['[topic=home name="home" message=%d kind=owner] ' % row['id'] + REPLY_FIRST + text])
        self.assertEqual([prompt for prompt in prompts if 'kind=callback]' in prompt],
                         ['[topic=home name="home" message=%d kind=callback] ' % callback['id'] +
                          parent_state.LOST_TURN % ('message=%d' % row['id'])])
        for item in (row, callback):
            self.assertEqual(tuple(self.store.db.execute('SELECT delivered,receipt FROM messages WHERE id=?',
                                                       (item['id'],)).fetchone()), ('received', 'replayed'))

    async def test_idle_model_change_closes_input_resumes_history_and_keeps_workers(self):
        self.select('codex', 'test-codex')
        before = [await self.start(topic) for topic in ('home', 'other')]
        natives = [session.session_id for session in before]
        self.select('codex', 'test-astra')
        with patch('coordinator.host.HostClient.stop', AsyncMock(side_effect=AssertionError('No signal stop'))):
            await self.service.feed_once()
            await until(lambda: all(session.closed for session in before))
        after = [await self.start(topic) for topic in ('home', 'other')]
        self.assertEqual([session.model for session in after], ['test-astra', 'test-astra'])
        self.assertEqual([session.session_id for session in after], natives)
        self.assertTrue(all(old.host_id != new.host_id for old, new in zip(before, after)))
        self.assertEqual((self.store.get('codex_model'), self.store.get('codex_active_account')),
                         ('test-codex', 'g'))

    async def test_completed_owner_message_is_not_replayed_when_selection_changes_before_receipt(self):
        binary = self.root / 'codex'
        source = binary.read_text()
        anchor = "        if method=='turn/start' and turn is None:\n"
        gate = ("        if text.endswith('receipt-gate'):\n"
                "            (root/'request-ready').touch()\n"
                "            while not (root/'request-go').exists():\n"
                "                time.sleep(.01)\n")
        self.assertIn(anchor, source)
        binary.write_text(source.replace(anchor, gate + anchor))
        self.select('codex', 'test-codex')
        before = await self.start()
        row = self.store.message_save('home', 'owner', 'receipt-gate', telegram_message=1)
        await self.service.feed_once()
        await until(lambda: (self.root / 'request-ready').exists())
        self.select('codex', 'test-astra')
        (self.root / 'request-go').touch()
        await settle(asyncio.gather(*self.service.feed_tasks.values()))
        await self.wait_for_receipt(before, row)
        saved = self.store.db.execute('SELECT delivered,receipt FROM messages WHERE id=?', (row['id'],)).fetchone()
        self.assertEqual(tuple(saved), ('received', 'replayed'))
        await self.service.feed_once()
        await self.wait_for_journal(before, {'method': 'turn/completed'})
        await settle(before.wait_closed())
        after = await self.start()
        self.assertEqual(self.store.get(after.key + '_selection_revision'),
                         parent_state.effective_agent(self.store, after.key)['revision'])
        await self.service.feed_once()
        await settle(asyncio.gather(*self.service.feed_tasks.values()))
        await self.wait_for_parent(after, after.idle, 'the replacement parent to finish its turn')
        requests = read_jsonl(self.root / 'requests')
        turns = [request for request in requests if request.get('method') == 'turn/start'
                 and request['params']['input'][0]['text'].endswith('receipt-gate')]
        self.assertEqual(len(turns), 1)
        self.assertEqual((before.model, after.model), ('test-codex', 'test-astra'))

    async def test_cross_provider_change_waits_for_claude_turn_and_starts_fresh(self):
        self.select('claude', 'claude-opus-5-5')
        before = await self.start()
        self.assertEqual(await before.send(str(uuid.uuid4()), 'slow'), 'received')
        await until(lambda: (self.root / 'slow.started').exists() and before.busy)
        self.select('codex', 'test-astra')
        await self.service.feed_once()
        self.assertFalse(before.rejected)
        self.assertFalse(before.closed)
        with patch('coordinator.host.HostClient.stop', AsyncMock(side_effect=AssertionError('No signal stop'))):
            (self.root / 'slow.go').touch()
            await until(lambda: before.closed)
        after = await self.start()
        self.assertTrue(after.switched)
        self.assertNotEqual(after.session_id, before.session_id)
        self.assertEqual((self.store.get('coordinator_provider'), after.model), ('codex', 'test-astra'))
        self.assertTrue((self.root / 'claude-home' / 'projects' / 'test' / (before.session_id + '.jsonl')).exists())
        self.assertIn('owner selected', after.switch['reason'])

    async def test_background_tasks_and_pending_receipts_delay_selection(self):
        self.select('claude', 'claude-opus-5-5')
        before = await self.start()
        self.assertEqual(await before.send(str(uuid.uuid4()), 'bg-work'), 'received')
        await self.wait_for_journal(before, {'type': 'result', 'result': 'done:bg-work'})
        await self.wait_for_parent(before, lambda: before.protocol.background and not before.busy,
                                   'the background task to remain active after its starting turn')
        self.select('codex', 'test-astra')
        await self.service.feed_once()
        self.assertFalse(before.rejected)
        self.assertFalse(before.closed)
        pending = self.store.message_save('home', 'owner', 'receipt while background settles',
                                          telegram_message=2)
        await self.service.feed_once()
        await settle(asyncio.gather(*self.service.feed_tasks.values()))
        self.assertEqual(tuple(self.store.db.execute('SELECT delivered,receipt FROM messages WHERE id=?',
                                                   (pending['id'],)).fetchone()), ('received', 'written'))
        self.assertFalse(before.rejected)
        self.assertFalse(before.closed)
        (self.root / 'bg-finish').touch()
        await self.wait_for_journal(before, {'type': 'result', 'result': 'done:auto'})
        await self.wait_for_receipt(before, pending)
        await settle(before.wait_closed())
        after = await self.start()
        self.assertEqual(after.model, 'test-astra')
        self.assertEqual(self.store.get(after.key + '_selection_revision'),
                         parent_state.effective_agent(self.store, after.key)['revision'])
        self.assertTrue((self.root / 'auto.finished').exists())

    async def test_codex_return_to_claude_preserves_old_native_history(self):
        self.select('codex', 'test-astra')
        before = await self.start()
        old_native = before.session_id
        old_path = before.transcript_path
        self.select('claude', 'claude-opus-5-5')
        with patch('coordinator.host.HostClient.stop', AsyncMock(side_effect=AssertionError('No signal stop'))):
            await self.service.feed_once()
            await until(lambda: before.closed)
        after = await self.start()
        self.assertTrue(after.switched)
        self.assertNotEqual(after.session_id, old_native)
        self.assertEqual((after.model, self.store.get('coordinator_provider')), ('claude-opus-5-5', 'claude'))
        self.assertTrue(Path(old_path).exists())

    async def test_live_host_is_reattached_before_retirement(self):
        self.select('codex', 'test-codex')
        before = await self.start()
        await before.detach()
        self.parents.remove(before)
        self.select('codex', 'test-astra')
        with patch('coordinator.host.HostClient.stop', AsyncMock(side_effect=AssertionError('No signal stop'))):
            attached = await parent.start(self.store, self.runner, self.root, 'unused', 'policy', topic='home')
            self.parents.append(attached)
            self.assertEqual(attached.host_id, before.host_id)
            await until(lambda: attached.closed)

    async def test_close_failure_leaves_existing_writer_and_records_pending(self):
        self.select('codex', 'test-codex')
        session = await self.start()
        self.select('codex', 'test-astra')
        with patch.object(session.process, 'request', AsyncMock(side_effect=BrokenPipeError)):
            await parent_state.retire(session)
        self.assertFalse(session.closed)
        self.assertEqual(self.store.get('coordinator_host')['id'], session.host_id)

    async def test_unfinished_retirement_does_not_block_other_parents_or_start_another_writer(self):
        self.select('codex', 'test-codex')
        first = await self.start()
        other = await self.start('other')
        self.select('codex', 'test-astra')
        with patch.object(first.process, 'request', AsyncMock(return_value={'ok': True})), \
                patch('coordinator.service.PARENT_IDLE', 0):
            await self.service.feed_once()
            await until(lambda: other.closed)
            self.assertIsNone(await asyncio.wait_for(self.service.start_session('home'), 1))
            self.assertFalse(first.closed)
            self.assertEqual(self.store.get('coordinator_host')['id'], first.host_id)
        after = await self.start('other')
        self.assertEqual(after.model, 'test-astra')
