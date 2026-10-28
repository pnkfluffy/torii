"""Offline OpenCode parent lifecycle, readiness, and recovery contracts."""

import asyncio
import fnmatch
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from coordinator import mcp, opencode_parent, opencode_runtime, parent, parent_state
from coordinator.opencode_parent import OpenCodeParentSession
from coordinator.policy import chat_policy
from coordinator.providers import ProviderRunner
from coordinator.service import Service
from coordinator.session import message_uuid
from coordinator.store import Store
from tests.fake_opencode import install_fake_opencode, install_versioned_opencode
from tests.support import read_jsonl, stop_test_hosts, until
from tests.test_opencode_runtime import ACCOUNT
from tests.test_service import FakeTelegram


class OpenCodeParentTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.addCleanup(stop_test_hosts, self.root)
        self.home = self.root / 'home'
        self.home.mkdir()
        environment = patch.dict(os.environ, {'HOME': str(self.home)})
        environment.start()
        self.addCleanup(environment.stop)
        self.account = self.root / 'test-account'
        self.account.mkdir()
        self.key = self.account / 'key'
        self.key.write_text('synthetic-parent-key-only\n')
        self.key.chmod(0o600)
        homes = patch('coordinator.opencode_accounts.account_home', return_value=self.account)
        homes.start()
        self.addCleanup(homes.stop)
        self.binary = install_fake_opencode(self.root / 'fake')
        self.cwd = self.root / 'project'
        self.cwd.mkdir()
        (self.cwd / 'AGENTS.md').write_text('Project instructions.\n')
        self.store = Store(self.root / 'state')
        self.addCleanup(self.store.close)
        with self.store.db:
            self.store.put('coordinator_home_topic', 'home')
            self.store.put('opencode_accounts', {'one': dict(ACCOUNT), 'two': dict(ACCOUNT, model='other')})
            self.store.put('opencode_active_account', 'one')
            self.store.put('opencode_binary', str(self.binary))
            self.store.put('opencode_enabled', True)
            self.store.put('coordinator:chat_selection', {'provider': 'opencode', 'model': None, 'effort': None, 'revision': 1})
            for topic in ('home', 'chat'):
                self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                                      (topic, 1, 1 if topic == 'home' else 2, topic, str(self.cwd)))
        self.runner = ProviderRunner(self.store.directory)
        self.parents = []
        self.session_key = 'coordinator:chat'
        self.data = opencode_runtime.parent_root(self.store, self.session_key)

    async def asyncTearDown(self):
        for session in self.parents:
            if not session._stopping or session.process.returncode is not None:
                await session.stop()

    def scenario(self, **options):
        (self.binary.parent / 'scenario.json').write_text(json.dumps(options))

    def calls(self, method=None):
        calls = read_jsonl(self.binary.parent / 'calls.jsonl')
        return [row for row in calls if method is None or row['message'].get('method') == method]

    async def start(self, instructions='COORDINATOR_TEST_POLICY'):
        session = await parent.start(self.store, self.runner, self.cwd, 'ignored', instructions, topic='chat')
        self.parents.append(session)
        return session

    def row(self, text='Hello', kind='owner'):
        row = self.store.message_save('chat', kind, text)
        self.store.db.execute('UPDATE messages SET telegram_message=? WHERE id=?', (row['id'] + 100, row['id']))
        self.store.db.commit()
        return row

    async def send(self, session, row):
        service = Service(self.store, None, self.runner, self.cwd)
        prompt = await service._prepare_message(row, session)
        self.store.message_sending(row['id'])
        receipt = await session.send(message_uuid(row['id']), prompt, row_id=row['id'])
        self.store.message_delivered(row['id'], receipt)
        self.assertEqual(receipt, 'received')
        return prompt

    def persisted(self, row):
        return dict(self.store.db.execute('SELECT * FROM messages WHERE id=?', (row['id'],)).fetchone())

    async def test_launch_writes_config_without_builtins_or_key(self):
        session = await self.start()
        path = self.data / 'config/opencode/opencode.json'
        config = json.loads(path.read_text())
        builtins = {'bash', 'edit', 'write', 'apply_patch', 'read', 'grep', 'glob', 'list', 'webfetch',
                    'task', 'todowrite', 'todoread', 'skill'}
        tools = {'torii_' + tool['name'] for tool in mcp.tools_list()}
        def allowed(name):
            result = None
            for pattern, decision in config['permission'].items():
                if fnmatch.fnmatchcase(name, pattern):
                    result = decision
            return result == 'allow'
        self.assertEqual({name for name in builtins | tools if allowed(name)}, tools)
        self.assertEqual(config['mcp'], {})
        self.assertEqual(config['enabled_providers'], ['torii-host'])
        self.assertEqual((config['share'], config['autoupdate'], config['lsp'], config['formatter']),
                         ('disabled', False, False, False))
        self.assertEqual(config['provider']['torii-host']['options']['apiKey'], '{file:' + str(self.key) + '}')
        self.assertNotIn('options', config['provider']['torii-host']['models'][ACCOUNT['model']])
        spec = json.loads((session.process.directory / 'spec.json').read_text())
        self.assertEqual(spec['provider'], 'opencode')
        self.assertEqual(spec['account'], 'one')
        self.assertEqual(spec['locks'], ['coordinator-coordinator:chat'])
        self.assertNotIn('ownership', spec)
        self.assertEqual(spec['argv'], [str(self.binary), 'acp', '--cwd', str(self.cwd)])
        for name in ('HOME', 'XDG_DATA_HOME', 'XDG_CONFIG_HOME', 'TMPDIR'):
            self.assertEqual(Path(spec['env'][name]).parent, self.data)
            self.assertEqual(Path(spec['env'][name]).stat().st_mode & 0o777, 0o700)
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        self.assertEqual((self.data / 'instructions.md').stat().st_mode & 0o777, 0o600)
        self.assertEqual(self.data.stat().st_mode & 0o777, 0o700)
        for folder in (self.store.directory, self.binary.parent):
            for written in folder.rglob('*'):
                if written.is_file():
                    self.assertNotIn(b'synthetic-parent-key-only', written.read_bytes(), str(written))
        host = self.store.get(self.session_key + '_host')
        self.assertEqual((host['picked'], host['effort'], host['model']), (True, None, ACCOUNT['model']))

    async def test_prompt_is_in_instructions_file_not_first_message(self):
        self.scenario(compaction=True, tool_calls=[{'sessionUpdate': 'tool_call', 'toolCallId': 'tool',
                                                   'title': 'torii_tasks_list', 'status': 'pending'}])
        session = await self.start()
        for text in ('first', 'with a tool', 'after fake compaction'):
            await self.send(session, self.row(text))
            await until(session.idle)
        config = json.loads((self.data / 'config/opencode/opencode.json').read_text())
        self.assertEqual(config['instructions'], [str(self.data / 'instructions.md'), str(self.cwd / 'AGENTS.md')])
        self.assertEqual((self.data / 'instructions.md').read_text(),
                         'COORDINATOR_TEST_POLICY\nCurrent usage policy:\n' + chat_policy(self.store, self.session_key))
        prompts = self.calls('session/prompt')
        self.assertEqual(len(prompts), 3)
        self.assertEqual([row['message']['params']['prompt'][0]['text'].splitlines()[-1] for row in prompts],
                         ['first', 'with a tool', 'after fake compaction'])
        self.assertTrue(all('COORDINATOR_TEST_POLICY' not in json.dumps(row['message']) for row in prompts))

    async def test_torii_mcp_sent_on_new_and_load_with_topic(self):
        self.scenario(mcp_real=True)
        session = await self.start()
        await session.stop()
        loaded = await self.start()
        calls = self.calls('session/new') + self.calls('session/load')
        self.assertEqual(len(calls), 2)
        for call in calls:
            server = call['message']['params']['mcpServers'][0]
            self.assertEqual((server['name'], server['command']), ('torii', sys.executable))
            self.assertEqual(server['args'], ['-m', 'coordinator', '--state-dir', str(self.store.directory), 'mcp', '--topic=chat'])
            env = {item['name']: item['value'] for item in server['env']}
            self.assertEqual(env['PYTHONPATH'], str(Path(__file__).resolve().parents[1]))
            marker = Path(env['TORII_MCP_READY'])
            self.assertEqual((marker.read_text(), marker.stat().st_mode & 0o777), ('{}', 0o600))
        self.assertEqual(loaded.session_id, session.session_id)
        await self.send(loaded, self.row())
        await until(loaded.idle)

    async def test_torii_mcp_queues_opencode_worker_with_service_home(self):
        account = self.home / '.opencode-accounts' / 'one'
        account.mkdir(parents=True, mode=0o700)
        key = account / 'key'
        key.write_text('synthetic-mcp-worker-key-only\n')
        key.chmod(0o600)
        task = self.store.task_create('chat', 'Queue an OpenCode worker', worktree=str(self.cwd))
        session = await self.start()
        spec = json.loads((session.process.directory / 'spec.json').read_text())
        server = self.calls('session/new')[0]['message']['params']['mcpServers'][0]
        environment = dict(spec['env'], **{item['name']: item['value'] for item in server['env']})
        requests = [
            {'jsonrpc': '2.0', 'id': 1, 'method': 'initialize', 'params': {}},
            {'jsonrpc': '2.0', 'id': 2, 'method': 'tools/list'},
            {'jsonrpc': '2.0', 'id': 3, 'method': 'tools/call', 'params': {'name': 'workers_spawn',
                'arguments': {'task': task['id'], 'provider': 'opencode', 'prompt': 'Check the queue.'}}},
        ]
        process = await asyncio.create_subprocess_exec(server['command'], *server['args'], cwd=self.cwd,
            env=environment, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(
                ''.join(json.dumps(request) + '\n' for request in requests).encode()), 30)
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()
        self.assertEqual(process.returncode, 0, stderr.decode())
        responses = {item['id']: item for item in map(json.loads, stdout.decode().splitlines())}
        self.assertIn('result', responses[2])
        result = responses[3]['result']
        self.assertFalse(result['isError'], result)
        self.assertEqual([tuple(row) for row in self.store.db.execute(
            'SELECT task,provider,status FROM workers ORDER BY id')], [(task['id'], 'opencode', 'queued')])

    async def test_torii_mcp_service_folders_stay_outside_parent_root(self):
        defaults = {'HOME': str(self.home), 'XDG_CONFIG_HOME': str(self.home / '.config'),
                    'XDG_DATA_HOME': str(self.home / '.local/share'), 'XDG_CACHE_HOME': str(self.home / '.cache'),
                    'XDG_STATE_HOME': str(self.home / '.local/state'), 'TMPDIR': str(self.root / 'service-tmp')}
        configured = dict(defaults, **{name: str(self.root / ('service-' + name.lower()))
                                      for name in defaults if name != 'HOME'})
        for explicit in (False, True):
            with self.subTest(explicit=explicit):
                service = dict(configured if explicit else {'HOME': str(self.home)}, PATH=os.environ['PATH'])
                with patch.dict(os.environ, service, clear=True), patch.object(tempfile, 'tempdir', defaults['TMPDIR']):
                    server = opencode_parent.mcp_servers(self.store, 'chat', self.data / 'mcp-ready-folders')[0]
                    environment = dict(opencode_runtime.parent_environment(self.data, os.environ),
                                       **{item['name']: item['value'] for item in server['env']})
                for name, value in (configured if explicit else defaults).items():
                    self.assertEqual(environment[name], value, name)
                for name, value in environment.items():
                    if name not in ('TORII_MCP_READY', 'OPENCODE_TEST_MANAGED_CONFIG_DIR'):
                        self.assertNotIn(str(self.data), value, name)

    async def test_missing_mcp_ready_fails_launch(self):
        self.scenario(mcp_missing=True)
        with patch.object(opencode_parent, 'MCP_READY_SECONDS', 0.05):
            with self.assertRaisesRegex(RuntimeError, 'MCP is unavailable'):
                await self.start()
        self.assertEqual(len(self.calls('session/new')), 1)
        self.assertEqual(len(self.calls('session/load')), 1)
        self.assertEqual(self.calls('session/prompt'), [])
        self.assertIsNone(self.store.get(self.session_key + '_host'))

    def failing_mcp(self):
        script = self.root / 'list-failure.py'
        script.write_text('import sys\nfrom pathlib import Path\nfrom coordinator import mcp\nfrom coordinator.store import Store\n'
                          'def fail():\n    raise RuntimeError("scripted tools/list failure")\n'
                          'mcp.tools_list = fail\n'
                          'store = Store(Path(sys.argv[1]))\nmcp.serve(store, None)\nstore.close()\n')
        original = opencode_parent.mcp_servers
        def servers(store, topic, ready):
            result = original(store, topic, ready)
            result[0]['args'] = [str(script), str(self.store.directory)]
            return result
        return patch.object(opencode_parent, 'mcp_servers', servers)

    async def test_mcp_ready_needs_tools_list_not_initialize(self):
        self.scenario(mcp_real=True)
        with self.failing_mcp(), patch.object(opencode_parent, 'MCP_READY_SECONDS', 0.05):
            with self.assertRaisesRegex(RuntimeError, 'MCP is unavailable'):
                await self.start()
        sessions = self.calls('session/new') + self.calls('session/load')
        self.assertEqual(len({row['pid'] for row in sessions}), 2)
        for row in sessions:
            self.assertFalse(opencode_parent.ready_path(row['message']['params']['mcpServers']).exists())
        self.assertEqual(self.calls('session/prompt'), [])
        self.assertIsNone(self.store.get(self.session_key + '_host'))

    async def test_failed_registration_relaunches_fresh_host_not_identical_load(self):
        waiting = self.row('still queued')
        self.scenario(mcp_missing=True)
        with patch.object(opencode_parent, 'MCP_READY_SECONDS', 0.05):
            with self.assertRaisesRegex(RuntimeError, 'MCP is unavailable'):
                await self.start()
        attempts = [row for row in self.calls() if row['message'].get('method') in ('session/new', 'session/load')]
        self.assertEqual([row['message']['method'] for row in attempts], ['session/new', 'session/load'])
        self.assertEqual(len({row['pid'] for row in attempts}), 2)
        self.assertEqual(len({str(opencode_parent.ready_path(row['message']['params']['mcpServers'])) for row in attempts}), 2)
        self.assertEqual(self.persisted(waiting)['delivered'], 'pending')
        self.assertEqual(self.calls('session/prompt'), [])
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM problems WHERE area='coordinator' AND code='mcp-unavailable'").fetchone()[0], 1)

    async def test_parent_start_uses_own_pin_during_setting_switch(self):
        paths = {v: install_versioned_opencode(self.root / 'versions', v) for v in ('1.19.0', '1.19.3')}
        for pin, selected in (('1.19.0', '1.19.3'), ('1.19.3', '1.19.0')):
            with self.subTest(pin=pin), patch('coordinator.opencode_accounts.PINNED_VERSION', pin):
                self.binary = paths[pin]
                self.store.put('opencode_binary', str(paths[selected]))
                session = await self.start()
                await self.send(session, self.row('work in the switch window'))
                await until(session.idle)
                spec = json.loads((session.process.directory / 'spec.json').read_text())
                self.assertEqual(spec['argv'][0], str(paths[pin]))
                self.assertIsNone(session.protocol.failure)
                self.assertIsNone(self.store.get('opencode_account_blocks'))
                self.assertEqual(self.store.get('opencode_binary'), str(paths[selected]))
                await session.stop()

    async def test_setting_switch_keeps_resolved_parent_fingerprint(self):
        old = install_versioned_opencode(self.root / 'versions', '1.19.0')
        new = install_versioned_opencode(self.root / 'versions', '1.19.3')
        self.store.put('opencode_binary', str(old))
        session = await self.start()
        await session.detach()
        self.parents.remove(session)
        self.store.put('opencode_binary', str(new))
        attached = await self.start()
        self.assertEqual(attached.host_id, session.host_id)
        self.assertFalse(attached.outdated)
        await self.send(attached, self.row('still usable'))
        await until(attached.idle)
        self.assertIsNone(attached.protocol.failure)

    async def test_pin_change_reattaches_busy_parent_then_loads_same_session(self):
        old = install_versioned_opencode(self.root / 'versions', '1.19.0')
        new = install_versioned_opencode(self.root / 'versions', '1.19.3')
        self.binary = old
        self.store.put('opencode_binary', str(old))
        release = self.root / 'release'
        self.scenario(version='1.19.0', release=str(release))
        with patch('coordinator.opencode_accounts.PINNED_VERSION', '1.19.0'):
            session = await self.start()
            row = self.row('finish this turn')
            await self.send(session, row)
            await until(lambda: len(self.calls('session/prompt')) == 1)
            await session.detach()
            self.parents.remove(session)
        with patch('coordinator.opencode_accounts.PINNED_VERSION', '1.19.3'):
            attached = await self.start()
            self.assertEqual(attached.host_id, session.host_id)
            self.assertTrue(attached.outdated)
            self.assertFalse(attached.rejected)
            self.assertIsNone(attached.protocol.failure)
            release.touch()
            await attached.wait_closed()
            self.assertEqual(self.store.get(self.session_key + '_turn'), [])
            self.assertEqual(len(self.calls('session/prompt')), 1)
            self.binary = new
            loaded = await self.start()
            self.assertNotEqual(loaded.host_id, session.host_id)
            self.assertEqual(loaded.session_id, session.session_id)
            await self.send(loaded, self.row('next turn'))
            await until(loaded.idle)
            self.assertEqual(len(self.calls('session/load')), 1)
            self.assertIsNone(loaded.protocol.failure)

    async def test_pin_change_retires_idle_parent_and_loads_same_session(self):
        old = install_versioned_opencode(self.root / 'versions', '1.19.0')
        new = install_versioned_opencode(self.root / 'versions', '1.19.3')
        self.store.put('opencode_binary', str(old))
        with patch('coordinator.opencode_accounts.PINNED_VERSION', '1.19.0'):
            session = await self.start()
            await session.detach()
            self.parents.remove(session)
        with patch('coordinator.opencode_accounts.PINNED_VERSION', '1.19.3'):
            attached = await self.start()
            self.assertEqual(attached.host_id, session.host_id)
            await attached.wait_closed()
            self.assertTrue(attached.rejected)
            self.binary = new
            loaded = await self.start()
            self.assertEqual(loaded.session_id, session.session_id)
            await self.send(loaded, self.row('after idle restart'))
            await until(loaded.idle)
            self.assertEqual(len(self.calls('session/load')), 1)
            self.assertIsNone(loaded.protocol.failure)

    async def test_two_messages_one_host(self):
        session = await self.start()
        for text in ('one', 'two'):
            await self.send(session, self.row(text))
            await until(session.idle)
        self.assertEqual(len(self.calls('initialize')), 1)
        self.assertEqual(len(self.calls('session/prompt')), 2)
        self.assertIsNone(session.process.returncode)
        self.assertEqual(self.store.get(self.session_key + '_opencode'),
                         {'account': 'one', 'session': session.session_id, 'usd': 0.12})

    async def test_batch_reattach_preserves_each_owner_and_completes_one_prompt(self):
        release = self.root / 'release'
        self.scenario(release=str(release), tool_calls=[
            {'sessionUpdate': 'tool_call', 'toolCallId': 'batch-call',
             'title': 'torii_workers_spawn', 'status': 'pending'}])
        session = await self.start()
        rows = [self.row('first ask'), self.row('second ask')]
        ids = {row['id'] for row in rows}
        anchor = rows[-1]['id']
        self.assertTrue(self.store.message_batch_sending([anchor, rows[0]['id']]))
        service = Service(self.store, None, self.runner, self.cwd)
        prompts = [await service._prepare_message(row, session) for row in reversed(rows)]
        self.assertEqual(await session.send(message_uuid(anchor), '\n\n'.join(prompts), row_id=anchor), 'received')
        self.store.messages_delivered(ids, 'written')
        await until(lambda: all(self.persisted(row)['reaction_desired'] == '👀' for row in rows))
        self.assertEqual(opencode_runtime.parent_receipts(self.data, session.session_id, ids), ids)
        await session.detach()
        self.parents.remove(session)
        attached = await self.start()
        self.assertEqual(attached.host_id, session.host_id)
        self.assertEqual(self.store.get(self.session_key + '_turn'), sorted(ids))
        release.touch()
        await until(attached.idle)
        self.assertEqual([(self.persisted(row)['reaction_desired'], self.persisted(row)['turn_started'])
                          for row in rows], [(None, None), (None, None)])
        self.assertEqual({self.persisted(row)['delivered'] for row in rows}, {'received'})
        self.assertEqual(len(self.calls('session/prompt')), 1)

    async def test_batch_queued_before_restart_requeues_every_unwritten_member(self):
        release = self.root / 'release'
        self.scenario(release=str(release))
        session = await self.start()
        held = self.row('held')
        await self.send(session, held)
        await until(lambda: len(self.calls('session/prompt')) == 1)
        rows = [self.row('queued first'), self.row('queued second')]
        ids = {row['id'] for row in rows}
        anchor = rows[-1]['id']
        self.assertTrue(self.store.message_batch_sending([anchor, rows[0]['id']]))
        service = Service(self.store, None, self.runner, self.cwd)
        prompts = [await service._prepare_message(row, session) for row in reversed(rows)]
        self.assertEqual(await session.send(message_uuid(anchor), '\n\n'.join(prompts), row_id=anchor), 'received')
        self.store.messages_delivered(ids, 'written')
        await session.detach()
        self.parents.remove(session)
        attached = await self.start()
        self.assertEqual({self.persisted(row)['delivered'] for row in rows}, {'pending'})
        self.assertEqual(self.store.message_batch_rows(anchor), ids)
        self.assertEqual(self.store.get(self.session_key + '_turn'), [held['id']])
        release.touch()
        await until(attached.idle)
        self.assertEqual(len(self.calls('session/prompt')), 1)

    async def test_reattach_after_service_restart_keeps_host_and_sends_nothing_twice(self):
        release = self.root / 'release'
        self.scenario(release=str(release))
        session = await self.start()
        first, queued = self.row('first held'), self.row('queued')
        await self.send(session, first)
        await until(lambda: len(self.calls('session/prompt')) == 1)
        await self.send(session, queued)
        self.assertEqual(self.store.get(self.session_key + '_turn'), [first['id'], queued['id']])
        await session.detach()
        self.parents.remove(session)
        attached = await self.start()
        self.assertEqual(attached.host_id, session.host_id)
        self.assertEqual(self.persisted(first)['delivered'], 'received')
        self.assertEqual(self.persisted(queued)['delivered'], 'pending')
        self.assertEqual(len(self.calls('session/prompt')), 1)
        release.touch()
        await until(attached.idle)
        await self.send(attached, queued)
        await until(attached.idle)
        prompts = [row['message']['params']['prompt'][0]['text'] for row in self.calls('session/prompt')]
        self.assertEqual(len(prompts), 2)
        self.assertEqual(sum('first held' in text for text in prompts), 1)
        self.assertEqual(sum('queued' in text for text in prompts), 1)
        self.assertEqual(len(self.calls('initialize')), 1)

    async def test_host_death_settles_seen_rows_as_lost_turn_and_requeues_others(self):
        self.scenario(release=str(self.root / 'release'))
        session = await self.start()
        first, queued = self.row('stored before model'), self.row('never stored')
        await self.send(session, first)
        await until(lambda: opencode_runtime.parent_receipts(self.data, session.session_id, [first['id']]) == {first['id']})
        await self.send(session, queued)
        await session.process.stop()
        await session.wait_closed()
        callbacks = [row for row in self.store.messages_pending() if row['kind'] == 'callback']
        self.assertEqual(len(callbacks), 1)
        self.assertIn('message=%d' % first['id'], callbacks[0]['text'])
        self.assertNotIn('message=%d' % queued['id'], callbacks[0]['text'])
        self.assertEqual(self.persisted(first)['delivered'], 'received')
        self.assertEqual(self.persisted(queued)['delivered'], 'pending')
        self.assertEqual(self.store.get(self.session_key + '_turn'), [])

    async def test_idle_wind_down_then_load_same_session(self):
        session = await self.start()
        await self.send(session, self.row('before idle'))
        await until(session.idle)
        await session.stop()
        self.assertEqual(session.process.returncode, 0)
        self.scenario(no_cost=True)
        loaded = await self.start()
        self.assertEqual(loaded.session_id, session.session_id)
        self.assertNotEqual(loaded.host_id, session.host_id)
        self.assertEqual(len(self.calls('session/load')), 1)
        self.assertEqual(self.store.get(self.session_key + '_opencode')['usd'], 0.12)
        await self.send(loaded, self.row('after idle'))
        await until(loaded.idle)
        self.assertEqual(self.store.get(self.session_key + '_opencode')['usd'], 0.12)
        spec = json.loads((loaded.process.directory / 'spec.json').read_text())
        self.assertIn('opencode-' + session.session_id, spec['locks'])

    async def test_stdin_close_during_turn_exits_and_settles(self):
        self.scenario(release=str(self.root / 'release'))
        session = await self.start()
        row = self.row('held')
        await self.send(session, row)
        await until(lambda: opencode_runtime.parent_receipts(self.data, session.session_id, [row['id']]) == {row['id']})
        await session.stop()
        self.assertEqual(session.process.returncode, 0)
        self.assertTrue(session.closed)
        callbacks = [row for row in self.store.messages_pending() if row['kind'] == 'callback']
        self.assertEqual(len(callbacks), 1)
        self.assertIn('message=%d' % row['id'], callbacks[0]['text'])

    async def test_account_change_starts_new_session_with_switch_notice(self):
        self.scenario(unique_sessions=True)
        session = await self.start()
        await self.send(session, self.row('one'))
        await until(session.idle)
        self.store.put('opencode_active_account', 'two')
        self.assertTrue(parent_state.selection_changed(session))
        self.assertEqual(await session.send('next', '[topic=chat] next'), 'closed')
        await session.wait_closed()
        changed = await self.start()
        self.assertNotEqual(changed.session_id, session.session_id)
        self.assertEqual((changed.account_alias, changed.model), ('two', 'other'))
        self.assertEqual(changed.switch, {'from': 'opencode', 'to': 'opencode',
                                         'reason': 'the OpenCode account changed to two'})
        self.assertTrue(changed.switched)
        self.assertEqual(self.calls('session/load'), [])
        self.assertEqual(self.store.get(self.session_key + '_opencode')['account'], 'two')

    async def test_outdated_fingerprint_relaunches_when_idle(self):
        release = self.root / 'release'
        self.scenario(release=str(release))
        session = await self.start()
        row = self.row('held')
        await self.send(session, row)
        await until(lambda: len(self.calls('session/prompt')) == 1)
        await session.detach()
        self.parents.remove(session)
        changed = await self.start(instructions='NEW_COORDINATOR_POLICY')
        self.assertEqual(changed.host_id, session.host_id)
        self.assertTrue(changed.outdated)
        self.assertFalse(changed.rejected)
        release.touch()
        await changed.wait_closed()
        self.assertTrue(changed.rejected)
        loaded = await self.start(instructions='NEW_COORDINATOR_POLICY')
        self.assertNotEqual(loaded.host_id, session.host_id)
        self.assertEqual(loaded.session_id, session.session_id)
        self.assertIn('NEW_COORDINATOR_POLICY', (self.data / 'instructions.md').read_text())
        self.assertFalse(loaded.outdated)

    async def test_prompt_never_starts_with_slash(self):
        session = await self.start()
        for row in (self.row('/compact'), self.row('The service restarted.', 'restarted')):
            prompt = await self.send(session, row)
            self.assertTrue(prompt.startswith('[topic='))
            await until(session.idle)
        with self.assertRaises(ValueError):
            await session.send('raw', '  /x')
        self.assertEqual(len(self.calls('session/prompt')), 2)
        self.assertTrue(all(row['message']['params']['prompt'][0]['text'].startswith('[topic=')
                            for row in self.calls('session/prompt')))

    async def test_tool_calls_set_reactions(self):
        for kind, status in (('tool_call', 'pending'), ('tool_call_update', 'in_progress')):
            with self.subTest(update=kind):
                release = self.root / ('release-' + kind)
                self.scenario(release=str(release), tool_calls=[
                    {'sessionUpdate': kind, 'toolCallId': 'call', 'title': 'torii_workers_spawn', 'status': status},
                    {'sessionUpdate': 'tool_call_update', 'toolCallId': 'call', 'status': 'completed'}])
                session = await self.start()
                row = self.row()
                await self.send(session, row)
                await until(lambda: self.persisted(row)['reaction_desired'] == '👀')
                self.assertEqual(self.persisted(row)['turn_eyes'], 1)
                release.touch()
                await until(session.idle)
                self.assertIsNone(self.persisted(row)['reaction_desired'])
                self.assertIsNone(self.persisted(row)['turn_started'])
                await session.stop()

    async def test_receipts_filter_session_role_and_part_type(self):
        session = await self.start()
        wanted = [self.row(str(index)) for index in range(5)]
        database = self.data / 'data/opencode/opencode.db'
        database.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(database) as db:
            db.execute('CREATE TABLE message (id TEXT, session_id TEXT, time_created INTEGER, data TEXT)')
            db.execute('CREATE TABLE part (id TEXT, message_id TEXT, session_id TEXT, data TEXT)')
            for index, (sid, role, kind, joined) in enumerate((
                    (session.session_id, 'user', 'text', True), ('ses_' + 'z' * 26, 'user', 'text', True),
                    (session.session_id, 'assistant', 'text', True), (session.session_id, 'user', 'tool', True),
                    (session.session_id, 'user', 'text', False))):
                mid = str(index)
                db.execute('INSERT INTO message VALUES (?,?,1,?)', (mid, sid, json.dumps({'role': role})))
                db.execute('INSERT INTO part VALUES (?,?,?,?)', (mid, mid if joined else 'missing', sid,
                    json.dumps({'type': kind, 'text': 'message=%d kind=owner' % wanted[index]['id']})))
        self.assertEqual(opencode_runtime.parent_receipts(self.data, session.session_id, [row['id'] for row in wanted]),
                         {wanted[0]['id']})

    async def test_home_chat_refused_and_invalid_native_id_refused(self):
        with self.assertRaisesRegex(ValueError, 'home chat'):
            await OpenCodeParentSession.start(self.store, self.runner, self.cwd, None, 'policy', topic='home')
        self.store.put(self.session_key + '_session', 'invalid')
        with self.assertRaisesRegex(ValueError, 'session ID'):
            await OpenCodeParentSession.start(self.store, self.runner, self.cwd, None, 'policy', topic='chat')
        self.assertEqual(self.calls(), [])

    async def test_reattach_missing_marker_uses_fresh_host_without_waiting_again(self):
        release = self.root / 'release'
        self.scenario(release=str(release))
        session = await self.start()
        row, queued = self.row('stored'), self.row('queued')
        await self.send(session, row)
        await until(lambda: opencode_runtime.parent_receipts(self.data, session.session_id, [row['id']]) == {row['id']})
        await self.send(session, queued)
        servers = self.calls('session/new')[0]['message']['params']['mcpServers']
        marker = opencode_parent.ready_path(servers)
        marker.rename(marker.with_suffix('.previous'))
        await session.detach()
        self.parents.remove(session)
        loaded = await self.start()
        self.assertNotEqual(loaded.host_id, session.host_id)
        self.assertEqual(loaded.session_id, session.session_id)
        self.assertEqual(self.persisted(queued)['delivered'], 'pending')
        self.assertEqual(len([item for item in self.store.messages_pending() if item['kind'] == 'callback']), 1)
        self.assertEqual(len(self.calls('session/prompt')), 1)
        self.assertEqual(len(self.calls('session/load')), 1)

    async def test_saved_cost_survives_load_and_zero_update_replaces_it(self):
        session = await self.start()
        await self.send(session, self.row())
        await until(session.idle)
        await session.stop()
        self.scenario(cost=0)
        loaded = await self.start()
        self.assertEqual(self.store.get(self.session_key + '_opencode')['usd'], 0.12)
        await self.send(loaded, self.row())
        await until(loaded.idle)
        self.assertEqual(self.store.get(self.session_key + '_opencode')['usd'], 0)

    async def test_unknown_cost_stays_unknown_across_load(self):
        self.scenario(no_cost=True)
        session = await self.start()
        self.assertEqual(self.store.get(self.session_key + '_opencode'),
                         {'account': 'one', 'session': session.session_id, 'usd': None})
        await self.send(session, self.row())
        await until(session.idle)
        await session.stop()
        loaded = await self.start()
        self.assertEqual(loaded.session_id, session.session_id)
        self.assertEqual(self.store.get(self.session_key + '_opencode'),
                         {'account': 'one', 'session': session.session_id, 'usd': None})

    async def test_guard_and_permission_requests_fail_closed(self):
        self.scenario(guard_unavailable=True)
        with self.assertRaisesRegex(RuntimeError, 'provider_guard_unavailable'):
            await self.start()
        self.assertEqual(self.calls('session/new'), [])
        self.scenario(permission=True)
        session = await self.start()
        await self.send(session, self.row())
        await until(session.idle)
        reply = next(row['message'] for row in self.calls() if row['message'].get('id') == 'permission-1')
        self.assertEqual(reply['result'], {'outcome': {'outcome': 'selected', 'optionId': 'deny-this'}})
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM problems WHERE code='opencode-permission-rejected'").fetchone()[0], 1)

    async def test_fingerprint_stable_across_launch_nonce_and_disabled_topic_reattach(self):
        session = await self.start()
        digest = self.store.get(self.session_key + '_host')['fingerprint']
        await session.stop()
        loaded = await self.start()
        self.assertEqual(self.store.get(self.session_key + '_host')['fingerprint'], digest)
        await loaded.detach()
        self.parents.remove(loaded)
        self.store.db.execute("UPDATE topics SET enabled=0 WHERE id='chat'")
        self.store.db.commit()
        attached = await self.start()
        self.assertEqual(attached.host_id, loaded.host_id)
        self.assertEqual(len(self.calls('initialize')), 2)
        self.assertIsNone(attached.protocol.background)

    async def test_settle_previous_uses_opencode_receipts_and_clears_cost_on_switch(self):
        from types import SimpleNamespace
        from unittest.mock import AsyncMock
        session = await self.start()
        row = self.row('settle')
        await self.send(session, row)
        await until(session.idle)
        await session.stop()
        self.store.put(self.session_key + '_lost', [row['id']])
        self.store.put(self.session_key + '_selection', {'provider': 'codex', 'model': 'test', 'effort': None, 'revision': 2})
        with patch('coordinator.parent.choose_parent', AsyncMock(return_value=('codex', 'test'))), \
                patch('coordinator.codex_session.CodexCoordinatorSession.start', AsyncMock(return_value=SimpleNamespace(host_id='new'))):
            switched = await parent.start(self.store, self.runner, self.cwd, None, 'policy', topic='chat')
        self.assertTrue(switched.switched)
        self.assertIsNone(self.store.get(self.session_key + '_opencode'))
        self.assertIsNone(self.store.get(self.session_key + '_session'))
        self.assertEqual(self.persisted(row)['delivered'], 'pending')


class OpenCodeRestartReceiptTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.chat = OpenCodeParentTests()
        self.chat.addCleanup = self.addCleanup
        self.chat.setUp()
        self.service = Service(self.chat.store, FakeTelegram(), self.chat.runner, self.chat.cwd)

    async def asyncTearDown(self):
        await self.chat.asyncTearDown()

    async def start(self):
        session = await self.service.start_session('chat')
        self.assertIsNotNone(session)
        self.chat.parents.append(session)
        return session

    async def assert_busy_native_batch_boundaries(self, through_service, switch_off=False):
        from collections import Counter
        from coordinator import opencode_recovery
        import re
        release = self.chat.root / 'batch-boundaries-release'
        self.chat.scenario(release=str(release))
        with self.chat.store.db:
            self.chat.store.put('owner', 1)
            self.chat.store.put('group', 1)
            self.chat.store.put('mode', 'group')
            self.chat.store.put('coordinator_intake_batching', True)
        session = await self.start()
        held = self.chat.row('held request')
        await self.service.feed_once()
        await asyncio.gather(*set(self.service.feed_tasks.values()))
        await until(lambda: session.busy and len(self.chat.calls('session/prompt')) == 1)
        workers = [self.chat.store.message_save('chat', 'worker_result', json.dumps(
                   {'worker': number + 1, 'task': number + 1, 'success': number not in (4, 12),
                    'text': 'result-%d' % number})) for number in range(30)]
        owner = self.chat.row('Answer this request.')
        urgent = [workers[number]['id'] for number in (4, 12)]
        expected = [owner['id']] + urgent + [row['id'] for row in workers if row['id'] not in urgent]
        try:
            if through_service:
                for pending in (11, 0):
                    self.assertTrue(await self.service.feed_once())
                    await asyncio.gather(*set(self.service.feed_tasks.values()))
                    self.assertEqual(len(self.chat.store.messages_pending()), pending)
            else:
                by_id = {row['id']: row for row in workers + [owner]}
                for message_ids in (expected[:20], expected[20:]):
                    self.assertTrue(self.chat.store.message_batch_sending(message_ids))
                if switch_off:
                    self.chat.store.put('coordinator_intake_batching', False)
                for message_ids in (expected[:20], expected[20:]):
                    prompts = [await self.service._prepare_message(by_id[message], session, batch=True)
                               for message in message_ids]
                    receipt = await session.send(message_uuid(message_ids[0]), '\n\n'.join(prompts),
                                                 row_id=message_ids[0])
                    self.assertEqual(receipt, 'received')
                    self.chat.store.messages_delivered(message_ids, 'written')
            self.assertEqual(len(self.chat.calls('session/prompt')), 1)
            self.assertEqual(self.chat.store.message_batch_rows(owner['id']), set(expected[:20]))
            self.assertEqual(self.chat.store.message_batch_rows(expected[20]), set(expected[20:]))
        finally:
            release.touch()
        await until(session.idle)
        calls = [row['message'] for row in self.chat.calls('session/prompt')]
        self.assertEqual(len(calls), 3)
        prompts = [row['params']['prompt'][0]['text'] for row in calls[1:]]
        batches = [[int(row) for row in re.findall(r'\bmessage=(\d+) kind=', text)] for text in prompts]
        self.assertEqual(batches, [expected[:20], expected[20:]])
        self.assertEqual(Counter(message for batch in batches for message in batch), Counter(expected))
        self.assertTrue(all(len(text) <= 8000 for text in prompts))
        self.assertEqual([row['params']['_meta']['toriiSteers'] for row in calls[1:]],
                         [[str(owner['id'])], [str(expected[20])]])
        reply = self.chat.store.db.execute('''SELECT r.turn,o.reply_to FROM parent_reply_owners p
            JOIN parent_reply_turns r ON r.host=p.host AND r.turn=p.turn
            JOIN outbox o ON o.id=r.outbox WHERE p.message=?''', (owner['id'],)).fetchall()
        self.assertEqual([tuple(row) for row in reply], [(str(calls[1]['id']), owner['id'] + 100)])
        self.assertEqual([tuple(row) for row in self.chat.store.db.execute(
            'SELECT checked,outbox IS NOT NULL FROM parent_reply_turns WHERE host=?', (session.host_id,))],
                         [(1, 1)] * 3)
        while await self.service.deliver_once():
            pass
        self.assertEqual(sum(row['reply_to'] == owner['id'] + 100 for row in self.service.telegram.sent), 1)
        self.assertEqual(opencode_runtime.parent_receipts(self.chat.data, session.session_id, expected), set(expected))
        opencode_recovery.confirm(session)
        self.assertEqual({tuple(row) for row in self.chat.store.db.execute(
            'SELECT delivered,receipt FROM messages')}, {('received', 'transcript')})
        self.assertEqual(self.chat.store.messages_pending(), [])
        self.assertIsNone(self.chat.persisted(held)['turn_started'])
        print('busy native OpenCode: capped prompts=20,11; owner answered next; 31 receipts once')

    async def test_busy_native_batches_keep_each_capped_input_in_its_own_prompt(self):
        await self.assert_busy_native_batch_boundaries(True)

    async def test_saved_batches_stay_separate_when_queued_directly_on_busy_parent(self):
        await self.assert_busy_native_batch_boundaries(False)

    async def test_saved_batches_stay_separate_after_batching_is_switched_off(self):
        await self.assert_busy_native_batch_boundaries(False, switch_off=True)

    async def test_switch_off_unbatched_inputs_keep_native_queue_coalescing(self):
        import re
        release = self.chat.root / 'legacy-coalescing-release'
        self.chat.scenario(release=str(release))
        with self.chat.store.db:
            self.chat.store.put('owner', 1)
            self.chat.store.put('group', 1)
            self.chat.store.put('mode', 'group')
            self.chat.store.put('coordinator_intake_batching', False)
        session = await self.start()
        held = self.chat.row('held request')
        await self.service.feed_once()
        await asyncio.gather(*set(self.service.feed_tasks.values()))
        await until(lambda: session.busy and len(self.chat.calls('session/prompt')) == 1)
        rows = [self.chat.row('first queued result', kind='worker_result'),
                self.chat.row('second queued result', kind='worker_result')]
        try:
            self.assertTrue(await self.service.feed_once())
            await asyncio.gather(*set(self.service.feed_tasks.values()))
            self.assertEqual(len(self.chat.calls('session/prompt')), 1)
            self.assertEqual([self.chat.persisted(row)['batch_id'] for row in rows], [None, None])
        finally:
            release.touch()
        await until(session.idle)
        calls = [row['message'] for row in self.chat.calls('session/prompt')]
        self.assertEqual(len(calls), 2)
        prompt = calls[1]['params']['prompt'][0]['text']
        self.assertEqual([int(row) for row in re.findall(r'\bmessage=(\d+) kind=', prompt)],
                         [row['id'] for row in rows])
        self.assertEqual(calls[1]['params']['_meta']['toriiSteers'], [str(row['id']) for row in rows])
        self.assertEqual(opencode_runtime.parent_receipts(self.chat.data, session.session_id,
                         [row['id'] for row in rows + [held]]), {row['id'] for row in rows + [held]})

    async def test_native_flood_answers_owner_next_prompt_and_receipts_every_row_once(self):
        from coordinator import opencode_recovery
        import re
        release = self.chat.root / 'flood-release'
        self.chat.scenario(release=str(release))
        with self.chat.store.db:
            self.chat.store.put('owner', 1)
            self.chat.store.put('group', 1)
            self.chat.store.put('mode', 'group')
            self.chat.store.put('coordinator_intake_batching', True)
        session = await self.start()
        held = self.chat.row('held request')
        await self.service.feed_once()
        await asyncio.gather(*set(self.service.feed_tasks.values()))
        await until(lambda: session.busy and len(self.chat.calls('session/prompt')) == 1)
        workers = [self.chat.store.message_save('chat', 'worker_result' if number % 2 == 0 else 'event',
                   json.dumps({'worker': number + 1, 'task': number + 1, 'success': number not in (4, 12),
                               'text': 'result-%d' % number})) for number in range(30)]
        owner = self.chat.row('Answer this request.')
        urgent = [workers[number]['id'] for number in (4, 12)]
        expected = [owner['id']] + urgent + [row['id'] for row in workers if row['id'] not in urgent]
        try:
            self.assertTrue(await self.service.feed_once())
            await asyncio.gather(*set(self.service.feed_tasks.values()))
            self.assertEqual(len(self.chat.calls('session/prompt')), 1)
            self.assertEqual(len(self.chat.store.messages_pending()), 11)
            self.assertEqual(self.chat.store.message_batch_rows(owner['id']), set(expected[:20]))
        finally:
            release.touch()
        await until(lambda: session.idle() and len(self.chat.calls('session/prompt')) == 2)
        first = self.chat.calls('session/prompt')[1]['message']
        first_text = first['params']['prompt'][0]['text']
        self.assertEqual([int(row) for row in re.findall(r'\bmessage=(\d+) kind=', first_text)], expected[:20])
        self.assertEqual(first['params']['_meta']['toriiSteers'], [str(owner['id'])])
        reply = self.chat.store.db.execute('''SELECT r.turn,o.reply_to FROM parent_reply_owners p
            JOIN parent_reply_turns r ON r.host=p.host AND r.turn=p.turn
            JOIN outbox o ON o.id=r.outbox WHERE p.message=?''', (owner['id'],)).fetchone()
        self.assertEqual(tuple(reply), (str(first['id']), owner['id'] + 100))
        while await self.service.deliver_once():
            pass
        self.assertTrue(any(row['reply_to'] == owner['id'] + 100 for row in self.service.telegram.sent))
        state = self.chat.persisted(owner)
        self.assertEqual((state['reaction_desired'], state['turn_started'], state['turn_eyes']), ('👌', None, 0))
        self.assertEqual(len(self.chat.store.messages_pending()), 11)
        self.assertTrue(await self.service.feed_once())
        await asyncio.gather(*set(self.service.feed_tasks.values()))
        await until(lambda: session.idle() and len(self.chat.calls('session/prompt')) == 3)
        prompts = [row['message']['params']['prompt'][0]['text'] for row in self.chat.calls('session/prompt')[1:]]
        batches = [[int(row) for row in re.findall(r'\bmessage=(\d+) kind=', text)] for text in prompts]
        self.assertEqual(batches, [expected[:20], expected[20:]])
        self.assertEqual(sorted(row for batch in batches for row in batch), sorted(expected))
        self.assertTrue(all(len(text) <= 8000 for text in prompts))
        self.assertEqual(opencode_runtime.parent_receipts(self.chat.data, session.session_id, expected), set(expected))
        opencode_recovery.confirm(session)
        self.assertEqual({tuple(row) for row in self.chat.store.db.execute(
            'SELECT delivered,receipt FROM messages')}, {('received', 'transcript')})
        self.assertEqual(self.chat.store.messages_pending(), [])
        self.assertIsNone(self.chat.persisted(held)['turn_started'])
        for row in workers + [owner]:
            prompt = next(text for text in prompts if 'message=%d kind=' % row['id'] in text)
            self.assertIn(row['text'], prompt)
        print('native opencode flood: owner answered in next prompt; batches=20,11; 31 row bodies received once')

    async def completed_message(self):
        session = await self.start()
        row = self.chat.row('Owner request already answered')
        prompt = await self.service._prepare_message(row, session)
        self.assertEqual(await self.service._send_message(row, prompt, session), 'received')
        await until(session.idle)
        self.assertEqual((self.chat.persisted(row)['delivered'], self.chat.persisted(row)['receipt']),
                         ('received', 'written'))
        return session, row

    async def restart(self, session, reattach):
        if reattach:
            await session.detach()
        else:
            await session.stop()
        self.chat.parents.remove(session)
        self.chat.store.close()
        self.chat.store = Store(self.chat.root / 'state')
        self.addCleanup(self.chat.store.close)
        self.chat.runner = ProviderRunner(self.chat.store.directory)
        self.chat.store.messages_uncertain()
        self.service = Service(self.chat.store, FakeTelegram(), self.chat.runner, self.chat.cwd)

    def restart_prompt(self):
        row = self.chat.store.db.execute('SELECT text FROM messages WHERE id=?',
                                         (self.service.restart_messages['chat'],)).fetchone()
        return row['text']

    async def completed_message_after_restart(self, reattach):
        session, row = await self.completed_message()
        self.assertEqual(opencode_runtime.parent_receipts(self.chat.data, session.session_id, [row['id']]),
                         {row['id']})
        await self.restart(session, reattach)
        self.assertEqual((self.chat.persisted(row)['delivered'], self.chat.persisted(row)['receipt']),
                         ('uncertain', 'service_restarted'))
        resumed = await self.start()
        self.assertEqual(resumed.session_id, session.session_id)
        self.assertEqual(resumed.host_id == session.host_id, reattach)
        self.assertEqual(len(self.chat.calls('session/load')), 0 if reattach else 1)
        self.assertEqual(self.chat.persisted(row)['delivered'], 'received')
        self.assertEqual(self.chat.persisted(row)['receipt'], 'transcript')
        self.assertIn('"count":0', self.restart_prompt())
        self.assertNotIn(row['text'], self.restart_prompt())
        self.assertEqual(len(self.chat.calls('session/prompt')), 1)

    async def test_completed_message_is_confirmed_after_reattach(self):
        await self.completed_message_after_restart(True)

    async def test_completed_message_is_confirmed_after_relaunch(self):
        await self.completed_message_after_restart(False)

    async def unrecorded_message_after_restart(self, reattach):
        session, _ = await self.completed_message()
        missing = self.chat.row('Owner request never sent to OpenCode')
        self.chat.store.message_sending(missing['id'])
        self.chat.store.message_delivered(missing['id'], 'written')
        await self.restart(session, reattach)
        resumed = await self.start()
        self.assertEqual(resumed.host_id == session.host_id, reattach)
        self.assertEqual(self.chat.persisted(missing)['delivered'], 'uncertain')
        self.assertEqual(self.chat.persisted(missing)['receipt'], 'service_restarted')
        self.assertIn(missing['text'], self.restart_prompt())
        self.assertEqual(len(self.chat.calls('session/prompt')), 1)

    async def test_unrecorded_message_stays_uncertain_after_reattach(self):
        await self.unrecorded_message_after_restart(True)

    async def test_unrecorded_message_stays_uncertain_after_relaunch(self):
        await self.unrecorded_message_after_restart(False)

    async def test_message_left_uncertain_by_an_earlier_restart_heals(self):
        session, row = await self.completed_message()
        self.chat.store.messages_uncertain()
        self.assertEqual(self.chat.persisted(row)['receipt'], 'service_restarted')
        await self.restart(session, True)
        await self.start()
        self.assertEqual((self.chat.persisted(row)['delivered'], self.chat.persisted(row)['receipt']),
                         ('received', 'transcript'))
        self.assertNotIn(row['text'], self.restart_prompt())

    async def test_account_change_does_not_confirm_the_previous_sessions_messages(self):
        self.chat.scenario(unique_sessions=True)
        session, row = await self.completed_message()
        await self.restart(session, False)
        self.chat.store.put('opencode_active_account', 'two')
        changed = await self.start()
        self.assertTrue(changed.switched)
        self.assertNotEqual(changed.session_id, session.session_id)
        self.assertEqual(opencode_runtime.parent_receipts(self.chat.data, changed.session_id, [row['id']]), set())
        self.assertEqual(opencode_runtime.parent_receipts(self.chat.data, session.session_id, [row['id']]), {row['id']})
        self.assertEqual(self.chat.persisted(row)['delivered'], 'uncertain')
        self.assertEqual(self.chat.persisted(row)['receipt'], 'service_restarted')
        self.assertIn('This chat moved from OpenCode to OpenCode', self.restart_prompt())
        self.assertIn(row['text'], self.restart_prompt())
