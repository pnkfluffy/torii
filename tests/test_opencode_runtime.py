import asyncio
from dataclasses import replace
import errno
import json
import os
import signal
import sqlite3
import shutil
import subprocess
from pathlib import Path
from types import SimpleNamespace
import tempfile
import threading
import time
import unittest
from unittest.mock import AsyncMock, patch

from coordinator import control_api, opencode_runtime, worker_storage
from coordinator.accounts import AccountBroker
from coordinator.native_protocol import RunControl
from coordinator.host import HostClient
from coordinator.service import Service
from coordinator.session import AccountsUnavailable
from coordinator.providers import ProviderRunner
from coordinator.store import Store
from coordinator.workers import WorkerPool, host_directory
from tests.fake_opencode import SESSION_ID, install_fake_opencode, install_versioned_opencode
from tests.support import read_jsonl, stop_test_hosts


ACCOUNT = {'base_url': 'http://127.0.0.1:1/v1', 'model': 'example-model', 'small_model': 'other-title-model',
           'context': 128000, 'output': 4096, 'price_in': 1, 'price_cached': 0.1, 'price_out': 2}
WEEKLY_REFUSAL = {'error': {'code': 'weekly_included_input_token_limit_exceeded', 'message':
    'Weekly included input token limit exceeded. Enable paid balance use after limits in your subscription settings '
    'at the host subscription page to continue using this model from your balance, use a paid model, '
    'or wait for the weekly reset.', 'type': 'rate_limit_error', 'status': 429},
    'resetAt': int(time.time() + 86400) * 1000}


class OpenCodeRuntimeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        platform = patch('coordinator.providers.platform', 'linux')
        platform.start()
        self.addCleanup(platform.stop)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.addCleanup(stop_test_hosts, self.root)
        self.home = self.root / 'owner-home'
        self.home.mkdir()
        self.managed_paths = (self.home / 'managed/user.plist', self.home / 'managed/system.plist')
        managed = patch('coordinator.opencode_runtime.managed_preference_paths',
                        return_value=self.managed_paths, create=True)
        managed.start()
        self.addCleanup(managed.stop)
        env = patch.dict(os.environ, {'HOME': str(self.home)})
        env.start()
        self.addCleanup(env.stop)
        self.key_home = self.root / 'dummy-account'
        self.key_home.mkdir()
        self.key = self.key_home / 'key'
        self.key.write_text('dummy-only\n')
        self.key.chmod(0o600)
        homes = patch('coordinator.opencode_accounts.account_home', return_value=self.key_home)
        homes.start()
        self.addCleanup(homes.stop)
        self.binary = install_fake_opencode(self.root / 'fake')
        self.store = Store(self.root / 'state')
        self.addCleanup(self.store.close)
        self.store.put('opencode_accounts', {'one': dict(ACCOUNT)})
        self.store.put('opencode_active_account', 'one')
        self.store.put('opencode_binary', str(self.binary))
        self.store.put('opencode_enabled', True)
        self.cwd = self.root / 'worktree'
        self.cwd.mkdir()
        (self.cwd / 'AGENTS.md').write_text('DUMMY_AGENT_MARKER\n')
        self.runner = ProviderRunner(self.store.directory, opencode_store=self.store)
        scratch = self.root / 'worker-temp'
        scratch.mkdir()
        temp_base = patch('coordinator.worker_storage.temp_base', return_value=scratch.resolve())
        temp_base.start()
        self.addCleanup(temp_base.stop)

    def scenario(self, **options):
        (self.binary.parent / 'scenario.json').write_text(json.dumps(options))

    def calls(self):
        path = self.binary.parent / 'calls.jsonl'
        return read_jsonl(path)

    async def run_worker(self, **options):
        return await asyncio.wait_for(self.runner.run('opencode', 'short prompt', self.cwd, None,
                                     fresh=True, worker_id=31, **options), 20)

    def make_pool(self):
        self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:2',1,2,'test',?,1)",
                              (str(self.cwd),))
        self.store.put('coordinator_home_topic', '1:2')
        task = self.store.task_create('1:2', 'Build', worktree=str(self.cwd))
        return WorkerPool(self.store, self.runner, AccountBroker(self.store)), task

    def spawn(self, task, **options):
        return control_api.call(self.store, 'workers.spawn', {'task': task['id'], 'provider': 'opencode',
                                'prompt': 'short prompt', **options}, topic='1:2')

    def shared_mcp(self, selection, entries):
        overrides = self.root / 'local-overrides.json'
        if selection is not None:
            overrides.write_text(json.dumps(selection))
        (self.home / '.claude.json').write_text(json.dumps({'mcpServers': entries}))
        selected = patch('coordinator.accounts.LOCAL_OVERRIDES_PATH', overrides)
        selected.start()
        self.addCleanup(selected.stop)

    def shared_problems(self):
        return [tuple(row) for row in self.store.db.execute(
            "SELECT area,code,detail FROM problems WHERE area='shared-mcp' ORDER BY id")]

    async def test_shared_mcp_two_servers_private_config_and_permissions_on_fresh_and_resume(self):
        entries = {
            'example-mcp': {'type': 'http', 'url': 'https://example.invalid/mcp',
                            'headers': {'Authorization': 'DUMMY-SHARED-AUTH', 'X-Workspace': 'DUMMY-WORKSPACE',
                                        'X-Client': 'DUMMY-CLIENT'}},
            'other.mcp': {'type': 'http', 'url': 'http://127.0.0.1:1/unavailable', 'headers': {}},
        }
        self.shared_mcp({'shared_mcp_servers': list(entries)}, dict(entries, unselected=entries['example-mcp']))
        expected = {name: {'type': 'remote', 'url': entry['url'], 'headers': entry['headers'], 'oauth': False}
                    for name, entry in entries.items()}
        for profile in (ACCOUNT, {'provider': 'opencode-go', 'model': 'example-model'}):
            self.store.put('opencode_accounts', {'one': profile})
            for fresh in (True, False):
                with self.subTest(provider=profile.get('provider', 'torii-host'), fresh=fresh):
                    result = await self.runner.run('opencode', 'short prompt', self.cwd,
                        None if fresh else SESSION_ID, fresh=fresh, worker_id=31, account_alias='one')
                    self.assertTrue(result.success, result)
                    path = self.launch_root(result) / 'config/opencode/opencode.json'
                    self.assertEqual(path.stat().st_mode & 0o777, 0o600)
                    self.assertEqual(path.parent.stat().st_mode & 0o777, 0o700)
                    config = json.loads(path.read_text())
                    self.assertEqual(config.get('$schema'), 'https://opencode.ai/config.json')
                    self.assertEqual(config['mcp'], expected)
                    self.assertEqual(self.calls()[-1]['config']['mcp'], expected)
                    self.assertEqual(config['permission']['example-mcp_*'], 'allow')
                    self.assertEqual(config['permission']['other_mcp_*'], 'allow')
                    self.assertEqual(config['permission']['external_directory'], 'deny')
        self.assertEqual(self.shared_problems(), [])

    async def test_shared_mcp_selection_and_entry_problems_once_per_launch(self):
        good = {'type': 'http', 'url': 'https://example.invalid/mcp', 'headers': {}}
        cases = (
            ('no-file', None, {}, {}, []),
            ('no-selection', {}, {}, {}, []),
            ('empty', {'shared_mcp_servers': []}, {}, {}, []),
            ('invalid-list', {'shared_mcp_servers': ['good', 3]}, {}, {},
             [('shared-mcp', 'selection-invalid', None)]),
            ('invalid-type', {'shared_mcp_servers': 'good'}, {}, {},
             [('shared-mcp', 'selection-invalid', None)]),
            ('partial', {'shared_mcp_servers': ['good', 'stdio', 'missing', 'stdio']},
             {'good': good, 'stdio': {'type': 'stdio', 'command': 'never-run'}},
             {'good': {'type': 'remote', 'url': good['url'], 'headers': {}, 'oauth': False}},
             [('shared-mcp', 'entry-invalid', 'entry=stdio'), ('shared-mcp', 'entry-missing', 'entry=missing')]),
            ('none-valid', {'shared_mcp_servers': ['stdio']},
             {'stdio': {'type': 'stdio', 'command': 'never-run'}}, {},
             [('shared-mcp', 'entry-invalid', 'entry=stdio')]),
        )
        for name, selection, entries, expected, problems in cases:
            with self.subTest(case=name):
                self.shared_mcp(selection, entries)
                before = len(self.shared_problems())
                result = await self.run_worker()
                self.assertTrue(result.success, result)
                config = json.loads((self.launch_root(result) / 'config/opencode/opencode.json').read_text())
                self.assertEqual(self.shared_problems()[before:], problems)
                self.assertEqual(config['mcp'], expected)

    async def test_shared_mcp_missing_and_invalid_source_keep_worker_available(self):
        self.shared_mcp({'shared_mcp_servers': ['example-mcp']}, {})
        source = self.home / '.claude.json'
        for mode, code in (('missing', 'source-missing'), ('json', 'source-invalid-json'),
                           ('directory', 'source-unreadable')):
            with self.subTest(mode=mode):
                if mode == 'missing':
                    source.rename(self.home / 'saved-source.json')
                elif mode == 'json':
                    source.write_text('{')
                else:
                    source.rename(self.home / 'invalid-source.json')
                    source.mkdir()
                before = len(self.shared_problems())
                result = await self.run_worker()
                self.assertTrue(result.success, result)
                self.assertEqual(self.shared_problems()[before:],
                                 [('shared-mcp', code, 'entries=example-mcp')])
                self.assertEqual(self.calls()[-1]['config']['mcp'], {})

    async def test_shared_mcp_output_and_errors_are_scrubbed_before_persistence(self):
        url, value = 'https://example.invalid/DUMMY-PRIVATE-URL', 'DUMMY-PRIVATE-HEADER'
        self.shared_mcp({'shared_mcp_servers': ['example-mcp']}, {
            'example-mcp': {'type': 'http', 'url': url, 'headers': {'Authorization': value}}})
        pool, task = self.make_pool()
        for status in (None, 401, 402):
            with self.subTest(status=status):
                failed = status is not None
                options = {'chunks': [url + ' ' + value], 'stderr': url + ' ' + value}
                if failed:
                    options.update(http_error=status, error_message=url + ' ' + value,
                                   response_body=url + ' ' + value)
                self.scenario(**options)
                before = self.store.db.execute('SELECT count(*) FROM problems').fetchone()[0]
                wid = self.spawn(task, prompt='failure prompt %d' % status if failed else 'success prompt').data['worker']
                done = await pool.run(pool.get(wid), self.store.topic('1:2'))
                self.assertEqual(done['status'], 'waiting_for_quota' if status == 402 else 'done')
                result = json.loads(done['result'])
                self.assertEqual(result['success'], not failed)
                host = Path(result['log_path']).parent
                captured = [done['result'], ''.join(self.store.db.iterdump())]
                captured.extend(path.read_text(errors='replace') for path in host.iterdir() if path.is_file())
                for secret in (url, value):
                    for text in captured:
                        self.assertNotIn(secret, text)
                self.assertIn('[secret example-mcp-', Path(result['log_path']).read_text())
                problems = self.store.db.execute('SELECT count(*) FROM problems').fetchone()[0]
                if status == 401:
                    self.assertGreater(problems, before)
                elif status == 402:
                    self.assertEqual(problems, before)
                    self.assertTrue(result['quota_limited'])

    async def test_shared_mcp_tools_pass_worker_guard(self):
        self.shared_mcp({'shared_mcp_servers': ['example-mcp']}, {
            'example-mcp': {'type': 'http', 'url': 'https://example.invalid/mcp', 'headers': {}}})
        result = await self.run_worker()
        self.assertTrue(result.success, result)
        root = self.launch_root(result)
        config = json.loads((root / 'config/opencode/opencode.json').read_text())
        self.assertEqual(config['permission'].get('example-mcp_*'), 'allow')
        guard = root / 'config/opencode/plugins/guard.mjs'
        guard.write_bytes((guard.parent / 'guard.js').read_bytes())
        driver = '''
import { pathToFileURL } from 'node:url'
const hooks = await (await import(pathToFileURL(process.argv[1]).href)).default({ client: {} })
const input = { tool: 'example-mcp_search', sessionID: 'dummy-session', callID: 'dummy-call' }
const output = { output: 'DUMMY-MCP-RESULT', metadata: {} }
await hooks['tool.execute.before']?.(input, { args: {} })
await hooks['tool.execute.after']?.(input, output)
console.log(JSON.stringify(output))
'''
        observed = subprocess.run(['node', '--input-type=module', '-e', driver, str(guard)],
                                  capture_output=True, text=True, timeout=20, check=True)
        self.assertEqual(json.loads(observed.stdout)['output'], 'DUMMY-MCP-RESULT')

    async def test_shared_mcp_reattach_scrubs_saved_launch_values_without_reading_selection(self):
        url, value = 'https://example.invalid/DUMMY-SAVED-URL', 'DUMMY-SAVED-HEADER'
        self.shared_mcp({'shared_mcp_servers': ['example-mcp']}, {
            'example-mcp': {'type': 'http', 'url': url, 'headers': {'Authorization': value}}})
        release = self.root / 'release'
        self.scenario(release=str(release), chunks=[url + ' ' + value])
        pool, task = self.make_pool()
        wid = self.spawn(task).data['worker']
        run = asyncio.create_task(pool.run(pool.get(wid), self.store.topic('1:2')))
        self.addAsyncCleanup(self.settle_run, run)
        await self.wait_prompt()
        run.cancel()
        await asyncio.gather(run, return_exceptions=True)
        host = host_directory(self.store.directory, pool.get(wid))
        spec = json.loads((host / 'spec.json').read_text())
        config = Path(spec['env']['XDG_CONFIG_HOME']) / 'opencode' / 'opencode.json'
        saved = json.loads(config.read_text())
        saved.pop('$schema', None)
        config.write_text(json.dumps(saved))
        before = len(self.calls())
        (self.home / '.claude.json').write_text('{}')
        (self.root / 'local-overrides.json').write_text('{')
        replacement = WorkerPool(self.store, ProviderRunner(self.store.directory, opencode_store=self.store),
                                 AccountBroker(self.store))
        ready = asyncio.Event()
        original = HostClient.connect

        async def connected(client):
            await original(client)
            ready.set()

        with patch.object(HostClient, 'connect', connected):
            attached = asyncio.create_task(replacement.run(replacement.get(wid), self.store.topic('1:2')))
            self.addAsyncCleanup(self.settle_run, attached)
            await asyncio.wait_for(ready.wait(), 5)
            release.touch()
            done = await asyncio.wait_for(attached, 10)
        result = json.loads(done['result'])
        self.assertTrue(result['success'], result)
        self.assertEqual(len(self.calls()), before)
        self.assertEqual(self.shared_problems(), [])
        self.assertNotIn(url, done['result'])
        self.assertNotIn(value, done['result'])
        self.assertIn('[secret example-mcp-', result['text'])
        self.assertEqual(json.loads(config.read_text()), dict(saved, **{'$schema': 'https://opencode.ai/config.json'}))

    async def reattach_shared_mcp_after_config_error(self, damage):
        url, value = 'https://example.invalid/DUMMY-REATTACH-URL', 'DUMMY-REATTACH-HEADER'
        entry = {'type': 'http', 'url': url, 'headers': {'Authorization': value}}
        self.shared_mcp({'shared_mcp_servers': ['example-mcp']}, {'example-mcp': entry})
        release = self.root / 'release'
        self.scenario(release=str(release), chunks=[url + ' ' + value])
        pool, task = self.make_pool()
        wid = self.spawn(task).data['worker']
        run = asyncio.create_task(pool.run(pool.get(wid), self.store.topic('1:2')))
        self.addAsyncCleanup(self.settle_run, run)
        await self.wait_prompt()
        run.cancel()
        await asyncio.gather(run, return_exceptions=True)
        host = host_directory(self.store.directory, pool.get(wid))
        spec = json.loads((host / 'spec.json').read_text())
        root = Path(spec['env']['XDG_CONFIG_HOME']).parent
        config = root / 'config/opencode/opencode.json'
        launch = (root / 'guard-launch.json').read_bytes()
        if damage == 'missing':
            config.rename(config.with_suffix('.saved'))
        elif damage == 'unreadable':
            config.chmod(0)
            self.addCleanup(lambda: config.chmod(0o600))
        elif damage == 'encoding':
            config.write_bytes(b'\xff')
        elif damage == 'reserved':
            saved = json.loads(config.read_text())
            saved['mcp']['torii'] = {'type': 'remote', 'url': 'https://collision.invalid/mcp',
                                    'headers': {}, 'oauth': False}
            saved['permission']['torii_*'] = 'allow'
            config.write_text(json.dumps(saved))
        else:
            config.write_text(damage)
        self.shared_mcp({'shared_mcp_servers': ['example-mcp', 'other-mcp']}, {
            'example-mcp': entry,
            'other-mcp': {'type': 'http', 'url': 'https://other.invalid/mcp', 'headers': {}},
        })
        before = list(self.calls())
        replacement = WorkerPool(self.store, ProviderRunner(self.store.directory, opencode_store=self.store),
                                 AccountBroker(self.store))
        ready = asyncio.Event()
        original = HostClient.connect

        async def connected(client):
            await original(client)
            ready.set()

        with patch.object(HostClient, 'connect', connected):
            attached = asyncio.create_task(replacement.run(replacement.get(wid), self.store.topic('1:2')))
            self.addAsyncCleanup(self.settle_run, attached)
            waiter = asyncio.create_task(ready.wait())
            try:
                await asyncio.wait({attached, waiter}, timeout=10, return_when=asyncio.FIRST_COMPLETED)
            finally:
                waiter.cancel()
                await asyncio.gather(waiter, return_exceptions=True)
                release.touch()
            done = await asyncio.wait_for(attached, 10)
        result = json.loads(done['result'])
        self.assertTrue(result['success'], result)
        self.assertTrue(ready.is_set())
        self.assertTrue(config.is_file(), 'reattach did not restore opencode.json')
        restored = json.loads(config.read_text())
        self.assertEqual(restored.get('$schema'), 'https://opencode.ai/config.json')
        expected = {
            'example-mcp': {'type': 'remote', 'url': url, 'headers': {'Authorization': value}, 'oauth': False},
        }
        if damage != 'reserved':
            expected['other-mcp'] = {'type': 'remote', 'url': 'https://other.invalid/mcp', 'headers': {}, 'oauth': False}
            self.assertEqual(restored['permission']['other-mcp_*'], 'allow')
        self.assertEqual(restored['mcp'], expected)
        self.assertEqual(restored['permission']['example-mcp_*'], 'allow')
        self.assertNotIn('torii_*', restored['permission'])
        self.assertEqual(restored['permission']['external_directory'], 'deny')
        self.assertEqual(config.stat().st_mode & 0o777, 0o600)
        self.assertEqual((root / 'guard-launch.json').read_bytes(), launch)
        self.assertEqual(host_directory(self.store.directory, done), host)
        self.assertEqual(result['session_id'], SESSION_ID)
        self.assertEqual(self.calls(), before)
        self.assertEqual(self.shared_problems(), [('shared-mcp', 'entry-reserved', 'entry=torii')]
                         if damage == 'reserved' else [])
        self.assertNotIn(url, done['result'])
        self.assertNotIn(value, done['result'])
        self.assertIn('[secret example-mcp-', result['text'])

    async def test_shared_mcp_reattach_restores_missing_config(self):
        await self.reattach_shared_mcp_after_config_error('missing')

    async def test_shared_mcp_reattach_restores_invalid_json_config(self):
        await self.reattach_shared_mcp_after_config_error('{')

    async def test_shared_mcp_reattach_restores_unreadable_config(self):
        await self.reattach_shared_mcp_after_config_error('unreadable')

    async def test_shared_mcp_reattach_restores_invalid_encoding_config(self):
        await self.reattach_shared_mcp_after_config_error('encoding')

    async def test_shared_mcp_reattach_restores_invalid_config_shape(self):
        await self.reattach_shared_mcp_after_config_error('[]')

    async def test_shared_mcp_reattach_restores_missing_mcp_config(self):
        await self.reattach_shared_mcp_after_config_error('{}')

    async def test_shared_mcp_reattach_skips_saved_reserved_torii(self):
        await self.reattach_shared_mcp_after_config_error('reserved')

    async def test_shared_mcp_reserved_torii_is_skipped_on_fresh_and_resume(self):
        good = {'type': 'http', 'url': 'https://example.invalid/mcp', 'headers': {}}
        self.shared_mcp({'shared_mcp_servers': ['torii', 'example-mcp', 'torii']},
                        {'torii': dict(good, url='https://collision.invalid/mcp'), 'example-mcp': good})
        for fresh in (True, False):
            with self.subTest(fresh=fresh):
                before = len(self.shared_problems())
                result = await self.runner.run('opencode', 'short prompt', self.cwd,
                    None if fresh else SESSION_ID, fresh=fresh, worker_id=31, account_alias='one')
                self.assertTrue(result.success, result)
                self.assertEqual(self.calls()[-1]['config']['mcp'], {
                    'example-mcp': {'type': 'remote', 'url': good['url'], 'headers': {}, 'oauth': False},
                })
                self.assertEqual(self.shared_problems()[before:], [('shared-mcp', 'entry-reserved', 'entry=torii')])
                self.assertNotIn('torii_*', self.calls()[-1]['config']['permission'])

    async def test_fresh_result_config_environment_and_journals(self):
        forbidden = {prefix + 'LEAK': 'not-a-key' for prefix in ('OPENCODE_', 'OTEL_', 'OPENAI_', 'ANTHROPIC_', 'AWS_')}
        forbidden.update(GIT_CONFIG_GLOBAL='/owner/git', GH_CONFIG_DIR='/owner/gh',
                         NPM_CONFIG_OFFLINE='false', npm_config_offline='false')
        original_open = Path.open

        def guarded_open(path, *args, **kwargs):
            self.assertNotEqual(path, self.key, 'Torii opened the account key')
            return original_open(path, *args, **kwargs)

        with patch.dict(os.environ, forbidden), patch('pathlib.Path.open', guarded_open):
            result = await self.run_worker(effort='max')
            self.assertTrue(result.success, result)
        self.assertEqual((result.session_id, result.text), (SESSION_ID, 'fresh answer'))
        self.assertEqual(result.spend, {'input_tokens': 123, 'output_tokens': 45, 'cached_tokens': 67,
                                       'usd_estimate': 0.12, 'source': opencode_runtime.SOURCE})
        root = self.store.directory / 'opencode' / 'workers' / '31'
        host = Path(result.log_path).parent
        spec = json.loads((host / 'spec.json').read_text())
        self.assertEqual(spec['account'], 'one')
        self.assertEqual(spec['argv'], [str(self.binary), 'acp', '--cwd', str(self.cwd)])
        await self.cleanup_storage(result)
        env = spec['env']
        self.assertEqual({name: env[name] for name in env if name.startswith('OPENCODE_') and name != 'OPENCODE_TEST_MANAGED_CONFIG_DIR'}, opencode_runtime.FLAGS)
        launch = Path(env['XDG_CONFIG_HOME']).parent
        self.assertEqual(launch.parent.parent, root)
        self.assertEqual(env['OPENCODE_TEST_MANAGED_CONFIG_DIR'], str(launch / 'managed'))
        self.assertTrue(set(forbidden).isdisjoint(env))
        for name in ('HOME', 'XDG_CONFIG_HOME', 'XDG_DATA_HOME', 'XDG_CACHE_HOME', 'XDG_STATE_HOME', 'TMPDIR'):
            self.assertEqual(Path(env[name]).parent, root if name == 'XDG_DATA_HOME' else launch)
            if name == 'TMPDIR':
                self.assertFalse(Path(env[name]).exists())
            else:
                self.assertEqual(Path(env[name]).stat().st_mode & 0o777, 0o700)
        path = launch / 'config' / 'opencode' / 'opencode.json'
        self.assertEqual((path.parent / '.npmrc').read_text(), 'offline=true\n')
        self.assertTrue(json.loads((path.parent / 'package.json').read_text())['private'])
        config = json.loads(path.read_text())
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        self.assertEqual((config['share'], config['autoupdate'], config['enabled_providers']),
                         ('disabled', False, ['torii-host']))
        self.assertEqual(config['model'], config['small_model'])
        self.assertEqual(config['mcp'], {})
        self.assertEqual(config['instructions'], [str(self.cwd / 'AGENTS.md')])
        self.assertEqual(config['permission'], {'edit': 'allow', 'bash': 'allow', 'webfetch': 'allow', 'external_directory': 'deny'})
        self.assertEqual(config['provider']['torii-host']['options']['apiKey'], '{file:' + str(self.key) + '}')
        for name in ('spec.json', 'events.jsonl', 'writes.jsonl', 'stderr.log', 'exit.json'):
            self.assertNotIn(str(self.key), (host / name).read_text(), name)
        self.assertEqual([row['message']['method'] for row in self.calls()], ['initialize', 'session/new', 'session/prompt'])

    async def cleanup_storage(self, result):
        if not self.store.db.execute('SELECT 1 FROM workers WHERE id=31').fetchone():
            self.store.db.execute("INSERT OR IGNORE INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:2',1,2,'test',?,1)",
                                  (str(self.cwd),))
            task = self.store.task_create('1:2', 'Direct provider fixture', worktree=str(self.cwd))
            self.store.db.execute("""INSERT INTO workers(id,task,topic,provider,prompt,cwd,status,created,updated)
                VALUES (31,?,'1:2','opencode','short prompt',?,'running',1,1)""", (task['id'], str(self.cwd)))
        storage = worker_storage.storage_for(self.store)
        host = Path(result.log_path).parent.name
        storage.register_host(31, host)
        self.store.db.execute("UPDATE workers SET status='running',updated=?,host=? WHERE id=31", (time.time(), host))
        self.store.worker_complete(31, {'success': result.success}, 'done')
        storage.schedule(31)
        while storage.tasks:
            await asyncio.gather(*list(storage.tasks.values()))
            await asyncio.sleep(0)
        return storage

    def cleanup_problems(self):
        return [row['detail'] for row in self.store.db.execute(
            "SELECT detail FROM problems WHERE code='temp-cleanup-failed' ORDER BY id")]

    def assert_launch_cleaned(self, result):
        launch = self.launch_root(result)
        self.assertFalse((launch / 'tmp').exists())
        self.assertFalse((launch / 'home/Library/Caches').exists())
        for name in ('guard-launch.json', 'guard-ready.json', 'config/opencode/opencode.json',
                     'config/opencode/plugins/guard.js', 'home/retained', 'managed/retained',
                     'stream-incomplete.json', 'observed-parts.jsonl'):
            self.assertTrue((launch / name).is_file(), name)
        self.assertEqual((launch.parent.parent / 'data/retained').read_text(), '{}\n')

    async def test_final_launch_removes_only_regenerable_scratch(self):
        self.scenario(scratch_files=True)
        result = await self.run_worker()
        self.assertTrue(result.success, result)
        launch = self.launch_root(result)
        self.assertEqual((launch / 'tmp/scratch').read_text(), '{}\n')
        self.assertEqual((launch / 'home/Library/Caches/scratch').read_text(), '{}\n')
        storage = await self.cleanup_storage(result)
        self.assert_launch_cleaned(result)
        self.assertEqual(storage.state(31)['pending'], 0)
        self.assertTrue((launch.parent.parent / 'data/opencode/opencode.db').is_file())
        await storage.cleanup(31)
        self.assertEqual(self.cleanup_problems(), [])

    async def test_exception_after_successful_stop_cleans_launch_scratch(self):
        self.scenario(scratch_files=True)
        original = HostClient.stop
        stopped = []

        async def stop(client):
            await original(client)
            stopped.append(client)

        def session(sid):
            raise RuntimeError('session callback failed')

        with patch.object(HostClient, 'stop', stop):
            result = await self.run_worker(on_session=session)
        self.assertEqual((result.success, result.failure_code), (False, 'execution_failed'))
        self.assertEqual(len(stopped), 1)
        await self.cleanup_storage(result)
        self.assert_launch_cleaned(result)

    async def test_cancelled_run_keeps_live_host_and_all_launch_scratch(self):
        self.scenario(scratch_files=True, wait_cancel=True)
        pool, task = self.make_pool()
        wid = self.spawn(task).data['worker']
        run = asyncio.create_task(pool.run(pool.get(wid), self.store.topic('1:2')))
        self.addAsyncCleanup(self.settle_run, run)
        await self.wait_prompt()
        entries = list((self.store.directory / 'opencode/workers' / str(wid) / 'launches').iterdir())
        self.assertEqual(len(entries), 1)
        launch = entries[0]
        before = {str(path.relative_to(launch)): path.read_bytes() for path in launch.rglob('*') if path.is_file()}
        self.assertEqual((launch / 'tmp/scratch').read_text(), '{}\n')
        self.assertEqual((launch / 'home/Library/Caches/scratch').read_text(), '{}\n')
        run.cancel()
        await asyncio.gather(run, return_exceptions=True)
        self.assertTrue(run.cancelled())
        host = HostClient.attach(host_directory(self.store.directory, pool.get(wid)))
        self.assertEqual(host.state, 'running')
        os.kill(host.child_pid, 0)
        after = {str(path.relative_to(launch)): path.read_bytes() for path in launch.rglob('*') if path.is_file()}
        self.assertEqual(after, before)

    async def test_launch_cleanup_failure_records_once_without_changing_result(self):
        self.scenario(scratch_files=True)
        baseline = await self.run_worker()
        await self.cleanup_storage(baseline)
        original = worker_storage._remove_tree

        def remove(path):
            if Path(path).name.startswith('tmp.deleting-'):
                raise OSError(errno.EACCES, 'cleanup refused')
            return original(path)

        result = await self.run_worker()
        with patch.object(worker_storage, '_remove_tree', side_effect=remove):
            storage = await self.cleanup_storage(result)
        self.assertEqual(replace(result, pid=baseline.pid, log_path=baseline.log_path), baseline)
        problems = self.cleanup_problems()
        self.assertEqual(len(problems), 1)
        self.assertIn('cleanup refused', problems[0])
        state = storage.state(31)
        self.assertEqual(state['pending'], 1)
        retained = [Path(path) for path in json.loads(state['staged'])]
        self.assertEqual(len(retained), 1)
        self.assertEqual((retained[0] / 'scratch').read_text(), '{}\n')
        self.assertFalse((self.launch_root(result) / 'home/Library/Caches').exists())
        await storage.cleanup(31)
        self.assertEqual(storage.state(31)['pending'], 0)
        self.assertFalse(retained[0].exists())

    async def check_launch_cleanup_link(self, target):
        self.scenario(scratch_files=True)
        baseline = await self.run_worker()
        await self.cleanup_storage(baseline)
        outside = self.root / 'outside'
        state = outside / 'Library/Caches/owner-app/state'
        state.parent.mkdir(parents=True)
        state.write_bytes(b'owner data')
        before = {str(path.relative_to(outside)): path.read_bytes() if path.is_file() else None
                  for path in outside.rglob('*')}
        original = opencode_runtime.finish

        def finish(result, *args, **kwargs):
            original(result, *args, **kwargs)
            path = Path(kwargs['launch_root']) / target
            path.rename(self.root / 'displaced')
            destination = (self.root / 'missing' if target == 'tmp' else
                           outside / 'Library' if target == 'home/Library' else outside)
            path.symlink_to(destination, target_is_directory=True)

        with patch('coordinator.opencode_runtime.finish', finish):
            result = await self.run_worker()
        storage = await self.cleanup_storage(result)
        self.assertEqual(replace(result, pid=baseline.pid, log_path=baseline.log_path), baseline)
        after = {str(path.relative_to(outside)): path.read_bytes() if path.is_file() else None
                 for path in outside.rglob('*')}
        self.assertEqual(after, before)
        launch = self.launch_root(result)
        if target == 'tmp':
            self.assertFalse((launch / target).is_symlink())
            self.assertFalse((launch / 'home/Library/Caches').exists())
            self.assertEqual(storage.state(31)['pending'], 0)
            self.assertEqual(self.cleanup_problems(), [])
        else:
            self.assertTrue((launch / target).is_symlink())
            self.assertEqual((launch / 'tmp/scratch').read_text(), '{}\n')
            self.assertEqual(storage.state(31)['pending'], 1)
            self.assertEqual(len(self.cleanup_problems()), 1)
            self.assertIn('Symbolic link in worker scratch path', self.cleanup_problems()[0])

    async def test_launch_cleanup_preserves_symlinked_home(self):
        await self.check_launch_cleanup_link('home')

    async def test_launch_cleanup_preserves_symlinked_library(self):
        await self.check_launch_cleanup_link('home/Library')

    async def test_launch_cleanup_records_dangling_tmp_symlink(self):
        await self.check_launch_cleanup_link('tmp')

    async def test_launch_cleanup_keeps_event_loop_responsive(self):
        self.scenario(scratch_files=True)
        result = await self.run_worker()
        entered, release, finished = threading.Event(), threading.Event(), threading.Event()
        original = worker_storage._remove_tree

        def remove(path):
            if Path(path).name.startswith('tmp.deleting-'):
                entered.set()
                release.wait(5)
                try:
                    return original(path)
                finally:
                    finished.set()
            return original(path)

        with patch.object(worker_storage, '_remove_tree', side_effect=remove):
            cleanup = asyncio.create_task(self.cleanup_storage(result))
            try:
                await self.wait_until(entered.is_set)
                await asyncio.sleep(.01)
                self.assertFalse(finished.is_set(), 'cleanup blocked the event loop')
            finally:
                release.set()
                await cleanup
        self.assertTrue(finished.is_set())
        self.assertTrue(result.success, result)
        self.assert_launch_cleaned(result)

    async def test_launch_cleanup_problem_callback_runs_on_event_loop(self):
        self.scenario(scratch_files=True)
        result = await self.run_worker()
        loop_thread = threading.get_ident()
        removal_threads, recorded = [], []
        original = worker_storage._remove_tree
        record = worker_storage.problems.record

        def remove(path):
            if Path(path).name.startswith('tmp.deleting-'):
                removal_threads.append(threading.get_ident())
                raise OSError(errno.EACCES, 'cleanup refused')
            return original(path)

        def problem(store, area, code, detail=None, **kwargs):
            recorded.append((threading.get_ident(), area, code))
            return record(store, area, code, detail, **kwargs)

        with patch.object(worker_storage, '_remove_tree', side_effect=remove), \
                patch.object(worker_storage.problems, 'record', side_effect=problem):
            await self.cleanup_storage(result)
        self.assertTrue(result.success, result)
        self.assertEqual(len(removal_threads), 1)
        self.assertNotEqual(removal_threads[0], loop_thread)
        self.assertEqual(recorded, [(loop_thread, 'worker', 'temp-cleanup-failed')])
        self.assertIn('cleanup refused', self.cleanup_problems()[0])
        self.assertFalse((self.launch_root(result) / 'home/Library/Caches').exists())

    async def test_cancel_during_launch_cleanup_propagates_and_removal_completes(self):
        self.scenario(scratch_files=True)
        result = await self.run_worker()
        entered, release, finished = threading.Event(), threading.Event(), threading.Event()
        original = worker_storage._remove_tree
        paths = []

        def remove(path):
            if Path(path).name.startswith('tmp.deleting-'):
                paths.append(path)
                entered.set()
                release.wait(5)
                try:
                    return original(path)
                finally:
                    finished.set()
            return original(path)

        with patch.object(worker_storage, '_remove_tree', side_effect=remove):
            cleanup = asyncio.create_task(self.cleanup_storage(result))
            try:
                await self.wait_until(entered.is_set)
                self.assertFalse(finished.is_set())
                cleanup.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await cleanup
                self.assertTrue(cleanup.cancelled())
                self.assertFalse(finished.is_set())
            finally:
                release.set()
                await self.wait_until(finished.is_set)
        self.assertEqual(len(paths), 1)
        self.assertFalse(paths[0].exists())
        self.assertFalse((self.launch_root(result) / 'home/Library/Caches').exists())
        self.assertEqual(self.cleanup_problems(), [])

    async def test_permission_is_rejected_and_recorded(self):
        self.scenario(permission=True)
        problems = []
        result = await self.run_worker(on_problem=lambda *args: problems.append(args))
        self.assertTrue(result.success, result)
        response = next(row['message'] for row in self.calls() if row['message'].get('id') == 'permission-1')
        self.assertEqual(response['result'], {'outcome': {'outcome': 'selected', 'optionId': 'deny-this'}})
        self.assertEqual(problems[0][:2], ('worker', 'opencode-permission-rejected'))

    async def test_http_failures_are_classified_without_inline_retry(self):
        for status, code in ((401, 'auth'), (402, 'out_of_credit'), (429, 'rate_limited')):
            with self.subTest(status=status):
                before = len(self.calls())
                self.scenario(http_error=status)
                result = await self.run_worker()
                self.assertFalse(result.success)
                self.assertEqual(result.failure_code, code)
                self.assertIn('one', result.error)
                self.assertIn({401: 'Invalid API key provided', 402: 'Insufficient balance, please top up',
                               429: 'Request capacity exhausted'}[status], result.error)
                self.assertEqual(result.quota_limited, status != 401)
                self.assertEqual(len(self.calls()) - before, 3)
        pool, task = self.make_pool()
        self.scenario(http_error=401)
        wid = self.spawn(task).data['worker']
        done = await pool.run(pool.get(wid), self.store.topic('1:2'))
        self.assertEqual(done['status'], 'done')
        self.assertEqual(json.loads(done['result'])['failure_code'], 'auth')
        self.assertEqual(self.store.get('opencode_account_blocks', {}), {})

    async def assert_refusal_parks_and_resumes(self, status, reason, delay=300, known=False, recheck=False,
                                             reset_at=None, **scenario):
        self.scenario(http_error=status, **scenario)
        pool, task = self.make_pool()
        service = Service(self.store, None, self.runner, self.cwd)
        service.workers = pool
        wid = self.spawn(task).data['worker']
        before = time.time()
        waiting = await pool.run(pool.get(wid), self.store.topic('1:2'))
        self.assertEqual(waiting['status'], 'waiting_for_quota')
        self.assertEqual((waiting['session'], waiting['account_alias'], waiting['fresh']), (SESSION_ID, 'one', 0))
        result = json.loads(waiting['result'])
        self.assertEqual(result['failure_code'], reason)
        block = self.store.get('opencode_account_blocks')['one']
        self.assertEqual((block['reason'], block['reset_known']), (reason, known))
        if reset_at is None:
            self.assertGreaterEqual(block['until'], before + delay)
            self.assertLessEqual(block['until'], time.time() + delay)
        else:
            self.assertEqual(block['until'], reset_at)
        self.assertEqual(result['waiting_until'], block['until'])
        self.assertIsNone(opencode_runtime.OpenCodeBroker(self.store).select('one'))
        with self.assertRaises(AccountsUnavailable) as paused:
            opencode_runtime.parent_ready(self.store)
        self.assertEqual(paused.exception.reset_at, block['until'])
        self.assertEqual(list(self.store.db.execute("SELECT text FROM messages WHERE kind='callback'")), [])
        reports = [tuple(row) for row in self.store.db.execute('SELECT topic,text FROM outbox')]
        self.assertEqual([topic for topic, text in reports], ['1:2'])
        self.assertIn('OpenCode account one', reports[0][1])
        self.assertIn('out of credit' if status == 402 else 'limited', reports[0][1])
        self.assertIn('automatically', reports[0][1])
        self.store.put('opencode_accounts', {'one': dict(ACCOUNT), 'two': dict(ACCOUNT)})
        self.store.put('opencode_active_account', 'two')
        count = len(self.calls())
        await service.workers_once()
        self.assertEqual(pool.get(wid)['status'], 'waiting_for_quota')
        self.assertEqual(len(self.calls()), count)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM messages WHERE kind='callback'").fetchone()[0], 0)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM outbox').fetchone()[0], 1)
        if recheck:
            now = block['until'] + 1
            with patch('coordinator.opencode_runtime.time.time', return_value=now):
                await service.workers_once()
                await asyncio.wait_for(asyncio.gather(*service.worker_tasks.values()), 20)
            again = pool.get(wid)
            self.assertEqual((again['status'], again['session'], again['account_alias']),
                             ('waiting_for_quota', SESSION_ID, 'one'))
            block = self.store.get('opencode_account_blocks')['one']
            self.assertEqual(block['until'], now + 600)
            self.assertFalse(block['reset_known'])
        self.scenario(history_reply=True)
        with patch('coordinator.opencode_runtime.time.time', return_value=block['until'] + 1):
            await service.workers_once()
            await asyncio.wait_for(asyncio.gather(*service.worker_tasks.values()), 20)
        done = pool.get(wid)
        self.assertEqual((done['status'], done['session'], done['account_alias']), ('done', SESSION_ID, 'one'))
        self.assertTrue(json.loads(done['result'])['success'], done['result'])
        self.assertEqual([call['message']['method'] for call in self.calls()],
                         ['initialize', 'session/new', 'session/prompt'] +
                         ['initialize', 'session/load', 'session/prompt'] * (2 if recheck else 1))
        self.assertIn('short prompt', json.loads(done['result'])['text'])

    async def test_credit_refusal_parks_and_resumes_the_same_session(self):
        await self.assert_refusal_parks_and_resumes(402, 'out_of_credit')

    async def test_rate_refusal_parks_and_resumes_the_same_session(self):
        await self.assert_refusal_parks_and_resumes(429, 'rate_limited', recheck=True)

    async def test_rate_refusal_obeys_retry_after(self):
        await self.assert_refusal_parks_and_resumes(429, 'rate_limited', delay=900, known=True,
                                                  response_headers={'Retry-After': '900'})

    async def test_weekly_refusal_masked_as_context_overflow_parks(self):
        with patch('coordinator.opencode_recovery.time.time', return_value=WEEKLY_REFUSAL['resetAt'] / 1000 - 3600):
            await self.assert_refusal_parks_and_resumes(429, 'usage_limited', known=True,
                reset_at=WEEKLY_REFUSAL['resetAt'] / 1000, saved_error={
                'name': 'ContextOverflowError', 'data': {'message': 'Context overflow',
                                                       'responseBody': json.dumps(WEEKLY_REFUSAL)}})

    async def test_weekly_refusal_before_compaction_survives_the_final_overflow(self):
        with patch('coordinator.opencode_recovery.time.time', return_value=WEEKLY_REFUSAL['resetAt'] / 1000 - 3600):
            await self.assert_refusal_parks_and_resumes(429, 'usage_limited', known=True,
                reset_at=WEEKLY_REFUSAL['resetAt'] / 1000,
                saved_error={'name': 'ContextOverflowError', 'data': {'message': 'Compaction failed'}},
                session_errors=[{'error': {'name': 'ContextOverflowError', 'data': {
                    'message': 'Context overflow', 'responseBody': json.dumps(WEEKLY_REFUSAL)}}}])

    async def test_precompaction_credit_refusal_obeys_retry_after(self):
        await self.assert_refusal_parks_and_resumes(402, 'out_of_credit', delay=900, known=True,
            saved_error={'name': 'ContextOverflowError', 'data': {'message': 'Compaction failed'}},
            session_errors=[{'error': {'name': 'APIError', 'data': {
                'statusCode': 402, 'responseHeaders': {'Retry-After': '900'}}}}])

    async def test_worker_waits_for_a_shared_chat_block_before_launch(self):
        opencode_runtime.record_parent_block(self.store, 'one', 'out_of_credit')
        pool, task = self.make_pool()
        wid = self.spawn(task).data['worker']
        waiting = await pool.run(pool.get(wid), self.store.topic('1:2'))
        self.assertEqual((waiting['status'], waiting['session'], waiting['fresh']), ('waiting_for_quota', None, 1))
        self.assertEqual(self.calls(), [])
        self.store.put('opencode_account_blocks', {})
        service = Service(self.store, None, self.runner, self.cwd)
        service.workers = pool
        await service.workers_once()
        await asyncio.wait_for(asyncio.gather(*service.worker_tasks.values()), 20)
        self.assertEqual(pool.get(wid)['status'], 'done')
        self.assertTrue(json.loads(pool.get(wid)['result'])['success'])

    async def test_precompaction_refusal_is_scoped_and_preserves_other_results(self):
        weekly = {'error': {'name': 'ContextOverflowError', 'data': {'responseBody': json.dumps(WEEKLY_REFUSAL)}}}
        overflow = {'name': 'ContextOverflowError', 'data': {'message': 'Compaction failed'}}
        self.scenario(http_error=429, saved_error=overflow, session_errors=[weekly])
        result = await self.run_worker()
        self.assertEqual(result.failure_code, 'usage_limited')
        self.assertTrue(result.quota_limited)
        marker = json.loads((self.launch_root(result) / 'provider-error.json').read_text())
        self.assertNotIn('responseBody', json.dumps(marker))
        self.assertEqual(set(marker), {'version', 'launch', 'session_id', 'error'})
        self.assertEqual(set(marker['error']), {'name', 'status', 'code', 'reset_at', 'message'})
        for options, code, success in (
                ({'http_error': 429, 'saved_error': overflow,
                  'session_errors': [{**weekly, 'session_id': 'ses_' + 'a' * 26}]}, 'context_overflow', False),
                ({'http_error': 429, 'saved_error': overflow, 'session_errors': [{'error': {
                    'name': 'ContextOverflowError', 'data': {'responseBody': '{"error":{"message":"token limit"}}'}}}]},
                 'context_overflow', False),
                ({'http_error': 401, 'session_errors': [weekly]}, 'auth', False),
                ({'session_errors': [weekly]}, None, True),
                ({'http_error': 429, 'saved_error': overflow}, 'context_overflow', False)):
            with self.subTest(options=options):
                self.scenario(**options)
                result = await self.run_worker()
                self.assertEqual((result.failure_code, result.success, result.quota_limited), (code, success, False))
        self.scenario(context_overflow=True)
        pool, task = self.make_pool()
        wid = self.spawn(task).data['worker']
        done = await pool.run(pool.get(wid), self.store.topic('1:2'))
        self.assertEqual(done['status'], 'done')
        result = json.loads(done['result'])
        self.assertEqual((result['failure_code'], result['quota_limited']), ('context_overflow', False))

    async def test_version_mismatch_starts_no_session(self):
        self.scenario(version='2.0.0')
        result = await self.run_worker()
        self.assertEqual(result.failure_code, 'version_mismatch')
        self.assertEqual([row['message']['method'] for row in self.calls()], ['initialize'])

    async def test_exit_after_our_eof_does_not_undo_verified_success(self):
        self.scenario(eof_exit=7)
        result = await self.run_worker()
        self.assertTrue(result.success, result)
        self.assertEqual(json.loads((Path(result.log_path).parent / 'exit.json').read_text())['returncode'], 7)

    async def test_crash_is_disconnected_and_no_cancel_is_written(self):
        self.scenario(crash=True)
        result = await self.run_worker()
        self.assertEqual(result.failure_code, 'disconnected')
        self.assertFalse(result.success)
        self.assertNotIn('session/cancel', [row['message']['method'] for row in self.calls()])

    async def test_resume_without_account_pin_starts_nothing(self):
        result = await self.runner.run('opencode', 'resume', self.cwd, SESSION_ID, fresh=False)
        self.assertEqual(result.failure_code, 'account_changed')
        self.assertFalse(result.success)
        self.assertEqual(self.calls(), [])
        self.assertFalse((self.store.directory / 'hosts').exists())

    async def test_pool_selects_model_and_pins_account(self):
        pool, task = self.make_pool()
        queued = self.spawn(task)
        self.assertTrue(queued.ok, queued.text)
        done = await pool.run(pool.get(queued.data['worker']), self.store.topic('1:2'))
        self.assertEqual(done['account_alias'], 'one')
        result = json.loads(done['result'])
        self.assertTrue(result['success'], result)
        spec = json.loads((Path(result['log_path']).parent / 'spec.json').read_text())
        self.assertEqual((spec['model'], spec['account']), ('example-model', 'one'))
        self.assertEqual(Path(spec['env']['XDG_DATA_HOME']).parent.name, str(done['id']))

    async def test_pool_resume_without_account_pin_starts_nothing(self):
        pool, task = self.make_pool()
        queued = self.spawn(task)
        self.store.db.execute('UPDATE workers SET fresh=0,session=? WHERE id=?', (SESSION_ID, queued.data['worker']))
        done = await pool.run(pool.get(queued.data['worker']), self.store.topic('1:2'))
        result = json.loads(done['result'])
        self.assertEqual(result['failure_code'], 'account_changed')
        self.assertFalse(result['success'])
        self.assertEqual(self.calls(), [])

    async def test_direct_runner_refuses_off_goals_secrets_and_wrong_model(self):
        for options in ({'goal': 'tests pass'}, {'secrets': {'DUMMY_SECRET': 'dummy-only'}},
                        {'secret_names': ['DUMMY_SECRET']}, {'model': 'wrong'}):
            with self.subTest(options=tuple(options)):
                result = await self.run_worker(**options)
                self.assertFalse(result.success)
                self.assertEqual(result.failure_code, 'start_failed')
        self.store.put('opencode_enabled', False)
        result = await self.run_worker()
        self.assertFalse(result.success)
        self.assertIn('workers are off', result.failure_detail)
        self.assertEqual(self.calls(), [])

    async def test_invalid_session_id_and_failed_stop_reasons_are_failures(self):
        self.scenario(session_id='../outside')
        result = await self.run_worker()
        self.assertEqual(result.failure_code, 'invalid_session_id')
        for reason in ('refusal', 'max_tokens', 'max_turn_requests', 'cancelled'):
            with self.subTest(reason=reason):
                self.scenario(stop_reason=reason)
                result = await self.run_worker()
                self.assertFalse(result.success)
                self.assertEqual(result.failure_code, reason)

    async def test_spawn_and_launch_stat_only_refusals(self):
        pool, task = self.make_pool()
        for failure in ('missing', 'mode', 'empty', 'model', 'off', 'goal', 'secrets'):
            with self.subTest(failure=failure):
                self.key.write_text('dummy-only\n')
                self.key.chmod(0o600)
                self.store.put('opencode_enabled', True)
                self.store.task_update(task['id'], secrets=[])
                queued = self.spawn(task)
                self.assertTrue(queued.ok, queued.text)
                options = {}
                if failure == 'missing':
                    self.key.rename(self.key_home / 'saved-key')
                elif failure == 'mode':
                    self.key.chmod(0o644)
                elif failure == 'empty':
                    self.key.write_text('')
                elif failure == 'model':
                    options['model'] = 'different'
                    self.store.db.execute('UPDATE workers SET model=? WHERE id=?', ('different', queued.data['worker']))
                elif failure == 'off':
                    self.store.put('opencode_enabled', False)
                elif failure == 'goal':
                    options['goal'] = 'tests pass'
                    self.store.db.execute('UPDATE workers SET goal=? WHERE id=?', ('tests pass', queued.data['worker']))
                else:
                    self.store.task_update(task['id'], secrets=['SERVICE_ACCESS=VAULT_ACCESS'])
                with patch('pathlib.Path.open', side_effect=AssertionError('key opened')), patch.object(self.runner, 'run', AsyncMock()) as run:
                    refused = self.spawn(task, **options)
                    self.assertFalse(refused.ok, refused.text)
                    done = await pool.run(pool.get(queued.data['worker']), self.store.topic('1:2'))
                    run.assert_not_awaited()
                result = json.loads(done['result'])
                self.assertFalse(result['success'])
                self.assertEqual(result['failure_detail'], refused.text)
                if failure == 'off':
                    self.assertEqual(refused.text, 'OpenCode workers are off.')

    def test_other_providers_do_not_get_account_paths(self):
        self.runner.codex_broker = None
        env = self.runner._environment('codex')
        self.assertNotIn(str(self.key), json.dumps(env))
        broker = AccountBroker(self.store)
        self.store.put('accounts', {'test': {'config_dir': str(self.root / 'claude-profile'), 'enabled': True}})
        self.runner.account_broker = broker
        env = self.runner._environment('claude', 'test')
        self.assertNotIn(str(self.key), json.dumps(env))

    def test_absent_and_zero_cost_have_no_estimate_and_delta_is_bounded(self):
        for update in (None, {'cost': {'amount': 0, 'currency': 'USD'}}):
            self.assertIsNone(opencode_runtime.usage({}, update)['usd_estimate'])
        self.assertAlmostEqual(opencode_runtime.usage({}, {'cost': {'amount': 0.7, 'currency': 'USD'}}, 0.5)['usd_estimate'], 0.2)

    def test_environment_allowlist_rejects_credentials_and_config(self):
        forbidden = {name: 'dummy' for name in (
            'GH_TOKEN', 'GITHUB_TOKEN', 'SSH_AUTH_SOCK', 'EXAMPLE_HOST_API_KEY', 'OTHER_HOST_API_KEY',
            'ZHIPU_API_KEY', 'OTHER_API_KEY', 'npm_config_userconfig', 'NPM_CONFIG_REGISTRY',
            'NpM_cOnFiG_cache', 'GIT_CONFIG_GLOBAL', 'GH_CONFIG_DIR', 'TORII_PRIVATE', 'UNKNOWN')}
        base = dict(forbidden, PATH='/usr/bin:/bin', LANG='C', SHELL='/bin/sh', USER='test',
                    LOGNAME='test', TERM='dumb', TZ='UTC')
        env = opencode_runtime.environment(self.root / 'isolated', base)
        self.assertEqual(set(env) & set(forbidden), set())
        for name in ('PATH', 'LANG', 'SHELL', 'USER', 'LOGNAME', 'TERM', 'TZ'):
            self.assertEqual(env[name], base[name])
        self.assertEqual(env['OPENCODE_DISABLE_LSP_DOWNLOAD'], '1')
        self.assertEqual(env['OPENCODE_DISABLE_SHARE'], '1')
        self.assertEqual(base['UNKNOWN'], 'dummy')

    def test_write_config_cleans_temporary_after_failure(self):
        for operation in ('json.dump', 'os.fsync', 'os.replace'):
            with self.subTest(operation=operation):
                root = self.root / operation
                with patch('coordinator.opencode_runtime.' + operation, side_effect=OSError('forced write failure')):
                    with self.assertRaisesRegex(OSError, 'forced write failure'):
                        opencode_runtime.write_config(root, ACCOUNT, self.key, self.cwd)
                self.assertEqual(list((root / 'config' / 'opencode').glob('.opencode-*')), [])

    async def test_key_lost_after_stat_is_auth(self):
        original = opencode_runtime.write_config
        for mode in ('missing', 'unreadable'):
            with self.subTest(mode=mode):
                self.key.write_text('dummy-only')
                self.key.chmod(0o600)
                self.scenario(crash=True)
                def write(*args):
                    path = original(*args)
                    if mode == 'missing':
                        self.key.rename(self.key_home / 'saved-key')
                    else:
                        self.key.chmod(0o000)
                    return path
                with patch('coordinator.opencode_runtime.write_config', write):
                    result = await self.run_worker()
                self.assertFalse(result.success)
                self.assertEqual(result.failure_code, 'auth')
                self.assertIn('one', result.error)
                self.assertIn('key file is missing or unreadable', result.error)
                self.assertNotIn(str(self.key), result.error)

    def test_unrelated_status_numbers_are_not_error_classes(self):
        for number in ('401', '402', '429'):
            error = {'code': -32603, 'message': 'Internal error: request ' + number + ' failed',
                     'data': {'service': 'session', 'errorName': 'APIError'}}
            self.assertEqual(opencode_runtime.classify_error('session/prompt', error), 'provider_error')

    async def test_recorded_length_is_max_tokens(self):
        self.scenario(finish='length')
        result = await self.run_worker()
        self.assertFalse(result.success)
        self.assertEqual(result.failure_code, 'max_tokens')

    async def test_retry_uses_completed_text_from_final_step(self):
        self.scenario(chunks=['start ', 'successful answer'], parts=[
            {'type': 'text', 'text': 'stale completed retry', 'time': {'start': 1, 'end': 2}},
            {'type': 'step-start'},
            {'type': 'text', 'text': 'unfinished retry', 'time': {'start': 2}},
            {'type': 'text', 'text': 'successful ', 'time': {'start': 2, 'end': 3}},
            {'type': 'text', 'text': 'answer', 'time': {'start': 3, 'end': 4}},
            {'type': 'step-finish', 'reason': 'stop'}])
        result = await self.run_worker()
        self.assertTrue(result.success, result)
        self.assertEqual(result.text, 'successful answer')

    async def test_record_reader_preserves_worker_files(self):
        result = await self.run_worker()
        root = self.store.directory / 'opencode/workers/31'
        def snapshot():
            return {str(p.relative_to(root)): (p.read_bytes(), p.stat().st_mtime_ns, p.stat().st_mode)
                    for p in root.rglob('*') if p.is_file()}
        before = snapshot()
        records = opencode_runtime.records(root, result.session_id)
        self.assertEqual(records[0]['finish'], 'stop')
        self.assertEqual(snapshot(), before)

    async def test_record_reader_rejects_paths_outside_worker(self):
        outside = self.root / 'outside'
        outside.mkdir()
        self.write_rows(outside)
        root = self.root / 'reader-root'
        (root / 'data').mkdir(parents=True)
        (root / 'data/opencode').symlink_to(outside, target_is_directory=True)
        self.assertIsNone(opencode_runtime.records(root, SESSION_ID))

    def test_record_reader_sees_wal_without_creating_source_shm(self):
        root = self.root / 'wal-reader'
        directory = root / 'data/opencode'
        directory.mkdir(parents=True)
        db = sqlite3.connect(str(directory / 'opencode.db'))
        self.addCleanup(db.close)
        db.execute('PRAGMA journal_mode=WAL')
        db.execute('CREATE TABLE message (id TEXT, session_id TEXT, time_created INTEGER, data TEXT)')
        db.execute('CREATE TABLE part (id TEXT, message_id TEXT, session_id TEXT, data TEXT)')
        db.execute('INSERT INTO message VALUES (?,?,?,?)', (
            'msg_final', SESSION_ID, 1, json.dumps({'role': 'assistant', 'finish': 'length'})))
        db.commit()
        (directory / 'opencode.db-shm').unlink()
        def snapshot():
            return {p.name: (p.read_bytes(), p.stat().st_mtime_ns) for p in directory.iterdir()}
        before = snapshot()
        self.assertEqual(opencode_runtime.records(root, SESSION_ID), ({'role': 'assistant', 'finish': 'length'}, []))
        self.assertEqual(snapshot(), before)

    def test_record_reader_reads_wal_database_without_wal_file(self):
        root = self.root / 'wal-reader-without-wal'
        directory = root / 'data/opencode'
        directory.mkdir(parents=True)
        database = directory / 'opencode.db'
        db = sqlite3.connect(str(database))
        self.addCleanup(db.close)
        self.assertEqual(db.execute('PRAGMA journal_mode=WAL').fetchone()[0], 'wal')
        db.execute('CREATE TABLE message (id TEXT, session_id TEXT, time_created INTEGER, data TEXT)')
        db.execute('CREATE TABLE part (id TEXT, message_id TEXT, session_id TEXT, data TEXT)')
        db.execute('INSERT INTO message VALUES (?,?,?,?)', (
            'msg_final', SESSION_ID, 1, json.dumps({'role': 'assistant', 'finish': 'stop'})))
        db.execute('INSERT INTO part VALUES (?,?,?,?)', (
            'part_final', 'msg_final', SESSION_ID,
            json.dumps({'type': 'text', 'text': 'source rows', 'time': {'end': 2}})))
        db.commit()
        db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        db.close()
        for suffix in ('-wal', '-shm'):
            sidecar = directory / ('opencode.db' + suffix)
            if sidecar.exists():
                sidecar.unlink()
        self.assertFalse((directory / 'opencode.db-wal').exists())
        before = {path.name: path.read_bytes() for path in directory.iterdir()}
        self.assertEqual(opencode_runtime.records(root, SESSION_ID), (
            {'role': 'assistant', 'finish': 'stop'},
            [{'type': 'text', 'text': 'source rows', 'time': {'end': 2}}]))
        self.assertEqual({path.name: path.read_bytes() for path in directory.iterdir()}, before)

    async def test_absent_records_cannot_verify_success(self):
        self.scenario(no_records=True)
        result = await self.run_worker()
        self.assertFalse(result.success)
        self.assertEqual(result.failure_code, 'provider_records_unavailable')

    async def test_error_without_records_keeps_bounded_host_message(self):
        self.scenario(no_records=True, http_error=429, error_message='Request capacity exhausted ' + 'x' * 600)
        result = await self.run_worker()
        self.assertFalse(result.success)
        self.assertEqual(result.failure_code, 'provider_error')
        self.assertIn('one', result.error)
        self.assertIn('Request capacity exhausted', result.error)
        self.assertLessEqual(len(result.error), 300)

    async def test_disconnection_reads_no_records_before_prompt_response(self):
        self.scenario(crash=True)
        with patch('coordinator.opencode_runtime.records', create=True) as read:
            result = await self.run_worker()
        self.assertEqual(result.failure_code, 'disconnected')
        read.assert_not_called()

    def write_rows(self, directory, wal=False):
        db = sqlite3.connect(str(directory / 'opencode.db'))
        if wal:
            db.execute('PRAGMA journal_mode=WAL')
        db.execute('CREATE TABLE message (id TEXT, session_id TEXT, time_created INTEGER, data TEXT)')
        db.execute('CREATE TABLE part (id TEXT, message_id TEXT, session_id TEXT, data TEXT)')
        db.commit()
        if wal:
            db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
            db.execute('PRAGMA wal_autocheckpoint=0')
        db.execute('INSERT INTO message VALUES (?,?,?,?)', (
            'msg_final', SESSION_ID, 1, json.dumps({'role': 'assistant', 'finish': 'stop'})))
        db.execute('INSERT INTO part VALUES (?,?,?,?)', (
            'part_final', 'msg_final', SESSION_ID, json.dumps({'type': 'text', 'text': 'outside rows'})))
        db.commit()
        if wal:
            self.addCleanup(db.close)
        else:
            db.close()
        return db

    def test_record_reader_rejects_linked_database(self):
        outside = self.root / 'outside-db'
        outside.mkdir()
        self.write_rows(outside)
        root = self.root / 'linked-db'
        directory = root / 'data/opencode'
        directory.mkdir(parents=True)
        (directory / 'opencode.db').symlink_to(outside / 'opencode.db')
        self.assertIsNone(opencode_runtime.records(root, SESSION_ID))

    def test_record_reader_rejects_linked_wal(self):
        outside = self.root / 'outside-wal'
        outside.mkdir()
        self.write_rows(outside, wal=True)
        root = self.root / 'linked-wal'
        directory = root / 'data/opencode'
        directory.mkdir(parents=True)
        shutil.copyfile(outside / 'opencode.db', directory / 'opencode.db')
        (directory / 'opencode.db-wal').symlink_to(outside / 'opencode.db-wal')
        self.assertIsNone(opencode_runtime.records(root, SESSION_ID))

    async def test_provider_error_redacts_key_path(self):
        for message in ('Could not open ' + str(self.key), 'Bad value {file:/dummy/key}'):
            with self.subTest(message=message):
                self.scenario(http_error=401, error_message=message)
                result = await self.run_worker()
                self.assertEqual(result.failure_code, 'auth')
                self.assertEqual(result.error, 'OpenCode account one failed: auth.')
                self.assertNotIn(str(self.key), result.error)

    async def test_guard_config_resource_and_permissions(self):
        result = await self.run_worker()
        self.assertTrue(result.success, result)
        directory = self.launch_root(result) / 'config/opencode'
        guard = directory / 'plugins/guard.js'
        self.assertEqual(guard.read_bytes(), (Path(opencode_runtime.__file__).parent / 'opencode_guard.js').read_bytes())
        self.assertEqual(guard.parent.stat().st_mode & 0o777, 0o700)
        self.assertEqual(guard.stat().st_mode & 0o777, 0o600)
        config = json.loads((directory / 'opencode.json').read_text())
        self.assertIs(config['lsp'], False)
        self.assertIs(config['formatter'], False)
        self.assertNotIn('--pure', self.calls()[0]['argv'])
        self.assertFalse((directory.parents[1] / 'guard-events.jsonl').exists())

    def test_guard_write_failure_cleans_temporary(self):
        replace = os.replace
        def fail_guard(source, target):
            if Path(target).name == 'guard.js':
                raise OSError('guard replace failed')
            return replace(source, target)
        root = self.root / 'failed-guard-write'
        with patch('coordinator.opencode_runtime.os.replace', fail_guard):
            with self.assertRaisesRegex(OSError, 'guard replace failed'):
                opencode_runtime.write_config(root, ACCOUNT, self.key, self.cwd)
        self.assertEqual(list((root / 'config/opencode/plugins').iterdir()), [])

    async def test_stream_incomplete_marker_maps_after_exit_without_writes(self):
        self.scenario(stream_incomplete=True)
        original = opencode_runtime.finish
        observations = []
        def finish(result, root, alias, error, **kwargs):
            def snapshot():
                return {str(p.relative_to(root)): (p.read_bytes() if p.is_file() else None,
                        p.stat().st_mode, p.stat().st_mtime_ns) for p in Path(root).rglob('*')}
            self.assertTrue((Path(result.log_path).parent / 'exit.json').is_file())
            wire = [json.loads(json.loads(line)['data']) for line in
                    (Path(result.log_path).parent / 'events.jsonl').read_text().splitlines()
                    if json.loads(line).get('data')]
            self.assertTrue(any((e.get('result') or {}).get('stopReason') == 'cancelled' for e in wire))
            before = snapshot()
            original(result, root, alias, error, **kwargs)
            self.assertEqual(snapshot(), before)
            observations.append(True)
        with patch('coordinator.opencode_runtime.finish', finish):
            result = await self.run_worker()
        self.assertFalse(result.success)
        self.assertEqual(result.failure_code, 'provider_stream_incomplete')
        self.assertEqual(result.error, 'OpenCode account one failed: provider_stream_incomplete.')
        self.assertEqual(observations, [True])

    async def test_marker_reader_rejects_invalid_or_stale_markers(self):
        valid = dict(version=1, code='provider_stream_incomplete', session_id=SESSION_ID, consecutive=3)
        cases = [dict(marker={**valid, **change}) for change in (
            {'session_id': 'ses_' + 'a' * 26}, {'version': 2}, {'version': True}, {'version': '1'},
            {'version': 1.0}, {'consecutive': 2}, {'consecutive': 4}, {'consecutive': True},
            {'consecutive': '3'}, {'consecutive': 3.0}, {'code': 'other'})]
        cases += [dict(marker=value) for value in ([], 'value', None, 1)]
        cases += [dict(marker_raw='broken'), dict(marker_raw=' ' * 1025 + json.dumps(valid))]
        cases += [dict(marker_kind=kind) for kind in ('missing', 'symlink', 'directory', 'fifo')]
        for index, case in enumerate(cases):
            with self.subTest(case=case):
                self.scenario(stream_incomplete=True, **case)
                result = await asyncio.wait_for(
                    self.runner.run('opencode', 'short prompt', self.cwd, None, fresh=True, worker_id=100 + index), 20)
                self.assertFalse(result.success)
                self.assertEqual(result.failure_code, 'cancelled')

    async def test_marker_does_not_override_owner_stopped_finish(self):
        self.scenario(stream_incomplete=True)
        result = await self.run_worker()
        result.failure_code = 'owner_stopped'
        opencode_runtime.finish(result, self.store.directory / 'opencode/workers/31', 'one', {})
        self.assertEqual(result.failure_code, 'owner_stopped')

    async def test_pool_stream_incomplete_runs_once_and_finishes(self):
        self.scenario(stream_incomplete=True)
        pool, task = self.make_pool()
        queued = self.spawn(task)
        self.assertTrue(queued.ok, queued.text)
        original = self.runner.run
        with patch.object(self.runner, 'run', wraps=original) as run:
            done = await pool.run(pool.get(queued.data['worker']), self.store.topic('1:2'))
        self.assertEqual(run.await_count, 1)
        self.assertEqual(done['status'], 'done')
        result = json.loads(done['result'])
        self.assertFalse(result['success'])
        self.assertEqual(result['failure_code'], 'provider_stream_incomplete')
        self.assertNotEqual(result.get('status'), 'waiting_for_quota')

    async def test_pool_owner_stop_wins_marker_race_after_finish(self):
        self.scenario(stream_incomplete=True)
        pool, task = self.make_pool()
        queued = self.spawn(task)
        worker_id = queued.data['worker']
        original = opencode_runtime.finish
        mapped = []
        def finish(*args, **kwargs):
            original(*args, **kwargs)
            mapped.append(args[0].failure_code)
            pool.stopping.add(worker_id)
        with patch('coordinator.opencode_runtime.finish', finish):
            done = await pool.run(pool.get(worker_id), self.store.topic('1:2'))
        self.assertEqual(mapped, ['provider_stream_incomplete'])
        result = json.loads(done['result'])
        self.assertEqual(result['failure_code'], 'owner_stopped')
        self.assertFalse(result['success'])
        self.assertEqual(done['status'], 'interrupted')

    async def test_guard_missing_proof_blocks_prompt(self):
        self.scenario(guard_unavailable=True)
        result = await self.run_worker()
        self.assertFalse(result.success)
        self.assertEqual(result.failure_code, 'provider_guard_unavailable')
        self.assertEqual([r['message']['method'] for r in self.calls()], ['initialize'])

    async def test_guard_previous_launch_proof_cannot_pass(self):
        result = await self.run_worker()
        self.assertTrue(result.success, result)
        before = len(self.calls())
        self.scenario(guard_unavailable=True)
        result = await self.run_worker()
        self.assertFalse(result.success)
        self.assertEqual(result.failure_code, 'provider_guard_unavailable')
        self.assertEqual([r['message']['method'] for r in self.calls()[before:]], ['initialize'])

    async def test_managed_preferences_refused_before_launch(self):
        for path in self.managed_paths:
            with self.subTest(path=path.name):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text('host policy must not be read')
                try:
                    result = await self.run_worker()
                    self.assertFalse(result.success)
                    self.assertEqual(result.failure_code, 'provider_managed_config')
                    self.assertEqual(self.calls(), [])
                finally:
                    path.unlink()


    def launch_root(self, result):
        spec = json.loads((Path(result.log_path).parent / 'spec.json').read_text())
        return Path(spec['env']['XDG_CONFIG_HOME']).parent

    def messages(self):
        return [row['message'] for row in self.calls()]

    async def wait_prompt(self, count=1):
        async def wait():
            while sum(e.get('method') == 'session/prompt' for e in self.messages()) < count:
                await asyncio.sleep(0.01)
        await asyncio.wait_for(wait(), 10)

    def service(self, pool):
        service = Service(self.store, None, self.runner, self.root)
        service.workers = pool
        return service

    async def test_owner_stop_sends_cancel_before_finish_and_beats_marker(self):
        self.scenario(wait_cancel=True)
        pool, task = self.make_pool()
        wid = self.spawn(task).data['worker']
        run = asyncio.create_task(pool.run(pool.get(wid), self.store.topic('1:2')))
        self.addAsyncCleanup(self.settle_run, run)
        await self.wait_prompt()
        observed = []
        original = opencode_runtime.finish
        def finish(result, root, alias, error, **kwargs):
            observed.append(result.failure_code)
            marker_root = kwargs.get('launch_root', root)
            (Path(marker_root) / 'stream-incomplete.json').write_text(json.dumps({
                'version': 1, 'consecutive': 3, 'code': 'provider_stream_incomplete', 'session_id': SESSION_ID}))
            original(result, root, alias, error, **kwargs)
        with patch('coordinator.opencode_runtime.finish', finish):
            await pool.stop(wid)
            done = await asyncio.wait_for(run, 10)
        self.assertEqual(observed, ['owner_stopped'])
        self.assertEqual(json.loads(done['result'])['failure_code'], 'owner_stopped')
        self.assertEqual(done['status'], 'interrupted')
        self.assertEqual([e.get('method') for e in self.messages()],
                         ['initialize', 'session/new', 'session/prompt', 'session/cancel'])
        self.assert_no_cancel_failed(wid)

    async def wait_until(self, predicate):
        async def wait():
            while not predicate():
                await asyncio.sleep(0.01)
        await asyncio.wait_for(wait(), 10)

    async def start_cancel_case(self, mode):
        self.scenario(wait_cancel=True, cancel_mode=mode)
        pool, task = self.make_pool()
        wid = self.spawn(task).data['worker']
        run = asyncio.create_task(pool.run(pool.get(wid), self.store.topic('1:2')))
        self.addAsyncCleanup(self.settle_run, run)
        await self.wait_prompt()
        return pool, wid, run

    def assert_owner_stopped(self, done):
        result = json.loads(done['result'])
        self.assertEqual((done['status'], result['failure_code'], result['success']),
                         ('interrupted', 'owner_stopped', False))
        self.assertEqual([e.get('method') for e in self.messages()],
                         ['initialize', 'session/new', 'session/prompt', 'session/cancel'])
        self.assert_no_cancel_failed(done['id'])

    def assert_no_cancel_failed(self, wid):
        problems = self.store.db.execute("SELECT detail FROM problems WHERE code='cancel-failed' AND worker=?",
                                         (wid,)).fetchall()
        self.assertEqual([row['detail'] for row in problems], [])

    async def test_pool_owner_stop_survives_exit_during_cancel_grace(self):
        pool, wid, run = await self.start_cancel_case('exit')
        await asyncio.wait_for(pool.stop(wid), 10)
        self.assert_owner_stopped(await asyncio.wait_for(run, 10))

    async def test_pool_owner_stop_survives_stdout_close_during_cancel_grace(self):
        pool, wid, run = await self.start_cancel_case('close_stdout')
        stop = asyncio.create_task(pool.stop(wid))
        self.addAsyncCleanup(self.settle_run, stop)
        await self.wait_until((self.binary.parent / 'stdout-closed').exists)
        host = HostClient.attach(host_directory(self.store.directory, pool.get(wid)))
        os.kill(host.child_pid, 0)
        self.assertIsNone(host.returncode)
        await asyncio.wait_for(stop, 10)
        self.assert_owner_stopped(await asyncio.wait_for(run, 10))

    async def test_pool_owner_stop_survives_stdout_close_then_exit_during_cancel_grace(self):
        pool, wid, run = await self.start_cancel_case('close_then_exit')
        stop = asyncio.create_task(pool.stop(wid))
        self.addAsyncCleanup(self.settle_run, stop)
        await self.wait_until((self.binary.parent / 'stdout-closed').exists)
        await asyncio.wait_for(stop, 10)
        self.assert_owner_stopped(await asyncio.wait_for(run, 10))

    async def test_pool_owner_stop_survives_host_killed_during_cancel_grace(self):
        pool, wid, run = await self.start_cancel_case('ignore')
        directory = host_directory(self.store.directory, pool.get(wid))
        host_pid = int((directory / 'pid').read_text())
        stop = asyncio.create_task(pool.stop(wid))
        self.addAsyncCleanup(self.settle_run, stop)
        await self.wait_until(lambda: any(e.get('method') == 'session/cancel' for e in self.messages()))
        self.assertFalse(stop.done())
        command = subprocess.run(['ps', '-p', str(host_pid), '-o', 'command='],
                                 capture_output=True, text=True, check=True).stdout.strip()
        self.assertTrue(command.endswith(' -m coordinator host ' + str(directory.resolve())), command)
        os.kill(host_pid, signal.SIGKILL)
        await asyncio.wait_for(stop, 10)
        self.assert_owner_stopped(await asyncio.wait_for(run, 10))

    async def test_service_owner_stop_records_failed_cancel_writes(self):
        self.scenario(wait_cancel=True)
        pool, task = self.make_pool()
        service = self.service(pool)
        original = HostClient.request
        errors = (OSError(errno.EMFILE, 'Too many open files'), json.JSONDecodeError('Expecting value', '', 0))
        for index, error in enumerate(errors):
            with self.subTest(error=type(error).__name__):
                wid = self.spawn(task).data['worker']
                run = asyncio.create_task(pool.run(pool.get(wid), self.store.topic('1:2')))
                self.addAsyncCleanup(self.settle_run, run)
                await self.wait_prompt(index + 1)

                async def request(client, op, **fields):
                    if op == 'write' and json.loads(fields['line']).get('method') == 'session/cancel':
                        raise error
                    return await original(client, op, **fields)

                reply = control_api.call(self.store, 'workers.stop', {'worker': wid}, topic='1:2')
                self.assertTrue(reply.ok, reply.text)
                with patch.object(HostClient, 'request', request):
                    await asyncio.wait_for(service.controls_once(), 10)
                    done = await asyncio.wait_for(run, 10)
                result = json.loads(done['result'])
                self.assertEqual((done['status'], result['failure_code'], result['success']),
                                 ('interrupted', 'owner_stopped', False))
                row = self.store.db.execute('SELECT state FROM service_requests WHERE id=?',
                                            (reply.data['request'],)).fetchone()
                self.assertEqual(row['state'], 'done')
                problems = self.store.db.execute("SELECT area,detail FROM problems WHERE code='cancel-failed' AND worker=?",
                                                 (wid,)).fetchall()
                self.assertEqual(len(problems), 1)
                self.assertEqual(problems[0]['area'], 'worker')
                self.assertIn(type(error).__name__ + ':', problems[0]['detail'])
                self.assertIn('test_opencode_runtime.py:', problems[0]['detail'])

    async def test_pool_owner_stop_survives_prompt_finishing_during_cancel_grace(self):
        pool, wid, run = await self.start_cancel_case('finish')
        await asyncio.wait_for(pool.stop(wid), 10)
        done = await asyncio.wait_for(run, 10)
        self.assert_owner_stopped(done)
        result = json.loads(done['result'])
        events = [json.loads(json.loads(line)['data']) for line in
                  (Path(result['log_path']).parent / 'events.jsonl').read_text().splitlines()]
        self.assertTrue(any(event.get('result', {}).get('stopReason') == 'end_turn' for event in events))

    async def test_service_owner_stop_survives_exit_during_cancel_grace(self):
        pool, wid, run = await self.start_cancel_case('exit')
        service = self.service(pool)
        service.extension = SimpleNamespace(start=AsyncMock(), close=AsyncMock())
        ready = asyncio.Event()
        with patch('coordinator.reactions.check_administrators', new=AsyncMock()), \
                patch.object(service, 'start_session', new=AsyncMock()), \
                patch.object(service, 'announce_start', new=AsyncMock(side_effect=ready.set)), \
                patch.object(service, 'supervised', return_value=[('controls', service.controls)]):
            serving = asyncio.create_task(service.run())
            try:
                await asyncio.wait_for(ready.wait(), 10)
                reply = control_api.call(self.store, 'workers.stop', {'worker': wid}, topic='1:2')
                self.assertTrue(reply.ok, reply.text)
                request = self.store.db.execute("SELECT id FROM service_requests WHERE op!='_cleanup.host_logs' "
                                                'ORDER BY id DESC LIMIT 1').fetchone()[0]
                service.wake('controls').set()
                def stopped():
                    return self.store.db.execute('SELECT state FROM service_requests WHERE id=?',
                                                 (request,)).fetchone()[0] == 'done'
                await self.wait_until(lambda: stopped() or serving.done())
                self.assertFalse(serving.done(), 'Service.run ended while handling the owner stop')
                self.assertTrue(stopped())
                self.assert_owner_stopped(await asyncio.wait_for(run, 10))
                later = self.spawn(self.store.task_get(pool.get(wid)['task'])).data['worker']
                reply = control_api.call(self.store, 'workers.stop', {'worker': later}, topic='1:2')
                self.assertTrue(reply.ok, reply.text)
                request = self.store.db.execute("SELECT id FROM service_requests WHERE op!='_cleanup.host_logs' "
                                                'ORDER BY id DESC LIMIT 1').fetchone()[0]
                service.wake('controls').set()
                await self.wait_until(lambda: stopped() or serving.done())
                self.assertFalse(serving.done(), 'Service.run ended before the later control request')
                self.assertTrue(stopped())
                self.assertEqual(pool.get(later)['status'], 'interrupted')
            finally:
                serving.cancel()
                await asyncio.wait_for(asyncio.gather(serving, return_exceptions=True), 10)

    async def settle_run(self, run):
        if not run.done():
            run.cancel()
        await asyncio.gather(run, return_exceptions=True)

    async def test_running_steers_queue_join_and_deliver_without_problem(self):
        release = self.root / 'release'
        self.scenario(release=str(release))
        pool, task = self.make_pool()
        wid = self.spawn(task).data['worker']
        service = self.service(pool)
        run = asyncio.create_task(pool.run(pool.get(wid), self.store.topic('1:2')))
        self.addAsyncCleanup(self.settle_run, run)
        await self.wait_prompt()
        control = pool.controls[wid]
        for text in ('first steer', 'second steer'):
            reply = control_api.call(self.store, 'workers.steer', {'worker': wid, 'prompt': text}, topic='1:2')
            self.assertTrue(reply.ok, reply.text)
            await service.controls_once()
            row = self.store.db.execute("SELECT * FROM service_requests WHERE op!='_cleanup.host_logs' "
                                        'ORDER BY id DESC LIMIT 1').fetchone()
            self.assertEqual((row['state'], json.loads(row['result'])['receipt']), ('done', 'queued'))
        self.assertEqual(sum(e.get('method') == 'session/prompt' for e in self.messages()), 1)
        release.touch()
        done = await asyncio.wait_for(run, 10)
        self.assertTrue(json.loads(done['result'])['success'], done['result'])
        prompts = [e['params']['prompt'] for e in self.messages() if e.get('method') == 'session/prompt']
        self.assertEqual(prompts[1], [{'type': 'text', 'text': 'first steer\n\nsecond steer'}])
        self.assertEqual(len(prompts), 2)
        self.assertEqual(await control.steer('after-close', 'late'), 'closed')
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM service_requests "
                                               "WHERE op!='_cleanup.host_logs' AND state!='done'").fetchone()[0], 0)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM problems WHERE code LIKE 'steer-%'").fetchone()[0], 0)

    async def test_undelivered_queued_steer_records_uncertain_when_run_ends(self):
        self.scenario(wait_cancel=True)
        pool, task = self.make_pool()
        wid = self.spawn(task).data['worker']
        service = self.service(pool)
        run = asyncio.create_task(pool.run(pool.get(wid), self.store.topic('1:2')))
        self.addAsyncCleanup(self.settle_run, run)
        await self.wait_prompt()
        control_api.call(self.store, 'workers.steer', {'worker': wid, 'prompt': 'unconfirmed'}, topic='1:2')
        await service.controls_once()
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM problems WHERE code='steer-uncertain'").fetchone()[0], 0)
        await pool.stop(wid)
        await asyncio.wait_for(run, 10)
        self.assert_no_cancel_failed(wid)
        row = self.store.db.execute("SELECT * FROM service_requests WHERE op!='_cleanup.host_logs' "
                                    'ORDER BY id DESC LIMIT 1').fetchone()
        self.assertEqual(row['state'], 'uncertain')
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM problems WHERE code='steer-uncertain'").fetchone()[0], 1)

    async def finished_worker(self):
        pool, task = self.make_pool()
        wid = self.spawn(task).data['worker']
        done = await pool.run(pool.get(wid), self.store.topic('1:2'))
        self.assertTrue(json.loads(done['result'])['success'], done['result'])
        return pool, wid

    async def test_finished_steer_refused_while_opencode_off(self):
        pool, wid = await self.finished_worker()
        self.store.put('opencode_enabled', False)
        before = pool.get(wid)
        requests = self.store.db.execute('SELECT count(*) FROM service_requests').fetchone()[0]
        reply = control_api.call(self.store, 'workers.steer', {'worker': wid, 'prompt': 'no run'}, topic='1:2')
        self.assertFalse(reply.ok)
        self.assertEqual(reply.text, 'OpenCode workers are off.')
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM service_requests').fetchone()[0], requests)
        self.assertEqual(pool.get(wid), before)

    async def test_running_steer_accepted_while_opencode_off(self):
        pool, task = self.make_pool()
        for enabled in (True, False):
            with self.subTest(opencode_enabled=enabled):
                release = self.root / ('release-' + str(enabled))
                self.scenario(release=str(release))
                self.store.put('opencode_enabled', True)
                wid = self.spawn(task).data['worker']
                prompts_before = sum(e.get('method') == 'session/prompt' for e in self.messages())
                run = asyncio.create_task(pool.run(pool.get(wid), self.store.topic('1:2')))
                self.addAsyncCleanup(self.settle_run, run)
                await self.wait_prompt(prompts_before + 1)
                self.assertEqual(pool.get(wid)['status'], 'running')
                self.store.put('opencode_enabled', enabled)
                reply = control_api.call(self.store, 'workers.steer',
                                         {'worker': wid, 'prompt': 'live follow-up'}, topic='1:2')
                self.assertTrue(reply.ok, reply.text)
                self.assertEqual(reply.text, 'Queued for the running service.')
                await self.service(pool).controls_once()
                release.touch()
                done = await asyncio.wait_for(run, 10)
                result = json.loads(done['result'])
                self.assertEqual((done['status'], result['success'], result['failure_code'], result['text']),
                                 ('done', True, None, 'fresh answer'))
                prompts = [e['params']['prompt'] for e in self.messages() if e.get('method') == 'session/prompt']
                self.assertEqual(prompts[prompts_before + 1:], [[{'type': 'text', 'text': 'live follow-up'}]])
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM problems WHERE code LIKE 'steer-%'").fetchone()[0], 0)

    async def test_finished_steer_accepted_while_opencode_on(self):
        pool, wid = await self.finished_worker()
        reply = control_api.call(self.store, 'workers.steer', {'worker': wid, 'prompt': 'follow-up'}, topic='1:2')
        self.assertTrue(reply.ok, reply.text)
        self.assertEqual(reply.text, 'Queued for the running service.')
        row = self.store.db.execute('SELECT op,state FROM service_requests WHERE id=?',
                                    (reply.data['request'],)).fetchone()
        self.assertEqual((row['op'], row['state']), ('workers.steer', 'queued'))
        await self.service(pool).controls_once()
        self.assertEqual(pool.get(wid)['status'], 'queued')

    async def test_queued_finished_steer_refused_after_opencode_switched_off(self):
        pool, wid = await self.finished_worker()
        before = pool.get(wid)
        calls = self.calls()
        hosts = sorted((self.store.directory / 'hosts').iterdir())
        reply = control_api.call(self.store, 'workers.steer', {'worker': wid, 'prompt': 'no run'}, topic='1:2')
        self.assertTrue(reply.ok, reply.text)
        self.store.put('opencode_enabled', False)
        await self.service(pool).controls_once()
        row = self.store.db.execute('SELECT state,result FROM service_requests WHERE id=?',
                                    (reply.data['request'],)).fetchone()
        self.assertEqual((row['state'], json.loads(row['result'])['text']),
                         ('refused', 'Worker cannot receive input.'))
        problems = self.store.db.execute("SELECT area FROM problems WHERE code='steer-refused' AND worker=?",
                                         (wid,)).fetchall()
        self.assertEqual([row['area'] for row in problems], ['worker'])
        self.assertEqual(pool.get(wid), before)
        self.assertEqual(self.calls(), calls)
        self.assertEqual(sorted((self.store.directory / 'hosts').iterdir()), hosts)

    async def test_cancel_hooks_cleared_after_run_and_owner_stopped_resume(self):
        pool, wid = await self.finished_worker()
        self.assertNotIn(wid, pool.cancels)
        reply = control_api.call(self.store, 'workers.steer', {'worker': wid, 'prompt': 'resume'}, topic='1:2')
        self.assertTrue(reply.ok, reply.text)
        await self.service(pool).controls_once()
        self.scenario(wait_cancel=True)
        run = asyncio.create_task(pool.run(pool.get(wid), self.store.topic('1:2')))
        self.addAsyncCleanup(self.settle_run, run)
        await self.wait_prompt(2)
        self.assertIn(wid, pool.cancels)
        await asyncio.wait_for(pool.stop(wid), 10)
        done = await asyncio.wait_for(run, 10)
        self.assertEqual((done['status'], json.loads(done['result'])['failure_code']),
                         ('interrupted', 'owner_stopped'))
        self.assert_no_cancel_failed(wid)
        self.assertNotIn(wid, pool.cancels)

    async def test_finished_steer_loads_exact_session_and_reports_once_on_pinned_account(self):
        pool, wid = await self.finished_worker()
        before = len(self.calls())
        old_spec = json.loads((Path(json.loads(pool.get(wid)['result'])['log_path']).parent / 'spec.json').read_text())
        self.store.put('opencode_accounts', {'one': dict(ACCOUNT), 'two': dict(ACCOUNT, model='other')})
        self.store.put('opencode_active_account', 'two')
        self.scenario(history_reply=True)
        reply = control_api.call(self.store, 'workers.steer', {'worker': wid, 'prompt': 'explicit follow-up'}, topic='1:2')
        self.assertTrue(reply.ok, reply.text)
        await self.service(pool).controls_once()
        self.assertEqual(pool.get(wid)['status'], 'queued')
        done = await pool.run(pool.get(wid), self.store.topic('1:2'))
        result = json.loads(done['result'])
        self.assertTrue(result['success'], result)
        self.assertIn('explicit follow-up', result['text'])
        self.assertIn('short prompt', result['text'])
        self.assertEqual(done['account_alias'], 'one')
        spec = json.loads((Path(result['log_path']).parent / 'spec.json').read_text())
        self.assertEqual(spec['env']['XDG_DATA_HOME'], old_spec['env']['XDG_DATA_HOME'])
        self.assertEqual((spec['account'], spec['model']), ('one', 'example-model'))
        messages = [r['message'] for r in self.calls()[before:]]
        self.assertEqual([e['method'] for e in messages], ['initialize', 'session/load', 'session/prompt'])
        self.assertEqual(messages[1]['params']['sessionId'], SESSION_ID)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM messages WHERE source_worker=?', (wid,)).fetchone()[0], 2)

    async def test_unknown_resume_has_no_new_session_or_prompt(self):
        result = await self.runner.run('opencode', 'resume', self.cwd, SESSION_ID, fresh=False,
                                       worker_id=31, account_alias='one')
        self.assertEqual(result.failure_code, 'session_missing')
        self.assertFalse(result.success)
        self.assertEqual([e['method'] for e in self.messages()], ['initialize', 'session/load'])

    async def test_resume_account_unavailable_launches_nothing(self):
        pool, wid = await self.finished_worker()
        for change in ('gone', 'disabled', 'unready', 'unpicked'):
            with self.subTest(change=change):
                self.store.put('opencode_accounts', {'one': dict(ACCOUNT), 'two': dict(ACCOUNT)})
                self.store.put('opencode_active_account', 'two')
                self.key.chmod(0o600)
                if change == 'gone':
                    self.store.put('opencode_accounts', {'two': dict(ACCOUNT)})
                elif change == 'disabled':
                    self.store.put('opencode_accounts', {'one': dict(ACCOUNT, enabled=False), 'two': dict(ACCOUNT)})
                elif change == 'unready':
                    self.key.chmod(0o644)
                self.store.db.execute("UPDATE workers SET status='queued',fresh=0,account_alias=?,prompt='follow-up' WHERE id=?",
                                      (None if change == 'unpicked' else 'one', wid))
                before = len(self.calls())
                done = await pool.run(pool.get(wid), self.store.topic('1:2'))
                self.assertEqual(json.loads(done['result'])['failure_code'], 'account_changed')
                self.assertEqual(len(self.calls()), before)
        self.key.chmod(0o600)

    async def test_unsupported_finished_provider_refused_by_api_and_service(self):
        pool, wid = await self.finished_worker()
        service = self.service(pool)
        for provider in ('future-provider',):
            with self.subTest(provider=provider):
                self.store.db.execute("UPDATE workers SET provider=?,status='done' WHERE id=?", (provider, wid))
                reply = control_api.call(self.store, 'workers.steer', {'worker': wid, 'prompt': 'no run'}, topic='1:2')
                self.assertFalse(reply.ok, reply.text)
                self.store.service_request('workers.steer', {'worker': wid, 'prompt': 'no run', '_topic': '1:2'})
                await service.controls_once()
                self.assertEqual(pool.get(wid)['status'], 'done')
                row = self.store.db.execute("SELECT state FROM service_requests WHERE op!='_cleanup.host_logs' "
                                            'ORDER BY id DESC LIMIT 1').fetchone()
                self.assertEqual(row['state'], 'refused')

    async def test_worker_spawn_uses_own_pin_during_setting_switch(self):
        paths = {v: install_versioned_opencode(self.root / 'versions', v) for v in ('1.19.0', '1.19.3')}
        pool, task = self.make_pool()
        for pin, selected in (('1.19.0', '1.19.3'), ('1.19.3', '1.19.0')):
            with self.subTest(pin=pin), patch('coordinator.opencode_accounts.PINNED_VERSION', pin):
                self.store.put('opencode_binary', str(paths[selected]))
                runner = ProviderRunner(self.store.directory, opencode_store=self.store)
                self.assertEqual(runner.binaries['opencode'], str(paths[pin]))
                pool = WorkerPool(self.store, self.runner, AccountBroker(self.store))
                self.assertEqual(self.runner.binaries['opencode'], str(paths[pin]))
                wid = self.spawn(task).data['worker']
                self.runner.binaries['opencode'] = str(paths[selected])
                done = await asyncio.wait_for(pool.run(pool.get(wid), self.store.topic('1:2')), 20)
                result = json.loads(done['result'])
                self.assertEqual((done['status'], result['success'], result['failure_code']), ('done', True, None))
                spec = json.loads((Path(result['log_path']).parent / 'spec.json').read_text())
                self.assertEqual(spec['argv'][0], str(paths[pin]))
                self.assertEqual(self.store.get('opencode_binary'), str(paths[selected]))

    async def test_pin_change_reattaches_worker_and_finishes_original_turn(self):
        old = install_versioned_opencode(self.root / 'versions', '1.19.0')
        install_versioned_opencode(self.root / 'versions', '1.19.3')
        self.binary = old
        self.store.put('opencode_binary', str(old))
        release = self.root / 'release'
        self.scenario(version='1.19.0', release=str(release))
        with patch('coordinator.opencode_accounts.PINNED_VERSION', '1.19.0'):
            pool, task = self.make_pool()
            wid = self.spawn(task).data['worker']
            run = asyncio.create_task(pool.run(pool.get(wid), self.store.topic('1:2')))
            self.addAsyncCleanup(self.settle_run, run)
            await self.wait_prompt()
            host = pool.get(wid)['host']
            run.cancel()
            await asyncio.gather(run, return_exceptions=True)
        with patch('coordinator.opencode_accounts.PINNED_VERSION', '1.19.3'):
            replacement = WorkerPool(self.store, ProviderRunner(self.store.directory, opencode_store=self.store), AccountBroker(self.store))
            attached = asyncio.create_task(replacement.run(replacement.get(wid), self.store.topic('1:2')))
            self.addAsyncCleanup(self.settle_run, attached)
            await asyncio.sleep(0.15)
            self.assertFalse(attached.done())
            release.touch()
            done = await asyncio.wait_for(attached, 10)
            result = json.loads(done['result'])
            self.assertEqual((done['status'], result['success'], result['failure_code'], result['text']),
                             ('done', True, None, 'fresh answer'))
            self.assertEqual(done['host'], host)
            self.assertEqual([e['method'] for e in self.messages()], ['initialize', 'session/new', 'session/prompt'])

    async def test_finished_worker_steer_after_pin_change_loads_new_binary(self):
        old = install_versioned_opencode(self.root / 'versions', '1.19.0')
        new = install_versioned_opencode(self.root / 'versions', '1.19.3')
        self.store.put('opencode_binary', str(old))
        with patch('coordinator.opencode_accounts.PINNED_VERSION', '1.19.0'):
            pool, wid = await self.finished_worker()
        previous = pool.get(wid)
        old_result = json.loads(previous['result'])
        old_spec = json.loads((Path(old_result['log_path']).parent / 'spec.json').read_text())
        self.assertEqual(old_spec['argv'][0], str(old))
        with patch('coordinator.opencode_accounts.PINNED_VERSION', '1.19.3'):
            reply = control_api.call(self.store, 'workers.steer', {'worker': wid, 'prompt': 'follow-up'}, topic='1:2')
            self.assertTrue(reply.ok, reply.text)
            await self.service(pool).controls_once()
            done = await asyncio.wait_for(pool.run(pool.get(wid), self.store.topic('1:2')), 20)
            result = json.loads(done['result'])
            self.assertTrue(result['success'], result)
            self.assertEqual(result['session_id'], SESSION_ID)
            spec = json.loads((Path(result['log_path']).parent / 'spec.json').read_text())
            self.assertEqual(spec['argv'][0], str(new))
            self.assertNotEqual(done['host'], previous['host'])
            self.assertEqual(spec['account'], old_spec['account'])
            self.assertEqual(spec['env']['XDG_DATA_HOME'], old_spec['env']['XDG_DATA_HOME'])
            messages = [row['message'] for row in read_jsonl(new.parent / 'calls.jsonl')]
            self.assertEqual([e['method'] for e in messages], ['initialize', 'session/load', 'session/prompt'])
            self.assertEqual(messages[1]['params']['sessionId'], SESSION_ID)

    async def test_restart_reattaches_live_host_without_second_prompt(self):
        release = self.root / 'release'
        self.scenario(release=str(release))
        pool, task = self.make_pool()
        wid = self.spawn(task).data['worker']
        run = asyncio.create_task(pool.run(pool.get(wid), self.store.topic('1:2')))
        await self.wait_prompt()
        run.cancel()
        await asyncio.gather(run, return_exceptions=True)
        self.assertEqual(pool.get(wid)['status'], 'running')
        replacement = WorkerPool(self.store, ProviderRunner(self.store.directory, opencode_store=self.store), AccountBroker(self.store))
        attached = asyncio.create_task(replacement.run(replacement.get(wid), self.store.topic('1:2')))
        self.addAsyncCleanup(self.settle_run, attached)
        await asyncio.sleep(0.15)
        release.touch()
        done = await asyncio.wait_for(attached, 10)
        self.assertTrue(json.loads(done['result'])['success'], done['result'])
        self.assertEqual(json.loads(done['result'])['text'], 'fresh answer')
        self.assertEqual([e['method'] for e in self.messages()], ['initialize', 'session/new', 'session/prompt'])

    async def test_root_reuse_preserves_history_and_ignores_all_old_launch_inputs(self):
        first = await self.run_worker()
        self.assertTrue(first.success, first)
        old_spec = json.loads((Path(first.log_path).parent / 'spec.json').read_text())
        old = Path(old_spec['env']['XDG_CONFIG_HOME']).parent
        stale = {'version': 1, 'consecutive': 3, 'code': 'provider_stream_incomplete', 'session_id': SESSION_ID}
        (old / 'stream-incomplete.json').write_text(json.dumps(stale))
        for folder in ('config/opencode/plugins', 'config/opencode/skills/old', 'home/.opencode', 'managed', 'cache', 'state', 'tmp'):
            directory = old / folder
            directory.mkdir(parents=True, exist_ok=True)
            (directory / 'stale').write_text('must survive but never load')
        (old / 'config/opencode/plugins/hostile.js').write_text('throw new Error("old plugin");')
        before = {str(p.relative_to(old)): p.read_bytes() for p in old.rglob('*') if p.is_file()}
        self.scenario(history_reply=True)
        result = await self.runner.run('opencode', 'next prompt', self.cwd, SESSION_ID, fresh=False,
                                       worker_id=31, account_alias='one')
        self.assertTrue(result.success, result)
        self.assertEqual(result.text, 'history: short prompt; next: next prompt')
        spec = json.loads((Path(result.log_path).parent / 'spec.json').read_text())
        self.assertEqual(spec['env']['XDG_DATA_HOME'], old_spec['env']['XDG_DATA_HOME'])
        for name in ('HOME', 'XDG_CONFIG_HOME', 'XDG_CACHE_HOME', 'XDG_STATE_HOME', 'TMPDIR', 'OPENCODE_TEST_MANAGED_CONFIG_DIR'):
            self.assertNotEqual(spec['env'][name], old_spec['env'][name], name)
            self.assertEqual(list(Path(spec['env'][name]).rglob('stale')), [], name)
        self.assertEqual({str(p.relative_to(old)): p.read_bytes() for p in old.rglob('*') if p.is_file()}, before)
        self.assertEqual([p.name for p in (Path(spec['env']['XDG_CONFIG_HOME']) / 'opencode/plugins').iterdir()], ['guard.js'])

    async def test_reused_root_cannot_accept_old_guard_proof(self):
        first = await self.run_worker()
        spec = json.loads((Path(first.log_path).parent / 'spec.json').read_text())
        old = Path(spec['env']['XDG_CONFIG_HOME']).parent
        proof = (old / 'guard-ready.json').read_bytes()
        self.scenario(guard_unavailable=True)
        original = opencode_runtime.write_config
        def write(*args):
            path = original(*args)
            (Path(args[0]) / 'guard-ready.json').write_bytes(proof)
            return path
        with patch('coordinator.opencode_runtime.write_config', write):
            result = await self.runner.run('opencode', 'next', self.cwd, SESSION_ID, fresh=False,
                                           worker_id=31, account_alias='one')
        self.assertEqual(result.failure_code, 'provider_guard_unavailable')


    async def test_second_resume_is_stopped_by_host_lock(self):
        first = await self.run_worker()
        self.assertTrue(first.success, first)
        release = self.root / 'release'
        self.scenario(release=str(release))
        running = asyncio.create_task(self.runner.run('opencode', 'resume one', self.cwd, SESSION_ID,
            fresh=False, worker_id=31, host_id='resume-one', account_alias='one'))
        self.addAsyncCleanup(self.settle_run, running)
        await self.wait_prompt(2)
        before = len(self.calls())
        with patch.object(self.runner, '_host_owns', return_value=False), \
                patch.object(self.runner, '_external_writer', AsyncMock(return_value=False)):
            second = await asyncio.wait_for(self.runner.run('opencode', 'resume two', self.cwd, SESSION_ID,
                fresh=False, worker_id=31, host_id='resume-two', account_alias='one'), 15)
        self.assertFalse(second.success)
        self.assertEqual(second.failure_code, 'session_busy')
        self.assertEqual(second.error, 'Session is already owned by another provider host')
        self.assertEqual(len(self.calls()), before)
        release.touch()
        done = await asyncio.wait_for(running, 10)
        self.assertTrue(done.success, done)

    async def test_restart_reattaches_session_busy_host_without_opencode_call(self):
        first = await self.run_worker()
        self.assertTrue(first.success, first)
        release = self.root / 'release'
        self.scenario(release=str(release))
        running = asyncio.create_task(self.runner.run('opencode', 'resume one', self.cwd, SESSION_ID,
            fresh=False, worker_id=31, host_id='resume-one', account_alias='one'))
        self.addAsyncCleanup(self.settle_run, running)
        await self.wait_prompt(2)
        before = len(self.calls())
        with patch.object(self.runner, '_host_owns', return_value=False), \
                patch.object(self.runner, '_external_writer', AsyncMock(return_value=False)):
            refused = await asyncio.wait_for(self.runner.run('opencode', 'resume two', self.cwd, SESSION_ID,
                fresh=False, worker_id=31, host_id='resume-two', account_alias='one'), 15)
        self.assertEqual(refused.failure_code, 'session_busy')
        directory = self.store.directory / 'hosts' / 'resume-two'
        self.assertEqual(sorted(p.name for p in directory.iterdir()),
                         ['exit.json', 'launch-failed.json', 'spec.json'])
        self.assertEqual(json.loads((directory / 'exit.json').read_text()),
                         {'returncode': 1, 'failure_code': 'session_busy'})
        replacement = ProviderRunner(self.store.directory, opencode_store=self.store)
        attached = await asyncio.wait_for(replacement.run('opencode', 'resume two', self.cwd, SESSION_ID,
            fresh=False, worker_id=31, host_id='resume-two', account_alias='one', attach=True), 15)
        self.assertEqual(len(self.calls()), before)
        release.touch()
        done = await asyncio.wait_for(running, 10)
        self.assertTrue(done.success, done)
        self.assertFalse(attached.success)
        self.assertEqual((attached.failure_code, attached.error),
                         ('session_busy', 'Session is already owned by another provider host'))
        self.assertTrue(attached.failure_detail.startswith(
            'SessionBusy: Session is already owned by another provider host at host.py:'), attached)

    async def test_restarted_pool_settles_session_busy_host_once(self):
        first = await self.run_worker()
        self.assertTrue(first.success, first)
        release = self.root / 'release'
        self.scenario(release=str(release))
        running = asyncio.create_task(self.runner.run('opencode', 'resume one', self.cwd, SESSION_ID,
            fresh=False, worker_id=31, host_id='resume-one', account_alias='one'))
        self.addAsyncCleanup(self.settle_run, running)
        await self.wait_prompt(2)
        pool, task = self.make_pool()
        with self.store.db:
            wid = self.store.db.execute('''INSERT INTO workers
                (task,topic,provider,prompt,cwd,workspace,fresh,session,account_alias,status,host,created,updated)
                VALUES (?,?,?,?,?,?,0,?,?,'running',?,1,1)''',
                (task['id'], '1:2', 'opencode', 'resume two', str(self.cwd), '{}', SESSION_ID,
                 'one', 'resume-two')).lastrowid
        before = len(self.calls())
        with patch.object(self.runner, '_host_owns', return_value=False), \
                patch.object(self.runner, '_external_writer', AsyncMock(return_value=False)):
            refused = await asyncio.wait_for(self.runner.run('opencode', 'resume two', self.cwd, SESSION_ID,
                fresh=False, worker_id=wid, host_id='resume-two', account_alias='one'), 15)
        self.assertEqual(refused.failure_code, 'session_busy')
        self.assertEqual(pool.get(wid)['status'], 'running')
        directory = host_directory(self.store.directory, pool.get(wid))
        self.assertEqual(sorted(p.name for p in directory.iterdir()),
                         ['exit.json', 'launch-failed.json', 'spec.json'])
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM problems WHERE worker=?',
                                              (wid,)).fetchone()[0], 0)
        replacement = WorkerPool(self.store, ProviderRunner(self.store.directory, opencode_store=self.store),
                                 AccountBroker(self.store))
        done = await asyncio.wait_for(replacement.run(replacement.get(wid), self.store.topic('1:2')), 15)
        self.assertEqual(len(self.calls()), before)
        release.touch()
        first_done = await asyncio.wait_for(running, 10)
        self.assertTrue(first_done.success, first_done)
        result = json.loads(done['result'])
        self.assertEqual(done['status'], 'done')
        self.assertFalse(result['success'])
        self.assertEqual((result['failure_code'], result['error']),
                         ('session_busy', 'Session is already owned by another provider host'))
        problems = self.store.db.execute('SELECT area,code FROM problems WHERE worker=? ORDER BY id',
                                         (wid,)).fetchall()
        self.assertEqual([(p['area'], p['code']) for p in problems], [('worker', 'session_busy')])

    async def _session_busy_restart_window(self):
        first = await self.run_worker()
        self.assertTrue(first.success, first)
        release = self.root / 'release'
        self.scenario(release=str(release))
        running = asyncio.create_task(self.runner.run('opencode', 'resume one', self.cwd, SESSION_ID,
            fresh=False, worker_id=31, host_id='resume-one', account_alias='one'))
        async def finish_running_host():
            release.touch()
            await asyncio.wait_for(asyncio.gather(running, return_exceptions=True), 10)
        self.addAsyncCleanup(finish_running_host)
        await self.wait_prompt(2)
        pool, task = self.make_pool()
        with self.store.db:
            wid = self.store.db.execute('''INSERT INTO workers
                (task,topic,provider,prompt,cwd,workspace,fresh,session,account_alias,status,host,created,updated)
                VALUES (?,?,?,?,?,?,0,?,?,'running',?,1,1)''',
                (task['id'], '1:2', 'opencode', 'resume two', str(self.cwd), '{}', SESSION_ID,
                 'one', 'resume-two')).lastrowid
        before = len(self.calls())
        with patch.object(self.runner, '_host_owns', return_value=False), \
                patch.object(self.runner, '_external_writer', AsyncMock(return_value=False)):
            refused = await asyncio.wait_for(self.runner.run('opencode', 'resume two', self.cwd, SESSION_ID,
                fresh=False, worker_id=wid, host_id='resume-two', account_alias='one'), 15)
        self.assertEqual(refused.failure_code, 'session_busy')
        self.assertEqual(pool.get(wid)['status'], 'running')
        replacement = WorkerPool(self.store, ProviderRunner(self.store.directory, opencode_store=self.store),
                                 AccountBroker(self.store))
        return release, running, before, replacement, wid

    async def test_owner_stop_of_session_busy_host_settles_stopped(self):
        release, running, before, replacement, wid = await self._session_busy_restart_window()
        service = self.service(replacement)
        reply = control_api.call(self.store, 'workers.stop', {'worker': wid}, topic='1:2')
        self.assertTrue(reply.ok, reply.text)
        self.assertTrue(await service.controls_once())
        request = self.store.db.execute('SELECT state,result FROM service_requests WHERE id=?',
                                        (reply.data['request'],)).fetchone()
        self.assertEqual(request['state'], 'done')
        self.assertEqual(json.loads(request['result']), {'stop': 'stopped'})
        problems = self.store.db.execute('SELECT area,code FROM problems WHERE worker=? ORDER BY id',
                                         (wid,)).fetchall()
        self.assertEqual([tuple(problem) for problem in problems], [])
        done = await asyncio.wait_for(replacement.run(replacement.get(wid), self.store.topic('1:2')), 15)
        self.assertEqual(len(self.calls()), before)
        release.touch()
        first_done = await asyncio.wait_for(running, 10)
        self.assertTrue(first_done.success, first_done)
        result = json.loads(done['result'])
        self.assertEqual(done['status'], 'done')
        self.assertFalse(result['success'])
        self.assertEqual((result['failure_code'], result['error']),
                         ('session_busy', 'Session is already owned by another provider host'))
        problems = self.store.db.execute('SELECT area,code FROM problems WHERE worker=? ORDER BY id',
                                         (wid,)).fetchall()
        self.assertEqual([tuple(problem) for problem in problems], [('worker', 'session_busy')])

    async def test_owner_stop_during_session_busy_reattach_preserves_run_result(self):
        release, running, before, replacement, wid = await self._session_busy_restart_window()
        service = self.service(replacement)
        started = asyncio.Event()
        continue_reattach = asyncio.Event()
        stop_entered = asyncio.Event()
        original_run = replacement.runner.run
        original_stop = replacement.stop

        async def gated_run(*args, **kwargs):
            started.set()
            await continue_reattach.wait()
            return await original_run(*args, **kwargs)

        async def observed_stop(worker_id):
            await original_stop(worker_id)
            stop_entered.set()

        with patch.object(replacement.runner, 'run', gated_run), \
                patch.object(replacement, 'stop', observed_stop):
            reattached = asyncio.create_task(replacement.run(replacement.get(wid), self.store.topic('1:2')))
            self.addAsyncCleanup(self.settle_run, reattached)
            await asyncio.wait_for(started.wait(), 10)
            self.assertFalse(reattached.done())
            reply = control_api.call(self.store, 'workers.stop', {'worker': wid}, topic='1:2')
            self.assertTrue(reply.ok, reply.text)
            control = asyncio.create_task(service.controls_once())
            self.addAsyncCleanup(self.settle_run, control)
            self.addAsyncCleanup(continue_reattach.set)
            await asyncio.wait_for(stop_entered.wait(), 10)
            continue_reattach.set()
            self.assertTrue(await asyncio.wait_for(control, 10))
            request = self.store.db.execute('SELECT state,result FROM service_requests WHERE id=?',
                                            (reply.data['request'],)).fetchone()
            self.assertEqual(request['state'], 'done')
            self.assertEqual(json.loads(request['result']), {'stop': 'stopped'})
            done = await asyncio.wait_for(reattached, 15)
        self.assertEqual(len(self.calls()), before)
        release.touch()
        first_done = await asyncio.wait_for(running, 10)
        self.assertTrue(first_done.success, first_done)
        result = json.loads(done['result'])
        self.assertEqual(done['status'], 'done')
        self.assertFalse(result['success'])
        self.assertEqual((result['failure_code'], result['error']),
                         ('session_busy', 'Session is already owned by another provider host'))
        problems = self.store.db.execute('SELECT area,code FROM problems WHERE worker=? ORDER BY id',
                                         (wid,)).fetchall()
        self.assertEqual([tuple(problem) for problem in problems], [('worker', 'session_busy')])

    async def test_owner_stop_of_dead_host_without_exit_record_is_uncertain(self):
        pool, task = self.make_pool()
        with self.store.db:
            wid = self.store.db.execute('''INSERT INTO workers
                (task,topic,provider,prompt,cwd,workspace,fresh,session,account_alias,status,host,created,updated)
                VALUES (?,?,?,?,?,?,0,?,?,'running',?,1,1)''',
                (task['id'], '1:2', 'opencode', 'resume two', str(self.cwd), '{}', SESSION_ID,
                 'one', 'dead-stop')).lastrowid
        directory = host_directory(self.store.directory, pool.get(wid))
        directory.mkdir(parents=True)
        self.assertEqual(HostClient(directory).state, 'dead')
        self.assertFalse((directory / 'exit.json').exists())
        service = self.service(pool)
        reply = control_api.call(self.store, 'workers.stop', {'worker': wid}, topic='1:2')
        self.assertTrue(reply.ok, reply.text)
        self.assertTrue(await service.controls_once())
        request = self.store.db.execute('SELECT state,result FROM service_requests WHERE id=?',
                                        (reply.data['request'],)).fetchone()
        self.assertEqual(request['state'], 'uncertain')
        self.assertEqual(json.loads(request['result']), {'text': 'The provider host is unavailable.'})
        problems = self.store.db.execute('SELECT area,code FROM problems WHERE worker=? ORDER BY id',
                                         (wid,)).fetchall()
        self.assertEqual([tuple(problem) for problem in problems], [('worker', 'stop-failed')])


    async def test_written_steer_receipt_is_recovered_from_journal(self):
        release = self.root / 'release'
        self.scenario(release=str(release))
        pool, task = self.make_pool()
        wid = self.spawn(task).data['worker']
        run = asyncio.create_task(pool.run(pool.get(wid), self.store.topic('1:2')))
        self.addAsyncCleanup(self.settle_run, run)
        await self.wait_prompt()
        pool.controls[wid].on_outcome = None
        control_api.call(self.store, 'workers.steer', {'worker': wid, 'prompt': 'durable steer'}, topic='1:2')
        await self.service(pool).controls_once()
        release.touch()
        done = await asyncio.wait_for(run, 10)
        self.assertTrue(json.loads(done['result'])['success'], done['result'])
        row = self.store.db.execute("SELECT state,result FROM service_requests WHERE op!='_cleanup.host_logs' "
                                    'ORDER BY id DESC LIMIT 1').fetchone()
        self.assertEqual(row['state'], 'done')
        self.assertEqual(json.loads(row['result']), {'receipt': 'received', 'reconciled': True})
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM problems WHERE code LIKE 'steer-%'").fetchone()[0], 0)

    def test_saved_auth_is_ignored_even_when_session_data_is_shared(self):
        adapter = opencode_runtime.adapter(lambda *args: None)
        env = adapter.environment(self.root / 'worker', {})
        self.assertEqual(env.get('OPENCODE_AUTH_CONTENT'), '{}')


    async def test_resume_rechecks_goal_and_secret_refusals_before_loading_secrets(self):
        pool, wid = await self.finished_worker()
        for kind in ('goal', 'secret'):
            with self.subTest(kind=kind):
                self.store.task_update(pool.get(wid)['task'], secrets=['DUMMY=VAULT_DUMMY'] if kind == 'secret' else [])
                self.store.db.execute("UPDATE workers SET status='queued',fresh=0,updated=updated+1,goal=? WHERE id=?",
                                      ('tests pass' if kind == 'goal' else None, wid))
                with patch.object(self.runner, 'run', AsyncMock()) as run, \
                        patch('coordinator.workers.load_for_worker', side_effect=AssertionError('secrets loaded')):
                    done = await pool.run(pool.get(wid), self.store.topic('1:2'))
                    run.assert_not_awaited()
                result = json.loads(done['result'])
                self.assertEqual(result['failure_code'], 'start_failed')
                self.assertIn("workers don't take", result['failure_detail'])
