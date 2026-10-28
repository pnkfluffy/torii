"""Offline worker scratch lifecycle and retained job results."""

import asyncio
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import AsyncMock, patch

from coordinator import control_api, mcp, worker_storage
from coordinator.accounts import AccountBroker
from coordinator.host import HostClient
from coordinator.providers import ProviderRunner, RunResult
from coordinator.service import Service
from coordinator.store import Store
from coordinator.workers import WorkerPool, host_directory
from tests.support import isolate_shared_mcp_home, stop_test_hosts, until_exists
from tests.test_codex_secrets_goals import SERVER


TOPIC = '1:2'
SID = '00000000-0000-4000-8000-000000000001'


class StorageSupport:
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.short = tempfile.TemporaryDirectory(prefix='tw-', dir='/tmp')
        self.addCleanup(self.short.cleanup)
        self.base = Path(self.short.name).resolve()
        isolate_shared_mcp_home(self, self.root)
        self.addCleanup(stop_test_hosts, self.root)
        self.store = Store(self.root / 'state')
        self.addCleanup(self.store.close)
        self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)",
                              (TOPIC, 1, 2, 'test', str(self.root)))
        self.store.put('accounts', {'one': {'config_dir': str(self.root / 'profile'), 'enabled': True}})
        self.store.put('account_status', {'one': {'observed_at': time.time(), 'identity': {'logged_in': True, 'email': 'one@example.test'},
                                                'usage': {'seven_day': {'utilization': 1}}}})
        (self.root / 'profile').mkdir()
        self.task = self.store.task_create(TOPIC, 'Build', worktree=str(self.root))
        self.expected_results = self.store.directory / 'results' / ('task-' + str(self.task['id']))

    def spawn(self, provider='claude', source='cli'):
        result = control_api.call(self.store, 'workers.spawn', {'task': self.task['id'], 'provider': provider,
                                  'prompt': 'build ' + provider}, source=source, topic=TOPIC)
        self.assertTrue(result.ok, result)
        worker = dict(self.store.db.execute('SELECT * FROM workers WHERE id=?',
                                           (result.data['worker'],)).fetchone())
        return worker, result

    def runner(self, provider='claude', hold=False):
        binary = self.root / ('fake-' + provider)
        observation = '''
import os, socket
from pathlib import Path
inherited = os.environ.get('TMPDIR')
scratch = Path(inherited) if inherited is not None else None
managed = scratch is not None and scratch.name.startswith('torii-w')
seen = {'tmp': inherited, 'mode': scratch.stat().st_mode & 0o777 if scratch is not None else None,
        'before': sorted(p.name for p in scratch.iterdir()) if managed else []}
if managed:
    outside = Path(OUTSIDE)
    outside.mkdir(exist_ok=True)
    (outside / 'kept').write_text('proof')
    (scratch / 'outside').symlink_to(outside, target_is_directory=True)
    readonly = scratch / 'readonly'
    readonly.mkdir()
    (readonly / 'file').write_text('scratch')
    readonly.chmod(0o500)
    (scratch / 'file').write_text('scratch')
    with socket.socket(socket.AF_UNIX) as channel:
        channel.bind(str(scratch / ('s' * 20)))
    seen['socket_bound'] = True
Path(OBSERVED).write_text(json.dumps(seen))
'''.replace('OUTSIDE', repr(str(self.root / 'outside'))).replace('OBSERVED', repr(str(self.root / 'observed')))
        if hold:
            observation += "\nwhile not Path(%r).exists():\n    time.sleep(.01)\n" % str(self.root / 'release')
        if provider == 'codex':
            script = SERVER.replace('SID', repr(SID)).replace('ROOT', repr(str(self.root)))
            script = script.replace('goal = None', observation + '\ngoal = None', 1)
        else:
            script = '''#!/usr/bin/env python3
import json, sys, time
sid = sys.argv[sys.argv.index('--session-id') + 1] if '--session-id' in sys.argv else SID
OBSERVE
print(json.dumps({'type': 'system', 'subtype': 'init', 'session_id': sid}), flush=True)
for line in sys.stdin:
    event = json.loads(line)
    print(json.dumps({**event, 'isReplay': True}), flush=True)
    print(json.dumps({'type': 'result', 'subtype': 'success', 'session_id': sid, 'result': 'done'}), flush=True)
'''.replace('SID', repr(SID)).replace('OBSERVE', observation)
        binary.write_text(script)
        binary.chmod(0o700)
        return ProviderRunner(self.store.directory, {provider: str(binary)})

    def pool(self, runner=None):
        return WorkerPool(self.store, runner or self.runner(), AccountBroker(self.store))

    def temp_scope(self):
        return patch('tempfile.tempdir', str(self.base))

    async def cleaned(self, pool):
        while pool.storage.tasks:
            await asyncio.gather(*list(pool.storage.tasks.values()))
            await asyncio.sleep(0)

    def scratch(self, worker):
        return self.base / ('torii-w' + str(worker['id']))

    def saved_host(self, worker, ownership=None):
        directory = host_directory(self.store.directory, worker)
        directory.mkdir(parents=True)
        spec = {'env': {'TMPDIR': str(self.scratch(worker))}, 'worker_temp': True}
        if ownership:
            spec['ownership'] = 'coalition'
            (directory / 'owner.json').write_text(json.dumps({'cleanup': ownership}))
        (directory / 'spec.json').write_text(json.dumps(spec))
        (directory / 'exit.json').write_text('{"returncode":0}')
        return directory


class StorageControlTests(StorageSupport, unittest.TestCase):
    def test_results_are_identical_in_cli_and_mcp_job_worktree_and_spawn(self):
        subprocess.run(['git', 'init', '-q', str(self.root)], check=True, capture_output=True)
        (self.root / 'source').write_text('source')
        subprocess.run(['git', '-C', str(self.root), 'add', 'source'], check=True, capture_output=True)
        subprocess.run(['git', '-C', str(self.root), '-c', 'user.name=Test', '-c', 'user.email=test@example.test',
                        'commit', '-qm', 'fixture'], check=True, capture_output=True)
        self.assertFalse(self.expected_results.exists())
        for source in ('cli', 'mcp'):
            before = control_api.call(self.store, 'tasks.get', {'task': self.task['id']}, source=source)
            self.assertEqual(before.data.get('results'), str(self.expected_results))
            created = control_api.call(self.store, 'worktree.create', {'task': self.task['id']}, source=source)
            self.assertTrue(created.ok, created)
            worker, spawned = self.spawn(source=source)
            self.assertEqual(created.data['results'], spawned.data['results'])
            self.assertEqual(spawned.data['results'], str(self.expected_results))
        self.assertEqual(self.expected_results.stat().st_mode & 0o777, 0o700)

    def test_retired_temp_switch_is_absent_and_old_settings_are_ignored(self):
        before = mcp.tools_hash()
        for saved in (False, True, 1):
            self.store.put('worker_temp', saved)
            self.assertNotIn('worker_temp', control_api.call(self.store, 'settings.get').data)
            for source in ('cli', 'mcp', 'telegram'):
                refused = control_api.call(self.store, 'cleanup.worker_temp', {'enabled': False}, source=source)
                self.assertFalse(refused.ok)
                self.assertEqual(self.store.get('worker_temp'), saved)
        self.assertNotIn('cleanup_worker_temp', mcp.TOOLS)
        self.assertEqual(before, mcp.tools_hash())


class WorkerStorageTests(StorageSupport, unittest.IsolatedAsyncioTestCase):
    async def test_unreadable_saved_temp_root_records_problem_and_keeps_healthy_cleanup(self):
        worker, _ = self.spawn()
        scratch = self.scratch(worker)
        scratch.mkdir()
        self.saved_host(worker)
        self.store.worker_complete(worker['id'], {'success': True}, 'done')
        old, _ = self.spawn()
        locked = self.base / 'old-root'
        locked.mkdir()
        self.store.db.execute('INSERT INTO worker_temp_state(worker,path) VALUES (?,?)',
                              (old['id'], str(locked / ('torii-w' + str(old['id'])))))
        scan = os.scandir

        def unreadable(path):
            if not isinstance(path, int) and Path(path) == locked:
                raise PermissionError(13, 'saved root unreadable', str(locked))
            return scan(path)

        service = Service(self.store, None, self.runner(), self.root)
        with self.temp_scope(), patch('os.scandir', side_effect=unreadable):
            await service.workers.cleanup_finished()
            await self.cleaned(service.workers)
        self.assertFalse(scratch.exists())
        rows = list(self.store.db.execute("SELECT detail FROM problems WHERE code='temp-reconcile-failed'"))
        self.assertEqual(len(rows), 1)
        self.assertIn(str(locked), rows[0]['detail'])
        self.assertIn('saved root unreadable', rows[0]['detail'])

    async def test_reconciliation_ignores_busy_read_slots_and_dispatch_does_not_repeat_scan(self):
        worker, _ = self.spawn()
        scratch = self.scratch(worker)
        scratch.mkdir()
        self.saved_host(worker)
        self.store.worker_complete(worker['id'], {'success': True}, 'done')
        service = Service(self.store, None, self.runner(), self.root)
        storage = service.workers.storage
        storage.read_slots = asyncio.Semaphore(0)
        with self.temp_scope(), patch.object(worker_storage, '_temp_index', wraps=worker_storage._temp_index) as scan:
            try:
                await asyncio.wait_for(service.workers.cleanup_finished(), 5)
                for _ in range(5):
                    self.assertFalse(await asyncio.wait_for(service.workers_once(), 5))
                self.assertEqual(scan.call_count, 1)
                self.assertTrue(scratch.exists())
            finally:
                for _ in range(4):
                    storage.read_slots.release()
                await self.cleaned(service.workers)
        self.assertFalse(scratch.exists())
        self.assertEqual(storage.state(worker['id'])['pending'], 0)

    async def test_existing_cleanup_pass_reconciles_recreated_temp_once_per_wake(self):
        worker, _ = self.spawn()
        scratch = self.scratch(worker)
        scratch.mkdir()
        self.saved_host(worker)
        self.store.worker_complete(worker['id'], {'success': True}, 'done')
        service = Service(self.store, None, self.runner(), self.root)
        passed = asyncio.Event()

        reconcile = service.workers.storage.reconcile

        async def reconciled():
            await reconcile()
            passed.set()

        with self.temp_scope(), patch('coordinator.workspaces.cleanup_closed_worktrees', AsyncMock()), \
                patch.object(service.workers.storage, 'reconcile', side_effect=reconciled), \
                patch.object(worker_storage, '_temp_index', wraps=worker_storage._temp_index) as scan:
            loop = asyncio.create_task(service.worktree_cleanup_loop())
            try:
                await asyncio.wait_for(passed.wait(), 5)
                await self.cleaned(service.workers)
                self.assertFalse(scratch.exists())
                scratch.mkdir()
                (scratch / 'late').write_text('late')
                for _ in range(5):
                    self.assertFalse(await service.workers_once())
                self.assertTrue(scratch.exists())
                self.assertEqual(scan.call_count, 1)
                passed.clear()
                service.wake('worktree_cleanup').set()
                await asyncio.wait_for(passed.wait(), 5)
                await self.cleaned(service.workers)
                self.assertFalse(scratch.exists())
                self.assertEqual(scan.call_count, 2)
            finally:
                loop.cancel()
                await asyncio.gather(loop, return_exceptions=True)

    async def test_dispatch_reconciles_recreated_final_temp_and_keeps_live_and_unknown_paths(self):
        self.store.put('worker_temp', True)
        worker, _ = self.spawn()
        scratch = self.scratch(worker)
        scratch.mkdir()
        self.saved_host(worker)
        service = Service(self.store, None, self.runner(), self.root)
        with self.temp_scope():
            await service.workers.storage.prepare(worker['id'])
            self.store.worker_complete(worker['id'], {'success': True}, 'done')
            await service.workers_once()
            await self.cleaned(service.workers)
            self.assertFalse(scratch.exists())
            self.assertFalse(service.workers.storage.state(worker['id'])['pending'])
            script = 'from pathlib import Path; import sys; p=Path(sys.argv[1]); p.mkdir(); (p/"late").write_text("late")'
            writer = await asyncio.create_subprocess_exec(sys.executable, '-c', script, str(scratch))
            self.assertEqual(await writer.wait(), 0)
            live, _ = self.spawn()
            live_path = Path(await service.workers.storage.prepare(live['id']))
            (live_path / 'keep').write_text('active lifetime')
            unknown = self.base / 'torii-w9999999'
            unknown.mkdir()
            changed_base = self.base / 'after-restart'
            changed_base.mkdir()
            with patch('tempfile.tempdir', str(changed_base)):
                await service.workers.cleanup_finished()
                await self.cleaned(service.workers)
        self.assertFalse(scratch.exists(), 'Final temp was recreated after completed cleanup')
        self.assertEqual((live_path / 'keep').read_text(), 'active lifetime')
        self.assertTrue(unknown.is_dir())

    async def test_resume_during_recreation_scan_keeps_the_new_temp_lifetime(self):
        from coordinator import worker_storage
        worker, _ = self.spawn()
        service = Service(self.store, None, self.runner(), self.root)
        storage = service.workers.storage
        with self.temp_scope():
            scratch = Path(await storage.prepare(worker['id']))
            self.saved_host(worker)
            self.store.worker_complete(worker['id'], {'success': True}, 'done')
            await service.workers.cleanup_finished()
            await self.cleaned(service.workers)
            self.assertFalse(scratch.exists())
            scratch.mkdir()
            (scratch / 'late').write_text('old lifetime')
            background = worker_storage._background

            async def resume_after_scan(function, *args):
                result = await background(function, *args)
                if function is worker_storage._temp_index:
                    self.store.db.execute("UPDATE workers SET status='queued' WHERE id=?", (worker['id'],))
                    await storage.prepare(worker['id'])
                    (scratch / 'current').write_text('new lifetime')
                return result

            with patch.object(worker_storage, '_background', side_effect=resume_after_scan):
                await service.workers.cleanup_finished()
                await self.cleaned(service.workers)
        self.assertEqual((scratch / 'current').read_text(), 'new lifetime')
        self.assertEqual(sorted(path.name for path in scratch.iterdir()), ['current'])
        self.assertEqual(storage.state(worker['id'])['finished'], 0)
        self.assertEqual(storage.worker(worker['id'])['status'], 'queued')

    async def check_launch(self, provider):
        worker, spawned = self.spawn(provider, source='mcp')
        pool = self.pool(self.runner(provider))
        with self.temp_scope(), patch.object(pool.runner, '_external_writer', AsyncMock(return_value=False)):
            done = await pool.run(worker, self.store.topic(TOPIC))
            await self.cleaned(pool)
        result = json.loads(done['result'])
        self.assertTrue(result['success'], result)
        seen = json.loads((self.root / 'observed').read_text())
        self.assertEqual(seen['tmp'], str(self.scratch(worker)))
        self.assertEqual(seen['mode'], 0o700)
        self.assertTrue(seen['socket_bound'])
        self.assertFalse(self.scratch(worker).exists())
        self.assertEqual(list(self.base.iterdir()), [])
        self.assertEqual((self.root / 'outside' / 'kept').read_text(), 'proof')
        self.assertEqual(spawned.data['results'], str(self.expected_results))
        self.assertEqual(self.expected_results.stat().st_mode & 0o777, 0o700)
        payload = json.loads((host_directory(self.store.directory, done) / 'spec.json').read_text())['initial_payload']
        self.assertIn(str(self.expected_results), payload)
        self.assertIn('$TMPDIR', payload)
        self.assertIn('deleted outright', payload)

    async def test_claude_private_temp_socket_symlink_and_readonly_cleanup(self):
        await self.check_launch('claude')

    async def test_codex_private_temp_socket_symlink_and_readonly_cleanup(self):
        await self.check_launch('codex')

    async def test_legacy_false_setting_cannot_disable_private_temp_or_cleanup(self):
        self.store.put('worker_temp', False)
        inherited = self.root / 'inherited-temp'
        inherited.mkdir()
        (inherited / 'keep').write_text('service temp')
        for parent_tmpdir in (str(inherited), None):
            for provider in ('claude', 'codex'):
                with self.subTest(parent_tmpdir=parent_tmpdir, provider=provider):
                    worker, spawned = self.spawn(provider)
                    pool = self.pool(self.runner(provider))
                    with self.temp_scope(), patch.dict(os.environ), \
                            patch.object(pool.runner, '_external_writer', AsyncMock(return_value=False)):
                        if parent_tmpdir is None:
                            os.environ.pop('TMPDIR', None)
                        else:
                            os.environ['TMPDIR'] = parent_tmpdir
                        done = await pool.run(worker, self.store.topic(TOPIC))
                        await self.cleaned(pool)
                    self.assertTrue(json.loads(done['result'])['success'], done)
                    seen = json.loads((self.root / 'observed').read_text())
                    self.assertEqual(seen['tmp'], str(self.scratch(worker)))
                    self.assertEqual(seen['mode'], 0o700)
                    self.assertFalse(self.scratch(worker).exists())
                    spec = json.loads((host_directory(self.store.directory, done) / 'spec.json').read_text())
                    self.assertEqual(spec['env']['TMPDIR'], str(self.scratch(worker)))
                    self.assertIn('$TMPDIR', spec['initial_payload'])
                    self.assertEqual(spawned.data['results'], str(self.expected_results))
        self.assertEqual((inherited / 'keep').read_text(), 'service temp')

    async def test_final_worker_retains_temp_until_owned_processes_are_gone(self):
        worker, _ = self.spawn()
        scratch = self.scratch(worker)
        scratch.mkdir()
        directory = self.saved_host(worker, 'pending')
        service = Service(self.store, None, self.runner(), self.root)
        self.store.worker_complete(worker['id'], {'success': True}, 'done')
        with self.temp_scope():
            await service.workers_once()
            await self.cleaned(service.workers)
            self.assertTrue(scratch.exists())
            (directory / 'owner.json').write_text('{"cleanup":"ok"}')
            with patch('coordinator.worker_storage.time.time', return_value=time.time() + 3):
                await service.workers_once()
                await self.cleaned(service.workers)
        self.assertFalse(scratch.exists())

    async def test_final_legacy_host_keeps_temp_while_child_pid_is_alive(self):
        worker, _ = self.spawn()
        scratch = self.scratch(worker)
        scratch.mkdir()
        directory = host_directory(self.store.directory, worker)
        directory.mkdir(parents=True)
        (directory / 'spec.json').write_text('{}')
        with subprocess.Popen(['/bin/sleep', '900'], cwd=self.root) as host:
            (directory / 'pid').write_text(str(host.pid))
            host.kill()
        child = subprocess.Popen([sys.executable, '-c', 'import sys; sys.stdin.read()'], stdin=subprocess.PIPE)
        self.addCleanup(lambda: child.poll() is None and child.kill())
        (directory / 'child.pid').write_text(str(child.pid))
        service = Service(self.store, None, self.runner(), self.root)
        self.store.worker_complete(worker['id'], {'success': True}, 'done')
        with self.temp_scope():
            await service.workers_once()
            await self.cleaned(service.workers)
            self.assertTrue(scratch.exists())
            child.communicate(timeout=10)
            with patch('coordinator.worker_storage.time.time', return_value=time.time() + 3):
                await service.workers_once()
                await self.cleaned(service.workers)
        self.assertFalse(scratch.exists())

    async def test_temp_cleanup_removes_only_its_git_registrations(self):
        worker, _ = self.spawn()
        scratch = self.scratch(worker)
        scratch.mkdir()
        repository = self.root / 'repository'
        repository.mkdir()

        def git(*args):
            return subprocess.run(['git', '-C', str(repository), *map(str, args)],
                                  capture_output=True, text=True, check=True).stdout

        git('init', '-q')
        git('-c', 'user.name=Test', '-c', 'user.email=test@example.test',
            'commit', '--allow-empty', '-qm', 'base')
        stale = scratch / 'checkout'
        live = self.root / 'live-checkout'
        git('worktree', 'add', '-qb', 'scratch', stale)
        registration = Path(subprocess.run(['git', '-C', str(stale), 'rev-parse', '--absolute-git-dir'],
                                          capture_output=True, text=True, check=True).stdout.strip())
        git('worktree', 'add', '-qb', 'live', live)
        missing = self.root / 'unplugged-checkout'
        git('worktree', 'add', '-q', '--detach', missing)
        subprocess.run(['git', '-C', str(missing), '-c', 'user.name=Test',
                        '-c', 'user.email=test@example.test', 'commit', '--allow-empty', '-qm', 'detached'],
                       capture_output=True, check=True)
        tip = subprocess.run(['git', '-C', str(missing), 'rev-parse', 'HEAD'],
                             capture_output=True, text=True, check=True).stdout.strip()
        missing.rename(self.root / 'moved-checkout')
        self.saved_host(worker)
        self.store.worker_complete(worker['id'], {'success': True}, 'done')
        service = Service(self.store, None, self.runner(), self.root)
        from coordinator.worker_storage import _remove_tree

        def fail_registration_once(path):
            if path == registration:
                raise OSError('Registration temporarily unavailable')
            return _remove_tree(path)

        with self.temp_scope():
            with patch('coordinator.worker_storage._remove_tree', side_effect=fail_registration_once):
                await service.workers_once()
                await self.cleaned(service.workers)
            self.assertFalse(scratch.exists())
            self.assertTrue(service.workers.storage.state(worker['id'])['pending'])
            self.assertIn(str(stale), git('worktree', 'list', '--porcelain'))
            await service.workers.storage.cleanup(worker['id'])
        listing = git('worktree', 'list', '--porcelain')
        self.assertNotIn(str(stale), listing)
        self.assertIn(str(live), listing)
        self.assertIn(str(missing), listing)
        self.assertIn(tip, git('rev-list', '--all', '--reflog'))
        self.assertTrue(live.is_dir())
        self.assertFalse(scratch.exists())
        self.assertFalse(service.workers.storage.state(worker['id'])['pending'])
        self.assertIsNone(self.store.get('worker_temp_repositories:' + str(worker['id'])))

    async def test_missing_recorded_repository_finishes_temp_cleanup(self):
        worker, _ = self.spawn()
        scratch = self.scratch(worker)
        scratch.mkdir()
        empty = self.root / 'empty-git-directory'
        empty.mkdir()
        self.store.put('worker_temp_repositories:' + str(worker['id']),
                       [str(self.root / 'missing-repository'), str(empty)])
        keys = [prefix + str(worker['id']) for prefix in (
            'worker_temp_registration_roots:', 'worker_temp_repositories:', 'host_log_secret_names:')]
        self.store.put(keys[0], [str(scratch)])
        self.store.put(keys[2], ['TEST_SECRET'])
        self.store.put('unrelated_worker_setting', ['keep'])
        self.saved_host(worker)
        self.store.worker_complete(worker['id'], {'success': True}, 'done')
        service = Service(self.store, None, self.runner(), self.root)
        with self.temp_scope():
            await service.workers_once()
            await self.cleaned(service.workers)
        self.assertFalse(scratch.exists())
        self.assertFalse(service.workers.storage.state(worker['id'])['pending'])
        for key in keys[:2]:
            self.assertIsNone(self.store.get(key), key)
        self.assertEqual(self.store.get(keys[2]), ['TEST_SECRET'])
        self.assertEqual(self.store.get('unrelated_worker_setting'), ['keep'])

    async def test_resume_replaces_leftover_temp_with_empty_folder_and_keeps_results(self):
        worker, _ = self.spawn('codex')
        scratch = self.scratch(worker)
        scratch.mkdir()
        (scratch / 'old').write_text('old')
        directory = self.saved_host(worker)
        self.store.worker_complete(worker['id'], {'success': False, 'session_id': SID}, 'done')
        self.store.db.execute("UPDATE workers SET status='queued' WHERE id=?", (worker['id'],))
        self.store.db.execute('UPDATE workers SET session=?,fresh=0 WHERE id=?', (SID, worker['id']))
        self.expected_results.mkdir(parents=True, exist_ok=True)
        self.expected_results.joinpath('proof').write_text('keep')
        pool = self.pool(self.runner('codex'))
        with self.temp_scope(), patch.object(pool.runner, '_external_writer', AsyncMock(return_value=False)):
            done = await pool.run(worker, self.store.topic(TOPIC))
            await self.cleaned(pool)
        self.assertTrue(json.loads(done['result'])['success'], done)
        seen = json.loads((self.root / 'observed').read_text())
        self.assertEqual(seen['tmp'], str(scratch))
        self.assertEqual(seen['before'], [])
        self.assertFalse(scratch.exists())
        self.assertEqual(self.expected_results.joinpath('proof').read_text(), 'keep')
        self.assertEqual(done['session'], SID)

    async def test_resume_cleans_finished_hosts_with_reused_child_pids(self):
        self.store.put('worker_temp', True)
        transcript = self.root / 'profile' / 'projects' / 'test' / (SID + '.jsonl')
        transcript.parent.mkdir(parents=True)
        transcript.write_text('{}\n')
        self.store.put('session_transcripts', {SID: str(transcript)})
        with subprocess.Popen(['/bin/sleep', '900'], cwd=self.root, start_new_session=True) as unrelated:
            try:
                for provider in ('claude', 'codex'):
                    with self.subTest(provider=provider):
                        worker, _ = self.spawn(provider)
                        scratch = self.scratch(worker)
                        scratch.mkdir()
                        (scratch / 'old').write_text('old')
                        directory = self.saved_host(worker)
                        (directory / 'child.pid').write_text(str(unrelated.pid))
                        (directory / 'pid').write_text(str(unrelated.pid))
                        self.store.worker_complete(worker['id'], {'success': True, 'session_id': SID}, 'done')
                        self.store.db.execute("UPDATE workers SET status='queued',session=?,fresh=0 WHERE id=?",
                                              (SID, worker['id']))
                        pool = self.pool(self.runner(provider))
                        with self.temp_scope(), \
                                patch.object(pool.runner, '_external_writer', AsyncMock(return_value=False)):
                            done = await pool.run(worker, self.store.topic(TOPIC))
                            await self.cleaned(pool)
                        self.assertTrue(json.loads(done['result'])['success'], done)
                        seen = json.loads((self.root / 'observed').read_text())
                        self.assertEqual(seen['tmp'], str(scratch))
                        self.assertEqual(seen['before'], [])
                        self.assertEqual(done['session'], SID)
                        self.assertEqual(list(self.base.iterdir()), [])
                        self.assertIsNone(unrelated.poll())
            finally:
                unrelated.terminate()

    async def test_final_legacy_host_cleans_temp_with_reused_child_pid(self):
        self.store.put('worker_temp', True)
        worker, _ = self.spawn()
        scratch = self.scratch(worker)
        scratch.mkdir()
        directory = self.saved_host(worker)
        with subprocess.Popen(['/bin/sleep', '900'], cwd=self.root, start_new_session=True) as unrelated:
            try:
                (directory / 'child.pid').write_text(str(unrelated.pid))
                self.store.worker_complete(worker['id'], {'success': True}, 'done')
                service = Service(self.store, None, self.runner(), self.root)
                with self.temp_scope():
                    await service.workers_once()
                    await self.cleaned(service.workers)
                self.assertEqual(list(self.base.iterdir()), [])
                self.assertIsNone(unrelated.poll())
            finally:
                unrelated.terminate()

    async def test_reattach_keeps_running_workers_temp_contents(self):
        worker, _ = self.spawn()
        runner = self.runner(hold=True)
        pool = self.pool(runner)
        with self.temp_scope():
            running = asyncio.create_task(pool.run(worker, self.store.topic(TOPIC)))
            await until_exists(self.root / 'observed')
            scratch = self.scratch(worker)
            self.assertEqual(json.loads((self.root / 'observed').read_text())['tmp'], str(scratch))
            (scratch / 'running-proof').write_text('keep')
            saved = pool.get(worker['id'])
            host = HostClient(host_directory(self.store.directory, saved))
            await host.connect()
            running.cancel()
            await asyncio.gather(running, return_exceptions=True)
            self.assertEqual(pool.get(worker['id'])['status'], 'running')
            self.assertEqual((scratch / 'running-proof').read_text(), 'keep')
            (self.root / 'release').touch()
            changed_base = self.base / 'after-restart'
            changed_base.mkdir()
            with patch('tempfile.tempdir', str(changed_base)):
                attached = self.pool(runner)
                done = await attached.run(saved, self.store.topic(TOPIC))
                await self.cleaned(attached)
        self.assertTrue(json.loads(done['result'])['success'], done)
        self.assertFalse(scratch.exists())

    async def test_startup_cleans_final_leftovers_and_remnants_only(self):
        final, _ = self.spawn()
        scratch = self.scratch(final)
        scratch.mkdir()
        remnant = scratch.with_name(scratch.name + '.deleting-' + 'a' * 32)
        remnant.mkdir()
        (remnant / 'partial').write_text('partial')
        self.saved_host(final)
        self.store.worker_complete(final['id'], {'success': True}, 'interrupted')
        other, _ = self.spawn('codex')
        live = self.scratch(other)
        live.mkdir()
        unrelated = self.base / 'unrelated'
        unrelated.mkdir()
        unknown = self.base / 'torii-w9999999'
        unknown.mkdir()
        invalid = self.base / (scratch.name + '.deleting-not-ours')
        invalid.mkdir()
        regular = scratch.with_name(scratch.name + '.deleting-' + 'b' * 32)
        regular.write_text('keep')
        file_worker, _ = self.spawn()
        file_path = self.scratch(file_worker)
        file_path.write_text('keep')
        self.saved_host(file_worker)
        self.store.worker_complete(file_worker['id'], {'success': True}, 'done')
        service = Service(self.store, None, self.runner(), self.root, pair_only=True)
        with self.temp_scope(), patch.object(service.extension, 'start', AsyncMock()), \
                patch('coordinator.reactions.check_administrators', AsyncMock()), \
                patch.object(service, 'announce_start', AsyncMock()), patch.object(service, 'supervised', return_value=[]):
            await service.run()
            await self.cleaned(service.workers)
        self.assertFalse(scratch.exists())
        self.assertFalse(remnant.exists())
        for path in (live, unrelated, unknown, invalid, regular, file_path):
            self.assertTrue(path.exists(), path)

    async def test_cleanup_rename_allows_resume_during_slow_delete_without_blocking_loop(self):
        worker, _ = self.spawn()
        scratch = self.scratch(worker)
        scratch.mkdir()
        (scratch / 'old').write_text('old')
        self.saved_host(worker)
        self.store.worker_complete(worker['id'], {'success': True}, 'done')
        entered = threading.Event()
        release = threading.Event()
        removed = threading.Event()
        import shutil
        actual = shutil.rmtree

        def slow(path, *args, **kwargs):
            if '.deleting-' in str(path):
                entered.set()
                release.wait(10)
            actual(path, *args, **kwargs)
            removed.set()

        slow.avoids_symlink_attacks = actual.avoids_symlink_attacks

        service = Service(self.store, None, self.runner(), self.root)
        with self.temp_scope(), patch('shutil.rmtree', slow):
            cleaning = asyncio.create_task(service.workers_once())
            try:
                self.assertTrue(await asyncio.wait_for(asyncio.to_thread(entered.wait, 2), 4))
                self.assertFalse(scratch.exists())
                scratch.mkdir()
                (scratch / 'new').write_text('new')
                cleaning.cancel()
                await asyncio.wait_for(asyncio.gather(cleaning, return_exceptions=True), 1)
            finally:
                release.set()
            self.assertTrue(await asyncio.to_thread(removed.wait, 10))
            await self.cleaned(service.workers)
        self.assertEqual((scratch / 'new').read_text(), 'new')

    async def test_opencode_gets_private_temp_and_shared_final_cleanup(self):
        from tests.fake_opencode import install_fake_opencode
        from tests.test_opencode_runtime import ACCOUNT
        binary = install_fake_opencode(self.root / 'opencode-fake')
        account = self.root / 'opencode-one'
        account.mkdir()
        (account / 'key').write_text('fake-only')
        (account / 'key').chmod(0o600)
        self.store.put('opencode_accounts', {'one': dict(ACCOUNT)})
        self.store.put('opencode_active_account', 'one')
        self.store.put('opencode_enabled', True)
        self.store.put('opencode_binary', str(binary))
        (binary.parent / 'scenario.json').write_text('{"scratch_files":true}')
        with patch('coordinator.providers.platform', 'linux'), \
                patch('coordinator.opencode_accounts.account_home', return_value=account), \
                patch('coordinator.opencode_runtime.managed_preference_paths', return_value=()):
            worker, _ = self.spawn('opencode')
            pool = self.pool(ProviderRunner(self.store.directory, opencode_store=self.store))
            with self.temp_scope():
                done = await pool.run(worker, self.store.topic(TOPIC))
                await self.cleaned(pool)
        self.assertTrue(json.loads(done['result'])['success'], done)
        spec = json.loads((host_directory(self.store.directory, done) / 'spec.json').read_text())
        scratch = Path(spec['env']['TMPDIR'])
        self.assertEqual(scratch, Path(spec['env']['XDG_CONFIG_HOME']).parent / 'tmp')
        self.assertNotEqual(scratch, self.scratch(worker))
        self.assertFalse(scratch.exists())
        self.assertTrue(self.expected_results.exists())

    async def test_opencode_retains_private_temp_until_final_process_proof(self):
        from tests.fake_opencode import install_fake_opencode
        from tests.test_opencode_runtime import ACCOUNT
        binary = install_fake_opencode(self.root / 'opencode-fake')
        key_home = self.root / 'opencode-key'
        key_home.mkdir()
        (key_home / 'key').write_text('fake-only')
        (key_home / 'key').chmod(0o600)
        self.store.put('opencode_accounts', {'one': dict(ACCOUNT)})
        self.store.put('opencode_active_account', 'one')
        self.store.put('opencode_enabled', True)
        self.store.put('opencode_binary', str(binary))
        (binary.parent / 'scenario.json').write_text('{"scratch_files":true}')
        homes = patch('coordinator.opencode_accounts.account_home', return_value=key_home)
        homes.start()
        self.addCleanup(homes.stop)
        worker, _ = self.spawn('opencode')
        pool = self.pool(ProviderRunner(self.store.directory, opencode_store=self.store))
        run = pool.runner.run
        async def preserve(*args, **kwargs):
            result = await run(*args, **kwargs)
            directory = Path(result.log_path).parent
            spec = json.loads((directory / 'spec.json').read_text())
            spec['ownership'] = 'coalition'
            (directory / 'spec.json').write_text(json.dumps(spec))
            (directory / 'owner.json').write_text('{"cleanup":"ok"}')
            return result
        with self.temp_scope(), patch('coordinator.providers.platform', 'linux'), \
                patch('coordinator.opencode_accounts.account_home', return_value=key_home), \
                patch('coordinator.opencode_runtime.managed_preference_paths', return_value=()), \
                patch.object(pool.runner, 'run', preserve):
            done = await pool.run(worker, self.store.topic(TOPIC))
            (host_directory(self.store.directory, done) / 'owner.json').write_text('{"cleanup":"pending"}')
            await self.cleaned(pool)
        self.assertTrue(json.loads(done['result'])['success'], done)
        directory = host_directory(self.store.directory, done)
        spec = json.loads((directory / 'spec.json').read_text())
        scratch = Path(spec['env']['TMPDIR'])
        self.assertNotEqual(scratch, self.scratch(worker))
        self.assertTrue((scratch / 'scratch').exists())
        self.assertEqual(scratch.stat().st_mode & 0o777, 0o700)
        launch = Path(spec['env']['XDG_CONFIG_HOME']).parent
        (directory / 'owner.json').write_text('{"cleanup":"ok"}')
        with self.temp_scope(), patch('coordinator.worker_storage.time.time', return_value=time.time() + 3):
            await pool.cleanup_finished()
            await self.cleaned(pool)
        self.assertFalse(scratch.exists())
        self.assertFalse((launch / 'home/Library/Caches').exists())
        self.assertTrue((launch / 'home/retained').exists())
        self.assertTrue(self.expected_results.exists())
