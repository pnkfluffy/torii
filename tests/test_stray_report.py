import asyncio
from datetime import datetime
import importlib
import json
import os
from pathlib import Path
import plistlib
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from coordinator import control_api
from coordinator.store import Store


class StrayReportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.state = self.root / 'state'
        self.store = Store(self.state / 'service')
        self.addCleanup(self.store.close)
        self.agents = self.root / 'agents'
        self.agents.mkdir()
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:2',1,2,'Example chat',?,1)",
                                  (str(self.root / 'project'),))
            self.store.put('coordinator_home_topic', '1:2')
            self.store.put('stray_report_config', dict(state_root=str(self.state), launch_agents=[str(self.agents)],
                                                     service_paths=[], results_min_bytes=8192, growth_bytes=8192, keep=2))
            self.store.put('stray_report_enabled', True)

    def module(self):
        return importlib.import_module('coordinator.stray_report')

    def folder(self, path, size=16384):
        path.mkdir(parents=True, exist_ok=True)
        (path / 'payload').write_bytes(b'x' * size)
        os.utime(path / 'payload', (1000000000, 1000000000))
        return path

    def run_report(self, references=()):
        module = self.module()
        with patch.object(module, 'process_paths', return_value=list(references)):
            return asyncio.run(module.run_once(self.store))

    def rows(self, report):
        return {row['path']: row for row in report['entries']}

    def call(self, **params):
        async def invoke():
            return await control_api.call_async(self.store, 'cleanup.stray_report', params)
        return asyncio.run(invoke())

    def job(self, status='open'):
        task = self.store.task_create('1:2', 'Example job')
        with self.store.db:
            self.store.db.execute('UPDATE tasks SET status=? WHERE id=?', (status, task['id']))
        return task

    def test_default_off_and_control_can_enable_without_scanning(self):
        with self.store.db:
            self.store.db.execute("DELETE FROM settings WHERE key='stray_report_enabled'")
        result = self.call()
        self.assertTrue(result.ok, result.text)
        self.assertFalse(result.data['enabled'])
        refused = self.call(run=True)
        self.assertFalse(refused.ok)
        enabled = self.call(enabled=True)
        self.assertTrue(enabled.ok, enabled.text)
        self.assertTrue(enabled.data['enabled'])
        self.assertIsNone(self.store.get('stray_report_history'))
        self.assertTrue(self.store.get('stray_report_requested'))

    def test_lists_unowned_with_allocated_size_mtime_and_origin(self):
        task = self.job('done')
        candidate = self.folder(self.state / ('retired-task-' + str(task['id'])))
        report = self.run_report()
        row = self.rows(report)[str(candidate)]
        self.assertEqual(row['category'], 'state')
        expected = candidate.lstat().st_blocks * 512 + (candidate / 'payload').lstat().st_blocks * 512
        self.assertEqual(row['allocated_bytes'], expected)
        self.assertEqual(row['newest_mtime'], 1000000000)
        self.assertEqual(row['origin']['task'], task['id'])
        self.assertEqual(row['origin']['chat'], 'Example chat')
        self.assertEqual(report['totals']['state'], expected)
        self.assertEqual(report['total_bytes'], expected)

    def test_open_waiting_and_interrupted_jobs_own_named_roots_and_results(self):
        for status in ('open', 'waiting', 'interrupted'):
            task = self.job(status)
            self.folder(self.state / ('legacy-task' + str(task['id']) + '-scratch'))
            self.folder(Path(self.module().results_directory(self.store, task['id'])))
        self.assertEqual(self.run_report()['entries'], [])

    def test_chat_service_and_process_references_protect_folders(self):
        chat = self.folder(self.state / 'chat-project')
        process = self.folder(self.state / 'process-data')
        service = self.folder(self.state / 'runtime')
        unknown = self.folder(self.state / 'stray')
        with self.store.db:
            self.store.db.execute("UPDATE topics SET cwd=? WHERE id='1:2'", (str(chat),))
            config = self.store.get('stray_report_config')
            config['service_paths'] = [str(service / 'payload')]
            self.store.put('stray_report_config', config)
        report = self.run_report([process / 'payload'])
        self.assertEqual(set(self.rows(report)), {str(unknown)})

    def test_launchagent_paths_protect_cwd_program_args_and_logs(self):
        paths = [self.folder(self.state / name) for name in ('cwd', 'program', 'argument', 'stdout', 'stderr')]
        config = dict(Label='example.fixture', WorkingDirectory=str(paths[0]), Program=str(paths[1] / 'payload'),
                      ProgramArguments=['/usr/bin/python3', str(paths[2] / 'payload')],
                      StandardOutPath=str(paths[3] / 'payload'), StandardErrorPath=str(paths[4] / 'payload'),
                      EnvironmentVariables={'PRIVATE_FIXTURE': 'never included'})
        with (self.agents / 'fixture.plist').open('wb') as handle:
            plistlib.dump(config, handle)
        report = self.run_report()
        self.assertEqual(report['entries'], [])
        self.assertNotIn('never included', json.dumps(report))

    def test_results_limit_is_allocated_and_service_private_data_is_not_walked(self):
        task = self.job('done')
        large = self.folder(Path(self.module().results_directory(self.store, task['id'])))
        small = self.store.directory / 'results' / 'task-9001'
        small.mkdir()
        opaque = self.folder(self.store.directory / 'hosts')
        original = os.scandir
        blocked = (opaque.lstat().st_dev, opaque.lstat().st_ino)
        def scandir(path):
            info = os.fstat(path) if isinstance(path, int) else Path(path).lstat()
            self.assertNotEqual((info.st_dev, info.st_ino), blocked)
            return original(path)
        for root in (self.state, self.store.directory, self.store.directory / 'results', opaque):
            with self.subTest(root=root):
                with self.store.db:
                    config = self.store.get('stray_report_config')
                    config['state_root'] = str(root)
                    self.store.put('stray_report_config', config)
                with patch('os.scandir', side_effect=scandir):
                    report = self.run_report()
                self.assertEqual(set(self.rows(report)), {str(large)})
                self.assertEqual(self.rows(report)[str(large)]['category'], 'results')
                self.assertNotIn(str(opaque), json.dumps(report))

    def test_scan_never_reads_payloads_changes_files_or_follows_symlinks(self):
        candidate = self.folder(self.state / 'unknown')
        outside = self.folder(self.root / 'outside')
        (candidate / 'linked').symlink_to(outside, target_is_directory=True)
        protected = self.state / '.codex-accounts'
        protected.mkdir()
        before = {path: (path.lstat().st_size, path.lstat().st_mtime_ns, path.lstat().st_mode)
                  for path in (candidate, candidate / 'payload', candidate / 'linked', outside, outside / 'payload', protected)}
        original = os.scandir
        forbidden = {(path.lstat().st_dev, path.lstat().st_ino) for path in (protected, outside)}
        def scandir(path):
            if isinstance(path, int):
                info = os.fstat(path)
                self.assertNotIn((info.st_dev, info.st_ino), forbidden)
            else:
                self.assertNotEqual(Path(path), protected)
                self.assertNotEqual(Path(path), outside)
            return original(path)
        with patch('os.scandir', side_effect=scandir), patch('pathlib.Path.read_bytes', side_effect=AssertionError('payload read')):
            report = self.run_report()
        after = {path: (path.lstat().st_size, path.lstat().st_mtime_ns, path.lstat().st_mode) for path in before}
        self.assertEqual(after, before)
        self.assertEqual(set(self.rows(report)), {str(candidate)})
        self.assertEqual((candidate / 'payload').read_bytes(), b'x' * 16384)

    def test_growth_threshold_only_one_home_summary_and_bounded_history(self):
        self.folder(self.state / 'first')
        first = self.run_report()
        outbox = self.store.db.execute('SELECT topic,text FROM outbox').fetchall()
        self.assertEqual(len(outbox), 1)
        self.assertEqual(outbox[0]['topic'], '1:2')
        self.assertIn('Report only', outbox[0]['text'])
        second = self.run_report()
        self.assertEqual(second['delta_bytes'], 0)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM outbox').fetchone()[0], 1)
        self.folder(self.state / 'second')
        third = self.run_report()
        self.assertGreater(third['delta_bytes'], 8192)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM outbox').fetchone()[0], 2)
        history = self.store.get('stray_report_history')
        self.assertEqual([r['created'] for r in history], [second['created'], third['created']])
        self.assertEqual(first['delta_bytes'], first['total_bytes'])

    def test_growth_equal_to_threshold_does_not_notify(self):
        candidate = self.folder(self.state / 'equal')
        total = candidate.lstat().st_blocks * 512 + (candidate / 'payload').lstat().st_blocks * 512
        with self.store.db:
            config = self.store.get('stray_report_config')
            config['growth_bytes'] = total
            self.store.put('stray_report_config', config)
        self.run_report()
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM outbox').fetchone()[0], 0)

    def test_process_probe_failure_is_incomplete_and_never_notifies(self):
        self.folder(self.state / 'stray')
        module = self.module()
        with patch.object(module, 'process_paths', side_effect=OSError('fixture probe failed')):
            report = asyncio.run(module.run_once(self.store))
        self.assertFalse(report['complete'])
        self.assertTrue(report['errors'])
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM outbox').fetchone()[0], 0)
        self.assertIsNone(self.store.get('stray_report_previous'))

    def test_bad_config_is_refused_atomically(self):
        before = self.store.get('stray_report_config')
        for config in ({'keep': 0}, {'growth_bytes': -1}, {'state_root': 'relative'}, {'unknown': True},
                       {'state_root': str(self.root / '.CoDeX')},
                       {'launch_agents': [str(self.root / '.CLAUDE-accounts' / 'profiles')]}):
            with self.subTest(config=config):
                result = self.call(enabled=False, config=json.dumps(config))
                self.assertFalse(result.ok)
                self.assertEqual(self.store.get('stray_report_config'), before)
                self.assertTrue(self.store.get('stray_report_enabled'))

    def test_off_switch_prevents_any_scan(self):
        with self.store.db:
            self.store.put('stray_report_enabled', False)
        with patch('os.scandir', side_effect=AssertionError('off must not scan')):
            self.assertIsNone(self.run_report())

    def test_nonessential_loop_is_registered_and_private_mode_excludes_it(self):
        from coordinator.service import LOOP_ESSENTIAL, Service
        from tests.test_service import FakeRunner, FakeTelegram
        service = Service(self.store, FakeTelegram(), FakeRunner(), self.root)
        self.assertFalse(LOOP_ESSENTIAL['stray_report'])
        self.assertIn('stray_report', dict(service.supervised()))
        service.pair_only = True
        self.assertNotIn('stray_report', dict(service.supervised()))

    def test_active_job_canonical_scratch_and_saved_copy_protect_paths_without_name_hints(self):
        task = self.job()
        saved = self.folder(self.state / 'arbitrary-copy')
        with self.store.db:
            self.store.db.execute('UPDATE tasks SET worktree=? WHERE id=?', (str(saved), task['id']))
        self.assertEqual(self.run_report()['entries'], [])
        canonical = self.folder(self.store.directory / 'scratch' / ('task-' + str(task['id'])))
        unknown = self.folder(self.state / 'unowned')
        owners = self.module().ownership(self.store, self.module().configuration(self.store))
        self.assertIn(canonical, owners.trees)
        self.assertIn(saved, owners.trees)
        self.assertNotIn(unknown, owners.trees)
        self.assertEqual(set(self.rows(self.run_report())), {str(unknown)})

    def test_unfinished_worker_and_pending_output_protect_closed_job_results(self):
        task = self.job('done')
        result = self.folder(self.store.directory / 'results' / ('task-' + str(task['id'])))
        worker_path = self.folder(self.state / 'worker-data')
        output = self.folder(self.state / 'queued-output')
        with self.store.db:
            self.store.db.execute("""INSERT INTO workers(task,topic,provider,prompt,cwd,status,created,updated)
                VALUES (?,'1:2','codex','fixture',?,'queued',0,0)""", (task['id'], str(worker_path)))
            self.store.enqueue_report('1:2', 'fixture', image=str(output / 'payload'), as_document=True)
        self.assertEqual(self.run_report()['entries'], [])
        self.assertTrue(result.is_dir())

    def test_invalid_launch_metadata_marks_report_incomplete(self):
        self.folder(self.state / 'stray')
        (self.agents / 'invalid.plist').write_bytes(b'not a plist')
        report = self.run_report()
        self.assertFalse(report['complete'])
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM outbox').fetchone()[0], 0)

    def test_metadata_budget_is_incomplete_and_does_not_advance_baseline(self):
        self.folder(self.state / 'stray')
        module = self.module()
        with patch.object(module, 'MAX_ENTRIES', 0):
            report = self.run_report()
        self.assertFalse(report['complete'])
        self.assertIsNone(self.store.get('stray_report_previous'))

    def test_hardlinks_count_once_and_sparse_file_uses_allocated_bytes(self):
        candidate = self.folder(self.state / 'linked')
        os.link(candidate / 'payload', candidate / 'hardlink')
        sparse = candidate / 'sparse'
        with sparse.open('wb') as handle:
            handle.truncate(1024 ** 3)
        expected = sum(path.lstat().st_blocks * 512 for path in (candidate, candidate / 'payload', sparse))
        report = self.run_report()
        self.assertEqual(self.rows(report)[str(candidate)]['allocated_bytes'], expected)
        self.assertLess(expected, 1024 ** 3)

    def test_default_thresholds_and_retention(self):
        with self.store.db:
            self.store.db.execute("DELETE FROM settings WHERE key='stray_report_config'")
        result = self.call()
        self.assertTrue(result.ok, result.text)
        self.assertEqual(result.data['config']['results_min_bytes'], 1024 ** 3)
        self.assertEqual(result.data['config']['growth_bytes'], 2 * 1024 ** 3)
        self.assertEqual(result.data['config']['keep'], 7)

    def test_requested_scan_runs_through_service_controls_then_waits_until_night(self):
        from coordinator.service import Service
        from tests.test_service import FakeRunner, FakeTelegram
        candidate = self.folder(self.state / 'stray')
        service = Service(self.store, FakeTelegram(), FakeRunner(), self.root)
        self.assertTrue(self.call(run=True).ok)
        module = self.module()
        async def drive():
            await service.controls_once()
            self.assertTrue(service.wake('stray_report').is_set())
            with patch('coordinator.service.pause', AsyncMock(side_effect=asyncio.CancelledError)), \
                    patch.object(module, 'process_paths', return_value=[]):
                for _ in range(2):
                    with self.assertRaises(asyncio.CancelledError):
                        await service.stray_report_loop()
        asyncio.run(drive())
        history = self.store.get('stray_report_history')
        self.assertEqual(len(history), 1)
        self.assertEqual(set(self.rows(history[0])), {str(candidate)})
        self.assertFalse(self.store.get('stray_report_requested'))
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM outbox').fetchone()[0], 1)

    def test_scan_root_with_symlink_ancestor_is_incomplete_without_following_it(self):
        target = self.folder(self.root / 'outside')
        alias = self.root / 'alias'
        alias.symlink_to(target, target_is_directory=True)
        with self.store.db:
            config = self.store.get('stray_report_config')
            config['state_root'] = str(alias)
            self.store.put('stray_report_config', config)
        report = self.run_report()
        self.assertFalse(report['complete'])
        self.assertEqual(report['entries'], [])
        self.assertEqual((target / 'payload').read_bytes(), b'x' * 16384)

    def test_schedule_uses_three_am_local_time(self):
        module = self.module()
        before = datetime(2026, 1, 5, 2, 59).timestamp()
        at = datetime(2026, 1, 5, 3).timestamp()
        self.assertEqual(module.next_night(before), at)
        self.assertEqual(module.next_night(at), datetime(2026, 1, 6, 3).timestamp())

    def test_launchagent_defaults_include_os_account_home_with_private_service_home(self):
        module = self.module()
        with self.store.db:
            self.store.db.execute("DELETE FROM settings WHERE key='stray_report_config'")
        owner_home = self.root / 'owner-home'
        private_home = self.root / 'private-home'
        with patch.object(module, 'getpwuid', return_value=SimpleNamespace(pw_dir=str(owner_home))), \
                patch('pathlib.Path.home', return_value=private_home):
            result = self.call()
        self.assertTrue(result.ok, result.text)
        folders = result.data['config']['launch_agents']
        self.assertIn(str(owner_home / 'Library' / 'LaunchAgents'), folders)
        self.assertIn(str(private_home / 'Library' / 'LaunchAgents'), folders)
