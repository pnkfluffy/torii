import asyncio
from datetime import datetime, timezone
import importlib
from pathlib import Path
import subprocess
import tempfile
import threading
import unittest
from unittest.mock import AsyncMock, patch

from coordinator import control_api, health, mcp
from coordinator.service import Service
from coordinator.store import Store


class StoreSupport:
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name))
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd) VALUES ('1:2',1,2,'Torii','')")

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def call(self, op, params=None, source='cli'):
        with self.store.db:
            return control_api.call(self.store, op, params, source=source)


class StageTests(StoreSupport, unittest.TestCase):
    def test_stage_uses_server_time_and_keeps_repeats(self):
        task = self.store.task_create('1:2', 'Measure')
        for now in (100.25, 101.75):
            with patch('coordinator.store.time.time', return_value=now):
                result = self.call('tasks.update', {'task': task['id'], 'stage': 'ci'})
            self.assertTrue(result.ok, result.text)
        rows = self.store.db.execute('SELECT task,stage,created FROM task_stamps ORDER BY id').fetchall()
        self.assertEqual([tuple(row) for row in rows], [(task['id'], 'ci', 100.25), (task['id'], 'ci', 101.75)])
        self.assertEqual(self.store.task_get(task['id'])['updated'], 101.75)

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
            'account_show', 'policy_show', 'projects_list', 'problems_list', 'problems_summary', 'tasks_stale'})


class ReaderTests(unittest.TestCase):
    def test_numeric_mac_reader(self):
        outputs = {
            ('sysctl', '-n', 'hw.ncpu'): '8\n',
            ('sysctl', '-n', 'hw.memsize'): '17179869184\n',
            ('vm_stat',): 'Mach Virtual Memory Statistics: (page size of 4096 bytes)\n'
                          'Pages free: 1048576.\nPages inactive: 1048576.\nPages speculative: 0.\n',
            ('memory_pressure', '-Q'): 'System-wide memory free percentage: 36%\n',
            ('sysctl', '-n', 'vm.swapusage'): 'total = 1024.00M  used = 256.00M  free = 768.00M\n'}
        with patch('coordinator.host_os.SYSTEM', 'darwin'), \
                patch.object(health, '_command', side_effect=lambda *args: outputs[args]), \
                patch.object(health.os, 'getloadavg', return_value=(2.5, 1, 1)):
            result = health.read_pressure()
        self.assertEqual(result, {'load1': 2.5, 'cores': 8, 'memory_used': 8589934592,
                                  'memory_total': 17179869184, 'swap_used': 268435456, 'pressure': 36.0})

    def test_numeric_linux_reader(self):
        files = {'meminfo': 'MemTotal: 16777216 kB\nMemAvailable: 8388608 kB\n'
                            'SwapTotal: 1048576 kB\nSwapFree: 786432 kB\n',
                 'pressure/memory': 'some avg10=0.25 avg60=0.10 avg300=0.00 total=123\n'}
        with patch('coordinator.host_os.SYSTEM', 'linux'), \
                patch('coordinator.host_os.proc_text', side_effect=lambda name: files.get(name, '')), \
                patch.object(health.os, 'getloadavg', return_value=(2.5, 1, 1)), \
                patch.object(health.os, 'cpu_count', return_value=8):
            result = health.read_pressure()
        self.assertEqual(result, {'load1': 2.5, 'cores': 8, 'memory_used': 8589934592,
                                  'memory_total': 17179869184, 'swap_used': 268435456, 'pressure': 0.25})

    def test_garbage_and_unreadable_values_are_null(self):
        for system in ('darwin', 'linux'):
            with self.subTest(system=system), patch('coordinator.host_os.SYSTEM', system), \
                    patch.object(health, '_command', return_value='garbage'), \
                    patch('coordinator.host_os.proc_text', return_value='some avg10=1.2.3\n'), \
                    patch.object(health.os, 'getloadavg', side_effect=OSError), \
                    patch.object(health.os, 'cpu_count', return_value=None):
                self.assertEqual(health.read_pressure(), {'load1': None, 'cores': None, 'memory_used': None,
                                                         'memory_total': None, 'swap_used': None, 'pressure': None})

    def test_partial_mac_metrics_survive_unreadable_memory(self):
        outputs = {('sysctl', '-n', 'hw.ncpu'): '4', ('memory_pressure', '-Q'):
                   'System-wide memory free percentage: 25%', ('sysctl', '-n', 'vm.swapusage'): 'used = 1.5G'}
        with patch('coordinator.host_os.SYSTEM', 'darwin'), \
                patch.object(health, '_command', side_effect=lambda *args: outputs.get(args, 'garbage')), \
                patch.object(health.os, 'getloadavg', return_value=(0.5, 1, 1)):
            self.assertEqual(health.read_pressure(), {'load1': 0.5, 'cores': 4, 'memory_used': None,
                                                     'memory_total': None, 'swap_used': 1610612736, 'pressure': 25.0})

    def test_host_commands_keep_one_second_timeouts(self):
        with patch('coordinator.host_os.SYSTEM', 'darwin'), \
                patch.object(health.subprocess, 'run', side_effect=subprocess.TimeoutExpired('probe', 1)) as run, \
                patch.object(health.os, 'getloadavg', side_effect=OSError):
            self.assertEqual(health.read_pressure(), {'load1': None, 'cores': None, 'memory_used': None,
                                                     'memory_total': None, 'swap_used': None, 'pressure': None})
        self.assertTrue(run.call_args_list)
        self.assertTrue(all(call.kwargs['timeout'] == 1 for call in run.call_args_list))

    def test_linux_zero_pressure_health_output_is_unchanged(self):
        files = {'meminfo': 'MemTotal: 1048576 kB\nMemAvailable: 524288 kB\nSwapTotal: 0 kB\nSwapFree: 0 kB\n',
                 'pressure/memory': 'some avg10=0.00 avg60=0.00 avg300=0.00 total=0\n'}
        with patch('coordinator.host_os.SYSTEM', 'linux'), \
                patch('coordinator.host_os.proc_text', side_effect=lambda name: files.get(name, '')), \
                patch.object(health.os, 'getloadavg', return_value=(0, 0, 0)), \
                patch.object(health.os, 'cpu_count', return_value=4), patch.object(health, '_disk', return_value=None):
            self.assertEqual(health.system_pressure('/tmp/state'),
                             ['CPU: 0.0 load / 4 cores', 'RAM: 0.5G used / 1.0G, 0.00% pressure stall, swap 0.0G'])

    def test_browser_executable_names_with_spaces_and_one_safe_ps_call(self):
        lines = '100 /Applications/Google Chrome.app/Contents/MacOS/Google Chrome\n' \
                '200 /Applications/Google Chrome Helper (Renderer)\n300 /usr/bin/chromium\n' \
                '400 /tmp/chrome-headless-shell\n500 /Applications/Firefox.app/Contents/MacOS/firefox\n' \
                '600 /System/com.apple.WebKit.WebContent\n700 /usr/lib/WebKitWebProcess\n' \
                '800 /usr/bin/Browser\n900 /tmp/chrome-backup\n1000 /tmp/chrome --arg\ngarbage\n'
        for system, args in (('darwin', ('ps', '-axo', 'rss=,comm=')), ('linux', ('ps', '-eo', 'rss=,comm='))):
            with self.subTest(system=system), patch('coordinator.host_os.SYSTEM', system), \
                    patch.object(health, '_command', return_value=lines) as command:
                self.assertEqual(health.read_browser_load(), {'browser_count': 7, 'browser_rss': 2867200})
                command.assert_called_once_with(*args)

    def test_browser_unavailable_differs_from_no_browsers(self):
        for output in ('', 'garbage', '-1 /tmp/chrome'):
            with patch.object(health, '_command', return_value=output):
                self.assertEqual(health.read_browser_load(), {'browser_count': None, 'browser_rss': None})
        with patch.object(health, '_command', return_value='20 /usr/bin/python\n'):
            self.assertEqual(health.read_browser_load(), {'browser_count': 0, 'browser_rss': 0})

    def test_unreadable_command_encoding_returns_null_metrics(self):
        with patch('coordinator.host_os.SYSTEM', 'darwin'), \
                patch.object(health.subprocess, 'run', side_effect=UnicodeDecodeError('utf-8', b'\xff', 0, 1, 'invalid')), \
                patch.object(health.os, 'getloadavg', side_effect=OSError):
            self.assertEqual(health.read_pressure(), {'load1': None, 'cores': None, 'memory_used': None,
                                                     'memory_total': None, 'swap_used': None, 'pressure': None})
            self.assertEqual(health.read_browser_load(), {'browser_count': None, 'browser_rss': None})

    def test_linux_browser_comm_supports_truncated_executable_names(self):
        lines = '100 chrome-headless\n200 WebKitWebProces\n300 chrome-backup\n'
        with patch('coordinator.host_os.SYSTEM', 'linux'), patch.object(health, '_command', return_value=lines):
            self.assertEqual(health.read_browser_load(), {'browser_count': 2, 'browser_rss': 307200})


class SamplerTests(StoreSupport, unittest.IsolatedAsyncioTestCase):
    async def test_one_loop_iteration_writes_numeric_row_off_event_loop(self):
        main_thread = threading.get_ident()
        threads = []

        def read_pressure():
            threads.append(threading.get_ident())
            return {'load1': 2.5, 'cores': 8, 'memory_used': 100, 'memory_total': 200,
                    'swap_used': 30, 'pressure': 36.0}

        def read_browsers():
            threads.append(threading.get_ident())
            return {'browser_count': 3, 'browser_rss': 102400}

        with self.store.db:
            for status in ('running', 'running', 'needs_input', 'queued'):
                self.store.db.execute("INSERT INTO workers(topic,provider,prompt,status,created,updated) "
                                      "VALUES ('1:2','codex','test',?,1,1)", (status,))
        service = Service(self.store, None, None, Path(self.temp.name))
        with patch.object(health, 'read_pressure', side_effect=read_pressure, create=True), \
                patch.object(health, 'read_browser_load', side_effect=read_browsers, create=True), \
                patch('time.time', return_value=10000000), \
                patch('coordinator.service.asyncio.sleep', new=AsyncMock(side_effect=asyncio.CancelledError)) as sleep:
            with self.assertRaises(asyncio.CancelledError):
                await service.metrics_loop()
        row, = self.store.db.execute('SELECT * FROM host_samples').fetchall()
        self.assertEqual(dict(row), {'id': 1, 'created': 10000000.0, 'load1': 2.5, 'cores': 8,
                                    'memory_used': 100, 'memory_total': 200, 'swap_used': 30, 'pressure': 36.0,
                                    'browser_count': 3, 'browser_rss': 102400, 'running_workers': 2})
        self.assertEqual(len(threads), 2)
        self.assertTrue(all(thread != main_thread for thread in threads))
        sleep.assert_awaited_once_with(300)

    async def test_missing_readings_are_stored_as_null(self):
        metrics = importlib.import_module('coordinator.metrics')
        with patch.object(health, 'read_pressure', return_value={'load1': None, 'cores': None,
                'memory_used': None, 'memory_total': None, 'swap_used': None, 'pressure': None}), \
                patch.object(health, 'read_browser_load', return_value={'browser_count': None, 'browser_rss': None}):
            await metrics.sample_once(self.store)
        row, = self.store.db.execute('SELECT load1,cores,memory_used,memory_total,swap_used,pressure,'
                                    'browser_count,browser_rss,running_workers FROM host_samples').fetchall()
        self.assertEqual(tuple(row), (None, None, None, None, None, None, None, None, 0))
        self.assertEqual(self.store.db.execute('SELECT area,code FROM problems').fetchone()[:],
                         ('metrics', 'readings-unavailable'))

    async def test_sampler_recovers_after_failure_and_records_rate_limit(self):
        metrics = importlib.import_module('coordinator.metrics')
        original = metrics.sample_once
        attempts = 0

        async def sample(store):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise ValueError('private probe data')
            return await original(store)

        service = Service(self.store, None, None, Path(self.temp.name))
        with patch.object(metrics, 'sample_once', side_effect=sample), \
                patch.object(health, 'read_pressure', return_value={'load1': 1.0, 'cores': 2,
                    'memory_used': 1, 'memory_total': 2, 'swap_used': 0, 'pressure': 0.0}), \
                patch.object(health, 'read_browser_load', return_value={'browser_count': 0, 'browser_rss': 0}), \
                patch('coordinator.service.problems.record', wraps=importlib.import_module('coordinator.problems').record) as record, \
                patch('coordinator.service.asyncio.sleep', new=AsyncMock(side_effect=[None, asyncio.CancelledError])):
            with self.assertRaises(asyncio.CancelledError):
                await service.metrics_loop()
        self.assertEqual(attempts, 2)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM host_samples').fetchone()[0], 1)
        self.assertEqual(record.call_args.kwargs['every'], 3600)
        self.assertEqual(self.store.db.execute('SELECT area,code,detail FROM problems').fetchone()[:],
                         ('metrics', 'sample-failed', 'ValueError'))

    async def test_retention_prunes_only_samples_strictly_older_than_thirty_days(self):
        metrics = importlib.import_module('coordinator.metrics')
        now = 10000000
        cutoff = now - 30 * 86400
        task = self.store.task_create('1:2', 'Keep stamps')
        with self.store.db:
            for stamp in (cutoff - 1, cutoff, now):
                self.store.db.execute('INSERT INTO host_samples(created,running_workers) VALUES (?,0)', (stamp,))
            self.store.db.execute("INSERT INTO task_stamps(task,stage,created) VALUES (?,'ready',1)", (task['id'],))
        with patch('coordinator.metrics.time.time', return_value=now):
            self.assertEqual(metrics.prune(self.store), 1)
        self.assertEqual([row[0] for row in self.store.db.execute('SELECT created FROM host_samples ORDER BY id')],
                         [cutoff, now])
        self.assertEqual(self.store.db.execute('SELECT stage,created FROM task_stamps').fetchone()[:], ('ready', 1.0))

    async def test_supervised_sampler_is_skipped_when_pair_only(self):
        service = Service(self.store, None, None, Path(self.temp.name))
        self.assertIn('metrics', {name for name, _ in service.supervised()})
        service.pair_only = True
        self.assertNotIn('metrics', {name for name, _ in service.supervised()})


class ReportTests(StoreSupport, unittest.TestCase):
    def test_single_sample_percentiles_preserve_the_measured_value(self):
        with self.store.db:
            self.store.db.execute('INSERT INTO host_samples(created,load1,running_workers) VALUES (100,4.39404296875,0)')
        with patch('time.time', return_value=150):
            result = self.call('metrics.report')
        self.assertTrue(result.ok, result.text)
        day, = result.data['daily']
        self.assertEqual((day['load1_p50'], day['load1_p95'], day['load1_max']),
                         (4.39404296875, 4.39404296875, 4.39404296875))

    def test_report_first_lifetime_stamps_and_daily_percentiles(self):
        now = datetime(2026, 10, 12, 12, tzinfo=timezone.utc).timestamp()
        cutoff = now - 7 * 86400
        with patch('coordinator.store.time.time', return_value=cutoff - 1000):
            task = self.store.task_create('1:2', 'Measure')
            old = self.store.task_create('1:2', 'Old')
        with self.store.db:
            for job, stage, stamp in ((task['id'], 'ready', cutoff - 900), (task['id'], 'ready', now - 100),
                                      (task['id'], 'reviewed', cutoff - 800), (task['id'], 'ci', cutoff - 600),
                                      (task['id'], 'ci', now - 50), (task['id'], 'merged', now - 40),
                                      (task['id'], 'live', now - 10), (old['id'], 'live', cutoff - 1)):
                self.store.db.execute('INSERT INTO task_stamps(task,stage,created) VALUES (?,?,?)', (job, stage, stamp))
            for i, load in enumerate((1, 2, 3, 4, 20)):
                self.store.db.execute('INSERT INTO host_samples(created,load1,cores,memory_used,memory_total,'
                                      'swap_used,pressure,browser_count,browser_rss,running_workers) '
                                      'VALUES (?,?,8,?,1000,?,0,?,?,?)',
                                      (now - 100 + i, load, 100 + i, 10 + i, i, 1000 * i, i))
            self.store.db.execute('INSERT INTO host_samples(created,running_workers) VALUES (?,2)', (now - 86400,))
            self.store.db.execute('INSERT INTO host_samples(created,load1,running_workers) VALUES (?,999,999)', (cutoff - 1,))
        with patch('time.time', return_value=now):
            result = self.call('metrics.report')
        self.assertTrue(result.ok, result.text)
        self.assertEqual(result.data['days'], 7)
        self.assertEqual(result.data['since'], '2026-10-05T12:00:00Z')
        self.assertEqual(result.data['until'], '2026-10-12T12:00:00Z')
        self.assertEqual(result.data['jobs'], [{
            'task': task['id'], 'topic': '1:2', 'channel': 'Torii', 'number': 1,
            'first_stamps': {'ready': cutoff - 900, 'reviewed': cutoff - 800, 'ci': cutoff - 600,
                             'merged': now - 40, 'live': now - 10},
            'durations': [{'from': 'created', 'to': 'ready', 'seconds': 100.0},
                          {'from': 'ready', 'to': 'reviewed', 'seconds': 100.0},
                          {'from': 'reviewed', 'to': 'ci', 'seconds': 200.0},
                          {'from': 'ci', 'to': 'merged', 'seconds': 605360.0},
                          {'from': 'merged', 'to': 'live', 'seconds': 30.0}]}])
        self.assertEqual(result.data['daily'], [
            {'date': '2026-10-11', 'samples': 1, 'load1_p50': None, 'load1_p95': None, 'load1_max': None,
             'memory_used_max': None, 'swap_used_max': None, 'browser_count_max': None,
             'browser_rss_max': None, 'running_workers_max': 2},
            {'date': '2026-10-12', 'samples': 5, 'load1_p50': 3.0, 'load1_p95': 16.8, 'load1_max': 20.0,
             'memory_used_max': 104, 'swap_used_max': 14, 'browser_count_max': 4,
             'browser_rss_max': 4000, 'running_workers_max': 4}])
        self.assertIn('Torii (1:2) job 1', result.text)
        self.assertIn('created -> ready: 100.0 s', result.text)
        self.assertIn('load1 p50=3.0 p95=16.8 max=20.0', result.text)
        self.assertIn('browser_rss_max=4000 bytes', result.text)

    def test_report_bridges_missing_stages_without_inventing_ready(self):
        task = self.store.task_create('1:2', 'Measure')
        with self.store.db:
            for stage, stamp in (('reviewed', 100), ('live', 130)):
                self.store.db.execute('INSERT INTO task_stamps(task,stage,created) VALUES (?,?,?)', (task['id'], stage, stamp))
        with patch('time.time', return_value=150):
            result = self.call('metrics.report', {'days': 1})
        self.assertTrue(result.ok, result.text)
        self.assertEqual(result.data['jobs'][0]['durations'], [{'from': 'reviewed', 'to': 'live', 'seconds': 30.0}])

    def test_report_is_local_read_op_and_refuses_invalid_days(self):
        result = self.call('metrics.report', {'days': '2'})
        self.assertTrue(result.ok, result.text)
        self.assertEqual(result.data['days'], 2)
        self.assertEqual(control_api.find('metrics.report').kind, control_api.READ)
        for days in (0, -1, 1.5, True, 10 ** 19 - 1):
            result = self.call('metrics.report', {'days': days})
            self.assertEqual((result.ok, result.state), (False, 'refused'))
        for source in ('mcp', 'telegram', 'service'):
            result = self.call('metrics.report', source=source)
            self.assertEqual((result.ok, result.state), (False, 'refused'))
        self.assertNotIn('metrics_report', mcp.TOOLS)

    def test_new_tables_preserve_existing_database_and_stamps_on_reopen(self):
        task = self.store.task_create('1:2', 'Measure')
        self.assertTrue(self.call('tasks.update', {'task': task['id'], 'stage': 'ready'}).ok)
        self.store.close()
        self.store = Store(Path(self.temp.name))
        result = self.call('tasks.get', {'task': task['id']})
        self.assertEqual((result.data['title'], result.data['stamps'][0]['stage']), ('Measure', 'ready'))
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM host_samples').fetchone()[0], 0)
