import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from coordinator import control_api
from coordinator.health import _codex, _subscription, format_health, system_pressure
from coordinator.store import Store
from coordinator.controls import handle_control


class HealthTestsSupport:
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name))
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd) VALUES ('1:2',1,2,'Torii','')")
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd) VALUES ('1:3',1,3,'Orbit','')")
            self.store.put('accounts', {'work': {'config_dir': self.temp.name, 'enabled': True}})
            self.store.put('account_status', {'work': {'identity': {'email': 'work', 'logged_in': True}, 'usage': {
                'five_hour': {'utilization': 22}, 'seven_day': {'utilization': 81}}}})

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()


class HealthTests(HealthTestsSupport, unittest.TestCase):
    def test_parent_provider_and_pause_are_visible_and_counted(self):
        self.store.put('coordinator_home_topic', '1:2')
        self.store.put('coordinator_host', {'id': 'host', 'provider': 'codex', 'account': 'gpt'})
        self.store.put('codex_account_status', {'gpt': {'identity': {'email': 'gpt@example.test'}}})
        with patch('coordinator.host.host_state', return_value='running'), \
                patch('coordinator.health.system_pressure', return_value=[]):
            text = control_api.call(self.store, 'health.show').text
        self.assertIn('parent live (ChatGPT gpt@example.test)', text)
        self.assertIn('Agents running: 1', text)
        self.store.put('coordinator_account_retry_at', time.time() + 3600)
        with patch('coordinator.host.host_state', return_value='exited'), \
                patch('coordinator.health.system_pressure', return_value=[]):
            text = control_api.call(self.store, 'health.show').text
        self.assertIn('parent paused until', text)

    def test_formatter_groups_running_and_counts_needs_input(self):
        topics = [{'id': '1:2', 'name': 'Torii'}, {'id': '1:3', 'name': 'Orbit'}]
        workers = [{'topic': '1:2', 'status': 'running', 'number': 4, 'title': 'Build /health'},
                   {'topic': '1:2', 'status': 'needs_input', 'number': 3, 'title': 'Waiting'},
                   {'topic': '1:3', 'status': 'running', 'number': 8, 'title': 'Check *UI*'},
                   {'topic': '1:3', 'status': 'done', 'number': 7, 'title': 'Done'}]
        text = format_health(topics, workers, ['CPU: 1.0 load / 8 cores'], '**Subscriptions left** (5h · week · Fable)')
        self.assertIn('**Agents running: 2**', text)
        self.assertIn('**Torii**: 1 running, 1 needs input\n- Job 4: Build /health', text)
        self.assertIn('**Orbit**: 1 running\n- Job 8: Check \\*UI\\*', text)
        self.assertNotIn('Waiting', text)
        self.assertNotIn('Done', text)

    def test_live_parent_counts_as_running_agent(self):
        topics = [{'id': '1:2', 'name': 'Torii'}, {'id': '1:3', 'name': 'Orbit'}]
        workers = [{'topic': '1:2', 'status': 'running', 'number': 4, 'title': 'Build'},
                   {'topic': '1:3', 'status': 'running', 'number': 8, 'title': 'Check'}]
        text = format_health(topics, workers, [], '', {'1:2': 'live', '1:3': 'wound down'})
        self.assertIn('**Agents running: 3**', text)
        self.assertIn('**Torii**: 2 running · parent live\n- Job 4: Build', text)
        self.assertIn('**Orbit**: 1 running · parent wound down\n- Job 8: Check', text)
        self.assertNotIn('Parent:', text)

    def test_system_pressure_parses_bounded_mac_commands(self):
        outputs = {
            ('sysctl', '-n', 'hw.ncpu'): '8\n',
            ('sysctl', '-n', 'hw.memsize'): '17179869184\n',
            ('vm_stat',): 'Mach Virtual Memory Statistics: (page size of 4096 bytes)\nPages free: 1048576.\nPages inactive: 1048576.\nPages speculative: 0.\n',
            ('memory_pressure', '-Q'): 'System-wide memory free percentage: 36%\n',
            ('sysctl', '-n', 'vm.swapusage'): 'total = 1024.00M  used = 256.00M  free = 768.00M\n',
            ('df', '-kP', '/'): 'Filesystem 1024-blocks Used Available Capacity Mounted on\n/dev/disk 104857600 52428800 52428800 50% /\n',
            ('df', '-kP', self.temp.name): 'Filesystem 1024-blocks Used Available Capacity Mounted on\n/dev/disk 104857600 52428800 52428800 50% /\n',
        }

        def run(args, capture_output, text, timeout, check):
            self.assertEqual(timeout, 1)
            return type('Completed', (), {'stdout': outputs[tuple(args)]})()

        with patch('coordinator.host_os.SYSTEM', 'darwin'), patch('coordinator.health.subprocess.run', side_effect=run), patch('coordinator.health.os.getloadavg', return_value=(2.5, 1, 1)):
            lines = system_pressure(self.temp.name)
        self.assertEqual(lines, ['CPU: 2.5 load / 8 cores',
                                 'RAM: 8.0G used / 16.0G, 36% pressure free, swap 0.2G',
                                 'Disk /: 50.0G free / 100.0G', 'Disk state: 50.0G free / 100.0G'])

    def test_health_command_reads_workers_and_cached_remaining_usage(self):
        task = self.store.task_create('1:2', 'Report status')
        with self.store.db:
            self.store.db.execute("INSERT INTO workers(task,topic,provider,prompt,status,created,updated) VALUES (?,'1:2','claude','p','running',1,1)", (task['id'],))
            with patch('coordinator.health.system_pressure', return_value=['CPU: test']):
                self.assertTrue(handle_control(self.store, '1:2', 42, '/health'))
        row = self.store.db.execute('SELECT text,kind,reply_to FROM outbox ORDER BY id DESC LIMIT 1').fetchone()
        self.assertEqual((row['kind'], row['reply_to']), ('status', 42))
        self.assertIn('**Agents running: 1**\n**Torii**: 1 running · parent wound down\n- Job 1: Report status', row['text'])
        self.assertIn('**Subscriptions left** (5h · week · Fable)\n🟡 work: 78% · 19% · ?, stale since unknown', row['text'])
        self.assertNotRegex(row['text'], r'/[A-Za-z]')
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM workers').fetchone()[0], 1)

    def test_subscription_line_shows_readable_times_and_disabled_accounts(self):
        now = time.time()
        accounts = {}
        status = {}
        for alias, enabled, observed in (('old', True, now - 3600), ('reset', True, None),
                                         ('full', True, now), ('off', False, now)):
            home = Path(self.temp.name) / alias
            home.mkdir()
            accounts[alias] = {'config_dir': str(home), 'enabled': enabled}
            status[alias] = {'identity': {'email': alias + '@example.com', 'logged_in': True},
                             'observed_at': observed, 'usage': {'five_hour': {'utilization': 1}}}
        with self.store.db:
            self.store.put('accounts', accounts)
            self.store.put('account_status', status)
            self.store.put('account_blocks', {'full': {'until': now + 7200, 'reason': 'quota'}})
        text = _subscription(self.store)
        self.assertNotRegex(text, r'\d{9,}', text)
        self.assertNotIn('None', text)
        lines = text.split('\n')
        self.assertEqual(lines[0], '**Subscriptions left** (5h · week · Fable)')
        self.assertEqual(len(lines), 5, text)
        self.assertRegex(text, r'\n🟡 old@example\.com: 99% · \? · \?, stale since 20\d\d-[^\n]*UTC\n', text)
        self.assertIn('\n🟡 reset@example.com: 99% · ? · ?, stale since unknown', text)
        self.assertRegex(text, r'\n🔴 full@example\.com: 99% · \? · \?, resets in 1h 59m\n', text)
        self.assertIn('\n⚫ off@example.com: 99% · ? · ?, disabled', text)
        self.assertNotRegex(text, r'/[A-Za-z]')

    def test_subscription_weekly_reset_is_shown_only_for_available_accounts(self):
        now = time.time()
        usage = {'five_hour': {'utilization': 22}, 'seven_day': {'utilization': 81,
                                                                  'resets_at': now + 25 * 3600}}
        with self.store.db:
            self.store.put('account_status', {'work': {'identity': {'email': 'work', 'logged_in': True},
                                                       'usage': usage}})
        with patch('coordinator.health.time.time', return_value=now), patch(
                'coordinator.health._until', return_value='1d 1h'):
            text = _subscription(self.store)
        self.assertIn('🟡 work: 78% · 19% · ?, stale since unknown, week resets in 1d 1h', text)

        for reset in (None, now - 1):
            usage['seven_day'].pop('resets_at', None)
            if reset is not None:
                usage['seven_day']['resets_at'] = reset
            with self.store.db:
                self.store.put('account_status', {'work': {'identity': {'email': 'work', 'logged_in': True},
                                                           'usage': usage}})
            with patch('coordinator.health.time.time', return_value=now):
                self.assertNotIn('week resets in', _subscription(self.store))

        with self.store.db:
            self.store.put('account_blocks', {'work': {'until': now + 7200, 'reason': 'quota'}})
        with patch('coordinator.health.time.time', return_value=now):
            self.assertIn('🔴 work: 78% · 19% · ?, resets in 2h 0m', _subscription(self.store))
            self.assertNotIn('week resets in', _subscription(self.store))

    def test_codex_block_shows_cached_weekly_left_active_account_and_banked_resets(self):
        self.assertIsNone(_codex(self.store))
        now = time.time()
        accounts = {}
        status = {}
        for alias, enabled, signed, observed, used, banked in (
                ('orbit', True, True, now, 74, 3), ('full', True, True, now, 100, 2), ('old', True, True, now - 3600, 40, 0),
                ('off', False, True, now, 10, 1), ('new', True, False, None, None, None)):
            home = Path(self.temp.name) / ('codex-' + alias)
            home.mkdir()
            accounts[alias] = {'config_dir': str(home), 'enabled': enabled}
            status[alias] = {'identity': {'email': alias + '@example.com', 'logged_in': signed}, 'observed_at': observed,
                             'resets_available': banked, 'credits': {'has_credits': True, 'balance': '5'},
                             'usage': {'seven_day': {'utilization': used, 'resets_at': now + 41 * 3600 + 30}}
                             if used is not None else {}}
        with self.store.db:
            self.store.put('codex_accounts', accounts)
            self.store.put('codex_account_status', status)
            self.store.put('codex_active_account', 'orbit')
        text = _codex(self.store)
        self.assertEqual(text.split('\n'), [
            '**ChatGPT left** (week)',
            '🟢 orbit@example.com: 26%, week resets in 1d 17h · active · 3 resets banked',
            '🔴 full@example.com: 0%, resets in 1d 17h · 2 resets banked',
            '⚫ off@example.com: 90%, disabled · 1 reset banked',
            text.split('\n')[4],
            '⚪ new@example.com: ?, not signed in'])
        self.assertRegex(text, r'\n🟡 old@example\.com: 60%, stale since 20\d\d-[^\n]*UTC, week resets in 1d 17h\n')
        self.assertNotIn('redit', text)
        with patch('coordinator.health.system_pressure', return_value=['CPU: test']):
            shown = control_api.call(self.store, 'health.show').text
        self.assertIn('**ChatGPT left** (week)\n🟢 orbit@example.com: 26%, week resets in 1d 17h · active', shown)

    def test_health_keeps_original_host_owner_after_home_changes_and_clears(self):
        with self.store.db:
            self.store.db.execute('UPDATE topics SET cwd=?,enabled=1', (self.temp.name,))
            self.store.put('coordinator_home_topic', '1:2')
            self.store.put('coordinator_host', {'id': 'original'})
            self.store.put('coordinator:1:3_host', {'id': 'other'})
        for params in ({'topic': '1:3'}, {'topic': '1:3', 'clear': True}):
            with self.store.db:
                self.assertTrue(control_api.call(self.store, 'topic.home', params).ok)
            with patch('coordinator.health.system_pressure', return_value=[]), patch(
                    'coordinator.host.host_state', side_effect=lambda path: 'running' if path.name == 'original' else 'dead'):
                text = control_api.call(self.store, 'health.show').text
            self.assertIn('**Agents running: 1**\n**Torii**: 1 running · parent live', text)
            self.assertIn('**Orbit**: 0 running · parent wound down', text)

    def test_health_shows_live_and_wound_down_parents_by_topic(self):
        with self.store.db:
            self.store.put('coordinator_home_topic', '1:2')
            self.store.put('coordinator_host', {'id': 'coordinator-test'})
            self.store.put('coordinator:1:3_session', 'saved')
        with patch('coordinator.health.system_pressure', return_value=[]), patch(
                'coordinator.host.host_state', return_value='running'):
            self.assertTrue(handle_control(self.store, '1:2', 42, '/health'))
        row = self.store.db.execute('SELECT text FROM outbox ORDER BY id DESC LIMIT 1').fetchone()
        self.assertIn('**Agents running: 1**\n**Torii**: 1 running · parent live', row['text'])
        self.assertIn('**Orbit**: 0 running · parent wound down', row['text'])
