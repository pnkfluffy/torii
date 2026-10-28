import asyncio
import os
import subprocess
import sys
import time
import unittest
from unittest.mock import patch

from coordinator import updates
from coordinator.service import Service, _Restart
from tests.test_service import ServiceTestsSupport, TOPIC
from tests.test_updates import TemporaryStore, tool


class RestartRecoveryTests(ServiceTestsSupport, unittest.IsolatedAsyncioTestCase):
    def request_update_restart(self, age=61):
        self.store.put(updates.KEY, dict(id='test', phase='restart', old_pid=os.getpid(), old_process='service'))
        request = dict(reason='Telegram update', update_id='test', created=time.time() - age)
        self.store.put('restart_requested_v2', request)
        return request

    async def test_update_restart_bounds_outbox_wait_and_keeps_pending_rows(self):
        self.request_update_restart(age=59)
        row = self.store.enqueue_report(TOPIC, 'Pending report')
        self.store.db.execute('UPDATE outbox SET next_attempt=9999999999')
        with patch.object(updates.watch, 'process_identity', return_value='service'):
            self.assertFalse(await self.service.restart_once())
            request = self.store.get('restart_requested_v2')
            request['created'] -= 2
            self.store.put('restart_requested_v2', request)
            with self.assertRaises(_Restart):
                await self.service.restart_once()
        self.assertEqual(self.store.db.execute('SELECT delivered FROM outbox WHERE id=?', (row,)).fetchone()[0], 0)
        self.assertIsNotNone(self.store.get(updates.KEY)['restart_started'])

    async def test_update_restart_bypasses_recent_restart_guard(self):
        self.request_update_restart()
        self.store.put('last_restart_v2', time.time())
        with patch.object(updates.watch, 'process_identity', return_value='service'):
            with self.assertRaises(_Restart):
                await self.service.restart_once()
        self.assertIsNotNone(self.store.get(updates.KEY)['restart_started'])

    async def test_update_restart_waits_for_send_and_leaves_next_row_pending(self):
        entered, finish = asyncio.Event(), asyncio.Event()
        first = self.store.enqueue_report(TOPIC, 'In flight')
        second = self.store.enqueue_report(TOPIC, 'Next report')
        async def send(row):
            entered.set()
            await finish.wait()
            return dict(message_id=77)
        self.telegram.send = send
        self.request_update_restart()
        delivery = asyncio.create_task(self.service.deliver_once())
        await entered.wait()
        with patch.object(updates.watch, 'process_identity', return_value='service'):
            restart = asyncio.create_task(self.service.restart_once())
            try:
                await asyncio.sleep(0.02)
                self.assertFalse(restart.done())
                finish.set()
                await delivery
                with self.assertRaises(_Restart):
                    await restart
                self.assertFalse(await self.service.deliver_once())
            finally:
                finish.set()
                await asyncio.gather(delivery, restart, return_exceptions=True)
        rows = self.store.db.execute('SELECT id,delivered FROM outbox ORDER BY id').fetchall()
        self.assertEqual([(row['id'], row['delivered']) for row in rows], [(first, 1), (second, 0)])

    async def test_startup_keeps_and_performs_restart_without_update_record(self):
        self.store.put('restart_requested_v2', dict(reason='deploy', created=time.time()))
        service = Service(self.store, self.telegram, self.runner, self.root)
        pending = self.store.get('restart_requested_v2')
        self.assertIsNotNone(pending)
        self.assertEqual(pending['reason'], 'deploy')
        with self.assertRaises(_Restart):
            await service.restart_once()

    async def test_startup_keeps_and_performs_restart_without_matching_update_id(self):
        self.store.put(updates.KEY, dict(id='live-update', phase='offer'))
        self.store.put('restart_requested_v2', dict(reason='deploy', created=time.time(), update_id='different'))
        service = Service(self.store, self.telegram, self.runner, self.root)
        pending = self.store.get('restart_requested_v2')
        self.assertIsNotNone(pending)
        self.assertEqual(pending['reason'], 'deploy')
        with self.assertRaises(_Restart):
            await service.restart_once()


class WatchdogRecoveryTests(TemporaryStore):
    def run_watchdog(self, record, exit_at=0, health_at=None, health_commit=None, timeout=240, restart_start_at=None):
        self.store.put(updates.KEY, record)
        clock = [0]
        head = [record['target']]
        killed = []
        installs = []
        versions = {item['binary']: item['latest'] for item in record.get('attempted', [])}
        def identity(pid):
            if pid == 111:
                return 'old' if clock[0] < exit_at and not killed else None
            return {222: 'new', 333: 'back', os.getpid(): 'watchdog'}.get(pid)
        def sleep(seconds):
            clock[0] += 1
            current = self.store.get(updates.KEY)
            if restart_start_at is not None and clock[0] == restart_start_at:
                updates.watch.write(self.store.db, 'test', restart_started=1000 + clock[0],
                                    restart_from=dict(pid=111, process='old'))
                current = self.store.get(updates.KEY)
            if health_at is not None and clock[0] >= health_at and current['phase'] == 'restart':
                updates.watch.write(self.store.db, 'test', health=dict(pid=222, process='new',
                    commit=health_commit or head[0], restart_started=current.get('restart_started')))
            if clock[0] > 600:
                self.fail('Watchdog did not settle')
        def command(argv, *args, **kwargs):
            if argv[0] == 'git':
                return head[0]
            current = self.store.get(updates.KEY)
            updates.watch.write(self.store.db, 'test', health=dict(pid=333, process='back',
                commit=head[0], restart_started=current.get('restart_started')))
            return ''
        def rollback(current):
            head[0] = current['old']
            return 'Torii restored.'
        def restore(runtime, database=None):
            installs.append((runtime['package'], 'offline'))
            for item in record.get('attempted', []):
                versions[item['binary']] = item['current']
        with patch.object(updates.watch.time, 'monotonic', side_effect=lambda: clock[0]):
            with patch.object(updates.watch.time, 'time', side_effect=lambda: 1000 + clock[0]):
                with patch.object(updates.watch.time, 'sleep', side_effect=sleep):
                    with patch.object(updates.watch, 'process_identity', side_effect=identity):
                        with patch.object(updates.watch, 'command', side_effect=command):
                            with patch.object(updates.watch, 'rollback_code', side_effect=rollback):
                                with patch.object(updates.watch, 'restore_runtime', side_effect=restore, create=True):
                                    with patch.object(updates.watch, 'version', side_effect=lambda binary: versions[binary]):
                                        with patch.object(updates.watch.os, 'kill', side_effect=lambda *args: killed.append(args)):
                                            updates.watch.watchdog(self.store.db, 'test', restart_timeout=timeout)
        return self.store.get(updates.KEY), head[0], installs, killed, clock[0]

    def pending(self, phase='restart', **fields):
        return self.record(phase, tools=[], attempted=[], old_pid=111, old_process='old', **fields)

    def test_stuck_service_backstop_starts_owned_restart(self):
        record = self.pending()
        current, head, installs, killed, elapsed = self.run_watchdog(record, exit_at=500, health_at=112)
        self.assertEqual(current['phase'], 'updated')
        self.assertEqual(head, record['target'])
        self.assertEqual(killed, [(111, updates.watch.signal.SIGKILL)])
        self.assertEqual(current['restart_from'], dict(pid=111, process='old'))

    def test_backstop_race_cannot_kill_shutdown_started_by_service(self):
        record = self.pending()
        original = updates.watch.write
        def write(db, update_id, **fields):
            if fields.get('restart_pending') and not self.store.get(updates.KEY).get('restart_started'):
                original(db, update_id, expected='restart', restart_started=updates.watch.time.time(),
                         restart_from=dict(pid=111, process='old'), shutdown_grace=50)
            return original(db, update_id, **fields)
        with patch.object(updates.watch, 'write', side_effect=write):
            current, head, installs, killed, elapsed = self.run_watchdog(record, exit_at=111, health_at=112)
        self.assertEqual(current['phase'], 'updated')
        self.assertFalse(killed)

    def test_crash_before_restart_rolls_back_checkout_and_touched_cli(self):
        item = tool()
        record = self.pending('tools')
        record.update(tools=[item], attempted=[item], runtime_installs=[dict(package=item['package'], stage=str(self.root / 'previous'))])
        current, head, installs, killed, elapsed = self.run_watchdog(record)
        self.assertEqual(current['phase'], 'rolled_back')
        self.assertEqual(head, record['old'])
        self.assertEqual(installs, [('@anthropic-ai/claude-code', 'offline')])

    def test_target_health_timeout_rolls_back_after_backstop(self):
        record = self.pending()
        current, head, installs, killed, elapsed = self.run_watchdog(record, exit_at=500, timeout=2)
        self.assertEqual(current['phase'], 'rolled_back')
        self.assertEqual(head, record['old'])
        self.assertTrue(killed)

    def test_late_service_exit_gets_full_health_window(self):
        record = self.pending()
        current, head, installs, killed, elapsed = self.run_watchdog(record, exit_at=110, health_at=345, restart_start_at=109)
        self.assertEqual(current['phase'], 'updated')
        self.assertEqual(elapsed, 345)
        self.assertFalse(killed)

    def test_unowned_service_restart_at_target_rolls_back(self):
        record = self.pending(health=dict(pid=222, process='new', commit='b' * 40))
        current, head, installs, killed, elapsed = self.run_watchdog(record)
        self.assertEqual(current['phase'], 'rolled_back')
        self.assertEqual(head, record['old'])

    def test_updated_requires_every_planned_cli(self):
        record = self.pending(restart_started=1000, restart_from=dict(pid=111, process='old'))
        record['tools'] = [tool()]
        record['health'] = dict(pid=222, process='new', commit=record['target'], restart_started=1000)
        current, head, installs, killed, elapsed = self.run_watchdog(record, timeout=2)
        self.assertEqual(current['phase'], 'rolled_back')
        self.assertEqual(head, record['old'])

    def test_external_deploy_restores_touched_cli_and_leaves_code(self):
        record = self.pending('tools')
        record['attempted'] = [tool()]
        self.store.put(updates.KEY, record)
        with patch.object(updates.watch, 'command', return_value='c' * 40):
            with patch.object(updates.watch, 'stop_installation') as stop:
                with patch.object(updates.watch, 'install') as install:
                    with patch.object(updates.watch, 'version', return_value='1.0.0'):
                        updates.watch.watchdog(self.store.db, 'test')
        install.assert_not_called()
        current = self.store.get(updates.KEY)
        self.assertEqual(current['phase'], 'failed')
        self.assertIn('stood down', current['result'])
        self.assertIn('Claude Code restored to 1.0.0.', updates.outcome(current))

    def test_failed_cli_restoration_does_not_write_final_phase(self):
        record = self.pending('tools')
        record['attempted'] = [tool()]
        self.store.put(updates.KEY, record)
        with patch.object(updates.watch, 'command', return_value='c' * 40):
            with patch.object(updates.watch, 'stop_installation'):
                with patch.object(updates.watch, 'install', side_effect=RuntimeError('broken CLI')):
                    with patch.object(updates.watch, 'version', side_effect=RuntimeError('unreadable')):
                        updates.watch.watchdog(self.store.db, 'test')
        self.assertNotIn(self.store.get(updates.KEY)['phase'], updates.TERMINAL)

    def test_stand_down_rechecks_checkout_after_restoring_cli(self):
        self.pending('tools')
        with patch.object(updates.watch, 'command', side_effect=['c' * 40, 'b' * 40]):
            with patch.object(updates.watch, 'process_identity', return_value='watchdog'):
                updates.watch.watchdog(self.store.db, 'test')
        self.assertNotIn(self.store.get(updates.KEY)['phase'], updates.TERMINAL)

    def test_dead_cli_only_rollback_requires_restart_after_cli_restoration(self):
        stamp = updates.watch.process_identity(os.getpid())
        record = self.record('rollback_restart', target='a' * 40, attempted=[tool()],
            restart_started=time.time(), restart_from=dict(pid=os.getpid(), process=stamp),
            old_pid=111, old_process='old')
        with patch.object(updates, 'git', return_value='a' * 40):
            with patch.object(updates.watch, 'version', return_value='1.0.0'):
                facts = updates.recovery_facts(record, 'a' * 40, stamp)
        self.assertEqual(facts['phase'], 'failed')
        self.assertIn('The service has not restarted after this update step.', facts['restored'])

    def test_dead_record_does_not_call_startup_snapshot_healthy(self):
        record = self.record('restart', tools=[], attempted=[], old_pid=111, old_process='old',
            restart_started=time.time(), restart_from=dict(pid=111, process='old'))
        stamp = updates.watch.process_identity(os.getpid())
        with patch.object(updates, 'git', return_value='b' * 40):
            facts = updates.recovery_facts(record, 'b' * 40, stamp)
        self.assertEqual(facts['phase'], 'failed')
        self.assertIn('A healthy poll after this update step is unverified.', facts['restored'])

    def test_dead_record_card_names_code_and_each_remaining_cli_change(self):
        self.record('tools', attempted=[tool(), tool('Codex')], watchdog_pid=999999, watchdog_process='dead')
        with patch.object(updates, 'git', return_value='b' * 40):
            with patch.object(updates.watch, 'version', side_effect=['2.0.0', '1.0.0']):
                self.assertTrue(updates.recover(self.store))
        self.assertIn('Torii code remains changed', self.latest_text())
        self.assertIn('Claude Code remains changed', self.latest_text())
        self.assertIn('Codex is restored', self.latest_text())


class ProcessIdentityRecoveryTests(unittest.TestCase):
    def test_process_identity_is_stable_across_caller_environments(self):
        process = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])
        self.addCleanup(lambda: process.poll() is None and (process.kill(), process.wait(timeout=5)))
        code = 'from coordinator import updates; print(updates.watch.process_identity(' + str(process.pid) + '))'
        base = dict(PATH='/usr/bin:/bin', PYTHONDONTWRITEBYTECODE='1')
        reference = subprocess.check_output([sys.executable, '-c', code], cwd=updates.ROOT, env=base, text=True).strip()
        self.assertNotEqual(reference, 'None')
        for env in (dict(TZ='Asia/Tokyo'), dict(LC_ALL='fr_FR.UTF-8'),
                    dict(LC_ALL='ja_JP.UTF-8'), dict(LANG='ja_JP.UTF-8')):
            with self.subTest(env=env):
                actual = subprocess.check_output([sys.executable, '-c', code], cwd=updates.ROOT,
                                                 env=dict(base, **env), text=True).strip()
                self.assertEqual(actual, reference)
