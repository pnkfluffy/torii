import asyncio
import json
import os
import sqlite3
from pathlib import Path
from types import SimpleNamespace
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import AsyncMock, Mock, patch

from coordinator import cli_identity, control_api, updates
from coordinator.controls import handle_control
from coordinator.control_ui import accept_callback
from coordinator.onboarding import COMMAND_MENU, command_menu
from coordinator.session import CoordinatorSession
from coordinator.store import Store


def tool(name='Claude Code', current='1.0.0', latest='2.0.0', prefix='/temporary/runtime'):
    return dict(label=name, package='@anthropic-ai/claude-code' if name == 'Claude Code' else '@openai/codex',
                binary='/temporary/bin/cli', prefix=prefix, current=current, latest=latest,
                outdated=updates.newer(current, latest))


def plan(subjects=(), tools=()):
    return dict(old='a' * 40, target='b' * 40 if subjects else 'a' * 40,
                subjects=list(subjects), tools=list(tools))


class CardTests(unittest.TestCase):
    def test_current(self):
        value = plan(tools=[tool(current='2.0.0'), tool('Codex', current='2.0.0')])
        self.assertEqual(updates.card(value), 'Torii and its npm tools are current.')
        self.assertFalse(updates.available(value))

    def test_torii_only_and_cap(self):
        text = updates.card(plan(['change %d' % i for i in range(14)]))
        self.assertEqual(text.splitlines()[0], 'Update Torii?')
        self.assertIn('• change 9\nand 4 more', text)
        self.assertNotIn('• change 10', text)
        self.assertTrue(text.endswith('Running chats and workers keep going.'))

    def test_subjects_with_html_escaping_still_fit_one_card(self):
        value = plan(['&' * 400] * 10, [tool(), tool('Codex')])
        self.assertLess(len(updates.markdown_to_html(updates.card(value))), 4000)

    def test_tools_only_and_combined(self):
        value = plan(tools=[tool(), tool('Codex')])
        self.assertEqual(updates.card(value), 'Update 2 tools?\nClaude Code 1.0.0 → 2.0.0\n'
                         'Codex 1.0.0 → 2.0.0\nRunning chats and workers keep going.')
        value['subjects'] = ['fix a bug']
        self.assertTrue(updates.card(value).startswith('Update Torii and 2 tools?'))
        value['tools'].pop()
        self.assertTrue(updates.card(value).startswith('Update Torii and 1 tool?'))

    def test_non_npm_version_is_shown_without_update(self):
        value = plan(tools=[tool(prefix=None)])
        self.assertFalse(updates.available(value))
        self.assertIn('Claude Code 1.0.0 → 2.0.0 · not installed through npm', updates.card(value))

    def test_no_downgrade_for_newer_installed_version(self):
        self.assertFalse(updates.newer('3.0.0', '2.0.0'))

    def test_semver_never_offers_prerelease_or_unknown_downgrades(self):
        for current, latest, expected in (
                ('3.0.0-beta.1', '2.0.0', False), ('2.0.0', '2.0.0-beta.1', False),
                ('2.0.0-beta.10', '2.0.0-beta.2', False), ('2.0.0-beta.2', '2.0.0-beta.10', True),
                ('2.0.0-beta.1', '2.0.0', True), ('2.0.0+build.1', '2.0.0+build.2', False),
                ('2.0.0-beta.01', '2.0.0-beta.2', False), ('2.0.0-foo_bar', '2.0.0', False),
                ('unknown', '2.0.0', False)):
            with self.subTest(current=current, latest=latest):
                self.assertEqual(updates.newer(current, latest), expected)

    def test_cli_only_outcome_names_only_changed_tools(self):
        record = dict(phase='updated', result='Updated.', attempted=[tool()], **plan(tools=[tool()]))
        self.assertEqual(updates.outcome(record), 'Updated.\nClaude Code 1.0.0 → 2.0.0')


class TemporaryStore(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.store = Store(self.root / 'state')
        with self.store.db:
            self.store.put('owner', 7)
            self.store.put('group', -10042)
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) "
                                  "VALUES ('-10042:4',-10042,4,'Test','',1)")
        self.service = SimpleNamespace(store=self.store, runner=SimpleNamespace(binaries={}), wake=lambda name: Mock())
        self.updater = updates.Updater(self.service)
        self.updater.root = self.root / 'checkout'

    def tearDown(self):
        self.store.close()
        self.temporary.cleanup()

    def command(self, owner=7):
        return self.store.accept({'update_id': 123 + owner, 'message': {'message_id': 42, 'message_thread_id': 4,
            'from': {'id': owner}, 'chat': {'id': -10042, 'type': 'supergroup'}, 'text': '/update'}})

    def deliver(self):
        with self.store.db:
            self.store.db.execute('UPDATE outbox SET delivered=1,telegram_message=1000')

    def record(self, phase, **values):
        record = dict(id='test', topic='-10042:4', message=1000, phase=phase, root=str(self.updater.root),
                      **plan(['change'], [tool()]))
        if phase in ('armed', 'merge', 'tools', 'restart', 'rolling_back', 'rollback_restart'):
            stamp = updates.watch.process_identity(os.getpid())
            record.update(old_pid=os.getpid(), old_process=stamp,
                          watchdog_pid=os.getpid(), watchdog_process=stamp)
        record.update(values)
        with self.store.db:
            self.store.put(updates.KEY, record)
        return record

    def once(self):
        return asyncio.run(self.updater.once())

    def latest_text(self):
        return self.store.db.execute('SELECT text FROM outbox ORDER BY id DESC LIMIT 1').fetchone()[0]


class OwnerTests(TemporaryStore):
    def test_owner_only_and_off_by_default(self):
        self.assertEqual(self.command(owner=8), 'unauthorized')
        self.assertIsNone(self.store.get(updates.KEY))
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM outbox').fetchone()[0], 0)
        self.assertEqual(self.command(), 'control')
        self.assertIn('Updates from Telegram are off', self.latest_text())
        self.assertIn('ctl call service.updates enabled=true', self.latest_text())
        self.assertIsNone(self.store.get(updates.KEY))
        self.assertNotIn('update', [item['command'] for item in COMMAND_MENU])
        self.assertEqual(command_menu(self.store), COMMAND_MENU)

    def test_setting_and_command_menu(self):
        with self.store.db:
            result = control_api.call(self.store, 'service.updates', {'enabled': True})
        self.assertTrue(result.ok)
        self.assertIn('update', [item['command'] for item in command_menu(self.store)])
        self.command()
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'checking')

    def test_check_edits_one_card_and_current_has_no_buttons(self):
        self.store.put('telegram_updates_enabled', True)
        self.command()
        self.assertFalse(self.once())
        self.deliver()
        with patch.object(updates, 'inventory', return_value=plan()):
            self.assertTrue(self.once())
        row = self.store.db.execute('SELECT * FROM outbox ORDER BY id DESC LIMIT 1').fetchone()
        self.assertEqual(row['edit_message'], 1000)
        self.assertIsNone(row['reply_markup'])
        self.assertEqual(self.latest_text(), 'Torii and its npm tools are current.')

    def offer(self):
        self.store.put('telegram_updates_enabled', True)
        with self.store.db:
            handle_control(self.store, '-10042:4', 42, '/update')
        self.deliver()
        with patch.object(updates, 'inventory', return_value=plan(['fix'], [tool()])):
            self.once()
        self.deliver()
        row = self.store.db.execute('SELECT * FROM outbox ORDER BY id DESC LIMIT 1').fetchone()
        return json.loads(row['reply_markup'])['inline_keyboard'][0]

    def callback(self, data, owner=7):
        with self.store.db:
            return accept_callback(self.store, {'from': {'id': owner}, 'data': data,
                'message': {'message_id': 1000, 'message_thread_id': 4,
                            'chat': {'id': -10042, 'type': 'supergroup'}}})

    def test_confirm_and_duplicate_are_owner_protected(self):
        buttons = self.offer()
        self.assertEqual([button['text'] for button in buttons], ['Update', 'Cancel'])
        self.assertEqual(self.callback(buttons[0]['callback_data'], owner=8), 'unauthorized')
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'offer')
        self.assertEqual(self.callback(buttons[0]['callback_data']), 'control_callback')
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'queued')
        self.assertEqual(self.callback(buttons[0]['callback_data']), 'stale_callback')

    def test_cancel_and_setting_turned_off(self):
        for disable in (False, True):
            buttons = self.offer()
            if disable:
                self.store.put('telegram_updates_enabled', False)
            self.callback(buttons[0 if disable else 1]['callback_data'])
            self.assertEqual(self.store.get(updates.KEY)['phase'], 'cancelled')
            self.assertIn('off' if disable else 'cancelled', self.latest_text())


class CheckoutTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.remote = self.root / 'remote.git'
        self.source = self.root / 'source'
        self.live = self.root / 'checkout'
        self.git(self.root, 'init', '--bare', '--initial-branch=main', str(self.remote))
        self.git(self.root, 'clone', str(self.remote), str(self.source))
        self.git(self.source, 'config', 'user.name', 'Test')
        self.git(self.source, 'config', 'user.email', 'test@example.com')
        (self.source / 'release.py').write_text('value = 1\n')
        self.commit('old')
        self.old = self.git(self.source, 'rev-parse', 'HEAD')
        self.git(self.source, 'push', 'origin', 'main')
        self.git(self.root, 'clone', str(self.remote), str(self.live))
        self.git(self.live, 'config', 'user.name', 'Test')
        self.git(self.live, 'config', 'user.email', 'test@example.com')
        (self.source / 'release.py').write_text('value = 2\n')
        self.commit('new change')
        self.target = self.git(self.source, 'rev-parse', 'HEAD')
        self.git(self.source, 'push', 'origin', 'main')
        self.git(self.live, 'fetch', 'origin', 'main')

    def tearDown(self):
        self.temporary.cleanup()

    def git(self, root, *args):
        return subprocess.run(['git', '-C', str(root), *args], check=True,
                              capture_output=True, text=True).stdout.strip()

    def commit(self, subject):
        self.git(self.source, 'add', '.')
        self.git(self.source, 'commit', '-m', subject)

    def test_local_remote_inventory_and_fast_forward(self):
        with patch.object(updates, 'installation', return_value=(None, None, None)):
            result = updates.inventory(self.live, {})
        self.assertEqual(result, dict(old=self.old, target=self.target, subjects=['new change'], tools=[]))
        self.assertEqual(updates.checkout(self.live, self.target), self.old)
        updates.git(self.live, 'merge', '--ff-only', self.target)
        self.assertEqual(self.git(self.live, 'rev-parse', 'HEAD'), self.target)

    def test_non_npm_tools_show_versions_without_invoking_npm(self):
        with patch.object(updates, 'installation', return_value=('/temporary/cli', None, None)):
            with patch.object(updates.watch, 'version', return_value='1.0.0'):
                with patch.object(updates.watch, 'command', wraps=updates.watch.command) as command:
                    result = updates.inventory(self.live, {})
        self.assertFalse(any(call.args[0][0] == 'npm' for call in command.call_args_list))
        self.assertIn('Claude Code 1.0.0 · not installed through npm.', updates.card(result))

    def test_untracked_files_do_not_block_update_or_rollback(self):
        (self.live / 'untracked').write_text('preserve me')
        self.assertEqual(updates.checkout(self.live, self.target), self.old)
        self.git(self.live, 'merge', '--ff-only', self.target)
        updates.watch.rollback_code(dict(root=str(self.live), old=self.old, target=self.target))
        self.assertEqual((self.live / 'untracked').read_text(), 'preserve me')
        self.assertEqual(self.git(self.live, 'rev-parse', 'HEAD'), self.old)

    def test_detached_and_untracked_branches_are_refused(self):
        self.git(self.live, 'checkout', '--detach', self.old)
        with self.assertRaisesRegex(RuntimeError, 'tracked main branch'):
            updates.checkout(self.live, self.target)
        self.git(self.live, 'checkout', '-b', 'pinned')
        with self.assertRaisesRegex(RuntimeError, 'tracked main branch'):
            updates.checkout(self.live, self.target)
        self.assertEqual(self.git(self.live, 'rev-parse', 'HEAD'), self.old)

    def test_dirty_ahead_and_diverged_change_nothing(self):
        for kind in ('dirty', 'ahead', 'diverged'):
            with self.subTest(kind=kind):
                if kind == 'dirty':
                    (self.live / 'release.py').write_text('local work')
                    target = self.target
                else:
                    if kind == 'ahead':
                        self.git(self.live, 'merge', '--ff-only', self.target)
                        target = self.target
                    else:
                        target = self.old
                    (self.live / 'local').write_text(kind)
                    self.git(self.live, 'add', 'local')
                    self.git(self.live, 'commit', '-m', 'local work')
                    if kind == 'diverged':
                        (self.source / 'other').write_text('remote branch')
                        self.commit('another remote commit')
                        self.git(self.source, 'push', 'origin', 'main')
                        self.git(self.live, 'fetch', 'origin', 'main')
                        target = self.git(self.source, 'rev-parse', 'HEAD')
                before = self.git(self.live, 'rev-parse', 'HEAD')
                with self.assertRaisesRegex(RuntimeError, kind):
                    updates.checkout(self.live, target)
                self.assertEqual(self.git(self.live, 'rev-parse', 'HEAD'), before)
                if kind == 'dirty':
                    self.assertEqual((self.live / 'release.py').read_text(), 'local work')
                    self.git(self.live, 'restore', 'release.py')

    def candidate_with_checks(self):
        (self.source / 'scripts').mkdir()
        shutil_source = updates.ROOT / 'scripts/check-no-comments.py'
        (self.source / 'scripts/check-no-comments.py').write_text(shutil_source.read_text())
        (self.source / 'coordinator').mkdir()
        (self.source / 'coordinator/__init__.py').write_text('')
        (self.source / 'tests').mkdir()
        (self.source / 'tests/__init__.py').write_text('')
        (self.source / 'tests/test_release.py').write_text('import unittest\nfrom release import value\n'
            'class ReleaseTest(unittest.TestCase):\n    def test_release(self):\n        self.assertEqual(value, 2)\n')
        self.commit('checks')
        target = self.git(self.source, 'rev-parse', 'HEAD')
        self.git(self.source, 'push', 'origin', 'main')
        return target

    def test_real_checks_run_fetched_checkout_on_service_interpreter(self):
        target = self.candidate_with_checks()
        record = dict(root=str(self.live), old=self.old, target=target)
        updates.prepare(record, self.root / 'checks')
        self.assertEqual((self.live / 'release.py').read_text(), 'value = 1\n')
        self.assertFalse((self.root / 'checks/checkout').exists())

    def test_resumed_checks_reuse_and_prune_only_their_worktree(self):
        target = self.candidate_with_checks()
        directory = self.root / 'checks'
        self.git(self.live, 'fetch', 'origin', 'main')
        self.git(self.live, 'worktree', 'add', '--detach', str(directory / 'checkout'), target)
        other = self.root / 'other-checkout'
        self.git(self.live, 'worktree', 'add', '--detach', str(other), self.old)
        (directory / 'home').mkdir()
        (directory / 'tmp').mkdir()
        updates.prepare(dict(root=str(self.live), old=self.old, target=target), directory)
        self.assertFalse((directory / 'checkout').exists())
        self.assertEqual(self.git(other, 'rev-parse', 'HEAD'), self.old)

    def test_real_watchdog_rolls_back_with_injected_service_control(self):
        self.git(self.live, 'merge', '--ff-only', self.target)
        store = Store(self.root / 'watch-state')
        script = self.root / 'fake-service.py'
        marker = self.root / 'restart'
        script.write_text('''import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import time
if sys.argv[1] == 'control':
    Path(sys.argv[2]).write_text('restart')
else:
    deadline = time.monotonic() + 10
    while not Path(sys.argv[3]).exists() and time.monotonic() < deadline:
        time.sleep(0.02)
    with sqlite3.connect(sys.argv[2], timeout=5) as db:
        row = db.execute("SELECT value FROM settings WHERE key='telegram_update_v1'").fetchone()
        record = json.loads(row[0])
        record['health'] = dict(pid=os.getpid(), process=subprocess.check_output(
            ['ps', '-p', str(os.getpid()), '-o', 'lstart='], text=True,
            env=dict(os.environ, LC_ALL='C', LANG='C', TZ='UTC')).strip(),
            commit=subprocess.check_output(['git', '-C', record['root'], 'rev-parse', 'HEAD'], text=True).strip(),
            restart_started=record.get('restart_started'))
        db.execute("UPDATE settings SET value=? WHERE key='telegram_update_v1'", (json.dumps(record),))
    time.sleep(10)
''')
        old_service = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(15)'])
        new_service = subprocess.Popen([sys.executable, str(script), 'service',
                                       str(store.directory / 'state.sqlite'), str(marker)])
        watchdog = None
        try:
            with store.db:
                store.put(updates.KEY, dict(id='real', phase='restart', root=str(self.live), old=self.old,
                    target=self.target, tools=[], attempted=[], old_pid=old_service.pid,
                    old_process=updates.watch.process_identity(old_service.pid),
                    restart_started=updates.time.time() - 51, restart_from=dict(pid=old_service.pid,
                    process=updates.watch.process_identity(old_service.pid)), health=None))
            watchdog = subprocess.Popen([sys.executable, str(updates.ROOT / 'scripts/update-watchdog.py'),
                str(store.directory / 'state.sqlite'), 'real', '--restart-timeout', '0.2',
                '--service-control', json.dumps([sys.executable, str(script), 'control', str(marker)])],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            output = watchdog.communicate(timeout=3)
            self.assertEqual(watchdog.returncode, 0, output)
            record = store.get(updates.KEY)
            self.assertEqual(record['phase'], 'rolled_back')
            self.assertEqual(record['health']['pid'], new_service.pid)
            self.assertEqual(self.git(self.live, 'rev-parse', 'HEAD'), self.old)
        finally:
            for process in (watchdog, new_service, old_service):
                if process is not None:
                    if process.poll() is None:
                        process.terminate()
                    process.communicate(timeout=5)
            store.close()

    def test_full_update_checks_merges_restarts_and_edits_original_card(self):
        target = self.candidate_with_checks()
        store = Store(self.root / 'state')
        try:
            with store.db:
                store.put('owner', 7)
                store.put('group', -10042)
                store.put('telegram_updates_enabled', True)
                store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) "
                                 "VALUES ('-10042:4',-10042,4,'Test','',1)")
                updates.request(store, '-10042:4', 42)
            service = SimpleNamespace(store=store, runner=SimpleNamespace(binaries={}), wake=lambda name: Mock())
            updater = updates.Updater(service)
            updater.root = self.live
            def tick():
                with store.db:
                    store.db.execute('UPDATE outbox SET delivered=1,telegram_message=1000')
                return asyncio.run(updater.once())
            with patch.object(updates, 'installation', return_value=(None, None, None)):
                tick()
            record = store.get(updates.KEY)
            with store.db:
                updates.accept_action(store, '-10042:4', 1000, dict(id=record['id'], action='confirm'))
            tick()
            tick()
            self.assertEqual(self.git(self.live, 'rev-parse', 'HEAD'), self.old)
            with patch.object(updates, 'start_watchdog', return_value=os.getpid()):
                tick()
            with store.db:
                record = store.get(updates.KEY)
                record['watchdog_armed'] = True
                store.put(updates.KEY, record)
            tick()
            tick()
            tick()
            self.assertEqual(self.git(self.live, 'rev-parse', 'HEAD'), target)
            self.assertEqual(store.get('restart_requested_v2')['reason'], 'Telegram update')
            with store.db:
                stamp = record['old_process']
                updates.watch.write(store.db, record['id'], restart_started=updates.time.time(),
                                    restart_from=dict(pid=os.getpid(), process=stamp))
            with patch.object(updates, 'ROOT', self.live):
                new_service = updates.Updater(service)
            with patch.object(updates.os, 'getpid', return_value=222):
                with patch.object(updates.watch, 'process_identity', return_value='new'):
                    new_service.healthy('new')
                    updates.watch.watchdog(store.db, record['id'])
            updater = new_service
            tick()
            row = store.db.execute('SELECT * FROM outbox ORDER BY id DESC LIMIT 1').fetchone()
            self.assertEqual(row['edit_message'], 1000)
            self.assertEqual(row['text'], 'Updated.\nTorii ' + self.old[:7] + ' → ' + target[:7])
        finally:
            store.close()

    def test_changed_remote_offer_refuses(self):
        with self.assertRaisesRegex(RuntimeError, 'origin/main changed'):
            updates.prepare(dict(root=str(self.live), old=self.old, target=self.old), self.root / 'checks')

    def test_rollback_reset_keep_preserves_old_commit(self):
        self.git(self.live, 'merge', '--ff-only', self.target)
        record = dict(root=str(self.live), old=self.old, target=self.target)
        self.assertIn('Torii restored', updates.watch.rollback_code(record))
        self.assertEqual(self.git(self.live, 'rev-parse', 'HEAD'), self.old)


class CheckFailureTests(unittest.TestCase):
    def test_each_failed_check_names_it_and_retries_suite_once(self):
        for failed in ('check-no-comments', 'unittest discover', 'compileall'):
            with self.subTest(failed=failed), tempfile.TemporaryDirectory() as directory:
                calls = []
                def run(argv, **kwargs):
                    calls.append(argv)
                    selected = ('check-no-comments' if argv[1].endswith('check-no-comments.py') else
                                'unittest discover' if argv[2] == 'unittest' else 'compileall')
                    self.assertEqual(argv[0], sys.executable)
                    self.assertEqual(Path(kwargs['cwd']), Path(directory) / 'checkout')
                    return SimpleNamespace(returncode=1 if selected == failed else 0)
                with patch.object(updates, 'git'), patch.object(updates.subprocess, 'run', side_effect=run):
                    with self.assertRaisesRegex(RuntimeError, failed + ' failed. Nothing changed.'):
                        updates.checks(Path(directory), 'commit', Path(directory))
                suites = [argv for argv in calls if 'unittest' in argv]
                self.assertEqual(len(suites), 2 if failed == 'unittest discover' else 0 if failed == 'check-no-comments' else 1)

    def test_one_replay_retry_can_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(updates, 'git'), patch.object(updates.subprocess, 'run', side_effect=[
                    SimpleNamespace(returncode=0), SimpleNamespace(returncode=1),
                    SimpleNamespace(returncode=0), SimpleNamespace(returncode=0)]) as run:
                updates.checks(Path(directory), 'commit', Path(directory))
        self.assertEqual(run.call_count, 4)


class StagingTests(TemporaryStore):
    def test_service_cannot_move_record_back_from_watchdog_rollback(self):
        self.record('rolling_back')
        self.updater.save(phase='tools')
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'rolling_back')

    def test_dead_watchdog_settles_success_rollback_and_partial_versions_from_facts(self):
        for phase, commit, actual, expected in (
                ('restart', 'b' * 40, '2.0.0', 'updated'),
                ('rollback_restart', 'a' * 40, '1.0.0', 'rolled_back'),
                ('restart', 'b' * 40, '1.0.0', 'failed')):
            with self.subTest(phase=phase, actual=actual):
                self.record(phase, old_pid=-1, old_process='gone', watchdog_pid=999999,
                            watchdog_process='gone', restart_started=updates.time.time(), attempted=[tool()])
                self.updater.commit = commit
                self.updater.healthy(updates.watch.process_identity(os.getpid()))
                with patch.object(updates, 'git', return_value=commit), patch.object(updates.watch, 'version', return_value=actual):
                    self.assertTrue(self.once())
                self.assertEqual(self.store.get(updates.KEY)['phase'], expected)
                self.assertIn('Claude Code is at ' + actual, self.latest_text())

    def test_restart_command_cannot_interrupt_an_update(self):
        self.record('tools')
        result = control_api.call(self.store, 'service.restart', {'reason': 'other restart'})
        self.assertFalse(result.ok)
        self.assertIn('update is already in progress', result.text)
        self.assertEqual(self.store.service_requests_pending(), [])

    def test_resumed_service_does_not_repeat_mutations(self):
        self.record('tools', old_pid=-1, old_process='gone', watchdog_pid=999999,
                    watchdog_process='gone', attempted=[])
        with patch.object(updates.watch, 'install') as install, patch.object(updates, 'git', return_value='a' * 40):
            self.assertTrue(self.once())
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'failed')
        self.assertTrue(control_api.call(self.store, 'service.restart', {'reason': 'deploy'}).ok)
        install.assert_not_called()

    def test_disable_settles_dead_record_and_finishes_original_card(self):
        self.record('restart', old_pid=-1, old_process='gone', watchdog_pid=999999,
                    watchdog_process='gone', attempted=[])
        with patch.object(updates, 'git', return_value='a' * 40):
            control_api.call(self.store, 'service.updates', {'enabled': False})
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'failed')
        self.assertEqual(self.store.db.execute('SELECT edit_message FROM outbox ORDER BY id DESC LIMIT 1').fetchone()[0], 1000)
        self.assertTrue(control_api.call(self.store, 'service.restart', {'reason': 'deploy'}).ok)

    def test_disable_cancels_work_before_watchdog_launch(self):
        for phase in ('checking', 'queued', 'checks', 'arming'):
            with self.subTest(phase=phase):
                self.record(phase)
                control_api.call(self.store, 'service.updates', {'enabled': False})
                self.assertEqual(self.store.get(updates.KEY)['phase'], 'cancelled')
                self.assertIn('Updates from Telegram are off', self.latest_text())

    def test_watchdog_pid_reuse_does_not_hold_restart_lock(self):
        self.record('tools', watchdog_pid=444, watchdog_process='old watchdog')
        with patch.object(updates.watch, 'process_identity', return_value='new unrelated process'):
            self.assertTrue(control_api.call(self.store, 'service.restart', {'reason': 'deploy'}).ok)

    def test_reused_old_service_pid_does_not_resume_mutations(self):
        self.record('tools', old_pid=os.getpid(), old_process='previous service', watchdog_pid=444,
                    watchdog_process='watchdog', attempted=[])
        with patch.object(updates.watch, 'process_identity', return_value='watchdog'):
            with patch.object(updates.watch, 'install') as install:
                self.assertFalse(self.once())
        install.assert_not_called()

    def test_save_cannot_overwrite_interleaved_watchdog_verdict(self):
        self.record('restart', old_pid=-1)
        self.updater.commit = 'b' * 40
        other = sqlite3.connect(str(self.store.directory / 'state.sqlite'), timeout=5)
        real_get = self.store.get
        reads = []
        def get(key, default=None):
            record = real_get(key, default)
            if key == updates.KEY:
                reads.append(key)
                if len(reads) == 2:
                    updates.watch.write(other, 'test', phase='updated', result='Updated.')
            return record
        try:
            with patch.object(self.store, 'get', side_effect=get):
                self.updater.healthy()
            self.assertEqual(real_get(updates.KEY)['phase'], 'updated')
        finally:
            other.close()

    def test_poll_health_does_not_run_ps_on_the_event_loop(self):
        self.record('restart', old_pid=-1)
        self.updater.commit = 'b' * 40
        with patch.object(updates.watch, 'process_identity', return_value='process') as identity:
            self.updater.healthy()
        identity.assert_not_called()

    def test_new_service_clears_only_its_update_restart_request(self):
        self.record('restart', old_pid=-1, old_process='old')
        self.store.put('restart_requested_v2', dict(update_id='test', reason='Telegram update'))
        with patch.object(updates, 'ROOT', self.updater.root), patch.object(updates, 'git', return_value='b' * 40):
            updates.Updater(self.service)
        self.assertIsNone(self.store.get('restart_requested_v2'))
        self.store.put('restart_requested_v2', dict(reason='someone else'))
        with patch.object(updates, 'ROOT', self.updater.root), patch.object(updates, 'git', return_value='b' * 40):
            updates.Updater(self.service)
        self.assertEqual(self.store.get('restart_requested_v2')['reason'], 'someone else')

    def test_waits_for_outbox_and_restart_guard(self):
        self.record('queued')
        self.store.enqueue_report('-10042:4', 'pending')
        self.assertFalse(self.once())
        self.deliver()
        self.store.put('last_restart_v2', updates.time.time())
        with patch.object(updates, 'prepare') as prepare:
            self.once()
        prepare.assert_not_called()
        self.assertIn('restarted recently', self.latest_text())

    def test_failed_checks_do_not_merge_or_install(self):
        self.record('checks')
        with patch.object(updates, 'prepare', side_effect=RuntimeError('unittest discover failed. Nothing changed.')):
            with patch.object(updates, 'git') as git, patch.object(updates.watch, 'install') as install:
                self.once()
        git.assert_not_called()
        install.assert_not_called()
        self.assertIn('unittest discover failed', self.latest_text())

    def test_failed_cli_install_restores_previous_version(self):
        self.record('tools', watchdog_armed=True, attempted=[])
        with patch.object(updates, 'installation', return_value=('/temporary/bin/cli', '1.0.0', '/temporary/runtime')):
            self.failed_install()

    def failed_install(self):
        with patch.object(updates.watch, 'version', return_value='1.0.0'):
            with patch.object(updates.watch, 'install', side_effect=[RuntimeError('npm failed'), None]) as install:
                with patch.object(updates.watch, 'rollback_code', return_value='Torii restored to aaaaaaa.'), patch.object(
                        updates, 'git', return_value='b' * 40):
                    self.once()
        self.assertEqual([call.args[1] for call in install.call_args_list], ['2.0.0', '1.0.0'])
        self.assertIn('Claude Code restored to 1.0.0.', self.latest_text())
        self.assertIn('Torii restored to aaaaaaa.', self.latest_text())
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'failed')

    def test_cli_only_skips_checks_and_uses_existing_restart_request(self):
        self.record('queued', **plan(tools=[tool()]))
        with patch.object(updates, 'prepare') as checks:
            self.once()
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'arming')
        checks.assert_not_called()
        self.deliver()
        self.record('tools', attempted=[tool()], **plan(tools=[tool()]))
        self.once()
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'restart')
        self.assertEqual(self.store.get('restart_requested_v2')['reason'], 'Telegram update')

    def test_watchdog_must_acknowledge_before_mutation(self):
        self.record('armed', armed_at=updates.time.time())
        with patch.object(updates, 'git') as git, patch.object(updates.watch, 'install') as install:
            self.assertFalse(self.once())
        git.assert_not_called()
        install.assert_not_called()

    def test_watchdog_is_copied_and_detached_before_checkout_changes(self):
        record = self.record('arming')
        with patch.object(updates.subprocess, 'Popen', return_value=SimpleNamespace(pid=444)) as spawn:
            self.assertEqual(updates.start_watchdog(record, self.store.directory), 444)
        args = spawn.call_args.args[0]
        copied = Path(args[1])
        self.assertEqual(copied.read_text(), (updates.ROOT / 'scripts/update-watchdog.py').read_text())
        self.assertEqual(args[0], sys.executable)
        self.assertEqual(args[2:], [str(self.store.directory / 'state.sqlite'), 'test'])
        self.assertTrue(spawn.call_args.kwargs['start_new_session'])
        self.assertTrue(spawn.call_args.kwargs['close_fds'])

    def test_service_interrupted_before_arming_reports_without_repeating(self):
        self.record('arming', old_pid=-1)
        with patch.object(updates, 'start_watchdog') as spawn:
            self.assertTrue(self.once())
        spawn.assert_not_called()
        self.assertIn('Update interrupted', self.latest_text())

    def test_updated_and_rollback_results_edit_same_card_once(self):
        for phase, text in (('updated', 'Updated.'), ('rolled_back', 'Update failed: health deadline.')):
            self.record(phase, result=text, attempted=[tool()], restored=[] if phase == 'updated' else ['Torii restored.'])
            self.once()
            row = self.store.db.execute('SELECT * FROM outbox ORDER BY id DESC LIMIT 1').fetchone()
            self.assertEqual(row['edit_message'], 1000)
            self.assertTrue(row['text'].startswith(text))
            if phase == 'updated':
                self.assertIn('Torii aaaaaaa → bbbbbbb', row['text'])
                self.assertIn('Claude Code 1.0.0 → 2.0.0', row['text'])
            self.assertFalse(self.once())

    def test_health_requires_new_service_commit_snapshot(self):
        self.record('restart', old_pid=123)
        self.updater.commit = None
        self.updater.healthy()
        self.assertIsNone(self.store.get(updates.KEY).get('health'))
        self.updater.commit = 'b' * 40
        with patch.object(updates.watch, 'process_identity', return_value='new process'):
            self.updater.healthy('new process')
        self.assertEqual(self.store.get(updates.KEY)['health']['commit'], 'b' * 40)


class WatchdogTests(TemporaryStore):
    def record_watch(self, **values):
        fields = dict(old_pid=111, old_process='old', tools=[], attempted=[], restart_started=updates.time.time() - 51,
                      restart_from=dict(pid=111, process='old'))
        fields.update(values)
        return self.record('restart', **fields)

    def test_success_requires_new_pid_and_commit(self):
        started = updates.time.time()
        self.record_watch(restart_started=started, health=dict(pid=222, process='new', commit='b' * 40, restart_started=started))
        with patch.object(updates.watch, 'process_identity', return_value='new'):
            with patch.object(updates.watch.os, 'kill') as kill, patch.object(updates.watch, 'command', return_value='b' * 40) as command:
                updates.watch.watchdog(self.store.db, 'test')
        kill.assert_not_called()
        self.assertTrue(all(call.args[0][0] in ('git', 'ps') for call in command.call_args_list))
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'updated')

    def test_kills_old_pid_after_deadline_then_accepts_health(self):
        started = updates.time.time() - 51
        self.record_watch(restart_started=started)
        def sleep(seconds):
            updates.watch.write(self.store.db, 'test', health=dict(pid=222, process='new', commit='b' * 40, restart_started=started))
        with patch.object(updates.watch.time, 'monotonic', side_effect=[1, 62]):
            with patch.object(updates.watch.time, 'sleep', side_effect=sleep):
                with patch.object(updates.watch, 'process_identity', side_effect=['watchdog', 'old', 'old', 'new']), patch.object(
                        updates.watch, 'command', return_value='b' * 40):
                    with patch.object(updates.watch.os, 'kill') as kill:
                        updates.watch.watchdog(self.store.db, 'test')
        kill.assert_called_once_with(111, updates.watch.signal.SIGKILL)
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'updated')

    def test_rollback_restores_tools_checkout_and_waits_for_old_poll(self):
        self.record_watch(attempted=[tool()])
        def restart(argv, *args, **kwargs):
            if argv[0] == 'git':
                return ('a' if self.store.get(updates.KEY)['phase'] in ('rolling_back', 'rollback_restart') else 'b') * 40
            self.assertEqual(argv[:3], ['launchctl', 'kickstart', '-k'])
            updates.watch.write(self.store.db, 'test', health=dict(pid=333, process='back', commit='a' * 40,
                restart_started=self.store.get(updates.KEY)['restart_started']))
            return ''
        with patch.object(updates.watch.time, 'monotonic', side_effect=[1, 1, 250, 251, 252]):
            with patch.object(updates.watch, 'stop_old'), patch.object(updates.watch, 'install') as install, patch.object(
                    updates.watch, 'version', return_value='1.0.0'):
                with patch.object(updates.watch, 'rollback_code', return_value='Torii restored.'):
                    with patch.object(updates.watch, 'command', side_effect=restart):
                        with patch.object(updates.watch, 'process_identity', return_value='back'), patch.object(
                                updates.watch.host_os, 'SYSTEM', 'darwin'):
                            updates.watch.watchdog(self.store.db, 'test')
        install.assert_called_once_with(tool(), '1.0.0')
        record = self.store.get(updates.KEY)
        self.assertEqual(record['phase'], 'rolled_back')
        self.assertEqual(record['restored'], ['Claude Code restored to 1.0.0.', 'Torii restored.'])

    def test_reused_pid_is_never_killed(self):
        record = self.record_watch()
        with patch.object(updates.watch, 'process_identity', return_value='unrelated'):
            with patch.object(updates.watch.os, 'kill') as kill:
                self.assertFalse(updates.watch.stop_old(record))
        kill.assert_not_called()

    def test_rollback_refuses_a_changed_checkout(self):
        self.record_watch()
        with patch.object(updates.watch.time, 'monotonic', side_effect=[1, 1, 250]):
            with patch.object(updates.watch, 'stop_old'), patch.object(updates.watch, 'rollback_code',
                    side_effect=RuntimeError('Checkout changed. Left untouched.')):
                with patch.object(updates.watch, 'command', return_value='b' * 40) as command:
                    updates.watch.watchdog(self.store.db, 'test')
        self.assertTrue(all(call.args[0][0] in ('git', 'ps') for call in command.call_args_list))
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'recovering')
        self.assertIn('Left untouched', self.store.get(updates.KEY)['result'])

    def test_npm_installs_only_in_discovered_prefix_and_checks_version(self):
        with patch.object(updates.watch, 'command', side_effect=['', 'Claude Code 2.0.0']) as command:
            updates.watch.install(tool(), '2.0.0')
        self.assertEqual(command.call_args_list[0].args[0], ['npm', '--prefix', '/temporary/runtime', 'install',
                         '--save-exact', '--include=optional', '--global=false', '--no-audit', '--no-fund',
                         '@anthropic-ai/claude-code@2.0.0'])
        self.assertEqual(command.call_args_list[1].args[0], ['/temporary/bin/cli', '--version'])

    def test_failed_version_check_refuses_installed_tool(self):
        with patch.object(updates.watch, 'command', side_effect=['', 'Claude Code 1.0.0']):
            with self.assertRaisesRegex(RuntimeError, 'Claude Code version check failed'):
                updates.watch.install(tool(), '2.0.0')

    def test_install_process_is_tracked_and_cleared_after_verification(self):
        self.record('tools')
        process = Mock(pid=444, returncode=0)
        with patch.object(updates.watch.subprocess, 'Popen', return_value=process) as spawn:
            with patch.object(updates.watch, 'process_identity', return_value='npm process'):
                with patch.object(updates.watch, 'version', return_value='2.0.0'):
                    updates.watch.install(tool(), '2.0.0', (str(self.store.directory / 'state.sqlite'), 'test'))
        self.assertTrue(spawn.call_args.kwargs['start_new_session'])
        self.assertIsNone(self.store.get(updates.KEY).get('installation'))
        process.communicate.assert_called_once_with(timeout=300)

    def test_install_timeout_stops_its_group_before_restore(self):
        self.record('tools')
        process = Mock(pid=444, returncode=-9)
        process.poll.return_value = None
        process.communicate.side_effect = [subprocess.TimeoutExpired('npm', 300), ('', '')]
        with patch.object(updates.watch.subprocess, 'Popen', return_value=process):
            with patch.object(updates.watch, 'process_identity', return_value='npm process'):
                with patch.object(updates.watch.os, 'killpg') as kill:
                    with self.assertRaisesRegex(RuntimeError, 'npm install timed out'):
                        updates.watch.install(tool(), '2.0.0', (str(self.store.directory / 'state.sqlite'), 'test'))
        kill.assert_called_once_with(444, updates.watch.signal.SIGKILL)
        self.assertIsNone(self.store.get(updates.KEY).get('installation'))

    def test_failed_installer_tracking_refuses_to_start_process(self):
        process = Mock(pid=444)
        process.poll.return_value = None
        with patch.object(updates.watch.subprocess, 'Popen', return_value=process) as spawn:
            with patch.object(updates.watch.sqlite3, 'connect', side_effect=updates.watch.sqlite3.OperationalError()):
                with patch.object(updates.watch.os, 'killpg') as kill:
                    with self.assertRaisesRegex(RuntimeError, 'npm installation tracking failed'):
                        updates.watch.tracked_install(['npm'], 'unused', 'test')
        kill.assert_not_called()
        spawn.assert_not_called()

    def test_orphan_installer_is_stopped_only_when_process_and_group_match(self):
        record = dict(installation=dict(pid=444, process='npm process'))
        for identity in ('npm process', 'unrelated process'):
            with patch.object(updates.watch, 'process_identity', return_value=identity):
                with patch.object(updates.watch.os, 'getpgid', return_value=444):
                    with patch.object(updates.watch.os, 'killpg') as kill:
                        updates.watch.stop_installation(record)
            self.assertEqual(kill.call_count, 1 if identity == 'npm process' else 0)


class IdentityTests(TemporaryStore):
    def package(self, name):
        prefix = self.root / 'runtime'
        root = prefix / 'node_modules' / cli_identity.PACKAGES[name]
        root.mkdir(parents=True, exist_ok=True)
        (prefix / 'package.json').write_text('{}')
        manifest = root / 'package.json'
        manifest.write_text(json.dumps(dict(name=cli_identity.PACKAGES[name], version='1.0.0')))
        binary = root / 'cli.js'
        binary.write_text('#!/usr/bin/env node\n')
        binary.chmod(0o755)
        bins = prefix / 'node_modules/.bin'
        bins.mkdir(exist_ok=True)
        link = bins / name
        link.symlink_to(binary)
        return link, manifest, prefix

    def test_identity_and_prefix_follow_service_binary_without_subprocess(self):
        for name in ('claude', 'codex'):
            link, manifest, prefix = self.package(name)
            with patch.object(subprocess, 'run') as run:
                before = cli_identity.identity(str(link), cli_identity.PACKAGES[name])
                manifest.write_text(json.dumps(dict(name=cli_identity.PACKAGES[name], version='2.0.0')))
                after = cli_identity.identity(str(link), cli_identity.PACKAGES[name])
                self.assertNotEqual(before, after)
                self.assertEqual(cli_identity.installation(str(link), cli_identity.PACKAGES[name])[2], str(prefix.resolve()))
            run.assert_not_called()

    def test_claude_launch_fingerprint_changes_without_setting(self):
        link, manifest, prefix = self.package('claude')
        before = CoordinatorSession._launch_inputs(self.store, 'model', 'prompt', None, binary=str(link)).fingerprint
        manifest.write_text(json.dumps(dict(name=cli_identity.PACKAGES['claude'], version='2.0.0')))
        after = CoordinatorSession._launch_inputs(self.store, 'model', 'prompt', None, binary=str(link)).fingerprint
        self.assertNotEqual(before, after)
        self.assertIsNone(self.store.get('telegram_updates_enabled'))


class WatchdogSafetyTests(TemporaryStore):
    def test_installer_records_its_identity_before_executing_the_command(self):
        record = self.record('tools')
        with patch.object(updates.subprocess, 'Popen', return_value=SimpleNamespace(pid=444)):
            updates.start_watchdog(record, self.store.directory)
        output = self.root / 'installer.json'
        database = str(self.store.directory / 'state.sqlite')
        query = "SELECT value FROM settings WHERE key='telegram_update_v1'"
        code = ('import json,os,sqlite3; from pathlib import Path; '
                'db=sqlite3.connect(' + repr(database) + '); '
                'record=json.loads(db.execute(' + repr(query) + ').fetchone()[0]); '
                'Path(' + repr(str(output)) + ').write_text(json.dumps(dict(pid=os.getpid(), installation=record.get("installation"))))')
        with patch.object(updates.watch, 'process_identity', return_value=None):
            updates.watch.tracked_install([sys.executable, '-c', code], database, 'test')
        observed = json.loads(output.read_text())
        self.assertEqual(observed['installation']['pid'], observed['pid'])
        self.assertTrue(observed['installation']['process'])
        self.assertIsNone(self.store.get(updates.KEY)['installation'])

    def owned_restart(self, **fields):
        defaults = dict(old_pid=111, old_process='old', tools=[], restart_started=updates.time.time() - 51,
                        restart_from=dict(pid=111, process='old'), attempted=[])
        defaults.update(fields)
        return self.record('restart', **defaults)

    def identity(self, pid):
        return {222: 'new', 333: 'back', os.getpid(): 'watchdog'}.get(pid)

    def test_linux_rollback_uses_systemd_user_unit(self):
        self.owned_restart()
        calls = []
        def command(argv, *args, **kwargs):
            if argv[0] == 'git':
                return ('a' if self.store.get(updates.KEY)['phase'] in ('rolling_back', 'rollback_restart') else 'b') * 40
            calls.append(argv)
            current = self.store.get(updates.KEY)
            updates.watch.write(self.store.db, 'test', health=dict(pid=333, process='back',
                commit='a' * 40, restart_started=current.get('restart_started')))
            return ''
        with patch.object(updates.watch, 'host_os', SimpleNamespace(linux=lambda: True), create=True):
            with patch.object(updates.watch, 'command', side_effect=command), patch.object(
                    updates.watch, 'rollback_code', return_value='Torii restored.'):
                with patch.object(updates.watch, 'process_identity', side_effect=self.identity):
                    with patch.object(updates.watch.time, 'monotonic', side_effect=[1, 1, 250, 251, 252]):
                        with patch.object(updates.watch, 'stop_old'):
                            updates.watch.watchdog(self.store.db, 'test')
        self.assertEqual(calls, [['systemctl', '--user', 'restart', 'local.telegram-agent-coordinator.service']])
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'rolled_back')

    def test_external_deploy_stands_down_before_health_or_rollback(self):
        self.owned_restart(health=dict(pid=222, process='new', commit='b' * 40))
        with patch.object(updates.watch, 'command', return_value='c' * 40):
            with patch.object(updates.watch, 'process_identity', side_effect=self.identity):
                with patch.object(updates.watch, 'restore_tools', wraps=updates.watch.restore_tools) as restore, patch.object(updates.watch, 'stop_old') as stop:
                    updates.watch.watchdog(self.store.db, 'test')
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'failed')
        self.assertIn('stood down', self.store.get(updates.KEY)['result'])
        restore.assert_called_once()
        stop.assert_not_called()

    def test_unowned_restart_cannot_count_as_update_success(self):
        self.owned_restart(restart_started=None, health=dict(pid=222, process='new', commit='b' * 40))
        def command(argv, *args, **kwargs):
            if argv[0] != 'git':
                current = self.store.get(updates.KEY)
                updates.watch.write(self.store.db, 'test', health=dict(pid=333, process='back',
                    commit='a' * 40, restart_started=current['restart_started']))
            return ('a' if self.store.get(updates.KEY)['phase'] in ('rolling_back', 'rollback_restart') else 'b') * 40
        with patch.object(updates.watch, 'command', side_effect=command):
            with patch.object(updates.watch, 'process_identity', side_effect=self.identity):
                with patch.object(updates.watch, 'rollback_code', return_value='Torii restored.'):
                    updates.watch.watchdog(self.store.db, 'test')
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'rolled_back')
        self.assertIn('outside this update', self.store.get(updates.KEY)['result'])

    def test_watchdog_cannot_kill_during_service_shutdown_grace(self):
        self.owned_restart(restart_started=updates.time.time())
        def sleep(seconds):
            updates.watch.write(self.store.db, 'test', phase='updated')
        with patch.object(updates.watch, 'command', return_value='b' * 40):
            with patch.object(updates.watch, 'process_identity', side_effect=lambda pid: 'old' if pid == 111 else self.identity(pid)):
                with patch.object(updates.watch.time, 'monotonic', side_effect=[1, 62]):
                    with patch.object(updates.watch.time, 'sleep', side_effect=sleep):
                        with patch.object(updates.watch.os, 'kill') as kill:
                            updates.watch.watchdog(self.store.db, 'test')
        kill.assert_not_called()

    def test_rollback_poll_from_old_service_is_not_healthy(self):
        self.owned_restart()
        def command(argv, *args, **kwargs):
            if argv[0] != 'git':
                current = self.store.get(updates.KEY)
                updates.watch.write(self.store.db, 'test', health=dict(pid=111, process='old',
                    commit='a' * 40, restart_started=current.get('restart_started')))
            return ('a' if self.store.get(updates.KEY)['phase'] in ('rolling_back', 'rollback_restart') else 'b') * 40
        with patch.object(updates.watch, 'command', side_effect=command), patch.object(
                updates.watch, 'rollback_code', return_value='Torii restored.'):
            with patch.object(updates.watch, 'process_identity', side_effect=self.identity):
                with patch.object(updates.watch.time, 'monotonic', side_effect=[1, 1, 250, 251, 252, 500]):
                    with patch.object(updates.watch.time, 'sleep'), patch.object(updates.watch, 'stop_old'):
                        updates.watch.watchdog(self.store.db, 'test')
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'failed')
        self.assertIn('poll was not healthy', self.store.get(updates.KEY)['result'])

    def test_installer_cannot_start_after_parent_dies_before_registration(self):
        record = self.record('tools')
        with patch.object(updates.subprocess, 'Popen', return_value=SimpleNamespace(pid=444)):
            updates.start_watchdog(record, self.store.directory)
        spawned = self.root / 'spawned'
        installed = self.root / 'installed'
        parent = self.root / 'install-parent.py'
        argv = [sys.executable, '-c', 'from pathlib import Path; import os,time; '
                'Path(' + repr(str(installed)) + ').write_text(str(os.getpid())); time.sleep(15)']
        parent.write_text('import sys\nsys.path.insert(0, ' + repr(str(updates.ROOT)) + ')\n'
            'from coordinator import updates\nfrom pathlib import Path\n'
            'real = updates.watch.subprocess.Popen\n'
            'def spawn(*args, **kwargs):\n    child = real(*args, **kwargs)\n'
            '    Path(' + repr(str(spawned)) + ').write_text(str(child.pid))\n    return child\n'
            'updates.watch.subprocess.Popen = spawn\nupdates.watch.tracked_install(' + repr(argv) + ', ' +
            repr(str(self.store.directory / 'state.sqlite')) + ", 'test')\n")
        self.store.db.execute('BEGIN IMMEDIATE')
        process = subprocess.Popen([sys.executable, str(parent)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        child_pid = None
        child_process = None
        try:
            deadline = updates.time.monotonic() + 3
            while not spawned.exists() and updates.time.monotonic() < deadline:
                updates.time.sleep(0.01)
            self.assertTrue(spawned.exists())
            child_pid = int(spawned.read_text())
            child_process = updates.watch.process_identity(child_pid)
            process.kill()
            process.communicate(timeout=3)
            record = self.store.get(updates.KEY)
            record['phase'] = 'failed'
            self.store.put(updates.KEY, record)
            self.store.db.commit()
            updates.time.sleep(0.2)
            self.assertFalse(installed.exists(), 'installer ran before recording ownership')
        finally:
            self.store.db.rollback()
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=3)
            if child_pid and child_process and updates.watch.process_identity(child_pid) == child_process:
                try:
                    os.kill(child_pid, updates.watch.signal.SIGKILL)
                except ProcessLookupError:
                    pass
