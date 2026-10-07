import asyncio
from contextlib import redirect_stdout
import io
import signal
import tempfile
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from coordinator import control_api
from coordinator.accounts import discover_accounts
from coordinator.envelopes import ask, sweep
from coordinator.setup_flow import check_setup_accounts
from coordinator.signin import KEY, SignIns
from coordinator.store import Store
from coordinator.vault import FakeVault
from tests.test_group_setup import group_update, pair_group, GroupTelegram
from tests.test_setup_installer import INSTALLER, login_launcher


class SetupFollowupTestsSupport:
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.home = patch('pathlib.Path.home', return_value=self.root)
        self.home.start()
        self.store = Store(self.root / 'state')
        self.store.vault = FakeVault()
        pair_group(self.store)
        self.driver = SignIns(self.store, poll=0.001)

    async def asyncTearDown(self):
        self.store.close()
        self.home.stop()
        self.temp.cleanup()

    def signin(self):
        with self.store.db:
            self.store.put(KEY, {'topic': '-10042:10', 'state': 'starting', 'created': False,
                                 'config_dir': str(self.root / 'unused')})
        self.driver.post_card('https://claude.ai/oauth/authorize?fake=true', 1)
        return self.store.get(KEY)['envelope']


class SetupFollowupTests(SetupFollowupTestsSupport, unittest.IsolatedAsyncioTestCase):
    def test_pairing_pointer_follows_persistence_and_is_limited_per_topic(self):
        from tests.test_group_setup import CHAT, HOME
        with self.store.db:
            self.store.db.execute('UPDATE topics SET enabled=1,cwd=?', (str(self.root),))
            self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                                  ('-10042:20', CHAT, 20, 'Project', str(self.root)))
        enqueue = self.store.enqueue_report
        observed = []

        def after_saved(topic, text, **kwargs):
            if text.startswith('Saved.'):
                observed.append(self.store.db.execute('SELECT count(*) FROM messages').fetchone()[0])
            return enqueue(topic, text, **kwargs)

        with patch.object(self.store, 'enqueue_report', side_effect=after_saved), \
                patch('coordinator.store.time.time', return_value=10000):
            for message, thread in ((1, 20), (2, 20), (3, 10)):
                self.assertEqual(self.store.accept(group_update(message, 'Help me', thread=thread)), 'queued')
        self.assertEqual(observed, [1, 3])
        pointers = list(self.store.db.execute("SELECT topic,text,reply_to FROM outbox WHERE text LIKE 'Saved.%'"))
        self.assertEqual(len(pointers), 2)
        self.assertIn('setup card in the Torii topic', pointers[0]['text'])
        self.assertEqual(pointers[0]['reply_to'], 1)
        self.assertIn('buttons on the setup card below', pointers[1]['text'])
        self.assertEqual(pointers[1]['topic'], HOME)
        self.assertIsNotNone(self.store.get('setup_card'))
        with patch('coordinator.store.time.time', return_value=13600):
            self.store.accept(group_update(4, 'Try again', thread=20))
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM outbox WHERE text LIKE 'Saved.%'").fetchone()[0], 3)

    async def test_signin_error_markers_reject_in_group_mode(self):
        self.signin()
        self.driver.update(state='checking')
        process = SimpleNamespace(returncode=None)
        for marker in ('Invalid code', 'ERROR', 'Expired', 'Try again', 'Press Enter to retry'):
            with self.subTest(marker=marker):
                self.assertFalse(await asyncio.wait_for(self.driver.wait_result(process, [marker]), 0.1))

    def test_discovery_resolves_registered_paths_before_comparing(self):
        root = self.root / 'profiles'
        directory = root / 'work'
        directory.mkdir(parents=True)
        (directory / '.claude.json').touch()
        link = self.root / 'linked-home'
        link.symlink_to(root, target_is_directory=True)
        profiles = {'owner': {'config_dir': str(link / 'work'), 'enabled': False}}
        with self.store.db:
            self.store.put('accounts', profiles)
        self.store.close()
        self.store = Store(self.root / 'state')
        self.assertEqual(discover_accounts(self.store, root), profiles)

    def test_setup_does_not_post_moved_below_for_undelivered_card(self):
        envelope = self.signin()
        old = self.store.db.execute('SELECT card_outbox FROM envelopes WHERE id=?', (envelope,)).fetchone()[0]
        self.assertEqual(self.store.accept(group_update(1, '/setup')), 'control')
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM outbox WHERE text='Moved below.'").fetchone()[0], 0)
        row = self.store.db.execute('SELECT * FROM outbox WHERE id=?', (old,)).fetchone()
        self.assertIsNone(row['reply_markup'])
        self.assertEqual(row['delivered'], 1)
        newest = self.store.db.execute('SELECT * FROM outbox ORDER BY id DESC LIMIT 1').fetchone()
        self.assertIn('https://claude.ai/oauth/authorize?fake=true', newest['text'])

    async def test_repeated_setup_during_check_does_not_schedule_another_check(self):
        with self.store.db:
            self.store.put('accounts', {'owner': {'config_dir': str(self.root), 'enabled': True}})
        self.store.accept(group_update(1, '/setup'))
        requested = self.store.get('setup_requests')['check_accounts']

        async def checking(store):
            self.store.accept(group_update(2, '/setup'))
            self.store.accept(group_update(3, '/setup'))

        with patch('coordinator.account_status.check_accounts', checking), \
                patch('coordinator.codex_accounts.refresh_codex_accounts', AsyncMock()):
            await check_setup_accounts(self.store, requested)
        self.assertNotIn('check_accounts', self.store.get('setup_requests'))
        self.assertIsNotNone(self.store.get('setup_accounts_checked'))

    async def test_failed_installer_login_is_not_adopted_on_restart(self):
        def login(argv, **kwargs):
            (Path(kwargs['cwd']) / '.claude.json').touch()
            return SimpleNamespace(returncode=1)

        with patch('asyncio.create_subprocess_exec', login_launcher(login)), redirect_stdout(io.StringIO()):
            self.assertFalse(await INSTALLER['connect_claude'](self.store))
        self.store.close()
        self.store = Store(self.root / 'state')
        self.assertEqual(discover_accounts(self.store), {})
        self.assertIsNone(self.store.get('setup_claude_dir'))
        self.assertEqual(len(list((self.root / '.claude-accounts').iterdir())), 1)

    async def test_ctrl_c_during_login_prints_skip_once(self):
        def login(argv, **kwargs):
            signal.raise_signal(signal.SIGINT)
            return SimpleNamespace(returncode=1)

        with patch('asyncio.create_subprocess_exec', login_launcher(login)), redirect_stdout(io.StringIO()) as output:
            self.assertFalse(await INSTALLER['connect_claude'](self.store))
        self.assertEqual(output.getvalue().count('Skipped.'), 1)
