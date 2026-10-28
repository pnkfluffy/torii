import tempfile
import time
import unittest
from pathlib import Path

from coordinator.store import Store
from coordinator.usage_notice import queue_limit_notice


class UsageNoticeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name) / 'state')
        with self.store.db:
            for topic, thread in (('home', 4), ('project', 5)):
                self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                    VALUES (?,-10042,?,?,?,1)''', (topic, thread, topic, self.temp.name))
            self.store.put('coordinator_home_topic', 'home')
        self.reset = time.time() + 3600

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def queue(self, windows=None, provider='Codex', account='first', **options):
        return queue_limit_notice(self.store, provider, account,
                                  {'weekly': self.reset} if windows is None else windows,
                                  'Work is blocked.', **options)

    def notices(self):
        return [tuple(row) for row in self.store.db.execute('SELECT topic,text FROM outbox ORDER BY id')]

    def test_notices_go_to_home_and_dedupe_by_account_provider_and_window(self):
        self.assertTrue(self.queue())
        self.assertFalse(self.queue())
        self.assertTrue(self.queue(provider='Claude'))
        self.assertTrue(self.queue(account='second'))
        self.assertTrue(self.queue({'5-hour': self.reset}))
        self.assertEqual(self.notices(), [('home', 'Work is blocked.')] * 4)

    def test_worker_blocked_before_account_selection_reuses_the_existing_provider_notice(self):
        self.assertTrue(self.queue())
        self.assertFalse(self.queue(account=None))
        self.assertEqual(len(self.notices()), 1)

    def test_missing_or_disabled_home_never_falls_back_to_another_chat_or_consumes_notice(self):
        for home in (None, 'missing', 'home'):
            with self.subTest(home=home):
                self.store.put('coordinator_home_topic', home)
                self.store.db.execute("UPDATE topics SET enabled=0 WHERE id='home'")
                self.assertFalse(self.queue())
                self.assertEqual(self.notices(), [])
        self.store.put('coordinator_home_topic', 'home')
        self.store.db.execute("UPDATE topics SET enabled=1 WHERE id='home'")
        self.assertTrue(self.queue())
        self.assertEqual(self.notices(), [('home', 'Work is blocked.')])

    def test_old_low_capacity_warning_does_not_suppress_a_hard_stop(self):
        self.store.put('low_capacity_warnings', {'Codex': [['first', 'weekly', self.reset]]})
        self.assertTrue(self.queue())

    def test_reset_jitter_dedupes_and_recovery_rearms_each_window(self):
        self.assertTrue(self.queue(reset_tolerance=1800, clear_recovered=True))
        self.assertFalse(self.queue({'weekly': self.reset + 60}, reset_tolerance=1800, clear_recovered=True))
        self.assertTrue(self.queue({'weekly': self.reset + 18000}, reset_tolerance=1800, clear_recovered=True))
        self.assertFalse(self.queue({}, clear_recovered=True))
        self.assertTrue(self.queue(clear_recovered=True))
        self.assertEqual(len(self.notices()), 3)

    def test_rejection_without_a_reset_warns_once_and_expired_windows_rearm(self):
        self.assertTrue(self.queue({'limit': None}))
        self.assertFalse(self.queue({'limit': None}))
        self.store.put('account_limit_notices', {'Codex': [['first', 'weekly', time.time() - 1]]})
        self.assertTrue(self.queue())
