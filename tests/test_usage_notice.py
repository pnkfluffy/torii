import tempfile
import time
import unittest
from pathlib import Path

from coordinator.store import Store
from coordinator.usage_notice import clear_low_capacity_warning, queue_low_capacity_notice, warn_low_capacity


class UsageNoticeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name) / 'state')
        with self.store.db:
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES (?,?,?,?,?,1)''', ('-10042:4', -10042, 4, 'Home', self.temp.name))
        self.reset = time.time() + 600

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def warn(self, provider, reset=None, capacity=95):
        return warn_low_capacity(self.store, provider, 'user@example.com', '5-hour', 83,
                                 self.reset if reset is None else reset, self.reset + 300,
                                 '-10042:4', capacity_pct=capacity)

    def notices(self):
        return [row[0] for row in self.store.db.execute("SELECT text FROM outbox WHERE text LIKE 'Heads-up:%'")]

    def test_provider_notice_dedupes_per_reset_and_keeps_other_provider_state(self):
        self.warn('Claude')
        self.warn('Claude')
        self.warn('Codex')
        self.assertEqual(len(self.notices()), 2)
        self.assertIn('Claude', self.notices()[0])
        self.assertIn('Codex', self.notices()[1])
        clear_low_capacity_warning(self.store, 'Claude')
        self.warn('Codex')
        self.assertEqual(len(self.notices()), 2)
        self.warn('Claude')
        self.warn('Claude', self.reset + 5 * 60 * 60)
        self.assertEqual(len(self.notices()), 4)

    def test_notice_uses_the_provider_capacity_limit(self):
        self.assertFalse(warn_low_capacity(self.store, 'Claude', 'orbit@example.com', 'Fable weekly',
                                           59, self.reset, None, '-10042:4', capacity_pct=80))
        self.assertTrue(warn_low_capacity(self.store, 'Claude', 'orbit@example.com', 'Fable weekly',
                                          60, self.reset, None, '-10042:4', capacity_pct=80))
        self.assertIn('60% of its Fable weekly limit', self.notices()[0])

    def test_missing_reset_still_warns_once(self):
        self.assertTrue(warn_low_capacity(self.store, 'Codex', 'user@example.com', 'weekly',
                                          81, None, None, '-10042:4'))
        self.assertFalse(warn_low_capacity(self.store, 'Codex', 'user@example.com', 'weekly',
                                           82, None, None, '-10042:4'))
        self.assertIn('resets at unknown', self.notices()[0])

    def test_claude_notice_can_fire_again_after_usage_recovers(self):
        self.assertTrue(self.warn('Claude'))
        self.assertFalse(warn_low_capacity(self.store, 'Claude', 'user@example.com', '5-hour', 60,
                                           self.reset, None, '-10042:4'))
        self.assertTrue(self.warn('Claude'))
        self.assertEqual(len(self.notices()), 2)

    def test_claude_notice_dedupes_reset_jitter_and_rearms_after_recovery(self):
        reset = time.time() + 5 * 60 * 60
        for percent, jitter in ((60, 0), (61, 20), (62, 40), (79, 60)):
            warn_low_capacity(self.store, 'Claude', 'user@example.com', '5-hour', percent,
                              reset + jitter, None, '-10042:4', capacity_pct=80)
        self.assertEqual(len(self.notices()), 1)
        next_reset = reset + 5 * 60 * 60
        self.assertTrue(warn_low_capacity(self.store, 'Claude', 'user@example.com', '5-hour',
                                          60, next_reset, None, '-10042:4', capacity_pct=80))
        self.assertEqual(len(self.notices()), 2)
        self.assertFalse(warn_low_capacity(self.store, 'Claude', 'user@example.com', '5-hour',
                                           59, next_reset, None, '-10042:4', capacity_pct=80))
        self.assertTrue(warn_low_capacity(self.store, 'Claude', 'user@example.com', '5-hour',
                                          60, next_reset, None, '-10042:4', capacity_pct=80))
        self.assertEqual(len(self.notices()), 3)

    def test_shared_notice_tracks_each_window_and_recovery_for_one_account(self):
        topic = '-10042:4'
        windows = {'weekly': self.reset}
        self.assertTrue(queue_low_capacity_notice(self.store, 'Codex', 'user@example.com', windows,
                                                   topic, 'first warning', clear_recovered=True))
        self.assertFalse(queue_low_capacity_notice(self.store, 'Codex', 'user@example.com', windows,
                                                    topic, 'duplicate warning', clear_recovered=True))
        self.assertTrue(queue_low_capacity_notice(self.store, 'Codex', 'user@example.com',
                                                   {'weekly': self.reset, '5-hour': self.reset + 300},
                                                   topic, 'second window warning', clear_recovered=True))
        self.assertFalse(queue_low_capacity_notice(self.store, 'Codex', 'user@example.com', {},
                                                    topic, 'recovery', clear_recovered=True))
        self.assertTrue(queue_low_capacity_notice(self.store, 'Codex', 'user@example.com', windows,
                                                   topic, 'new warning', clear_recovered=True))
        self.assertEqual([row[0] for row in self.store.db.execute('SELECT text FROM outbox ORDER BY id')],
                         ['first warning', 'second window warning', 'new warning'])
