from pathlib import Path
import tempfile
import unittest

from coordinator.envelopes import HANDOFF, ask, sweep
from coordinator.store import Store
from coordinator.vault import FakeVault
from tests.test_envelopes import private
from tests.test_group_setup import HOME, pair_group


class ExpiredIntakeTests(unittest.TestCase):
    def test_group_expiry_keeps_rejection_and_never_queues_dm_work_after_restart(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            for swept in (False, True):
                for restart in (False, True):
                    for name in ('API_KEY', HANDOFF):
                        with self.subTest(swept=swept, restart=restart, name=name):
                            directory = root / ('%s-%s-%s' % (swept, restart, name))
                            store = Store(directory)
                            try:
                                pair_group(store)
                                store.vault = FakeVault()
                                with store.db:
                                    envelope = ask(store, HOME, name, 'reason', 'worker')
                                    store.db.execute("UPDATE envelopes SET state='armed',chat=7,expires=0 WHERE id=?", (envelope,))
                                    if swept:
                                        sweep(store)
                                if restart:
                                    store.close()
                                    store = Store(directory)
                                    store.vault = FakeVault()
                                code = 'FAKE-LATE-SIGNIN-CODE-0123456789'
                                for number, text in enumerate((code, 'hi'), 1):
                                    self.assertEqual(store.accept(private(number, text)), 'envelope_reject')
                                reasons = store.db.execute("SELECT reason FROM envelope_events WHERE event='reject'").fetchall()
                                self.assertEqual([row[0] for row in reasons], ['no_armed', 'no_armed'])
                                self.assertEqual(store.db.execute("SELECT count(*) FROM envelope_effects WHERE kind='delete'").fetchone()[0], 2)
                                self.assertEqual(store.db.execute('SELECT count(*) FROM messages').fetchone()[0], 0)
                                for table in ('messages', 'settings', 'outbox', 'envelopes', 'envelope_effects'):
                                    self.assertNotIn(code, str([tuple(row) for row in store.db.execute('SELECT * FROM ' + table)]))
                            finally:
                                store.close()
