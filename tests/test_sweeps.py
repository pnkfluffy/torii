import asyncio
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from coordinator import problems, sweeps
from coordinator.store import Store


class SweepTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name) / 'state')
        self.calls = []

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def rows(self):
        return [dict(row) for row in self.store.db.execute('SELECT * FROM problems ORDER BY id')]

    def state(self, item):
        row = self.store.db.execute("SELECT * FROM sweep_failures WHERE sweep='trim' AND item=?", (str(item),)).fetchone()
        return dict(row) if row else None

    def action(self, item):
        self.calls.append(item)
        if item == 2:
            raise ValueError('job 2 has a bad folder /Users/someone/private/state.json')

    def run_sweep(self, items, **options):
        with self.assertLogs('coordinator.problems', 'WARNING'):
            return sweeps.run(self.store, 'trim', items, self.action, refs=lambda item: {'task': item}, **options)

    def test_one_failed_item_does_not_stop_the_others(self):
        self.assertEqual(self.run_sweep([1, 2, 3]), [1, 2, 3])
        self.assertEqual(self.calls, [1, 2, 3])
        self.assertIsNone(self.state(1))
        self.assertEqual(self.state(2)['failures'], 1)

    def test_problem_row_names_item_type_message_and_frames(self):
        self.run_sweep([2])
        [row] = self.rows()
        self.assertEqual((row['area'], row['code'], row['task']), ('cleanup', 'trim-failed', 2))
        self.assertTrue(row['detail'].startswith('item=2 failures=1 ValueError: job 2 has a bad folder '
                                                 '/Users/someone/private/state.json at test_sweeps.py:'), row['detail'])
        self.assertIn(' action < sweeps.py:', row['detail'])
        self.assertEqual(self.state(2)['detail'], row['detail'].split(' ', 2)[2])

    def test_detail_keeps_a_long_path_and_survives_bad_text_and_a_failed_report(self):
        long_path = '/state/' + 'x' * 250 + '/copy'

        def bad(item):
            raise OSError('cannot delete %s \udcff' % long_path)
        with self.assertLogs('coordinator.problems', 'WARNING'):
            sweeps.run(self.store, 'trim', [1], bad)
        detail = self.rows()[0]['detail']
        self.assertIn(long_path, detail)
        self.assertIn('\\udcff', detail)
        self.assertIn(' at test_sweeps.py:', detail)
        with patch('coordinator.problems.record', side_effect=RuntimeError('report failed')), \
                self.assertLogs('coordinator.sweeps', 'WARNING'):
            self.assertEqual(sweeps.run(self.store, 'trim', [2, 3], self.action), [2, 3])
        self.assertEqual(self.calls, [2, 3])
        self.assertEqual(self.state(2)['failures'], 1)

    def test_failed_item_backs_off_with_a_bound(self):
        self.run_sweep([2])
        self.calls.clear()
        self.assertEqual(sweeps.run(self.store, 'trim', [2, 3], self.action), [3])
        self.assertEqual(self.calls, [3])
        self.assertAlmostEqual(sweeps.next_retry(self.store, 'trim'), sweeps.FIRST, delta=5)
        self.assertIsNone(sweeps.next_retry(self.store, 'other'))
        start = time.time()
        for count in range(2, 30):
            with patch('coordinator.sweeps.time.time', return_value=start + count * 2 * sweeps.LIMIT):
                self.run_sweep([2])
            state = self.state(2)
            self.assertEqual(state['failures'], count)
            self.assertEqual(state['retry'] - state['failed'], min(sweeps.FIRST * 2 ** (count - 1), sweeps.LIMIT))
        self.assertEqual(len(self.rows()), 29)

    def test_success_clears_failure_state(self):
        self.run_sweep([2])
        with patch('coordinator.sweeps.time.time', return_value=time.time() + sweeps.LIMIT):
            self.assertEqual(sweeps.run(self.store, 'trim', [2], lambda item: None), [2])
        self.assertIsNone(self.state(2))
        self.assertIsNone(sweeps.next_retry(self.store, 'trim'))

    def test_limit_counts_only_due_items(self):
        self.run_sweep([2])
        self.assertEqual(sweeps.run(self.store, 'trim', [1, 2, 3, 4], self.action, limit=2), [1, 3])

    def test_deadline_starts_no_further_items(self):
        def slow(item):
            self.calls.append(item)
            clock[0] += 10
        clock = [0]
        with patch('coordinator.sweeps.time.monotonic', side_effect=lambda: clock[0]):
            self.assertEqual(sweeps.run(self.store, 'trim', [1, 2, 3], slow, deadline=15), [1, 2])
        self.assertEqual(self.calls, [1, 2])

    def test_async_actions_and_cancellation(self):
        async def action(item):
            await asyncio.sleep(0)
            if item == 'b':
                raise OSError('disk')
            if item == 'c':
                raise asyncio.CancelledError()
            self.calls.append(item)
        with self.assertLogs('coordinator.problems', 'WARNING'):
            self.assertEqual(asyncio.run(sweeps.arun(self.store, 'trim', ['a', 'b'], action, area='records')),
                             ['a', 'b'])
        self.assertEqual(self.rows()[0]['code'], 'trim-failed')
        self.assertEqual(self.rows()[0]['area'], 'records')
        with self.assertRaises(asyncio.CancelledError):
            asyncio.run(sweeps.arun(self.store, 'trim', ['c'], action))
        self.assertIsNone(self.state('c'))

    def test_prune_forgets_old_failure_state(self):
        self.run_sweep([2])
        self.store.db.execute('UPDATE sweep_failures SET failed=0')
        self.store.db.commit()
        with self.assertLogs('coordinator.problems', 'INFO'):
            problems.prune(self.store)
        self.assertIsNone(self.state(2))


if __name__ == '__main__':
    unittest.main()
