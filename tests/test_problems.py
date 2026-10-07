from datetime import datetime
import json
import logging
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from coordinator import control_api, mcp, problems
from coordinator.store import Store

FAKE_TOKEN = '987654321:AAFakeTokenValueForTheSanitizerTest_x-9'


class ProblemTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name) / 'state')

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def rows(self):
        return [dict(row) for row in self.store.db.execute('SELECT * FROM problems ORDER BY id')]

    def call(self, op, **params):
        with self.store.db:
            return control_api.call(self.store, op, params)

    def test_record_saves_one_row_and_logs_one_line(self):
        with self.assertLogs('coordinator.problems', 'WARNING') as logs:
            row_id = problems.record(self.store, 'worker', 'start_failed', 'FileExistsError at providers.py:201',
                                     topic='-10042:4', task=41, worker=207)
        self.assertEqual(logs.output, ['WARNING:coordinator.problems:problem area=worker code=start_failed '
                                       'topic=-10042:4 task=41 worker=207 '
                                       'detail="FileExistsError at providers.py:201"'])
        row = self.rows()[0]
        self.assertEqual(row['id'], row_id)
        self.assertEqual({key: row[key] for key in ('area', 'code', 'detail', 'topic', 'task', 'worker',
                                                    'message', 'attachment')},
                         {'area': 'worker', 'code': 'start_failed', 'detail': 'FileExistsError at providers.py:201',
                          'topic': '-10042:4', 'task': 41, 'worker': 207, 'message': None, 'attachment': None})
        self.assertAlmostEqual(row['created'], time.time(), delta=5)

    def test_error_level_and_a_row_without_detail(self):
        with self.assertLogs('coordinator.problems', 'ERROR') as logs:
            problems.record(self.store, 'service', 'unclean-exit', level=logging.ERROR)
        self.assertEqual(logs.output, ['ERROR:coordinator.problems:problem area=service code=unclean-exit'])
        self.assertIsNone(self.rows()[0]['detail'])

    def test_record_inside_a_caller_transaction_keeps_that_transaction_open(self):
        self.store.db.execute('BEGIN')
        self.store.put('marker', 1)
        with self.assertLogs('coordinator.problems', 'WARNING'):
            problems.record(self.store, 'outbox', 'send-failed')
        self.assertTrue(self.store.db.in_transaction)
        self.store.db.rollback()
        self.assertEqual(self.rows(), [])
        self.assertIsNone(self.store.get('marker'))

    def test_a_failed_save_is_logged_and_not_raised(self):
        self.store.db.execute('DROP TABLE problems')
        with self.assertLogs('coordinator.problems', 'WARNING') as logs:
            self.assertIsNone(problems.record(self.store, 'worker', 'start_failed'))
        self.assertIn('ERROR:coordinator.problems:problem not saved area=worker code=start_failed '
                      'type=OperationalError', logs.output)

    def test_sanitizer_removes_token_credentials_query_and_email(self):
        credentials = ':'.join(('user', 'hunter2'))
        text = ('HTTPError: https://api.telegram.org/bot' + FAKE_TOKEN + '/getFile?file_id=secret-file '
                'raw ' + FAKE_TOKEN + f' https://{credentials}@example.com/path#frag owner@example.com\n\tend')
        clean = problems.sanitize(text)
        self.assertEqual(clean, 'HTTPError: https://api.telegram.org/bot[token]/getFile?[query] raw [token] '
                                'https://[credentials]@example.com/path?[query] [email] end')
        for secret in (FAKE_TOKEN, 'AAFakeTokenValue', 'secret-file', 'hunter2', 'owner@'):
            self.assertNotIn(secret, clean)
        self.assertEqual(len(problems.sanitize('x' * 1000)), problems.DETAIL_LIMIT)

    def test_record_never_saves_or_logs_the_token(self):
        with self.assertLogs('coordinator.problems', 'WARNING') as logs:
            problems.record(self.store, 'telegram', 'poll-502', 'url=https://api.telegram.org/bot' + FAKE_TOKEN +
                            '/getUpdates', topic='bot' + FAKE_TOKEN)
        saved = json.dumps(self.rows())
        for text in logs.output + [saved]:
            self.assertNotIn(FAKE_TOKEN, text)
            self.assertNotIn('AAFakeTokenValue', text)
        self.assertIn('[token]', saved)

    def test_repeats_inside_the_window_are_counted_into_the_next_row(self):
        now = [1000.0]
        with patch('coordinator.problems.time.time', side_effect=lambda: now[0]), \
                self.assertLogs('coordinator.problems', 'WARNING') as logs:
            for _ in range(4):
                problems.record(self.store, 'telegram', 'poll-502', 'retry_after=None', every=60)
                now[0] += 10
            problems.record(self.store, 'telegram', 'poll-409', every=60)
            now[0] = 1061.0
            problems.record(self.store, 'telegram', 'poll-502', 'retry_after=None', every=60)
        self.assertEqual([(row['code'], row['detail']) for row in self.rows()],
                         [('poll-502', 'retry_after=None'), ('poll-409', None),
                          ('poll-502', 'repeated=3 retry_after=None')])
        self.assertEqual(len(logs.output), 3)

    def test_prune_removes_rows_older_than_thirty_days(self):
        with self.assertLogs('coordinator.problems', 'WARNING'):
            old = problems.record(self.store, 'worker', 'old')
            kept = problems.record(self.store, 'worker', 'recent')
        with self.store.db:
            self.store.db.execute('UPDATE problems SET created=? WHERE id=?', (time.time() - 31 * 86400, old))
            self.store.db.execute('UPDATE problems SET created=? WHERE id=?', (time.time() - 29 * 86400, kept))
        self.assertEqual(problems.prune(self.store), 1)
        self.assertEqual([row['code'] for row in self.rows()], ['recent'])

    def seed(self):
        with self.assertLogs('coordinator.problems', 'WARNING'):
            for created, area, code in ((100.0, 'worker', 'start_failed'), (200.0, 'telegram', 'poll-502'),
                                        (300.0, 'worker', 'start_failed'), (400.0, 'worker', 'auth_failed')):
                row = problems.record(self.store, area, code, 'detail ' + str(int(created)), worker=7)
                with self.store.db:
                    self.store.db.execute('UPDATE problems SET created=? WHERE id=?', (created, row))

    def test_list_is_newest_first_and_filters(self):
        self.seed()
        result = self.call('problems.list')
        self.assertTrue(result.ok)
        self.assertEqual([row['created'] for row in result.data['problems']],
                         [problems.stamp(value) for value in (400.0, 300.0, 200.0, 100.0)])
        self.assertEqual(result.text.splitlines()[0],
                         problems.stamp(400.0) + ' worker auth_failed worker=7: detail 400')
        since = datetime.fromtimestamp(250.0).isoformat()
        self.assertEqual([row['code'] for row in self.call('problems.list', since=since, area='worker').data['problems']],
                         ['auth_failed', 'start_failed'])
        self.assertEqual(len(self.call('problems.list', code='start_failed', limit='1').data['problems']), 1)
        self.assertEqual(self.call('problems.list', limit='0').state, 'refused')
        self.assertEqual(self.call('problems.list', since='yesterday').state, 'refused')
        self.assertEqual(self.call('problems.list', since='1970-01-01T00:00:00Z').state, 'done')

    def test_summary_counts_by_area_and_code_in_the_window(self):
        self.seed()
        result = self.call('problems.summary', since=datetime.fromtimestamp(150.0).isoformat())
        self.assertEqual([(row['area'], row['code'], row['count']) for row in result.data['counts']],
                         [('telegram', 'poll-502', 1), ('worker', 'auth_failed', 1), ('worker', 'start_failed', 1)])
        result = self.call('problems.summary', since=datetime.fromtimestamp(0.0).isoformat())
        self.assertEqual(result.data['counts'][0]['count'], 2)
        self.assertIn('worker start_failed: 2 (last ' + problems.stamp(300.0) + ')', result.text)
        self.assertTrue(self.call('problems.summary').text.startswith('No problems since '))

    def test_both_views_are_read_only_mcp_tools(self):
        for op_id in ('problems.list', 'problems.summary'):
            self.assertEqual(control_api.find(op_id).kind, control_api.READ)
            self.assertIn(op_id.replace('.', '_'), mcp.TOOLS)
        self.seed()
        response = mcp.dispatch(self.store, {'method': 'tools/call', 'params': {
            'name': 'problems_list', 'arguments': {'area': 'telegram'}}})
        self.assertFalse(response['isError'])
        self.assertIn('telegram poll-502 worker=7: detail 200', response['content'][0]['text'])


if __name__ == '__main__':
    unittest.main()
