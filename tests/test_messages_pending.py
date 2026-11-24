from pathlib import Path
import tempfile
import unittest

from coordinator.store import Store
from coordinator.service import Service


class MessagesPendingTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.store = Store(self.root)
        self.addCleanup(lambda: self.store.close())
        with self.store.db:
            self.store.db.execute("""INSERT INTO topics(id,chat,thread,name,cwd)
                VALUES ('test',1,1,'Test','')""")

    def pending_plan(self):
        statements = []
        self.store.db.set_trace_callback(statements.append)
        try:
            pending = self.store.messages_pending()
        finally:
            self.store.db.set_trace_callback(None)
        self.assertEqual(len(statements), 1)
        plan = [row['detail'] for row in self.store.db.execute('EXPLAIN QUERY PLAN ' + statements[0])]
        self.assertTrue(any('USING INDEX messages_pending' in detail for detail in plan), plan)
        self.assertFalse(any('TEMP B-TREE' in detail for detail in plan), plan)
        return pending

    def test_pending_lookup_uses_partial_index_and_keeps_id_order(self):
        with self.store.db:
            for message_id, state in ((9, 'pending'), (4, 'sent'), (2, 'pending'),
                                      (6, 'received'), (7, 'uncertain')):
                self.store.db.execute("""INSERT INTO messages(id,topic,kind,text,delivered,created,updated)
                    VALUES (?,'test','owner','Fixture',?,1,1)""", (message_id, state))
        self.assertEqual([row['id'] for row in self.pending_plan()], [2, 9])
        self.store.message_sending(2)
        self.store.message_delivered(9, 'written')
        self.assertEqual(self.pending_plan(), [])
        self.store.requeue_messages([2, 4, 6, 7, 9])
        self.assertEqual([row['id'] for row in self.pending_plan()], [2, 4, 6, 7, 9])

    def test_topic_pending_lookup_uses_partial_index(self):
        plan = [row['detail'] for row in self.store.db.execute(
            "EXPLAIN QUERY PLAN SELECT 1 FROM messages WHERE topic=? AND delivered='pending'", ('test',))]
        self.assertTrue(any('USING INDEX messages_pending' in detail for detail in plan), plan)

    def test_chat_settled_uses_partial_index_and_keeps_callback_exclusion(self):
        service = Service.__new__(Service)
        service.store = self.store
        service.sessions = {}
        service.feed_tasks = {}
        pending = self.store.message_save('test', 'callback', 'Fixture')
        statements = []
        self.store.db.set_trace_callback(statements.append)
        try:
            self.assertFalse(service.chat_settled('test'))
        finally:
            self.store.db.set_trace_callback(None)
        query, = [statement for statement in statements if statement.startswith('SELECT id FROM messages')]
        plan = [row['detail'] for row in self.store.db.execute('EXPLAIN QUERY PLAN ' + query)]
        self.assertTrue(any('USING INDEX messages_pending' in detail for detail in plan), plan)
        self.assertTrue(service.chat_settled('test', (pending['id'],)))
        self.store.message_sending(pending['id'])
        self.assertTrue(service.chat_settled('test'))

    def test_existing_database_gets_index_and_preserves_messages_on_reopen(self):
        pending = self.store.message_save('test', 'owner', 'Pending fixture')
        received = self.store.message_save('test', 'owner', 'Received fixture')
        self.store.message_delivered(received['id'], 'written')
        before = [tuple(row) for row in self.store.db.execute('SELECT * FROM messages ORDER BY id')]
        with self.store.db:
            self.store.db.execute('DROP INDEX messages_pending')
        self.store.close()
        self.store = Store(self.root)
        self.assertEqual([tuple(row) for row in self.store.db.execute('SELECT * FROM messages ORDER BY id')], before)
        self.assertEqual(self.pending_plan(), [pending])
        version = self.store.db.execute('PRAGMA schema_version').fetchone()[0]
        self.store.close()
        self.store = Store(self.root)
        self.assertEqual(self.store.db.execute('PRAGMA schema_version').fetchone()[0], version)
        self.assertEqual(self.pending_plan(), [pending])
