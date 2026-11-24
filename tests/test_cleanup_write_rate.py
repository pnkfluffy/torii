import sqlite3
import threading
import time
import unittest

from tests import test_idle_trim

RATE = 0.25
SWEEPS = 3


class Writer:
    """Commit one unrelated settings row from another connection every RATE seconds."""
    def __init__(self, path):
        self.path, self.stop, self.commits = path, threading.Event(), 0

    def run(self):
        db = sqlite3.connect(str(self.path), timeout=30)
        try:
            while not self.stop.wait(RATE):
                with db:
                    db.execute("INSERT OR REPLACE INTO settings(key,value) VALUES ('unrelated_tick', ?)",
                               (str(time.time()),))
                self.commits += 1
        finally:
            db.close()

    def __enter__(self):
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.stop.set()
        self.thread.join()


class LiveRateIdleTrim(test_idle_trim.IdleTrimTests):
    def test_idle_trim_under_live_commit_rate(self):
        task = self.job(closed=False)
        self.age(task)
        scratch = self.state / 'scratch' / ('task-' + str(task['id']))
        for i in range(10):
            (scratch / '.cache' / ('d%d' % i)).mkdir(parents=True)
            for j in range(100):
                (scratch / '.cache' / ('d%d' % i) / ('f%d' % j)).write_text('x')
        self.call('cleanup.idle_trim', enabled=True)
        with Writer(self.state / 'state.sqlite') as writer:
            for _ in range(SWEEPS):
                with self.store.db:
                    self.store.db.execute('DELETE FROM idle_trims WHERE task=?', (task['id'],))
                self.sweep()
                if not (scratch / '.cache').exists():
                    break
        print('\nidle_trim: unrelated commits during sweeps=%d cache_exists=%s' % (writer.commits, (scratch / '.cache').exists()))
        self.assertFalse((scratch / '.cache').exists(), 'idle trim never removed .cache in %d sweeps under a live-like commit rate' % SWEEPS)



from tests import test_close_saves


class LiveRateCloseSave(test_close_saves.CloseSavesTests):
    def test_close_save_stays_strict_under_live_commit_rate(self):
        task = self.job()
        with Writer(self.state / 'state.sqlite') as writer:
            current = self.close(task)
        manifest = self.manifest(task) if (self.folder(task) / 'saved.json').exists() else {}
        print('\nclose save: commits=%d copy=%s reason=%r loose=%r' % (
            writer.commits, current['copy'], current.get('copy_reason'), manifest.get('loose')))
        self.assertNotIn('loose', manifest, 'strict close save degraded to a loose save')
        self.assertEqual(current['copy'], 'cleared')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    suite.addTest(LiveRateIdleTrim('test_idle_trim_under_live_commit_rate'))
    suite.addTest(LiveRateCloseSave('test_close_save_stays_strict_under_live_commit_rate'))
    return suite
