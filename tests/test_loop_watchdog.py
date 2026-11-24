import asyncio
from pathlib import Path
import sqlite3
import tempfile
import time
from types import SimpleNamespace
import unittest

from coordinator import loop_watchdog
from coordinator.loop_watchdog import LoopWatchdog
from tests.support import SETTLE


class WatchdogLoggingTests(unittest.TestCase):
    def test_locked_database_logs_once_for_repeated_attempts(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / 'logs').mkdir()
            with sqlite3.connect(str(directory / 'state.sqlite')) as db:
                db.execute('CREATE TABLE problems (created REAL, area TEXT, code TEXT, detail TEXT)')
                db.execute('BEGIN IMMEDIATE')
                loop_watchdog._last_failure_log = float('-inf')
                self.assertFalse(loop_watchdog.record_stall(directory, 60))
                self.assertFalse(loop_watchdog.record_stall(directory, 60))
                db.rollback()
            self.assertEqual((directory / 'logs' / 'watchdog.log').read_text().count('watchdog problem could not be saved'), 1)


class LoopWatchdogTests(unittest.IsolatedAsyncioTestCase):
    async def test_stall_dumps_stacks_once_and_records_problem(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / 'logs').mkdir()
            log = directory / 'logs' / 'service.log'
            db = sqlite3.connect(str(directory / 'state.sqlite'))
            db.execute('''CREATE TABLE problems (id INTEGER PRIMARY KEY, created REAL, area TEXT, code TEXT,
                       detail TEXT, topic TEXT, task INTEGER, worker INTEGER, message INTEGER, attachment INTEGER)''')
            db.commit()
            watchdog = LoopWatchdog(asyncio.get_running_loop(), SimpleNamespace(directory=directory, db=db), log,
                                    timeout=0.3, interval=0.05)
            watchdog.start()
            try:
                deadline = time.monotonic() + SETTLE
                while True:
                    rows = db.execute('SELECT area, code FROM problems').fetchall()
                    if rows and self._testMethodName in log.read_text():
                        break
                    if time.monotonic() >= deadline:
                        self.fail('Timed out waiting for the stack dump and the stall record.')
                    time.sleep(0.01)
            finally:
                watchdog.stop()
                db.close()
            text = log.read_text()
            self.assertEqual(text.count('Timeout ('), 1)
            self.assertIn('test_stall_dumps_stacks_once_and_records_problem', text)
            self.assertEqual(rows, [('service', 'event-loop-stalled')])

    async def test_rotation_refreshes_dump_destination(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / 'logs').mkdir()
            log = directory / 'logs' / 'service.log'
            rotated = directory / 'logs' / 'service.log.1'
            watchdog = LoopWatchdog(asyncio.get_running_loop(), SimpleNamespace(directory=directory), log,
                                    timeout=0.3, interval=0.05)
            watchdog.start()
            try:
                log.rename(rotated)
                log.touch()
                await asyncio.sleep(0.1)
                time.sleep(0.6)
                await asyncio.sleep(0.1)
            finally:
                watchdog.stop()
            self.assertIn('test_rotation_refreshes_dump_destination', log.read_text())
            self.assertNotIn('Timeout (', rotated.read_text())

    async def test_locked_database_does_not_block_monitor_and_problem_is_saved(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / 'logs').mkdir()
            db = sqlite3.connect(str(directory / 'state.sqlite'))
            db.execute('''CREATE TABLE problems (id INTEGER PRIMARY KEY, created REAL, area TEXT, code TEXT,
                       detail TEXT, topic TEXT, task INTEGER, worker INTEGER, message INTEGER, attachment INTEGER)''')
            db.commit()
            watchdog = LoopWatchdog(asyncio.get_running_loop(), SimpleNamespace(directory=directory, db=db),
                                    directory / 'logs' / 'service.log', timeout=0.3, interval=0.05)
            watchdog.start()
            try:
                await asyncio.sleep(0.8)
                db.execute('BEGIN IMMEDIATE')
                time.sleep(0.8)
                db.rollback()
                for _ in range(30):
                    await asyncio.sleep(0.1)
                    if db.execute('SELECT COUNT(*) FROM problems').fetchone()[0]:
                        break
                self.assertEqual(db.execute('SELECT code FROM problems').fetchall(), [('event-loop-stalled',)])
            finally:
                watchdog.stop()
                db.close()
