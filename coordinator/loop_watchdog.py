"""Capture all thread stacks when the service event loop stops ticking."""

import faulthandler
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import select
import sqlite3
import subprocess
import sys
import time
from types import SimpleNamespace

from . import log, problems, scan_process


_last_failure_log = float('-inf')


class LoopWatchdog:
    def __init__(self, loop, store, log_path, timeout=60, interval=1):
        self.loop = loop
        self.store = store
        self.log_path = log_path
        self.timeout = timeout
        self.interval = interval
        self.output = None
        self.timer = None
        self.armed = None
        self.child = None
        self.child_failed = False

    def start(self):
        self.output = self.log_path.open('ab', buffering=0)
        try:
            self.child = subprocess.Popen(
                (sys.executable, '-m', 'coordinator.loop_watchdog', str(self.store.directory), str(self.timeout)),
                stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                cwd=str(Path(__file__).resolve().parent.parent), env=scan_process.child_env())
            os.set_blocking(self.child.stdin.fileno(), False)
        except OSError as error:
            self.child_failed = True
            problems.record(self.store, 'service', 'watchdog-start-failed', type(error).__name__, every=3600)
        self.tick()

    def tick(self):
        now = time.monotonic()
        stalled = self.armed is not None and now - self.armed >= self.timeout
        try:
            info = self.log_path.stat()
            current = self.output.fileno()
            opened = os.fstat(current)
            if (info.st_dev, info.st_ino) != (opened.st_dev, opened.st_ino):
                replacement = self.log_path.open('ab', buffering=0)
                faulthandler.cancel_dump_traceback_later()
                self.output.close()
                self.output = replacement
        except OSError as error:
            problems.record(self.store, 'service', 'watchdog-log-failed', type(error).__name__, every=3600)
        self.armed = time.monotonic()
        faulthandler.dump_traceback_later(self.timeout, file=self.output, repeat=False)
        self.timer = self.loop.call_later(self.interval, self.tick)
        if self.child and not self.child_failed:
            try:
                os.write(self.child.stdin.fileno(), b'.')
            except (OSError, ValueError) as error:
                self.child_failed = True
                problems.record(self.store, 'service', 'watchdog-heartbeat-failed', type(error).__name__, every=3600)
        if stalled and self.child_failed:
            problems.record(self.store, 'service', 'event-loop-stalled',
                            'Event loop did not tick for at least %s seconds; thread stacks are in the service log.'
                            % self.timeout, level=logging.ERROR, every=3600)

    def stop(self):
        if self.timer is not None:
            self.timer.cancel()
            self.timer = None
        faulthandler.cancel_dump_traceback_later()
        if self.output is not None:
            self.output.close()
            self.output = None
        if self.child is not None:
            self.child.stdin.close()
            try:
                self.child.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.child.kill()
                self.child.wait()
            self.child = None


def record_stall(directory, timeout):
    global _last_failure_log
    logger = logging.getLogger('coordinator.problems')
    handler = RotatingFileHandler(directory / 'logs' / 'watchdog.log', maxBytes=log.MAX_BYTES,
                                  backupCount=log.BACKUPS, encoding='utf-8')
    logger.addHandler(handler)
    logger.setLevel(logging.WARNING)
    try:
        db = sqlite3.connect(str(directory / 'state.sqlite'), timeout=1)
        try:
            detail = 'Event loop did not tick for at least %s seconds; thread stacks are in the service log.' % timeout
            store = SimpleNamespace(directory=directory, db=db)
            saved = problems.record(store, 'service', 'event-loop-stalled', detail,
                                    level=logging.ERROR, every=60)
            if saved is not None or db.execute("SELECT 1 FROM problems WHERE area='service' AND code='event-loop-stalled' AND created>?", (time.time() - 60,)).fetchone():
                return True
            raise sqlite3.OperationalError('problem was not saved')
        finally:
            db.close()
    except sqlite3.Error:
        now = time.monotonic()
        if now - _last_failure_log >= 60:
            _last_failure_log = now
            logger.warning('watchdog problem could not be saved')
        return False
    finally:
        logger.removeHandler(handler)
        handler.close()


def monitor(directory, timeout):
    deadline = time.monotonic() + timeout
    stalled = False
    pending = False
    retry = None
    while True:
        now = time.monotonic()
        wake = None if stalled else deadline
        if pending and (wake is None or retry < wake):
            wake = retry
        ready, _, _ = select.select([sys.stdin.fileno()], [], [], None if wake is None else max(0, wake - now))
        now = time.monotonic()
        if not stalled and now >= deadline:
            stalled = True
            pending = True
            retry = now
        if ready:
            if not os.read(sys.stdin.fileno(), 4096):
                return
            deadline = now + timeout
            stalled = False
        if pending and now >= retry:
            pending = not record_stall(directory, timeout)
            retry = now + min(10, timeout)


if __name__ == '__main__':
    monitor(Path(sys.argv[1]), float(sys.argv[2]))
