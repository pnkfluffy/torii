"""One runner for every periodic sweep.

Each item runs alone, so one failed item never stops the others. A failed item
waits before its next try: FIRST seconds, twice as long after each further
failure, and at most LIMIT seconds. Each failure writes one problem row, code
`<sweep>-failed`, with the item, the exception type and message, and the
innermost frames of its traceback. A success clears the item's failure state.
"""

import inspect
import logging
from pathlib import Path
import sqlite3
import time
import traceback

from . import problems
from .failures import one_line
from .scrub import sanitize

logger = logging.getLogger(__name__)

FIRST = 60
LIMIT = 24 * 60 * 60
FRAMES = 3
DETAIL = 600


def describe(error):
    """Type, message and the innermost FRAMES frames, innermost first, on one line."""
    frames = traceback.extract_tb(error.__traceback__)[-FRAMES:]
    where = ' < '.join('%s:%d %s' % (Path(frame.filename).name, frame.lineno, frame.name) for frame in reversed(frames))
    text = type(error).__name__ + ': ' + one_line(error, 400)
    return text + ' at ' + where if where else text


def delay(failures):
    return min(FIRST * 2 ** min(failures - 1, 20), LIMIT)


def waiting(store, sweep, item, now=None):
    """The time a failed item may run again, or 0 when it may run now."""
    row = store.db.execute('SELECT retry FROM sweep_failures WHERE sweep=? AND item=?', (sweep, str(item))).fetchone()
    return row[0] if row and row[0] > (time.time() if now is None else now) else 0


def next_retry(store, sweep):
    """Seconds until the first failed item of `sweep` may run again, or None when none waits."""
    row = store.db.execute('SELECT MIN(retry) FROM sweep_failures WHERE sweep=?', (sweep,)).fetchone()
    return None if row[0] is None else max(0, row[0] - time.time())


def _write(store, statement, values):
    if store.db.in_transaction:
        store.db.execute(statement, values)
    else:
        with store.db:
            store.db.execute(statement, values)


def failed(store, sweep, item, error, area, refs):
    now = time.time()
    detail = sanitize(describe(error), DETAIL)
    try:
        row = store.db.execute('SELECT failures FROM sweep_failures WHERE sweep=? AND item=?',
                               (sweep, str(item))).fetchone()
        count = (row[0] if row else 0) + 1
        _write(store, '''INSERT INTO sweep_failures(sweep,item,failures,since,failed,retry,detail) VALUES (?,?,?,?,?,?,?)
            ON CONFLICT(sweep,item) DO UPDATE SET failures=excluded.failures,failed=excluded.failed,
            retry=excluded.retry,detail=excluded.detail''', (sweep, str(item), count, now, now, now + delay(count), detail))
    except sqlite3.Error as failure:
        count = 0
        logger.warning('sweep failure not saved sweep=%s type=%s', sweep, type(failure).__name__)
    try:
        problems.record(store, area, sweep + '-failed', 'item=%s failures=%d %s' % (item, count, detail),
                        limit=DETAIL, **refs)
    except Exception as failure:
        logger.warning('sweep problem not saved sweep=%s type=%s', sweep, type(failure).__name__)


def succeeded(store, sweep, item):
    try:
        if store.db.execute('SELECT 1 FROM sweep_failures WHERE sweep=? AND item=?', (sweep, str(item))).fetchone():
            _write(store, 'DELETE FROM sweep_failures WHERE sweep=? AND item=?', (sweep, str(item)))
    except sqlite3.Error as failure:
        logger.warning('sweep success not saved sweep=%s type=%s', sweep, type(failure).__name__)


def _due(store, sweep, items, key, limit, deadline):
    now = time.time()
    count = 0
    for item in items:
        if limit is not None and count >= limit or deadline is not None and time.monotonic() >= deadline:
            return
        if not waiting(store, sweep, key(item), now):
            count += 1
            yield item


def _settle(store, sweep, item, error, area, key, refs):
    if error is None:
        succeeded(store, sweep, key(item))
    else:
        failed(store, sweep, key(item), error, area, refs(item) if refs else {})


def run(store, sweep, items, action, *, area='cleanup', key=str, refs=None, limit=None, deadline=None):
    """Call action(item) for each item not waiting after a failure, at most `limit` of them,
    and start none after the `time.monotonic()` deadline. Return the items it called action for."""
    ran = []
    for item in _due(store, sweep, items, key, limit, deadline):
        ran.append(item)
        error = None
        try:
            action(item)
        except Exception as caught:
            error = caught
        _settle(store, sweep, item, error, area, key, refs)
    return ran


async def arun(store, sweep, items, action, *, area='cleanup', key=str, refs=None, limit=None, deadline=None):
    """`run` for an action that may be a coroutine function."""
    ran = []
    for item in _due(store, sweep, items, key, limit, deadline):
        ran.append(item)
        error = None
        try:
            result = action(item)
            if inspect.isawaitable(result):
                await result
        except Exception as caught:
            error = caught
        _settle(store, sweep, item, error, area, key, refs)
    return ran
