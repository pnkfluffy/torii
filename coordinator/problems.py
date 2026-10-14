"""Failures kept for audit: one log line and one row in the `problems` table.

A row holds an area, a fixed code, a short sanitized detail, and the ids it concerns.
Owner-supplied restart reasons may contain private text. The filter removes
selected token, email, and URL shapes but does not remove every secret.
"""

from datetime import datetime
import json
import logging
import re
import sqlite3
import time

from .control_api import Refused, Result

logger = logging.getLogger(__name__)

DETAIL_LIMIT = 300
RETENTION_DAYS = 30
PAGE = 50
PAGE_LIMIT = 500
SUMMARY_HOURS = 24
REFS = ('topic', 'task', 'worker', 'message', 'attachment')

TOKEN = re.compile(r'(?<!\d)\d{5,}:[A-Za-z0-9_-]{30,}')
URL_CREDENTIALS = re.compile(r'(?i)\b([a-z][a-z0-9+.-]*://)[^\s/@]+@')
URL_QUERY = re.compile(r'(?i)(\b[a-z][a-z0-9+.-]*://[^\s?#]*)[?#]\S*')
EMAIL = re.compile(r'[\w.+-]+@[\w-]+(?:\.[\w-]+)+')

_quiet = {}


def sanitize(text, limit=DETAIL_LIMIT):
    """One short line without a bot token, URL credentials or query, or email address."""
    if text is None:
        return None
    text = ' '.join(str(text).split())
    text = TOKEN.sub('[token]', text)
    text = URL_CREDENTIALS.sub(r'\1[credentials]@', text)
    text = URL_QUERY.sub(r'\1?[query]', text)
    text = EMAIL.sub('[email]', text)
    return text if len(text) <= limit else text[:limit - 3] + '...'


def record(store, area, code, detail=None, level=logging.WARNING, every=None,
           topic=None, task=None, worker=None, message=None, attachment=None):
    """Log and save one problem. With `every`, repeats of one area and code inside
    that many seconds are only counted, and the next saved row reports the count.
    A failed save is logged and never raised, so a failure path stays a failure path."""
    now = time.time()
    if every:
        key = (str(store.directory), area, code)
        last, repeated = _quiet.get(key, (0, 0))
        if now - last < every:
            _quiet[key] = (last, repeated + 1)
            return None
        _quiet[key] = (now, 0)
        if repeated:
            detail = 'repeated=%d %s' % (repeated, detail or '')
    detail = sanitize(detail)
    refs = {name: sanitize(value, 80) if isinstance(value, str) else value
            for name, value in zip(REFS, (topic, task, worker, message, attachment)) if value is not None}
    logger.log(level, 'problem area=%s code=%s%s%s', area, code,
               ''.join(' %s=%s' % item for item in refs.items()),
               ' detail=' + json.dumps(detail, ensure_ascii=False) if detail else '')
    values = (now, area, code, detail) + tuple(refs.get(name) for name in REFS)
    statement = ('INSERT INTO problems(created,area,code,detail,' + ','.join(REFS) +
                 ') VALUES (?,?,?,?,?,?,?,?,?)')
    try:
        if store.db.in_transaction:
            return store.db.execute(statement, values).lastrowid
        with store.db:
            return store.db.execute(statement, values).lastrowid
    except sqlite3.Error as error:
        logger.error('problem not saved area=%s code=%s type=%s', area, code, type(error).__name__)
        return None


def prune(store, days=RETENTION_DAYS):
    with store.db:
        removed = store.db.execute('DELETE FROM problems WHERE created<?',
                                   (time.time() - days * 86400,)).rowcount
    logger.info('problems pruned removed=%d days=%d', removed, days)
    return removed


def stamp(created):
    return datetime.fromtimestamp(created).astimezone().isoformat(timespec='seconds')


def _since(value, default=None):
    if value is None:
        return default
    try:
        moment = datetime.fromisoformat(value[:-1] + '+00:00' if value.endswith('Z') else value)
    except ValueError:
        raise Refused('Send since as an ISO time such as 2026-09-24T06:00 or 2026-09-24T06:00+02:00.') from None
    return moment.timestamp()


def _row(row):
    return dict(row, created=stamp(row['created']))


def _line(row):
    refs = ''.join(' %s=%s' % (name, row[name]) for name in REFS if row[name] is not None)
    return '%s %s %s%s%s' % (row['created'], row['area'], row['code'], refs,
                             ': ' + row['detail'] if row['detail'] else '')


def op_problems_list(store, ctx, since=None, area=None, code=None, limit=None):
    limit = PAGE if limit is None else limit
    if not 1 <= limit <= PAGE_LIMIT:
        raise Refused('Send a limit from 1 to %d.' % PAGE_LIMIT)
    rows = [_row(row) for row in store.db.execute(
        '''SELECT * FROM problems WHERE created>=? AND (? IS NULL OR area=?) AND (? IS NULL OR code=?)
        ORDER BY id DESC LIMIT ?''', (_since(since, 0), area, area, code, code, limit))]
    return Result(True, '\n'.join(_line(row) for row in rows) or 'No problems recorded.', {'problems': rows})


def op_problems_summary(store, ctx, since=None):
    start = _since(since, time.time() - SUMMARY_HOURS * 3600)
    rows = [dict(row, first=stamp(row['first']), last=stamp(row['last'])) for row in store.db.execute(
        '''SELECT area,code,COUNT(*) AS count,MIN(created) AS first,MAX(created) AS last FROM problems
        WHERE created>=? GROUP BY area,code ORDER BY count DESC,area,code''', (start,))]
    lines = ['%s %s: %d (last %s)' % (row['area'], row['code'], row['count'], row['last']) for row in rows]
    return Result(True, '\n'.join(['Problems since ' + stamp(start) + ':'] + lines) if rows else
                  'No problems since ' + stamp(start) + '.', {'since': stamp(start), 'counts': rows})
