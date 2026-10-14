"""Job milestones and bounded host samples for the local operator."""

import asyncio
from datetime import datetime, timezone
import math
import time

from . import health, problems
from .control_api import Refused, Result, STAGES


SAMPLE_SECONDS = 300
RETENTION_DAYS = 30


def stamp(created):
    return datetime.fromtimestamp(created, timezone.utc).isoformat(timespec='seconds').replace('+00:00', 'Z')


def prune(store):
    with store.db:
        return store.db.execute('DELETE FROM host_samples WHERE created<?',
                                (time.time() - RETENTION_DAYS * 86400,)).rowcount


async def sample_once(store):
    pressure, browsers = await asyncio.gather(asyncio.to_thread(health.read_pressure),
                                             asyncio.to_thread(health.read_browser_load))
    values = dict(pressure, **browsers)
    values['created'] = time.time()
    values['running_workers'] = store.db.execute("SELECT COUNT(*) FROM workers WHERE status='running'").fetchone()[0]
    with store.db:
        store.db.execute('INSERT INTO host_samples(' + ','.join(values) + ') VALUES (' +
                         ','.join('?' for _ in values) + ')', tuple(values.values()))
    prune(store)
    missing = [name for name, value in values.items() if value is None]
    if missing:
        problems.record(store, 'metrics', 'readings-unavailable', ','.join(missing), every=3600)


def _jobs(store, since, until):
    rows = store.db.execute('''SELECT t.id,t.topic,c.name AS channel,t.number,t.created,s.stage,MIN(s.created) AS first
        FROM tasks t JOIN topics c ON c.id=t.topic JOIN task_stamps s ON s.task=t.id
        WHERE s.created<=? AND EXISTS (SELECT 1 FROM task_stamps w WHERE w.task=t.id AND w.created BETWEEN ? AND ?)
        GROUP BY t.id,s.stage ORDER BY t.topic,t.number''', (until, since, until))
    jobs = {}
    for row in rows:
        job = jobs.setdefault(row['id'], {'task': row['id'], 'topic': row['topic'], 'channel': row['channel'],
                                         'number': row['number'], 'first_stamps': {}, 'durations': []})
        job['first_stamps'][row['stage']] = row['first']
        if row['stage'] == 'ready':
            job['durations'].append({'from': 'created', 'to': 'ready', 'seconds': row['first'] - row['created']})
    for job in jobs.values():
        present = [stage for stage in STAGES if stage in job['first_stamps']]
        for before, after in zip(present, present[1:]):
            job['durations'].append({'from': before, 'to': after,
                                    'seconds': job['first_stamps'][after] - job['first_stamps'][before]})
    return list(jobs.values())


def _percentile(values, fraction):
    if not values:
        return None
    position = (len(values) - 1) * fraction
    lower, upper = math.floor(position), math.ceil(position)
    if lower == upper:
        return values[lower]
    return round(values[lower] + (values[upper] - values[lower]) * (position - lower), 10)


def _daily(store, since, until):
    groups = {}
    for row in store.db.execute('SELECT * FROM host_samples WHERE created BETWEEN ? AND ? ORDER BY created',
                                (since, until)):
        date = datetime.fromtimestamp(row['created'], timezone.utc).date().isoformat()
        groups.setdefault(date, []).append(row)
    days = []
    for date, rows in groups.items():
        loads = sorted(row['load1'] for row in rows if row['load1'] is not None)
        day = {'date': date, 'samples': len(rows), 'load1_p50': _percentile(loads, 0.5),
               'load1_p95': _percentile(loads, 0.95), 'load1_max': max(loads, default=None)}
        for field in ('memory_used', 'swap_used', 'browser_count', 'browser_rss', 'running_workers'):
            day[field + '_max'] = max((row[field] for row in rows if row[field] is not None), default=None)
        days.append(day)
    return days


def _value(value):
    return 'unavailable' if value is None else str(value)


def op_metrics_report(store, ctx, days=None):
    if ctx.source != 'cli':
        raise Refused('metrics.report is available only from the local CLI.')
    days = 7 if days is None else days
    if days < 1:
        raise Refused('Send days of 1 or more.')
    until = time.time()
    try:
        since = until - days * 86400
        start = stamp(since)
    except (ValueError, OverflowError, OSError):
        raise Refused('Send days within the supported UTC date range.') from None
    data = {'days': days, 'since': start, 'until': stamp(until),
            'jobs': _jobs(store, since, until), 'daily': _daily(store, since, until)}
    lines = ['Metrics for %d days, %s to %s' % (days, data['since'], data['until'])]
    for job in data['jobs']:
        lines.append('%s (%s) job %d' % (job['channel'], job['topic'], job['number']))
        lines.extend('  %s -> %s: %s s' % (duration['from'], duration['to'], duration['seconds'])
                     for duration in job['durations'])
        if not job['durations']:
            lines.append('  No duration between recorded stages.')
    if not data['jobs']:
        lines.append('No job stamps in this window.')
    for day in data['daily']:
        maxima = []
        for field in ('memory_used_max', 'swap_used_max', 'browser_count_max', 'browser_rss_max', 'running_workers_max'):
            unit = ' bytes' if field in ('memory_used_max', 'swap_used_max', 'browser_rss_max') else ''
            maxima.append('%s=%s%s' % (field, _value(day[field]), unit))
        lines.append('%s UTC: samples=%d load1 p50=%s p95=%s max=%s %s' % (
            day['date'], day['samples'], _value(day['load1_p50']), _value(day['load1_p95']),
            _value(day['load1_max']), ' '.join(maxima)))
    if not data['daily']:
        lines.append('No host samples in this window.')
    return Result(True, '\n'.join(lines), data)
