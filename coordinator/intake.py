"""Priority and identity for whole coordinator intake items."""

from datetime import datetime, timezone
import json
import re


MAX_ITEMS = 20
MAX_CHARACTERS = 8000
FAILURE_STATES = ('failed', 'blocked', 'interrupted', 'waiting_for_secret', 'waiting_for_quota')
FAILURE_HEADER = re.compile(
    r'^\s*(?:(?:job|worker|task)\s*#?\d+(?:\.\d+)?\s*[:=-]?\s*)?'
    r'(?:failed|failure|blocked|blocker|error|interrupted)\b', re.IGNORECASE | re.MULTILINE)


def payload(row):
    try:
        value = json.loads(row['text'])
    except (ValueError, TypeError):
        return {}
    return value if isinstance(value, dict) else {}


def priority(row):
    if row['kind'] in ('owner', 'forwarded', 'secret_filled'):
        level = 0
    else:
        data = payload(row)
        failed = data.get('success') is False or data.get('status') in FAILURE_STATES
        level = 1 if failed or not data and FAILURE_HEADER.search(row['text']) else 2
    return level, row['created'], row['id']


def identity(store, row):
    data = payload(row) if row['kind'] not in ('owner', 'forwarded') else {}
    worker = row.get('source_worker') or data.get('worker')
    sender = row.get('from_name') or ('worker-%s' % worker if worker is not None else row['kind'])
    job = data.get('task')
    if job is None and row.get('source_worker') is not None:
        saved = store.db.execute('SELECT task FROM workers WHERE id=?', (row['source_worker'],)).fetchone()
        job = saved['task'] if saved else None
    created = datetime.fromtimestamp(row['created'], timezone.utc).isoformat(timespec='seconds').replace('+00:00', 'Z')
    return '[sender=%s job=%s created=%s] ' % (json.dumps(sender, ensure_ascii=False),
                                             json.dumps(job, ensure_ascii=False), created)
