"""Job finish reminders and the worker result line."""

import re

from .control_api import job_number


FINISH_NUDGE = 2


REPORTED = ('blocked', 'partial')


def result_line(payload):
    """`done`, `blocked` or `partial` from the worker's last line `RESULT: <status> [why]`, or None."""
    lines = (payload.get('text') or '').strip().splitlines()
    last = lines[-1].strip().strip('*`_ ') if lines else ''
    found = re.match(r'RESULT:\s*(done|blocked|partial)\b', last, re.IGNORECASE)
    return found.group(1).lower() if found else None


class JobLifecycle:
    def __init__(self, service):
        self.service = service
        self.store = service.store

    def finish_due(self, topic):
        return self.store.db.execute('''SELECT t.id,t.number,t.iteration,t.finish,i.id AS round FROM tasks t
            JOIN task_iterations i ON i.task=t.id AND i.k=t.iteration
            WHERE t.topic=? AND t.status='open' AND t.finish IS NOT NULL AND (i.nudged & ?)=0
            AND EXISTS(SELECT 1 FROM task_stamps s WHERE s.task=t.id AND s.iteration=t.iteration AND s.stage=t.finish)
            AND NOT EXISTS(SELECT 1 FROM workers w WHERE w.task=t.id
                AND w.status IN ('queued','running','waiting_for_secret','waiting_for_quota'))
            AND NOT EXISTS(SELECT 1 FROM wait_jobs j JOIN waits w ON w.id=j.wait WHERE j.task=t.id AND w.state='armed')
            ORDER BY t.number''', (topic, FINISH_NUDGE)).fetchall()

    def finish_reminders(self, topic):
        if not self.finish_due(topic):
            return 0
        from .waits import _callback
        with self.store.transaction():
            rows = self.finish_due(topic)
            for row in rows:
                self.store.db.execute('UPDATE task_iterations SET nudged=nudged|? WHERE id=?', (FINISH_NUDGE, row['round']))
                _callback(self.store, topic, 'Job %s reached its finish stage (%s). Close it with its result, '
                          'or note what is left.' % (job_number(row['number'], row['iteration']), row['finish']))
        return len(rows)
