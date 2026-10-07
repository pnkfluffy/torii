"""Actionable reports without provider logs, tokens, or internal exception text."""

from pathlib import Path
import re

PRIVATE_PATH = re.compile(r"(?<![\w.])~?/(?:[^\s'\"/]+/)+([^\s'\"/]*)")


class TaskFailure(Exception):
    def __init__(self, cause, action, code=None, detail=None):
        self.cause = cause
        self.action = action
        self.code = code
        self.detail = detail
        super().__init__(cause)


def one_line(text, limit=200):
    return ' '.join(str(text or '').split())[:limit]


def internal_detail(error):
    """Exception type, message, and the innermost frame. One line, never a traceback."""
    frame = ''
    traceback = error.__traceback__
    while traceback:
        frame = '%s:%d' % (Path(traceback.tb_frame.f_code.co_filename).name, traceback.tb_lineno)
        traceback = traceback.tb_next
    detail = type(error).__name__ + ': ' + one_line(PRIVATE_PATH.sub(r'\1', str(error)), 140)
    return detail + ' at ' + frame if frame else detail


def failure_state(error, failure):
    if failure.code:
        return failure.code, failure.detail or one_line(failure.cause)
    if isinstance(error, TaskFailure):
        return 'task_failed', one_line(error.cause)
    return 'internal_error', internal_detail(error)
