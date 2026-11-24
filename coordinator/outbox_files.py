"""Manage files staged for queued Telegram messages."""

import logging
from pathlib import Path
import shutil
import sqlite3

logger = logging.getLogger(__name__)


def remove(path, root, store=None):
    path, root = Path(path), Path(root)
    if path.parent.parent != root or path.parent.is_symlink():
        return
    try:
        shutil.rmtree(path.parent)
    except FileNotFoundError:
        pass
    except OSError as error:
        if store is None:
            logger.warning('outbox attachment cleanup failed: %s', type(error).__name__)
        else:
            from . import problems
            problems.record(store, 'outbox', 'attachment-cleanup-failed', type(error).__name__, every=3600)


class Connection(sqlite3.Connection):
    def track_outbox_file(self, path, root):
        if not hasattr(self, '_outbox_files'):
            self._outbox_files = {}
        self._outbox_files[str(path)] = str(root)

    def cleanup_outbox_file(self, path, root):
        if not hasattr(self, '_outbox_cleanup'):
            self._outbox_cleanup = {}
        self._outbox_cleanup[str(path)] = str(root)
        if not self.in_transaction:
            self._reconcile_outbox_files()

    def _reconcile_outbox_files(self):
        if getattr(self, '_outbox_reconciling', False):
            return
        self._outbox_reconciling = True
        try:
            files = getattr(self, '_outbox_files', {})
            for path, root in list(files.items()):
                if not self.execute('SELECT 1 FROM outbox WHERE image=? LIMIT 1', (path,)).fetchone():
                    remove(path, root, getattr(self, '_outbox_owner', None))
                    del files[path]
                elif not self.in_transaction:
                    del files[path]
            if not self.in_transaction:
                cleanup = getattr(self, '_outbox_cleanup', {})
                for path, root in list(cleanup.items()):
                    if not self.execute('SELECT 1 FROM outbox WHERE image=? AND delivered=0 AND retired=0 LIMIT 1', (path,)).fetchone():
                        remove(path, root, getattr(self, '_outbox_owner', None))
                    del cleanup[path]

        finally:
            self._outbox_reconciling = False

    def execute(self, sql, parameters=()):
        result = super().execute(sql, parameters)
        if sql.lstrip().upper().startswith('ROLLBACK'):
            self._reconcile_outbox_files()
        return result

    def commit(self):
        super().commit()
        self._reconcile_outbox_files()

    def rollback(self):
        super().rollback()
        self._reconcile_outbox_files()

    def __exit__(self, *args):
        try:
            return super().__exit__(*args)
        finally:
            self._reconcile_outbox_files()

    def close(self):
        if getattr(self, '_outbox_closed', False):
            return
        if self.in_transaction:
            self.rollback()
        super().close()
        self._outbox_closed = True
