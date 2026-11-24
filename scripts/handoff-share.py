#!/usr/bin/env python3
"""Report the share of measured worker ends with a fresh handoff."""

import argparse
from pathlib import Path
import sqlite3


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('database', type=Path, help='Torii SQLite database path')
    parser.add_argument('--since', type=float, default=0, help='earliest worker end, Unix seconds')
    args = parser.parse_args()
    uri = args.database.resolve().as_uri() + '?mode=ro'
    with sqlite3.connect(uri, uri=True) as db:
        fresh, total = db.execute('''SELECT COALESCE(SUM(json_extract(text,'$.no_handoff')=0),0), COUNT(*)
            FROM messages WHERE kind='worker_result' AND created>=? AND json_valid(text)
            AND json_type(text,'$.no_handoff') IN ('true','false','integer')''', (args.since,)).fetchone()
    print('Fresh handoffs: %d/%d (%s)' %
          (fresh, total, '%.1f%%' % (100 * fresh / total) if total else 'no measured ends'))


if __name__ == '__main__':
    main()
