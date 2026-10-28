#!/usr/bin/env python3
"""Fail if a unittest run leaves a detached Torii host running."""

import argparse
from pathlib import Path
import re
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path)
    args = parser.parse_args()
    root = args.root.resolve() if args.root else None
    output = subprocess.check_output(['ps', '-axo', 'pid=,ppid=,stat=,command='],
                                     text=True, errors='replace')
    survivors = []
    for line in output.splitlines():
        fields = line.split(None, 3)
        if len(fields) != 4 or fields[2].startswith('Z'):
            continue
        executable, separator, directory = fields[3].partition(' -m coordinator host ')
        if not separator or not re.fullmatch(r'python(?:\d+(?:\.\d+)*)?', Path(executable).name,
                                            flags=re.IGNORECASE):
            continue
        if root is None or Path(directory).resolve().is_relative_to(root):
            survivors.append(line.strip())
    if survivors:
        print('FAIL: test hosts survived the unittest run', file=sys.stderr)
        print('\n'.join(survivors), file=sys.stderr)
        return 1
    print('PASS: no test hosts remain')
    return 0


if __name__ == '__main__':
    sys.exit(main())
