#!/usr/bin/env python3
"""Install this checkout as a background service. No token enters the plist."""

import argparse
from pathlib import Path
import sys


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from coordinator import host_os
from coordinator.__main__ import service_running
from coordinator.policy import COORDINATOR_MODEL


def install(coordinator_model=COORDINATOR_MODEL, write_only=False, pair_only=False):
    root = Path(__file__).resolve().parents[1]
    state = Path.home() / '.local/state/telegram-agent-coordinator'
    command = [sys.executable, '-m', 'coordinator', 'serve', '--coordinator-model', coordinator_model]
    if pair_only:
        command.append('--pair-only')
    host_os.install_service(root, state, command, service_running, write_only=write_only)
    if not write_only:
        print('Service installed in ' + ('pairing-only' if pair_only else 'agent') + ' mode.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pair-only", action="store_true", help="Poll and deliver without starting agents")
    parser.add_argument("--write-only", action="store_true", help="Write the plist without loading it")
    parser.add_argument("--coordinator-model", default=COORDINATOR_MODEL,
                        help="Claude model available to your account")
    args = parser.parse_args()
    return install(args.coordinator_model, args.write_only, args.pair_only)


if __name__ == "__main__":
    main()
