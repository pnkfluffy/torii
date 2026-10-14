#!/usr/bin/env python3
"""Install this checkout as a background service. No token enters the plist."""

import argparse
import os
from pathlib import Path
import plistlib
import re
import shutil
import subprocess
from types import SimpleNamespace
import sys
import time


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from coordinator import host_os
from coordinator.__main__ import service_running


def systemd_path(value):
    value = str(value)
    if any(char in value for char in ('\n', '\r', '\x00')):
        raise ValueError('Invalid systemd path or argument')
    return value.replace('%', '%%')


def systemd_quote(value):
    value = systemd_path(value)
    return '"' + value.replace('\\', '\\\\').replace('"', '\\"').replace('$', '$$') + '"'


def install_linux(args):
    if os.getuid() == 0:
        raise SystemExit('Refusing to install Torii as root. Use a normal user with linger.')
    os.umask(0o077)
    root = Path(__file__).resolve().parents[1]
    state = Path.home() / '.local/state/telegram-agent-coordinator'
    state.mkdir(parents=True, exist_ok=True, mode=0o700)
    state.chmod(0o700)
    label = 'local.telegram-agent-coordinator.service'
    target = Path.home() / '.config/systemd/user' / label
    target.parent.mkdir(parents=True, exist_ok=True)
    command = [sys.executable, '-m', 'coordinator', 'serve', '--coordinator-model', args.coordinator_model]
    if args.pair_only:
        command.append('--pair-only')
    unit = '\n'.join([
        '[Unit]', 'Description=Torii Telegram agent coordinator', '', '[Service]',
        'ExecStart=' + ' '.join(systemd_quote(arg) for arg in command),
        'WorkingDirectory=' + systemd_path(root), 'Restart=always', 'RestartSec=10',
        'KillMode=process', 'UMask=0077',
        'Environment="PATH=%h/.local/bin:/usr/local/bin:/usr/bin:/bin"',
        'StandardOutput=append:' + systemd_path(state / 'service.stdout.log'),
        'StandardError=append:' + systemd_path(state / 'service.stderr.log'),
        '', '[Install]', 'WantedBy=default.target', '',
    ])
    target.write_text(unit)
    target.chmod(0o600)
    print(f'Wrote {target}')
    if args.write_only:
        return
    subprocess.run(['loginctl', 'enable-linger'], check=True)
    subprocess.run(['systemctl', '--user', 'daemon-reload'], check=True)
    active = subprocess.run(['systemctl', '--user', 'is-active', '--quiet', label],
                            capture_output=True).returncode == 0
    subprocess.run(['systemctl', '--user', 'enable', '--now', label], check=True)
    if active:
        subprocess.run(['systemctl', '--user', 'restart', label], check=True)
    print('Service installed in ' + ('pairing-only' if args.pair_only else 'agent') + ' mode.')


def unload(domain, label):
    target = domain + "/" + label
    prior = subprocess.run(["launchctl", "print", target], capture_output=True, text=True)
    match = re.search(r"^\s*pid = (\d+)\s*$", prior.stdout, re.MULTILINE)
    pid = int(match.group(1)) if match else None
    subprocess.run(["launchctl", "bootout", target], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.monotonic() + 40
    while True:
        alive = False
        if pid:
            try:
                os.kill(pid, 0)
                alive = True
            except ProcessLookupError:
                pass
        registered = subprocess.run(["launchctl", "print", target],
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0
        if not alive and not registered:
            return
        if time.monotonic() >= deadline:
            raise SystemExit("Old service has not stopped. Inspect its process before rerunning the installer.")
        time.sleep(0.2)


def external_drive_hint(state):
    paths = (state, Path.home() / '.config/telegram-agent-coordinator/bot-token',
             Path(sys.executable), Path(shutil.which('claude') or Path.home() / '.local/bin/claude'))
    if any(path.resolve().is_relative_to('/Volumes') for path in paths):
        return (' macOS stops background services from using files on external drives. '
                'Keep Torii folders and Claude Code on the internal disk, or run scripts/run-service.sh in a terminal.')
    return ''


def wait_for_start(state, domain, label, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if service_running(state):
            return
        time.sleep(0.25)
    result = subprocess.run(['launchctl', 'print', domain + '/' + label], capture_output=True, text=True)
    detail = ''
    if result.returncode == 0:
        pid = re.search(r'^\s*pid = (\d+)\s*$', result.stdout, re.MULTILINE)
        exit_status = re.search(r'^\s*last exit (?:code|status) = (\d+)\s*$', result.stdout, re.MULTILINE)
        if pid:
            detail += ' launchd reported pid ' + pid.group(1) + '.'
        if exit_status:
            detail += ' Last exit status: ' + exit_status.group(1) + '.'
    subprocess.run(['launchctl', 'bootout', domain + '/' + label],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    raise ValueError('The background service did not start. Check the service log and rerun setup.'
                     + detail + external_drive_hint(state))


def install(enable_agents=False, coordinator_model='claude-opus-5-5', write_only=False, pair_only=False):
    args = SimpleNamespace(enable_agents=enable_agents, coordinator_model=coordinator_model, write_only=write_only, pair_only=pair_only)
    if host_os.linux():
        return install_linux(args)
    if sys.platform != "darwin":
        raise SystemExit("This installer requires macOS. Use run-service.sh on other systems.")
    os.umask(0o077)
    root = Path(__file__).resolve().parents[1]
    state = Path.home() / ".local/state/telegram-agent-coordinator"
    state.mkdir(parents=True, exist_ok=True, mode=0o700)
    state.chmod(0o700)
    label = "local.telegram-agent-coordinator"
    target = Path.home() / "Library/LaunchAgents" / (label + ".plist")
    target.parent.mkdir(parents=True, exist_ok=True)
    command = [sys.executable, "-m", "coordinator", "serve", "--coordinator-model", args.coordinator_model]
    if args.pair_only:
        command.append("--pair-only")
    environment = {"PATH": "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"}
    if "BROWSER" in os.environ:
        environment["BROWSER"] = os.environ["BROWSER"]
    data = {
        "Label": label, "ProgramArguments": command, "WorkingDirectory": str(root),
        "RunAtLoad": True, "KeepAlive": True, "ThrottleInterval": 10,
        "AbandonProcessGroup": True,
        "ProcessType": "Interactive", "Umask": 0o077,
        "EnvironmentVariables": environment,
        "StandardOutPath": str(state / "service.stdout.log"),
        "StandardErrorPath": str(state / "service.stderr.log"),
    }
    with target.open("wb") as stream:
        plistlib.dump(data, stream)
    target.chmod(0o600)
    print(f"Wrote {target}")
    if args.write_only:
        return
    domain = f"gui/{os.getuid()}"
    unload(domain, label)
    subprocess.run(["launchctl", "bootstrap", domain, str(target)], check=True)
    wait_for_start(state, domain, label)
    print("Service installed in " + ("pairing-only" if args.pair_only else "agent") + " mode.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--enable-agents", action="store_true", help="Accepted for compatibility; agents are enabled by default")
    parser.add_argument("--pair-only", action="store_true", help="Poll and deliver without starting agents")
    parser.add_argument("--write-only", action="store_true", help="Write the plist without loading it")
    parser.add_argument("--coordinator-model", default="claude-opus-5-5",
                        help="Claude model available to your account")
    args = parser.parse_args()
    return install(args.enable_agents, args.coordinator_model, args.write_only, args.pair_only)


if __name__ == "__main__":
    main()
