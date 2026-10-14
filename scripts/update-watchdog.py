#!/usr/bin/env python3
"""Copied before mutation so rollback survives a broken checkout or service."""

import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import signal
import sqlite3
import subprocess
import sys
import time


KEY = 'telegram_update_v1'
TERMINAL = ('updated', 'failed', 'rolled_back', 'cancelled', 'current')
PHASES = ('checking', 'offer', 'queued', 'checks', 'arming', 'armed', 'merge', 'tools',
          'restart', 'rolling_back', 'rollback_restart', 'recovering')
HOST_FILE = Path(__file__).with_name('host_os.py')
if not HOST_FILE.is_file():
    HOST_FILE = Path(__file__).resolve().parents[1] / 'coordinator/host_os.py'
HOST_SPEC = importlib.util.spec_from_file_location('torii_update_host_os', HOST_FILE)
host_os = importlib.util.module_from_spec(HOST_SPEC)
HOST_SPEC.loader.exec_module(host_os)


def command(argv, cwd=None, timeout=120, env=None):
    try:
        done = subprocess.run(argv, cwd=cwd, capture_output=True, text=True, timeout=timeout, env=env)
    except (OSError, subprocess.SubprocessError):
        raise RuntimeError('Command did not finish: ' + argv[0]) from None
    if done.returncode:
        raise RuntimeError('Command failed: ' + argv[0])
    return done.stdout.strip()


def process_identity(pid):
    try:
        value = command(['ps', '-p', str(pid), '-o', 'stat=', '-o', 'lstart='], timeout=5,
                        env=dict(os.environ, LC_ALL='C', LANG='C', TZ='UTC')).split(None, 1)
        return value[1] if len(value) == 2 and not value[0].startswith('Z') else None
    except RuntimeError:
        return None


def read(db):
    row = db.execute('SELECT value FROM settings WHERE key=?', (KEY,)).fetchone()
    return json.loads(row[0]) if row else {}


def write(db, update_id, expected=None, restart_pending=False, **fields):
    with db:
        if not db.in_transaction:
            db.execute('BEGIN IMMEDIATE')
        row = db.execute('SELECT value FROM settings WHERE key=?', (KEY,)).fetchone()
        if not row:
            return False
        record = json.loads(row[0])
        if record.get('id') != update_id or (expected is not None and record.get('phase') != expected):
            return False
        if restart_pending and record.get('restart_started'):
            return False
        phase = record.get('phase')
        selected = fields.get('phase', phase)
        if phase in TERMINAL and selected != phase:
            return False
        if phase in PHASES and selected in PHASES and PHASES.index(selected) < PHASES.index(phase):
            return False
        record.update(fields)
        changed = db.execute('UPDATE settings SET value=? WHERE key=? AND value=?',
                             (json.dumps(record), KEY, row[0]))
    return changed.rowcount == 1


def alive(pid, stamp):
    return bool(pid and stamp and process_identity(pid) == stamp)


def watchdog_alive(record):
    return alive(record.get('watchdog_pid'), record.get('watchdog_process'))


def version(binary):
    output = command([binary, '--version'], timeout=20)
    match = re.search(r'(?<![\w.])(\d+\.\d+\.\d+(?:-[\w.-]+)?)(?![\w.])', output)
    if not match:
        raise RuntimeError('CLI version check failed')
    return match[1]


def stop_installation(record):
    active = record.get('installation') or {}
    if active.get('process') and process_identity(active['pid']) == active['process']:
        try:
            if os.getpgid(active['pid']) == active['pid']:
                os.killpg(active['pid'], signal.SIGKILL)
        except ProcessLookupError:
            pass


def register_install(database, update_id, expected, argv):
    if expected != 'tools':
        raise RuntimeError('Update no longer owns this installation')
    stamp = process_identity(os.getpid())
    if not stamp:
        raise RuntimeError('npm installation process could not be identified')
    with sqlite3.connect(database, timeout=30) as db:
        if not write(db, update_id, expected=expected, installation=dict(pid=os.getpid(), process=stamp)):
            raise RuntimeError('Update no longer owns this installation')
    os.execvp(argv[0], argv)


def tracked_install(argv, database, update_id):
    try:
        with sqlite3.connect(database, timeout=30) as db:
            record = read(db)
            expected = record.get('phase')
            if record.get('id') != update_id or expected != 'tools':
                raise RuntimeError('Update no longer owns this installation')
    except sqlite3.Error:
        raise RuntimeError('npm installation tracking failed') from None
    script = Path(database).parent / 'update-watchdogs' / update_id / 'watchdog.py'
    process = subprocess.Popen([sys.executable, str(script), '--install', database,
                                update_id, expected, json.dumps(argv)], stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True, start_new_session=True, close_fds=True)
    try:
        process.communicate(timeout=300)
        if process.returncode:
            raise RuntimeError('npm install failed')
    except subprocess.TimeoutExpired:
        terminate_installation(process)
        raise RuntimeError('npm install timed out') from None
    finally:
        with sqlite3.connect(database, timeout=30) as db:
            active = read(db).get('installation') or {}
            if active.get('pid') == process.pid:
                write(db, update_id, installation=None)


def terminate_installation(process):
    if process.poll() is None:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    process.communicate(timeout=10)


def install(tool, selected, tracking=None):
    argv = ['npm', '--prefix', tool['prefix'], 'install', '--save-exact', '--include=optional',
            '--global=false', '--no-audit', '--no-fund', tool['package'] + '@' + selected]
    if tracking:
        tracked_install(argv, *tracking)
    else:
        command(argv, timeout=300)
    if version(tool['binary']) != selected:
        raise RuntimeError(tool['label'] + ' version check failed')


def restore_tools(record, guard=None):
    results = []
    for tool in reversed(record.get('attempted', [])):
        if guard and guard():
            results.append('Tool rollback stopped because another deploy took over.')
            break
        try:
            install(tool, tool['current'])
            results.append(tool['label'] + ' restored to ' + tool['current'] + '.')
        except RuntimeError:
            results.append(tool['label'] + ' restoration failed. Repair it on the host.')
    return results


def rollback_code(record):
    root = record['root']
    head = command(['git', 'rev-parse', 'HEAD'], root)
    if head == record['old']:
        return 'Torii remains at ' + head[:7] + '.'
    if head != record['target'] or command(['git', 'status', '--porcelain', '--untracked-files=no'], root):
        raise RuntimeError('Torii changed during the update. Checkout left untouched; repair it on the host.')
    command(['git', 'reset', '--keep', record['old']], root)
    return 'Torii restored to ' + record['old'][:7] + '.'


def stop_old(record):
    if record.get('old_process') and process_identity(record['old_pid']) == record['old_process']:
        os.kill(record['old_pid'], signal.SIGKILL)
        return True
    return False


def service_command():
    if host_os.linux():
        return ['systemctl', '--user', 'restart', 'local.telegram-agent-coordinator.service']
    return ['launchctl', 'kickstart', '-k', 'gui/%d/local.telegram-agent-coordinator' % os.getuid()]


def new_health(record, commit):
    health = record.get('health') or {}
    before = record.get('restart_from') or {}
    identity = (health.get('pid'), health.get('process'))
    return bool(record.get('restart_started') and health.get('restart_started') == record['restart_started']
                and health.get('commit') == commit and identity != (before.get('pid'), before.get('process'))
                and identity != (record.get('old_pid'), record.get('old_process'))
                and alive(*identity))


def tools_match(record, selected):
    try:
        return all(version(tool['binary']) == tool[selected] for tool in record.get('attempted', []))
    except RuntimeError:
        return False


def superseded(db, record):
    head = command(['git', 'rev-parse', 'HEAD'], record['root'])
    if head not in (record['old'], record['target']):
        if not write(db, record['id'], phase='recovering'):
            return True
        stop_installation(read(db))
        results = restore_tools(record)
        restored = tools_match(record, 'current')
        head = command(['git', 'rev-parse', 'HEAD'], record['root'])
        write(db, record['id'], phase='failed' if restored and head not in (record['old'], record['target']) else 'recovering', restored=results,
              result='Update stood down: another deploy took over. Torii checkout left at ' + head[:7] + '. ' +
              ('Touched CLIs restored.' if restored else 'CLI restoration is incomplete. Repair it on the host.'))
        return True
    return False


def watchdog(db, update_id, service_control=None, restart_timeout=240):
    if not write(db, update_id, watchdog_armed=True, watchdog_pid=os.getpid(),
                 watchdog_process=process_identity(os.getpid())):
        return
    restart_at = None
    health_at = None
    failure = 'Update failed: healthy restart deadline expired.'
    while True:
        record = read(db)
        if record.get('id') != update_id or record['phase'] in TERMINAL:
            return
        if superseded(db, record):
            return
        health = record.get('health') or {}
        if (not record.get('restart_started') and health.get('commit') and
                (health.get('pid'), health.get('process')) != (record.get('old_pid'), record.get('old_process'))
                and alive(health.get('pid'), health.get('process'))):
            failure = 'Update interrupted: the service restarted outside this update.'
            break
        if record['phase'] == 'restart':
            if restart_at is None:
                restart_at = time.monotonic()
            planned = [tool for tool in record.get('tools', []) if tool['outdated'] and tool['prefix']]
            if (new_health(record, record['target']) and
                    command(['git', 'rev-parse', 'HEAD'], record['root']) == record['target'] and
                    record.get('attempted', []) == planned and tools_match(record, 'latest')):
                write(db, update_id, phase='updated', result='Updated.')
                return
            started = record.get('restart_started')
            old_alive = alive(record.get('old_pid'), record.get('old_process'))
            if not started and not old_alive:
                failure = 'Update interrupted before its own restart.'
                break
            if (not started and time.monotonic() - restart_at >= min(110, restart_timeout)):
                if not write(db, update_id, expected='restart', restart_pending=True, restart_started=time.time(),
                             restart_from=dict(pid=record['old_pid'], process=record['old_process'])):
                    continue
                try:
                    stop_old(record)
                except ProcessLookupError:
                    pass
            elif started and old_alive and time.time() - started >= record.get('shutdown_grace', 50):
                try:
                    stop_old(record)
                except ProcessLookupError:
                    pass
            if not old_alive and health_at is None:
                health_at = time.monotonic()
            expired = health_at is not None and time.monotonic() - health_at >= restart_timeout
        else:
            expired = not alive(record.get('old_pid'), record.get('old_process'))
            if expired:
                failure = 'Update interrupted before its own restart.'
        if expired:
            break
        time.sleep(min(1, restart_timeout / 4))
    if not write(db, update_id, phase='rolling_back', result=failure):
        return
    record = read(db)
    if superseded(db, record):
        return
    stop_installation(record)
    results = restore_tools(record)
    restored = tools_match(record, 'current')
    try:
        if superseded(db, read(db)):
            return
        results.append(rollback_code(record))
        if command(['git', 'rev-parse', 'HEAD'], record['root']) != record['old']:
            raise RuntimeError('Torii rollback could not be verified. Repair it on the host.')
        before = record.get('health') or dict(pid=record['old_pid'], process=record['old_process'])
        if not write(db, update_id, phase='rollback_restart', result=failure, restored=results,
                     health=None, restart_started=time.time(), restart_from=before):
            return
        if superseded(db, read(db)):
            return
        command(service_control if service_control is not None else service_command())
    except RuntimeError as error:
        write(db, update_id, phase='recovering', interrupted_phase='rollback_restart', result=str(error), restored=results)
        return
    deadline = time.monotonic() + restart_timeout
    while time.monotonic() < deadline:
        current = read(db)
        if current.get('id') != update_id or current.get('phase') in TERMINAL or superseded(db, current):
            return
        if new_health(current, record['old']):
            restored = (command(['git', 'rev-parse', 'HEAD'], record['root']) == record['old'] and
                        tools_match(current, 'current'))
            write(db, update_id, phase='rolled_back' if restored else 'recovering',
                  interrupted_phase='rollback_restart',
                  result=failure if restored else 'Rollback could not restore every CLI. Repair it on the host.')
            return
        time.sleep(min(1, restart_timeout / 4))
    restored = command(['git', 'rev-parse', 'HEAD'], record['root']) == record['old'] and tools_match(read(db), 'current')
    write(db, update_id, phase='failed' if restored else 'recovering', interrupted_phase='rollback_restart',
          result='Rollback restarted, but its Telegram poll was not healthy.',
          restored=results)


def main():
    os.umask(0o077)
    if len(sys.argv) > 1 and sys.argv[1] == '--install':
        register_install(sys.argv[2], sys.argv[3], sys.argv[4], json.loads(sys.argv[5]))
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('database')
    parser.add_argument('update_id')
    parser.add_argument('--service-control', type=json.loads)
    parser.add_argument('--restart-timeout', type=float, default=240)
    args = parser.parse_args()
    if args.restart_timeout <= 0:
        parser.error('restart timeout must be positive')
    with sqlite3.connect(args.database, timeout=30) as db:
        watchdog(db, args.update_id, args.service_control, args.restart_timeout)


if __name__ == '__main__':
    main()
