#!/usr/bin/env python3
"""Copied before mutation so rollback survives a broken checkout or service."""

import argparse
import ctypes
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time


KEY = 'telegram_update_v1'
INSTALL_TIMEOUT = 3600
CLI_PACKAGES = {'claude': '@anthropic-ai/claude-code', 'codex': '@openai/codex'}
TERMINAL = ('updated', 'failed', 'rolled_back', 'cancelled', 'current')
PHASES = ('checking', 'offer', 'queued', 'checks', 'arming', 'armed', 'merge', 'tools',
          'restart', 'rolling_back', 'rollback_restart', 'recovering')
HOST_FILE = Path(__file__).with_name('host_os.py')
if not HOST_FILE.is_file():
    HOST_FILE = Path(__file__).resolve().parents[1] / 'coordinator/host_os.py'
HOST_SPEC = importlib.util.spec_from_file_location('torii_update_host_os', HOST_FILE)
host_os = importlib.util.module_from_spec(HOST_SPEC)
HOST_SPEC.loader.exec_module(host_os)
SCRUB_SPEC = importlib.util.spec_from_file_location('torii_update_scrub', HOST_FILE.with_name('scrub.py'))
scrub = importlib.util.module_from_spec(SCRUB_SPEC)
SCRUB_SPEC.loader.exec_module(scrub)


def command(argv, cwd=None, timeout=120, env=None, raw=False):
    try:
        done = subprocess.run(argv, cwd=cwd, capture_output=True, text=True, timeout=timeout, env=env)
    except (OSError, subprocess.SubprocessError):
        raise RuntimeError('Command did not finish: ' + argv[0]) from None
    if done.returncode:
        detail = scrub.sanitize(done.stderr or '', limit=None)[-400:]
        message = 'Command failed: ' + argv[0] + (': ' + detail if detail else '')
        raise RuntimeError(message)
    return done.stdout if raw else done.stdout.strip()


def local_npm():
    node = Path(shutil.which('node') or '/usr/bin/node').resolve()
    for candidate in ('/opt/homebrew/bin/npm', '/opt/homebrew/opt/node@22/bin/npm',
                      '/usr/local/bin/npm', '/usr/bin/npm', str(node.with_name('npm'))):
        if Path(candidate).resolve().name == 'npm-cli.js' and os.access(candidate, os.X_OK):
            return candidate
    raise RuntimeError('A local npm installation could not be found')


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


def version(binary, exact=False, timeout=None):
    argv = [binary, '--version']
    if not shutil.which('node'):
        try:
            with open(binary, 'rb') as source:
                shebang = source.readline(128).strip()
        except OSError:
            shebang = None
        if shebang == b'#!/usr/bin/env node':
            for candidate in ('/opt/homebrew/bin/node', '/opt/homebrew/opt/node@22/bin/node',
                              '/usr/local/bin/node', '/usr/bin/node'):
                node = Path(candidate).resolve()
                if os.access(node, os.X_OK):
                    argv.insert(0, str(node))
                    break
    if exact:
        output = command(argv, timeout=10 if timeout is None else timeout, raw=True)
        if not re.fullmatch(r'\d+\.\d+\.\d+(?:-[\w.-]+)?\n?', output):
            raise RuntimeError('CLI version check failed')
        return output[:-1] if output.endswith('\n') else output
    output = command(argv, timeout=20 if timeout is None else timeout)
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
        process.communicate(timeout=INSTALL_TIMEOUT)
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


def tool_path(tool, selected):
    return tool.get('paths', {}).get(selected, tool)


def tool_version(tool, db=None):
    binary = tool['binary']
    if tool.get('setting'):
        if db is None:
            raise RuntimeError('Tool setting could not be read')
        row = db.execute('SELECT value FROM settings WHERE key=?', (tool['setting'],)).fetchone()
        binary = json.loads(row[0]) if row else None
        if not isinstance(binary, str) or not Path(binary).is_absolute():
            raise RuntimeError('Tool binary setting could not be read')
    return version(binary, exact=True) if tool.get('paths') else version(binary)


def select_tool(db, tool, selected):
    path = tool_path(tool, selected)
    if version(path['binary'], exact=True) != selected:
        raise RuntimeError(tool['label'] + ' version check failed')
    db.execute('INSERT OR REPLACE INTO settings VALUES (?,?)', (tool['setting'], json.dumps(path['binary'])))


def install(tool, selected, tracking=None):
    path = tool_path(tool, selected)
    if not tool.get('paths'):
        return staged_install(tool, selected, tracking)
    argv = [local_npm(), '--prefix', path['prefix'], 'install', '--save-exact', '--include=optional',
            '--global=false', '--no-audit', '--no-fund', tool['package'] + '@' + selected]
    if tracking:
        tracked_install(argv, *tracking)
    else:
        command(argv, timeout=INSTALL_TIMEOUT)
    if (version(path['binary'], exact=True) if tool.get('paths') else version(path['binary'])) != selected:
        raise RuntimeError(tool['label'] + ' version check failed')


def file_identity(path):
    try:
        stat = path.lstat()
        return [stat.st_dev, stat.st_ino]
    except FileNotFoundError:
        return None


def exchange(left, right):
    library = ctypes.CDLL(None, use_errno=True)
    if sys.platform == 'darwin':
        rename = library.renamex_np
        rename.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint]
        args = (os.fsencode(left), os.fsencode(right), 2)
    elif sys.platform.startswith('linux') and hasattr(library, 'renameat2'):
        rename = library.renameat2
        rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
        args = (-100, os.fsencode(left), -100, os.fsencode(right), 2)
    else:
        raise RuntimeError('Atomic tool installation is not supported on this host')
    rename.restype = ctypes.c_int
    if rename(*args):
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))


def restore_runtime(runtime, database):
    with sqlite3.connect(database, timeout=30) as db:
        db.execute('BEGIN IMMEDIATE')
        stage = Path(runtime['stage'])
        if not stage.exists() or (stage / '.restored').exists():
            return
        prefix = Path(runtime['prefix'])
        for name, entry in reversed(list(runtime['entries'].items())):
            live = prefix / name
            previous = stage / name
            actual = file_identity(live)
            if actual == entry['old']:
                continue
            if actual != entry['new'] or file_identity(previous) != entry['old']:
                raise RuntimeError('Tool runtime changed during the update. Repair it on the host.')
            if entry['old'] is None:
                live.rename(previous)
            else:
                exchange(live, previous)
        (stage / '.restored').touch()


def staged_install(tool, selected, tracking):
    prefix = Path(tool['prefix'])
    checks = {}
    packages = [tool['package'] + '@' + selected]
    for name, package in CLI_PACKAGES.items():
        manifest = prefix / 'node_modules' / package / 'package.json'
        if not manifest.is_file():
            continue
        data = json.loads(manifest.read_text())
        bins = data['bin']
        relative = Path('node_modules') / package / (bins[name] if isinstance(bins, dict) else bins)
        checks[str(relative)] = selected if package == tool['package'] else data['version']
        if package != tool['package']:
            packages.append(package + '@' + data['version'])
    relative = Path(tool['binary']).resolve().relative_to(prefix.resolve())
    checks[str(relative)] = selected
    stage = Path(tempfile.mkdtemp(prefix='.' + prefix.name + '-update-', dir=prefix.parent))
    runtime = dict(package=tool['package'], prefix=str(prefix), stage=str(stage), entries={})
    try:
        shutil.copy2(prefix / 'package.json', stage / 'package.json')
        with sqlite3.connect(tracking[0], timeout=30) as db:
            record = read(db)
            if not write(db, tracking[1], expected='tools', runtime_installs=record.get('runtime_installs', []) + [runtime]):
                raise RuntimeError('Update no longer owns this installation')
        argv = [local_npm(), '--prefix', str(stage), 'install', '--save-exact', '--include=optional',
                '--global=false', '--no-audit', '--no-fund', *packages]
        tracked_install(argv, *tracking)
        for binary, expected in checks.items():
            if version(str(stage / binary)) != expected:
                raise RuntimeError(tool['label'] + ' version check failed')
        runtime['entries'] = {name: dict(old=file_identity(prefix / name), new=file_identity(stage / name))
                              for name in ('node_modules', 'package.json', 'package-lock.json') if (stage / name).exists()}
        with sqlite3.connect(tracking[0], timeout=30) as db:
            db.execute('BEGIN IMMEDIATE')
            record = read(db)
            runtimes = [runtime if item['stage'] == str(stage) else item for item in record.get('runtime_installs', [])]
            if not write(db, tracking[1], expected='tools', runtime_installs=runtimes):
                raise RuntimeError('Update no longer owns this installation')
        for name, entry in runtime['entries'].items():
            with sqlite3.connect(tracking[0], timeout=30) as db:
                db.execute('BEGIN IMMEDIATE')
                record = read(db)
                if record.get('id') != tracking[1] or record.get('phase') != 'tools':
                    raise RuntimeError('Update no longer owns this installation')
                if file_identity(prefix / name) != entry['old'] or file_identity(stage / name) != entry['new']:
                    raise RuntimeError('Tool runtime changed during the update. Repair it on the host.')
                if entry['old'] is None:
                    (stage / name).rename(prefix / name)
                else:
                    exchange(stage / name, prefix / name)
        for binary, expected in checks.items():
            if version(str(prefix / binary)) != expected:
                raise RuntimeError(tool['label'] + ' version check failed')
        return runtime
    except BaseException:
        restore_runtime(runtime, tracking[0])
        if stage.is_dir():
            shutil.rmtree(stage)
        raise


def discard_runtimes(record):
    for runtime in record.get('runtime_installs', []):
        stage = Path(runtime['stage'])
        if stage.is_dir():
            shutil.rmtree(stage)


def restore_tools(record, guard=None, database=None):
    results = []
    restored_all = True
    for tool in reversed(record.get('attempted', [])):
        if guard and guard():
            results.append('Tool rollback stopped because another deploy took over.')
            restored_all = False
            break
        try:
            if tool.get('setting'):
                with sqlite3.connect(database, timeout=30) as db:
                    select_tool(db, tool, tool['current'])
            else:
                for runtime in reversed(record.get('runtime_installs', [])):
                    if runtime['package'] == tool['package']:
                        restore_runtime(runtime, database)
                if version(tool['binary']) != tool['current']:
                    raise RuntimeError(tool['label'] + ' version check failed')
            results.append(tool['label'] + ' restored to ' + tool['current'] + '.')
        except (RuntimeError, OSError, ValueError, sqlite3.Error):
            restored_all = False
            results.append(tool['label'] + ' restoration failed. Repair it on the host.')
    if restored_all:
        discard_runtimes(record)
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


def tools_match(record, selected, db=None):
    try:
        return all(tool_version(tool, db) == tool[selected]
                   for tool in record.get('attempted', []))
    except (RuntimeError, sqlite3.Error):
        return False


def superseded(db, record):
    head = command(['git', 'rev-parse', 'HEAD'], record['root'])
    if head not in (record['old'], record['target']):
        if not write(db, record['id'], phase='recovering'):
            return True
        stop_installation(read(db))
        record = read(db)
        database = db.execute('PRAGMA database_list').fetchone()[2]
        results = restore_tools(record, database=database)
        restored = tools_match(record, 'current', db)
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
                    record.get('attempted', []) == planned and tools_match(record, 'latest', db)):
                if write(db, update_id, phase='updated', result='Updated.'):
                    discard_runtimes(record)
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
    database = db.execute('PRAGMA database_list').fetchone()[2]
    results = restore_tools(record, database=database)
    restored = tools_match(record, 'current', db)
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
                        tools_match(current, 'current', db))
            write(db, update_id, phase='rolled_back' if restored else 'recovering',
                  interrupted_phase='rollback_restart',
                  result=failure if restored else 'Rollback could not restore every CLI. Repair it on the host.')
            return
        time.sleep(min(1, restart_timeout / 4))
    restored = command(['git', 'rev-parse', 'HEAD'], record['root']) == record['old'] and tools_match(read(db), 'current', db)
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
