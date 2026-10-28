"""Owner update cards and persisted, staged checkout and CLI updates."""

import asyncio
import ast
import importlib.util
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
import sys
import time
import unicodedata
import uuid

from .cli_identity import installation, PACKAGES
from .formatting import markdown_to_html
from .extension import optional


KEY = 'telegram_update_v1'
OFF = ('Updates from Telegram are off. Turn them on in /accounts → Models & Codex, '
       'or run python3 -m coordinator ctl call service.updates enabled=true on the host.')
TERMINAL = ('updated', 'failed', 'rolled_back', 'cancelled', 'current')
WATCHED = ('armed', 'merge', 'tools', 'restart', 'rolling_back', 'rollback_restart', 'recovering')
ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('torii_update_watchdog', ROOT / 'scripts/update-watchdog.py')
watch = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(watch)


def git(root, *args):
    return watch.command(['git', *args], str(root))


def checkout(root, target):
    if git(root, 'status', '--porcelain', '--untracked-files=no'):
        raise RuntimeError('Checkout is dirty. Nothing changed.')
    try:
        branch = git(root, 'symbolic-ref', '--short', 'HEAD')
        upstream = git(root, 'rev-parse', '--abbrev-ref', '@{upstream}')
    except RuntimeError:
        raise RuntimeError('Checkout must be on its tracked main branch. Nothing changed.') from None
    if branch != 'main' or upstream != 'origin/main':
        raise RuntimeError('Checkout must be on its tracked main branch. Nothing changed.')
    old = git(root, 'rev-parse', 'HEAD')
    ahead, behind = map(int, git(root, 'rev-list', '--left-right', '--count', old + '...' + target).split())
    if ahead and behind:
        raise RuntimeError('Checkout has diverged from origin/main. Nothing changed.')
    if ahead:
        raise RuntimeError('Checkout is ahead of origin/main. Nothing changed.')
    return old


def newer(current, latest):
    def key(value):
        match = re.fullmatch(r'(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)'
                             r'(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?'
                             r'(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?', value)
        if not match:
            return None
        major, minor, patch, pre = match.groups()
        if pre and any(part.isdigit() and len(part) > 1 and part.startswith('0') for part in pre.split('.')):
            return None
        identifiers = tuple((0, int(part)) if part.isdigit() else (1, part) for part in (pre or '').split('.'))
        return (int(major), int(minor), int(patch), (0, identifiers) if pre else (1,))
    a, b = key(current), key(latest)
    return bool(a is not None and b is not None and b > a)


def pinned_version(root, target):
    try:
        source = ast.parse(git(root, 'show', target + ':coordinator/opencode_accounts.py'))
        pins = [ast.literal_eval(node.value) for node in source.body if isinstance(node, ast.Assign)
                and any(isinstance(name, ast.Name) and name.id == 'PINNED_VERSION' for name in node.targets)]
        if len(pins) == 1 and isinstance(pins[0], str) and re.fullmatch(r'\d+\.\d+\.\d+(?:-[\w.-]+)?', pins[0]):
            return pins[0]
    except (RuntimeError, SyntaxError, ValueError, TypeError):
        pass
    raise RuntimeError('OpenCode supported version could not be read. Nothing changed.')


def inventory(root, binaries, opencode_binary=None):
    git(root, 'fetch', 'origin', 'main')
    old = git(root, 'rev-parse', 'HEAD')
    target = git(root, 'rev-parse', 'origin/main')
    checkout(root, target)
    subjects = []
    for entry in git(root, 'log', '--first-parent', '--format=%s%x00%b%x1e', old + '..' + target).split('\x1e'):
        entry = entry.strip('\n')
        if not entry:
            continue
        subject, _, body = entry.partition('\x00')
        merge = re.fullmatch(r'Merge pull request #(\d+) from .+', subject)
        title = next((line.strip() for line in body.splitlines() if line.strip()), None)
        subjects.append(title + ' (#' + merge.group(1) + ')' if merge and title else subject)
    tools = []
    for name, label in (('claude', 'Claude Code'), ('codex', 'Codex')):
        binary, package_version, prefix = installation(binaries.get(name, name), PACKAGES[name])
        if not binary:
            continue
        current = watch.version(binary)
        latest = (watch.command([watch.local_npm(), 'view', PACKAGES[name] + '@latest', 'version'], timeout=30)
                  if prefix else current)
        if not re.fullmatch(r'\d+\.\d+\.\d+(?:-[\w.-]+)?', latest):
            raise RuntimeError(label + ' latest version could not be read. Nothing changed.')
        if prefix and package_version != current:
            raise RuntimeError(label + ' installation and version disagree. Nothing changed.')
        tools.append(dict(label=label, package=PACKAGES[name], binary=binary, prefix=prefix,
                          current=current, latest=latest, outdated=newer(current, latest)))
    if opencode_binary:
        latest = pinned_version(root, target)
        paths = {}
        try:
            current = watch.version(opencode_binary, exact=True)
            binary, _, prefix = installation(opencode_binary, 'opencode-ai', current)
            if prefix:
                destination = Path(prefix).parent / latest
                relative = Path(binary).resolve().relative_to(Path(prefix))
                paths = {current: dict(prefix=prefix, binary=opencode_binary),
                         latest: dict(prefix=str(destination), binary=str(destination / relative))}
        except (OSError, RuntimeError, ValueError):
            current, prefix = None, None
        tool = dict(label='OpenCode', package='opencode-ai', binary=opencode_binary, prefix=prefix,
                    current=current, latest=latest, outdated=bool(current and current != latest),
                    setting='opencode_binary', paths=paths, note='',
                    unmanaged=" · not installed in Torii's OpenCode folder" +
                    ('; update on the host.' if current != latest else '.'))
        if current:
            try:
                released = watch.command([watch.local_npm(), '--prefix', prefix or str(Path(opencode_binary).parent),
                                          'view', 'opencode-ai@latest', 'version'], timeout=30)
                if newer(latest, released):
                    tool['note'] = ' · ' + released + ' is out but not tested with Torii yet.'
            except RuntimeError:
                pass
        tools.append(tool)
    return dict(old=old, target=target, subjects=subjects, tools=tools)


def available(plan):
    return bool(plan['subjects'] or any(tool['outdated'] and tool['prefix'] for tool in plan['tools']))


def card(plan):
    tools = [tool for tool in plan['tools'] if tool['outdated'] and tool['prefix']]
    torii = bool(plan['subjects'])
    if not available(plan):
        title = 'Torii and its npm tools are current.'
    elif torii and tools:
        title = 'Update Torii and %d %s?' % (len(tools), 'tool' if len(tools) == 1 else 'tools')
    elif torii:
        title = 'Update Torii?'
    else:
        title = 'Update %d %s?' % (len(tools), 'tool' if len(tools) == 1 else 'tools')
    lines = [title]
    if torii:
        lines += ['Torii ' + plan['old'][:7] + ' → ' + plan['target'][:7]]
        for subject in plan['subjects'][:10]:
            subject = subject[:220]
            while len(markdown_to_html('• ' + subject)) > 240:
                subject = subject[:-1]
            lines.append('• ' + subject)
        if len(plan['subjects']) > 10:
            lines.append('and %d more' % (len(plan['subjects']) - 10))
    for tool in plan['tools']:
        if tool['current'] is None:
            lines.append(tool['label'] + ' · cannot read its binary at the saved path; left out of this update.')
            continue
        line = tool['label'] + ' ' + tool['current']
        if tool['outdated']:
            line += ' → ' + tool['latest']
            if not tool['prefix']:
                line += tool.get('unmanaged', ' · not installed through npm; update on the host.')
        elif not tool['prefix']:
            line += tool.get('unmanaged', ' · not installed through npm.')
        if tool['outdated'] or not tool['prefix'] or tool.get('note') or tool.get('setting'):
            lines.append(line + tool.get('note', ''))
    if available(plan):
        lines += ['Running chats and workers keep going.']
    return '\n'.join(lines)


def request(store, topic, message):
    if store.get('telegram_updates_enabled', False) is not True:
        store.enqueue_report(topic, OFF, kind='status', reply_to=message)
        return
    recover(store)
    prior = store.get(KEY) or {}
    if prior and prior['phase'] not in TERMINAL and prior['phase'] != 'offer':
        store.enqueue_report(topic, 'An update is already in progress.', kind='status', reply_to=message)
        return
    outbox = store.enqueue_report(topic, 'Checking for Torii and tool updates…', kind='status', reply_to=message)
    store.put(KEY, dict(id=uuid.uuid4().hex, phase='checking', topic=topic, outbox=outbox))


def accept_action(store, topic, message, action):
    record = store.get(KEY) or {}
    if record.get('id') != action['id'] or record.get('topic') != topic or record.get('phase') != 'offer':
        return 'stale_callback'
    if store.get('telegram_updates_enabled', False) is not True:
        record.update(phase='cancelled', result=OFF)
    elif action['action'] == 'cancel':
        record.update(phase='cancelled', result='Update cancelled. Nothing changed.')
    else:
        record.update(phase='queued', result='Waiting for queued Telegram messages to be sent…')
    record['message'] = message
    record['outbox'] = store.enqueue_report(topic, record['result'], kind='status', edit=message,
                                           reply_markup={'inline_keyboard': []})
    store.put(KEY, record)
    return 'control_callback'


def accept_reply(store, topic, message):
    if any(field in message for field in ('forward_origin', 'forward_from', 'forward_from_chat',
                                          'forward_sender_name', 'forward_date', 'via_bot', 'business_connection_id')):
        return None
    text = message.get('text', '').casefold()
    while text and (text[0].isspace() or unicodedata.category(text[0]).startswith('P')):
        text = text[1:]
    while text and (text[-1].isspace() or unicodedata.category(text[-1]).startswith('P')):
        text = text[:-1]
    if text != 'yes':
        return None
    record = store.get(KEY) or {}
    reply = message.get('reply_to_message', {}).get('message_id')
    if not reply or record.get('phase') != 'offer':
        return None
    card = store.db.execute('SELECT telegram_message FROM outbox WHERE id=? AND delivered=1',
                            (record.get('outbox'),)).fetchone()
    if not card or reply != card['telegram_message']:
        return None
    if (store.get('control_ui:' + topic) or {}).get('outbox') != record.get('outbox'):
        return None
    outcome = accept_action(store, topic, reply, dict(id=record['id'], action='confirm'))
    return outcome if outcome == 'control_callback' else None


def prepare(record):
    root = record['root']
    git(root, 'fetch', 'origin', 'main')
    if git(root, 'rev-parse', 'origin/main') != record['target']:
        raise RuntimeError('origin/main changed since this card. Run /update again. Nothing changed.')
    if checkout(root, record['target']) != record['old']:
        raise RuntimeError('Checkout changed since this card. Nothing changed.')


def start_watchdog(record, state):
    directory = state / 'update-watchdogs' / record['id']
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    script = directory / 'watchdog.py'
    for source, name in ((ROOT / 'scripts/update-watchdog.py', 'watchdog.py'),
                         (ROOT / 'coordinator/host_os.py', 'host_os.py'),
                         (ROOT / 'coordinator/scrub.py', 'scrub.py')):
        temporary = directory / (name + '.tmp')
        shutil.copyfile(source, temporary)
        temporary.chmod(0o600)
        temporary.replace(directory / name)
    with (directory / 'watchdog.log').open('a') as output:
        process = subprocess.Popen([sys.executable, str(script), str(state / 'state.sqlite'), record['id']],
                                   cwd=directory, stdin=subprocess.DEVNULL, stdout=output, stderr=output,
                                   start_new_session=True, close_fds=True)
    return process.pid


def outcome(record):
    lines = [record.get('result', 'Updated.')]
    if record['phase'] == 'updated':
        if record['old'] != record['target']:
            lines.append('Torii ' + record['old'][:7] + ' → ' + record['target'][:7])
        lines += [tool['label'] + ' ' + tool['current'] + ' → ' + tool['latest']
                  for tool in record.get('attempted', [])]
    lines += record.get('restored', [])
    return '\n'.join(lines)


def recovery_facts(record, commit=None, process=None, database=None):
    phase = record.get('interrupted_phase', record['phase'])
    facts = []
    versions = []
    try:
        head = git(record['root'], 'rev-parse', 'HEAD')
        facts.append('Torii checkout is at ' + head[:7] + '.')
    except (RuntimeError, KeyError):
        head = None
        facts.append('Torii checkout could not be read.')
    health = record.get('health') or {}
    running = commit or (health.get('commit') if watch.alive(health.get('pid'), health.get('process')) else None)
    if running:
        facts.append('Running Torii is at ' + running[:7] + '.')
    for tool in record.get('attempted', []):
        try:
            with sqlite3.connect(database or ':memory:', timeout=30) as db:
                actual = watch.tool_version(tool, db)
        except (RuntimeError, sqlite3.Error):
            actual = None
        versions.append(actual)
        facts.append(tool['label'] + ' is at ' + (actual or 'an unreadable version') + '.')
    tools = record.get('attempted', [])
    identity = (os.getpid(), process) if commit and process else (health.get('pid'), health.get('process'))
    before = record.get('restart_from') or {}
    restarted = bool(record.get('restart_started') and running and identity[1] and
                     identity != (record.get('old_pid'), record.get('old_process')) and
                     identity != (before.get('pid'), before.get('process')) and
                     (commit or health.get('restart_started') == record['restart_started']))
    if running and identity[1]:
        facts.append('The service restarted after this update step.' if restarted
                     else 'The service has not restarted after this update step.')
    selected = 'failed'
    result = 'Update interrupted: its watchdog is no longer running.'
    healthy = bool(running and watch.new_health(record, running))
    if restarted and healthy and head == record['target'] and running == record['target'] and all(
            actual == tool['latest'] for tool, actual in zip(tools, versions)) and tools == [
                tool for tool in record.get('tools', []) if tool['outdated'] and tool['prefix']]:
        selected, result = 'updated', 'Updated. Recovered after the watchdog exited.'
    elif restarted and healthy and phase in ('rolling_back', 'rollback_restart') and head == record['old'] and running == record['old'] and all(
            actual == tool['current'] for tool, actual in zip(tools, versions)):
        selected, result = 'rolled_back', 'Update failed. Previous versions are running.'
    if selected == 'failed':
        if not healthy:
            facts.append('A healthy poll after this update step is unverified.')
        if head == record['old']:
            facts.append('Torii code is restored to ' + head[:7] + '.')
        elif head == record['target']:
            facts.append('Torii code remains changed at ' + head[:7] + '; previous commit was ' + record['old'][:7] + '.')
        else:
            facts.append('Torii code was deployed elsewhere.' if head else 'Torii code state is unknown.')
        for tool, actual in zip(tools, versions):
            if actual == tool['current']:
                facts.append(tool['label'] + ' is restored to ' + actual + '.')
            elif actual:
                facts.append(tool['label'] + ' remains changed at ' + actual + '; previous version was ' + tool['current'] + '.')
            else:
                facts.append(tool['label'] + ' restoration is unverified; previous version was ' + tool['current'] + '.')
    return dict(phase=selected, result=result, restored=facts)


def recover_tools(record, commit, process, database):
    fields = recovery_facts(record, commit, process, database)
    tools = [tool for tool in record.get('attempted', []) if tool.get('setting') or
             any(item['package'] == tool['package'] for item in record.get('runtime_installs', []))]
    previous = (watch.alive(record.get('old_pid'), record.get('old_process')) or
                (commit == record['old'] and record['old'] != record['target']))
    if fields['phase'] == 'failed' and tools and previous:
        restored = watch.restore_tools(dict(record, attempted=tools), database=database)
        fields = recovery_facts(record, commit, process, database)
        fields['restored'] += restored
    return fields


def claim_recovery(store, record):
    if watch.write(store.db, record['id'], expected=record['phase'], phase='recovering',
                   interrupted_phase=record.get('interrupted_phase', record['phase'])):
        return store.get(KEY)
    return None


def recover(store, commit=None, process=None, observed=None):
    record = observed[0] if observed is not None else (store.get(KEY) or {})
    phase = record.get('phase')
    if phase not in WATCHED and not (phase == 'arming' and record.get('old_pid')):
        return False
    if observed is None and watch.watchdog_alive(record):
        return False
    if observed is None:
        record = claim_recovery(store, record)
        if not record:
            return False
        watch.stop_installation(record)
        phase = 'recovering'
    fields = observed[1] if observed is not None else recover_tools(record, commit, process, str(store.directory / 'state.sqlite'))
    selected = fields['phase']
    with store.db:
        if not watch.write(store.db, record['id'], expected=phase, **fields):
            return False
        current = store.get(KEY)
        outbox = store.enqueue_report(current['topic'], outcome(current), kind='status',
                                      edit=current.get('message'), reply_markup={'inline_keyboard': []})
        watch.write(store.db, current['id'], expected=selected, outbox=outbox, reported=True)
        request = store.get('restart_requested_v2') or {}
        if request.get('update_id') == record['id']:
            store.put('restart_requested_v2', None)
    return True


class Updater:
    def __init__(self, service):
        self.service = service
        self.store = service.store
        freeze = optional('update_freeze')
        self.freeze = freeze.UpdateFreeze(service) if freeze is not None else None
        self.root = ROOT
        self.commit = None
        self.process = None
        self.update_id = None
        self.waits_started = False
        self.waits_update = None
        record = self.store.get(KEY) or {}
        try:
            self.commit = git(self.root, 'rev-parse', 'HEAD')
        except RuntimeError:
            self.commit = None
        pending = self.store.get('restart_requested_v2') or {}
        if record.get('id') and pending.get('update_id') == record['id'] and record.get('old_pid') != os.getpid():
            with self.store.db:
                self.store.put('restart_requested_v2', None)

    def save(self, **fields):
        with self.store.db:
            record = self.store.get(KEY)
            if self.update_id and record['id'] != self.update_id:
                return record
            watch.write(self.store.db, record['id'], expected=record['phase'], **fields)
        saved = watch.read(self.store.db)
        if self.freeze is not None and fields.get('phase') == 'offer':
            self.service.wake('update_freeze').set()
        return saved

    async def acknowledge_freeze(self, record):
        if self.freeze is None:
            return True
        result = await self.freeze.before_update(record['id'], on_wait=lambda: self.show(
            record['phase'], 'Waiting for the merge queue to finish landing a change…'))
        current = self.store.get(KEY) or {}
        if current.get('id') != record['id'] or current.get('phase') != record['phase']:
            return False
        if result:
            self.save(launcher_freeze_ack=result)
        return True

    def show(self, phase, text, **fields):
        with self.store.db:
            record = self.store.get(KEY)
            if self.update_id and record['id'] != self.update_id:
                return
            if record['phase'] in TERMINAL and record['phase'] != phase:
                return
            outbox = self.store.enqueue_report(record['topic'], text, kind='status', edit=record.get('message'),
                                              reply_markup={'inline_keyboard': []})
            self.save(phase=phase, outbox=outbox, **fields)
        self.service.wake('deliver').set()

    def healthy(self, process=None):
        record = self.store.get(KEY) or {}
        if record.get('phase') in WATCHED and self.commit:
            self.save(health=dict(commit=self.commit, pid=os.getpid(), process=process or self.process,
                                  restart_started=record.get('restart_started')))

    async def waits_changed(self, started=False, armed=False):
        try:
            if not self.store.get('waits_enabled', False):
                return
            from .waits import update_landed
            record = self.store.get(KEY) or {}
            update = record.get('id') if record.get('phase') == 'updated' else None
            if armed:
                await update_landed(self, armed=True)
            elif started and not self.waits_started or update and update != self.waits_update:
                await update_landed(self)
                self.waits_started = self.waits_started or started
                self.waits_update = update
        except Exception as error:
            from . import problems
            problems.record(self.store, 'waits', 'update-hook-failed', type(error).__name__)

    async def once(self):
        await self.waits_changed()
        record = self.store.get(KEY) or {}
        self.update_id = record.get('id')
        if record.get('phase') in WATCHED or (record.get('phase') == 'arming' and record.get('old_pid')):
            self.process = await asyncio.to_thread(watch.process_identity, os.getpid())
            if await asyncio.to_thread(watch.watchdog_alive, record) is False:
                record = claim_recovery(self.store, record)
                if not record:
                    return False
                await asyncio.to_thread(watch.stop_installation, record)
                fields = await asyncio.to_thread(recover_tools, record, self.commit, self.process,
                                                 str(self.store.directory / 'state.sqlite'))
                if recover(self.store, self.commit, self.process, observed=(record, fields)):
                    await self.waits_changed()
                    self.service.wake('deliver').set()
                    return True
        phase = record.get('phase')
        if not phase or record.get('reported') or phase == 'offer':
            return False
        row = self.store.db.execute('SELECT delivered,telegram_message FROM outbox WHERE id=?',
                                    (record.get('outbox'),)).fetchone()
        if row and not row['delivered']:
            return False
        if not record.get('message') and row:
            record = self.save(message=row['telegram_message'])
        if phase in TERMINAL:
            if phase in ('updated', 'rolled_back', 'failed'):
                self.show(phase, outcome(record), reported=True)
            else:
                self.save(reported=True)
            return True
        if record.get('old_pid') and (record['old_pid'], record.get('old_process')) != (os.getpid(), self.process):
            if phase in ('arming', 'armed') and not record.get('watchdog_armed'):
                self.show('failed', 'Update interrupted before the watchdog armed. Nothing changed.',
                          result='Update interrupted before the watchdog armed. Nothing changed.', reported=True)
                return True
            return False
        try:
            if phase in ('checks', 'arming', 'armed', 'merge', 'tools'):
                if not await self.acknowledge_freeze(record):
                    return False
            if phase == 'checking':
                binaries = getattr(self.service.runner, 'binaries', {})
                plan = await asyncio.to_thread(inventory, self.root, binaries, self.store.get('opencode_binary'))
                from .control_ui import emit
                with self.store.db:
                    rows = [[('Update', {'update': dict(id=record['id'], action='confirm')}),
                             ('Cancel', {'update': dict(id=record['id'], action='cancel')})]] if available(plan) else []
                    emit(self.store, record['topic'], card(plan), rows, edit_message=record['message'])
                    outbox = self.store.get('control_ui:' + record['topic'])['outbox']
                    self.save(phase='offer' if rows else 'current', root=str(self.root), outbox=outbox, **plan)
            elif phase == 'queued':
                if self.store.outbox_has_pending():
                    return False
                from .service import RESTART_GUARD_SECONDS
                last = self.store.get('last_restart_v2')
                if last is not None and time.time() - last < RESTART_GUARD_SECONDS:
                    raise RuntimeError('Torii restarted recently. Try again later. Nothing changed.')
                if self.store.get('restart_requested_v2'):
                    raise RuntimeError('A restart is already pending. Try /update after it finishes. Nothing changed.')
                if not await self.acknowledge_freeze(record):
                    return False
                if record['old'] == record['target']:
                    self.show('arming', 'Preparing tool updates…')
                else:
                    self.show('checks', 'Fetching Torii…')
            elif phase == 'checks':
                await asyncio.to_thread(prepare, record)
                self.show('arming', 'Preparing the update…')
            elif phase == 'arming':
                if self.store.get('restart_requested_v2'):
                    raise RuntimeError('Another restart is pending. Nothing changed.')
                if await asyncio.to_thread(git, self.root, 'rev-parse', 'HEAD') != record['old']:
                    raise RuntimeError('Checkout changed since this card. Nothing changed.')
                for prefix in {Path(tool['prefix']) for tool in record['tools'] if tool['prefix'] and not tool.get('setting')}:
                    for stage in prefix.parent.glob('.' + prefix.name + '-update-*'):
                        if stage.is_dir() and not stage.is_symlink():
                            await asyncio.to_thread(shutil.rmtree, stage)
                old_process = await asyncio.to_thread(watch.process_identity, os.getpid())
                if not old_process:
                    raise RuntimeError('Could not identify the service process. Nothing changed.')
                record = self.save(old_pid=os.getpid(), old_process=old_process, attempted=[], health=None)
                self.process = old_process
                pid = await asyncio.to_thread(start_watchdog, record, self.store.directory)
                stamp = await asyncio.to_thread(watch.process_identity, pid)
                self.save(phase='armed', watchdog_pid=pid, watchdog_process=stamp, armed_at=time.time())
            elif phase == 'armed':
                if not record.get('watchdog_armed'):
                    if time.time() - record['armed_at'] > 15:
                        raise RuntimeError('Watchdog did not arm. Nothing changed.')
                    return False
                self.show('merge', 'Fast-forwarding Torii…' if record['old'] != record['target'] else 'Updating tools…')
            elif phase == 'merge':
                if record['old'] != record['target']:
                    if await asyncio.to_thread(checkout, self.root, record['target']) != record['old']:
                        raise RuntimeError('Checkout changed since verification. Nothing changed.')
                    await asyncio.to_thread(git, self.root, 'merge', '--ff-only', record['target'])
                self.show('tools', 'Updating agent CLIs…')
            elif phase == 'tools':
                tools = [tool for tool in record['tools'] if tool['outdated'] and tool['prefix']]
                remaining = tools[len(record['attempted']):]
                if remaining:
                    tool = remaining[0]
                    args = (tool['current'],) if tool.get('paths') else ()
                    if tool.get('setting') and self.store.get(tool['setting']) != tool['binary']:
                        raise RuntimeError(tool['label'] + ' changed since this card. Update refused.')
                    binary, installed_version, prefix = await asyncio.to_thread(installation, tool['binary'], tool['package'], *args)
                    if binary != tool['binary'] or prefix != tool['prefix'] or installed_version != tool['current']:
                        raise RuntimeError(tool['label'] + ' installation changed since this card. Update refused.')
                    version_args = dict(exact=True) if tool.get('paths') else {}
                    if await asyncio.to_thread(watch.version, tool['binary'], **version_args) != tool['current']:
                        raise RuntimeError(tool['label'] + ' changed since this card. Update refused.')
                    record = self.save(attempted=record['attempted'] + [tool])
                    self.show('tools', 'Installing ' + tool['label'] + '… Downloads can take several minutes.')
                    await asyncio.to_thread(watch.install, tool, tool['latest'],
                                            (str(self.store.directory / 'state.sqlite'), record['id']))
                    self.show('tools', tool['label'] + ' updated. Checking the next step…')
                else:
                    for tool in record['attempted']:
                        if tool.get('setting') and await asyncio.to_thread(
                                watch.version, watch.tool_path(tool, tool['latest'])['binary'], exact=True) != tool['latest']:
                            raise RuntimeError(tool['label'] + ' version check failed')
                    with self.store.db:
                        if not self.store.db.in_transaction:
                            self.store.db.execute('BEGIN IMMEDIATE')
                        current = self.store.get(KEY)
                        if current.get('id') != record['id'] or current.get('phase') != 'tools':
                            return False
                        for tool in record['attempted']:
                            if tool.get('setting'):
                                if self.store.get(tool['setting']) != tool['binary']:
                                    raise RuntimeError(tool['label'] + ' changed since this card. Update refused.')
                                self.store.put(tool['setting'], watch.tool_path(tool, tool['latest'])['binary'])
                        self.store.put('restart_requested_v2', dict(reason='Telegram update', created=time.time(),
                                                                   update_id=record['id']))
                        self.show('restart', 'Restarting Torii and waiting for a healthy Telegram poll…')
            elif phase in ('restart', 'rolling_back', 'rollback_restart'):
                return False
        except (RuntimeError, OSError, ValueError, sqlite3.Error, subprocess.SubprocessError) as error:
            record = self.store.get(KEY)
            if record.get('phase') in TERMINAL:
                return True
            failure = watch.scrub.sanitize(str(error), limit=None)
            try:
                log = self.store.directory / 'update-watchdogs' / record['id'] / 'watchdog.log'
                log.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                with log.open('a') as output:
                    print(failure, file=output)
            except OSError:
                pass
            if record.get('watchdog_armed'):
                head = await asyncio.to_thread(git, record['root'], 'rev-parse', 'HEAD')
                if head not in (record['old'], record['target']):
                    self.show('failed', 'Update stood down: another deploy took over. No rollback was attempted.',
                              result='Update stood down: another deploy took over. No rollback was attempted.',
                              reported=True)
                    return True
            guard = None
            if record.get('watchdog_armed'):
                guard = lambda: git(record['root'], 'rev-parse', 'HEAD') not in (record['old'], record['target'])
            restored = await asyncio.to_thread(watch.restore_tools, record, guard=guard,
                                             database=str(self.store.directory / 'state.sqlite'))
            if record.get('old') and record.get('watchdog_armed'):
                try:
                    restored.append(await asyncio.to_thread(watch.rollback_code, record))
                except RuntimeError as rollback_error:
                    restored.append(str(rollback_error))
            self.show('failed', 'Update failed: ' + failure + '\n' + '\n'.join(restored),
                      result='Update failed: ' + failure, restored=restored, reported=True)
        self.service.wake('deliver').set()
        return True

    async def run(self):
        while True:
            await self.once()
            await asyncio.sleep(0.5)
