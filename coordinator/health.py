"""Read-only owner health summary from saved workers and bounded host probes."""

import os
import re
import subprocess
import time

from . import host_os
from .accounts import listed_accounts
from .controls import _mapping, _timestamp, _until, _window_left, blocked, resets
from . import extension


def _command(*args):
    try:
        return subprocess.run(args, capture_output=True, text=True, timeout=1, check=True).stdout
    except (OSError, subprocess.SubprocessError):
        return ''


def _size(value):
    return f'{value / 1024 ** 3:.1f}G'


def _plain(value):
    return re.sub(r'([\\`*_\[\]~|])', r'\\\1', ' '.join(str(value).split()))


def _disk(path):
    lines = _command('df', '-kP', str(path)).splitlines()
    if len(lines) < 2:
        return None
    fields = lines[-1].split()
    try:
        total, free = int(fields[1]) * 1024, int(fields[3]) * 1024
    except (IndexError, ValueError):
        return None
    return f'{_size(free)} free / {_size(total)}'


def linux_pressure():
    lines = []
    try:
        lines.append(f'CPU: {os.getloadavg()[0]:.1f} load / {os.cpu_count()} cores')
    except OSError:
        pass
    memory = {key: int(value) * 1024 for key, value in
              re.findall(r'^(MemTotal|MemAvailable|SwapTotal|SwapFree):\s*(\d+) kB$',
                         host_os.proc_text('meminfo'), re.MULTILINE)}
    if memory.get('MemTotal') and 'MemAvailable' in memory:
        used = max(0, memory['MemTotal'] - memory['MemAvailable'])
        detail = f"RAM: {_size(used)} used / {_size(memory['MemTotal'])}"
        pressure = re.search(r'^some avg10=([\d.]+)', host_os.proc_text('pressure/memory'), re.MULTILINE)
        if pressure:
            detail += f', {pressure.group(1)}% pressure stall'
        if 'SwapTotal' in memory and 'SwapFree' in memory:
            detail += ', swap ' + _size(max(0, memory['SwapTotal'] - memory['SwapFree']))
        lines.append(detail)
    return lines


def system_pressure(state_dir):
    if host_os.linux():
        lines = linux_pressure()
        for label, path in (('Disk /: ', '/'), ('Disk state: ', state_dir)):
            disk = _disk(path)
            if disk:
                lines.append(label + disk)
        return lines
    lines = []
    cores = _command('sysctl', '-n', 'hw.ncpu').strip()
    try:
        if int(cores) > 0:
            lines.append(f'CPU: {os.getloadavg()[0]:.1f} load / {int(cores)} cores')
    except (ValueError, OSError):
        pass
    total = _command('sysctl', '-n', 'hw.memsize').strip()
    stats = _command('vm_stat')
    page = re.search(r'page size of (\d+) bytes', stats)
    pages = {key: int(value.replace('.', '')) for key, value in
             re.findall(r'^Pages (free|inactive|speculative):\s+(\d+\.?)[\s]*$', stats, re.M)}
    try:
        memory = int(total)
        if memory > 0 and page and 'free' in pages:
            available = sum(pages.get(key, 0) for key in ('free', 'inactive', 'speculative')) * int(page.group(1))
            used = max(0, memory - available)
            pressure = _command('memory_pressure', '-Q')
            free = re.search(r'System-wide memory free percentage:\s*(\d+)%', pressure)
            swap = _command('sysctl', '-n', 'vm.swapusage')
            swapped = re.search(r'used\s*=\s*([\d.]+)([KMG])', swap)
            detail = f'RAM: {_size(used)} used / {_size(memory)}'
            if free:
                detail += f', {free.group(1)}% pressure free'
            if swapped:
                amount, unit = swapped.groups()
                detail += f', swap {_size(float(amount) * 1024 ** ("KMG".index(unit) + 1))}'
            lines.append(detail)
    except ValueError:
        pass
    root = _disk('/')
    state = _disk(state_dir)
    if root:
        lines.append('Disk /: ' + root)
    if state:
        lines.append('Disk state: ' + state)
    return lines


def _subscription(store):
    from .accounts import AccountBroker, account_label, authenticated
    aliases = listed_accounts(store)[0]
    snapshots = _mapping(store.get('account_status', {}))
    entries = []
    for alias in aliases:
        if not alias:
            continue
        usage = _mapping(_mapping(snapshots.get(alias)).get('usage'))
        account_block = blocked(store, alias)
        windows = []
        for key in ('five_hour', 'seven_day', 'seven_day_fable'):
            window = _mapping(usage.get(key))
            remaining, _ = _window_left(window, account_block)
            windows.append(f'{remaining:g}%' if remaining is not None else '?')
        state = AccountBroker(store).account_state(alias)
        profile = (store.get('accounts', {}) or {}).get(alias)
        mark, state_text = (('⚪', 'not signed in') if not authenticated(store, alias, profile) else
                            ('⚫', 'disabled') if not profile.get('enabled') else
                            ('🔴', resets(state.get('until'))) if state['state'] == 'limited' else
                            ('🟡', 'stale since ' + _timestamp(state.get('since')))
                            if state['state'] == 'stale' else ('🟢', ''))
        weekly_reset = _mapping(usage.get('seven_day')).get('resets_at')
        if mark in ('🟢', '🟡') and type(weekly_reset) in (int, float) and weekly_reset > time.time():
            state_text += ', week resets in ' + _until(weekly_reset)
        entries.append(f'{mark} {_plain(account_label(store, alias))}: ' + ' · '.join(windows)
                       + (', ' + state_text if state_text else ''))
    return '\n'.join(['**Subscriptions left** (5h · week · Fable)'] + (entries or ['No accounts']))


def _codex(store):
    from .codex_accounts import CodexBroker
    broker = CodexBroker(store)
    listed, active = broker.listed()
    aliases = listed + sorted((alias for alias in broker.accounts() if alias not in listed),
                              key=lambda alias: (broker.label(alias).casefold(), alias))
    entries = []
    for alias in aliases:
        snapshot = broker.snapshot(alias)
        remaining, _ = _window_left(_mapping(_mapping(snapshot.get('usage')).get('seven_day')))
        state = broker.account_state(alias)
        mark, state_text = (('⚪', 'not signed in') if not broker.authenticated(alias) else
                            ('⚫', 'disabled') if not broker.signed_in(alias) else
                            ('🔴', resets(state.get('until'))) if state['state'] == 'limited' else
                            ('🟡', 'stale since ' + _timestamp(state.get('since')))
                            if state['state'] == 'stale' else ('🟢', ''))
        weekly_reset = _mapping(_mapping(snapshot.get('usage')).get('seven_day')).get('resets_at')
        if mark in ('🟢', '🟡') and type(weekly_reset) in (int, float) and weekly_reset > time.time():
            state_text += (', ' if state_text else '') + 'week resets in ' + _until(weekly_reset)
        banked = snapshot.get('resets_available')
        entries.append(f'{mark} {_plain(broker.label(alias))}: '
                       + (f'{remaining:g}%' if remaining is not None else '?')
                       + (', ' + state_text if state_text else '') + (' · active' if alias == active else '')
                       + (' · %d reset%s banked' % (banked, '' if banked == 1 else 's')
                          if type(banked) is int and banked > 0 else ''))
    return '\n'.join(['**ChatGPT left** (week)'] + entries) if entries else None


def format_health(topics, workers, pressure, subscription, parents=None, extension_line=None, codex=None):
    groups = {}
    for worker in workers:
        if worker['status'] in ('running', 'needs_input'):
            groups.setdefault(worker['topic'], []).append(worker)
    parents = parents or {}
    running = (sum(worker['status'] == 'running' for members in groups.values() for worker in members)
               + sum(parents.get(topic['id'], '').startswith('live') for topic in topics))
    lines = [f'**Agents running: {running}**']
    for topic in topics:
        members = groups.pop(topic['id'], [])
        active = [worker for worker in members if worker['status'] == 'running']
        needs = sum(worker['status'] == 'needs_input' for worker in members)
        parent = parents.get(topic['id'])
        count = len(active) + bool(parent and parent.startswith('live'))
        lines.append(f'**{_plain(topic["name"])}**: {count} running' + (f', {needs} needs input' if needs else '')
                     + (' · parent ' + parent if parent else ''))
        for worker in active:
            number = worker['number'] if worker['number'] is not None else '?'
            lines.append(f'- Job {number}: {_plain(worker["title"] or "Untitled job")}')
    lines.extend(['', '**System pressure**'] + (pressure or ['Unavailable']))
    lines.extend(['', subscription])
    if extension_line:
        lines.append(extension_line)
    if codex:
        lines.extend(['', codex])
    return '\n'.join(lines)


def op_health_show(store, ctx):
    from .host import host_state

    workers = [dict(row) for row in store.db.execute(
        """SELECT w.topic,w.status,t.number,t.title FROM workers w
        LEFT JOIN tasks t ON t.id=w.task WHERE w.status IN ('running','needs_input') ORDER BY w.id""")]
    topics = store.topics()
    home = store.get('coordinator_session_topic') or store.get('coordinator_home_topic')
    if home is None:
        home = next((topic['id'] for topic in topics if topic['enabled'] and store.get('coordinator_session')), None)
    parents = {}
    for topic in topics:
        key = 'coordinator' if topic['id'] == home else 'coordinator:' + topic['id']
        host = store.get(key + '_host')
        if host and host_state(store.directory / 'hosts' / host['id']) == 'running':
            if host.get('provider', 'claude') == 'codex':
                from .codex_accounts import CodexBroker
                label = CodexBroker(store).label(host.get('account'))
                parents[topic['id']] = 'live (ChatGPT %s)' % _plain(label or 'unknown')
            else:
                parents[topic['id']] = 'live (Claude)'
        elif store.get('coordinator_account_retry_at'):
            parents[topic['id']] = 'paused until ' + _timestamp(store.get('coordinator_account_retry_at'))
        else:
            parents[topic['id']] = 'wound down'
    return format_health(topics, workers, system_pressure(store.directory), _subscription(store), parents,
                         extension.active().health(store), _codex(store))
