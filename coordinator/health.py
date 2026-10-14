"""Read-only owner health summary from saved workers and bounded host probes."""

import math
import os
from pathlib import Path
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
    except (OSError, subprocess.SubprocessError, UnicodeError):
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


def _number(value, convert=float):
    try:
        result = convert(value)
        return result if result >= 0 and math.isfinite(result) else None
    except (TypeError, ValueError, OverflowError):
        return None


def read_pressure():
    """Numeric host values. Unavailable readings stay None, independently of other probes."""
    result = dict.fromkeys(('load1', 'cores', 'memory_used', 'memory_total', 'swap_used', 'pressure'))
    try:
        result['load1'] = _number(os.getloadavg()[0])
    except OSError:
        pass
    if host_os.linux():
        cores = _number(os.cpu_count(), int)
        memory = {key: _number(value, int) for key, value in re.findall(
            r'^(MemTotal|MemAvailable|SwapTotal|SwapFree):\s*(\d+) kB$', host_os.proc_text('meminfo'), re.M)}
        total = memory.get('MemTotal')
        result['memory_total'] = total * 1024 if total else None
        if total and memory.get('MemAvailable') is not None:
            result['memory_used'] = max(0, total - memory['MemAvailable']) * 1024
        if memory.get('SwapTotal') is not None and memory.get('SwapFree') is not None:
            result['swap_used'] = max(0, memory['SwapTotal'] - memory['SwapFree']) * 1024
        pressure = re.search(r'^some avg10=(\d+(?:\.\d+)?)(?:\s|$)',
                             host_os.proc_text('pressure/memory'), re.M)
    else:
        cores = _number(_command('sysctl', '-n', 'hw.ncpu').strip(), int)
        total = _number(_command('sysctl', '-n', 'hw.memsize').strip(), int)
        result['memory_total'] = total if total else None
        stats = _command('vm_stat')
        page = re.search(r'page size of (\d+) bytes', stats)
        page_size = _number(page.group(1), int) if page else None
        pages = {key: _number(value.rstrip('.'), int) for key, value in
                 re.findall(r'^Pages (free|inactive|speculative):\s+(\d+\.?)[\s]*$', stats, re.M)}
        if total and page_size and pages.get('free') is not None:
            available = sum(pages.get(key) or 0 for key in ('free', 'inactive', 'speculative')) * page_size
            result['memory_used'] = max(0, total - available)
        pressure = re.search(r'System-wide memory free percentage:\s*(\d+)%', _command('memory_pressure', '-Q'))
        swapped = re.search(r'used\s*=\s*(\d+(?:\.\d+)?)([KMG])(?:\s|$)',
                            _command('sysctl', '-n', 'vm.swapusage'))
        if swapped:
            amount = _number(swapped.group(1))
            if amount is not None:
                result['swap_used'] = _number(amount * 1024 ** ('KMG'.index(swapped.group(2)) + 1), int)
    result['cores'] = cores if cores else None
    if pressure:
        value = _number(pressure.group(1))
        result['pressure'] = value if value is not None and value <= 100 else None
    return result


BROWSER = re.compile(r'(?:google chrome|chrome|chromium)(?: helper(?: \([^()]+\))?)?'
                     r'|chromium-browser|chrome-headless-shell|firefox|firefox-bin'
                     r'|com\.apple\.webkit\.webcontent|webkitwebprocess|webkit webcontent')


def read_browser_load():
    """Count browser executables and RSS bytes without reading arguments or environments."""
    output = _command('ps', '-eo' if host_os.linux() else '-axo', 'rss=,comm=')
    count, rss, readable = 0, 0, False
    for line in output.splitlines():
        fields = line.split(maxsplit=1)
        if len(fields) != 2:
            continue
        size = _number(fields[0], int)
        if size is None:
            continue
        readable = True
        name = Path(fields[1]).name.casefold()
        if BROWSER.fullmatch(name) or (host_os.linux() and name in ('chrome-headless', 'webkitwebproces')):
            count += 1
            rss += size * 1024
    return {'browser_count': count if readable else None, 'browser_rss': rss if readable else None}


def system_pressure(state_dir):
    values = read_pressure()
    lines = []
    if values['load1'] is not None and values['cores'] is not None:
        lines.append(f'CPU: {values["load1"]:.1f} load / {values["cores"]} cores')
    if values['memory_used'] is not None and values['memory_total'] is not None:
        detail = f'RAM: {_size(values["memory_used"])} used / {_size(values["memory_total"])}'
        if values['pressure'] is not None:
            detail += (f', {values["pressure"]:.2f}% pressure stall' if host_os.linux() else
                       f', {values["pressure"]:g}% pressure free')
        if values['swap_used'] is not None:
            detail += ', swap ' + _size(values['swap_used'])
        lines.append(detail)
    for label, path in (('Disk /: ', '/'), ('Disk state: ', state_dir)):
        disk = _disk(path)
        if disk:
            lines.append(label + disk)
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
