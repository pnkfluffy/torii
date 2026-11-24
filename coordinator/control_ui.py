"""Persisted Telegram menus. All mutations use the existing owner controls."""

import secrets
import logging
import re
import time

from .extension import optional


CANCEL_DATA = re.compile(r'envelope:(\d{1,18}):cancel\Z')
AGAIN_DATA = re.compile(r'envelope:(\d{1,18}):again\Z')
FILL_DATA = re.compile(r'envelope:(\d{1,18}):fill\Z')
NO_ARGUMENTS = 'Commands take no arguments now. Use the buttons below.'
CARD_LIMIT = 4000
PAGE = 8
ACCOUNT_PAGE = 12
BUTTON = 40


def _key(topic):
    return 'control_ui:' + topic


def _command(label, command):
    return label, {'command': command}


def _view(label, view):
    return label, {'view': view}


def _input(label, kind):
    return label, {'input': kind}


def _operation(label, op, page, **params):
    return label, {'op': op, 'params': params, 'page': page}


def _back_row(name):
    """The one way back, to the parent page. Top cards have none."""
    if name.startswith('model:') or name == 'policy':
        return [_view('Back', 'settings')]
    if name == 'settings' or name.startswith(('account:', 'remove:')):
        return [_view('Back', 'accounts:0')]
    if name == 'project':
        return [_command('Back', '/projects')]
    if name == 'jobs':
        return [_view('Back', 'project')]
    if name.startswith('secret:'):
        return [_view('Back', 'secrets:0')]
    if name.startswith('revoke:'):
        return [_view('Back', 'secret:' + name.split(':', 1)[1])]
    return None


def emit(store, topic, text, rows, reply_to=None, pending=None, where=None, in_place=False, edit_message=None):
    """Show a card. It replaces the delivered card when the owner tapped it or `in_place` answers its form."""
    token = secrets.token_hex(8)
    actions = []
    keyboard = []
    for row in rows:
        buttons = []
        for label, action in row:
            if 'url' in action:
                buttons.append({'text': label, 'url': action['url']})
                continue
            index = len(actions)
            actions.append(action)
            buttons.append({'text': label, 'callback_data': f'torii:{token}:{index}'})
        keyboard.append(buttons)
    previous = (store.get(_key(topic), {}) or {}).get('outbox')
    card = store.db.execute('SELECT telegram_message FROM outbox WHERE id=? AND delivered=1',
                            (previous,)).fetchone() if previous else None
    edit = edit_message or (card['telegram_message'] if card and (in_place or card['telegram_message'] == reply_to) else None)
    outbox = store.enqueue_report(topic, text, kind='status', reply_to=reply_to, edit=edit,
                                 reply_markup={'inline_keyboard': keyboard} if keyboard else None)
    store.put(_key(topic), {'token': token, 'actions': actions, 'pending': pending,
                            'outbox': outbox, 'where': where})


def clip(text, limit=300):
    """Shorten text for a card. The caller stores the full text."""
    return text if len(text) <= limit else text[:limit - 1].rstrip() + '…'


def _fit(head, items, tail, limit=CARD_LIMIT):
    """Join a card, dropping list lines from the end so a cut never lands mid-line."""
    shown = len(items)
    while True:
        more = ['…and %d more.' % (len(items) - shown)] if shown < len(items) else []
        text = '\n'.join(head + items[:shown] + more + tail)
        if len(text) <= limit or not shown:
            return clip(text, limit)
        shown -= 1


def _button(template, name):
    """Fill a button label, shortening the name so the whole label stays within Telegram's comfortable width."""
    room = BUTTON - len(template % '')
    return template % (name if len(name) <= room else name[:max(room - 1, 1)] + '…')


def _plain(value):
    from .health import _plain
    return _plain(value)


def _date(value):
    return time.strftime('%Y-%m-%d', time.gmtime(value))


def setup_report(store, topic, text, reply_to=None, in_place=False):
    from .onboarding import _key as setup_key, _can_bind
    state = store.get(setup_key(topic), {}) or {}
    rows = []
    if _can_bind(store, topic):
        stage = state.get('stage')
        if stage == 'first':
            name = state['suggestion']
            rows = [[_command("Use '" + name + "'", '/setup new ' + name)],
                    [_input('Type a name', 'first_project_name')]]
        elif stage == 'choose':
            rows = [[_command('New project', '/setup new')], [_command('Existing project', '/setup existing')]]
        elif stage == 'existing':
            page = state.get('page', 0)
            choices = state.get('choices', [])
            rows = [[_command((str(index) + '. ' + (entry['name'] or entry['path']))[:60], '/setup use ' + str(index))]
                    for index, entry in enumerate(choices[page * 8:(page + 1) * 8], page * 8 + 1)]
            navigation = []
            if page:
                navigation.append(_command('Previous projects', '/setup page ' + str(page - 1)))
            if (page + 1) * 8 < len(choices):
                navigation.append(_command('Next projects', '/setup page ' + str(page + 1)))
            if navigation:
                rows.append(navigation)
            if not choices:
                rows.append([_command('New project', '/setup new')])
        if stage in ('new', 'existing'):
            rows.append([_command('Back', '/setup back'), _command('Cancel', '/setup cancel')])
        elif not stage:
            rows.append([_command('Start setup', '/setup')])
    rows.append([_view('Accounts', 'accounts:0'), _command('Projects', '/projects')])
    if state.get('stage') == 'first':
        previous = (store.get(_key(topic), {}) or {}).get('outbox')
        card = store.db.execute('SELECT text,telegram_message FROM outbox WHERE id=?', (previous,)).fetchone()
        if card:
            store.db.execute('UPDATE outbox SET reply_markup=NULL WHERE id=?', (previous,))
            if card['telegram_message']:
                store.enqueue_report(topic, card['text'], edit=card['telegram_message'],
                                     reply_markup={'inline_keyboard': []})
        in_place = False
    emit(store, topic, text, rows, reply_to, in_place=in_place)


def _projects(store, topic, reply_to=None, prefix='', in_place=False, text=None):
    """The /projects card. Its text is the projects.list reply."""
    from .onboarding import _can_bind
    if text is None:
        from .control_api import call
        text = call(store, 'projects.list', {}, topic=topic, source='telegram', message=reply_to).text
    rows = [[_input('New project', 'project_name'), _input('Projects folder', 'root')]]
    if topic != store.get('control_topic'):
        if (store.topic(topic) or {}).get('cwd'):
            rows.append([_view('This topic ›', 'project')])
        elif _can_bind(store, topic):
            rows.append([_command('Link this topic', '/setup')])
    text = (prefix + '\n\n' if prefix else '') + clip(text, CARD_LIMIT)
    emit(store, topic, clip(text, 4096), rows, reply_to, where='/projects', in_place=in_place)
    return True


def control_report(store, topic, text, command, args, reply_to=None, in_place=False):
    if command in ('/setup', '/start') or (command == '/project' and args[:1] == ['new']):
        return setup_report(store, topic, text, reply_to, in_place)
    prefix = NO_ARGUMENTS if args and command in ('/accounts', '/secrets', '/projects', '/help', '/health') else ''
    if command == '/accounts':
        return view(store, topic, 'accounts:0', reply_to, prefix, in_place)
    if command == '/secrets':
        return view(store, topic, 'secrets:0', reply_to, prefix, in_place)
    if command == '/projects':
        return _projects(store, topic, reply_to, prefix, in_place, None if args else text)
    if command == '/help' and args:
        from .controls import _help
        text = _help(store)
    if command == '/health' and args:
        from .control_api import call
        text = call(store, 'health.show', {}, topic=topic, source='telegram', message=reply_to).text
    rows = [[_view('Accounts', 'accounts:0')]] if command == '/health' else []
    emit(store, topic, (prefix + '\n\n' if prefix else '') + text, rows, reply_to,
         where=' '.join([command] + [str(a) for a in args]), in_place=in_place)
    return True


_MODEL_TITLES = {'worker': 'Worker model', 'codex': 'Codex model', 'coordinator': 'Coordinator model'}
_MODEL_UNSET = {'worker': 'native default', 'codex': 'Codex CLI default', 'coordinator': 'service default'}
_ACCOUNT_BUTTONS = {'⚪': ('Sign in %s', 'account:'), '⚫': ('Turn on %s', 'account:'), None: ('Remove %s', 'remove:')}
_SIGNIN_STATES = {'starting': 'starting', 'link': 'waiting for you to open the link',
                  'code': 'waiting for the code', 'checking': 'checking the sign-in',
                  'approve': 'waiting for the device code', 'cancelled': 'closing'}


def _model_value(store, role):
    return store.get(role + '_model', 'opus' if role == 'worker' else None) or _MODEL_UNSET[role]


def _claude_accounts(store):
    """Every registered Claude alias: the next account, then signed in, then signed out, each by label."""
    from .accounts import account_label, listed_accounts
    from .controls import _ALIAS
    registry = store.get('accounts', {}) or {}
    listed, active = listed_accounts(store, model='opus')
    rest = sorted((alias for alias, profile in registry.items()
                   if isinstance(profile, dict) and alias not in listed),
                  key=lambda alias: (account_label(store, alias).casefold(), alias))
    return [alias for alias in listed + rest if _ALIAS.fullmatch(alias)], active


def _codex_accounts(broker):
    from .controls import _ALIAS
    listed, current = broker.listed()
    rest = sorted((alias for alias in broker.accounts() if alias not in listed),
                  key=lambda alias: (broker.label(alias).casefold(), alias))
    return [alias for alias in listed + rest if _ALIAS.fullmatch(alias)], current


def _short_names(labels):
    """Button names that fit a phone and still tell accounts apart: the email's name, plus its domain when shared."""
    from collections import Counter
    local = {alias: label.partition('@')[0] or label for alias, label in labels.items()}
    shared = Counter(local.values())
    names = {}
    for alias, label in labels.items():
        domain = label.partition('@')[2]
        if shared[local[alias]] > 1 and domain:
            first = domain.split('.')[0]
            twins = [other.partition('@')[2].split('.')[0] for key, other in labels.items() if local[key] == local[alias]]
            names[alias] = local[alias] + ' · ' + (first if twins.count(first) == 1 else domain)
        else:
            names[alias] = local[alias]
    repeated = Counter(names.values())
    return {alias: name + ' (' + alias + ')' if repeated[name] > 1 else name for alias, name in names.items()}


def _accounts_view(store, page):
    from . import opencode_subscription
    from .accounts import AccountBroker, account_label, account_picks, identity_email, switch_threshold
    from .codex_accounts import CODEX_SWITCH_THRESHOLD, CodexBroker
    from .controls import _mapping
    from .health import claude_line, codex_line
    from .signin import pending, provider_name, signin_label
    claude, next_alias = _claude_accounts(store)
    broker = CodexBroker(store)
    codex, current = _codex_accounts(broker)
    threshold = switch_threshold(store)
    record = pending(store)
    picks = account_picks(store)
    buttons = []
    lines = []
    if claude:
        router = AccountBroker(store)
        snapshots = _mapping(store.get('account_status', {}))
        identities = {}
        for alias in claude:
            mark, _, line = claude_line(store, router, alias)
            buttons.append((_ACCOUNT_BUTTONS.get(mark, _ACCOUNT_BUTTONS[None]), 'Claude', account_label(store, alias), alias))
            if alias == next_alias and len(claude) > 1:
                line += ' · next'
            if switch_threshold(store, alias) != threshold:
                line += ' · reserve %d%%' % round((1 - switch_threshold(store, alias)) * 100)
            email = identity_email(_mapping(_mapping(snapshots.get(alias)).get('identity')))
            if email:
                first = identities.setdefault(email, alias)
                if first != alias:
                    line += ' · same login as ' + _plain(account_label(store, first))
            lines.append(line)
        named = {group: _plain(account_label(store, pick)) if pick else 'automatic' for group, pick in picks.items()}
        lines[0] = ('\n**Claude** (left: week · Fable)\n'
                    + ('Fable: %s · Other models: %s\n' % (named['fable'], named['other']) if any(picks.values())
                       else 'Automatic\n') + lines[0])
    if codex:
        start = len(lines)
        for alias in codex:
            mark, _, line = codex_line(broker, alias, current, len(codex))
            buttons.append((_ACCOUNT_BUTTONS.get(mark, _ACCOUNT_BUTTONS[None]), 'ChatGPT', broker.label(alias), alias))
            lines.append(line)
        lines[start] = ('\n**ChatGPT** · auto-switch ' + ('on' if broker.automatic() else 'off')
                        + ' (left: week)\n' + lines[start])
    antigravity = optional('antigravity_accounts')
    start = len(lines)
    for alias, mark, line in antigravity.lines(store) if antigravity else []:
        buttons.append((_ACCOUNT_BUTTONS.get(mark, _ACCOUNT_BUTTONS[None]), 'Antigravity', alias, alias))
        lines.append(line)
    if len(lines) > start:
        lines[start] = '\n**Antigravity** (left: week)\n' + lines[start]
    opencode = opencode_subscription.card(store)
    if opencode:
        lines.append('\n' + opencode)
    routing = optional('routing')
    tail = [routing.order_line(routing.Brokers(store))] if routing is not None else []
    if claude and not next_alias:
        tail.append('No Claude account can take work now.')
    if record:
        target = record.get('target')
        tail.append('Sign-in open: ' + ('ChatGPT' if provider_name(record) == 'Codex' else provider_name(record))
                    + (' for ' + _plain(signin_label(store, target, record.get('provider', 'claude')))
                       if target else '')
                    + ', ' + _SIGNIN_STATES.get(record.get('state'), 'in progress') + '.')
    if lines:
        text = _fit(['**Accounts** · Claude switches at %d%% used; Codex switches at %d%% used with auto-switch' %
                     (round(threshold * 100), round(CODEX_SWITCH_THRESHOLD * 100))], lines,
                    [''] + tail if tail else [])
    else:
        text = '\n'.join(['**Accounts**', 'No accounts yet. Add one to start work. Project setup works before sign-in.']
                         + ([''] + tail if tail else []))
    names = {provider: _short_names({alias: label for _, kind, label, alias in buttons if kind == provider})
             for provider in ('Claude', 'ChatGPT', 'Antigravity')}
    page = min(page, max(0, (len(buttons) - 1) // ACCOUNT_PAGE))
    name = 'accounts:%d' % page
    choices = [[_view(_button(template.replace('%s', '%s (' + provider + ')'), names[provider][alias]), view + alias)]
               for (template, view), provider, _, alias in buttons]
    rows = choices[page * ACCOUNT_PAGE:(page + 1) * ACCOUNT_PAGE]
    rows += [[_view('OpenCode subscription', 'account:' + alias)] for alias in opencode_subscription.accounts(store)]
    if record:
        rows.append([_operation('Cancel sign-in', 'account.signin_cancel', name)])
    else:
        rows.append([_operation('Add Claude', 'account.add', name), _operation('Add ChatGPT', 'account.add', name, provider='codex')]
                    + ([_operation('Add Antigravity', 'account.add', name, provider='antigravity')]
                       if store.get('antigravity_enabled', False) is True else []))
    rows.append([_view('Models ›', 'settings')])
    navigation = []
    if page:
        navigation.append(_view('‹ Previous', 'accounts:%d' % (page - 1)))
    if (page + 1) * ACCOUNT_PAGE < len(choices):
        navigation.append(_view('Next ›', 'accounts:%d' % (page + 1)))
    if navigation:
        rows.append(navigation)
    return text, rows


def _registered(store, alias):
    """The provider and profile for a registered alias, else (None, None)."""
    from .controls import _ALIAS
    if not isinstance(alias, str) or not _ALIAS.fullmatch(alias):
        return None, None
    for provider, key in (('claude', 'accounts'), ('codex', 'codex_accounts'), ('antigravity', 'antigravity_accounts')):
        profile = (store.get(key, {}) or {}).get(alias)
        if isinstance(profile, dict):
            return provider, profile
    return None, None


def _account_view(store, alias):
    from . import opencode_subscription
    if alias in opencode_subscription.accounts(store):
        return opencode_subscription.card(store, alias), []
    from .signin import account_authenticated, signin_label
    provider, profile = _registered(store, alias)
    if not provider:
        return None
    label = _plain(signin_label(store, alias, provider))
    name = {'claude': 'Claude', 'codex': 'ChatGPT', 'antigravity': 'Antigravity'}[provider]
    if not account_authenticated(store, alias, provider):
        text = (f'{label} ({name}) is signed out.\n'
                f'Sign in with the same {name} account to bring it back. Its alias, history and settings stay.\n'
                'Remove takes it off this list. ' + ('Its profile folder goes to the Trash.' if provider == 'antigravity'
                                                     else 'Its folder stays on this Mac.'))
        return text, [[_operation('Sign in', 'account.add', 'accounts:0', alias=alias),
                       _view('Remove', 'remove:' + alias)]]
    if provider != 'antigravity' and profile.get('enabled') is not True:
        return (f"{label} ({name}) is signed in but turned off, so Torii doesn't use it.",
                [[_operation('Turn on', 'account.enable', 'accounts:0', alias=alias)]])
    return None


def _remove_view(store, alias):
    from .signin import account_authenticated, signin_label
    provider, _ = _registered(store, alias)
    if not provider:
        return None
    signed = account_authenticated(store, alias, provider)
    detail = ''
    if provider == 'antigravity' and signed:
        detail = optional('antigravity_accounts').card(store, alias) + '\n\n'
    text = (detail + 'Remove %s from Torii? ' % _plain(signin_label(store, alias, provider))
            + ('New work moves to your other accounts. ' if signed else "It's signed out, so no work uses it. ")
            + ('Its profile folder goes to the Trash.' if provider == 'antigravity' else
               "Its folder stays on this Mac and Torii won't re-add it on its own."))
    return text, [[_operation('Remove', 'account.remove', 'accounts:0', alias=alias)]]


def _opencode_line(store):
    from . import opencode_subscription
    from .opencode_accounts import owner_settings
    settings = owner_settings(store)
    if not settings['opencode_enabled'] and not settings['opencode_active_account']:
        return []
    if settings['opencode_active_account'] in opencode_subscription.accounts(store):
        settings['opencode_active_account'] = 'OpenCode subscription'
    parts = [(label, settings[key]) for label, key in (('account', 'opencode_active_account'),
                                                         ('model', 'opencode_model'),
                                                         ('host', 'opencode_base_url_host'))]
    return [' · '.join(['OpenCode workers: ' + ('on' if settings['opencode_enabled'] else 'off')]
                       + [label + ' ' + _plain(value) for label, value in parts if value])]


def _settings_view(store):
    from .controls import _until
    from .codex_accounts import CodexBroker
    enabled = store.get('codex_enabled', True)
    broker = CodexBroker(store)
    quota = ''
    if enabled and broker.managed() and not broker.select() and broker.automatic():
        until = broker.earliest_reset()
        quota = (' · every Codex account is limited, back in ' + _until(until) if until
                 else ' · no Codex account is available')
    elif enabled and broker.managed() and not broker.select():
        active = broker.active()
        until = broker.earliest_reset()
        quota = (' · the active Codex account %s is ' % (_plain(broker.label(active)) if active else 'none') +
                 ('limited, back in ' + _until(until) if until else 'unavailable'))
    routing = optional('routing')
    text = '\n'.join(['**Models**']
                     + [_MODEL_TITLES[role] + ': ' + _plain(_model_value(store, role)) for role in _MODEL_TITLES]
                     + ['Codex: ' + ('on' if enabled else 'off') + quota]
                     + _opencode_line(store)
                     + (routing.table_lines(store) if routing is not None else [])
                     + ['Worker and Codex changes apply to future dispatches. '
                        'The coordinator change applies at its next launch.'])
    return text, ([[_view(title, 'model:' + role)] for role, title in _MODEL_TITLES.items()]
                  + [[_view('Usage policy', 'policy')]])


def _secret_rows(store):
    """op_secret_list rows with the latest ask time and whether a filled value is still stored."""
    from .envelopes import HANDOFF, op_secret_list
    extra = {row['name']: row for row in store.db.execute(
        "SELECT name, MAX(state='filled') AS stored, (SELECT created FROM envelopes latest WHERE latest.name=e.name "
        'ORDER BY id DESC LIMIT 1) AS asked FROM envelopes e WHERE name!=? GROUP BY name', (HANDOFF,))}
    return [dict(row, stored=bool(extra[row['name']]['stored']), asked=extra[row['name']]['asked'])
            for row in op_secret_list(store, None).data if row['name'] in extra]


def _source(value):
    return _plain(str(value).replace(':', ' ')) if value else 'unknown'


def _secret_line(row):
    state = row['state']
    if state == 'filled':
        return ('🟢 %s — filled %s' % (row['name'], _date(row['filled_at']))
                + (', last used %s by %s' % (_date(row['last_use']), _source(row['last_use_source']))
                   if row['last_use'] else ''))
    if state in ('open', 'armed'):
        return '🟡 %s — waiting for you, asked %s' % (row['name'], _date(row['asked']))
    if state == 'revoked':
        return '🔴 %s — revoked' % row['name']
    return '⚪ %s — %s, %s' % (row['name'], state, 'previous value still stored' if row['stored'] else 'nothing stored')


def _secrets_view(store, page):
    from .envelopes import used_jobs_text, valid_name
    rows = _secret_rows(store)
    if not rows:
        return ('**Secrets**\nNo secrets yet. When a job needs a key, its agent asks with an envelope card '
                'and the name shows up here.'), []
    text = _fit(['**Secrets** · %d' % len(rows)],
                [_secret_line(row) + ' · ' + used_jobs_text(row['jobs']) for row in rows],
                ['Tap a name to rotate, revoke or ask again.'])
    names = [row['name'] for row in rows if valid_name(row['name'])]
    page = min(page, max(0, (len(names) - 1) // PAGE))
    shown = names[page * PAGE:(page + 1) * PAGE]
    buttons = [[_view(name, 'secret:' + name) for name in shown[index:index + 2]] for index in range(0, len(shown), 2)]
    navigation = []
    if page:
        navigation.append(_view('‹ Previous', 'secrets:%d' % (page - 1)))
    if (page + 1) * PAGE < len(names):
        navigation.append(_view('Next ›', 'secrets:%d' % (page + 1)))
    return text, buttons + ([navigation] if navigation else [])


def _secret(store, name):
    from .envelopes import valid_name
    if not valid_name(name):
        return None
    return next((row for row in _secret_rows(store) if row['name'] == name), None)


def _secret_view(store, topic, name):
    from .envelopes import used_jobs_text
    row = _secret(store, name)
    if not row:
        return None
    lines = ['**%s** · %s' % (name, row['state'])]
    if row['length'] is not None:
        lines.append('Length %d · fingerprint %s · filled %s' % (row['length'], _plain(row['fingerprint']),
                                                                  _date(row['filled_at'])))
    task = store.task_get(row['task']) if row['task'] else None
    lines.append('For: %s · %s' % (_plain(clip(row['consumer'], 200)), 'job %d' % task['number'] if task else 'any job'))
    lines.append('Jobs: ' + used_jobs_text(row['jobs']))
    if row['last_use']:
        lines.append('Last use: %s by %s' % (_date(row['last_use']), _source(row['last_use_source'])))
    rotate = _operation('Rotate' if row['state'] == 'filled' else 'Ask again', 'secret.rotate', 'secret:' + name,
                        name=name, topic=topic)
    if row['state'] in ('open', 'armed'):
        place = (store.topic(row['topic']) or {}).get('name')
        lines.append('Its envelope card is waiting in %s.' % (_plain(place) if place else 'its topic'))
        buttons = []
    elif row['state'] == 'filled' or row['stored']:
        buttons = [[rotate, _view('Revoke', 'revoke:' + name)]]
    else:
        buttons = [[rotate]]
    return '\n'.join(lines), buttons


def _revoke_view(store, name):
    row = _secret(store, name)
    if not row or not row['stored']:
        return None
    return ('Revoke %s? This deletes the stored value. Workers already running keep their copy until they exit.' % name,
            [[_operation('Revoke', 'secret.revoke', 'secret:' + name, name=name)]])


def _jobs_view(store, topic):
    jobs = store.tasks_list(topic, 'open')
    lines = [clip('Job %d: %s' % (job['number'], job['title']), 120) for job in jobs[:25]]
    if len(jobs) > 25:
        lines.append('…and %d more open jobs.' % (len(jobs) - 25))
    return 'Open jobs\n' + ('\n'.join(lines) or 'No open jobs in this channel.'), []


def _project_view(store, topic):
    bound = store.topic(topic) or {}
    if not bound.get('cwd'):
        return None
    text = ('**This topic** · ' + _plain(bound.get('name') or 'Unnamed') + '\n' + _plain(bound['cwd']) + '\n' +
            ('This channel accepts work.' if bound.get('enabled') else
             'This channel is disabled. Use the local controls to enable it before sending work.'))
    return text, [[_input('Change folder', 'folder'), _view('Open jobs', 'jobs')]]


def _model_view(store, role):
    if role not in _MODEL_TITLES:
        return None
    text = f'{_MODEL_TITLES[role]}: {_plain(_model_value(store, role))}\nChoose a model or enter its ID. Your provider checks model access.'
    presets = ([[_operation('GPT-6.2 Sol (default)', 'model.codex', 'settings', model='gpt-6.2-sol'),
                 _operation('GPT-6 Luna', 'model.codex', 'settings', model='gpt-6-luna')],
                [_operation('GPT-6 Astra', 'model.codex', 'settings', model='gpt-6-astra'),
                 _operation('GPT-6.2 Astra', 'model.codex', 'settings', model='gpt-6.2-astra')]] if role == 'codex' else
               [[_operation('Opus 5.5', 'model.' + role, 'settings', model='claude-opus-5-5')]])
    return text, presets + [[_input('Custom model', 'model:' + role)],
                  [_operation(_MODEL_UNSET[role][0].upper() + _MODEL_UNSET[role][1:], 'model.' + role, 'settings', model=None)]]


def _readable(policy):
    """The policy as the owner reads it: no source comments, no blank-line runs they leave."""
    return re.sub(r'\n{3,}', '\n\n', re.sub(r'[ \t]*<!--.*?-->', '', policy, flags=re.S)).strip()


def _page(name):
    number = name.split(':', 1)[1]
    return int(number) if number.isascii() and number.isdigit() and len(number) <= 6 else None


def view(store, topic, name, reply_to=None, prefix='', in_place=False):
    if name in ('models', 'delegation'):
        name = 'settings'
    head, _, rest = name.partition(':')
    if head in ('accounts', 'secrets') and rest:
        page = _page(name)
        if page is None:
            return False
        shown = _accounts_view(store, page) if head == 'accounts' else _secrets_view(store, page)
    elif head == 'account' and rest:
        shown = _account_view(store, rest)
    elif head == 'remove' and rest:
        shown = _remove_view(store, rest)
    elif head == 'secret' and rest:
        shown = _secret_view(store, topic, rest)
    elif head == 'revoke' and rest:
        shown = _revoke_view(store, rest)
    elif head == 'model' and rest:
        shown = _model_view(store, rest)
    elif name == 'settings':
        shown = _settings_view(store)
    elif name == 'jobs':
        shown = _jobs_view(store, topic)
    elif name == 'project':
        shown = _project_view(store, topic)
    elif name == 'policy':
        from .policy import personal_path, read_rule
        try:
            rules = read_rule(personal_path(store))
        except (OSError, UnicodeError):
            shown = 'Personal rules\npersonal.md is unavailable.', []
        else:
            shown = 'Personal rules\n' + _readable(rules), []
    else:
        return False
    if shown is None:
        return False
    text, rows = shown
    back = _back_row(name)
    if back:
        rows = rows + [back]
    whole = name == 'policy'
    text = (prefix + '\n\n' if prefix else '') + (text if whole else clip(text, CARD_LIMIT))
    emit(store, topic, text if whole else clip(text, 4096), rows, reply_to, where=name, in_place=in_place)
    return True


def prompt(store, topic, kind, reply_to=None, error='', in_place=False):
    if kind == 'first_project_name':
        from .onboarding import _key as setup_key, first_project_question
        state = store.get(setup_key(topic), {}) or {}
        if state.get('stage') != 'first':
            return False
        store.put(setup_key(topic), dict(state, name_armed=True))
        setup_report(store, topic, first_project_question(store, topic))
        return True
    if kind.startswith('model:'):
        if kind.split(':', 1)[1] not in _MODEL_TITLES:
            return False
        text = 'Reply to this message with the model ID for the ' + kind.split(':', 1)[1] + ' model.'
        back = _view('Cancel', 'settings')
    elif kind == 'project_name':
        from .onboarding import project_root
        text = ("Reply to this message with the new project's name, for example My Website. Torii makes a folder in "
                + _plain(project_root(store)) + ', runs git init, and opens a topic for it.')
        back = _command('Cancel', '/projects')
    elif kind == 'folder':
        text = ('Reply to this message with the full path of this project\'s folder. Only the folder changes: the saved '
                'conversation, provider, and settings stay as they are.')
        back = _view('Cancel', 'project')
    elif kind == 'root':
        text = ('Reply to this message with the full path of an existing folder for new projects. '
                'Existing projects will stay where they are.')
        back = _command('Cancel', '/projects')
    else:
        return False
    emit(store, topic, (error + '\n\n' if error else '') + text, [[back]], reply_to, pending=kind, in_place=in_place)
    return True


def _new_project(store, topic, name):
    """Create a project from the New project form. The form's state is restored when creation fails."""
    from .onboarding import _create, _key as setup_key
    previous = store.get(setup_key(topic))
    store.put(setup_key(topic), {'stage': 'project_new'})
    reply = _create(store, topic, name)
    if (store.get(setup_key(topic), {}) or {}).get('stage') == 'creating':
        return True, reply
    store.put(setup_key(topic), previous)
    return False, reply


def accept_text(store, topic, text, message_id, reply_to=None):
    if (text.startswith('/goal ') or reply_to is None
            or reply_to == store.topic(topic)['thread']):
        return False
    state = store.get(_key(topic), {}) or {}
    kind = state.get('pending')
    target = store.db.execute('SELECT id,kind,reply_markup FROM outbox WHERE topic=? AND telegram_message=?',
                              (topic, reply_to)).fetchone()
    if not target or target['kind'] == 'wait-ask' or not target['reply_markup']:
        return False
    if target['id'] != state.get('outbox'):
        store.enqueue_report(topic, 'This form has expired. Open the command again.', reply_to=message_id)
        return True
    if not kind:
        return False
    from .control_api import call
    from .controls import _MODEL
    value = text.strip()
    if kind == 'project_name':
        created, reply = _new_project(store, topic, value)
        if created:
            _projects(store, topic, message_id, reply, in_place=True)
        else:
            prompt(store, topic, kind, message_id, reply, in_place=True)
        return True
    if kind.startswith('model:'):
        valid = bool(_MODEL.fullmatch(value))
        op, params, page = 'model.' + kind.split(':', 1)[1], {'model': value}, 'settings'
        error = 'Send one model ID using letters, digits, dots, colons, underscores, or hyphens.'
    elif kind == 'folder':
        valid = bool(value) and '\x00' not in value
        op, params, page = 'topic.folder', {'project': value}, 'project'
        error = 'Send one absolute folder path.'
    elif kind == 'root':
        from .onboarding import _existing_path
        valid = _existing_path(value) is not None
        op, params, page = 'projects.root', {'path': value}, '/projects'
        error = 'That folder is unavailable. Send an existing absolute folder path.'
    else:
        return False
    if not valid:
        prompt(store, topic, kind, message_id, error, in_place=True)
        return True
    result = call(store, op, params, topic=topic, source='telegram', message=message_id)
    if page == '/projects':
        _projects(store, topic, message_id, result.text, in_place=True)
    elif not view(store, topic, page, message_id, prefix=result.text, in_place=True):
        emit(store, topic, result.text, [], message_id, in_place=True)
    return True


def accept_callback(store, query):
    sender = query.get('from', {})
    message = query.get('message', {})
    chat = message.get('chat', {})
    if (sender.get('is_bot') or type(sender.get('id')) is not int or sender.get('id') != store.get('owner')
            or chat.get('id') != store.chat() or chat.get('type') not in ('group', 'supergroup')):
        return 'unauthorized'
    thread = message.get('message_thread_id')
    message_id = message.get('message_id')
    if type(message_id) is not int:
        return 'stale_callback'
    if type(thread) is int:
        topic = f"{chat['id']}:{thread}"
    else:
        delivered = store.db.execute('SELECT topic FROM outbox WHERE telegram_message=? AND delivered=1', (message_id,)).fetchone()
        topic = delivered['topic'] if delivered else ''
    state = store.get(_key(topic), {}) or {}
    data = query.get('data')
    if not isinstance(data, str):
        return 'stale_callback'
    if data.startswith('wait:'):
        from .waits import accept_button
        return store.wait_hook('button-trigger-failed', accept_button, store, topic, message_id, data) or 'stale_callback'
    envelope = CANCEL_DATA.fullmatch(data)
    if envelope:
        from .envelopes import cancel
        return cancel(store, int(envelope.group(1)), 'button')
    envelope = AGAIN_DATA.fullmatch(data)
    if envelope:
        from .envelopes import again
        return again(store, int(envelope.group(1)))
    envelope = FILL_DATA.fullmatch(data)
    if envelope:
        from .envelopes import fill_private
        return fill_private(store, int(envelope.group(1)))
    from .signin import CANCEL_SIGNIN_DATA, CODEX_RETRY_DATA, RETRY_DATA, cancel_matches
    if data in (RETRY_DATA, CODEX_RETRY_DATA):
        return _signin_retry(store, topic, message_id, data == CODEX_RETRY_DATA)
    if data.startswith(CANCEL_SIGNIN_DATA):
        if not cancel_matches(store, data):
            return 'stale_callback'
        from .control_api import call
        call(store, 'account.signin_cancel', {}, topic=topic, source='telegram', message=message_id)
        return 'control_callback'
    parts = data.split(':')
    close_card = store.get('topic_close_card:' + parts[1]) if len(parts) == 3 and parts[0] == 'torii' else None
    if close_card and close_card['topic'] == topic and parts[2].isascii() and parts[2].isdigit() and len(parts[2]) <= 3:
        actions = close_card['actions']
        index = int(parts[2])
        if index >= len(actions):
            return 'stale_callback'
        from .topics import accept_close
        action = actions[index]
        return accept_close(store, action['topic_close'], action['choice'], action['revision'])
    if (len(parts) != 3 or parts[0] != 'torii' or parts[1] != state.get('token')
            or not parts[2].isascii() or not parts[2].isdigit() or len(parts[2]) > 3):
        return 'stale_callback'
    index = int(parts[2])
    actions = state.get('actions', [])
    if index >= len(actions):
        return 'stale_callback'
    action = actions[index]
    from .controls import _USAGE
    if 'command' in action and action['command'].split()[0] not in _USAGE:
        return 'stale_callback'
    if ('view' in action or 'command' in action) and action.get('view', action.get('command')) == state.get('where'):
        logging.getLogger(__name__).info('control button already here topic=%s', topic)
        return 'already_here'
    logging.getLogger(__name__).info('control button topic=%s', topic)
    if 'update' in action:
        from .updates import accept_action
        return accept_action(store, topic, message_id, action['update'])
    if 'command' in action:
        from .controls import handle_control
        shown = handle_control(store, topic, message_id, action['command'])
    elif 'op' in action:
        from .control_api import call
        result = call(store, action['op'], action.get('params', {}), topic=topic,
                      source='telegram', message=message_id)
        if action['op'].startswith('setup.'):
            if action['op'] != 'setup.topics_check':
                from .setup_flow import setup_status
                setup_status(store, force=True, prefix='' if result.ok else result.text)
            shown = True
        else:
            shown = view(store, topic, action['page'], message_id, prefix=result.text)
    elif 'view' in action:
        shown = view(store, topic, action['view'], message_id)
    else:
        shown = prompt(store, topic, action.get('input', ''), message_id)
    return 'control_callback' if shown else 'stale_callback'


def _signin_retry(store, topic, message_id, codex=False):
    """Start the same sign-in again: the provider and target of the one that just ended."""
    from .control_api import call
    from .signin import RETRY_KEY
    if not store.topic(topic):
        return 'stale_callback'
    provider = 'codex' if codex else 'claude'
    retry = store.get(RETRY_KEY)
    target = retry.get('target') if isinstance(retry, dict) and retry.get('provider') == provider else None
    params = dict({'provider': provider}, **({'alias': target} if target else {}))
    result = call(store, 'account.add', params, topic=topic, source='telegram', message=message_id)
    view(store, topic, 'accounts:0', message_id, prefix=result.text)
    return 'control_callback'
