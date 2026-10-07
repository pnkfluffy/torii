"""Select native account homes without copying credentials or conversation history."""

import json
import math
import logging
from pathlib import Path
import re
import shlex
import time

from .providers import RunResult, child_environment
from .failures import TaskFailure


DEFAULT_ACCOUNTS = {}
LIMIT_CONTINUATION = ('The prior turn stopped at a provider usage limit. Continue the same saved job. '
                      'Inspect prior progress before repeating any action with external effects.\n\n')
SWITCH_THRESHOLD = 0.95
LOCAL_OVERRIDES_PATH = Path(__file__).resolve().parent.parent / 'local-overrides.json'
LEGACY_SETTINGS = ('account_selection', 'account_pin_fallback', 'account_switch_threshold',
                   'auto_rotate', 'model_account_blocks', 'model_account_usage', 'model_account_access')
PROFILE = re.compile(r'[A-Za-z0-9][A-Za-z0-9_-]{0,63}\Z')


def authenticated(store, alias, profile):
    if not isinstance(alias, str) or not PROFILE.fullmatch(alias) or not isinstance(profile, dict):
        return False
    directory = profile.get('config_dir')
    if not isinstance(directory, str) or not directory or not Path(directory).is_dir():
        return False
    snapshot = _dict(_dict(store.get('account_status', {})).get(alias))
    identity = _dict(snapshot.get('identity'))
    return (identity.get('logged_in') is True and bool(identity_email(identity))
            and snapshot.get('error') != 'login_required')


def signed_in(store, alias, profile):
    return isinstance(profile, dict) and profile.get('enabled') is True and authenticated(store, alias, profile)


def discover_accounts(store, root=None):
    root = Path(root or Path.home() / '.claude-accounts').resolve()
    accounts = {alias: dict(profile) for alias, profile in
                store.get('accounts', DEFAULT_ACCOUNTS).items() if isinstance(profile, dict)}
    default_identity = _dict(_dict(_dict(store.get('account_status', {})).get('default')).get('identity'))
    if default_identity.get('logged_in') is not True or not identity_email(default_identity):
        accounts.pop('default', None)
    for alias in list(accounts):
        if alias.startswith('home-login-needs-name'):
            accounts.pop(alias, None)
    status = _dict(store.get('account_status', {}))
    if default_identity.get('logged_in') is not True or not identity_email(default_identity):
        status.pop('default', None)
    for alias in list(status):
        if alias.startswith('home-login-needs-name'):
            status.pop(alias, None)
    store.put('account_status', status)
    store.db.execute('DELETE FROM settings WHERE key IN (%s)' % ','.join('?' * len(LEGACY_SETTINGS)),
                     LEGACY_SETTINGS)
    for alias, profile in accounts.items():
        if profile.get('enabled') and not authenticated(store, alias, profile):
            profile['enabled'] = False
            profile['awaiting_login'] = True
    added = []
    setup_directory = store.get('setup_claude_dir')
    if root.is_dir():
        known = {str(Path(profile['config_dir']).resolve()) for profile in accounts.values()
                 if profile.get('config_dir')}
        known.update(str(Path(directory).resolve()) for profile in accounts.values()
                     for directory in profile.get('previous_config_dirs', []))
        known.update(str(Path(directory).resolve()) for directory in store.get('account_dirs_removed', []))
        for path in sorted(root.iterdir()):
            if (str(path.resolve()) in known):
                continue
            internal = bool(re.fullmatch(r'\.torii-[a-f0-9]{8}', path.name)
                            and str(path.resolve()) == setup_directory)
            alias = path.name.lstrip('.') if internal else path.name
            if ((PROFILE.fullmatch(path.name) or internal) and path.name != 'default' and path.is_dir()
                    and path.resolve().parent == root and (path / '.claude.json').is_file()
                    and alias not in accounts):
                accounts[alias] = {'config_dir': str(path.resolve()), 'enabled': False,
                                       'awaiting_login': True}
                added.append(alias)
    with store.db:
        store.put('accounts', accounts)
        if setup_directory and any(profile.get('config_dir') == setup_directory for profile in accounts.values()):
            store.put('setup_claude_dir', None)
        logging.getLogger(__name__).info('accounts discovered count=%s', len(added))
    return accounts


def _dict(value):
    return value if isinstance(value, dict) else {}


def identity_email(identity):
    email = identity.get('email')
    return email.strip().casefold() if isinstance(email, str) else ''


def account_label(store, alias):
    snapshot = _dict(_dict(store.get('account_status', {})).get(alias))
    identity = _dict(snapshot.get('identity'))
    email = identity.get('email')
    return email.strip() if isinstance(email, str) and email.strip() else alias


def _number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def temporary_local_switch_thresholds():
    """TEMPORARY local override hook. Remove account_switch_thresholds to remove reserves; preserve shared_mcp_servers."""
    try:
        data = json.loads(LOCAL_OVERRIDES_PATH.read_text())
        thresholds = data['account_switch_thresholds']
        if not isinstance(thresholds, dict) or any(
                not isinstance(alias, str) or not _number(value) or not 0 < value < 1
                for alias, value in thresholds.items()):
            raise ValueError('invalid account switch thresholds')
        return thresholds
    except FileNotFoundError:
        return {}
    except (OSError, ValueError, TypeError, KeyError) as error:
        logging.getLogger(__name__).warning('invalid local account overrides: %s', error)
        return {}


def switch_threshold(store, alias=None):
    return temporary_local_switch_thresholds().get(alias, SWITCH_THRESHOLD) if alias else SWITCH_THRESHOLD


def listed_accounts(store):
    """Return display-sorted signed-in accounts and the broker's next choice."""
    registered = _dict(store.get('accounts', {}))
    accounts = {alias: profile for alias, profile in registered.items() if isinstance(profile, dict)}
    aliases = [alias for alias in sorted(accounts) if authenticated(store, alias, accounts[alias])]
    aliases.sort(key=lambda alias: (account_label(store, alias).casefold(), alias))
    router = AccountBroker(store)
    active = router.select(accounts)
    if active not in aliases:
        return aliases, None
    return [active] + [alias for alias in aliases if alias != active], active


def account_block(store, alias):
    state = AccountBroker(store).account_state(alias)
    return ({'reason': 'quota', 'until': state.get('until')}
            if state.get('state') == 'limited' else None)


def _claude_environment(config_dir):
    """The one place a Claude account enters an environment. There is no home-login fallback."""
    if not config_dir:
        raise ValueError('Claude account profile is unavailable')
    env = {key: value for key, value in child_environment().items()
           if key not in ('ANTHROPIC_API_KEY', 'ANTHROPIC_AUTH_TOKEN', 'ANTHROPIC_PROFILE',
                          'CLAUDE_SECURESTORAGE_CONFIG_DIR')
           and not key.startswith('CLAUDE_CODE_OAUTH')}
    env['CLAUDE_CONFIG_DIR'] = str(config_dir)
    return env


def find_transcript(session_id, roots):
    candidates = set()
    for root in roots:
        for path in (Path(root) / 'projects').glob('*/' + session_id + '.jsonl'):
            if path.is_file():
                candidates.add(str(path.resolve()))
    if len(candidates) > 1:
        raise TaskFailure(
            'This agent conversation ID appears in more than one saved transcript: '
            + ', '.join(sorted(candidates)) + '.',
            'Inspect which one this project owns, then move or remove the others. '
            'Do not create a replacement session; its history would be lost.')
    return next(iter(candidates), None)


class AccountBroker:
    FRESH_SECONDS = 600
    REJECTION_HOLD_SECONDS = 300

    def __init__(self, store):
        self.store = store

    def environment(self, alias):
        env = _claude_environment((self.store.get('accounts', DEFAULT_ACCOUNTS).get(alias) or {}).get('config_dir'))
        return env

    @staticmethod
    def profile_environment(account):
        """For a usage check, which reads one profile and has no store."""
        return _claude_environment(account.get('config_dir'))

    @staticmethod
    def signin_environment(folder):
        """For `claude auth login` into a new folder that no profile names yet."""
        return _claude_environment(folder)

    @staticmethod
    def working_directory(env):
        return env['CLAUDE_CONFIG_DIR']

    def login_command(self, alias):
        account = self.store.get('accounts', {}).get(alias) or {}
        directory = account.get('config_dir')
        if not directory:
            raise ValueError('Claude account profile is unavailable')
        return 'env CLAUDE_CONFIG_DIR=' + shlex.quote(directory) + ' claude auth login'

    def transcript_roots(self):
        homes = [str(Path.home() / '.claude')]
        for account in self.store.get('accounts', {}).values():
            if isinstance(account, dict):
                if account.get('config_dir'):
                    homes.append(account['config_dir'])
                homes.extend(account.get('previous_config_dirs', []))
        return list(dict.fromkeys(homes))

    def alias_for_environment(self, env):
        config_dir = env.get('CLAUDE_CONFIG_DIR')
        target = str(Path(config_dir).resolve()) if config_dir else None
        return next((alias for alias, account in self.store.get('accounts', {}).items()
                     if isinstance(account, dict) and account.get('config_dir') and
                     str(Path(account['config_dir']).resolve()) == target), None)

    def resume_transcript(self, alias, session_id, transcript):
        if not transcript:
            return None
        target_root = Path((self.store.get('accounts', {}).get(alias) or {}).get('config_dir') or '')
        source = Path(transcript).resolve()
        if source.parent.parent.name != 'projects':
            raise ValueError('Claude transcript is outside a Claude profile')
        if target_root.resolve() == source.parents[2]:
            return str(transcript)
        destination = target_root / 'projects' / source.parent.name / source.name
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            if not destination.samefile(source):
                raise ValueError('A different Claude transcript already uses this session ID')
        else:
            destination.symlink_to(source)
        return str(destination)

    def account_state(self, alias):
        snapshot = _dict(_dict(self.store.get('account_status', {})).get(alias))
        block = self.blocks().get(alias)
        if block:
            return {'state': 'limited', 'until': block.get('until')}
        if self._usage_full(alias, snapshot):
            return {'state': 'limited', 'until': self._usage_reset(alias, snapshot)}
        observed = snapshot.get('observed_at')
        if not _number(observed) or snapshot.get('error') or time.time() - observed > self.FRESH_SECONDS:
            return {'state': 'stale', 'since': observed}
        return {'state': 'available'}

    def full_windows(self, alias, snapshot=None):
        snapshot = snapshot if snapshot is not None else _dict(_dict(self.store.get('account_status', {})).get(alias))
        threshold = switch_threshold(self.store, alias) * 100
        return {key: window for key in ('five_hour', 'seven_day', 'seven_day_fable')
                for window in [_dict(_dict(snapshot.get('usage')).get(key))]
                if not (_number(window.get('resets_at')) and window['resets_at'] <= time.time())
                and _number(window.get('utilization')) and window['utilization'] >= threshold}

    def _usage_full(self, alias, snapshot=None):
        return bool(self.full_windows(alias, snapshot))

    def log_crossings(self, alias, previous, snapshot):
        before = self.full_windows(alias, previous)
        for meter, window in self.full_windows(alias, snapshot).items():
            if meter not in before:
                logging.getLogger(__name__).info(
                    'account switch point crossed account=%s meter=%s used_pct=%s switch_point_pct=%s reset=%s',
                    account_label(self.store, alias), meter, window['utilization'],
                    switch_threshold(self.store, alias) * 100, window.get('resets_at'))

    def selection_reason(self, accounts, current, attempted=()):
        if not current:
            return 'reason=account_available'
        if current in attempted:
            return 'reason=attempted'
        if accounts.get(current, {}).get('enabled') is False:
            return 'reason=disabled'
        if not signed_in(self.store, current, accounts.get(current)):
            return 'reason=signed_out'
        block = self.blocks().get(current)
        if block and block.get('reason') != 'threshold':
            return 'reason=hold hold_reason=%s until=%s' % (block.get('reason'), block.get('until'))
        windows = self.full_windows(current)
        if windows:
            meter, window = next(iter(windows.items()))
            return 'reason=%s used_pct=%s switch_point_pct=%s reset=%s' % (
                meter, window['utilization'], switch_threshold(self.store, current) * 100, window.get('resets_at'))
        if block:
            return 'reason=hold hold_reason=%s until=%s' % (block.get('reason'), block.get('until'))
        return 'reason=sooner_reset'

    def _usage_reset(self, alias, snapshot):
        usage = _dict(snapshot.get('usage'))
        resets = [_dict(usage.get(key)).get('resets_at') for key in
                  ('five_hour', 'seven_day', 'seven_day_fable')
                  if _number(_dict(usage.get(key)).get('utilization')) and
                  _dict(usage.get(key)).get('utilization') >= switch_threshold(self.store, alias) * 100]
        values = [value for value in resets if _number(value) and value > time.time()]
        return max(values) if values else None

    def is_full(self, alias):
        return self.account_state(alias)['state'] == 'limited'

    def transcript(self, session):
        if not session:
            return None
        saved = self.store.get('session_transcripts', {}).get(session)
        if saved:
            if not Path(saved).is_file():
                raise TaskFailure('The saved agent conversation file is unavailable.', 'Restore access to its original location before continuing.')
            return saved
        roots = self.transcript_roots()
        return find_transcript(session, roots)

    @staticmethod
    def future_reset(info):
        value = info.get('resetsAt')
        return (value if isinstance(value, (int, float)) and not isinstance(value, bool)
                and math.isfinite(value) and value > time.time() else None)

    def blocks(self):
        now = time.time()
        saved = self.store.get('account_blocks', {})
        kept = {alias: block for alias, block in saved.items()
                if isinstance(block, dict) and (block.get('until') is None or block['until'] > now)}
        for alias, block in saved.items():
            snapshot = _dict(_dict(self.store.get('account_status', {})).get(alias))
            if (isinstance(block, dict) and block.get('reason') == 'threshold'
                    and not self._usage_full(alias, snapshot)):
                kept.pop(alias, None)
            elif (isinstance(block, dict) and block.get('until') is None and
                  _number(snapshot.get('observed_at')) and
                  snapshot['observed_at'] > (block.get('observed_at') or 0) and
                  not self._usage_full(alias, snapshot)):
                kept.pop(alias, None)
        if kept != saved:
            outer = self.store.db.in_transaction
            self.store.put('account_blocks', kept)
            if not outer:
                self.store.db.commit()
        return kept

    def available(self, accounts, alias, attempted=()):
        return (alias not in attempted and signed_in(self.store, alias, accounts.get(alias))
                and not self.is_full(alias))

    def select(self, accounts=None, attempted=()):
        accounts = accounts or self.store.get('accounts', DEFAULT_ACCOUNTS)
        eligible = [alias for alias in accounts if self.available(accounts, alias, attempted)]
        snapshots = self.store.get('account_status', {})
        def weekly_reset(alias):
            usage = _dict(_dict(snapshots.get(alias)).get('usage'))
            reset = _dict(usage.get('seven_day')).get('resets_at')
            return reset if _number(reset) and reset > time.time() else math.inf
        return min(eligible, key=lambda alias: (weekly_reset(alias), alias)) if eligible else None

    def activate(self, accounts=None, attempted=()):
        accounts = accounts or self.store.get('accounts', DEFAULT_ACCOUNTS)
        current = self.store.get('active_account')
        alias = self.select(accounts, attempted)
        if alias != current:
            reason = self.selection_reason(accounts, current, attempted)
            if alias is None:
                logging.getLogger(__name__).info('no eligible account from=%s %s',
                                                 account_label(self.store, current), reason)
            else:
                logging.getLogger(__name__).info('account rotated from=%s to=%s %s',
                                                 account_label(self.store, current), account_label(self.store, alias), reason)
            outer = self.store.db.in_transaction
            self.store.put('active_account', alias)
            if not outer:
                self.store.db.commit()
        return alias

    def earliest_reset(self, accounts=None):
        accounts = accounts or self.store.get('accounts', DEFAULT_ACCOUNTS)
        resets = []
        for alias in accounts:
            if not signed_in(self.store, alias, accounts.get(alias)):
                continue
            block = self.blocks().get(alias)
            account_resets = []
            if block and _number(block.get('until')) and block['until'] > time.time():
                account_resets.append(block['until'])
            snapshot = _dict(_dict(self.store.get('account_status', {})).get(alias))
            usage = _dict(snapshot.get('usage'))
            for name in ('five_hour', 'seven_day', 'seven_day_fable'):
                window = _dict(usage.get(name))
                if (_number(window.get('utilization')) and window['utilization'] >= switch_threshold(self.store, alias) * 100
                        and _number(window.get('resets_at')) and window['resets_at'] > time.time()):
                    account_resets.append(window['resets_at'])
            if account_resets:
                resets.append(max(account_resets))
        return min(resets) if resets else None

    def record_rate_limit(self, alias, info, rejected=False):
        previous_snapshot = _dict(_dict(self.store.get('account_status', {})).get(alias))
        limit_type = str((info or {}).get('rateLimitType', ''))
        window_name = ('five_hour' if limit_type.startswith('five_hour') else
                       'seven_day_fable' if limit_type == 'seven_day_overage_included' or 'fable' in limit_type.lower() else
                       'seven_day' if limit_type in ('', 'seven_day') else None)
        if info and window_name:
            snapshot = self.store.get('account_status', {})
            account = dict(snapshot.get(alias) or {})
            usage = dict(account.get('usage') or {})
            utilization = info.get('utilization')
            if type(utilization) in (int, float) and 0 <= utilization <= 1:
                utilization *= 100
            window = {'utilization': utilization} if type(utilization) in (int, float) else {}
            if _number(info.get('resetsAt')):
                window['resets_at'] = info['resetsAt']
            if window:
                previous = _dict(usage.get(window_name))
                if _number(previous.get('resets_at')) and previous['resets_at'] <= time.time():
                    previous = {}
                usage[window_name] = {**previous, **window}
                account['usage'] = usage
                snapshot[alias] = account
                self.store.put('account_status', snapshot)
        self.log_crossings(alias, previous_snapshot,
                           _dict(_dict(self.store.get('account_status', {})).get(alias)))
        utilization = (info or {}).get('utilization')
        if type(utilization) in (int, float) and utilization <= 1:
            utilization *= 100
        full = bool(window_name) and type(utilization) in (int, float) and utilization >= switch_threshold(self.store, alias) * 100
        if rejected or full:
            until = self.future_reset(info or {}) or (time.time() + self.REJECTION_HOLD_SECONDS if rejected else None)
            self.hold(alias, until, 'quota' if rejected else 'threshold')

    def hold(self, alias, until, reason):
        blocks = self.blocks()
        previous = blocks.get(alias) or {}
        if previous.get('until') == until and previous.get('reason') == reason:
            return
        blocks[alias] = {'until': until, 'reason': reason, 'observed_at': time.time()}
        self.store.put('account_blocks', blocks)
        self.store.put('account_status_refresh_requested', time.time())
        logging.getLogger(__name__).info('account limited account=%s reason=%s until=%s',
                                         account_label(self.store, alias), reason, until)

    def record_usage(self, alias, windows, rejected=False, reset=None):
        """Usage read outside Claude stream events. It is written only when it changes."""
        snapshot = _dict(self.store.get('account_status', {}))
        account = dict(_dict(snapshot.get(alias)))
        usage = _dict(account.get('usage'))
        merged = {**usage, **{name: {**_dict(usage.get(name)), **window} for name, window in windows.items()}}
        if merged != usage:
            merged = {name: dict(window, recorded_at=time.time()) if name in windows else window
                      for name, window in merged.items()}
        full = self._usage_full(alias, account)
        previous = dict(account)
        if merged != usage:
            account['usage'] = merged
            snapshot[alias] = account
            self.store.put('account_status', snapshot)
        self.log_crossings(alias, previous, account)
        if rejected and alias not in self.blocks():
            self.hold(alias, self.future_reset({'resetsAt': reset}) or time.time() + self.REJECTION_HOLD_SECONDS,
                      'quota')
        elif not full and self._usage_full(alias, account):
            self.store.put('account_status_refresh_requested', time.time())

    async def run(self, run, provider, prompt, cwd, session_id, notice_topic=None, stopped=None, **options):
        on_account = options.pop('on_account', None)
        if provider != 'claude':
            return await run(provider, prompt, cwd, session_id, **options)
        attempted = set()
        launched_pid = None
        previous = None
        model = options.get('model') or 'native-default'
        resume_path = self.transcript(session_id) if not options['fresh'] else None
        last = RunResult(session_id, error='No enabled Claude account is available.', failure_code='accounts_unavailable')
        while True:
            accounts = self.store.get('accounts', DEFAULT_ACCOUNTS)
            alias = self.activate(accounts, attempted)
            if alias is None:
                return last
            config_dir = accounts[alias].get('config_dir')
            if not options['fresh'] and config_dir and not resume_path:
                return RunResult(session_id, error='Cannot switch account without the existing native transcript.', failure_code='transcript_missing')
            if not options['fresh'] and config_dir and resume_path:
                resume_path = self.resume_transcript(alias, session_id, resume_path)
            attempted.add(alias)
            if on_account:
                on_account(alias)
            with self.store.db:
                self.store.put('last_account', alias)
                if previous and options.get('on_problem'):
                    options['on_problem']('accounts', 'rotated', 'from=%s to=%s model=%s' %
                                          (account_label(self.store, previous), account_label(self.store, alias), model))
            last = await run(provider, prompt, cwd, session_id, account_alias=alias,
                             resume_path=resume_path, **options)
            launched_pid = last.pid or launched_pid
            last.pid = launched_pid
            with self.store.db:
                if last.transcript_path:
                    paths = self.store.get('session_transcripts', {})
                    paths[last.session_id] = last.transcript_path
                    self.store.put('session_transcripts', paths)
                    resume_path = last.transcript_path
                if not last.managed:
                    self.record_rate_limit(alias, last.rate_limit_info,
                                           last.quota_limited and not last.success)
            if last.success or not last.quota_limited or (stopped and stopped()):
                return last
            previous = alias
            if not self.select(self.store.get('accounts', DEFAULT_ACCOUNTS), attempted):
                return last
            resume_path = resume_path or self.transcript(last.session_id)
            if not resume_path:
                last.error = 'Usage limit reached, but the native transcript is unavailable. No job was replayed.'
                last.failure_code = 'transcript_missing'
                return last
            session_id = last.session_id
            options['fresh'] = False
            if not prompt.startswith(LIMIT_CONTINUATION):
                prompt = LIMIT_CONTINUATION + prompt
