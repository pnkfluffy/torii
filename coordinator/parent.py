"""Choose a provider only when a topic's parent launches."""

from .accounts import AccountBroker, signed_in
from .codex_accounts import CodexBroker
from .codex_session import CodexCoordinatorSession, codex_binary, codex_transcript_receipts, transcript_path
from .host import HostClient
from .parent_state import settle
from .session import AccountsUnavailable, CoordinatorSession


def session_key(store, cwd, topic):
    home = next((item['id'] for item in store.topics() if item['enabled'] and item['cwd'] == str(cwd)), None)
    main = store.get('coordinator_session_topic') or store.get('coordinator_home_topic') or home
    return 'coordinator' if topic is None or topic == main else 'coordinator:' + topic


def connected_agents(store):
    names = ['Claude'] if any(signed_in(store, alias, profile)
            for alias, profile in (store.get('accounts', {}) or {}).items()) else []
    if CodexBroker(store).managed():
        names.append('ChatGPT')
    return names or ['none']


def available(store, accounts, codex, runner=None):
    return bool(accounts.select() or codex_binary(store, runner) and codex.parent_account())


def unavailable(store, accounts, codex):
    alias = codex.active()
    resets = [reset for reset in (accounts.earliest_reset(), codex.account_state(alias, parent=True).get('until'))
              if reset is not None]
    known_codex = codex.managed()
    known_claude = any(signed_in(store, name, profile)
                       for name, profile in (store.get('accounts', {}) or {}).items())
    provider = 'both' if known_codex and known_claude else 'codex' if known_codex else 'claude'
    return AccountsUnavailable(min(resets) if resets else None, provider=provider, alias=alias)


async def choose_parent(store, accounts, codex, key='coordinator', runner=None):
    host = store.get(key + '_host')
    if host:
        state = await HostClient(store.directory / 'hosts' / host['id']).recover_state()
        if state != 'dead':
            return host.get('provider', 'claude'), host['account']
        with store.db:
            store.put(key + '_host', None)
    alias = accounts.select()
    if alias is not None:
        return 'claude', alias
    alias = codex.parent_account() if codex_binary(store, runner) else None
    if alias is not None:
        return 'codex', alias
    raise unavailable(store, accounts, codex)


def settle_previous(store, key, provider, native, accounts, codex):
    rows = store.get(key + '_lost') or []
    if not rows:
        return
    if provider == 'claude':
        try:
            path = accounts.transcript(native) if native else None
        except Exception:
            path = None
        recorded = lambda rows: CoordinatorSession.transcript_receipts(path, rows) if path else set()
    else:
        path = next((path for profile in codex.accounts().values()
                     if (path := transcript_path(profile.get('config_dir'), native))), None)
        recorded = lambda rows: codex_transcript_receipts(path, rows) if path else set()
    settle(store, key, rows, recorded)


def switch_reason(store, previous, accounts, codex):
    if previous == 'claude':
        return ('every Claude account is out of usage' if any(signed_in(store, name, profile)
                for name, profile in (store.get('accounts', {}) or {}).items())
                else 'the previous provider is no longer connected')
    alias = codex.active()
    return ('ChatGPT account %s is out of usage' % codex.label(alias)
            if codex.signed_in(alias) and codex.exhausted(alias, parent=True)
            else 'Claude can take work again' if accounts.select() else 'the previous provider is no longer connected')


async def start(store, runner, cwd, model, instructions, **kwargs):
    accounts, codex = AccountBroker(store), CodexBroker(store)
    from .providers import ProviderRunner
    binary_runner = runner if isinstance(runner, ProviderRunner) else None
    key = session_key(store, cwd, kwargs.get('topic'))
    host = store.get(key + '_host') or {}
    previous = host.get('provider', store.get(key + '_provider', 'claude'))
    original = {suffix: store.get(key + suffix) for suffix in ('_session', '_fresh', '_switch')}
    provider, alias = await choose_parent(store, accounts, codex, key, binary_runner)

    async def launch(provider):
        switched = provider != previous and (original['_session'] or store.get(key + '_provider'))
        notice = store.get(key + '_switch')
        if switched:
            settle_previous(store, key, previous, original['_session'], accounts, codex)
            notice = {'from': previous, 'to': provider, 'reason': switch_reason(store, previous, accounts, codex)}
            with store.db:
                store.put(key + '_session', None)
                store.put(key + '_fresh', None)
                store.put(key + '_switch', notice)
        factory = CoordinatorSession.start if provider == 'claude' else CodexCoordinatorSession.start
        session = await factory(store, runner, cwd, model, instructions, **kwargs)
        session.switched = bool(notice and notice['to'] == provider)
        session.switch = notice if session.switched else None
        with store.db:
            store.put(key + '_provider', provider)
        return session

    try:
        return await launch(provider)
    except AccountsUnavailable:
        if provider != 'claude' or not codex_binary(store, binary_runner) or not codex.parent_account():
            raise unavailable(store, accounts, codex)
        with store.db:
            for suffix, value in original.items():
                store.put(key + suffix, value)
        return await launch('codex')
