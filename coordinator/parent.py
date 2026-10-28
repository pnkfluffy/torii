"""Choose a provider only when a topic's parent launches."""

import time

from .accounts import AccountBroker, signed_in
from .codex_accounts import CodexBroker
from .codex_session import CodexCoordinatorSession, codex_binary, codex_transcript_receipts, transcript_path
from .host import HostClient
from . import opencode_runtime
from .parent_state import effective_agent, recover_lost_turns, settle, topic_enabled
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
    if store.get('opencode_enabled', False) is True:
        from . import opencode_accounts
        if opencode_accounts.active(store) is not None:
            names.append('OpenCode workers (no goals or secrets; spend is an estimate)')
    return names or ['none']


def available(store, accounts, codex, runner=None, key='coordinator'):
    preference = effective_agent(store, key)['selection'].get('provider')
    if preference == 'opencode':
        try:
            opencode_runtime.parent_ready(store)
        except AccountsUnavailable:
            return False
        return True
    if preference == 'codex':
        return bool(codex_binary(store, runner) and codex.parent_account())
    return bool(accounts.select() or codex_binary(store, runner) and codex.parent_account())


def unavailable(store, accounts, codex, key='coordinator'):
    alias = codex.active()
    preference = effective_agent(store, key)['selection'].get('provider')
    if preference == 'opencode':
        try:
            opencode_runtime.parent_ready(store)
        except AccountsUnavailable as error:
            return error
        from . import opencode_accounts
        return AccountsUnavailable(None, provider='opencode', alias=opencode_accounts.active(store))
    if preference == 'codex':
        reset = codex.account_state(alias, parent=True).get('until')
        return AccountsUnavailable(reset, provider=preference, alias=alias)
    resets = [reset for reset in (accounts.earliest_reset(), codex.account_state(alias, parent=True).get('until'))
              if reset is not None]
    known_codex = codex.managed()
    known_claude = any(signed_in(store, name, profile)
                       for name, profile in (store.get('accounts', {}) or {}).items())
    provider = 'both' if known_codex and known_claude else 'codex' if known_codex else 'claude'
    return AccountsUnavailable(min(resets) if resets else None, provider=provider, alias=alias)


async def retry_paused(store, pause_key):
    notice = store.get(pause_key + '_notice') or {}
    provider = (notice.get('key') or [None])[0]
    if provider == 'opencode':
        from . import opencode_accounts, opencode_subscription
        alias = opencode_accounts.active(store)
        with store.db:
            blocks = store.get('opencode_account_blocks', {}) or {}
            block = blocks.get(alias) or {}
            if block.get('retry_seconds') and block.get('worker_id') is not None:
                blocks[alias] = dict(block, account_until=time.time())
            elif block:
                blocks[alias] = dict(block, until=time.time(), account_until=time.time())
            store.put('opencode_account_blocks', blocks)
        if (opencode_subscription.is_subscription(opencode_accounts.accounts(store).get(alias) or {})
                and not opencode_subscription.available(store, alias)):
            await opencode_subscription.refresh_accounts(store, aliases=[alias])
    else:
        if provider in ('claude', 'both'):
            from .account_status import refresh_accounts
            with store.db:
                alias = (store.get('claude_account_pick') or store.get('active_account') or
                         store.get('coordinator_rotation_from'))
                blocks = store.get('account_blocks', {}) or {}
                if blocks.get(alias, {}).get('reason') in ('quota', 'threshold'):
                    blocks.pop(alias)
                    store.put('account_blocks', blocks)
            await refresh_accounts(store, aliases=[alias] if alias else None)
        if provider in ('codex', 'both'):
            from .codex_accounts import refresh_codex_accounts
            alias = CodexBroker(store).active()
            with store.db:
                blocks = store.get('codex_account_blocks', {}) or {}
                if blocks.get(alias, {}).get('reason') in ('quota', 'threshold'):
                    blocks.pop(alias)
                    store.put('codex_account_blocks', blocks)
            await refresh_codex_accounts(store)


async def choose_parent(store, accounts, codex, key='coordinator', runner=None):
    host = store.get(key + '_host')
    if host:
        state = await HostClient(store.directory / 'hosts' / host['id']).recover_state()
        if state != 'dead' and not HostClient(store.directory / 'hosts' / host['id']).logs_deleted:
            return host.get('provider', 'claude'), host['account']
        with store.db:
            store.put(key + '_host', None)
    preference = effective_agent(store, key)['selection'].get('provider')
    if preference == 'opencode':
        return 'opencode', opencode_runtime.parent_ready(store)
    alias = accounts.select() if preference != 'codex' else None
    if alias is not None:
        return 'claude', alias
    alias = codex.parent_account() if codex_binary(store, runner) else None
    if alias is not None:
        return 'codex', alias
    raise unavailable(store, accounts, codex, key)


def settle_previous(store, key, provider, native, accounts, codex):
    rows = store.get(key + '_lost') or []
    if not rows:
        return
    if provider == 'claude':
        try:
            path = accounts.transcript(native) if native else None
        except Exception:
            path = None
        recorded = lambda rows: CoordinatorSession.transcript_receipts(
            path, rows, store.message_batch_ids(rows)) if path else set()
    elif provider == 'opencode':
        root = opencode_runtime.parent_root(store, key)
        recorded = lambda rows: opencode_runtime.parent_receipts(root, native, rows) if native else set()
    else:
        path = next((path for profile in codex.accounts().values()
                     if (path := transcript_path(profile.get('config_dir'), native))), None)
        recorded = lambda rows: codex_transcript_receipts(path, rows) if path else set()
    settle(store, key, rows, recorded)


def switch_reason(store, previous, accounts, codex, key='coordinator'):
    from . import opencode_accounts
    agent = effective_agent(store, key)
    preference = agent['selection'].get('provider')
    if not agent['picked'] and store.get(key + '_selection_cleared'):
        return 'this chat is back on the default agent'
    if (previous == 'codex' and preference == 'claude' and
            store.get(key + '_selection_revision') == agent['revision']):
        return 'Claude can take work again'
    if agent['picked'] and preference and preference != previous:
        return "the owner selected %s as this chat's own pick" % preference
    if preference and preference != previous:
        return 'the owner selected %s parent chats' % preference
    if previous == 'opencode':
        return 'the OpenCode account changed to %s' % opencode_accounts.active(store)
    if previous == 'claude':
        return ('every Claude account is out of usage' if any(signed_in(store, name, profile)
                for name, profile in (store.get('accounts', {}) or {}).items())
                else 'the previous provider is no longer connected')
    alias = codex.active()
    return ('ChatGPT account %s is out of usage' % codex.label(alias)
            if codex.signed_in(alias) and codex.exhausted(alias, parent=True)
            else 'Claude can take work again' if accounts.select() else 'the previous provider is no longer connected')


async def start(store, runner, cwd, model, instructions, **kwargs):
    from . import opencode_accounts
    from .opencode_parent import OpenCodeParentSession
    accounts, codex = AccountBroker(store), CodexBroker(store)
    from .providers import ProviderRunner
    binary_runner = runner if isinstance(runner, ProviderRunner) else None
    key = session_key(store, cwd, kwargs.get('topic'))
    host = store.get(key + '_host') or {}
    factories = {'claude': CoordinatorSession.start, 'codex': CodexCoordinatorSession.start,
                 'opencode': OpenCodeParentSession.start}
    if not topic_enabled(store, key):
        if not host:
            return None
        factory = factories[host.get('provider', 'claude')]
        return await factory(store, runner, cwd, model, instructions, **kwargs)
    previous = host.get('provider', store.get(key + '_provider', 'claude'))
    original = {suffix: store.get(key + suffix) for suffix in ('_session', '_fresh', '_switch')}
    provider, alias = await choose_parent(store, accounts, codex, key, binary_runner)

    async def launch(provider):
        switched = provider != previous and (original['_session'] or store.get(key + '_provider'))
        saved_opencode = store.get(key + '_opencode') or {}
        if (provider == previous == 'opencode' and saved_opencode.get('account')
                and saved_opencode['account'] != opencode_accounts.active(store) and not store.get(key + '_host')):
            switched = True
        notice = store.get(key + '_switch')
        if switched:
            settle_previous(store, key, previous, original['_session'], accounts, codex)
            notice = {'from': previous, 'to': provider, 'reason': switch_reason(store, previous, accounts, codex, key)}
            with store.db:
                store.put(key + '_session', None)
                store.put(key + '_fresh', None)
                store.put(key + '_switch', notice)
                store.put(key + '_opencode', None)
        factory = factories[provider]
        selected_model = effective_agent(store, key, provider, model)['model']
        session = await factory(store, runner, cwd, selected_model, instructions, **kwargs)
        if session is None:
            return None
        session.switched = bool(notice and notice['to'] == provider)
        session.switch = notice if session.switched else None
        with store.db:
            recover_lost_turns(store, key, switched)
            store.put(key + '_provider', provider)
            if store.get(key + '_selection_cleared') and session.host_id != host.get('id'):
                store.put(key + '_selection_cleared', None)
        return session

    try:
        return await launch(provider)
    except AccountsUnavailable:
        if provider == 'opencode' or effective_agent(store, key)['selection'].get('provider') == 'opencode':
            raise
        if (effective_agent(store, key)['selection'].get('provider') == 'codex' or provider != 'claude' or
                not codex_binary(store, binary_runner) or not codex.parent_account()):
            raise unavailable(store, accounts, codex, key)
        with store.db:
            for suffix, value in original.items():
                store.put(key + suffix, value)
        return await launch('codex')
