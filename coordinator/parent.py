"""Choose a provider only when a topic's parent launches."""

import time

from .accounts import AccountBroker, account_pick, signed_in
from .codex_accounts import CodexBroker
from .codex_session import CodexCoordinatorSession, codex_binary, codex_transcript_receipts, transcript_path
from .host import HostClient
from . import opencode_runtime
from .parent_state import (effective_agent, failed_start, missing_cli, provider_out, providers, recover_lost_turns,
                           settle, topic_enabled)
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


def ready(store, accounts, codex, key, provider, runner=None, model=None):
    """The account that can run this chat on a provider now, or None."""
    failed = store.get(key + '_start_failed') or {}
    if failed.get('provider') == provider and failed.get('until', 0) > time.time():
        return None
    if provider == 'claude':
        return accounts.select(model=effective_agent(store, key, 'claude', model)['model'])
    if provider == 'codex':
        return codex.parent_account() if codex_binary(store, runner) else None
    if provider == 'opencode':
        try:
            return opencode_runtime.parent_ready(store)
        except AccountsUnavailable:
            return None
    from .extension import optional
    return optional('antigravity_parent').account(store, key, effective_agent(store, key, 'antigravity')['model'])


def route(store, accounts, codex, key='coordinator', runner=None, model=None):
    """The provider that runs this chat now, and its account or None when the chat must wait.

    A chat runs on its own provider. While that provider is out, it runs on the next provider
    of its queue that has an account for it.
    """
    order = providers(store, key)
    if provider_out(store, key, order[0], accounts, model):
        for provider in order[1:]:
            alias = ready(store, accounts, codex, key, provider, runner, model)
            if alias is not None:
                return provider, alias
    return order[0], ready(store, accounts, codex, key, order[0], runner, model)


def available(store, accounts, codex, runner=None, key='coordinator'):
    return route(store, accounts, codex, key, runner)[1] is not None


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
    if preference == 'antigravity':
        from .extension import optional
        antigravity_parent = optional('antigravity_parent')
        return antigravity_parent.unavailable(store, key)
    if preference == 'codex':
        reset = codex.account_state(alias, parent=True).get('until')
        return AccountsUnavailable(reset, provider=preference, alias=alias)
    resets = [reset for reset in (accounts.earliest_reset(model=effective_agent(store, key, 'claude')['model']), codex.account_state(alias, parent=True).get('until'))
              if reset is not None]
    known_codex = codex.managed()
    known_claude = any(signed_in(store, name, profile)
                       for name, profile in (store.get('accounts', {}) or {}).items())
    provider = 'both' if known_codex and known_claude else 'codex' if known_codex else 'claude'
    return AccountsUnavailable(min(resets) if resets else None, provider=provider, alias=alias)


async def retry_paused(store, pause_key):
    notice = store.get(pause_key + '_notice') or {}
    provider = (notice.get('key') or [None])[0]
    if provider == 'antigravity':
        from .extension import optional
        antigravity_accounts = optional('antigravity_accounts')
        alias = (store.get(pause_key.removesuffix('_selection') + '_antigravity') or {}).get('account')
        for name in [alias] if alias else antigravity_accounts.accounts(store):
            await antigravity_accounts.refresh_usage(store, name)
    elif provider == 'opencode':
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
                key = pause_key.removesuffix('_selection') if pause_key.endswith('_selection') else 'coordinator'
                alias = (account_pick(store, effective_agent(store, key, 'claude')['model'])
                         or store.get('active_account') or
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
                if (blocks.get(alias, {}).get('reason') in ('quota', 'threshold')
                        and blocks[alias].get('credits') is not True and not CodexBroker(store).has_credits(alias)):
                    blocks.pop(alias)
                    store.put('codex_account_blocks', blocks)
            await refresh_codex_accounts(store)


async def choose_parent(store, accounts, codex, key='coordinator', runner=None, model=None):
    host = store.get(key + '_host')
    if host:
        state = await HostClient(store.directory / 'hosts' / host['id']).recover_state()
        if state != 'dead' and not HostClient(store.directory / 'hosts' / host['id']).logs_deleted:
            return host.get('provider', 'claude'), host['account']
        with store.db:
            store.put(key + '_host', None)
    if providers(store, key)[0] == 'codex' and not codex_binary(store, runner):
        missing_cli(store, key, 'codex', runner.binaries['codex'] if runner else 'codex')
    provider, alias = route(store, accounts, codex, key, runner, model)
    if alias is None:
        raise unavailable(store, accounts, codex, key)
    return provider, alias


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
    elif provider == 'antigravity':
        from .extension import optional
        journal_receipts = optional('antigravity_parent').journal_receipts
        host = (store.get(key + '_antigravity') or {}).get('host')
        path = store.directory / 'hosts' / host / 'writes.jsonl' if host else None
        recorded = lambda rows: journal_receipts(path, rows) if path else set()
    elif provider == 'opencode':
        root = opencode_runtime.parent_root(store, key)
        recorded = lambda rows: opencode_runtime.parent_receipts(root, native, rows) if native else set()
    else:
        path = next((path for profile in codex.accounts().values()
                     if (path := transcript_path(profile.get('config_dir'), native))), None)
        recorded = lambda rows: codex_transcript_receipts(path, rows) if path else set()
    settle(store, key, rows, recorded)


def switch_reason(store, previous, provider, accounts, codex, key='coordinator'):
    from . import opencode_accounts
    agent = effective_agent(store, key)
    preference = agent['selection'].get('provider')
    order = providers(store, key)
    labels = {'claude': 'Claude', 'codex': 'ChatGPT', 'opencode': 'OpenCode', 'antigravity': 'Antigravity'}
    current = store.get(key + '_selection_revision') == agent['revision']
    failed = store.get(key + '_start_failed') or {}
    if previous == preference and failed.get('provider') == previous:
        if failed.get('missing'):
            return 'the %s CLI is missing' % previous
        return 'the %s agent did not start' % labels[previous]
    connected = (codex.managed() if order[0] == 'codex' else order[0] != 'claude' or any(
        signed_in(store, name, profile) for name, profile in (store.get('accounts', {}) or {}).items()))
    if provider in order[1:] and connected and provider_out(store, key, order[0], accounts):
        return '%s is out of usage' % labels[order[0]]
    if not agent['picked'] and store.get(key + '_selection_cleared'):
        return 'this chat is back on the default agent'
    if provider == order[0] and previous in order[1:] and (current or not preference):
        return '%s can take work again' % labels[provider]
    if agent['picked'] and preference and preference != previous:
        return "the owner selected %s as this chat's own pick" % preference
    if preference and preference != previous:
        return 'the owner selected %s parent chats' % preference
    if previous == 'opencode':
        return 'the OpenCode account changed to %s' % opencode_accounts.active(store)
    if previous == 'antigravity':
        return 'the Antigravity account changed'
    return 'the previous provider is no longer connected'


async def start(store, runner, cwd, model, instructions, **kwargs):
    from . import opencode_accounts
    from .extension import optional
    from .opencode_parent import OpenCodeParentSession
    accounts, codex = AccountBroker(store), CodexBroker(store)
    from .providers import ProviderRunner
    binary_runner = runner if isinstance(runner, ProviderRunner) else None
    key = session_key(store, cwd, kwargs.get('topic'))
    host = store.get(key + '_host') or {}
    factories = {'claude': CoordinatorSession.start, 'codex': CodexCoordinatorSession.start,
                 'opencode': OpenCodeParentSession.start}
    if optional('antigravity_runtime') is not None:
        from .extension import optional
        AntigravityParentSession = optional('antigravity_parent').AntigravityParentSession
        factories['antigravity'] = AntigravityParentSession.start
    if not topic_enabled(store, key):
        if not host:
            return None
        factory = factories[host.get('provider', 'claude')]
        return await factory(store, runner, cwd, model, instructions, **kwargs)
    previous = host.get('provider', store.get(key + '_provider', 'claude'))
    original = {suffix: store.get(key + suffix) for suffix in ('_session', '_fresh', '_switch')}
    provider, alias = await choose_parent(store, accounts, codex, key, binary_runner, model)

    async def launch(provider, alias):
        switched = provider != previous and (original['_session'] or store.get(key + '_provider'))
        saved_opencode = store.get(key + '_opencode') or {}
        if (provider == previous == 'opencode' and saved_opencode.get('account')
                and saved_opencode['account'] != opencode_accounts.active(store) and not store.get(key + '_host')):
            switched = True
        saved_antigravity = store.get(key + '_antigravity') or {}
        if (provider == previous == 'antigravity' and saved_antigravity.get('account')
                and saved_antigravity['account'] != alias and not store.get(key + '_host')):
            switched = True
        notice = store.get(key + '_switch')
        if switched:
            settle_previous(store, key, previous, original['_session'], accounts, codex)
            notice = {'from': previous, 'to': provider,
                      'reason': switch_reason(store, previous, provider, accounts, codex, key)}
            with store.db:
                store.put(key + '_session', None)
                store.put(key + '_fresh', None)
                store.put(key + '_switch', notice)
                store.put(key + '_opencode', None)
                store.put(key + '_antigravity', None)
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
            if (store.get(key + '_start_failed') or {}).get('provider') == provider:
                store.put(key + '_start_failed', None)
            if store.get(key + '_selection_cleared') and session.host_id != host.get('id'):
                store.put(key + '_selection_cleared', None)
        return session

    try:
        return await launch(provider, alias)
    except Exception as error:
        pick_failed = provider != 'claude' and provider == effective_agent(store, key)['selection'].get('provider')
        if isinstance(error, BlockingIOError) or not (pick_failed or isinstance(error, AccountsUnavailable)):
            raise
        order = providers(store, key)
        later = order[order.index(provider) + 1:] if provider in order else []
        found = ((name, ready(store, accounts, codex, key, name, binary_runner)) for name in later)
        target = next((item for item in found if item[1] is not None), None)
        if target is None and provider == order[0]:
            raise
        with store.db:
            for suffix, value in original.items():
                store.put(key + suffix, value)
        if target is None:
            raise unavailable(store, accounts, codex, key) from error
        left = store.get(key + '_host')
        if left:
            await HostClient(store.directory / 'hosts' / left['id']).stop()
            with store.db:
                store.put(key + '_host', None)
        if pick_failed:
            failed_start(store, key, provider)
        return await launch(*target)
