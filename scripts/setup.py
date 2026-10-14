#!/usr/bin/env python3
"""Terminal-only group setup. The status path reads no credential values."""

import argparse
import asyncio
import getpass
import importlib.util
import os
from pathlib import Path
import shlex
import shutil
import secrets
import signal
import subprocess
import sys
import tempfile
import threading
import time
import warnings

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from coordinator.__main__ import service_running, state_dir
from coordinator.bot_token import save_token
from coordinator.pairing import link
from coordinator.setup_flow import PRIVATE_REFUSAL, REMOVED, setup_state, replace_pairing
from coordinator import isolation
from coordinator import setup_terminal
from coordinator.store import Store
from coordinator.telegram import Telegram, TelegramError

ROOT = Path(__file__).resolve().parents[1]


def require_tty():
    if not sys.stdin.isatty() or not sys.stdout.isatty():
        print('Run this in your own terminal: cd ' + shlex.quote(str(ROOT)) + ' && python3 scripts/setup.py')
        return False
    return True


def check_prereqs():
    rows = [('Python ' + '.'.join(map(str, sys.version_info[:3])), 'ok' if sys.version_info >= (3, 9) else '3.9 or newer required')]
    fatal = sys.version_info < (3, 9)
    agent_found = False
    for binary, label in (('claude', 'Claude CLI'), ('codex', 'Codex CLI')):
        path = shutil.which(binary)
        found = False
        if path:
            try:
                found = subprocess.run([path, '--version'], capture_output=True, timeout=10).returncode == 0
            except (OSError, subprocess.TimeoutExpired):
                pass
        problem = isolation.codex_install_problem(path) if found and binary == 'codex' else None
        if problem:
            found = False
        rows.append((label, problem or ('ok' if found else 'not found (optional when the other agent is installed)')))
        agent_found = agent_found or found
    fatal = fatal or not agent_found
    if sys.platform == 'darwin':
        rows.append(('Service manager', 'launchd'))
    elif sys.platform.startswith('linux') and os.getuid() != 0:
        try:
            working = subprocess.run(['systemctl', '--user', 'show-environment'], capture_output=True, timeout=10).returncode == 0
        except (OSError, subprocess.TimeoutExpired):
            working = False
        rows.append(('Service manager', 'systemd user service, linger on at install' if working else 'unavailable; see scripts/run-service.sh'))
        fatal = fatal or not working
    else:
        rows.append(('Service manager', 'Linux root is refused' if sys.platform.startswith('linux') else 'macOS or Linux required'))
        fatal = True
    return rows, fatal


async def check_bot(token_file):
    api = Telegram(token_file)
    bot = await api.call('getMe')
    if not bot.get('is_bot') or not bot.get('username'):
        raise ValueError('The token must belong to a Telegram bot.')
    if (await api.call('getWebhookInfo')).get('url'):
        raise ValueError('This bot has a webhook set. Torii polls instead. Remove the webhook or use a new bot.')
    return api, bot


async def obtain_token(folder, store=None):
    saved = folder / 'bot-token'
    if saved.is_file():
        try:
            api, bot = await check_bot(saved)
        except (TelegramError, ValueError):
            print('The saved token could not be checked. Try a new token.')
        else:
            if setup_terminal.prompt('Found a saved token for @' + bot['username'] + '. Use it? [Y/n] ').strip().lower() != 'n':
                return api, bot
    automatic = setup_terminal.interactive_mac()
    folder.mkdir(mode=0o700, parents=True, exist_ok=True)
    first = True
    if not automatic:
        print('Make a new bot just for Torii.')
        print('A bot used by another program will stop working there when Torii polls it.')
        print('Pick a name and a username ending in "bot", and copy the token it sends.')
        print('No bot yet? Open https://t.me/BotFather?startapp, or send /newbot to @BotFather.')
    try:
        while True:
            if automatic:
                token = setup_terminal.obtain_candidate() if first else setup_terminal.obtain_candidate(show_screen=False)
                first = False
            else:
                setup_terminal.flush_input()
                with warnings.catch_warnings():
                    warnings.simplefilter('error', getpass.GetPassWarning)
                    token = getpass.getpass('Paste the bot token from @BotFather (input is hidden): ')
            with tempfile.TemporaryDirectory(prefix='.token-check-', dir=store.directory if store is not None else folder) as temporary:
                try:
                    candidate_file = save_token(token, Path(temporary))
                except ValueError:
                    print("That token didn't work. Copy it again, or paste it here.")
                    continue
                finally:
                    del token
                print('Got the token. Checking it with Telegram…')
                while True:
                    try:
                        api, bot = await check_bot(candidate_file)
                    except ValueError as error:
                        print(str(error))
                        break
                    except TelegramError as error:
                        if error.code == 401:
                            print("That token didn't work. Copy it again, or paste it here.")
                            break
                        print('Telegram could not be reached. Check the connection.')
                        if setup_terminal.prompt('Retry checking this token? [Y/n] ', retain_hidden=automatic).strip().lower() == 'n':
                            raise
                    else:
                        token_file = save_token(candidate_file.read_text(), folder,
                                                temporary_folder=store.directory if store is not None else None)
                        if isinstance(api, Telegram):
                            api = Telegram(token_file)
                        print('Bot @' + bot['username'] + ' ok.')
                        return api, bot
    except BaseException:
        setup_terminal.restore_input()
        raise


async def confirm_busy_bot(store, api, bot):
    if store.get('bot_id') == bot['id']:
        return
    signals = []
    if await api.call('getMyCommands'):
        signals.append('bot commands are already set')
    description = await api.call('getMyDescription')
    if description.get('description'):
        signals.append('the bot has a description')
    if signals:
        print('This bot may already be in use: ' + ', '.join(signals) + '.')
        print('Torii polling will interrupt another program that reads this bot.')
        if setup_terminal.prompt('Use this bot for Torii anyway? [y/N] ',
                                 retain_hidden=setup_terminal.interactive_mac()).strip().lower() != 'y':
            raise ValueError('Make a new bot with /newbot in @BotFather and rerun setup.')


def open_group_link(username, code):
    url = link(username, code)
    print('Next: add the bot to a Telegram group you own.')
    print('No group yet? In Telegram tap New Group, give it a name, and create it. Then come back here and press Enter.')
    setup_terminal.prompt()
    print('Opening Telegram... pick your group, then tap "Add as admin".')
    if setup_terminal.interactive_mac():
        print('Telegram is opening on this Mac. To use your phone instead, scan this:')
        setup_terminal.open_url(url)
        print('If nothing opened, use this link or QR (valid 30 minutes):')
        setup_terminal.print_link(url)
        return
    print('If nothing opened, use this link (valid 30 minutes): ' + url)
    browser = os.environ.get('BROWSER')
    command = [browser, url] if browser else (
        ['open', url] if sys.platform == 'darwin' else (
            ['xdg-open', url] if os.environ.get('DISPLAY') or os.environ.get('WAYLAND_DISPLAY') else None))
    if command:
        try:
            subprocess.run(command, check=False, timeout=10)
        except (OSError, subprocess.SubprocessError):
            pass


async def wait_paired(store, timeout=1800):
    print('Waiting for you to add the bot... (Ctrl-C to stop)')
    deadline = time.monotonic() + timeout
    paired = False
    while time.monotonic() < deadline:
        if store.get('owner') is not None and store.get('group') is not None:
            if not paired:
                print('Paired with ' + (store.get('owner_name') or 'Telegram user') + ' in "' +
                      (store.get('group_name') or 'Telegram group') + '".')
                print('Waiting for Topics to be turned on... (see the message in the group)')
                paired = True
            if store.get('control_topic'):
                setup_done(store)
                return
        await asyncio.sleep(1)
    raise ValueError('The setup link expired. Run setup again on your Mac.')


async def wait_running(store, code, username):
    open_group_link(username, code)
    await wait_paired(store)


def setup_done(store):
    state = setup_state(store)
    if state['claude'] or state['codex']:
        print('Done. Open the Torii topic in your group and send your first request.')
    else:
        print('Done. Open the Torii topic in your group and tap Add ChatGPT or Connect Claude.')


def install(enable_in_argv=True):
    spec = importlib.util.spec_from_file_location('torii_install_service', ROOT / 'scripts/install-service.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.install(enable_agents=enable_in_argv)


async def connect_claude(store):
    task = asyncio.current_task()
    interrupted = False

    def interrupt(signum, frame):
        nonlocal interrupted
        interrupted = True
        task.cancel()

    previous = signal.signal(signal.SIGINT, interrupt)
    try:
        connected = await _connect_claude(store)
        await asyncio.sleep(0)
        if not connected:
            print('Skipped. The bot will ask you to connect Claude.')
        return connected
    except asyncio.CancelledError:
        if not interrupted:
            raise
        if hasattr(task, 'uncancel'):
            task.uncancel()
        print('Skipped. The bot will ask you to connect Claude.')
        return False
    finally:
        setup_terminal.restore_input()
        with store.db:
            store.put('setup_claude_dir', None)
        signal.signal(signal.SIGINT, previous)


async def _connect_claude(store):
    from coordinator.accounts import authenticated, discover_accounts, signed_in
    from coordinator import extension
    from coordinator.signin import accounts_root, register_claude_account, SignIns
    try:
        with store.db:
            profiles = discover_accounts(store)
        driver = SignIns(store)
        candidates = []
        for alias, profile in profiles.items():
            if profile.get('enabled') or profile.get('awaiting_login'):
                directory = Path(profile['config_dir'])
                identity = await driver.identity(directory)
                if identity and identity.get('email'):
                    candidates.append((alias, directory, identity))
        existing = bool(candidates) or any(authenticated(store, alias, profile) for alias, profile in profiles.items())
        if not existing:
            directory = accounts_root() / ('.torii-' + secrets.token_hex(4))
            directory.mkdir(mode=0o700, parents=True, exist_ok=True)
            with store.db:
                store.put('setup_claude_dir', str(directory.resolve()))
            print('Connect Claude')
            if setup_terminal.interactive_mac():
                line = await setup_terminal.hidden_line(
                    'Press Enter to sign in to Claude in your browser, or Ctrl-C to skip.')
                token_file = Path.home() / '.config/telegram-agent-coordinator/bot-token'
                if token_file.is_file() and token_file.read_text().strip() in line:
                    print("Setup already has the bot token, so you don't need to paste it.")
                del line
                setup_terminal.flush_input()
                setup_terminal.restore_input()
            print('Your browser is opening. Sign in if asked, then click Authorize.')
            print('If a code appears instead, paste it here.')
            print('Press Control-C to skip. You can connect Claude in Telegram later.', flush=True)
            connected = await claude_login(store, directory)
            if not connected:
                with store.db:
                    store.put('setup_claude_dir', None)
                raise ValueError('Sign-in skipped')
            identity = await driver.identity(directory)
            candidates = [(None, directory, identity)] if identity and identity.get('email') else []
            if not candidates:
                print('Claude sign-in finished, but Torii could not read the saved login identity. '
                      'Run setup again to sign in. You can also connect Claude in Telegram later.')
        ready = extension.active()
        for alias, directory, identity in candidates:
            if alias is None and driver.known(identity['email']):
                with store.db:
                    store.put('setup_claude_dir', None)
                continue
            if not await ready.login_ready(str(directory), prompt=print):
                print('Claude sign-in finished, but Torii could not confirm the saved login. '
                      'Run setup again to sign in. You can also connect Claude in Telegram later.')
                continue
            with store.db:
                if alias is None:
                    alias = register_claude_account(store, directory, identity)
                else:
                    snapshots = dict(store.get('account_status', {}) or {})
                    snapshots[alias] = {'identity': {'email': identity['email'], 'logged_in': True}}
                    store.put('account_status', snapshots)
                    profiles = dict(store.get('accounts', {}) or {})
                    profiles[alias] = dict(profiles[alias], enabled=True)
                    profiles[alias].pop('awaiting_login', None)
                    store.put('accounts', profiles)
        ready = any(signed_in(store, alias, profile) for alias, profile in (store.get('accounts', {}) or {}).items())
        if ready:
            from coordinator.setup_flow import enabled_profiles
            with store.db:
                store.put('setup_accounts_checked', {'checked': time.time(), 'profiles': enabled_profiles(store)})
                store.put('execution', 'agents')
                from coordinator.setup_flow import setup_status
                setup_status(store, force=True)
            return True
    except (KeyboardInterrupt, EOFError, OSError, ValueError, RuntimeError, subprocess.SubprocessError, asyncio.TimeoutError):
        pass
    return False


async def claude_login(store, directory):
    from contextlib import nullcontext
    from coordinator.accounts import AccountBroker
    process = None
    finished = False
    setup_terminal.flush_input()
    tty = setup_terminal.terminal() if setup_terminal.interactive_mac() else nullcontext()
    try:
        with tty:
            try:
                process = await asyncio.create_subprocess_exec(
                    'claude', 'auth', 'login', '--claudeai',
                    env=AccountBroker.signin_environment(directory), cwd=str(directory))
                finished = await process.wait() == 0
                return finished
            finally:
                if process is not None and process.returncode is None:
                    try:
                        process.terminate()
                    except ProcessLookupError:
                        pass
                    try:
                        await asyncio.wait_for(process.wait(), 1)
                    except asyncio.TimeoutError:
                        process.kill()
                        await process.wait()
    finally:
        if not finished:
            with store.db:
                store.put('setup_claude_dir', None)


def status():
    directory = state_dir()
    token_saved = (Path.home() / '.config/telegram-agent-coordinator/bot-token').is_file()
    values = {'Token saved': 'yes' if token_saved else 'no', 'Bot': 'unknown', 'Paired': 'no',
              'Mode': 'unset', 'Agents': 'off', 'Service running': 'no', 'Claude': 'not connected'}
    if (directory / 'state.sqlite').is_file():
        store = Store(directory, read_only=True)
        try:
            if store.get('mode') == 'private':
                print(PRIVATE_REFUSAL)
                return 1
            problem = store.get('setup_problem')
            if problem:
                print(REMOVED if problem == 'removed' else 'Setup problem: ' + problem)
                if problem == 'removed' and store.get('bot_username'):
                    print(link(store.get('bot_username'), 'fix'))
            if store.get('group_owner_changed'):
                print(store.get('group_owner_changed'))
            values.update({'Bot': '@' + (store.get('bot_username') or 'unknown'),
                           'Paired': 'yes' if store.get('owner') is not None else 'no',
                           'Mode': store.get('mode') or 'unset',
                           'Agents': 'on' if store.get('execution', 'agents') == 'agents' and not store.get('pair_only', False) else 'off',
                           'Service running': 'yes' if service_running(directory) else 'no',
                           'Claude': 'connected' if setup_state(store)['claude'] else 'not connected',
                           'ChatGPT': 'connected' if setup_state(store)['codex'] else 'not connected',
                           'Main chat': {'claude': 'Claude', 'chatgpt': 'ChatGPT'}.get(setup_state(store)['main'], 'not ready')})
        finally:
            store.close()
    for name, value in values.items():
        print(name + ': ' + str(value))
    return 0


async def setup(connect=None):
    directory = state_dir()
    store = Store(directory)
    if connect is None:
        connect = setup_terminal.interactive_mac() and not any(
            profile.get('enabled') for profile in (store.get('accounts', {}) or {}).values())
    converted = False
    try:
        if store.get('mode') == 'private':
            print(PRIVATE_REFUSAL)
            if setup_terminal.prompt('Move this install to a group now? Your projects and history stay. [y/N] ').strip().lower() != 'y':
                return 1
            replace_pairing(store)
            converted = True
        if store.get('owner') is not None and store.get('control_topic'):
            if connect:
                await connect_claude(store)
            if not service_running(store.directory):
                install()
            status()
            setup_done(store)
            return 0
        print('Torii setup\n')
        rows, fatal = check_prereqs()
        if fatal:
            for label, value in rows:
                print('  ' + label + ' ... ' + value)
            print('Install Claude Code (https://code.claude.com/docs/en/setup) or the Codex CLI, then run setup again.')
            return 1
        print('Checking this Mac... ok')
        running = service_running(store.directory) and not converted
        if running:
            username = store.get('bot_username')
            if not username:
                _, bot = await check_bot(Path.home() / '.config/telegram-agent-coordinator/bot-token')
                username = bot['username']
        else:
            api, bot = await obtain_token(Path.home() / '.config/telegram-agent-coordinator', store)
            await confirm_busy_bot(store, api, bot)
            await api.set_default_admin_rights()
            username = bot['username']
            with store.db:
                if store.get('bot_id') not in (None, bot['id']):
                    store.put('offset', 0)
                store.put('bot_id', bot['id'])
                store.put('bot_username', username)
        if connect and setup_terminal.interactive_mac():
            await connect_claude(store)
        with store.db:
            if store.get('execution') is None:
                store.put('execution', 'pairing')
        code = store.pairing_code()
        if not running:
            print('Bot @' + username + ' ok. Starting Torii in the background...', end=' ')
            install()
            print('ok')
        await wait_running(store, code, username)
        if connect and not setup_terminal.interactive_mac():
            await connect_claude(store)
        return 0
    finally:
        setup_terminal.restore_input()
        store.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--status', action='store_true')
    connection = parser.add_mutually_exclusive_group()
    connection.add_argument('--connect-claude-in-terminal', '--connect', dest='connect', action='store_true',
                            help='Connect Claude in the terminal (default on an interactive Mac)')
    connection.add_argument('--skip-claude', dest='connect', action='store_false',
                            help='Connect Claude later in Telegram')
    parser.set_defaults(connect=None)
    args = parser.parse_args(argv)
    if args.status:
        return status()
    if not require_tty():
        return 1
    os.umask(0o077)
    previous = None
    if threading.current_thread() is threading.main_thread():
        def interrupt(signum, frame):
            raise KeyboardInterrupt()
        previous = signal.signal(signal.SIGINT, interrupt)
    try:
        return asyncio.run(setup(connect=args.connect))
    except (KeyboardInterrupt, EOFError, getpass.GetPassWarning):
        print('\nCancelled. Run setup in your own terminal.')
        return 1
    except ValueError as error:
        print(str(error))
        return 1
    except TelegramError as error:
        if error.code == 401:
            print('Telegram rejected this token. Copy the current token from BotFather and rerun setup.')
        else:
            print('Telegram could not be reached. Check the connection and rerun setup.')
        return 1
    except (OSError, subprocess.SubprocessError):
        print('The service failed to install or start. Check the service manager and rerun setup.')
        return 1
    finally:
        setup_terminal.restore_input()
        if previous is not None:
            signal.signal(signal.SIGINT, previous)


if __name__ == '__main__':
    raise SystemExit(main())
