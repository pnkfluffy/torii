import argparse
import asyncio
import fcntl
import json
import os
from pathlib import Path
import signal
import sys

from .policy import COORDINATOR_MODEL
from .store import Store
from .telegram import Telegram


def state_dir():
    """One resolution for the service and the CLI, so both open the same database."""
    return Path(os.environ.get("TORII_STATE_DIR") or Path.home() / ".local/state/telegram-agent-coordinator")


def home_dir():
    """The coordinator's working folder, which also selects the home topic.

    A pinned release sets TORII_HOME to the development checkout. Code then loads
    from the release while the native session keeps its project folder.
    """
    home = Path(os.environ.get("TORII_HOME") or Path(__file__).resolve().parents[1]).expanduser()
    if not home.is_absolute() or not home.is_dir():
        raise SystemExit("TORII_HOME must name an existing absolute folder.")
    return home.resolve()


def parser():
    result = argparse.ArgumentParser(description="Torii — owner-only Telegram agent coordinator")
    result.add_argument("--state-dir", type=Path, default=state_dir())
    result.add_argument("--token-file", type=Path, default=Path.home() / ".config/telegram-agent-coordinator/bot-token")
    commands = result.add_subparsers(dest="command", required=True)
    pair = commands.add_parser("pair", help="Print an expiring owner-pairing link or group command")
    pair.add_argument('--group', action='store_true', help=argparse.SUPPRESS)
    pair.add_argument('--replace', action='store_true')
    pair.add_argument('--yes', action='store_true', help='Confirm replacement without a terminal prompt')
    commands.add_parser("status", help="Show linked channels and jobs without credentials")
    commands.add_parser("bot", help="Check the Telegram bot identity")
    accounts = commands.add_parser('accounts', help='Register existing local Claude account profiles')
    accounts.add_argument('action', choices=['discover', 'check'])
    serve = commands.add_parser("serve", help="Run foreground polling, feed and delivery")
    serve.add_argument("--pair-only", action="store_true", help="Poll and deliver without starting agents")
    serve.add_argument("--coordinator-model", default=COORDINATOR_MODEL,
                       help=f"Operator override for the coordinator model (default: {COORDINATOR_MODEL})")
    control = commands.add_parser("ctl", help="Run one owner control operation")
    operations = control.add_subparsers(dest="operation", required=True)
    listing = operations.add_parser("list", help="List every operation")
    listing.add_argument("--kind", choices=["read", "write"])
    description = operations.add_parser("describe", help="Show one operation")
    description.add_argument("op")
    invocation = operations.add_parser("call", help="Run one operation")
    invocation.add_argument("op")
    invocation.add_argument("--topic", help="Channel ID; defaults to none")
    invocation.add_argument("pairs", nargs="*", metavar="key=value")
    bind = commands.add_parser("bind", help="Link an observed channel to a local project")
    bind.add_argument("--topic", required=True, help="Use --topic=-100123:4 for a group channel")
    bind.add_argument("--cwd", type=Path, required=True)
    bind.add_argument("--name", required=True)
    bind.add_argument("--provider", choices=["claude", "codex"])
    bind.add_argument("--session")
    bind.add_argument("--source-pid", type=int)
    bind.add_argument("--enable", action="store_true")
    commands.add_parser('mcp', help='Serve coordinator tools over stdio')
    host = commands.add_parser('host', help=argparse.SUPPRESS)
    host.add_argument('directory', type=Path)
    return result


async def serve(args, store):
    if store.get('mode') == 'private':
        import logging
        from .setup_flow import PRIVATE_REFUSAL
        with store.db:
            store.put('setup_problem', 'private_mode_removed')
            store.put('pair_only', True)
        logging.getLogger(__name__).error(PRIVATE_REFUSAL)
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        await stop.wait()
        return
    from . import log
    from .providers import ProviderRunner
    from .service import Service
    from .telegram import TelegramError
    from .vault import vault_from_environment
    from .accounts import discover_accounts
    from .codex_accounts import discover_codex_accounts
    from . import host_os
    from .isolation import codex_binary, codex_install_problem
    host_os.prepare_trash()
    code_root = Path(__file__).resolve().parents[1]
    home = home_dir()
    log_path = log.configure(args.state_dir)
    logger = log.logger('coordinator.__main__')
    logger.info('service start commit=%s code=%s home=%s pair_only=%s log=%s',
                log.running_commit(code_root), code_root, home, args.pair_only, log_path)
    codex = codex_binary(store)
    problem = codex_install_problem(codex)
    if problem:
        from . import problems
        problems.record(store, 'codex', 'helper-missing', problem)
    discover_accounts(store)
    discover_codex_accounts(store)
    api = Telegram(args.token_file, store=store)
    webhook = await api.call("getWebhookInfo")
    if webhook.get("url"):
        raise RuntimeError("This bot has a webhook. Remove it deliberately before using long polling.")
    try:
        me = await api.call("getMe")
        with store.db:
            store.put("bot_username", me["username"])
            store.put('bot_id', me['id'])
    except TelegramError as error:
        logger.info('bot identity retry code=%s', error.code)
    runner = ProviderRunner(args.state_dir, binaries={'claude': 'claude', 'codex': codex},
                            codex_model=lambda: store.get('codex_model'))
    service = Service(store, api, runner, home, args.pair_only,
                      coordinator_model=args.coordinator_model, vault=vault_from_environment(args.state_dir))
    loop = asyncio.get_running_loop()
    task = asyncio.create_task(service.run())
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, task.cancel)
    logger.info('bridge running pair_only=%s', args.pair_only)
    try:
        await task
    except asyncio.CancelledError:
        logger.info('service stopping on signal')


def service_running(directory):
    """The lock the running service holds. A free lock means no service to hand work to."""
    probe = os.open(str(directory / "service.lock"), os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (BlockingIOError, OSError):
        return True
    finally:
        os.close(probe)
    return False


def describe(op):
    return {"id": op.id, "kind": op.kind, "transport": op.transport, "params": op.params,
            "description": op.description}


def ctl(args, store):
    """Operations run against the store the service shares. Two need the running process."""
    from . import control_api
    if args.operation == "list":
        print(json.dumps([describe(op) for op in control_api.OPS
                          if not args.kind or op.kind == args.kind], indent=2))
        return 0
    found = control_api.find(args.op)
    if args.operation == "describe":
        if not found:
            print(json.dumps({"ok": False, "error": "Unknown operation: " + args.op}))
            return 2
        print(json.dumps(describe(found), indent=2))
        return 0
    params = {}
    for pair in args.pairs:
        key, separator, value = pair.partition("=")
        if not separator or not key:
            print(json.dumps({"ok": False, "error": "Send parameters as key=value."}))
            return 2
        params[key] = value
    if found and found.transport == control_api.SERVICE and not service_running(store.directory):
        print(json.dumps({"ok": False, "op": args.op, "state": "refused",
                          "text": found.id + " needs the running service. Start it, then try again."}))
        return 3
    with store.db:
        result = control_api.call(store, args.op, params, topic=args.topic, source="cli")
    print(json.dumps({"ok": result.ok, "op": result.op, "kind": found.kind if found else None,
                      "state": result.state, "text": result.text, "data": result.data}, indent=2, default=str))
    return {"done": 0, "queued": 0, "refused": 3, "failed": 1}[result.state]


def pair(args, store):
    from .pairing import link
    from .setup_flow import PRIVATE_REFUSAL, replace_pairing
    if store.get('mode') == 'private' and not args.replace:
        print(PRIVATE_REFUSAL)
        return 1
    if args.replace:
        if not getattr(args, 'yes', False):
            if not sys.stdin.isatty():
                print('Replacing pairing needs your terminal. Run with --yes to confirm without a prompt.')
                return 1
            try:
                confirmed = input('Move this install to a group now? Your projects and history stay. [y/N] ').strip().lower() == 'y'
            except EOFError:
                confirmed = False
            if not confirmed:
                print('Pairing unchanged.')
                return 1
        legacy = store.get('mode') == 'private'
        replace_pairing(store)
        if legacy:
            print('Run setup again or ./scripts/install-service.py to restart the paused service before using this link.')
    elif store.get('owner') is not None:
        print('Already paired. Use --replace to move Torii to another group.')
        return 0
    with store.db:
        if store.get('execution') is None:
            store.put('execution', 'pairing')
    code = store.pairing_code()
    username = store.get('bot_username')
    if username:
        print('Open this link in Telegram to add Torii to a group you own (expires in 30 minutes):')
        print(link(username, code))
    else:
        print('Send this in a group you own (expires in 30 minutes):')
        print('/pair ' + code)
    return 0


def main():
    args = parser().parse_args()
    os.umask(0o077)
    if args.command == 'host':
        from .host import main as host_main
        host_main(args.directory)
        return 0
    store = Store(args.state_dir, token_file=args.token_file)
    lock = None
    try:
        if args.command == "pair":
            return pair(args, store)
        elif args.command == "status":
            if store.get('mode') == 'private':
                from .setup_flow import PRIVATE_REFUSAL
                print(PRIVATE_REFUSAL)
                return 1
            tasks = store.tasks_list()
            workers = [dict(r) for r in store.db.execute(
                "SELECT id,task,topic,provider,status,pid FROM workers ORDER BY id DESC LIMIT 10")]
            print(json.dumps({"owner": store.get("owner"), "group": store.get("group"),
                              **({'mode': store.get('mode'), 'execution': store.get('execution')}
                                 if store.get('mode') is not None else {}),
                              **({'setup_problem': store.get('setup_problem')} if store.get('setup_problem') else {}),
                              **({'group_owner_changed': store.get('group_owner_changed')} if store.get('group_owner_changed') else {}),
                              "topics": store.topics(), "tasks": tasks, "workers": workers}, indent=2))
        elif args.command == "bot":
            me = asyncio.run(Telegram(args.token_file).call("getMe"))
            print(json.dumps({k: me.get(k) for k in ("id", "username", "can_read_all_group_messages")}, indent=2))
        elif args.command == 'accounts':
            if args.action == 'check':
                from .account_status import check_accounts
                checks = asyncio.run(check_accounts(store))
                print(json.dumps({'accounts': checks}, indent=2))
                return int(any(value != 'ready' for value in checks.values()))
            from .accounts import discover_accounts
            print(json.dumps({'registered': list(discover_accounts(store))}))
        elif args.command == "ctl":
            return ctl(args, store)
        elif args.command == "bind":
            from .workers import process_alive
            if args.enable and process_alive(args.source_pid):
                raise ValueError("Source process is still alive; bind disabled until ownership transfers")
            store.bind(args.topic, args.cwd, args.name, args.provider, args.session, args.enable, args.source_pid)
            print("Channel link saved. Accepting work:", args.enable)
        elif args.command == "serve":
            lock = os.open(str(args.state_dir / "service.lock"), os.O_CREAT | os.O_RDWR, 0o600)
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            asyncio.run(serve(args, store))
        elif args.command == 'mcp':
            from .mcp import serve as serve_mcp
            serve_mcp(store)
        return 0
    except (OSError, ValueError, RuntimeError) as error:
        print(f"Cannot continue: {error}")
        return 1
    finally:
        if lock is not None:
            os.close(lock)
        store.close()


if __name__ == "__main__":
    raise SystemExit(main())
