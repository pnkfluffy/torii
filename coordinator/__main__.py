import argparse
import asyncio
import fcntl
import json
import os
from pathlib import Path
import re
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import unicodedata

from .log import code_stamp

STARTUP_CODE = code_stamp(Path(__file__).resolve().parents[1])

from .policy import COORDINATOR_MODEL
from .store import Store
from .telegram import Telegram, follow_server


RESTART_DEADLINE_SECONDS = 30


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


def restart_watchdog():
    def expire():
        time.sleep(RESTART_DEADLINE_SECONDS)
        os._exit(1)
    threading.Thread(target=expire, name='restart-watchdog', daemon=True).start()


def bot_name(value):
    value = value.strip()
    if not 1 <= len(value) <= 40 or any(
            unicodedata.category(char) in ('Cc', 'Cf', 'Cs') or char in '\"[]\u2028\u2029' for char in value):
        raise argparse.ArgumentTypeError(
            'Use a name of 1 to 40 characters on one line, without control characters, double quotes, [ or ].')
    return value


def calling_app():
    try:
        pid = os.getppid()
        seen = set()
        while pid > 1 and pid not in seen and len(seen) < 32:
            seen.add(pid)
            result = subprocess.run(['/bin/ps', '-p', str(pid), '-o', 'ppid=', '-o', 'ucomm='],
                                    capture_output=True, timeout=2)
            fields = result.stdout.decode('utf-8', errors='replace').strip().split(None, 1)
            if result.returncode or len(fields) != 2 or not fields[0].isdigit():
                return None
            pid = int(fields[0])
            name = Path(fields[1]).name.lstrip('-')
            if name not in ('sh', 'bash', 'zsh', 'dash', 'fish', 'ksh', 'csh', 'tcsh', 'nu', 'env') and not re.fullmatch(
                    r'python(?:\d+(?:\.\d+)*)?', name, re.IGNORECASE):
                return name
    except Exception:
        return None
    return None


def parser():
    result = argparse.ArgumentParser(description="Torii — owner-only Telegram agent coordinator")
    result.add_argument("--state-dir", type=Path, default=state_dir())
    result.add_argument("--token-file", type=Path, default=Path.home() / ".config/telegram-agent-coordinator/bot-token")
    commands = result.add_subparsers(dest="command", required=True)
    pair = commands.add_parser("pair", help="Print an expiring owner-pairing link or group command")
    pair.add_argument('--replace', action='store_true')
    pair.add_argument('--yes', action='store_true', help='Confirm replacement without a terminal prompt')
    commands.add_parser("status", help="Show linked channels and jobs without credentials")
    records = commands.add_parser('records', help='Manage job records')
    records.add_argument('action', choices=['backfill', 'check', 'restore'])
    records.add_argument('--source', type=Path, help='Records clone to restore into a fresh state directory')
    serve = commands.add_parser("serve", help="Run foreground polling, feed and delivery")
    serve.add_argument("--pair-only", action="store_true", help="Poll and deliver without starting agents")
    serve.add_argument("--coordinator-model",
                       help=f"Operator override for the coordinator model (new install default: {COORDINATOR_MODEL})")
    control = commands.add_parser("ctl", help="Run one owner control operation")
    operations = control.add_subparsers(dest="operation", required=True)
    operations.add_parser("list", help="List every operation")
    description = operations.add_parser("describe", help="Show one operation")
    description.add_argument("op")
    invocation = operations.add_parser("call", help="Run one operation")
    invocation.add_argument("op")
    invocation.add_argument('--from', dest='from_name', type=bot_name, metavar='NAME',
                            help="For events.post, coordinators use --from '<your chat name> coordinator' "
                                 "to show a 🤖 note. Torii workers, lanes, and scripts omit --from; "
                                 "outside programs use their own name. Put --from right after call.")
    invocation.add_argument("--topic", help="Channel ID; defaults to none")
    invocation.add_argument("pairs", nargs="*", metavar="key=value")
    mcp = commands.add_parser('mcp', help='Serve coordinator tools over stdio')
    mcp.add_argument('--topic', help='The parent channel for owner-message authorization')
    host = commands.add_parser('host', help=argparse.SUPPRESS)
    host.add_argument('directory', type=Path)
    return result


async def serve(args, store):
    from . import log
    from .loop_watchdog import LoopWatchdog
    watchdog = LoopWatchdog(asyncio.get_running_loop(), store, log.configure(args.state_dir))
    watchdog.start()
    try:
        return await serve_watched(args, store)
    finally:
        watchdog.stop()


async def serve_watched(args, store):
    from . import log
    from .providers import ProviderRunner
    from .service import Service
    from .telegram import TelegramError
    from .vault import vault_from_environment
    from .accounts import discover_accounts
    from .codex_accounts import discover_codex_accounts
    from . import host_os
    from .isolation import codex_install_problem
    host_os.prepare_trash()
    code_root = Path(__file__).resolve().parents[1]
    home = home_dir()
    log_path = log.configure(args.state_dir)
    logger = log.logger('coordinator.__main__')
    logger.info('service start commit=%s code=%s home=%s pair_only=%s log=%s',
                log.running_commit(code_root), code_root, home, args.pair_only, log_path)
    from . import extension
    extension.active().prepare(store)
    codex = 'codex'
    problem = codex_install_problem(codex)
    if problem:
        from . import problems
        problems.record(store, 'codex', 'helper-missing', problem)
    discover_accounts(store)
    discover_codex_accounts(store)
    api = Telegram(args.token_file, store=store)
    follow_server(store)
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
                      coordinator_model=args.coordinator_model, vault=vault_from_environment(args.state_dir),
                      code=STARTUP_CODE)
    service.on_restart = restart_watchdog
    loop = asyncio.get_running_loop()
    task = asyncio.create_task(service.run())
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, task.cancel)
    logger.info('bridge running pair_only=%s', args.pair_only)
    try:
        await task
    except asyncio.CancelledError:
        logger.info('service stopping on signal')
        return False
    return service.restart_reason is not None


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
        print(json.dumps([describe(op) for op in control_api.OPS], indent=2))
        return 0
    found = control_api.find(args.op)
    if args.operation == "describe":
        if not found:
            print(json.dumps({"ok": False, "error": "Unknown operation: " + args.op}))
            return 2
        print(json.dumps(describe(found), indent=2))
        return 0
    params = {}
    from_name = getattr(args, 'from_name', None)

    def record(state, result=None):
        if not found or found.kind != control_api.WRITE:
            return
        try:
            with store.db:
                topic = control_api.resolve_topic(store, params.get('topic') or args.topic)
                if result and isinstance(result.data, dict):
                    topic = topic or control_api.resolve_topic(store, result.data.get('topic'))
                if result and isinstance(result.data, dict) and result.data.get('request'):
                    request = store.db.execute('SELECT params FROM service_requests WHERE id=?',
                                               (result.data['request'],)).fetchone()
                    queued = json.loads(request['params']) if request else {}
                    topic = topic or queued.get('_topic')
                for parameter, table in (('task', 'tasks'), ('worker', 'workers')):
                    value = params.get(parameter, '')
                    if args.op.startswith(parameter + 's.') and topic is None and value.isascii() and value.isdigit() and len(value) <= 19 and int(value) < 2 ** 63:
                        row = store.db.execute('SELECT topic FROM ' + table + ' WHERE id=?', (int(value),)).fetchone()
                        topic = row['topic'] if row else None
                store.record_cli_call(args.op, from_name, topic, state, calling_app())
        except Exception:
            print("Could not record the CLI call.", file=sys.stderr)

    for pair in args.pairs:
        key, separator, value = pair.partition("=")
        if not separator or not key:
            record('refused')
            print(json.dumps({"ok": False, "error": "Send parameters as key=value."}))
            return 2
        if found and found.params.get(key, '').rstrip('?') == 'bool' and value in ('true', 'false'):
            value = value == 'true'
        params[key] = value
    if found and found.transport == control_api.SERVICE and not service_running(store.directory):
        record('refused')
        print(json.dumps({"ok": False, "op": args.op, "state": "refused",
                          "text": found.id + " needs the running service. Start it, then try again."}))
        return 3
    source_worker = None
    if found and found.kind == control_api.WRITE:
        worker_id = os.environ.get('TORII_WORKER_ID', '')
        task_id = os.environ.get('TORII_TASK_ID', '')
        if all(value.isascii() and value.isdigit() and len(value) <= 18 for value in (worker_id, task_id)):
            row = store.db.execute('SELECT id FROM workers WHERE id=? AND task=?', (int(worker_id), int(task_id))).fetchone()
            source_worker = row['id'] if row else None
        if found.id == 'trash.move' and (worker_id or task_id) and source_worker is None:
            record('refused')
            print(json.dumps({"ok": False, "op": found.id, "state": "refused",
                              "text": "Trash needs a valid worker job."}))
            return 3
    with store.db:
        result = control_api.call(store, args.op, params, topic=args.topic, source="cli", from_name=from_name,
                                  source_worker=source_worker)
    record(result.state, result)
    compact = result.ok and result.op == 'tasks.get' and params.get('full') is not True
    print(json.dumps({"ok": result.ok, "op": result.op, "kind": found.kind if found else None,
                      "state": result.state, "text": result.text, "data": result.data}, default=str,
                     indent=None if compact else 2, separators=(',', ':') if compact else None))
    return {"done": 0, "queued": 0, "refused": 3, "failed": 1}[result.state]


def pair(args, store):
    from .pairing import link
    from .setup_flow import replace_pairing
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
        replace_pairing(store)
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


def open_store(args):
    if (args.command in ('ctl', 'mcp') and STARTUP_CODE is not None
            and (args.state_dir / 'state.sqlite').is_file()):
        store = Store(args.state_dir, token_file=args.token_file, existing=True)
        try:
            running = store.get('service_run') or {}
            if isinstance(running, dict) and running.get('state') == 'running' and running.get('code') == STARTUP_CODE:
                return store
        except (sqlite3.Error, TypeError, ValueError):
            pass
        store.close()
    return Store(args.state_dir, token_file=args.token_file)


def main():
    args = parser().parse_args()
    os.umask(0o077)
    if args.command == 'host':
        from .host import main as host_main
        host_main(args.directory)
        return 0
    store = open_store(args)
    lock = None
    try:
        if args.command == "pair":
            return pair(args, store)
        elif args.command == "status":
            tasks = store.tasks_list()
            workers = [dict(r) for r in store.db.execute(
                "SELECT id,task,topic,provider,status,pid FROM workers ORDER BY id DESC LIMIT 10")]
            print(json.dumps({"owner": store.get("owner"), "group": store.get("group"),
                              **({'mode': store.get('mode'), 'execution': store.get('execution')}
                                 if store.get('mode') is not None else {}),
                              **({'setup_problem': store.get('setup_problem')} if store.get('setup_problem') else {}),
                              **({'group_owner_changed': store.get('group_owner_changed')} if store.get('group_owner_changed') else {}),
                              "topics": store.topics(), "tasks": tasks, "workers": workers}, indent=2))
        elif args.command == 'records':
            from . import records
            if args.action == 'restore':
                if args.source is None:
                    raise ValueError('records restore requires --source')
                result = records.restore(store, args.source)
            else:
                result = records.backfill(store) if args.action == 'backfill' else records.check(store)
            print(json.dumps(result, indent=2))
            return int(result.get('count', 0) != 0 or result.get('bytes', 0) >= 50_000_000)
        elif args.command == "ctl":
            return ctl(args, store)
        elif args.command == "serve":
            lock = os.open(str(args.state_dir / "service.lock"), os.O_CREAT | os.O_RDWR, 0o600)
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)

            async def run_service():
                nonlocal lock
                if await serve(args, store):
                    os.close(lock)
                    lock = None
                    store.close()
                    import logging
                    logging.shutdown()
                    os._exit(0)

            asyncio.run(run_service())
        elif args.command == 'mcp':
            from .mcp import serve as serve_mcp
            serve_mcp(store, STARTUP_CODE, topic=args.topic)
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
