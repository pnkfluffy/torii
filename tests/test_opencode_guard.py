"""Guard event regressions and opt-in pinned OpenCode registration checks."""

from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import pwd
import shutil
import subprocess
import tempfile
import threading
import unittest
from unittest.mock import patch

from coordinator import opencode_runtime
from tests.test_opencode_contract import BINARY_ENV, capture_run, prompt_responses, stub_servers


GUARD = Path(opencode_runtime.__file__).with_name('opencode_guard.js')
NODE_DRIVER = r'''
import { readFileSync, existsSync, writeFileSync } from "node:fs"
import { pathToFileURL } from "node:url"
const input = JSON.parse(readFileSync(0, "utf8"))
process.env.XDG_DATA_HOME = input.root + "/data"
const plugin = (await import(pathToFileURL(input.guard).href)).default
const aborts = []
const hooks = await plugin({ client: { session: {
  abort: async ({ path }) => { aborts.push(path.id) }
} } })
const before = existsSync(input.root + "/guard-ready.json")
await hooks.config?.({})
const refused = []
if (input.capture) writeFileSync(input.root + "/provider-error.json", typeof input.capture === "string" ? input.capture : JSON.stringify(input.capture), { mode: 0o600 })
for (const event of input.events ?? []) await hooks.event({ event })
for (const part of input.parts) {
  await hooks.event({ event: { type: "message.part.updated", properties: { part } } })
  try { await hooks["chat.params"]({ sessionID: part.sessionID }) }
  catch (error) { refused.push(error.message) }
}
const read = (name) => {
  try { return JSON.parse(readFileSync(input.root + "/" + name, "utf8")) } catch { return null }
}
process.stdout.write(JSON.stringify({ before, ready: read("guard-ready.json"),
  marker: read("stream-incomplete.json"), providerError: read("provider-error.json"), aborts, refused }))
'''

SHELL_DRIVER = r'''
import { readFileSync, statSync } from "node:fs"
import { spawn } from "node:child_process"
import { dirname } from "node:path"
import { pathToFileURL } from "node:url"
const input = JSON.parse(readFileSync(0, "utf8"))
process.env.XDG_DATA_HOME = input.root + "/data"
const plugin = (await import(pathToFileURL(input.guard).href)).default
const hooks = await plugin({ client: {} })
const config = {}
await hooks.config(config)
const observations = await Promise.all(input.runs.map(async (run) => {
  const context = { sessionID: run.session, callID: "same-call" }
  const extra = { env: {} }
  const output = { title: run.command, output: run.original ?? "(no output)", metadata: {} }
  try {
    if (!run.missing) await hooks["shell.env"](context, extra)
    let wire = ""
    if (!run.unstarted && !run.missing) {
      output.metadata.exit = await new Promise((resolve, reject) => {
        const child = spawn(config.shell, ["-c", run.command], {
          cwd: input.root, env: { ...process.env, ...extra.env }, stdio: ["ignore", "pipe", "pipe"]
        })
        child.stdout.on("data", (data) => { wire += data })
        child.stderr.on("data", (data) => { wire += data })
        child.on("error", reject)
        child.on("close", resolve)
      })
    }
    await hooks["tool.execute.after"]({ ...context, tool: run.tool ?? "bash" }, output)
    const path = extra.env.TORII_SHELL_OUTPUT
    return { output, wire, full: path ? readFileSync(path, "utf8") : null,
      mode: path ? statSync(path).mode & 0o777 : null,
      folderMode: path ? statSync(dirname(path)).mode & 0o777 : null,
      retained: output.metadata.outputPath ? readFileSync(output.metadata.outputPath, "utf8") : null,
      retainedMode: output.metadata.outputPath ? statSync(output.metadata.outputPath).mode & 0o777 : null }
  } catch (error) { return { error: error.message } }
}))
const extra = { env: {} }
await hooks["shell.env"]({}, extra)
process.stdout.write(JSON.stringify({ config, observations, extra }))
'''


class OpenCodeShellCaptureTests(unittest.TestCase):
    def drive(self, runs):
        self.assertIsNotNone(shutil.which('node'), 'Node is required for offline shell capture checks')
        with tempfile.TemporaryDirectory(prefix='opencode-shell-node-') as temporary:
            root = Path(temporary)
            guard = root / 'config/opencode/plugins/guard.mjs'
            guard.parent.mkdir(parents=True)
            guard.write_bytes(GUARD.read_bytes())
            (root / 'guard-launch.json').write_text(json.dumps({'version': 1, 'launch': 'a' * 32}))
            completed = subprocess.run(['node', '--input-type=module', '-e', SHELL_DRIVER],
                                       input=json.dumps({'root': str(root), 'guard': str(guard), 'runs': runs}),
                                       text=True, capture_output=True, timeout=20, check=True)
            return json.loads(completed.stdout)

    def test_fast_stdout_stderr_exit_and_shell_syntax_errors(self):
        for command, expected, code in (("printf 'tool-ok'", 'tool-ok', 0),
                                       ("printf out; printf err >&2; exit 7", 'outerr', 7),
                                       ("printf 'quote\" and apostrophe'", 'quote" and apostrophe', 0),
                                       ('true', '', 0)):
            with self.subTest(command=command):
                value = self.drive([dict(session='one', command=command)])
                self.assertEqual(value['config']['shell'], '/bin/bash')
                self.assertEqual(value['extra'], {'env': {}})
                observed = value['observations'][0]
                self.assertEqual(observed['wire'], '')
                self.assertEqual(observed['full'], expected)
                self.assertEqual(observed['output'], {'title': command, 'output': expected or '(no output)',
                                                     'metadata': {'exit': code, 'output': expected or '(no output)',
                                                                  'truncated': False}})
                self.assertEqual(observed['mode'], 0o600)
                self.assertEqual(observed['folderMode'], 0o700)
        observed = self.drive([dict(session='one', command='if then')])['observations'][0]
        self.assertEqual(observed['output']['metadata']['exit'], 2)
        self.assertIn('syntax error', observed['output']['output'])
        self.assertEqual(observed['output']['output'], observed['full'])

    def test_concurrent_sessions_and_nested_bash_keep_separate_output(self):
        commands = ["printf one; bash -c 'printf nested'; printf end",
                    'printf two; sleep 0.02; printf end']
        value = self.drive([dict(session=str(i), command=command) for i, command in enumerate(commands)])
        self.assertEqual([row['output']['output'] for row in value['observations']], ['onenestedend', 'twoend'])

    def test_byte_and_line_truncation_keeps_full_output_and_timeout_notice(self):
        notice = '\n\n<shell_metadata>\nUser aborted the command\n</shell_metadata>'
        for command, expected in (("printf '%s' '" + 'x' * 60000 + "'", 'x' * 60000),
                                  ("printf '%s' '" + '€' * 18000 + "'", '€' * 18000),
                                  ("printf 'line\\n%.0s' {1..2100}", 'line\n' * 2100)):
            with self.subTest(size=len(expected)):
                row = self.drive([dict(session='one', command=command, original='(no output)' + notice)])['observations'][0]
                self.assertEqual(row['full'], expected)
                self.assertTrue(row['output']['metadata']['truncated'])
                self.assertIn('/data/opencode/tool-output/tool_torii_', row['output']['metadata']['outputPath'])
                self.assertEqual(row['retained'], expected)
                self.assertEqual(row['retainedMode'], 0o600)
                self.assertIn('Full output saved to:', row['output']['output'])
                self.assertTrue(row['output']['output'].endswith(notice))
                tail = row['output']['output'].split('\n\n', 2)[2][:-len(notice)]
                self.assertLessEqual(len(tail.encode()), 50 * 1024)
                self.assertLessEqual(len(tail.split('\n')), 2000)
                self.assertNotIn('\ufffd', tail)

    def test_missing_capture_or_startup_fails_and_other_tools_are_unchanged(self):
        for condition in ('missing', 'unstarted'):
            value = self.drive([dict(session='one', command='true', **{condition: True})])
            self.assertIn('capture', value['observations'][0]['error'].lower())
        row = self.drive([dict(session='one', command='true', missing=True, tool='edit', original='edited')])['observations'][0]
        self.assertEqual(row['output']['output'], 'edited')


def step(index, reason, tool=None, session='session-main'):
    common = {'sessionID': session, 'messageID': 'message-%s' % index}
    parts = []
    if tool:
        parts.append(dict(common, id='tool-%s' % index, type='tool', state={'status': tool}))
    parts.append(dict(common, id='finish-%s' % index, type='step-finish', reason=reason))
    return parts


@unittest.skipUnless(shutil.which('node'), 'Node is required for guard event tests')
class OpenCodeGuardNodeTests(unittest.TestCase):
    def drive(self, parts, events=None, capture=None):
        with tempfile.TemporaryDirectory(prefix='opencode-guard-node-') as temporary:
            root = Path(temporary)
            guard = root / 'config/opencode/plugins/guard.mjs'
            guard.parent.mkdir(parents=True)
            guard.write_bytes(GUARD.read_bytes())
            launch = {'version': 1, 'launch': 'a' * 32}
            (root / 'guard-launch.json').write_text(json.dumps(launch))
            completed = subprocess.run([shutil.which('node'), '--input-type=module', '-e', NODE_DRIVER],
                                       input=json.dumps({'root': str(root), 'guard': str(guard), 'parts': parts,
                                                         'events': events or [], 'capture': capture}),
                                       text=True, capture_output=True, timeout=20, check=True)
            return json.loads(completed.stdout)

    def test_tool_calls_without_tool_part_stop_at_three(self):
        observed = self.drive(sum((step(i, 'tool-calls') for i in range(1, 4)), []))
        self.assertEqual(observed['marker'], {'version': 1, 'code': 'provider_stream_incomplete',
                         'session_id': 'session-main', 'consecutive': 3})
        self.assertEqual(observed['aborts'], ['session-main'])
        self.assertEqual(observed['refused'], ['provider_stream_incomplete'])

    def test_tool_calls_with_tool_progress_continue(self):
        for status in ('completed', 'error'):
            with self.subTest(status=status):
                observed = self.drive(sum((step(i, 'tool-calls', status) for i in range(1, 6)), [])
                                      + step(6, 'stop'))
                self.assertIsNone(observed['marker'])
                self.assertEqual(observed['aborts'], [])
                self.assertEqual(observed['refused'], [])

    def test_five_unknown_steps_with_tools_continue(self):
        observed = self.drive(sum((step(i, 'unknown', 'completed') for i in range(1, 6)), [])
                              + step(6, 'stop'))
        self.assertIsNone(observed['marker'])
        self.assertEqual(observed['aborts'], [])
        self.assertEqual(observed['refused'], [])

    def test_tool_progress_resets_consecutive_incomplete_steps(self):
        observed = self.drive(step(1, 'unknown') + step(2, 'tool-calls')
                              + step(3, 'unknown', 'completed') + step(4, None)
                              + step(5, 'tool-calls') + step(6, 'stop'))
        self.assertIsNone(observed['marker'])
        self.assertEqual(observed['aborts'], [])
        self.assertEqual(observed['refused'], [])

    def test_proof_is_written_only_by_registered_config_hook(self):
        observed = self.drive([])
        self.assertFalse(observed['before'])
        self.assertEqual(observed['ready'], {'version': 1, 'launch': 'a' * 32})

    def test_corrupt_error_capture_does_not_break_event_processing(self):
        event = {"type": "session.error", "properties": {"sessionID": "session-main", "error": {"name": "APIError"}}}
        for capture in ("{bad", "null"):
            with self.subTest(capture=capture):
                observed = self.drive(sum((step(i, "tool-calls") for i in range(1, 4)), []), [event], capture)
                self.assertEqual(observed["aborts"], ["session-main"])

    def test_original_weekly_429_survives_compaction_masking(self):
        from tests.test_opencode_recovery import WEEKLY_BODY
        marker = {'version': 1, 'launch': 'a' * 32, 'session_id': 'session-main', 'error': None}
        original = {'type': 'session.error', 'properties': {'sessionID': 'session-main', 'error': {
            'name': 'ContextOverflowError', 'data': {'message': WEEKLY_BODY['error']['message'],
                                                   'responseBody': json.dumps(WEEKLY_BODY)}}}}
        generic = {'type': 'session.error', 'properties': {'sessionID': 'session-main', 'error': {
            'name': 'ContextOverflowError', 'data': {'message': 'Conversation history too large to compact'}}}}
        observed = self.drive([], [original, generic], marker)['providerError']
        self.assertEqual(observed['error'], {'name': 'ContextOverflowError', 'status': 429,
                         'code': WEEKLY_BODY['error']['code'], 'reset_at': WEEKLY_BODY['resetAt'] / 1000,
                         'message': WEEKLY_BODY['error']['message']})

    def test_error_capture_checks_launch_and_session_and_redacts_credentials(self):
        marker = {'version': 1, 'launch': 'a' * 32, 'session_id': 'session-main', 'error': None}
        url = 'https://user:' + 'password@example.invalid/?secret=value'
        event = {'type': 'session.error', 'properties': {'sessionID': 'session-other', 'error': {
            'name': 'APIError', 'data': {'statusCode': 402,
            'message': 'Refused Bearer synthetic-secret-value ' + url}}}}
        self.assertIsNone(self.drive([], [event], marker)['providerError']['error'])
        event['properties']['sessionID'] = 'session-main'
        self.assertIsNone(self.drive([], [event], dict(marker, launch='b' * 32))['providerError']['error'])
        observed = self.drive([], [event], marker)['providerError']['error']
        self.assertIsNotNone(observed)
        self.assertEqual(observed['status'], 402)
        self.assertEqual(observed['message'], 'Refused [credential] [url]')


class OpenCodeManagedPreferencePathsTests(unittest.TestCase):
    def test_exact_managed_plist_paths_match_pinned_source(self):
        user = pwd.getpwuid(os.geteuid()).pw_name or 'user'
        expected = (Path('/Library/Managed Preferences') / user / 'ai.opencode.managed.plist',
                    Path('/Library/Managed Preferences/ai.opencode.managed.plist'))
        with patch('coordinator.opencode_runtime.sys.platform', 'darwin'):
            self.assertEqual(opencode_runtime.managed_preference_paths(), expected)


@contextmanager
def scripted_stub(root, finish, tool_steps=0):
    attempts = []

    class Stub(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def log_message(self, *args):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            main = body.get('stream') and body.get('tools') and any(
                'CONTRACT_MAIN_PROMPT' in str(item.get('content', '')) for item in body.get('messages', []))
            if main:
                attempts.append(len(attempts) + 1)
            index = len(attempts)
            delta = {'role': 'assistant', 'content': 'contract finished' if main else 'Local title'}
            reason = 'stop'
            if main and index <= tool_steps:
                delta = {'role': 'assistant', 'tool_calls': [{
                    'index': 0, 'id': 'call-edit-%d' % index, 'type': 'function',
                    'function': {'name': 'edit', 'arguments': json.dumps({
                        'filePath': str(root / 'project/sample.py'), 'oldString': 'value = %d' % index,
                        'newString': 'value = %d' % (index + 1)})}}]}
                reason = finish
            elif main and not tool_steps and index <= 3:
                delta = {'role': 'assistant', 'content': 'unfinished'}
                reason = finish
            usage = {'prompt_tokens': 1, 'completion_tokens': 1, 'total_tokens': 2}
            self.send_response(200)
            if not body.get('stream'):
                content = json.dumps({'id': 'local', 'object': 'chat.completion', 'created': 1,
                                      'model': body['model'], 'choices': [{'index': 0, 'message': delta,
                                      'finish_reason': 'stop'}], 'usage': usage}).encode()
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(content)))
                self.end_headers()
                self.wfile.write(content)
                return
            self.send_header('Content-Type', 'text/event-stream')
            self.send_header('Connection', 'close')
            self.end_headers()
            for choices, extra in (([{'index': 0, 'delta': delta, 'finish_reason': None}], {}),
                                   ([{'index': 0, 'delta': {}, 'finish_reason': reason}], {}),
                                   ([], {'usage': usage})):
                chunk = {'id': 'local', 'object': 'chat.completion.chunk', 'created': 1,
                         'model': body['model'], 'choices': choices, **extra}
                self.wfile.write(('data: ' + json.dumps(chunk) + '\n\n').encode())
            self.wfile.write(b'data: [DONE]\n\n')
            self.wfile.flush()
            self.close_connection = True

    server = ThreadingHTTPServer(('127.0.0.1', 0), Stub)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        with stub_servers(mode='fresh') as (_, proxy, other_requests, egress):
            yield 'http://127.0.0.1:%d/v1' % server.server_port, proxy, attempts, egress
            if other_requests:
                raise AssertionError('Model request reached the proxy fixture model server')
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


@unittest.skipUnless(os.environ.get(BINARY_ENV) and Path(os.environ.get(BINARY_ENV, '')).is_file(),
                     'Set TORII_OPENCODE_CONTRACT_BINARY to the pinned OpenCode binary')
class OpenCodeGuardBinaryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='opencode-guard-binary-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.binary = Path(os.environ[BINARY_ENV]).resolve()

    async def test_tool_call_finishes_without_tools_stop_after_three_requests(self):
        for finish in ('tool_calls', 'function_call'):
            with self.subTest(finish=finish):
                root = self.root / finish
                root.mkdir()
                with scripted_stub(root, finish) as (url, proxy, attempts, egress):
                    result, cwd = await capture_run(root, self.binary, url, proxy)
                print('guard finish=%s requests=%d success=%s code=%s' % (
                      finish, len(attempts), result.success, result.failure_code), flush=True)
                self.assertFalse(result.success)
                self.assertEqual(result.failure_code, 'provider_stream_incomplete')
                self.assertEqual(attempts, [1, 2, 3])
                self.assertEqual(prompt_responses(result), ['cancelled'])
                self.assertEqual(egress, [])

    async def check_tool_progress(self, finish, steps):
        initialized = []

        def config_hook(config):
            guard = config.parent / 'plugins/guard.js'
            source = guard.read_text()
            source = 'import { appendFileSync } from "node:fs"\n' + source
            source = source.replace('      const part = event.properties?.part',
                                    '      appendFileSync(resolve(root, "observed-parts.jsonl"), '
                                    'JSON.stringify(event.properties?.part ?? null) + "\\n")\n'
                                    '      const part = event.properties?.part')
            guard.write_text(source)

        with scripted_stub(self.root, finish, steps) as (url, proxy, attempts, egress):
            def on_initialize(root):
                self.assertEqual(attempts, [])
                self.assertEqual(json.loads((root / 'guard-ready.json').read_text()),
                                 json.loads((root / 'guard-launch.json').read_text()))
                initialized.append(True)
            result, cwd = await capture_run(self.root, self.binary, url, proxy,
                                            config_hook=config_hook, on_initialize=on_initialize)
        print('guard progress finish=%s tool_steps=%d requests=%d success=%s code=%s config_before_initialize=%s' % (
              finish, steps, len(attempts), result.success, result.failure_code, bool(initialized)), flush=True)
        self.assertTrue(result.success, result)
        self.assertEqual(result.text, 'contract finished')
        self.assertEqual(attempts, list(range(1, steps + 2)))
        self.assertEqual((cwd / 'sample.py').read_text(), 'value = %d\n' % (steps + 1))
        self.assertEqual(initialized, [True])
        self.assertEqual(egress, [])
        launches = list((self.root / 'state/opencode/workers/1/launches').iterdir())
        self.assertEqual(len(launches), 1)
        launch = launches[0]
        self.assertTrue(launch.is_dir())
        self.assertFalse((launch / 'stream-incomplete.json').exists())
        parts = [json.loads(line) for line in (launch / 'observed-parts.jsonl').read_text().splitlines()]
        completed = [(i, part) for i, part in enumerate(parts) if part and part.get('sessionID') == result.session_id
                     and part['type'] == 'tool' and part['state']['status'] == 'completed']
        messages = {part['messageID'] for _, part in completed}
        self.assertEqual(len(messages), steps)
        for message in messages:
            tool = next(i for i, part in completed if part['messageID'] == message)
            finish_part = next((i, part) for i, part in enumerate(parts) if part
                               and part['type'] == 'step-finish' and part['messageID'] == message)
            self.assertLess(tool, finish_part[0])
            self.assertEqual(finish_part[1]['reason'], 'unknown' if finish is None else 'tool-calls')
        print('guard event order completed_tool_before_step_finish=%d/%d' % (len(messages), steps), flush=True)

    async def test_normal_tool_use_preserves_answer_and_event_order(self):
        await self.check_tool_progress('tool_calls', 1)

    async def test_five_unknown_tool_steps_preserve_answer(self):
        await self.check_tool_progress(None, 5)

    async def test_guard_throw_after_building_hooks_leaves_no_proof_or_requests(self):
        def config_hook(config):
            guard = config.parent / 'plugins/guard.js'
            source = guard.read_text()
            self.assertEqual(source.count('  return hooks\n'), 1)
            guard.write_text(source.replace('  return hooks\n', '  throw new Error("guard registration probe")\n'))

        with stub_servers(mode='fresh') as (url, proxy, requests, egress):
            result, cwd = await capture_run(self.root, self.binary, url, proxy, 'fresh', config_hook=config_hook)
        launches = list((self.root / 'state/opencode/workers/1/launches').iterdir())
        self.assertEqual(len(launches), 1)
        launch = launches[0]
        self.assertTrue(launch.is_dir())
        proof = launch / 'guard-ready.json'
        print('guard throw requests=%d proof=%s success=%s code=%s' % (
              len(requests), proof.exists(), result.success, result.failure_code), flush=True)
        self.assertFalse(proof.exists())
        self.assertFalse(result.success)
        self.assertEqual(result.failure_code, 'provider_guard_unavailable')
        self.assertEqual(requests, [])
        self.assertEqual(egress, [])
