# Verification

Torii's offline baseline uses a scratch state directory, fake Telegram
transport, and fake native providers. It checks owner authentication, topic
routing, message ordering, restart recovery, worker result delivery, outbox
retry behavior, and native protocol adapters. It does not send Telegram
messages or start the running service.

Run the baseline with the service interpreter after a unit is complete.

```sh
/usr/bin/python3 ./scripts/verify-local.py
python3 scripts/check-no-comments.py
```

`verify-local.py` records CLI help and status output, control operations,
SQLite table counts, unittest output, compilation, and `git diff --check` in a
private proof directory under
`~/.local/state/agent-workflow/proofs/torii/`. Read its `result.json`.
A proof directory without `passed: true` is not a pass. The result includes
the test count and whether the scratch directory was removed.

Use `python3 -m unittest tests.test_store` for focused migration tests. The old
schema fixture in that file creates its own database. It does not read or
modify the live state file. For a live-schema rehearsal, make a SQLite backup
into a temporary directory and open only that copy with `Store`.

The [group setup live checklist](group-setup-live-test.md) covers the picker,
`/start` delivery, and rights after the supergroup upgrade. These remain unverified
by offline tests. Live Telegram checks need separate authorization and an isolated bot, topic,
state directory, and native session. Confirm that only one poller owns the bot.
Pairing, `/ping`, and `--pair-only` still send Telegram messages. Never treat
them as offline checks. Do not restart the running service, change a local
account, or send test messages without task-specific authorization.

A native provider receipt proves message delivery to the session. It does not
prove that the model applied steering. A worker process exit does not prove that
a deployment worked. Verify reported effects at their target and distinguish
that evidence from offline tests.

## macOS worker ownership

`tests.test_process_ownership` runs portable fake-backend contracts on Linux
and macOS. Existing OpenCode runtime and pinned-binary contract fixtures
explicitly use the Popen path. Only the separate Darwin ownership class creates
test launchd jobs. Run it with an isolated HOME and TMPDIR, with TMPDIR inside
the chosen scratch folder and `TORII_OWNERSHIP_DARWIN=1`. The Darwin class
uses Python fake providers and does not require Node on PATH.

```sh
python3 -m unittest -v tests.test_process_ownership.DarwinOwnershipTests
```

The class uses only `local.torii-host-test.` labels in `gui/<uid>` and retains
its plists, PID registries, signal logs and sentinels in the scratch folder's
`darwin` subfolder. Each signal checks a recorded PID and unique identity. D1
checks owner Stop, D2 checks host SIGKILL, D3 checks provider exit with detached
writers, and D4 checks refusal of a host started in a shared coalition. Each
positive check must stop a plain child, a separate-group child, a setsid child
and a double-forked setsid grandchild. Sentinel lengths must stop growing while
an outside control continues. Run the class three times, then audit all saved
test PIDs and the test label prefix. These are fake-provider OS checks.

A separately authorized live OpenCode acceptance check on this Mac is still
required. Keep the live switch off until that authorization. Use the pinned
binary, an isolated worker and exact native session, and record all test-created
writers before their work. Check completion, a completed shell tool that leaves
a late writer, cancellation mid-turn, stdin EOF or disconnection, host death,
and service reattach. For each terminal path, confirm `owner.json` and
`exit.json` report successful cleanup, all four local writer shapes stop, their
sentinels stay unchanged, the outside control continues, the job is removed,
and launch scratch is removed only after cleanup. Check that removed launch
scratch does not reappear. Reattach must preserve the running process and native
session and send no ownership signals. Record actual process identities and
result status; a provider exit alone is insufficient. This does not cover
changed uid, broker-created jobs, remote work or pre-existing shared hosts.
