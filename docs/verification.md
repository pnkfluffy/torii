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
