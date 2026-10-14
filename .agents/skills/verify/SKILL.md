---
name: verify
description: Verify Torii's local CLI and offline persistent coordinator behavior, with explicit authorization for live Telegram checks.
---

# Verify Torii

Telegram topics and inline buttons are the owner surface. The local CLI is the
operator surface. Offline tests use fake Telegram transport and native providers.
They do not establish live Telegram or model behavior.

## Launch

From the repository root, run `/usr/bin/python3 ./scripts/verify-local.py` once
at the end of a unit. It uses `TORII_PYTHON` when set, then the service's
Python 3.9 interpreter. It launches short-lived CLI processes
against a fresh scratch `--state-dir`, then removes the scratch state. Read the
printed proof path and `result.json`.

## Doctor

Run `python3 -m coordinator --help` from this checkout when checking the CLI.
Confirm that it lists `status` and `--state-dir`. Use a scratch state directory
for `status`; opening the default directory can migrate the live database.
Confirm the proof's Git revision and Python version match the intended checkout
and interpreter.

## Drive

Use `python3 -m unittest tests.test_x` for a focused check while editing. The
final helper runs real CLI help, status, and `ctl list` commands in scratch
state. It checks empty `tasks`, `messages`, `workers`, `outbox`, and related v2
tables, and confirms legacy tables are absent. It also runs the complete
unittest suite, comment check, compilation, and `git diff --check`. Select
additional coverage from [the feature map](features/README.md).

For authorized live work, follow `docs/operations.md` and
`docs/verification.md`. Select a separate bot, topic, state directory, and
native session before starting `serve`. Confirm one poller owns the bot. Never
start a second poller against the running service. Pairing, `/ping`, and
`--pair-only` send Telegram messages. Do not run live setup, account changes,
model calls, restarts, or test messages without task-specific authorization.
Report an unverified live path when authorization is absent.

## Evidence

The helper writes a private proof under
`~/.local/state/agent-workflow/proofs/torii/`. `result.json` records pass
status, test count, interpreter, revision, and scratch cleanup. `actions.json`
links each command to its captured output. A proof directory alone is not a
pass. The SQLite observation records v2 table counts and absent legacy tables.

For live checks, record the authorized action and observed reply in the same
topic, plus the relevant delivery outcome. Redact identities and content.
Never copy credentials, pairing secrets, settings, native transcripts, or the
live database into proofs. Label fake-provider results offline.

## Cleanup

The helper removes only its scratch directory. It leaves proofs intact and
starts no persistent service. Confirm `scratch_removed` is true. For authorized
live checks, stop only processes created by that check. Preserve evidence and
existing agent history.
