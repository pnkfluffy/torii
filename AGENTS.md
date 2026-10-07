# Torii

## Purpose and authority

This is an independent local service. Only the configured Telegram owner may
submit work. Treat Telegram usernames as display text, never as authentication.
The owner authorizes work through merge and deployment. Follow each target
repository's instructions and required verification. Never claim a deployment
worked from a process exit alone.

## Coordinator policy

`coordinator/policy.py` holds the coordinator's instructions and is the one
source for them. The service owns delivery, persistence, and recovery; the
coordinator owns delegation, verification, and reporting. Follow the owner
delegation settings and each target repository's instructions.

## Owner controls

The coordinator acts through the MCP tools from `coordinator/control_api.py`,
and `python3 -m coordinator ctl list` shows the same operations locally.
Delegation, model, and policy settings are owner controls on Telegram and the
local CLI. Claude account choice is always automatic. Every selection uses the
eligible account whose all-model weekly reset comes soonest, with alias as the
tie-break. Any meter at 95% excludes an account; local per-account reserves also
apply. Codex uses one active account unless the owner turns on automatic Codex
switching; the coordinator sets the active Codex account with `account.use` only
when the owner picks one. Account enable and disable remain owner controls.

## Local accounts

Each Claude profile keeps its own sign-in, and Claude Code refreshes it.
Torii never reads or passes Claude sign-in tokens. New work picks the eligible
account with the soonest weekly reset, with alias as the tie-break. A running
parent stays on its account until it is ineligible. The causes are 95% usage
in a 5-hour, all-model weekly or Fable weekly meter, a local reserve, a hold,
a signed-out profile, or an account the owner disabled. The parent waits for
its turn and background tasks to finish, then restarts under the next eligible
account and resumes the exact native transcript without copying history.
A worker switches accounts when Claude rejects it for usage.
Codex accounts live each in its own `CODEX_HOME`. Claude or ChatGPT runs the
main chat; ChatGPT accounts never switch automatically for it. Each new parent
launch prefers an available Claude account, then the active ChatGPT account.
With automatic Codex switching on, workers follow the same rules; see
`docs/operations.md`. Never delete, move, or copy `~/.codex` history, an
account home under `~/.codex-accounts`, or an `auth.json`, and never run
`codex login` or `codex logout` in an account home.
Never print tokens, credential files, or secret values.

## Implementation rules

- Persist each request before acknowledging it to Telegram.
- Never retry agent execution because Telegram delivery failed.
- Never run two processes against the same native agent session.
- Code has no comments, including lint pragmas such as `# noqa`. Docstrings, a
  shebang on line 1, and a coding line on line 1 or 2 are allowed.
  `scripts/check-no-comments.py` finds comments; `verify-local.py` and the unittest
  suite fail on one.
- Run tests on the service interpreter through `./scripts/verify-local.py`.

## Native session transfer

An existing native session must remain untouched until live bridge checks
pass and its active work settles. Transfer ownership, not a fork. Preserve its
working directory and native session ID. Inspect its live state before stopping
it. Do not restart the old writer while the service owns the session.

## Engineering workflow

For engineering work on Torii, use `pstack:poteto-mode` when installed and its
workflow fits. Otherwise follow the bundled `verify` skill in
`.agents/skills/verify/SKILL.md`. In either case, follow its verification rules.
This guidance does not route unrelated Telegram conversations into engineering.
Run `./scripts/verify-local.py` for the offline verification baseline. It uses
fresh scratch state and does not verify the running Telegram service. Do not
restart that service or send test messages without task-specific authorization.

Setting Torii up for a user? Follow [docs/agent-setup.md](docs/agent-setup.md); never ask for or read the bot token.
