# Contributing

The public repository is shared as-is and does not accept outside pull requests.
Send feedback through issues. Report security problems privately as described
in [SECURITY.md](SECURITY.md).

Use Python 3.9-compatible standard-library code. Do not add dependencies for
source inspection or documentation work. Keep new behavior small and explicit.

## Local checks

```sh
python3 -m unittest discover -v
python3 -m compileall -q coordinator scripts tests
git diff --check
```

Tests use temporary state, fake transport, and fake provider processes. They do
not need a Telegram token or provider login. Do not point tests at a real agent
history directory. A live test must use a separately selected bot/topic and native
session. State clearly whether evidence is offline or live.

## Behavior to preserve

- Only the paired numeric owner can submit work.
- Replies and reports return to the originating topic.
- Queue intake and update cursors persist together.
- A delivery retry must not repeat agent execution.
- Only an answer to the active question resumes its waiting job.
- Never run two writers against one native session or silently fork it.
- Never delete, move, or truncate provider history.
- Keep secrets and runtime state outside Git and user-facing errors.

Add a regression test for a changed queue, routing, ownership, or recovery rule.
Use fake executables for CLI arguments and process cleanup. Document behavior
changes, test evidence, and operational limits with your changes.

Before opening an issue, remove tokens, pairing codes, account data, prompts,
transcripts, and private paths from logs. Report security problems privately as
described in SECURITY.md.
