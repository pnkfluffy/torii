# Torii agent instructions

Read [README.md](README.md) for setup, operation, safety, and verification.
Only the configured Telegram owner may submit work. Follow the owner's
settings and each target repository's instructions. The coordinator policy is
in `coordinator/policy.py`.

Persist requests before acknowledging them. Never rerun execution because
delivery failed. Never run two writers against a native session. Preserve
native history, account homes, and credentials. Never access the bot token.
Inspect actual state before resuming interrupted work. Verify live effects at
the target; an exit code alone is not deployment evidence.

Code has no comments, including lint pragmas. Docstrings and a first-line
shebang are allowed. Before a PR merges, run the test modules it touches with
`python3 -m unittest tests.test_x …`. Run the full suite,
`python3 scripts/verify-local.py`, for risky changes (cleanup, secrets,
startup, accounts, worker launch).
Use the installed `pstack:poteto-mode` workflow when it fits;
otherwise use `.agents/skills/verify/SKILL.md`. Never restart the running
service or send test messages without task-specific authorization.

## Messaging another chat or agent

Coordinators message another Torii chat's agent with the `events_post` MCP
tool (topic, text). Torii signs it with the chat's name, so the owner sees a
🤖 note. Workers, lanes, and scripts inside Torii use the local CLI without
`--from`; their messages reach the other chat's coordinator silently:

```sh
python3 -m coordinator ctl call events.post topic='<id>' text='<message>'
```

Programs outside Torii add `--from` with their own name, immediately after
`call`. Use single quotes and no backticks in the command. Closing and closed
chats receive no note. Reach agents outside Torii through their own task or
channel.
