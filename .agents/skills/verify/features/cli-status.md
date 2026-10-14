# CLI status

Run `/usr/bin/python3 ./scripts/verify-local.py`. Inspect
`cli-status.stdout.txt`. Expect `owner` and `group` to be null, with empty
`topics`, `tasks`, and `workers` arrays. `state-observation.json` must show
zero rows in every v2 table and an empty `legacy_tables` list. The helper runs
`status` twice to check that empty state remains empty. `actions.json` records
the commands and exit codes.

`status` opens SQLite and initializes or migrates its schema. Use only the
helper's scratch state for verification. The default directory points at live
state. Empty-state success does not prove Telegram connectivity.
