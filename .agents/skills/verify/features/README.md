# Torii verification map

| Feature | Offline coverage | Live limitation |
| --- | --- | --- |
| [CLI status](cli-status.md) | Real CLI with isolated empty state | Does not inspect the running service |
| [Onboarding and controls](onboarding-controls.md) | Fake Telegram transport and temporary projects | Buttons and account login need separate verification |
| [Messages and recovery](queue-recovery.md) | Temporary SQLite and fake providers | Authenticated steering, relaunch, and Telegram delivery need separate verification |
| [Envelope credentials](envelope.md) | Fake vault, a real `FileVault` in a temporary directory, fake Telegram transport, and fake providers | A live deep link, a real `deleteMessage`, the group delete permission, and a vault write by the running service need separate authorization |
| Native session ownership | `tests.test_session_storage` uses fake homes, fake providers, and temporary Store state | Live deletion is off by default and requires separate activation |

Run `/usr/bin/python3 ./scripts/verify-local.py` for the offline baseline. A
service restart activates new source, but this helper never restarts the service.
