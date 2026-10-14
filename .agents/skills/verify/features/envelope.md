# Envelope credentials

Run `/usr/bin/python3 -m unittest tests.test_vault tests.test_envelopes -v`
under a temporary `TORII_STATE_DIR`. Leave `TORII_VAULT_KEY_FILE` unset.

`tests.test_vault` covers the seal round trip, fresh nonces, every tamper case,
the pinned known-answer vector, key resolution, crash leftovers, and file modes.
`tests.test_envelopes` drives `Store.accept` with hand-built private updates,
a `FakeVault` or a real `FileVault` in the temporary directory, and a fake
Telegram transport. `EndToEndTests` scans every file under the temporary state
directory in binary mode for the fake value, its encoded forms, and the token.

Offline tests do not prove a live Telegram deep link, `deleteMessage` in a real
private chat, the bot's delete permission in the group, or a vault write by the
running LaunchAgent. Each needs task-specific authorization. Never put a real
credential in a test. Use values such as `FAKE-ENVELOPE-VALUE-0123456789`.
