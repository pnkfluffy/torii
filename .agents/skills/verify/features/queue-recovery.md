# Messages and recovery

Offline tests cover owner-only intake, ordered messages, same-topic steering
while a turn runs, worker completion messages, delivery retry, and relaunch
recovery. `tests.test_store` includes an old-schema fixture. It proves that
eligible jobs become tasks, workers link to them, legacy tables disappear, and
a second open does not copy jobs again.

Run `python3 -m unittest tests.test_store tests.test_service tests.test_session
tests.test_workers` for focused coverage, or run the final offline helper. The
tests use temporary SQLite and fake provider processes. A saved `sent` message
becomes `uncertain` after restart; the service does not replay it automatically.
A later replay or a transcript entry confirms it. A running service worker keeps
running under its host; it becomes `interrupted` only when its host died without
an exit record. Telegram delivery failure retries the outbox without rerunning work.

Live verification needs an authorized disposable topic and native session.
Observe the request, native receipt, worker result, and Telegram reply without
copying prompts or transcripts. Never run two writers against one native
session. A fake report does not establish live Telegram delivery.
