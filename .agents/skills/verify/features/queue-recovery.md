# Messages and recovery

Offline tests cover owner-only intake, ordered messages, same-topic steering
while a turn runs, worker completion messages, delivery retry, and relaunch
recovery.

Run `python3 -m unittest tests.test_store tests.test_service tests.test_session
tests.test_workers` for focused coverage, or run the final offline helper. The
tests use temporary SQLite and fake provider processes. A saved `sent` message
becomes `uncertain` after restart; the service does not replay it automatically.
A later replay or a transcript entry confirms it. A running service worker keeps
running under its host; it becomes `interrupted` only when its host died without
an exit record. Telegram delivery failure retries the outbox without rerunning work.

`tests.test_job_disposition` and `tests.test_job_resume` cover the optional
`auto_resume` switch. They check settle reminders, owner waits, stop sources,
cause mapping, retry bounds, progress resets, quota rebuilds, network recovery,
native session reuse and final results from processes that remain running.
They also check the default-off behavior and durable resume state after restart.
`tests.test_job_review` covers Muse availability, per-job error isolation,
parked secret and quota workers, condition failures after automatic continuation,
account-fire attempt preservation, queued-worker notices and held-copy recovery.
It also covers the deferred result-line requirement and short owner error text.

Live verification needs an authorized disposable topic and native session.
Observe the request, native receipt, worker result, and Telegram reply without
copying prompts or transcripts. Never run two writers against one native
session. A fake report does not establish live Telegram delivery.
