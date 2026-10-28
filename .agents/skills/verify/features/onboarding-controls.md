# Onboarding and controls

The owner uses `/setup`, `/projects`, `/help`, `/accounts`, `/secrets`, and
`/health` in a paired project topic. Models and delegation are `/setup` pages.
Setup offers New project and Existing project. Account registration does not
establish a successful login.

For offline coverage, run `python3 -m unittest tests.test_onboarding
tests.test_control_ui tests.test_controls tests.test_group_setup tests.test_setup_flow`. These tests use fake updates and
temporary folders. They check project selection, settings persistence, owner
checks, stale callbacks, and that menu input creates no task.

The group setup client checks are in `docs/group-setup-live-test.md`.
Live Telegram work needs a separate authorization and selected topic. Confirm
that replies stay in that topic and no worker starts for menu input. Settings
can affect future real work. The offline helper sends no Telegram messages.
