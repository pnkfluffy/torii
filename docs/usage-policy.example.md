# Example usage policy

This is an example for a new install. Torii does not load this file.
Choose providers and effort for your own accounts and projects. Replace the
state directory's `USAGE.md`, use **Usage policy** in Telegram, or call
`policy.set text=...` through local controls after you adapt these rules.

New installs use the Claude `sonnet` alias and `medium` parent effort.
Use `model.coordinator`, `model.codex`, `model.parent`, and `effort.parent`
to choose your launch settings. Policy text guides delegation; it does not
change these settings. Existing installs retain their saved choices and
previous implicit defaults during migration.

The following Markdown is sample policy text. The task routing is optional.
Use only providers you have connected and enabled.

```markdown
# Agent usage policy

## Coordinators

Owner messages come first. Answer at your next step when you can.
Otherwise, say what you are checking before you continue work.
Follow the project's review and release rules.

## Who does the work

- Use an available Codex worker for development, tests, and operations.
- Use an available Claude worker for interface design and reviews.
- Choose an available provider when the preferred one cannot take work.
- Only the owner picks accounts. Workers use the agents Torii starts.

## Effort

Start workers at medium effort. Use low for small changes and high for hard
bugs. Choose max for work that needs more reasoning and fits your usage budget.
Parent effort is a separate setting controlled by effort.parent.

## Review

Review risky changes with a fresh reviewer who does not fix them.
Require a failing test or reproduction for a blocking finding.
Follow the repository's required checks.

## Economy

Avoid polling and repeated checks. Keep each job focused.
```
