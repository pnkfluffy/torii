# Agent usage policy

Owner guidance. It overrides any other model or delegation text (global CLAUDE.md or AGENTS.md files, pstack model lists, older guidance).

## Who does the work

Coordinators use Claude Opus 5.5 when an account is available, otherwise the
active ChatGPT account on the configured Codex model. ChatGPT accounts never
switch automatically for the main chat. Coordinators choose each worker by fit.

- Most development work: Codex (ChatGPT) workers, `provider="codex"`, on `gpt-6.1-sol`. Use `gpt-6-luna` for small mechanical changes with a clear check and `gpt-6-astra` for audits and the hardest bugs.
- Frontend and UI work, extended planning and design, and reviews stay in Claude: an Opus 5.5 worker (`provider="claude"`) or a Claude helper through the Agent tool (Fable, Opus, Sonnet or Haiku, by fit).
- Saved secrets and worker goals work with either provider. The requesting job's worker receives the values.
- If one side runs out of capacity, use the other. Only the owner picks the Codex account.
- Codex work runs only in Codex workers that Torii starts on its chosen account. Claude workers never start Codex themselves (`codex exec`), even where older instructions say to hand work to Codex.

## Effort

Workers default to medium: low for small changes, high for debugging or large tasks, max for hard autonomous work.

## Review

Review only risky changes (the repo's risk check, else the coordinator's call) with one fresh Claude agent that never fixes. A finding blocks only for a concrete failure in real use, shown by a failing test or repro; anything already live is a follow-up. At most two rounds, the second re-checking only earlier blockers; then the coordinator picks merge, split or redesign and tells the owner.

## Economy

No workflows or large fan-outs unless the owner asks; a few helpers are fine. No polling. One focused worker beats many.
