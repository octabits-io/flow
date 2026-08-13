---
"octaflow": patch
---

Fix a permanent stall when the dispatch following a step completion is lost.

The engine commits `completeStep` and then enqueues the newly-ready steps as
separate operations. A crash in that window — an ordinary deploy is enough —
left the dependents `pending` with no job behind them. Nothing recovered it:
`recoverStuckWorkflows` only looks at steps stuck in `running`, so a step that
was never picked up is invisible to it, and the workflow sat in `running`
forever with no error.

The queue redelivering the completed step's job was the one remaining signal,
and the engine discarded it — a redelivered job for a non-`pending` step
returned early as "already processed".

A redelivered job for a step that already **completed** now re-drives readiness
instead of no-opping, for both keyed steps and map children. It re-runs only the
advance, not the completion write, so counters are not inflated; and repeating a
dispatch is safe because claiming a step is atomic, so the duplicate delivery
loses and does nothing.

This is a repair path, not a guarantee: it depends on the dispatcher redelivering
the completed step's job. Removing the underlying dual write — enqueueing inside
the same transaction as the state change, which pg-boss supports via
`SendOptions.db` — is the durable fix and is tracked separately.
