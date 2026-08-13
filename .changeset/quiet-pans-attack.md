---
"octaflow": minor
---

Commit a state change and the dispatches it unlocks in one transaction, when the
adapters allow it.

The engine wrote state and then enqueued, as two operations. A crash in that
window left steps `pending` with no job behind them — a workflow stalled forever,
invisible to `recoverStuckWorkflows`, which only looks at steps stuck in
`running`. An ordinary deploy was enough. `startWorkflow` had the same shape: a
queue failure returned `ok` with a workflow nobody would ever run.

Two optional capabilities close it:

- `WorkflowStore.runInTransaction(fn)` — runs `fn` in one transaction, handing it
  a transaction-bound store and an opaque handle. Implemented by `store-pg`.
- `Dispatcher.enqueueStepIn(handle, payload, options)` — enqueues on that handle.
  Implemented by `dispatcher-pgboss` via pg-boss's `SendOptions.db`.

The engine negotiates at construction. **Both present → the write and its
dispatches commit atomically; either missing → the previous behaviour, unchanged.**
So `store-pg` + `dispatcher-pgboss` on one Postgres now gets an exactly-once
handoff, while a queue in a different system (SQS, Redis) keeps working as before.

Both additions are optional, so existing custom stores and dispatchers continue
to compile and run.

Two deliberate boundaries:

- Only writes and enqueues go inside the transaction. The failure path runs saga
  compensation — user handlers that may do network I/O — so it stays outside; a
  rollback handler must never hold a database transaction open.
- `workflow.started` is now emitted after the transaction commits, so an observer
  never records a workflow that rolled back.

Currently covers `startWorkflow` and step completion, the two paths that run on
every workflow. Map fan-out and sub-workflow starts still write-then-enqueue and
are covered by the redelivery repair added alongside this.
