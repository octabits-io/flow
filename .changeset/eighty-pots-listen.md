---
'octaflow': minor
---

Step heartbeats — liveness for long steps, and a way to interrupt a cancelled one.

`startedAt` used to be the engine's only liveness signal, so the stuck-step sweeper had to read
"started a while ago" as "the worker is dead". One number could not serve both: a short
threshold condemns a legitimately long step, a long one leaves a dead step squatting for the
full 15 minutes.

A step type that declares `heartbeatTimeoutMs` is now judged on **silence** instead:

```ts
const transcode = defineStep({
  type: 'transcode',
  heartbeatTimeoutMs: 2 * 60 * 1000,   // silence for 2 minutes ⇒ presumed dead
  handler: async (ctx) => { /* … */ },
});
```

That is the whole opt-in — the engine beats automatically while the handler runs, so an evicted
pod is noticed in seconds without the handler being touched. `heartbeat: 'manual'` suppresses
the timer for handlers that want silence to mean *hung* as well as *dead*, and
`defineMapStep` takes `itemHeartbeatTimeoutMs` for per-item work.

**The beat doubles as a cancellation channel.** `ctx.heartbeat()` resolves `false` when the step
is no longer this invocation's to run — the workflow was cancelled, it blew its deadline, or the
sweeper re-queued the step. The engine then fires `ctx.signal` and **discards the handler's
outcome**. Two consequences:

- Cancelling a run can now interrupt a step that was already executing, for any step that beats
  and respects its abort signal. Previously that was impossible by construction.
- The concurrent-double-execution hazard introduced when crashed steps became re-queueable is
  removed rather than merely made unlikely: a live step keeps proving it, and a superseded one
  finds out on its next beat instead of stamping its result over the new owner's.

Opt-in throughout: a step type that declares no `heartbeatTimeoutMs` behaves exactly as before,
judged by `stepExpirySeconds + stuckStepBufferSeconds` from when it started.

### Breaking changes

Both land in the same **unreleased** minor as the store changes from the branching/deadlines
work, so consumers absorb one store migration rather than two.

**`WorkflowStore` gains `heartbeatStep(stepId, at)`.** It must write conditionally in one
statement and return whether the step is still `running` under a live workflow; `markStepRunning`
and `resetStep` must clear `heartbeat_at`, or a fresh attempt inherits a stale stamp and looks
dead on arrival. The bundled stores are updated — see CONTRIBUTING for the contract.

**`StepRecord` gains `heartbeatAt`** and `StepExecutionContext` / `TypedStepContext` gain
`heartbeat`, backed by a new `heartbeat_at` column. `flowStoreDdl()` emits an
`ALTER TABLE … ADD COLUMN IF NOT EXISTS`, so re-applying the DDL migrates an existing database.
If you host the tables in your own migrations:

```sql
ALTER TABLE flow_workflow_step ADD COLUMN IF NOT EXISTS heartbeat_at timestamptz;
```

`findStuckSteps` now selects on `COALESCE(heartbeat_at, started_at)`, and the engine holds each
candidate to its own step type's window afterwards.
