---
title: Cancellation & recovery
description: Stop a workflow on purpose, and clear the ones a crashed worker left behind.
---

Two engine methods handle the endings that aren't "every step completed": one you
call deliberately, one you run on a timer.

## Cancelling a workflow

```ts
const cancelled = await engine.cancelWorkflow(workflowId);
if (!cancelled.ok) throw new Error(cancelled.error.message); // workflow_not_found
```

Every `pending` **and `waiting`** step is marked `skipped`, the workflow finishes
`cancelled`, and a `workflow.cancelled` event is emitted. Three things worth knowing:

- **It does not interrupt a step already running.** There is no signal into an
  in-flight handler; the step runs to completion, and its result lands on a workflow
  that has already finished. Cancellation is a "stop scheduling more work" operation,
  not a kill.
- **Compensation does not run.** Saga rollback is wired to the *failure* path only
  (see [Saga compensation](/octaflow/core/saga-compensation/)). If you need completed
  steps undone on a cancel, do it yourself after the call returns.
- **Cancelling a sub-workflow child fails the parent step**, which cascades into the
  parent workflow exactly as a child failure would.

Calling it on a workflow that already reached a terminal state is a no-op that still
returns `ok` — so a double-click on a cancel button is safe.

## Recovering after a crash

A worker that dies mid-step leaves its step in `running` forever. Nothing detects that
on its own: the queue's job expiry releases the *job*, but the row stays claimed, and
an atomic claim (`markStepRunning`) means a redelivered job can't take it over.

`recoverStuckWorkflows` is the sweeper that clears them:

```ts
const { recoveredSteps, recoveredWorkflows } = await engine.recoverStuckWorkflows();
```

It scans every `running` workflow in the partition for steps that entered `running`
longer ago than the stuck threshold, marks each one **failed**, and cascades the
workflow to `failed` in the usual way (dependents skipped, compensation run).

:::caution
Recovery is to a **terminal** state, not a resume. A stuck step is not re-attempted —
it is failed, and the workflow fails with it. Set a step's `retry` policy if you want
attempts; the sweeper is the backstop for work that will never report back at all.
:::

Nothing calls this for you. Run it on a schedule in one process — a cron job, a
`setInterval`, or a pg-boss schedule:

```ts
setInterval(() => {
  void engine.recoverStuckWorkflows().catch((e) => logger.error('sweep failed', e));
}, 60_000);
```

Once per partition is enough. Two sweepers racing on the same partition will not corrupt
state — the step transition is a plain `UPDATE` and the cascade is recomputed from
`listSteps` — but each one increments the workflow's `failedSteps` counter, so a
double sweep can inflate that progress number. Run it from one process, or accept the
skew.

### The stuck threshold

```
stuckThreshold = stepExpirySeconds + stuckStepBufferSeconds
```

Both come from `WorkflowEngineConfig`, and both have defaults:

| Option | Default | Meaning |
|---|---|---|
| `stepExpirySeconds` | `600` | What you told the *dispatcher* a step may occupy a worker for |
| `stuckStepBufferSeconds` | `300` | Grace on top, so a step that is merely slow isn't swept |

```ts
const engine = createWorkflowEngine({
  store, dispatcher, registry, partitionKey,
  config: { stepExpirySeconds: 900, stuckStepBufferSeconds: 300 }, // sweep at 20 min
});
```

:::danger[Keep this in sync with the queue]
`stepExpirySeconds` is **not** enforced by the engine — it is the engine's *copy* of
the dispatcher's expiry, used only to compute the threshold. With pg-boss the real
value is `StepQueueConfig.expireInSeconds` (also 600 by default). Change one and you
must change the other:

```ts
const dispatcher = createPgBossDispatcher({ boss, queueName, partitionKey,
  config: { expireInSeconds: 900 } });
const engine = createWorkflowEngine({ …, config: { stepExpirySeconds: 900 } });
```

Set the engine's value **too low** and the sweeper fails steps that are still legitimately
running. Set it **too high** and genuinely dead work sits in `running` for longer than
it needs to.
:::

## What recovery does not cover

The sweeper only looks at steps stuck in `running`. A step stuck in `pending` with no
job behind it is invisible to it — that is the dual-write window between persisting a
transition and enqueueing the jobs it unlocks.

Closing that window is the job of
[transactional dispatch](/octaflow/extending/interfaces/#transactional-dispatch): with
`store-pg` + `dispatcher-pgboss` on one Postgres, the write and its enqueues commit
together and the window doesn't exist. On a queue that lives elsewhere (SQS, Redis) the
engine falls back to write-then-enqueue, and the repair path is the dispatcher
redelivering the *completed* step's job, which re-drives readiness.
