---
title: Signals & waitForEvent
description: Suspend a step until an external event arrives.
---

```ts
const approval = defineWaitStep({
  type: 'await-approval',
  outputSchema: z.object({ approved: z.boolean() }),
  dependencies: { draft },
});

const wf = buildWorkflow({ type: 'publish', inputSchema, steps: { draft, approval, publish } });

// …elsewhere, when the webhook/human responds:
await engine.resumeStep(workflowId, 'approval', { approved: true });
```
The step suspends (`waiting`) once its dependencies complete — its handler never runs — until
`resumeStep` delivers the event payload, which becomes its output.
→ [`examples/08-wait-for-event.ts`](https://github.com/octabits-io/octaflow/blob/main/examples/08-wait-for-event.ts)

:::note[The second argument is the step *key*, not the step type]
`'approval'` above is the property name under `steps:` in `buildWorkflow` — **not**
`type: 'await-approval'`. The type names the handler; the key names this step within this
workflow. Getting them confused is the usual first bug here, and it fails quietly: an unknown
key returns a `workflow_not_found` error rather than throwing.
:::

## The resume payload is the output

Whatever you pass becomes the step's output verbatim — the engine does not validate it against
`outputSchema` on the way in. Validation happens on the way *out*, when a dependent step reads
it through `ctx.deps`:

```ts
const publish = defineStep({
  dependencies: { approval },
  handler: async (ctx) => {
    ctx.deps.approval.approved;  // typed, and parsed against approval's outputSchema here
  },
});
```

So a malformed webhook body doesn't fail at `resumeStep` — it fails the *dependent* step, as a
non-retryable validation error. Validate at your HTTP boundary if you want a 400 instead.

## Idempotency and the states that ignore a resume

`resumeStep` is safe to call more than once. It returns `{ ok: true }` and does nothing when:

- the step is not `waiting` (already resumed, already failed, or still `pending` because its
  dependencies haven't completed — a resume that arrives *early* is dropped, not queued)
- the workflow is not `pending` or `running` (already completed, failed, or cancelled)

It returns an error only when the workflow or the step key doesn't exist. That means a
re-delivered webhook is a no-op, but so is a resume that arrives before the step is ready — if
your event can beat the DAG, persist it and replay after the `step.waiting` event.

## Waiting forever

A `waiting` step has no timeout. It is **not** swept by
[`recoverStuckWorkflows`](/octaflow/running/cancellation-and-recovery/), which only looks at
steps stuck in `running`, so a workflow awaiting an event that never arrives waits
indefinitely. Two ways to bound it:

- **Cancel it.** `engine.cancelWorkflow(id)` marks `waiting` steps `skipped` and finishes the
  workflow as `cancelled`.
- **Race it with a sleep.** Add a [sleep step](/octaflow/core/durable-sleep/) on a parallel
  branch and have its downstream step check whether the approval landed. The DAG is static, so
  there is no built-in "first one wins" — you model the timeout as a step.

`waiting` folds to the display state `running` in the
[public view](/octaflow/extending/http/), so a UI shows it as in-flight rather than as its own
state. Watch the `step.waiting` and `step.resumed`
[events](/octaflow/running/observability/) if you need to show "awaiting approval" specifically.
