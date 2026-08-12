---
title: Stores, dispatchers, gates
description: Every backend is an interface — and the correctness requirements a custom one must meet.
---

Everything is an interface — implement your own backend without touching the engine:

- **`WorkflowStore`** — persistence (`createWorkflow`, `listSteps`, `markStep*`, `addChildSteps`, …).
  Ship one for any database. **One correctness requirement**: `markStepRunning` is the step
  *claim* — it must flip `pending` → `running` **atomically** and return whether the caller won
  (`UPDATE … WHERE id = $1 AND status = 'pending'`, then check the affected row count). A
  read-then-write lets two workers handed the same job by an at-least-once dispatcher both run
  the handler.
- **`Dispatcher`** — `enqueueStep(payload, { startAfterSeconds })`. Back it with any queue (SQS,
  BullMQ, …); honor `startAfterSeconds` for retry/sleep to work durably.
- **`StepGate`** — `acquire(req)` → admit (with a `release`) or defer. Build org-wide caps however
  you like.
- **`FlowObserver` / `FlowTracer`** — run history + spans for your telemetry stack.
- **`WorkflowHooks`** — `onBeforeStart` (guard/quota), `buildStepContext` (inject `ctx.context`),
  `onAfterStep`, `onWorkflowCompleted`.

The in-memory store and the Postgres/pg-boss adapters are reference implementations.
