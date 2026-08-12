---
title: Defining steps
description: defineStep and its variants for waits, maps, sub-workflows and durable sleep.
---

| Helper | Use it for |
|---|---|
| `defineStep({ type, workflowInputSchema, outputSchema, dependencies?, handler, retry?, timeoutMs?, delayMs?, compensate? })` | a normal step |
| `defineSleepStep({ type, sleepMs, dependencies? })` | a durable no-op delay |
| `defineWaitStep({ type, outputSchema, dependencies? })` | suspend until `engine.resumeStep` |
| `defineMapStep({ type, workflowInputSchema, itemOutputSchema, items, each, dependencies?, itemRetry?, itemTimeoutMs? })` | runtime-sized fan-out |
| `defineSubWorkflowStep({ type, workflowInputSchema, childWorkflow, input, outputSchema?, dependencies? })` | start + await a child workflow |
| `defineAiStep({ ... })` | a step whose `ctx.context` is an instrumented `AiContext` (AI add-on) |

A handler receives a **typed context**: `ctx.workflowInput` (validated), `ctx.deps`
(each dependency's validated output), `ctx.stepInput`, `ctx.context` (host value from the
`buildStepContext` hook), `ctx.signal` (abort), plus ids. It returns the step's output object
(validated against `outputSchema`). Throw, or return a retryable error, to fail the step.
