---
title: Observability
description: Lifecycle events and per-step spans, both pluggable.
---

Two pluggable surfaces, both no-op by default.

```ts
import { createRecordingObserver, createRecordingTracer } from 'octaflow';

const observer = createRecordingObserver(); // captures FlowEvents in memory (tests/introspection)
const tracer = createRecordingTracer();     // captures spans in memory
const engine = createWorkflowEngine({ store, dispatcher, registry, partitionKey, observer, tracer });
```

- **`FlowObserver`** receives a `FlowEvent` at every transition — `workflow.started/completed/
  failed/cancelled` and `step.started/completed/failed/retrying/skipped/waiting/resumed/mapping/
  compensating/compensated`, each with `{ workflowId, stepKey, stepType, attempt, durationMs,
  error, partitionKey, at }`. One surface powers **run history** (persist the events) and **metrics**
  (feed OTel counters/histograms).
- **`FlowTracer`** wraps each step execution in a `flow.step` span (records the error on failure).
  An OpenTelemetry adapter is a ~10-line `startSpan` shim.
- **Postgres run history**: `createPgEventSink({ pool, partitionKey })` is a `FlowObserver` that
  appends to `flow_step_event`; read a run's timeline back with `readFlowEvents(pool, { workflowId,
  partitionKey })`. A step that retried/transitioned is fully reconstructable after the fact.

→ [`examples/11-observability.ts`](https://github.com/octabits-io/octaflow/blob/main/examples/11-observability.ts)
