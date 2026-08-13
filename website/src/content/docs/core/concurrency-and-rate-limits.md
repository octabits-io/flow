---
title: Concurrency & rate limits
description: Per-step-type caps and token buckets through a pluggable admission gate.
---

## Concurrency & rate limiting
```ts
const gate = createInMemoryStepGate({
  concurrency: { 'ai:generate': { maxConcurrent: 2 } },
  rateLimit: { 'ai:generate': { perSecond: 5, burst: 10 } },
});
const engine = createWorkflowEngine({ store, dispatcher, registry, partitionKey, gate });
```
A gated step is admitted or deferred (re-enqueued) **without consuming a retry attempt**. Use
`createPgStepGate` for cross-process caps (crash-safe leases + a token bucket in Postgres).
→ [`examples/05-concurrency-rate-limit.ts`](https://github.com/octabits-io/octaflow/blob/main/examples/05-concurrency-rate-limit.ts)
