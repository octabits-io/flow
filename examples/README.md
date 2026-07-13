# flow examples

Runnable, focused examples for [`@octabits-io/flow`](../README.md). Each file is
self-contained with a `main()` and prints what it does.

## Running

These are TypeScript files that import `@octabits-io/flow`. From a project that has the package
installed (or from this monorepo after `pnpm build`):

```bash
# with a TS runner that resolves the workspace package
bunx tsx examples/01-in-memory-quickstart.ts
# or
bun examples/01-in-memory-quickstart.ts
```

Examples **01–11** run fully in-memory (no Postgres, no queue) — they share a small driver,
[`runtime.ts`](./runtime.ts), that builds an engine over the in-memory store and an in-process
queue you drain. Examples **12–13** are reference wiring for Postgres + pg-boss and the AI
add-on; they need a real database / model and are structured to copy into your app.

## Index

| # | File | Feature |
|---|---|---|
| 01 | [01-in-memory-quickstart.ts](./01-in-memory-quickstart.ts) | minimal setup + manual run loop |
| 02 | [02-dag-parallel-fan-in.ts](./02-dag-parallel-fan-in.ts) | parallel branches + fan-in (diamond DAG) |
| 03 | [03-retry-timeout.ts](./03-retry-timeout.ts) | per-step retry + timeout |
| 04 | [04-durable-sleep.ts](./04-durable-sleep.ts) | durable delay between steps |
| 05 | [05-concurrency-rate-limit.ts](./05-concurrency-rate-limit.ts) | in-memory `StepGate` |
| 06 | [06-start-idempotency.ts](./06-start-idempotency.ts) | dedup key collapses duplicate starts |
| 07 | [07-dynamic-map.ts](./07-dynamic-map.ts) | runtime fan-out / map |
| 08 | [08-wait-for-event.ts](./08-wait-for-event.ts) | suspend + `resumeStep` |
| 09 | [09-sub-workflows.ts](./09-sub-workflows.ts) | child workflow compose + await |
| 10 | [10-saga-compensation.ts](./10-saga-compensation.ts) | reverse-order rollback on failure |
| 11 | [11-observability.ts](./11-observability.ts) | observer events + tracer spans |
| 12 | [12-postgres-pgboss-production.ts](./12-postgres-pgboss-production.ts) | pg store + gate + event sink + pg-boss + cron |
| 13 | [13-ai-workflow.ts](./13-ai-workflow.ts) | AI add-on (instrumented model + cost) |
