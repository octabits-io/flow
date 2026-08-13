---
title: Installation
description: Install the package and only the peers the layers you import need.
---

```bash
pnpm add octaflow zod
```

`zod` is a **required** peer. The heavy dependencies are **optional peers** — install only
what the layers you import need:

```bash
# Postgres store / gate / event sink
pnpm add pg

# pg-boss dispatcher, workers, cron scheduler
pnpm add pg-boss

# the AI add-on
pnpm add ai @ai-sdk/provider
```

> Pure in-memory usage (great for tests and single-process apps) needs **nothing** beyond
> `zod` — the engine, `defineStep`/`buildWorkflow`, and the in-memory store are all in the core.

Each layer is a separate subpath export, so importing one never drags in another's heavy deps:

| Import | Layer | Heavy deps |
|---|---|---|
| `octaflow` | **core** — engine, `defineStep`/`buildWorkflow`, store/dispatcher/gate interfaces, in-memory store, observability | none |
| `octaflow/store-pg` | **store-pg** — `WorkflowStore` + `StepGate` + event sink over Postgres, with DDL | `pg` |
| `octaflow/dispatcher-pgboss` | **dispatcher-pgboss** — `Dispatcher` + step/DLQ workers + cron scheduler over pg-boss | `pg-boss` |
| `octaflow/ai` | **ai** — instrumented model, cost, quota, `defineAiStep`, hooks factory | `ai`, `@ai-sdk/provider` |

Enforced dependency tree (`scripts/check-boundaries.mjs`, part of `lint`):

```
core               → (nothing internal)        forbid: ai, @ai-sdk, pg, pg-boss
ai                 → core                       forbid: pg, pg-boss
store-pg           → core                       forbid: ai, @ai-sdk, pg-boss
dispatcher-pgboss  → core                       forbid: ai, @ai-sdk, pg
```
