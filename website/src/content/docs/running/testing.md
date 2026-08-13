---
title: Testing
description: Fast deterministic tests against the in-memory store.
---

```bash
pnpm test:unit         # fast, no Docker (in-memory)
pnpm test:integration  # Postgres + pg-boss via testcontainers
pnpm lint              # dependency-boundary check (scripts/check-boundaries.mjs)
pnpm typecheck         # tsc --noEmit
```

Write your own workflows against `createInMemoryWorkflowStore()` + an in-process dispatcher (see
[`examples/runtime.ts`](https://github.com/octabits-io/octaflow/blob/main/examples/runtime.ts)) for fast, deterministic unit tests; use
`createRecordingObserver()` to assert the lifecycle.
