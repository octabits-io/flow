---
title: Start idempotency
description: Collapse double-clicks and overlapping ticks with a dedup key.
---

```ts
await wf.start(engine, input, { idempotencyKey: `import:${fileId}` });
// a second start with the same key returns the existing workflow instead of duplicating it
```
→ [`examples/06-start-idempotency.ts`](https://github.com/octabits-io/flow/blob/main/examples/06-start-idempotency.ts)
