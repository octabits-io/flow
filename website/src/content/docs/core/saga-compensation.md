---
title: Saga compensation
description: Run rollback handlers in reverse dependency order when a workflow fails.
---

```ts
const reserve = defineStep({
  type: 'reserve', workflowInputSchema, outputSchema: z.object({ ticketId: z.string() }),
  handler: async () => ({ ticketId: await reserveSeat() }),
  compensate: async (ctx) => { await releaseSeat(ctx.output.ticketId); }, // undo on later failure
});
```
On workflow failure the engine runs each completed step's `compensate` in **reverse dependency
order** (`compensating` → `compensated`). Best-effort: a throwing rollback is logged + surfaced,
the rest still run. → [`examples/10-saga-compensation.ts`](https://github.com/octabits-io/octaflow/blob/main/examples/10-saga-compensation.ts)
