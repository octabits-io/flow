---
title: Fan-out & map
description: Spawn one child step per item of a runtime-sized list.
---

## Dynamic fan-out / map
```ts
const resizeAll = defineMapStep({
  type: 'resize-all',
  workflowInputSchema,
  itemOutputSchema: z.object({ url: z.string() }),
  dependencies: { listImages },
  items: (ctx) => ctx.deps.listImages.urls,          // runtime-sized list
  each: async (url, info) => ({ url: await resize(url, info.index) }),
});
// downstream reads resizeAll.items: { url: string }[]
```
The engine spawns one child step per item (own retry/gate), suspends the parent as `mapping`, and
completes it with the aggregated outputs. A failed item fails the whole map.
→ [`examples/07-dynamic-map.ts`](https://github.com/octabits-io/octaflow/blob/main/examples/07-dynamic-map.ts)
