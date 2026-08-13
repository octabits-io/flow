---
title: Signals & waitForEvent
description: Suspend a step until an external event arrives.
---

```ts
const approval = defineWaitStep({ type: 'await-approval', outputSchema: z.object({ approved: z.boolean() }), dependencies: { draft } });
// …elsewhere, when the webhook/human responds:
await engine.resumeStep(workflowId, 'approval', { approved: true });
```
The step suspends (`waiting`) until `resumeStep` delivers the event payload, which becomes its
output. Idempotent — a re-delivered event is a no-op.
→ [`examples/08-wait-for-event.ts`](https://github.com/octabits-io/octaflow/blob/main/examples/08-wait-for-event.ts)
