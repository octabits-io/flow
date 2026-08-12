---
title: Sub-workflows
description: A step starts a child workflow and awaits its result.
---

```ts
const enrich = defineSubWorkflowStep({
  type: 'enrich', workflowInputSchema,
  childWorkflow: enrichmentWorkflow,                 // a built workflow
  input: (ctx) => ({ listingId: ctx.workflowInput.id }),
  outputSchema: z.object({ /* child's output shape */ }),
});
```
Starts the child workflow (same partition), suspends the parent step, and resumes it with the
child's output when it terminates. A failed/cancelled child fails the parent step.
→ [`examples/09-sub-workflows.ts`](https://github.com/octabits-io/flow/blob/main/examples/09-sub-workflows.ts)
