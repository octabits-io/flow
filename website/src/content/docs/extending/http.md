---
title: Serving over HTTP
description: "The public wire view: project engine records for HTTP consumers."
---

## Serving workflows over HTTP (public view)

flow ships no HTTP layer, but it does ship the projection every HTTP consumer
otherwise hand-writes. `getWorkflowStatus`/`listWorkflows` return the engine's
*records* — which carry fields that must not leak onto a public API
(`partitionKey`, `idempotencyKey`, sub-workflow linkage, `metadata`, retry
`attempts`) and step statuses that are engine mechanics (`waiting`, `mapping`,
`compensating`, `compensated`) rather than display states. `toPublicWorkflow`
owns that boundary, and the matching Zod schemas slot straight into a route's
`response` declaration (OpenAPI, response validation, typed clients):

```ts
import { toPublicWorkflow, PUBLIC_WORKFLOW_SCHEMA } from '@octabits-io/flow';

app.get('/workflows/:id', async ({ params }) => {
  const status = await engine.getWorkflowStatus(Number(params.id));
  if (!status.ok) throw mapError(status.error);
  return toPublicWorkflow(status.value); // no partitionKey/idempotencyKey/metadata on the wire
}, { response: { 200: PUBLIC_WORKFLOW_SCHEMA } });
```

Step statuses fold to five display states (`pending | running | completed |
failed | skipped`; suspensions read as `running`, `compensated` as `skipped`)
via the exported `STEP_DISPLAY_STATUS` map — exhaustive over `StepStatus`, so
an engine-status addition is a compile error, not a hole in your API.
Consumer-specific fields ride on top: `PUBLIC_WORKFLOW_SCHEMA.extend({...})` +
spread over `toPublicWorkflow(...)`.
