---
"@octabits-io/flow": minor
---

Add the public wire view to core: `toPublicWorkflow`/`toPublicStep` project engine records for HTTP consumers (dropping `partitionKey`, `idempotencyKey`, sub-workflow linkage, `metadata`, and `attempts`), `STEP_DISPLAY_STATUS`/`toDisplayStepStatus` fold engine step statuses to the five display states (suspensions → `running`, `compensated` → `skipped`), and `PUBLIC_WORKFLOW_SCHEMA`/`PUBLIC_WORKFLOW_STEP_SCHEMA`/`WORKFLOW_STATUS_SCHEMA`/`STEP_DISPLAY_STATUS_SCHEMA` ship the same shapes as Zod schemas for route `response` declarations. Extend with consumer fields via `PUBLIC_WORKFLOW_SCHEMA.extend({...})` + spread.
