---
title: API reference
description: Every export, by entry point.
---

Condensed list of public exports per subpath.

**`@octabits-io/flow`** (core)
- Engine: `createWorkflowEngine`, `createStepHandlerRegistry`
- Steps: `defineStep`, `defineSleepStep`, `defineWaitStep`, `defineMapStep`, `defineSubWorkflowStep`, `buildWorkflow`
- Store: `createInMemoryWorkflowStore`, `WorkflowStore` (interface)
- Dispatch: `Dispatcher`, `DispatchStepPayload`, `EnqueueOptions` (interfaces)
- Gate: `createInMemoryStepGate`, `StepGate`, `ConcurrencyRule`, `RateRule`
- Observability: `createRecordingObserver`, `createRecordingTracer`, `noopObserver`, `noopTracer`, `FlowObserver`, `FlowTracer`, `FlowEvent`
- Hooks/types: `WorkflowHooks`, `Logger`, `Result`, `RetryPolicy`, `StepStatus`, `WorkflowStatus`, `WorkflowWithSteps`
- Public view: `toPublicWorkflow`, `toPublicStep`, `toDisplayStepStatus`, `STEP_DISPLAY_STATUS`, `PUBLIC_WORKFLOW_SCHEMA`, `PUBLIC_WORKFLOW_STEP_SCHEMA`, `WORKFLOW_STATUS_SCHEMA`, `STEP_DISPLAY_STATUS_SCHEMA`, `PublicWorkflow`, `PublicWorkflowStep`, `StepDisplayStatus`

**`@octabits-io/flow/store-pg`**
- `createPgWorkflowStore`, `applySchema`, `flowStoreDdl`, `FLOW_STORE_DDL`
- `createWorkflowStore`, `SqlExecutor`, `poolExecutor`, `toExecutor` — inject your own executor (e.g. RLS-scoped) instead of a raw `Pool`
- `createPgStepGate`, `flowGateDdl`, `FLOW_GATE_DDL`
- `createPgEventSink`, `readFlowEvents`, `flowEventDdl`, `FLOW_EVENT_DDL`

**`@octabits-io/flow/dispatcher-pgboss`**
- `createPgBossDispatcher`, `ensureStepQueue`
- `createPgBossStepWorker`, `createPgBossDlqWorker`
- `createPgBossScheduler`, `createPgBossStartWorker`, `ensureStartQueue`

**`@octabits-io/flow/ai`**
- `defineAiStep`, `buildAiWorkflow`, `createAiWorkflowHooks`
- `createInstrumentedModel`, `createUsageAccumulator`
- `createInstrumentedEmbeddingModel`, `createEmbeddingUsageAccumulator`
- `createCostEstimator`, `estimateCostMicros`, `DEFAULT_MODEL_PRICING`, `AiContext`
- `createAiQuotaService`, `DEFAULT_AI_QUOTA`, `AiQuotaStore`, `AiQuotaConfig`, `AiQuotaExceededError`
- `createAiUsageAggregationService`, `AiUsageStore`, `UsageSummaryRow`, `UsageByTypeRow`, `CurrentQuotaUsage`
