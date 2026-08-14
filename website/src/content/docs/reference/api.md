---
title: API reference
description: Every export, by entry point.
---

Every public export, grouped by entry point. Types are marked *(type)*; everything else is a
value you can call or read.

## `octaflow` (core)

No heavy dependencies — importing this pulls in neither `pg`, `pg-boss`, nor the AI SDK.

### Engine

| Export | |
|---|---|
| `createWorkflowEngine` | the engine factory |
| `createStepHandlerRegistry` | type → handler + policy registry |
| `WorkflowEngine` *(type)* | the object `createWorkflowEngine` returns |
| `WorkflowEngineDeps`, `WorkflowEngineConfig` *(type)* | constructor arguments |

Engine methods: `validateDefinition`, `startWorkflow`, `start`, `executeStep`, `resumeStep`,
`handleStepExhausted`, `recoverStuckWorkflows`, `cancelWorkflow`, `getWorkflowStatus`,
`listWorkflows`.

### Defining workflows

`defineStep`, `defineSleepStep`, `defineWaitStep`, `defineMapStep`, `defineSubWorkflowStep`,
`buildWorkflow`

*(type)* `TypedStep`, `TypedStepContext`, `TypedWorkflow`, `StepOutput`, `WorkflowOutput`,
`StepDefinition`, `WorkflowDefinition`, `RetryPolicy`, `StartOptions`

### Retryability

`retryableError`, `nonRetryableError`, `markRetryable`, `isRetryableError`,
`explicitRetryability`

### Result & errors

`ok`, `err`

*(type)* `Result`, `FlowError`, `FlowErrorShape`, `StepError`, `WorkflowNotFoundError`,
`InvalidWorkflowDefinitionError`, `StepHandlerNotFoundError`

### Store

`createInMemoryWorkflowStore`

*(type)* `WorkflowStore`, `TransactionalScope`, `CreateWorkflowParams`, `CreateWorkflowStep`,
`CreatedWorkflow`, `WorkflowCreatedResult`, `CompleteStepParams`, `FailStepParams`,
`FinishWorkflowParams`, `AddChildStep`, `ListWorkflowsFilters`, `WorkflowRecord`, `StepRecord`,
`WorkflowWithSteps`, `WorkflowStatus`, `StepStatus`, `WorkflowId`, `StepId`

### Dispatch

*(type)* `Dispatcher`, `DispatchStepPayload`, `EnqueueOptions`

### Gate

`createInMemoryStepGate`

*(type)* `StepGate`, `StepGateRequest`, `StepGateDecision`, `ConcurrencyRule`, `RateRule`,
`InMemoryStepGateConfig`

### Handlers & registry

*(type)* `StepHandler`, `StepExecutionContext`, `StepHandlerRegistry`, `StepRegistration`,
`StepCompensateHandler`, `StepCompensationContext`

### Hooks

*(type)* `WorkflowHooks`, `BeforeStartArgs`, `BeforeStartResult`, `BuildStepContextArgs`,
`AfterStepArgs`, `WorkflowCompletedArgs`

### Observability

`createRecordingObserver`, `createRecordingTracer`, `noopObserver`, `noopTracer`, `noopLogger`

*(type)* `FlowObserver`, `FlowTracer`, `FlowEvent`, `FlowEventType`, `FlowSpan`, `RecordedSpan`,
`RecordingObserver`, `RecordingTracer`, `Logger`, `LogAttributes`

### Public view

`toPublicWorkflow`, `toPublicStep`, `toDisplayStepStatus`, `STEP_DISPLAY_STATUS`,
`PUBLIC_WORKFLOW_SCHEMA`, `PUBLIC_WORKFLOW_STEP_SCHEMA`, `WORKFLOW_STATUS_SCHEMA`,
`STEP_DISPLAY_STATUS_SCHEMA`

*(type)* `PublicWorkflow`, `PublicWorkflowStep`, `StepDisplayStatus`

## `octaflow/store-pg`

Requires `pg`.

### Store

`createPgWorkflowStore` — batteries-included, owns its pool and transactions
`createWorkflowStore` — inject your own `SqlExecutor` (e.g. RLS-scoped)
`applySchema` — dev/test convenience for running DDL

*(type)* `PgWorkflowStoreDeps`, `WorkflowStoreDeps`

### Executor seam

`poolExecutor`, `toExecutor`

*(type)* `SqlExecutor`, `SqlResult`

### Gate

`createPgStepGate` (pool) · `createStepGate` (injected executor) · `flowGateDdl`, `FLOW_GATE_DDL`

*(type)* `PgStepGateConfig`, `StepGateConfig`

### Event sink

`createPgEventSink` (pool) · `createEventSink` (injected executor) · `readFlowEvents` ·
`flowEventDdl`, `FLOW_EVENT_DDL`

*(type)* `PgEventSink`, `PgEventSinkDeps`, `EventSinkDeps`

### DDL

`flowStoreDdl(schema?)`, `FLOW_STORE_DDL`, `flowGateDdl({ schema?, rateBucketTable?, leaseTable? })`,
`FLOW_GATE_DDL`, `flowEventDdl(schema?)`, `FLOW_EVENT_DDL`, `createSchemaDdl(schema)`

## `octaflow/dispatcher-pgboss`

Requires `pg-boss`.

`createPgBossDispatcher`, `ensureStepQueue`
`createPgBossStepWorker`, `createPgBossDlqWorker`
`createPgBossScheduler`, `createPgBossStartWorker`, `ensureStartQueue`
`DEFAULT_STEP_QUEUE_CONFIG`, `WIRE_STEP_PAYLOAD_SCHEMA`, `WIRE_START_PAYLOAD_SCHEMA`
`resolveStartIdempotencyKey`

*(type)* `PgBossDispatcherDeps`, `PgBossStepWorkerDeps`, `PgBossStepWorkerOptions`,
`PgBossDlqWorkerDeps`, `PgBossSchedulerDeps`, `PgBossStartWorkerDeps`, `StepQueueConfig`,
`ScheduleStartInput`, `StepJobProcessor`, `DlqProcessor`, `StartJobProcessor`,
`WireStepPayload`, `WireStartPayload`, `StartJobContext`

The two `WIRE_*` schemas are the cross-process contract: a worker parses the job body with them
before routing to a partition's engine.

`StartJobContext` is what a start worker hands its processor — the wire payload plus `jobId` and
a **per-delivery** `idempotencyKey`, computed by `resolveStartIdempotencyKey` (exported so you
can reproduce it). See [cron idempotency](/octaflow/running/postgres-and-pg-boss/#cron-idempotency).

## `octaflow/ai`

Requires `ai` and `@ai-sdk/provider`.

### Steps & hooks

`defineAiStep`, `buildAiWorkflow`, `createAiWorkflowHooks`

*(type)* `AiContext`, `AiTypedStep`, `CreateAiWorkflowHooksDeps`, `AiModelResolver`,
`AiUsageRecorder`, `AiQuotaPolicy`

`AiModelResolver`, `AiUsageRecorder` and `AiQuotaPolicy` are the three seams you implement.

### Instrumentation

`createInstrumentedModel`, `createUsageAccumulator`
`createInstrumentedEmbeddingModel`, `createEmbeddingUsageAccumulator`

*(type)* `UsageAccumulator`, `AccumulatedUsage`, `EmbeddingUsageAccumulator`,
`EmbeddingAccumulatedUsage`

### Cost

`createCostEstimator`, `estimateCostMicros`, `DEFAULT_MODEL_PRICING`

*(type)* `CostEstimator`, `CostEstimatorOptions`, `ModelPricing`, `TokenUsage`

### Quota

`createAiQuotaService`, `DEFAULT_AI_QUOTA`

*(type)* `AiQuotaService`, `AiQuotaStore`, `AiQuotaConfig`, `AiQuotaConfigResolver`,
`AiQuotaExceededError`, `AiQuotaExceededReason`, `AiUsageCountQuery`,
`CreateAiQuotaServiceDeps`

### Usage aggregation

`createAiUsageAggregationService`, `toIsoDate`, `monthStartOf`

*(type)* `AiUsageAggregationService`, `AiUsageStore`, `UsageSummaryRow`, `UsageByTypeRow`,
`CurrentQuotaUsage`, `DailyUsageDelta`, `WorkflowUsageInput`, `EmbeddingUsageInput`,
`AiUsageRangeQuery`, `AiUsageError`, `CreateAiUsageAggregationServiceDeps`
