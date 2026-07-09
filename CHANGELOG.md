# @octabits-io/flow

## 0.10.0

### Minor Changes

- [`0c26dbd`](https://github.com/octabits-io/platform/commit/0c26dbdffe7ca94439b31b65f21abfe63969be95) - Add an injectable `SqlExecutor` seam to the Postgres `WorkflowStore` plus a `./store-pg/schema` Drizzle column-set subpath, so a consumer can host the flow tables in its own schema, migrations, and Row Level Security instead of applying a copied DDL blob.

  - **`SqlExecutor` + `createWorkflowStore({ exec, partitionKey, schema })`** — the store now addresses all SQL through an injected executor instead of opening its own pool connections. Because the executor owns the transactions, a host can inject one that sets a transaction-local tenant GUC, so the engine's own `createWorkflow`/`completeStep`/… transactions run under RLS. `poolExecutor(pool)` is the batteries-included executor (top-level queries autocommit; `transaction` wraps `BEGIN`/`COMMIT`/`ROLLBACK`).
  - **`createPgWorkflowStore(deps)` is unchanged** — it now delegates to `createWorkflowStore` over a `poolExecutor(deps.pool)`. Same signature, same behavior (verified against the full integration suite); existing callers need no change.
  - **`@octabits-io/flow/store-pg/schema`** — spreadable Drizzle column-sets (`flowWorkflowColumns`, `flowWorkflowStepColumns`, `flowStepEventColumns`, `flowRateBucketColumns`, `flowStepLeaseColumns`) mirroring `flowStoreDdl()`/`flowEventDdl()`/`flowGateDdl()`. Following the `drizzle-toolkit/scope` precedent they ship columns only; the required indexes/uniques/PKs/FKs — notably the partial-unique `flow_workflow_idempotency_idx` that `createWorkflow`'s `ON CONFLICT` targets — are documented as a copy-paste snippet for the consumer to own.
  - `drizzle-orm` is added as an **optional** peer dependency, needed only by the new `./store-pg/schema` subpath; the raw-`pg` store bundle does not import it.

  The pg-boss dispatcher, `createPgEventSink`, and `createPgStepGate` still take a `Pool` directly — threading the executor seam through them is a follow-up.

## 0.8.0

### Minor Changes

- [`ed7813e`](https://github.com/octabits-io/platform/commit/ed7813e8274c1246ab694703d59ced0839b2e5d3) - `./ai` gains store-agnostic quota enforcement and usage aggregation.

  - `createAiQuotaService({ store, getQuota })` — concurrency / per-day / per-month workflow quota checks per `partitionKey`; quota config comes from an injected `getQuota` callback (`null` = exempt), errors surface as `ai_quota_exceeded` Result values.
  - `createAiUsageAggregationService({ store })` — token/cost rollups (daily upsert deltas, date and workflow-type aggregation, current-quota-usage windows) reusing the existing `TokenUsage` shape.

  Both engines talk to narrow structural stores (`AiQuotaStore`, `AiUsageStore`) so consumers keep raw SQL on their side; the ai layer stays free of pg/drizzle per the boundary lint.

## 0.7.0

### Minor Changes

- [`1cc1230`](https://github.com/octabits-io/platform/commit/1cc12302fb98e38267d3d15a785050f0711a4e69) - store-pg: consistent schema qualification across all DDL and runtime SQL, making a dedicated Postgres schema a first-class deployment option. `flowGateDdl` and `createPgStepGate` now accept `schema` (default `'public'`), matching the store and event sink — previously the gate's two tables resolved via `search_path` while the rest were pinned to `public`, so a non-default `search_path` could split the tables across schemas. DDL for a non-default schema now emits `CREATE SCHEMA IF NOT EXISTS` (new `createSchemaDdl` export).

## 0.5.0

### Minor Changes

- `keySource` in the AI hooks (`AiModelResolver.resolveKeySource`, `AiUsageRecorder.recordWorkflowDaily`) is now `string` instead of the hardcoded `'platform' | 'tenant'` union — that pair stays the documented convention and `'platform'` remains the default, but consumers can stamp any attribution value (e.g. `'byok'`). Non-breaking for existing implementers.

## 0.3.0

### Minor Changes

- [`2446776`](https://github.com/octabits-io/platform/commit/2446776b6007b2be8eaa9890d84b9b0df4af1cf0) - **flow/ai:** add embedding-model usage instrumentation. New exports `createInstrumentedEmbeddingModel` and `createEmbeddingUsageAccumulator` (with `EmbeddingUsageAccumulator` / `EmbeddingAccumulatedUsage` types) mirror the existing language-model instrumentation for `EmbeddingModelV4`: they transparently capture input-token usage from every `embed`/`embedMany` call via the AI SDK's `wrapEmbeddingModel` middleware, additively across a batch, with a `reset()` for long-lived accumulators. The recorded `inputTokens` feed straight into the existing `estimateCostMicros` pricing table (output/cache fields = 0). Provider-agnostic. Unblocks consumers that track embedding costs (e.g. listing-vector / semantic-search pipelines).

## 0.2.0

### Minor Changes

- [`ef2238e`](https://github.com/octabits-io/platform/commit/ef2238e3549096c88b3c48e539f5faef4d9d5e30) - Add `@octabits-io/flow` — durable DAG workflow engine (Zod-typed steps, Postgres store, pg-boss dispatcher, optional AI add-on with token/cost/quota instrumentation).

  BREAKING (`@octabits-io/drizzle-toolkit`): the `./workflow` export has been removed; it is superseded by `@octabits-io/flow`. The unused `drizzle-orm` and `zod` peer dependencies were dropped along with it — the remaining `./db` module (error handling, pagination) is unchanged. `@octabits-io/foundation` moved from peerDependencies to dependencies (it is a plain utility library — consumers no longer need to install it themselves).

- Widened `typescript` peer range to `^5 || ^6`.
