# @octabits-io/flow

## 0.13.0

### Minor Changes

- [`d342f6a`](https://github.com/octabits-io/flow/commit/d342f6a85f65384fde29295c1a71fe602c397d32) - Fix a double-execution window in the step claim.
  
  `executeStep` read a step, checked it was `pending`, and then wrote `running` as a
  separate statement. Two workers handed the same job by an at-least-once dispatcher
  could both pass the read and both run the handler — with the step's `attempts`
  double-incremented.
  
  The claim is now atomic: `markStepRunning` flips `pending` → `running` only if the
  step is still `pending`, and reports whether the caller won. The Postgres store does
  this with `UPDATE … WHERE id = $1 AND status = 'pending'` and checks `rowCount`; the
  engine bails out (releasing its gate slot) when it loses the race.
  
  **Breaking for custom `WorkflowStore` implementations**: `markStepRunning` now returns
  `Promise<boolean>` instead of `Promise<void>`, and MUST perform the status check and
  the write as one atomic operation. Implementations that unconditionally write the row
  will reintroduce the double-execution window. The bundled Postgres and in-memory
  stores are already updated; consumers using them need no changes.

- [`0a3aff0`](https://github.com/octabits-io/flow/commit/0a3aff0f47a81405b3259afcacf39ffd986ebbb9) - Make the pg-boss step worker settle jobs individually, and expose the throughput
  knobs that were previously unreachable.
  
  **Per-job settlement.** pg-boss fails an entire batch when the handler throws, so
  one bad step dragged its batch neighbours into a retry — wasteful (the engine's
  atomic claim made re-execution a no-op) and it obscured which job actually
  dead-lettered. The worker now reports each job's own outcome: a step that throws
  fails alone under the queue's retry policy, and a payload that fails schema
  validation is dead-lettered directly rather than burning attempts it can never pass.
  
  **New `workerOptions`** on `createPgBossStepWorker`, all optional and defaulting to
  today's behaviour:
  
  - `burstWhenBatchFull` — keep fetching with no delay while batches come back full.
  - `burstWhenReadyExceeds` — burst while the queue's ready count exceeds a threshold.
  - `notifyPollingIntervalSeconds` — poll interval used while LISTEN/NOTIFY is active.
  - `concurrency` — steps run at once from one fetched batch (default 1, i.e. serial).
  
  Measured on the repo's benchmark (200 workflows × 6 steps, 1 worker, batch 25):
  50 → 274 steps/sec with `burstWhenBatchFull`, and 646 with `concurrency: 8` on top.
  `concurrency` alone, without burst, changes nothing — a poll-bound worker drains its
  batch in milliseconds and then waits, so the wait is the bottleneck, not the work.
  Budget connections before raising it: each in-flight step holds one, so
  `workers × concurrency` must fit the pool and Postgres `max_connections`.
  
  **Peer range**: the optional `pg-boss` peer moves from `^12.0.0` to `^12.21.0`, the
  release that introduced `perJobResults` and the burst options.

- [`f032407`](https://github.com/octabits-io/flow/commit/f032407ef3581aabbe9d88a0e7e3b4cca787f656) - Add an explicit escape hatch for retryability.
  
  Whether a failed step was retried was decided solely by `isRetryableError`, which
  matches the error *message* against a small vocabulary (`rate limit`, `429`,
  `timeout`, `ECONNRESET`, `503`, …). That silently misjudges both directions:
  `'connection refused'` is transient but failed terminally, while a permanent bug
  whose message happened to contain `'timeout'` was retried until the budget ran out.
  
  Retryability is now decided in this order:
  
  1. **An explicit marker on the error** — `retryableError(msg)`, `nonRetryableError(msg)`,
     or `markRetryable(err, bool)` to tag an error you didn't construct. Also
     `explicitRetryability(err)` to read the decision back. Markers are found through
     the `cause` chain, so wrapping an error doesn't lose its decision.
  2. **The step's own predicate** — `defineStep({ isRetryable: (e) => … })`, and
     `defineMapStep({ itemIsRetryable })` for per-item children.
  3. **`isRetryableError`** — now reads structured fields before the message: `code`
     (`ECONNRESET`, `ECONNREFUSED`, `ETIMEDOUT`, `EAI_AGAIN`, undici timeouts, …) and
     HTTP status from `status` / `statusCode` / `httpStatusCode` / `response.status`
     (408, 425, 429, 5xx except 501/505). The message vocabulary is unchanged and is
     now the last resort.
  
  Also adds `defaultRetryable` to the engine config, for hosts that would rather not
  guess at all:
  
  ```ts
  createWorkflowEngine({ …, config: { defaultRetryable: false } });
  ```
  
  It applies **only where the classifier guessed** — explicit markers, per-step
  predicates and engine-generated failures (a step timeout) are unaffected. `StepError`
  gains `retryableFrom: 'explicit' | 'predicate' | 'heuristic'` to make that distinction
  available to custom dispatchers.
  
  The marker is a non-enumerable `Symbol.for` property, so it does not leak into
  `JSON.stringify` or spread, and survives a duplicated copy of the module. Marking
  never throws — errors that are frozen, sealed or non-extensible fall back to a
  `WeakMap`, since marking is usually evaluated inside a `throw` and a `TypeError`
  there would replace the failure being reported.
  
  **Behaviour change**: errors carrying a transient `code` or a 5xx/429 status now
  retry where previously they failed terminally (their message was never consulted).
  Steps that mark nothing, define no predicate, and throw plain message-only errors
  behave exactly as before.

## 0.12.0

### Minor Changes

- [`914b82f`](https://github.com/octabits-io/flow/commit/914b82f58ff66a19514dea95a2687cc13350ee77) - Add the public wire view to core: `toPublicWorkflow`/`toPublicStep` project engine records for HTTP consumers (dropping `partitionKey`, `idempotencyKey`, sub-workflow linkage, `metadata`, and `attempts`), `STEP_DISPLAY_STATUS`/`toDisplayStepStatus` fold engine step statuses to the five display states (suspensions → `running`, `compensated` → `skipped`), and `PUBLIC_WORKFLOW_SCHEMA`/`PUBLIC_WORKFLOW_STEP_SCHEMA`/`WORKFLOW_STATUS_SCHEMA`/`STEP_DISPLAY_STATUS_SCHEMA` ship the same shapes as Zod schemas for route `response` declarations. Extend with consumer fields via `PUBLIC_WORKFLOW_SCHEMA.extend({...})` + spread.

## 0.11.2

### Patch Changes

- [`c4de746`](https://github.com/octabits-io/platform/commit/c4de746634cb164c51580ae16fd2cee2100941b2) - Fix workflows stranded in `running` forever when a parallel branch fails while another branch is still in flight.

  `checkWorkflowFailure` correctly waited for in-flight steps to settle, but `onStepCompleted`'s terminal check only counted `completed`/`skipped` — a `failed` sibling made it wait too, so when the LAST in-flight step completed after an earlier parallel failure, neither path finalized the workflow. The completion path now routes through the failure check when any keyed sibling has failed, finalizing the workflow as `failed` (with dependent-skip cascade and compensation) once every remaining step settles. The map-child path already re-checked; this brings keyed DAG steps in line.

## 0.11.1

### Patch Changes

- [`eae8882`](https://github.com/octabits-io/platform/commit/eae888215cf06b50c1da2a71f424966f7f8ec3f9) - Widen the `typescript` peer range to `^5 || ^6 || ^7` — the packages build and typecheck cleanly under TypeScript 7 (native compiler), and the emitted declarations are semantically identical to the TS 5/6 output.

## 0.11.0

### Minor Changes

- [`4643be5`](https://github.com/octabits-io/platform/commit/4643be58b3eea62325e4e85268963adbb872f77f) - store-pg: **remove the `@octabits-io/flow/store-pg/schema` Drizzle column-set subpath** (`flowWorkflowColumns`, `flowWorkflowStepColumns`, `flowStepEventColumns`, `flowRateBucketColumns`, `flowStepLeaseColumns`).

  BREAKING for anyone importing that subpath — but it had no known consumers: it was added speculatively for a host that ended up defining the flow tables from the DDL blob (`flowStoreDdl()`/`flowGateDdl()`/`flowEventDdl()`) instead. It also shipped columns only, leaving the load-bearing partial-unique idempotency index (`createWorkflow`'s `ON CONFLICT` target) as a copy-paste snippet, and had no test tying the column-sets to the DDL — so the two representations could silently drift.

  Removing it drops `drizzle-orm` as an (optional) peer dependency entirely — the raw-`pg` store bundle never imported it. Hosts that want the flow tables in their own Drizzle migrations should model them on the DDL emitted by `flowStoreDdl()` / `flowGateDdl()` / `flowEventDdl()`, which remain the single source of truth. If a real Drizzle-native consumer appears, a column-set subpath can be reintroduced with the constraints exported (not copy-pasted) and a DDL-parity test.

- [`fe07889`](https://github.com/octabits-io/platform/commit/fe078899d1613ded7a63e20ae5559b0ee7d1ec27) - store-pg: thread the injectable `SqlExecutor` seam through the **step gate** and **event sink**, so a host can run _all_ flow SQL (store + gate + sink) through one executor — e.g. one that sets a transaction-local tenant GUC, bringing the flow tables under Row Level Security. Previously only `createWorkflowStore` took an executor while the gate and sink hardwired a `pg.Pool`, so a host could not adopt RLS on `flow.*` consistently (the 0.10.0 follow-up called out in that changelog).

  - **`createStepGate({ exec, … })`** — executor-backed gate; `createPgStepGate({ pool, … })` is unchanged and now delegates over `poolExecutor(pool)`. The concurrency-lease acquire runs inside `exec.transaction`, preserving the exact prior rollback-on-cap-hit behavior (advisory lock + expired-lease cleanup roll back together when the cap is hit).
  - **`createEventSink({ exec, … })`** — executor-backed observer; `createPgEventSink({ pool, … })` unchanged and delegates. `readFlowEvents` now accepts a `Pool | SqlExecutor`, so run-history reads can also run scoped.
  - The `SqlExecutor` / `SqlResult` / `poolExecutor` seam moved to a shared `./executor` module (re-exported from `./store` for compatibility) and gained `toExecutor(pool | exec)`.

  No behavior change for existing `createPg*` callers (all delegate through `poolExecutor`, verified against the full integration suite). The pg-boss dispatcher still takes a `Pool` directly — it owns its own connections and writes no `flow.*` tables, so it is out of scope for the executor seam.

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
