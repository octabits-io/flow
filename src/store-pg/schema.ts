/**
 * @octabits-io/flow/store-pg/schema — Drizzle column-sets for the Postgres
 * WorkflowStore, gate, and event-sink tables.
 *
 * These mirror `flowStoreDdl()` / `flowGateDdl()` / `flowEventDdl()` on the column
 * side, but as spreadable Drizzle column definitions instead of a DDL string.
 * Spread them into your own `pgTable(...)` so the flow tables live in *your*
 * schema, *your* drizzle-kit migrations, and (if you want it) under *your* Row
 * Level Security — instead of being applied out-of-band from a copied DDL blob.
 *
 * Pair this with an RLS-aware {@link SqlExecutor} + `createWorkflowStore(...)` and
 * the engine's own transactions run scoped to your tenant GUC.
 *
 * Following the `@octabits-io/drizzle-toolkit/scope` precedent, this module ships
 * **column-sets only** — indexes, uniqueness, composite PKs, and FKs are yours to
 * declare, because they depend on your schema/RLS choices. Copy the constraints
 * below verbatim; the store's `createWorkflow` uses `ON CONFLICT` against the
 * partial-unique idempotency index, so **do not drop it**:
 *
 * ```ts
 * import { pgTable, index, uniqueIndex, unique, primaryKey, foreignKey } from 'drizzle-orm/pg-core';
 * import { sql } from 'drizzle-orm';
 * import {
 *   flowWorkflowColumns, flowWorkflowStepColumns, flowStepEventColumns,
 *   flowRateBucketColumns, flowStepLeaseColumns,
 * } from '@octabits-io/flow/store-pg/schema';
 *
 * export const flowWorkflow = pgTable('flow_workflow', flowWorkflowColumns, (t) => [
 *   index('flow_workflow_partition_status_idx').on(t.partitionKey, t.status),
 *   index('flow_workflow_parent_idx').on(t.parentWorkflowId),
 *   index('flow_workflow_partition_type_idx').on(t.partitionKey, t.type),
 *   index('flow_workflow_partition_entity_idx').on(t.partitionKey, t.entityRef),
 *   uniqueIndex('flow_workflow_idempotency_idx')            // ← load-bearing (ON CONFLICT target)
 *     .on(t.partitionKey, t.idempotencyKey)
 *     .where(sql`${t.idempotencyKey} is not null`),
 * ]);
 *
 * export const flowWorkflowStep = pgTable('flow_workflow_step', flowWorkflowStepColumns, (t) => [
 *   unique('flow_workflow_step_workflow_key_uq').on(t.workflowId, t.key),
 *   index('flow_workflow_step_workflow_idx').on(t.workflowId),
 *   index('flow_workflow_step_status_idx').on(t.workflowId, t.status),
 *   index('flow_workflow_step_parent_idx').on(t.parentStepId),
 *   foreignKey({ columns: [t.workflowId], foreignColumns: [flowWorkflow.id] }).onDelete('cascade'),
 *   foreignKey({ columns: [t.parentStepId], foreignColumns: [t.id] }).onDelete('cascade'),
 * ]);
 *
 * export const flowStepEvent = pgTable('flow_step_event', flowStepEventColumns, (t) => [
 *   index('flow_step_event_workflow_idx').on(t.partitionKey, t.workflowId, t.id),
 * ]);
 *
 * export const flowRateBucket = pgTable('flow_rate_bucket', flowRateBucketColumns, (t) => [
 *   primaryKey({ columns: [t.partitionKey, t.stepType] }),
 * ]);
 *
 * export const flowStepLease = pgTable('flow_step_lease', flowStepLeaseColumns, (t) => [
 *   primaryKey({ columns: [t.partitionKey, t.stepType, t.stepId] }),
 *   index('flow_step_lease_active_idx').on(t.partitionKey, t.stepType, t.expiresAt),
 * ]);
 * ```
 *
 * Only `drizzle-orm` primitives are used — no framework or app imports. `drizzle-orm`
 * is an *optional* peer; importing this subpath is the only thing in `store-pg` that
 * needs it (the raw-`pg` store does not).
 */
import { bigint, bigserial, doublePrecision, integer, jsonb, text, timestamp } from 'drizzle-orm/pg-core';

const ts = (name: string) => timestamp(name, { withTimezone: true, mode: 'string' });

// ---------------------------------------------------------------------------
// flow_workflow
// ---------------------------------------------------------------------------

/**
 * Columns for the `flow_workflow` table. `partition_key` is the generic
 * multi-tenancy column the store filters on; add a Row Level Security policy over
 * it for hard isolation. See the module doc for the required indexes (notably the
 * partial-unique `flow_workflow_idempotency_idx`) and FKs.
 */
export const flowWorkflowColumns = {
  id: bigserial('id', { mode: 'number' }).primaryKey(),
  partitionKey: text('partition_key').notNull(),
  type: text('type').notNull(),
  status: text('status').notNull(),
  input: jsonb('input').$type<Record<string, unknown>>().notNull().default({}),
  output: jsonb('output').$type<Record<string, unknown>>(),
  error: text('error'),
  entityRef: text('entity_ref'),
  totalSteps: integer('total_steps').notNull(),
  completedSteps: integer('completed_steps').notNull().default(0),
  failedSteps: integer('failed_steps').notNull().default(0),
  metadata: jsonb('metadata').$type<Record<string, unknown>>(),
  idempotencyKey: text('idempotency_key'),
  parentWorkflowId: bigint('parent_workflow_id', { mode: 'number' }),
  parentStepId: bigint('parent_step_id', { mode: 'number' }),
  createdAt: ts('created_at').defaultNow().notNull(),
  startedAt: ts('started_at'),
  completedAt: ts('completed_at'),
};

// ---------------------------------------------------------------------------
// flow_workflow_step
// ---------------------------------------------------------------------------

/**
 * Columns for the `flow_workflow_step` table. See the module doc for the required
 * `UNIQUE(workflow_id, key)` guard, read indexes, and the `workflow_id` /
 * `parent_step_id` foreign keys.
 */
export const flowWorkflowStepColumns = {
  id: bigserial('id', { mode: 'number' }).primaryKey(),
  workflowId: bigint('workflow_id', { mode: 'number' }).notNull(),
  partitionKey: text('partition_key').notNull(),
  key: text('key').notNull(),
  type: text('type').notNull(),
  status: text('status').notNull(),
  dependencies: jsonb('dependencies').$type<string[]>().notNull().default([]),
  input: jsonb('input').$type<Record<string, unknown>>(),
  output: jsonb('output').$type<Record<string, unknown>>(),
  error: text('error'),
  metadata: jsonb('metadata').$type<Record<string, unknown>>(),
  attempts: integer('attempts').notNull().default(0),
  parentStepId: bigint('parent_step_id', { mode: 'number' }),
  startedAt: ts('started_at'),
  completedAt: ts('completed_at'),
};

// ---------------------------------------------------------------------------
// flow_step_event (append-only run history)
// ---------------------------------------------------------------------------

/** Columns for the append-only `flow_step_event` run-history table (see `createPgEventSink`). */
export const flowStepEventColumns = {
  id: bigserial('id', { mode: 'number' }).primaryKey(),
  partitionKey: text('partition_key').notNull(),
  workflowId: bigint('workflow_id', { mode: 'number' }).notNull(),
  workflowType: text('workflow_type'),
  stepId: bigint('step_id', { mode: 'number' }),
  stepKey: text('step_key'),
  stepType: text('step_type'),
  eventType: text('event_type').notNull(),
  attempt: integer('attempt'),
  durationMs: integer('duration_ms'),
  error: text('error'),
  at: ts('at').notNull(),
};

// ---------------------------------------------------------------------------
// flow_rate_bucket / flow_step_lease (StepGate)
// ---------------------------------------------------------------------------

/** Columns for the gate's token-bucket table `flow_rate_bucket` (composite PK is consumer-declared). */
export const flowRateBucketColumns = {
  partitionKey: text('partition_key').notNull(),
  stepType: text('step_type').notNull(),
  tokens: doublePrecision('tokens').notNull(),
  updatedAt: ts('updated_at').defaultNow().notNull(),
};

/** Columns for the gate's concurrency-lease table `flow_step_lease` (composite PK is consumer-declared). */
export const flowStepLeaseColumns = {
  partitionKey: text('partition_key').notNull(),
  stepType: text('step_type').notNull(),
  stepId: bigint('step_id', { mode: 'number' }).notNull(),
  acquiredAt: ts('acquired_at').defaultNow().notNull(),
  expiresAt: ts('expires_at').notNull(),
};
