import { describe, it, expect } from 'vitest';
import { sql } from 'drizzle-orm';
import { pgTable, getTableConfig, index, uniqueIndex, unique, primaryKey, foreignKey } from 'drizzle-orm/pg-core';
import {
  flowWorkflowColumns,
  flowWorkflowStepColumns,
  flowStepEventColumns,
  flowRateBucketColumns,
  flowStepLeaseColumns,
} from './schema';

// Assemble the tables exactly as the module doc instructs a consumer to, then read
// them back with getTableConfig. This proves the column-sets are complete AND that
// the documented constraint snippet uses a valid drizzle API (a wrong builder call
// throws at pgTable() time) and preserves the load-bearing constraints.

const flowWorkflow = pgTable('flow_workflow', flowWorkflowColumns, (t) => [
  index('flow_workflow_partition_status_idx').on(t.partitionKey, t.status),
  index('flow_workflow_parent_idx').on(t.parentWorkflowId),
  index('flow_workflow_partition_type_idx').on(t.partitionKey, t.type),
  index('flow_workflow_partition_entity_idx').on(t.partitionKey, t.entityRef),
  uniqueIndex('flow_workflow_idempotency_idx')
    .on(t.partitionKey, t.idempotencyKey)
    .where(sql`${t.idempotencyKey} is not null`),
]);

const flowWorkflowStep = pgTable('flow_workflow_step', flowWorkflowStepColumns, (t) => [
  unique('flow_workflow_step_workflow_key_uq').on(t.workflowId, t.key),
  index('flow_workflow_step_workflow_idx').on(t.workflowId),
  index('flow_workflow_step_status_idx').on(t.workflowId, t.status),
  index('flow_workflow_step_parent_idx').on(t.parentStepId),
  foreignKey({ columns: [t.workflowId], foreignColumns: [flowWorkflow.id] }).onDelete('cascade'),
  foreignKey({ columns: [t.parentStepId], foreignColumns: [t.id] }).onDelete('cascade'),
]);

const flowStepEvent = pgTable('flow_step_event', flowStepEventColumns, (t) => [
  index('flow_step_event_workflow_idx').on(t.partitionKey, t.workflowId, t.id),
]);

const flowRateBucket = pgTable('flow_rate_bucket', flowRateBucketColumns, (t) => [
  primaryKey({ columns: [t.partitionKey, t.stepType] }),
]);

const flowStepLease = pgTable('flow_step_lease', flowStepLeaseColumns, (t) => [
  primaryKey({ columns: [t.partitionKey, t.stepType, t.stepId] }),
  index('flow_step_lease_active_idx').on(t.partitionKey, t.stepType, t.expiresAt),
]);

const columnNames = (t: Parameters<typeof getTableConfig>[0]) =>
  getTableConfig(t).columns.map((c) => c.name).sort();

describe('flow store-pg schema column-sets', () => {
  it('flow_workflow has exactly the DDL columns, snake_case', () => {
    expect(columnNames(flowWorkflow)).toEqual(
      [
        'completed_at', 'completed_steps', 'created_at', 'entity_ref', 'error',
        'failed_steps', 'id', 'idempotency_key', 'input', 'metadata',
        'output', 'parent_step_id', 'parent_workflow_id', 'partition_key',
        'started_at', 'status', 'total_steps', 'type',
      ].sort(),
    );
  });

  it('the documented idempotency index is unique and partial (ON CONFLICT target)', () => {
    const idem = getTableConfig(flowWorkflow).indexes.find(
      (i) => i.config.name === 'flow_workflow_idempotency_idx',
    );
    expect(idem).toBeDefined();
    expect(idem!.config.unique).toBe(true);
    expect(idem!.config.where).toBeDefined(); // WHERE idempotency_key IS NOT NULL
  });

  it('flow_workflow_step carries the UNIQUE(workflow_id, key) guard and both FKs', () => {
    const cfg = getTableConfig(flowWorkflowStep);
    expect(cfg.uniqueConstraints.map((u) => u.name)).toContain('flow_workflow_step_workflow_key_uq');
    expect(cfg.foreignKeys).toHaveLength(2);
  });

  it('flow_step_event is column-complete', () => {
    expect(columnNames(flowStepEvent)).toEqual(
      [
        'at', 'attempt', 'duration_ms', 'error', 'event_type', 'id',
        'partition_key', 'step_id', 'step_key', 'step_type', 'workflow_id', 'workflow_type',
      ].sort(),
    );
  });

  it('gate tables declare their composite primary keys', () => {
    expect(getTableConfig(flowRateBucket).primaryKeys[0]!.columns.map((c) => c.name)).toEqual([
      'partition_key', 'step_type',
    ]);
    expect(getTableConfig(flowStepLease).primaryKeys[0]!.columns.map((c) => c.name)).toEqual([
      'partition_key', 'step_type', 'step_id',
    ]);
  });
});
