import type { Pool } from 'pg';
import type {
  WorkflowStore,
  CreateWorkflowParams,
  CreatedWorkflow,
  CompleteStepParams,
  FailStepParams,
  FinishWorkflowParams,
  ListWorkflowsFilters,
  AddChildStep,
  WorkflowId,
  StepId,
  WorkflowRecord,
  StepRecord,
  WorkflowStatus,
  StepStatus,
  WorkflowWithSteps,
} from '../core';
import { type SqlExecutor, poolExecutor } from './executor';

// The executor seam (`SqlExecutor`, `SqlResult`, `poolExecutor`) moved to
// `./executor` so the store, gate and event sink can share one seam. Re-exported
// here for backward compatibility with `@octabits-io/flow/store-pg` consumers.
export { type SqlExecutor, type SqlResult, poolExecutor, toExecutor } from './executor';

export interface PgWorkflowStoreDeps {
  pool: Pool;
  /** Partition this store instance is bound to (e.g. a tenant id). */
  partitionKey: string;
  /** Schema the flow tables live in. Default 'public'. */
  schema?: string;
}

export interface WorkflowStoreDeps {
  /** How the store talks to Postgres (pool-backed, RLS-scoped, …). */
  exec: SqlExecutor;
  /** Partition this store instance is bound to (e.g. a tenant id). */
  partitionKey: string;
  /** Schema the flow tables live in. Default 'public'. */
  schema?: string;
}

// --- row mappers -----------------------------------------------------------

const iso = (v: Date | string | null): string | null =>
  v == null ? null : v instanceof Date ? v.toISOString() : v;

/**
 * Raw `flow_workflow` row as returned by `pg`. Columns are snake_case; `bigint`/`bigserial`
 * come back as `string`, `integer` as `number`, `timestamptz` as `Date`, `jsonb` parsed.
 * Typing this makes the row→record mapping compiler-checked (column typos no longer compile).
 */
type WorkflowRow = {
  id: string;
  partition_key: string;
  type: string;
  status: WorkflowStatus;
  input: Record<string, unknown> | null;
  output: Record<string, unknown> | null;
  error: string | null;
  entity_ref: string | null;
  total_steps: number;
  completed_steps: number;
  failed_steps: number;
  metadata: Record<string, unknown> | null;
  idempotency_key: string | null;
  parent_workflow_id: string | null;
  parent_step_id: string | null;
  created_at: Date;
  started_at: Date | null;
  completed_at: Date | null;
};

/** Raw `flow_workflow_step` row as returned by `pg` (see `WorkflowRow` for column-type notes). */
type StepRow = {
  id: string;
  workflow_id: string;
  partition_key: string;
  key: string;
  type: string;
  status: StepStatus;
  dependencies: string[] | null;
  input: Record<string, unknown> | null;
  output: Record<string, unknown> | null;
  error: string | null;
  metadata: Record<string, unknown> | null;
  attempts: number;
  parent_step_id: string | null;
  started_at: Date | null;
  completed_at: Date | null;
};

function mapWorkflow(r: WorkflowRow): WorkflowRecord {
  return {
    id: Number(r.id),
    type: r.type,
    status: r.status,
    partitionKey: r.partition_key,
    input: r.input ?? {},
    output: r.output ?? null,
    error: r.error ?? null,
    entityRef: r.entity_ref ?? null,
    totalSteps: Number(r.total_steps),
    completedSteps: Number(r.completed_steps),
    failedSteps: Number(r.failed_steps),
    metadata: r.metadata ?? null,
    idempotencyKey: r.idempotency_key ?? null,
    parentWorkflowId: r.parent_workflow_id == null ? null : Number(r.parent_workflow_id),
    parentStepId: r.parent_step_id == null ? null : Number(r.parent_step_id),
    createdAt: iso(r.created_at)!,
    startedAt: iso(r.started_at),
    completedAt: iso(r.completed_at),
  };
}

function mapStep(r: StepRow): StepRecord {
  return {
    id: Number(r.id),
    workflowId: Number(r.workflow_id),
    key: r.key,
    type: r.type,
    status: r.status,
    dependencies: r.dependencies ?? [],
    input: r.input ?? null,
    output: r.output ?? null,
    error: r.error ?? null,
    metadata: r.metadata ?? null,
    attempts: Number(r.attempts ?? 0),
    parentStepId: r.parent_step_id == null ? null : Number(r.parent_step_id),
    startedAt: iso(r.started_at),
    completedAt: iso(r.completed_at),
  };
}

/**
 * A `WorkflowStore` over Postgres, addressing raw parameterized SQL through an
 * injected {@link SqlExecutor}. Partition-scoped at construction; every query is
 * additionally filtered by `partition_key` as defense-in-depth. Requires the two
 * tables from `flowStoreDdl()`.
 *
 * The executor decides *how* queries run: {@link createPgWorkflowStore} wires a
 * plain pool; a host that wants Row Level Security injects an executor whose
 * `transaction` sets the tenant GUC, so the store's own transactions run scoped.
 */
export function createWorkflowStore(deps: WorkflowStoreDeps): WorkflowStore {
  const { exec, partitionKey } = deps;
  const schema = deps.schema ?? 'public';
  const WF = `${schema}.flow_workflow`;
  const STEP = `${schema}.flow_workflow_step`;

  async function createWorkflow(params: CreateWorkflowParams): Promise<CreatedWorkflow> {
    return exec.transaction(async (client) => {
      // ON CONFLICT targets the partial unique index on (partition_key, idempotency_key).
      // A null key is excluded from the index, so unkeyed starts never conflict.
      const wfRes = await client.query<{ id: string }>(
        `INSERT INTO ${WF} (partition_key, type, status, input, total_steps, entity_ref, idempotency_key, metadata, parent_workflow_id, parent_step_id, created_at, started_at)
         VALUES ($1, $2, 'running', $3::jsonb, $4, $5, $6, $7::jsonb, $8, $9, $10, $10)
         ON CONFLICT (partition_key, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
         RETURNING id`,
        [
          partitionKey,
          params.type,
          JSON.stringify(params.input ?? {}),
          params.steps.length,
          params.entityRef ?? null,
          params.idempotencyKey ?? null,
          params.metadata != null ? JSON.stringify(params.metadata) : null,
          params.parentWorkflowId ?? null,
          params.parentStepId ?? null,
          params.startedAt,
        ],
      );

      // Idempotency hit: no row inserted — load and return the existing workflow + steps.
      if (wfRes.rows.length === 0) {
        const existing = await client.query<{ id: string }>(
          `SELECT id FROM ${WF} WHERE partition_key = $1 AND idempotency_key = $2`,
          [partitionKey, params.idempotencyKey],
        );
        const existingId = Number(existing.rows[0]!.id);
        const stepRows = await client.query<StepRow>(
          `SELECT * FROM ${STEP} WHERE workflow_id = $1 AND partition_key = $2 ORDER BY id`,
          [existingId, partitionKey],
        );
        return { workflowId: existingId, steps: stepRows.rows.map(mapStep), alreadyExisted: true };
      }

      const workflowId = Number(wfRes.rows[0]!.id);

      const steps: StepRecord[] = [];
      for (const s of params.steps) {
        const stepRes = await client.query<StepRow>(
          `INSERT INTO ${STEP} (workflow_id, partition_key, key, type, status, dependencies, input)
           VALUES ($1, $2, $3, $4, 'pending', $5::jsonb, $6::jsonb)
           RETURNING *`,
          [
            workflowId,
            partitionKey,
            s.key,
            s.type,
            JSON.stringify(s.dependencies ?? []),
            s.input != null ? JSON.stringify(s.input) : null,
          ],
        );
        steps.push(mapStep(stepRes.rows[0]!));
      }

      return { workflowId, steps, alreadyExisted: false };
    });
  }

  async function getWorkflow(workflowId: WorkflowId): Promise<WorkflowRecord | null> {
    const res = await exec.query<WorkflowRow>(`SELECT * FROM ${WF} WHERE id = $1 AND partition_key = $2`, [workflowId, partitionKey]);
    return res.rows[0] ? mapWorkflow(res.rows[0]) : null;
  }

  async function getStep(stepId: StepId): Promise<StepRecord | null> {
    const res = await exec.query<StepRow>(`SELECT * FROM ${STEP} WHERE id = $1 AND partition_key = $2`, [stepId, partitionKey]);
    return res.rows[0] ? mapStep(res.rows[0]) : null;
  }

  async function listSteps(workflowId: WorkflowId): Promise<StepRecord[]> {
    const res = await exec.query<StepRow>(
      `SELECT * FROM ${STEP} WHERE workflow_id = $1 AND partition_key = $2 ORDER BY id`,
      [workflowId, partitionKey],
    );
    return res.rows.map(mapStep);
  }

  async function markStepRunning(stepId: StepId, startedAt: string): Promise<void> {
    await exec.query(
      `UPDATE ${STEP} SET status = 'running', started_at = $2, attempts = attempts + 1 WHERE id = $1 AND partition_key = $3`,
      [stepId, startedAt, partitionKey],
    );
  }

  async function markStepPending(stepId: StepId): Promise<void> {
    await exec.query(
      `UPDATE ${STEP} SET status = 'pending' WHERE id = $1 AND partition_key = $2`,
      [stepId, partitionKey],
    );
  }

  async function markStepWaiting(stepId: StepId): Promise<void> {
    await exec.query(
      `UPDATE ${STEP} SET status = 'waiting' WHERE id = $1 AND partition_key = $2`,
      [stepId, partitionKey],
    );
  }

  async function markStepMapping(stepId: StepId): Promise<void> {
    await exec.query(
      `UPDATE ${STEP} SET status = 'mapping' WHERE id = $1 AND partition_key = $2`,
      [stepId, partitionKey],
    );
  }

  async function markStepCompensating(stepId: StepId): Promise<void> {
    await exec.query(
      `UPDATE ${STEP} SET status = 'compensating' WHERE id = $1 AND partition_key = $2`,
      [stepId, partitionKey],
    );
  }

  async function markStepCompensated(stepId: StepId, error?: string): Promise<void> {
    await exec.query(
      `UPDATE ${STEP} SET status = 'compensated', error = COALESCE($3, error) WHERE id = $1 AND partition_key = $2`,
      [stepId, partitionKey, error ?? null],
    );
  }

  async function addChildSteps(workflowId: WorkflowId, parentStepId: StepId, children: AddChildStep[]): Promise<StepRecord[]> {
    return exec.transaction(async (client) => {
      const created: StepRecord[] = [];
      for (const c of children) {
        const res = await client.query<StepRow>(
          `INSERT INTO ${STEP} (workflow_id, partition_key, key, type, status, dependencies, input, parent_step_id)
           VALUES ($1, $2, $3, $4, 'pending', '[]'::jsonb, $5::jsonb, $6)
           RETURNING *`,
          [workflowId, partitionKey, c.key, c.type, c.input != null ? JSON.stringify(c.input) : null, parentStepId],
        );
        created.push(mapStep(res.rows[0]!));
      }
      await client.query(
        `UPDATE ${WF} SET total_steps = total_steps + $2 WHERE id = $1 AND partition_key = $3`,
        [workflowId, children.length, partitionKey],
      );
      return created;
    });
  }

  async function listChildSteps(parentStepId: StepId): Promise<StepRecord[]> {
    const res = await exec.query<StepRow>(
      `SELECT * FROM ${STEP} WHERE parent_step_id = $1 AND partition_key = $2 ORDER BY id`,
      [parentStepId, partitionKey],
    );
    return res.rows.map(mapStep);
  }

  async function completeStep(params: CompleteStepParams): Promise<void> {
    await exec.transaction(async (client) => {
      await client.query(
        `UPDATE ${STEP} SET status = 'completed', output = $2::jsonb, completed_at = $3 WHERE id = $1 AND partition_key = $4`,
        [params.stepId, JSON.stringify(params.output), params.completedAt, partitionKey],
      );
      await client.query(
        `UPDATE ${WF} SET completed_steps = completed_steps + 1 WHERE id = $1 AND partition_key = $2`,
        [params.workflowId, partitionKey],
      );
    });
  }

  async function failStep(params: FailStepParams): Promise<void> {
    await exec.transaction(async (client) => {
      await client.query(
        `UPDATE ${STEP} SET status = 'failed', error = $2, completed_at = $3 WHERE id = $1 AND partition_key = $4`,
        [params.stepId, params.error, params.completedAt, partitionKey],
      );
      await client.query(
        `UPDATE ${WF} SET failed_steps = failed_steps + 1 WHERE id = $1 AND partition_key = $2`,
        [params.workflowId, partitionKey],
      );
    });
  }

  async function skipStep(stepId: StepId, reason: string): Promise<void> {
    await exec.query(
      `UPDATE ${STEP} SET status = 'skipped', error = $2 WHERE id = $1 AND partition_key = $3`,
      [stepId, reason, partitionKey],
    );
  }

  async function skipPendingSteps(workflowId: WorkflowId, reason: string): Promise<void> {
    await exec.query(
      `UPDATE ${STEP} SET status = 'skipped', error = $2 WHERE workflow_id = $1 AND partition_key = $3 AND status IN ('pending', 'waiting')`,
      [workflowId, reason, partitionKey],
    );
  }

  async function finishWorkflow(params: FinishWorkflowParams): Promise<void> {
    await exec.query(
      `UPDATE ${WF}
         SET status = $2,
             output = COALESCE($3::jsonb, output),
             error = COALESCE($4, error),
             completed_at = $5
       WHERE id = $1 AND partition_key = $6`,
      [
        params.workflowId,
        params.status,
        params.output != null ? JSON.stringify(params.output) : null,
        params.error ?? null,
        params.completedAt,
        partitionKey,
      ],
    );
  }

  async function listWorkflows(filters: ListWorkflowsFilters): Promise<WorkflowWithSteps[]> {
    const conds = ['partition_key = $1'];
    const args: unknown[] = [partitionKey];
    if (filters.status) {
      args.push(filters.status);
      conds.push(`status = $${args.length}`);
    }
    if (filters.type) {
      args.push(filters.type);
      conds.push(`type = $${args.length}`);
    }
    if (filters.entityRef) {
      args.push(filters.entityRef);
      conds.push(`entity_ref = $${args.length}`);
    }
    args.push(filters.limit ?? 50);
    const limitIdx = args.length;

    const wfRes = await exec.query<WorkflowRow>(
      `SELECT * FROM ${WF} WHERE ${conds.join(' AND ')} ORDER BY id DESC LIMIT $${limitIdx}`,
      args,
    );
    if (wfRes.rows.length === 0) return [];

    const ids = wfRes.rows.map((r) => Number(r.id));
    const stepRes = await exec.query<StepRow>(
      `SELECT * FROM ${STEP} WHERE workflow_id = ANY($1::bigint[]) AND partition_key = $2 ORDER BY id`,
      [ids, partitionKey],
    );
    const stepsByWf = new Map<number, StepRecord[]>();
    for (const row of stepRes.rows) {
      const s = mapStep(row);
      const list = stepsByWf.get(s.workflowId) ?? [];
      list.push(s);
      stepsByWf.set(s.workflowId, list);
    }
    return wfRes.rows.map((r) => ({ ...mapWorkflow(r), steps: stepsByWf.get(Number(r.id)) ?? [] }));
  }

  async function listRunningWorkflows(): Promise<WorkflowRecord[]> {
    const res = await exec.query<WorkflowRow>(`SELECT * FROM ${WF} WHERE partition_key = $1 AND status = 'running'`, [partitionKey]);
    return res.rows.map(mapWorkflow);
  }

  async function findStuckSteps(workflowId: WorkflowId, cutoff: string): Promise<StepRecord[]> {
    const res = await exec.query<StepRow>(
      `SELECT * FROM ${STEP}
        WHERE workflow_id = $1 AND partition_key = $2 AND status = 'running' AND started_at IS NOT NULL AND started_at < $3::timestamptz`,
      [workflowId, partitionKey, cutoff],
    );
    return res.rows.map(mapStep);
  }

  return {
    createWorkflow,
    getWorkflow,
    getStep,
    listSteps,
    markStepRunning,
    markStepPending,
    markStepWaiting,
    markStepMapping,
    markStepCompensating,
    markStepCompensated,
    addChildSteps,
    listChildSteps,
    completeStep,
    failStep,
    skipStep,
    skipPendingSteps,
    finishWorkflow,
    listWorkflows,
    listRunningWorkflows,
    findStuckSteps,
  };
}

/**
 * A `WorkflowStore` backed by a `pg` {@link Pool} — the batteries-included
 * adapter that owns its own connections/transactions (via {@link poolExecutor})
 * and requires the tables from `flowStoreDdl()`. Unchanged public surface: hosts
 * that need Row Level Security or their own migrations should instead build an
 * executor and call {@link createWorkflowStore}.
 */
export function createPgWorkflowStore(deps: PgWorkflowStoreDeps): WorkflowStore {
  return createWorkflowStore({
    exec: poolExecutor(deps.pool),
    partitionKey: deps.partitionKey,
    schema: deps.schema,
  });
}

/** Convenience: apply the flow-store schema to a database. */
export async function applySchema(pool: Pool, ddl: string): Promise<void> {
  await pool.query(ddl);
}
