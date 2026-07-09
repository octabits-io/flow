import { describe, it, expect, vi } from 'vitest';
import type { Pool } from 'pg';
import {
  createWorkflowStore,
  createPgWorkflowStore,
  poolExecutor,
  type SqlExecutor,
  type SqlResult,
} from './store';

// A scripted SqlExecutor: hands back canned rows per matched SQL and records every
// call, so we can assert routing (query vs transaction) without a real database.
function recordingExecutor(script: (sql: string) => unknown[] = () => []) {
  const calls: { sql: string; params: unknown[]; tx: boolean }[] = [];
  let inTx = false;

  const run = async <R>(sql: string, params: unknown[] = []): Promise<SqlResult<R>> => {
    calls.push({ sql: sql.replace(/\s+/g, ' ').trim(), params, tx: inTx });
    const rows = script(sql) as R[];
    return { rows, rowCount: rows.length };
  };

  const exec: SqlExecutor = {
    query: (sql, params) => run(sql, params),
    async transaction(fn) {
      inTx = true;
      try {
        return await fn(exec);
      } finally {
        inTx = false;
      }
    },
  };
  return { exec, calls };
}

describe('createWorkflowStore — executor seam', () => {
  it('routes reads through exec.query and threads partitionKey + schema', async () => {
    const { exec, calls } = recordingExecutor(() => []);
    const store = createWorkflowStore({ exec, partitionKey: 'tenant-42', schema: 'flow' });

    const result = await store.getWorkflow(7);

    expect(result).toBeNull();
    expect(calls).toHaveLength(1);
    expect(calls[0]!.tx).toBe(false);
    expect(calls[0]!.sql).toContain('flow.flow_workflow');
    expect(calls[0]!.sql).toContain('partition_key = $2');
    expect(calls[0]!.params).toEqual([7, 'tenant-42']);
  });

  it('defaults the schema to public', async () => {
    const { exec, calls } = recordingExecutor(() => []);
    const store = createWorkflowStore({ exec, partitionKey: 'p' });
    await store.listRunningWorkflows();
    expect(calls[0]!.sql).toContain('public.flow_workflow');
  });

  it('runs createWorkflow inside a single transaction and returns mapped rows', async () => {
    const { exec, calls } = recordingExecutor((sql) => {
      if (sql.includes('INSERT INTO') && sql.includes('flow_workflow ')) return [{ id: '99' }];
      if (sql.includes('INSERT INTO') && sql.includes('flow_workflow_step')) {
        return [
          { id: '1', workflow_id: '99', partition_key: 'p', key: 'a', type: 't', status: 'pending', dependencies: [], input: null, output: null, error: null, metadata: null, attempts: 0, parent_step_id: null, started_at: null, completed_at: null },
        ];
      }
      return [];
    });
    const store = createWorkflowStore({ exec, partitionKey: 'p' });

    const created = await store.createWorkflow({
      type: 'demo',
      input: { x: 1 },
      startedAt: '2026-07-09T00:00:00.000Z',
      steps: [{ key: 'a', type: 't', dependencies: [], input: null }],
    });

    expect(created.workflowId).toBe(99);
    expect(created.alreadyExisted).toBe(false);
    expect(created.steps.map((s) => s.id)).toEqual([1]);
    // Every write happened on the transaction connection, not autocommit.
    expect(calls.length).toBeGreaterThanOrEqual(2);
    expect(calls.every((c) => c.tx)).toBe(true);
  });

  it('increments the workflow counter in the same tx as the step update on completeStep', async () => {
    const { exec, calls } = recordingExecutor(() => []);
    const store = createWorkflowStore({ exec, partitionKey: 'p' });

    await store.completeStep({ workflowId: 1, stepId: 2, output: { ok: true }, completedAt: '2026-07-09T00:00:00.000Z' });

    expect(calls).toHaveLength(2);
    expect(calls.every((c) => c.tx)).toBe(true);
    expect(calls[0]!.sql).toContain("status = 'completed'");
    expect(calls[1]!.sql).toContain('completed_steps = completed_steps + 1');
  });
});

describe('poolExecutor', () => {
  function fakePool() {
    const clientQueries: string[] = [];
    const client = {
      query: vi.fn(async (sql: string) => {
        clientQueries.push(sql);
        return { rows: [], rowCount: 0 };
      }),
      release: vi.fn(),
    };
    const pool = {
      query: vi.fn(async () => ({ rows: [{ n: 1 }], rowCount: 1 })),
      connect: vi.fn(async () => client),
    } as unknown as Pool;
    return { pool, client, clientQueries };
  }

  it('runs top-level queries on the pool (autocommit)', async () => {
    const { pool } = fakePool();
    const exec = poolExecutor(pool);
    const res = await exec.query('SELECT 1');
    expect(res.rows).toEqual([{ n: 1 }]);
    expect((pool.connect as ReturnType<typeof vi.fn>)).not.toHaveBeenCalled();
  });

  it('wraps transaction bodies in BEGIN/COMMIT and always releases', async () => {
    const { pool, client, clientQueries } = fakePool();
    const exec = poolExecutor(pool);

    await exec.transaction(async (tx) => {
      await tx.query('UPDATE t SET x = 1');
    });

    expect(clientQueries).toEqual(['BEGIN', 'UPDATE t SET x = 1', 'COMMIT']);
    expect(client.release).toHaveBeenCalledOnce();
  });

  it('rolls back and releases when the transaction body throws', async () => {
    const { pool, client, clientQueries } = fakePool();
    const exec = poolExecutor(pool);

    await expect(
      exec.transaction(async () => {
        throw new Error('boom');
      }),
    ).rejects.toThrow('boom');

    expect(clientQueries).toContain('ROLLBACK');
    expect(clientQueries).not.toContain('COMMIT');
    expect(client.release).toHaveBeenCalledOnce();
  });
});

describe('createPgWorkflowStore — backward compatibility', () => {
  it('still builds a full WorkflowStore over a bare pool and filters by partition_key', async () => {
    const query = vi.fn(async () => ({ rows: [], rowCount: 0 }));
    const pool = { query, connect: vi.fn() } as unknown as Pool;

    const store = createPgWorkflowStore({ pool, partitionKey: 'tenant-1' });

    // Shape is the full WorkflowStore surface (spot-check a few methods exist).
    for (const m of ['createWorkflow', 'getWorkflow', 'completeStep', 'listWorkflows', 'findStuckSteps'] as const) {
      expect(typeof store[m]).toBe('function');
    }

    await store.getStep(5);
    expect(query).toHaveBeenCalledOnce();
    const [sql, params] = (query.mock.calls[0] ?? []) as unknown as [string, unknown[]];
    expect(sql).toContain('public.flow_workflow_step');
    expect(params).toEqual([5, 'tenant-1']);
  });
});
