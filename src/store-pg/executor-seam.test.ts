import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Pool } from 'pg';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import {
  createStepGate,
  createEventSink,
  readFlowEvents,
  poolExecutor,
  applySchema,
  FLOW_GATE_DDL,
  FLOW_EVENT_DDL,
  type SqlExecutor,
} from './index';
import type { StepGateRequest, FlowEvent } from '../core';

let container: StartedPostgreSqlContainer;
let pool: Pool;

beforeAll(async () => {
  container = await new PostgreSqlContainer('postgres:17-alpine').start();
  pool = new Pool({ connectionString: container.getConnectionUri() });
  await applySchema(pool, FLOW_GATE_DDL);
  await applySchema(pool, FLOW_EVENT_DDL);
}, 120_000);

afterAll(async () => {
  await pool?.end();
  await container?.stop();
});

const req = (partitionKey: string, stepType: string, stepId: number): StepGateRequest => ({
  partitionKey,
  workflowId: 1,
  stepId,
  stepKey: `k${stepId}`,
  stepType,
});

const evt = (partitionKey: string, workflowId: number): FlowEvent => ({
  type: 'step.completed',
  at: new Date().toISOString(),
  partitionKey,
  workflowId,
  stepId: 1,
  stepKey: 'k1',
  stepType: 'task',
  attempt: 1,
  durationMs: 5,
});

/** An executor that wraps another and counts every query/transaction it sees. */
function spyExecutor(inner: SqlExecutor): SqlExecutor & { queries: number; txns: number } {
  const spy = {
    queries: 0,
    txns: 0,
    async query<R = Record<string, unknown>>(sql: string, params?: unknown[]) {
      spy.queries++;
      return inner.query<R>(sql, params);
    },
    async transaction<T>(fn: (tx: SqlExecutor) => Promise<T>) {
      spy.txns++;
      return inner.transaction(fn);
    },
  };
  return spy;
}

describe('store-pg executor seam', () => {
  it('routes all gate + event-sink SQL through the injected executor', async () => {
    const spy = spyExecutor(poolExecutor(pool));
    const gate = createStepGate({ exec: spy, partitionKey: 'seam', concurrency: { task: { maxConcurrent: 1 } } });
    const sink = createEventSink({ exec: spy, partitionKey: 'seam' });

    const decision = await gate.acquire(req('seam', 'task', 1));
    expect(decision.admitted).toBe(true);
    if (decision.admitted) await decision.release();

    sink.record(evt('seam', 10));
    await sink.flush();

    // The adapters opened no connections of their own — every statement went
    // through the injected executor (≥1 transaction for the lease acquire, ≥1
    // top-level query for the release + the event insert).
    expect(spy.txns).toBeGreaterThanOrEqual(1);
    expect(spy.queries).toBeGreaterThanOrEqual(2);
  });

  it('createStepGate over poolExecutor matches the pool-backed behavior (cap + release)', async () => {
    const gate = createStepGate({
      exec: poolExecutor(pool),
      partitionKey: 'cap',
      concurrency: { task: { maxConcurrent: 1 } },
      concurrencyRetrySeconds: 2,
    });

    const a = await gate.acquire(req('cap', 'task', 1));
    const b = await gate.acquire(req('cap', 'task', 2));
    expect(a.admitted).toBe(true);
    expect(b.admitted).toBe(false); // cap hit → the acquire transaction rolled back
    if (!b.admitted) expect(b.retryAfterSeconds).toBe(2);

    if (a.admitted) await a.release();
    expect((await gate.acquire(req('cap', 'task', 2))).admitted).toBe(true);
  });

  it('runs the gate and event sink under Row Level Security via a GUC-scoped executor', async () => {
    // Hard tenant isolation: RLS policies keyed on a per-transaction GUC. A host
    // injects an executor that sets that GUC, so the adapters' own SQL runs scoped.
    // The scoped executor runs as a non-superuser app role — a superuser (the
    // testcontainer's default login) bypasses RLS entirely.
    await pool.query(`
      ALTER TABLE flow_step_lease  ENABLE ROW LEVEL SECURITY;
      ALTER TABLE flow_step_lease  FORCE  ROW LEVEL SECURITY;
      ALTER TABLE flow_step_event  ENABLE ROW LEVEL SECURITY;
      ALTER TABLE flow_step_event  FORCE  ROW LEVEL SECURITY;
      CREATE POLICY p_lease ON flow_step_lease
        USING (partition_key = current_setting('app.flow_pk', true))
        WITH CHECK (partition_key = current_setting('app.flow_pk', true));
      CREATE POLICY p_event ON flow_step_event
        USING (partition_key = current_setting('app.flow_pk', true))
        WITH CHECK (partition_key = current_setting('app.flow_pk', true));
      CREATE ROLE flow_app NOSUPERUSER;
      GRANT SELECT, INSERT, UPDATE, DELETE ON flow_step_lease, flow_step_event, flow_rate_bucket TO flow_app;
      GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO flow_app;
    `);

    const scoped = (pk: string): SqlExecutor => {
      const enter = async (c: import('pg').PoolClient) => {
        await c.query('SET LOCAL ROLE flow_app'); // drop superuser so RLS applies
        await c.query('SELECT set_config($1, $2, true)', ['app.flow_pk', pk]);
      };
      return {
        async query(sql, params) {
          const c = await pool.connect();
          try {
            await c.query('BEGIN');
            await enter(c);
            const r = await c.query(sql, params as unknown[]);
            await c.query('COMMIT');
            return { rows: r.rows, rowCount: r.rowCount };
          } catch (e) {
            await c.query('ROLLBACK').catch(() => {});
            throw e;
          } finally {
            c.release();
          }
        },
        async transaction(fn) {
          const c = await pool.connect();
          try {
            await c.query('BEGIN');
            await enter(c);
            const inner: SqlExecutor = {
              async query(sql, params) {
                const r = await c.query(sql, params as unknown[]);
                return { rows: r.rows, rowCount: r.rowCount };
              },
              transaction(f) {
                return f(inner);
              },
            };
            const result = await fn(inner);
            await c.query('COMMIT');
            return result;
          } catch (e) {
            await c.query('ROLLBACK').catch(() => {});
            throw e;
          } finally {
            c.release();
          }
        },
      };
    };

    // Tenant A holds a concurrency lease and records an event, all under RLS.
    const gateA = createStepGate({ exec: scoped('A'), partitionKey: 'A', concurrency: { task: { maxConcurrent: 1 } } });
    expect((await gateA.acquire(req('A', 'task', 1))).admitted).toBe(true);

    const sinkA = createEventSink({ exec: scoped('A'), partitionKey: 'A' });
    sinkA.record(evt('A', 99));
    await sinkA.flush();

    // Tenant A sees its own lease + event through its scoped executor…
    const leasesA = await scoped('A').query<{ n: number }>(`SELECT count(*)::int AS n FROM flow_step_lease`);
    expect(leasesA.rows[0]!.n).toBe(1);
    const eventsA = await readFlowEvents(scoped('A'), { workflowId: 99, partitionKey: 'A' });
    expect(eventsA).toHaveLength(1);

    // …but tenant B's scoped executor is isolated by RLS — it sees neither.
    const leasesB = await scoped('B').query<{ n: number }>(`SELECT count(*)::int AS n FROM flow_step_lease`);
    expect(leasesB.rows[0]!.n).toBe(0);
    const eventsB = await readFlowEvents(scoped('B'), { workflowId: 99, partitionKey: 'A' });
    expect(eventsB).toHaveLength(0);
  });
});
