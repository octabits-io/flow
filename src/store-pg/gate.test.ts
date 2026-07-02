import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Pool } from 'pg';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { createPgStepGate, applySchema, FLOW_GATE_DDL } from './index';
import type { StepGateRequest } from '../core';

let container: StartedPostgreSqlContainer;
let pool: Pool;

beforeAll(async () => {
  container = await new PostgreSqlContainer('postgres:17-alpine').start();
  pool = new Pool({ connectionString: container.getConnectionUri() });
  await applySchema(pool, FLOW_GATE_DDL);
});

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

describe('createPgStepGate (integration)', () => {
  it('enforces a global token-bucket rate limit and refills over time', async () => {
    const gate = createPgStepGate({ pool, partitionKey: 'rl', rateLimit: { llm: { perSecond: 2, burst: 2 } } });

    expect((await gate.acquire(req('rl', 'llm', 1))).admitted).toBe(true);
    expect((await gate.acquire(req('rl', 'llm', 2))).admitted).toBe(true);
    const denied = await gate.acquire(req('rl', 'llm', 3));
    expect(denied.admitted).toBe(false);
    if (!denied.admitted) expect(denied.retryAfterSeconds).toBeGreaterThanOrEqual(1);

    // simulate elapsed time → bucket refills to burst
    await pool.query(`UPDATE flow_rate_bucket SET updated_at = now() - interval '10 seconds' WHERE partition_key = 'rl' AND step_type = 'llm'`);
    expect((await gate.acquire(req('rl', 'llm', 4))).admitted).toBe(true);
  });

  it('enforces a global concurrency cap via leases and frees on release', async () => {
    const gate = createPgStepGate({ pool, partitionKey: 'cc', concurrency: { task: { maxConcurrent: 2 } }, concurrencyRetrySeconds: 3 });

    const a = await gate.acquire(req('cc', 'task', 1));
    const b = await gate.acquire(req('cc', 'task', 2));
    const c = await gate.acquire(req('cc', 'task', 3));
    expect(a.admitted && b.admitted).toBe(true);
    expect(c.admitted).toBe(false);
    if (!c.admitted) expect(c.retryAfterSeconds).toBe(3);

    // releasing one lease lets the next step in
    if (a.admitted) await a.release();
    expect((await gate.acquire(req('cc', 'task', 3))).admitted).toBe(true);
  });

  it('reclaims an expired lease (crash-safety)', async () => {
    const gate = createPgStepGate({ pool, partitionKey: 'exp', concurrency: { job: { maxConcurrent: 1 } } });

    expect((await gate.acquire(req('exp', 'job', 1))).admitted).toBe(true);
    expect((await gate.acquire(req('exp', 'job', 2))).admitted).toBe(false); // cap reached

    // the holder "crashed" without releasing → its lease expires
    await pool.query(`UPDATE flow_step_lease SET expires_at = now() - interval '1 second' WHERE partition_key = 'exp' AND step_type = 'job'`);
    expect((await gate.acquire(req('exp', 'job', 2))).admitted).toBe(true); // expired lease reclaimed
  });

  it('does not consume a concurrency slot when the rate limit denies', async () => {
    const gate = createPgStepGate({
      pool,
      partitionKey: 'mix',
      concurrency: { m: { maxConcurrent: 1 } },
      rateLimit: { m: { perSecond: 1, burst: 1 } },
    });

    const first = await gate.acquire(req('mix', 'm', 1));
    expect(first.admitted).toBe(true);
    if (first.admitted) await first.release();

    // token bucket now empty → denied; the slot must have been freed (rate denial, not cap)
    const second = await gate.acquire(req('mix', 'm', 2));
    expect(second.admitted).toBe(false);

    // no lease should be lingering for step 1 or held by the denied step 2
    const leases = await pool.query(`SELECT count(*)::int AS n FROM flow_step_lease WHERE partition_key = 'mix' AND step_type = 'm'`);
    expect(leases.rows[0].n).toBe(0);

    // refill the token → slot is free → admitted
    await pool.query(`UPDATE flow_rate_bucket SET updated_at = now() - interval '10 seconds' WHERE partition_key = 'mix' AND step_type = 'm'`);
    expect((await gate.acquire(req('mix', 'm', 2))).admitted).toBe(true);
  });
});
