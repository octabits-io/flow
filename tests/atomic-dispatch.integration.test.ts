/**
 * Cross-adapter integration: store-pg's transaction carrying dispatcher-pgboss's
 * enqueue, so a job and the state change that produced it commit together.
 *
 * Lives outside `src/` on purpose. The boundary lint forbids the two adapters
 * from importing each other — rightly, since neither should depend on the other
 * at runtime. This test exercises their *composition*, which is the consumer's
 * job, so it sits where a consumer would: outside the layer system.
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Pool } from 'pg';
import { PgBoss } from 'pg-boss';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { createPgWorkflowStore, applySchema, FLOW_STORE_DDL } from '../src/store-pg/index.ts';
import { createPgBossDispatcher, ensureStepQueue } from '../src/dispatcher-pgboss/index.ts';

let container: StartedPostgreSqlContainer;
let pool: Pool;
let boss: PgBoss;

beforeAll(async () => {
  container = await new PostgreSqlContainer('postgres:17-alpine').start();
  pool = new Pool({ connectionString: container.getConnectionUri() });
  await applySchema(pool, FLOW_STORE_DDL);
  boss = new PgBoss({ connectionString: container.getConnectionUri() });
  await boss.start();
});

afterAll(async () => {
  await boss?.stop({ graceful: false });
  await pool?.end();
  await container?.stop();
});

describe('transactional enqueue', () => {
  it('commits the job with the transaction and loses it on rollback', async () => {
    const queueName = 'flow-step-tx';
    await ensureStepQueue(boss, queueName);

    const store = createPgWorkflowStore({ pool, partitionKey: 'tx' });
    const dispatcher = createPgBossDispatcher({ boss, queueName, partitionKey: 'tx' });
    const payload = { workflowId: 1, stepId: 1, stepKey: 'k', stepType: 't' };

    const queued = async () => {
      const { rows } = await pool.query<{ n: number }>(
        `SELECT count(*)::int AS n FROM pgboss.job WHERE name = $1`,
        [queueName],
      );
      return rows[0]!.n;
    };
    const before = await queued();

    // Rolled back: the enqueue rode the transaction, so the job goes with it.
    await expect(
      store.runInTransaction!(async ({ handle }) => {
        const res = await dispatcher.enqueueStepIn!(handle, payload);
        expect(res.ok).toBe(true);
        throw new Error('rollback');
      }),
    ).rejects.toThrow('rollback');
    expect(await queued()).toBe(before);

    // Committed: the same call without the throw.
    await store.runInTransaction!(async ({ handle }) => {
      const res = await dispatcher.enqueueStepIn!(handle, { ...payload, stepId: 2 });
      expect(res.ok).toBe(true);
    });
    expect(await queued()).toBe(before + 1);
  });
});
