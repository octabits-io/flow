import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { PgBoss } from 'pg-boss';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import {
  createPgBossScheduler,
  createPgBossStartWorker,
  ensureStartQueue,
  type WireStartPayload,
} from './index';
import type { Logger } from '../core';

let container: StartedPostgreSqlContainer;
let boss: PgBoss;

beforeAll(async () => {
  container = await new PostgreSqlContainer('postgres:17-alpine').start();
  boss = new PgBoss({ connectionString: container.getConnectionUri() });
  await boss.start();
});

afterAll(async () => {
  await boss?.stop({ graceful: false });
  await container?.stop();
});

function deferred<T>() {
  let resolve!: (v: T) => void;
  const promise = new Promise<T>((r) => (resolve = r));
  return { promise, resolve };
}

function capturingLogger() {
  const errors: string[] = [];
  const logger: Logger = { info() {}, warn() {}, error(message) { errors.push(message); } };
  return { logger, errors };
}

async function waitFor(cond: () => boolean, timeoutMs = 8000) {
  const start = Date.now();
  while (!cond()) {
    if (Date.now() - start > timeoutMs) throw new Error('waitFor timed out');
    await new Promise((r) => setTimeout(r, 50));
  }
}

describe('pg-boss scheduler (integration)', () => {
  it('registers a cron schedule that carries the start payload, then removes it', async () => {
    const queueName = 'flow-starts-sched';
    const scheduler = createPgBossScheduler({ boss, queueName, partitionKey: 'tenant-9' });

    await scheduler.schedule({
      key: 'nightly',
      cron: '0 3 * * *',
      workflowType: 'enrichment',
      input: { full: true },
      tz: 'UTC',
    });

    const schedules = await scheduler.list();
    const mine = schedules.find((s: any) => s.key === 'nightly') as any;
    expect(mine).toBeTruthy();
    expect(mine.cron).toBe('0 3 * * *');
    expect(mine.data).toMatchObject({ partitionKey: 'tenant-9', workflowType: 'enrichment', input: { full: true } });

    await scheduler.unschedule('nightly');
    const after = await scheduler.list();
    expect(after.find((s: any) => s.key === 'nightly')).toBeFalsy();
  });

  it('drives a host start callback when a start job arrives', async () => {
    const queueName = 'flow-starts-worker';
    const got = deferred<WireStartPayload>();

    const worker = createPgBossStartWorker({ boss, queueName, workerOptions: { pollingIntervalSeconds: 1 } });
    await worker.start(async (payload) => {
      got.resolve(payload);
    });

    // simulate a cron tick: a start job lands on the queue (this is what schedule() does on fire)
    await ensureStartQueue(boss, queueName);
    await boss.send(queueName, {
      partitionKey: 'tenant-3',
      workflowType: 'daily-report',
      input: { date: '2026-06-29' },
    });

    const payload = await got.promise;
    expect(payload).toMatchObject({ partitionKey: 'tenant-3', workflowType: 'daily-report', input: { date: '2026-06-29' } });

    await worker.stop();
  });

  it('ensureStartQueue is idempotent (swallows "already exists")', async () => {
    await ensureStartQueue(boss, 'flow-starts-idem');
    await expect(ensureStartQueue(boss, 'flow-starts-idem')).resolves.toBeUndefined();
  });

  it('start worker logs + skips a schema-invalid start payload', async () => {
    const queueName = 'flow-starts-badjob';
    const { logger, errors } = capturingLogger();
    await ensureStartQueue(boss, queueName);

    const worker = createPgBossStartWorker({ boss, queueName, logger, workerOptions: { pollingIntervalSeconds: 1 } });
    await worker.start(async () => {
      throw new Error('process should not be reached for an invalid start payload');
    });

    // missing required fields (workflowType) → schema parse fails
    await boss.send(queueName, { partitionKey: 'tenant-1' } as unknown as WireStartPayload);

    await waitFor(() => errors.some((m) => m.includes('Invalid start payload')));
    await worker.stop();
  });
});
