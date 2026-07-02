import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { PgBoss } from 'pg-boss';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import {
  createPgBossDispatcher,
  createPgBossStepWorker,
  createPgBossDlqWorker,
  ensureStepQueue,
  type WireStepPayload,
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

describe('pg-boss dispatcher (integration)', () => {
  it('delivers an enqueued step to a worker with the partition key', async () => {
    const queueName = 'flow-step-deliver';
    const got = deferred<WireStepPayload>();

    const worker = createPgBossStepWorker({ boss, queueName, workerOptions: { pollingIntervalSeconds: 1 } });
    await worker.start(async (payload) => {
      got.resolve(payload);
    });

    const dispatcher = createPgBossDispatcher({ boss, queueName, partitionKey: 'tenant-7' });
    const res = await dispatcher.enqueueStep({ workflowId: 11, stepId: 22, stepKey: 'generate', stepType: 'ai:generate' });
    expect(res.ok).toBe(true);

    const payload = await got.promise;
    expect(payload).toEqual({ partitionKey: 'tenant-7', workflowId: 11, stepId: 22, stepKey: 'generate', stepType: 'ai:generate' });

    await worker.stop();
  });

  it('routes an exhausted job to the dead-letter worker', async () => {
    const queueName = 'flow-step-dlq';
    const dead = deferred<WireStepPayload>();

    // retryLimit 0 → a throwing handler dead-letters immediately
    const worker = createPgBossStepWorker({ boss, queueName, config: { retryLimit: 0 }, workerOptions: { pollingIntervalSeconds: 1 } });
    await worker.start(async () => {
      throw new Error('always fails');
    });

    const dlqWorker = createPgBossDlqWorker({ boss, queueName, pollingIntervalSeconds: 1 });
    await dlqWorker.start(async (payload) => {
      dead.resolve(payload);
    });

    const dispatcher = createPgBossDispatcher({ boss, queueName, partitionKey: 'tenant-9', config: { retryLimit: 0 } });
    const res = await dispatcher.enqueueStep({ workflowId: 1, stepId: 2, stepKey: 'x', stepType: 'ai:x' });
    expect(res.ok).toBe(true);

    const payload = await dead.promise;
    expect(payload).toMatchObject({ partitionKey: 'tenant-9', workflowId: 1, stepId: 2 });

    await worker.stop();
    await dlqWorker.stop();
  });

  it('rejects an invalid payload at enqueue without enqueuing', async () => {
    const dispatcher = createPgBossDispatcher({ boss, queueName: 'flow-step-invalid', partitionKey: 'tenant-x' });
    const res = await dispatcher.enqueueStep({ workflowId: -1, stepId: 0, stepKey: '', stepType: '' } as any);
    expect(res.ok).toBe(false);
    if (res.ok) return;
    expect(res.error.key).toBe('queue_error');
    expect(res.error.message).toMatch(/Invalid step payload/);
  });

  it('ensureStepQueue is idempotent (swallows "already exists")', async () => {
    await ensureStepQueue(boss, 'flow-step-idem');
    await expect(ensureStepQueue(boss, 'flow-step-idem')).resolves.toBeUndefined();
  });

  it('worker.stop() before start is a no-op', async () => {
    const worker = createPgBossStepWorker({ boss, queueName: 'flow-step-nostart' });
    await expect(worker.stop()).resolves.toBeUndefined();
  });

  it('surfaces a queue_error when the underlying boss call throws', async () => {
    // A stopped boss makes createQueue/send throw — the dispatcher must catch and surface it.
    const dead = new PgBoss({ connectionString: container.getConnectionUri() });
    await dead.start();
    await dead.stop({ graceful: false });

    const dispatcher = createPgBossDispatcher({ boss: dead, queueName: 'flow-step-deadboss', partitionKey: 'tenant-d' });
    const res = await dispatcher.enqueueStep({ workflowId: 1, stepId: 2, stepKey: 'k', stepType: 'ai:k' });
    expect(res.ok).toBe(false);
    if (res.ok) return;
    expect(res.error.key).toBe('queue_error');
  });

  it('dead-letters a schema-invalid job and the DLQ worker logs + skips it', async () => {
    const queueName = 'flow-step-badjob';
    await ensureStepQueue(boss, queueName, { retryLimit: 0 });
    const { logger, errors } = capturingLogger();

    // retryLimit 0 → the worker's schema-parse throw dead-letters the job immediately
    const worker = createPgBossStepWorker({ boss, queueName, config: { retryLimit: 0 }, workerOptions: { pollingIntervalSeconds: 1 } });
    await worker.start(async () => {
      throw new Error('processor should not be reached for an invalid payload');
    });

    const dlqWorker = createPgBossDlqWorker({ boss, queueName, logger, pollingIntervalSeconds: 1 });
    await dlqWorker.start(async () => {
      throw new Error('onDlq should not be reached for an invalid payload');
    });

    // a raw, schema-invalid job lands directly on the queue
    await boss.send(queueName, { garbage: true } as unknown as WireStepPayload);

    await waitFor(() => errors.some((m) => m.includes('Invalid payload in DLQ')));
    await worker.stop();
    await dlqWorker.stop();
  });

  it('logs when the DLQ handler itself throws', async () => {
    const queueName = 'flow-step-dlqthrow';
    const { logger, errors } = capturingLogger();

    const worker = createPgBossStepWorker({ boss, queueName, config: { retryLimit: 0 }, workerOptions: { pollingIntervalSeconds: 1 } });
    await worker.start(async () => {
      throw new Error('always fails');
    });

    const dlqWorker = createPgBossDlqWorker({ boss, queueName, logger, pollingIntervalSeconds: 1 });
    await dlqWorker.start(async () => {
      throw new Error('dlq handler boom');
    });

    const dispatcher = createPgBossDispatcher({ boss, queueName, partitionKey: 'tenant-z', config: { retryLimit: 0 } });
    const res = await dispatcher.enqueueStep({ workflowId: 5, stepId: 6, stepKey: 'y', stepType: 'ai:y' });
    expect(res.ok).toBe(true);

    await waitFor(() => errors.some((m) => m.includes('DLQ handler failed')));
    await worker.stop();
    await dlqWorker.stop();
  });
});
