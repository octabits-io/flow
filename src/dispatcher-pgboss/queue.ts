import type { PgBoss, Job, WorkOptions } from 'pg-boss';
import type { Dispatcher, DispatchStepPayload, EnqueueOptions, Result, FlowErrorShape, Logger } from '../core';
import { WIRE_STEP_PAYLOAD_SCHEMA, DEFAULT_STEP_QUEUE_CONFIG, type WireStepPayload, type StepQueueConfig } from './payload';

function dlqName(queueName: string): string {
  return `${queueName}-dlq`;
}

/**
 * Create the step queue and its dead-letter queue (idempotent). Call once at
 * startup; the dispatcher also calls it lazily on first enqueue.
 */
export async function ensureStepQueue(boss: PgBoss, queueName: string, config: StepQueueConfig = {}): Promise<void> {
  const cfg = { ...DEFAULT_STEP_QUEUE_CONFIG, ...config };
  const dlq = dlqName(queueName);

  const swallowExists = (error: unknown) => {
    if (error instanceof Error && error.message.includes('already exists')) return;
    throw error;
  };

  try {
    await boss.createQueue(dlq, { retryLimit: 0 });
  } catch (e) {
    swallowExists(e);
  }
  try {
    await boss.createQueue(queueName, {
      retryLimit: cfg.retryLimit,
      retryDelay: cfg.retryDelay,
      expireInSeconds: cfg.expireInSeconds,
      deadLetter: dlq,
    });
  } catch (e) {
    swallowExists(e);
  }
}

// ============================================================================
// Dispatcher
// ============================================================================

export interface PgBossDispatcherDeps {
  boss: PgBoss;
  queueName: string;
  /** Partition this dispatcher is bound to — stamped into every enqueued job. */
  partitionKey: string;
  config?: StepQueueConfig;
}

/** A flow-core `Dispatcher` backed by pg-boss. */
export function createPgBossDispatcher(deps: PgBossDispatcherDeps): Dispatcher {
  const { boss, queueName, partitionKey } = deps;
  const cfg = { ...DEFAULT_STEP_QUEUE_CONFIG, ...deps.config };
  let ensured = false;

  return {
    async enqueueStep(payload: DispatchStepPayload, options?: EnqueueOptions): Promise<Result<void, FlowErrorShape>> {
      try {
        const wire: WireStepPayload = { partitionKey, ...payload };
        const parsed = WIRE_STEP_PAYLOAD_SCHEMA.safeParse(wire);
        if (!parsed.success) {
          return { ok: false, error: { key: 'queue_error', message: `Invalid step payload: ${parsed.error.message}` } };
        }
        if (!ensured) {
          await ensureStepQueue(boss, queueName, cfg);
          ensured = true;
        }
        const startAfter = options?.startAfterSeconds;
        const jobId = await boss.send(queueName, parsed.data, {
          retryLimit: cfg.retryLimit,
          retryDelay: cfg.retryDelay,
          expireInSeconds: cfg.expireInSeconds,
          ...(startAfter != null && startAfter > 0 ? { startAfter } : {}),
        });
        if (!jobId) {
          return { ok: false, error: { key: 'queue_error', message: `Failed to enqueue job to ${queueName}` } };
        }
        return { ok: true, value: undefined };
      } catch (error) {
        return { ok: false, error: { key: 'queue_error', message: error instanceof Error ? error.message : 'Unknown enqueue error' } };
      }
    },
  };
}

// ============================================================================
// Worker
// ============================================================================

export interface PgBossStepWorkerDeps {
  boss: PgBoss;
  queueName: string;
  config?: StepQueueConfig;
  logger?: Logger;
  workerOptions?: { batchSize?: number; pollingIntervalSeconds?: number };
}

/** Handles a single step job. Reconstruct the partition-scoped engine and call
 * `engine.executeStep(workflowId, stepId)`. Throw to trigger a pg-boss retry. */
export type StepJobProcessor = (payload: WireStepPayload) => Promise<void>;

/** A pg-boss worker that drives `executeStep` for step jobs. */
export function createPgBossStepWorker(deps: PgBossStepWorkerDeps) {
  const { boss, queueName } = deps;
  let workerId: string | null = null;

  async function start(process: StepJobProcessor): Promise<void> {
    await ensureStepQueue(boss, queueName, deps.config);
    const workOpts: WorkOptions = {
      batchSize: deps.workerOptions?.batchSize ?? 1,
      ...(deps.workerOptions?.pollingIntervalSeconds != null && { pollingIntervalSeconds: deps.workerOptions.pollingIntervalSeconds }),
    };
    workerId = await boss.work<WireStepPayload>(queueName, workOpts, async (jobs: Job<WireStepPayload>[]) => {
      for (const job of jobs) {
        const parsed = WIRE_STEP_PAYLOAD_SCHEMA.safeParse(job.data);
        if (!parsed.success) {
          // Invalid payload — don't retry, send straight to DLQ
          throw new Error(`Invalid step payload for job ${job.id}: ${parsed.error.message}`);
        }
        await process(parsed.data);
      }
    });
  }

  async function stop(): Promise<void> {
    if (workerId) {
      await boss.offWork(workerId);
      workerId = null;
    }
  }

  return { start, stop };
}

// ============================================================================
// Dead-letter worker
// ============================================================================

export interface PgBossDlqWorkerDeps {
  boss: PgBoss;
  queueName: string;
  logger?: Logger;
  pollingIntervalSeconds?: number;
}

/** Invoked for a step job that exhausted all retries. Typically calls
 * `engine.handleStepExhausted(workflowId, stepId, reason)`. */
export type DlqProcessor = (payload: WireStepPayload) => Promise<void>;

/** A pg-boss worker on the step queue's dead-letter queue. */
export function createPgBossDlqWorker(deps: PgBossDlqWorkerDeps) {
  const { boss, queueName, logger } = deps;
  const dlq = dlqName(queueName);
  let workerId: string | null = null;

  async function start(onDlq: DlqProcessor): Promise<void> {
    workerId = await boss.work<WireStepPayload>(
      dlq,
      { pollingIntervalSeconds: deps.pollingIntervalSeconds ?? 30 },
      async (jobs: Job<WireStepPayload>[]) => {
        for (const job of jobs) {
          const parsed = WIRE_STEP_PAYLOAD_SCHEMA.safeParse(job.data);
          if (!parsed.success) {
            logger?.error('Invalid payload in DLQ', undefined, { jobId: job.id });
            continue;
          }
          logger?.error('Step job dead-lettered', undefined, {
            jobId: job.id,
            workflowId: parsed.data.workflowId,
            stepId: parsed.data.stepId,
            stepKey: parsed.data.stepKey,
          });
          try {
            await onDlq(parsed.data);
          } catch (error) {
            logger?.error('DLQ handler failed', error instanceof Error ? error : undefined, { jobId: job.id });
          }
        }
      },
    );
  }

  async function stop(): Promise<void> {
    if (workerId) {
      await boss.offWork(workerId);
      workerId = null;
    }
  }

  return { start, stop };
}
