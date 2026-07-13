import type { PgBoss, Job, WorkOptions } from 'pg-boss';
import type { Logger } from '../core';
import { WIRE_START_PAYLOAD_SCHEMA, type WireStartPayload } from './payload';

// ============================================================================
// Cron / scheduled workflow starts
// ============================================================================
//
// pg-boss owns the cron: `schedule(queue, cron, data, { key })` enqueues `data` onto
// `queue` on the cron (durable — survives restarts, keyed by `key` so one queue can
// carry many schedules). A start worker reads that queue and calls a host-provided
// `start` callback (which resolves the workflow type to a definition and calls
// `engine.startWorkflow`). flow-core stays generic — only the host knows definitions.

/** Create the start queue (idempotent). */
export async function ensureStartQueue(boss: PgBoss, queueName: string): Promise<void> {
  try {
    await boss.createQueue(queueName);
  } catch (e) {
    if (e instanceof Error && e.message.includes('already exists')) return;
    throw e;
  }
}

export interface PgBossSchedulerDeps {
  boss: PgBoss;
  /** Queue scheduled start jobs are sent to (and the start worker reads). */
  queueName: string;
  /** Partition stamped into every scheduled start payload. */
  partitionKey: string;
  logger?: Logger;
}

export interface ScheduleStartInput {
  /** Unique key for this schedule within the queue (allows many crons per queue). */
  key: string;
  /** Cron expression (5- or 6-field). */
  cron: string;
  /** Workflow type to start (the host resolves it to a definition). */
  workflowType: string;
  input?: Record<string, unknown>;
  entityRef?: string;
  /** Forwarded to the start so overlapping ticks don't double-start. */
  idempotencyKey?: string;
  /** Optional IANA time zone (default UTC). */
  tz?: string;
}

/** Registers cron schedules that enqueue workflow-start jobs. */
export function createPgBossScheduler(deps: PgBossSchedulerDeps) {
  const { boss, queueName, partitionKey } = deps;
  let ensured = false;

  async function ensure(): Promise<void> {
    if (!ensured) {
      await ensureStartQueue(boss, queueName);
      ensured = true;
    }
  }

  return {
    /** Create or replace a cron schedule that starts a workflow. */
    async schedule(input: ScheduleStartInput): Promise<void> {
      await ensure();
      const payload: WireStartPayload = {
        partitionKey,
        workflowType: input.workflowType,
        input: input.input ?? {},
        entityRef: input.entityRef,
        idempotencyKey: input.idempotencyKey,
      };
      await boss.schedule(queueName, input.cron, payload, {
        key: input.key,
        ...(input.tz ? { tz: input.tz } : {}),
      });
    },

    /** Remove a schedule by key. */
    async unschedule(key: string): Promise<void> {
      await boss.unschedule(queueName, key);
    },

    /** List the schedules registered on this queue. */
    async list() {
      // Filter in JS: getSchedules(name) is unreliable across pg-boss versions.
      const all = await boss.getSchedules();
      return all.filter((s) => s.name === queueName);
    },
  };
}

// ============================================================================
// Start worker
// ============================================================================

/** Invoked for each start job — typically resolves the type and calls `engine.startWorkflow`. */
export type StartJobProcessor = (payload: WireStartPayload) => Promise<void>;

export interface PgBossStartWorkerDeps {
  boss: PgBoss;
  queueName: string;
  logger?: Logger;
  workerOptions?: { batchSize?: number; pollingIntervalSeconds?: number };
}

/** A pg-boss worker on the start queue that drives a host start callback. */
export function createPgBossStartWorker(deps: PgBossStartWorkerDeps) {
  const { boss, queueName, logger } = deps;
  let workerId: string | null = null;

  async function start(process: StartJobProcessor): Promise<void> {
    await ensureStartQueue(boss, queueName);
    const workOpts: WorkOptions = {
      batchSize: deps.workerOptions?.batchSize ?? 1,
      ...(deps.workerOptions?.pollingIntervalSeconds != null && { pollingIntervalSeconds: deps.workerOptions.pollingIntervalSeconds }),
    };
    workerId = await boss.work<WireStartPayload>(queueName, workOpts, async (jobs: Job<WireStartPayload>[]) => {
      for (const job of jobs) {
        const parsed = WIRE_START_PAYLOAD_SCHEMA.safeParse(job.data);
        if (!parsed.success) {
          logger?.error('Invalid start payload', undefined, { jobId: job.id });
          continue;
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
