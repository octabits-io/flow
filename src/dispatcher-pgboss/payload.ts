import { z } from 'zod';

/**
 * The wire payload for a step job. It is the flow-core `DispatchStepPayload`
 * plus the `partitionKey` — the worker needs the partition to reconstruct a
 * partition-scoped engine before handing the job to `engine.handleStepJob`.
 */
export const WIRE_STEP_PAYLOAD_SCHEMA = z.object({
  partitionKey: z.string().min(1),
  workflowId: z.number().int().positive(),
  stepId: z.number().int().positive(),
  stepKey: z.string().min(1),
  stepType: z.string().min(1),
  /**
   * What the job asks for: run the step, or settle it because its wait deadline
   * elapsed. Defaulted rather than required, so a job enqueued before wait
   * deadlines existed still parses — and still means "run the step".
   */
  kind: z.enum(['execute', 'timeout']).default('execute'),
});

export type WireStepPayload = z.infer<typeof WIRE_STEP_PAYLOAD_SCHEMA>;

export interface StepQueueConfig {
  /** Retries before a job is dead-lettered. Default 2. */
  retryLimit?: number;
  /** Seconds between retries. Default 30. */
  retryDelay?: number;
  /** Seconds before an in-flight job is considered expired. Default 600. */
  expireInSeconds?: number;
}

export const DEFAULT_STEP_QUEUE_CONFIG: Required<StepQueueConfig> = {
  retryLimit: 2,
  retryDelay: 30,
  expireInSeconds: 600,
};

/**
 * The wire payload for a scheduled (or ad-hoc) workflow **start**. Carries the
 * partition plus what `engine.startWorkflow` needs; the host resolves `workflowType`
 * to a definition.
 *
 * Two mutually exclusive ways to ask for start idempotency, because a cron schedule
 * stores its payload **once** and pg-boss redelivers that same payload on every tick:
 *
 * - `idempotencyKeyPrefix` — the usual choice. The start worker appends the job id,
 *   so each tick gets a distinct key while a redelivery of the *same* tick reuses it.
 * - `idempotencyKey` — used verbatim. On a schedule this means "start exactly one
 *   workflow, ever"; every later tick returns the first one.
 */
export const WIRE_START_PAYLOAD_SCHEMA = z.object({
  partitionKey: z.string().min(1),
  workflowType: z.string().min(1),
  input: z.record(z.string(), z.unknown()).default({}),
  entityRef: z.string().optional(),
  /** Verbatim dedup key — collapses *every* delivery, including future cron ticks. */
  idempotencyKey: z.string().optional(),
  /** Per-tick dedup: the start worker resolves this to `${prefix}:${jobId}`. */
  idempotencyKeyPrefix: z.string().optional(),
});

export type WireStartPayload = z.infer<typeof WIRE_START_PAYLOAD_SCHEMA>;

/**
 * What a {@link WireStartPayload} becomes at delivery time: the same fields, plus the
 * pg-boss job id and the **resolved** `idempotencyKey` to hand `engine.startWorkflow`.
 *
 * `idempotencyKey` is verbatim when the payload set one, `${idempotencyKeyPrefix}:${jobId}`
 * when it set a prefix, and `undefined` when it set neither.
 */
export interface StartJobContext extends WireStartPayload {
  /** pg-boss job id — unique per cron tick, stable across that job's retries. */
  jobId: string;
}

/** Resolve the per-delivery idempotency key. Explicit key wins over a prefix. */
export function resolveStartIdempotencyKey(payload: WireStartPayload, jobId: string): string | undefined {
  if (payload.idempotencyKey !== undefined) return payload.idempotencyKey;
  if (payload.idempotencyKeyPrefix !== undefined) return `${payload.idempotencyKeyPrefix}:${jobId}`;
  return undefined;
}
