import { z } from 'zod';

/**
 * The wire payload for a step job. It is the flow-core `DispatchStepPayload`
 * plus the `partitionKey` — the worker needs the partition to reconstruct a
 * partition-scoped engine before calling `executeStep`.
 */
export const WIRE_STEP_PAYLOAD_SCHEMA = z.object({
  partitionKey: z.string().min(1),
  workflowId: z.number().int().positive(),
  stepId: z.number().int().positive(),
  stepKey: z.string().min(1),
  stepType: z.string().min(1),
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
 * to a definition. `idempotencyKey` is forwarded to the start so overlapping
 * cron ticks don't double-start.
 */
export const WIRE_START_PAYLOAD_SCHEMA = z.object({
  partitionKey: z.string().min(1),
  workflowType: z.string().min(1),
  input: z.record(z.string(), z.unknown()).default({}),
  entityRef: z.string().optional(),
  idempotencyKey: z.string().optional(),
});

export type WireStartPayload = z.infer<typeof WIRE_START_PAYLOAD_SCHEMA>;
