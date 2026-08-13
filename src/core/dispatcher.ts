import type { Result, FlowErrorShape } from './result';
import type { WorkflowId, StepId } from './types';

/** Payload handed to the dispatcher to schedule a single step for execution. */
export interface DispatchStepPayload {
  workflowId: WorkflowId;
  stepId: StepId;
  stepKey: string;
  stepType: string;
}

export interface EnqueueOptions {
  /**
   * Delay before the step becomes eligible to run, in seconds. Durable — survives
   * restarts. Used for retry backoff (and, later, durable sleep). Default 0.
   */
  startAfterSeconds?: number;
}

/**
 * Schedules step execution. The default adapter is a pg-boss queue, but any
 * durable-job mechanism works: the only contract is "eventually call
 * `engine.executeStep(workflowId, stepId)` for this payload, with retries".
 */
export interface Dispatcher {
  enqueueStep(payload: DispatchStepPayload, options?: EnqueueOptions): Promise<Result<void, FlowErrorShape>>;

  /**
   * **Optional capability.** Enqueue inside the caller's store transaction, so
   * the job and the state change that produced it commit together.
   *
   * `handle` comes from `WorkflowStore.runInTransaction` and is opaque to the
   * engine — the store and dispatcher agree on its meaning. Implement this only
   * when the queue lives in the same database as the store (pg-boss on the same
   * Postgres does; SQS or a separate Redis cannot). Without it the engine writes
   * state and then enqueues, which is at-least-once with a crash window.
   */
  enqueueStepIn?(
    handle: unknown,
    payload: DispatchStepPayload,
    options?: EnqueueOptions,
  ): Promise<Result<void, FlowErrorShape>>;
}
