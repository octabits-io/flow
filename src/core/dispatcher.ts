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
}
