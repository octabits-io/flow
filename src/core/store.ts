import type {
  WorkflowId,
  StepId,
  WorkflowStatus,
  WorkflowRecord,
  StepRecord,
  WorkflowWithSteps,
} from './types';

// ============================================================================
// Store parameter shapes
// ============================================================================

export interface CreateWorkflowStep {
  key: string;
  type: string;
  dependencies: string[];
  input: Record<string, unknown> | null;
}

/** A map child step to insert at runtime. */
export interface AddChildStep {
  /** Synthetic key, e.g. `${parentKey}#${index}`. */
  key: string;
  type: string;
  input: Record<string, unknown> | null;
}

export interface CreateWorkflowParams {
  type: string;
  input: Record<string, unknown>;
  entityRef?: string;
  metadata?: Record<string, unknown>;
  /** Dedup key (per partition); a collision returns the existing workflow. */
  idempotencyKey?: string;
  /** Sub-workflow linkage: the parent workflow + step that started this child. */
  parentWorkflowId?: WorkflowId;
  parentStepId?: StepId;
  startedAt: string;
  /** Absolute deadline for the run (from `StartOptions.timeoutMs`), if any. */
  deadlineAt?: string;
  steps: CreateWorkflowStep[];
}

export interface CreatedWorkflow {
  workflowId: WorkflowId;
  /** The step rows (new on insert, existing on an idempotency hit) — used to enqueue roots. */
  steps: StepRecord[];
  /**
   * True when `idempotencyKey` matched an existing workflow and nothing new was created.
   * The engine then returns the existing workflow without re-enqueuing roots.
   */
  alreadyExisted: boolean;
}

export interface CompleteStepParams {
  workflowId: WorkflowId;
  stepId: StepId;
  output: Record<string, unknown>;
  completedAt: string;
}

export interface FailStepParams {
  workflowId: WorkflowId;
  stepId: StepId;
  error: string;
  completedAt: string;
}

export interface FinishWorkflowParams {
  workflowId: WorkflowId;
  status: Extract<WorkflowStatus, 'completed' | 'failed' | 'cancelled'>;
  output?: Record<string, unknown>;
  error?: string;
  completedAt: string;
}

/**
 * Put a `failed` workflow back into flight after its steps have been reset.
 * Counters are recomputed by the engine and passed in whole rather than
 * adjusted, because a retry may also have dropped rows (a map parent's
 * children) — an increment/decrement contract could not express that.
 */
export interface ReopenWorkflowParams {
  workflowId: WorkflowId;
  totalSteps: number;
  completedSteps: number;
  failedSteps: number;
}

export interface ListWorkflowsFilters {
  status?: WorkflowStatus;
  type?: string;
  entityRef?: string;
  limit?: number;
}

// ============================================================================
// WorkflowStore
// ============================================================================

/**
 * A store bound to an open transaction, plus an opaque handle a capable
 * {@link Dispatcher} can join so a job and the state change that produced it
 * commit together. Core never inspects `handle` — it belongs to the adapter
 * pair (e.g. store-pg hands dispatcher-pgboss its transaction-bound executor).
 */
export interface TransactionalScope {
  /** A store whose writes go through the open transaction. */
  store: WorkflowStore;
  /** Opaque; pass to `Dispatcher.enqueueStepIn`. */
  handle: unknown;
}

/**
 * Persistence boundary for the engine. Implementations are **partition-scoped at
 * construction** (e.g. bound to one tenant) — exactly like the engine — so methods
 * never take a partition key. The default adapter is Postgres/Drizzle.
 *
 * Counter ownership: `completeStep` MUST atomically increment the workflow's
 * `completedSteps`; `failStep` MUST atomically increment `failedSteps`. The engine
 * relies on these counters only for progress reporting — readiness and termination
 * are computed from `listSteps`.
 */
export interface WorkflowStore {
  /**
   * **Optional capability.** Run `fn` inside one store transaction.
   *
   * When the dispatcher also implements `enqueueStepIn`, the engine uses both so
   * that a state change and the dispatches it unlocks commit atomically —
   * closing the window where a crash leaves a step `pending` with no job behind
   * it. A store that cannot offer this simply omits it, and the engine falls
   * back to writing then enqueueing.
   *
   * Implementations MUST run every operation on `scope.store` inside the same
   * transaction, and MUST roll back if `fn` throws.
   */
  runInTransaction?<T>(fn: (scope: TransactionalScope) => Promise<T>): Promise<T>;

  /** Create the workflow + all step rows atomically (transactional). */
  createWorkflow(params: CreateWorkflowParams): Promise<CreatedWorkflow>;

  getWorkflow(workflowId: WorkflowId): Promise<WorkflowRecord | null>;
  getStep(stepId: StepId): Promise<StepRecord | null>;
  listSteps(workflowId: WorkflowId): Promise<StepRecord[]>;

  /**
   * **Atomically claim** a step: flip it to `running`, set `startedAt` and increment
   * `attempts` — but **only if it is still `pending`**. Returns `true` if this caller
   * won the claim, `false` if the step had already left `pending` (another worker
   * claimed it, or it was cancelled/skipped meanwhile).
   *
   * Implementations MUST make the status check and the write a single atomic
   * operation (e.g. `UPDATE … WHERE id = $1 AND status = 'pending'` and report
   * whether a row matched). A read-then-write would let two workers handed the
   * same job by an at-least-once dispatcher both run the handler.
   */
  markStepRunning(stepId: StepId, startedAt: string): Promise<boolean>;
  /** Reset a step to `pending` for a scheduled retry (leaves `attempts` as-is). */
  markStepPending(stepId: StepId): Promise<void>;
  /**
   * Flip a ready step to `waiting` — it suspends until `resumeStep` (waitForEvent)
   * or a sub-workflow child settles. `waitingAt` is stamped on `startedAt`: it is
   * when the suspension began, which is what a wait deadline is measured from
   * (and what a UI shows as "waiting since").
   */
  markStepWaiting(stepId: StepId, waitingAt: string): Promise<void>;
  /** Flip a map parent to `mapping` — it suspends until all spawned children finish. */
  markStepMapping(stepId: StepId): Promise<void>;
  /** Flip a completed step to `compensating` while its rollback handler runs. */
  markStepCompensating(stepId: StepId): Promise<void>;
  /** Flip a step to `compensated`; optionally record a compensation error for surfacing. */
  markStepCompensated(stepId: StepId, error?: string): Promise<void>;

  /** Insert map child step rows at runtime and return them with ids. */
  addChildSteps(workflowId: WorkflowId, parentStepId: StepId, children: AddChildStep[]): Promise<StepRecord[]>;
  /** List the child steps of a map parent, ordered by creation (item order). */
  listChildSteps(parentStepId: StepId): Promise<StepRecord[]>;
  /**
   * Delete a map parent's child rows and report how many went. Used by
   * `retryWorkflow`: a map parent that is re-run fans out afresh, so the
   * previous attempt's children must not linger and be counted twice.
   */
  deleteChildSteps(parentStepId: StepId): Promise<number>;
  /** Flip a step to `completed`, persist output, and increment `completedSteps`. */
  completeStep(params: CompleteStepParams): Promise<void>;
  /** Flip a step to `failed`, persist error, and increment `failedSteps`. */
  failStep(params: FailStepParams): Promise<void>;
  /** Flip a single pending/ready step to `skipped`. */
  skipStep(stepId: StepId, reason: string): Promise<void>;
  /** Flip every still-pending or `waiting` step of a workflow to `skipped` (used on cancel). */
  skipPendingSteps(workflowId: WorkflowId, reason: string): Promise<void>;

  /**
   * Put a settled step back to square one so it can run again: `pending`, no
   * output, no error, no timestamps, and `attempts` back to 0 — a **fresh retry
   * budget**, which is the point of an operator-driven retry.
   *
   * Unlike `markStepPending` (an in-flight retry, which keeps the attempt count
   * so the budget still runs out), this is only used by `retryWorkflow`.
   */
  resetStep(stepId: StepId): Promise<void>;

  /** Transition the workflow to a terminal state. */
  finishWorkflow(params: FinishWorkflowParams): Promise<void>;

  /**
   * Move a `failed` workflow back to `running` with recomputed counters,
   * clearing its `output`, `error` and `completedAt`. The engine calls this
   * after resetting the steps a retry should re-run.
   */
  reopenWorkflow(params: ReopenWorkflowParams): Promise<void>;

  /** List workflows (with their steps) for status/dashboard reads. */
  listWorkflows(filters: ListWorkflowsFilters): Promise<WorkflowWithSteps[]>;

  // --- crash recovery ---
  /** All workflows currently in `running` state (for the stuck-step sweeper). */
  listRunningWorkflows(): Promise<WorkflowRecord[]>;
  /** Steps stuck in `running` with `startedAt` older than `cutoff` (ISO string). */
  findStuckSteps(workflowId: WorkflowId, cutoff: string): Promise<StepRecord[]>;
}
