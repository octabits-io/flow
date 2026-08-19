import type { Result, FlowErrorShape } from './result';
import type { JoinRule } from './readiness';

// ============================================================================
// Identifiers & status
// ============================================================================

/** Workflow / step identifiers. Numeric to match common bigserial-backed stores. */
export type WorkflowId = number;
export type StepId = number;

export type WorkflowStatus = 'pending' | 'running' | 'completed' | 'failed' | 'cancelled';
export type StepStatus =
  | 'pending'
  | 'running'
  | 'completed'
  | 'failed'
  | 'skipped'
  | 'waiting'
  | 'mapping'
  | 'compensating'
  | 'compensated';

// ============================================================================
// Workflow definition (the DAG)
// ============================================================================

/**
 * Declarative workflow definition: a DAG of steps with dependencies.
 * Steps without dependencies start immediately and in parallel; a step starts
 * as soon as ALL of its dependencies have completed.
 */
export interface WorkflowDefinition {
  /** Workflow type identifier (e.g. 'document-enrichment'). */
  type: string;
  /** The steps. Order is irrelevant — execution order is derived from dependencies. */
  steps: StepDefinition[];
}

export interface StepDefinition {
  /** Unique key within the workflow (e.g. 'analyze-images'). */
  key: string;
  /** Handler type — looked up in the StepHandlerRegistry. */
  type: string;
  /** Keys of steps this step depends on. All must complete before this step runs. */
  dependencies?: string[];
  /** Optional static input baked into the step at definition time. */
  input?: Record<string, unknown>;
}

export interface StartOptions {
  /** Optional reference for efficient filtering/lookup (e.g. 'document:123'). */
  entityRef?: string;
  /**
   * Opaque metadata persisted on the workflow row. Add-ons use this as an escape
   * hatch (e.g. the AI add-on stamps `keySource`) without flow-core knowing the shape.
   */
  metadata?: Record<string, unknown>;
  /**
   * Dedup key (per partition). A start with a key that already exists returns the
   * existing workflow instead of creating a duplicate — so a double-click, retried
   * request, or overlapping cron tick can't start the same work twice.
   */
  idempotencyKey?: string;
  /**
   * Wall-clock budget for the whole run, in ms. Stored as an absolute deadline;
   * once it passes, the workflow fails (and compensates) instead of running on —
   * whether it was executing, queued, or suspended on an event. Enforced when a
   * step is picked up and by `recoverStuckWorkflows`, so the sweeper's cadence is
   * the resolution. Omit for no deadline.
   */
  timeoutMs?: number;
  /**
   * Internal: set by a sub-workflow step to link a child workflow back to the
   * parent workflow + step that started it, so the parent step resumes when the child ends.
   */
  parentWorkflowId?: WorkflowId;
  parentStepId?: StepId;
}

export interface WorkflowCreatedResult {
  workflowId: WorkflowId;
  totalSteps: number;
  /** Keys of the steps that were immediately enqueued (the dependency-free roots). */
  enqueuedSteps: string[];
}

// ============================================================================
// Step execution context & handlers
// ============================================================================

/**
 * Context passed to a step handler. `context` is the host-provided per-step value
 * (a DI scope, an AI model, domain services) produced by the `buildStepContext`
 * hook — flow-core treats it as opaque.
 */
export interface StepExecutionContext<TContext = unknown> {
  workflowId: WorkflowId;
  stepId: StepId;
  stepKey: string;
  partitionKey: string;
  /** Workflow-level input. */
  workflowInput: Record<string, unknown>;
  /** Static step-level input from the definition. */
  stepInput: Record<string, unknown>;
  /** Outputs of completed dependency steps, keyed by step key. */
  dependencyOutputs: Record<string, unknown>;
  /** Cancellation signal. */
  signal?: AbortSignal;
  /**
   * Report that this step is still alive, and ask whether it should keep going.
   *
   * Resolves `false` when the step is no longer this invocation's to run — the
   * workflow was cancelled or blew its deadline, or the stuck-step sweeper
   * decided the worker was dead and re-queued the step. All three mean *stop*;
   * the engine fires {@link signal} and discards whatever the handler returns.
   *
   * Writes are throttled, so calling it in a tight loop is cheap. A step type
   * that declares no `heartbeatTimeoutMs` gets a no-op that always resolves
   * `true`.
   */
  heartbeat: () => Promise<boolean>;
  /** Host-provided per-step context. */
  context: TContext;
}

export interface StepError extends FlowErrorShape {
  key: 'step_error';
  message: string;
  /** Whether the failure is transient and the step should be retried. */
  retryable?: boolean;
  /**
   * How `retryable` was decided. `'heuristic'` means nothing authoritative said
   * either way and the default classifier guessed from the error's shape/message —
   * the engine's `defaultRetryable` may override those, and only those.
   */
  retryableFrom?: 'explicit' | 'predicate' | 'heuristic';
}

export type StepHandler<TContext = unknown> = (
  ctx: StepExecutionContext<TContext>,
) => Promise<Result<Record<string, unknown>, StepError>>;

/** Context for a step's compensation handler — the execution context plus the step's own output. */
export interface StepCompensationContext<TContext = unknown> extends StepExecutionContext<TContext> {
  /** The output the step produced when it completed (what compensation undoes). */
  output: Record<string, unknown>;
}

/**
 * Optional rollback handler. On workflow failure the engine runs it once for each
 * `completed` step, in reverse dependency order, to undo side effects. Best-effort: a throw is
 * logged + surfaced on the step (not retried).
 */
export type StepCompensateHandler<TContext = unknown> = (
  ctx: StepCompensationContext<TContext>,
) => Promise<void> | void;

/**
 * Guard deciding whether a ready step actually runs. Evaluated by the engine
 * after the step is claimed and its dependency outputs are resolved, but before
 * the handler: `false` skips the step (and, transitively, everything reachable
 * only through it) instead of running it.
 *
 * Returns a `Result` rather than a bare boolean so a guard that throws is
 * classified exactly like a failing handler — and therefore retried on a
 * transient error rather than silently skipping the branch.
 */
export type StepConditionHandler<TContext = unknown> = (
  ctx: StepExecutionContext<TContext>,
) => Promise<Result<boolean, StepError>>;

/**
 * What happens when a suspended step's wait budget (`timeoutMs`) runs out.
 *
 * - `'fail'` (default) — the step fails; the workflow fails with it.
 * - `{ output }` — the step **completes** with this output and the DAG carries
 *   on. Pair it with a `when` guard downstream to express "approve within 48h,
 *   otherwise escalate": the wait completes with `{ approved: false }` and the
 *   escalation branch picks it up.
 */
export type WaitTimeoutPolicy = 'fail' | { output: Record<string, unknown> };

/** Per-step retry policy. Applied by the engine when a step fails retryably. */
export interface RetryPolicy {
  /** Total attempts including the first (1 = no retry). */
  maxAttempts: number;
  /** Backoff curve between attempts. Default `'fixed'`. */
  backoff?: 'fixed' | 'exponential';
  /** Delay before the 2nd attempt, in ms. Default 1000. */
  initialDelayMs?: number;
  /** Cap on the computed backoff delay, in ms. Default 60000. */
  maxDelayMs?: number;
}

/** A registered handler plus its optional retry/timeout/delay policy. */
export interface StepRegistration<TContext = unknown> {
  handler: StepHandler<TContext>;
  retry?: RetryPolicy;
  /**
   * Per-step wall-clock budget in ms — how long this step may take, whether it
   * spends the time working or waiting.
   *
   * For a normal step it bounds the handler: on expiry the step is aborted and
   * retried. For a `waitForEvent` step (which has no handler to abort) it bounds
   * the **suspension**: if no `resumeStep` arrives in time, {@link onTimeout}
   * decides what happens.
   */
  timeoutMs?: number;
  /** For a `waitForEvent` step: what a `timeoutMs` expiry does. Default `'fail'`. */
  onTimeout?: WaitTimeoutPolicy;
  /**
   * How long this step may go **silent** before the sweeper treats its worker as
   * dead, in ms. Opt-in: without it the step is judged by the engine-wide
   * `stepExpirySeconds + stuckStepBufferSeconds` measured from when it started,
   * which cannot tell a long step from a dead one.
   *
   * Setting it makes the engine beat automatically while the handler runs, so a
   * crash is noticed in seconds rather than minutes without touching the handler.
   */
  heartbeatTimeoutMs?: number;
  /**
   * Who does the beating. Default `'auto'` — the engine beats on a timer for as
   * long as the handler runs, which detects a dead *process* (the timer dies
   * with it).
   *
   * `'manual'` suppresses that timer, so only `ctx.heartbeat()` counts. Silence
   * then also means a **hung** handler, at the cost of having to place the calls.
   */
  heartbeat?: 'auto' | 'manual';
  /**
   * Durable start delay in ms: once the step becomes ready (all deps complete), its
   * first dispatch is held for this long via the queue. A no-op handler with a delay
   * is a durable "sleep" step. Does not affect retry backoff.
   */
  delayMs?: number;
  /**
   * When true, a ready step **suspends** (status `waiting`) instead of being dispatched,
   * until `engine.resumeStep(workflowId, stepKey, payload)` delivers an external event.
   * The handler never runs; the resume payload becomes the step's output.
   */
  waitForEvent?: boolean;
  /**
   * When true, this is a **map** parent: its handler returns `{ items: T[] }`;
   * the engine spawns one child step (of `childType`) per item, suspends the parent as
   * `mapping`, and completes it with `{ items: childOutputs[] }` once all children finish.
   */
  map?: boolean;
  /** For a map parent: the step `type` registered for its per-item child steps. */
  childType?: string;
  /**
   * When set, this is a **sub-workflow** step: its handler returns the child
   * workflow's input; the engine starts a child workflow from this definition, suspends
   * the parent step as `waiting`, and resumes it with the child's output once it terminates.
   */
  subWorkflowDefinition?: WorkflowDefinition;
  /** Optional saga rollback handler: undoes this step's effects on workflow failure. */
  compensate?: StepCompensateHandler<TContext>;
  /**
   * Optional guard: when it returns `false` the step is **skipped** instead of
   * run, and so is everything reachable only through it. This is how a
   * declarative DAG branches — see {@link StepConditionHandler}.
   */
  condition?: StepConditionHandler<TContext>;
  /**
   * How this step's dependencies gate it. Default `'all'`. Use `'any'` on the
   * step where conditional branches converge, so a skipped arm doesn't skip the
   * join. See {@link JoinRule}.
   */
  join?: JoinRule;
}

/** Registry mapping step `type` strings to handlers + their policies. */
export interface StepHandlerRegistry<TContext = unknown> {
  register(
    type: string,
    handler: StepHandler<TContext>,
    options?: {
      retry?: RetryPolicy;
      timeoutMs?: number;
      onTimeout?: WaitTimeoutPolicy;
      heartbeatTimeoutMs?: number;
      heartbeat?: 'auto' | 'manual';
      delayMs?: number;
      waitForEvent?: boolean;
      map?: boolean;
      childType?: string;
      subWorkflowDefinition?: WorkflowDefinition;
      compensate?: StepCompensateHandler<TContext>;
      condition?: StepConditionHandler<TContext>;
      join?: JoinRule;
    },
  ): void;
  get(type: string): StepHandler<TContext> | undefined;
  getRegistration(type: string): StepRegistration<TContext> | undefined;
  has(type: string): boolean;
  types(): string[];
}

// ============================================================================
// Persisted records (the store's data model)
// ============================================================================

export interface WorkflowRecord {
  id: WorkflowId;
  type: string;
  status: WorkflowStatus;
  partitionKey: string;
  input: Record<string, unknown>;
  output: Record<string, unknown> | null;
  error: string | null;
  entityRef: string | null;
  idempotencyKey: string | null;
  /** For a sub-workflow child: the parent workflow + step that started it. */
  parentWorkflowId: WorkflowId | null;
  parentStepId: StepId | null;
  totalSteps: number;
  completedSteps: number;
  failedSteps: number;
  metadata: Record<string, unknown> | null;
  /** Absolute wall-clock deadline for the run (from `StartOptions.timeoutMs`), or null. */
  deadlineAt: string | null;
  createdAt: string;
  startedAt: string | null;
  completedAt: string | null;
}

export interface StepRecord {
  id: StepId;
  workflowId: WorkflowId;
  key: string;
  type: string;
  status: StepStatus;
  dependencies: string[];
  input: Record<string, unknown> | null;
  output: Record<string, unknown> | null;
  error: string | null;
  metadata: Record<string, unknown> | null;
  /** Number of execution attempts so far (incremented each time the step runs). */
  attempts: number;
  /** For a map child: the id of its map-parent step; null for normal/keyed steps. */
  parentStepId: StepId | null;
  /**
   * When the running worker last reported in, or null if it never has. The
   * stuck-step sweeper prefers this over `startedAt` — it is the difference
   * between "started a while ago" and "hasn't spoken recently".
   */
  heartbeatAt: string | null;
  startedAt: string | null;
  completedAt: string | null;
}

export interface WorkflowWithSteps extends WorkflowRecord {
  steps: StepRecord[];
}

// ============================================================================
// Errors
// ============================================================================

export interface WorkflowNotFoundError extends FlowErrorShape {
  key: 'workflow_not_found';
}
export interface InvalidWorkflowDefinitionError extends FlowErrorShape {
  key: 'invalid_workflow_definition';
}
export interface StepHandlerNotFoundError extends FlowErrorShape {
  key: 'step_handler_not_found';
  stepType: string;
}
/** `retryWorkflow` was called on a run that cannot be resumed from where it stopped. */
export interface WorkflowNotRetryableError extends FlowErrorShape {
  key: 'workflow_not_retryable';
  status: WorkflowStatus;
}

/**
 * Engine error type. The named members cover flow-core's own failures; the
 * bare `FlowErrorShape` arm lets hooks (e.g. an AI quota guard in `onBeforeStart`)
 * reject a start with a domain-specific error key the engine just passes through.
 */
export type FlowError =
  | WorkflowNotFoundError
  | InvalidWorkflowDefinitionError
  | StepHandlerNotFoundError
  | WorkflowNotRetryableError
  | StepError
  | FlowErrorShape;
