import type { Result, FlowErrorShape } from './result';

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
  /** Workflow type identifier (e.g. 'listing-enrichment'). */
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
  /** Optional reference for efficient filtering/listing (e.g. 'listing:123'). */
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
   * Internal (gap 08): set by a sub-workflow step to link a child workflow back to the
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
  /** Host-provided per-step context. */
  context: TContext;
}

export interface StepError extends FlowErrorShape {
  key: 'step_error';
  message: string;
  /** Whether the failure is transient and the step should be retried. */
  retryable?: boolean;
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
 * Optional rollback handler (gap 09). On workflow failure the engine runs it once for each
 * `completed` step, in reverse dependency order, to undo side effects. Best-effort: a throw is
 * logged + surfaced on the step (not retried).
 */
export type StepCompensateHandler<TContext = unknown> = (
  ctx: StepCompensationContext<TContext>,
) => Promise<void> | void;

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
  /** Per-step wall-clock timeout in ms. On expiry the step is aborted + retried. */
  timeoutMs?: number;
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
   * When true, this is a **map** parent (gap 06): its handler returns `{ items: T[] }`;
   * the engine spawns one child step (of `childType`) per item, suspends the parent as
   * `mapping`, and completes it with `{ items: childOutputs[] }` once all children finish.
   */
  map?: boolean;
  /** For a map parent: the step `type` registered for its per-item child steps. */
  childType?: string;
  /**
   * When set, this is a **sub-workflow** step (gap 08): its handler returns the child
   * workflow's input; the engine starts a child workflow from this definition, suspends
   * the parent step as `waiting`, and resumes it with the child's output once it terminates.
   */
  subWorkflowDefinition?: WorkflowDefinition;
  /** Optional saga rollback handler (gap 09): undoes this step's effects on workflow failure. */
  compensate?: StepCompensateHandler<TContext>;
}

/** Registry mapping step `type` strings to handlers + their policies. */
export interface StepHandlerRegistry<TContext = unknown> {
  register(
    type: string,
    handler: StepHandler<TContext>,
    options?: {
      retry?: RetryPolicy;
      timeoutMs?: number;
      delayMs?: number;
      waitForEvent?: boolean;
      map?: boolean;
      childType?: string;
      subWorkflowDefinition?: WorkflowDefinition;
      compensate?: StepCompensateHandler<TContext>;
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
  /** For a sub-workflow child (gap 08): the parent workflow + step that started it. */
  parentWorkflowId: WorkflowId | null;
  parentStepId: StepId | null;
  totalSteps: number;
  completedSteps: number;
  failedSteps: number;
  metadata: Record<string, unknown> | null;
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
  /** For a map child (gap 06): the id of its map-parent step; null for normal/keyed steps. */
  parentStepId: StepId | null;
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

/**
 * Engine error type. The named members cover flow-core's own failures; the
 * bare `FlowErrorShape` arm lets hooks (e.g. an AI quota guard in `onBeforeStart`)
 * reject a start with a domain-specific error key the engine just passes through.
 */
export type FlowError =
  | WorkflowNotFoundError
  | InvalidWorkflowDefinitionError
  | StepHandlerNotFoundError
  | StepError
  | FlowErrorShape;
