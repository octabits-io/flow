import type { Result, FlowErrorShape } from './result';
import type { WorkflowId, StepId, WorkflowDefinition, WorkflowRecord, StepRecord, StartOptions } from './types';

// ============================================================================
// Lifecycle hooks
// ============================================================================
//
// Hooks are how add-ons (e.g. flow-ai) inject behavior without forking the
// engine. flow-core never references AI, tokens, cost, or quota — those live
// entirely in hook implementations.

export interface BeforeStartArgs {
  definition: WorkflowDefinition;
  input: Record<string, unknown>;
  options?: StartOptions;
}

export interface BeforeStartResult {
  /** Extra metadata persisted on the workflow row (merged over `options.metadata`). */
  metadata?: Record<string, unknown>;
}

export interface BuildStepContextArgs {
  workflowId: WorkflowId;
  stepId: StepId;
  stepKey: string;
  partitionKey: string;
  workflow: WorkflowRecord;
  step: StepRecord;
}

export interface AfterStepArgs<TContext> {
  workflowId: WorkflowId;
  stepId: StepId;
  partitionKey: string;
  workflow: WorkflowRecord;
  step: StepRecord;
  output: Record<string, unknown>;
  /** The same context object that was handed to the step handler. */
  context: TContext;
}

export interface WorkflowCompletedArgs {
  workflowId: WorkflowId;
  partitionKey: string;
  workflow: WorkflowRecord;
}

export interface WorkflowHooks<TContext = unknown> {
  /**
   * Runs before a workflow is created. Return an error to reject the start
   * (e.g. quota exceeded) — the engine passes the error through untouched.
   */
  onBeforeStart?(args: BeforeStartArgs): Promise<Result<BeforeStartResult, FlowErrorShape>>;
  /**
   * Produces the per-step `context` handed to the handler (and later to
   * `onAfterStep`). This is where the AI add-on resolves and instruments a model.
   * If omitted, handlers receive `undefined` as their context.
   */
  buildStepContext?(args: BuildStepContextArgs): TContext | Promise<TContext>;
  /**
   * Runs after a step handler succeeds, before the step is marked completed.
   * The AI add-on reads token usage off `context` here and persists cost.
   */
  onAfterStep?(args: AfterStepArgs<TContext>): Promise<void>;
  /** Runs once when a workflow reaches `completed` (e.g. daily usage rollup). */
  onWorkflowCompleted?(args: WorkflowCompletedArgs): Promise<void>;
}
