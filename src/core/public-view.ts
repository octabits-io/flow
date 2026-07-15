/**
 * Public wire view of a workflow — the projection every HTTP consumer was
 * hand-writing.
 *
 * The engine's records ({@link WorkflowRecord}, {@link StepRecord}) are the
 * store's data model: they carry fields that must not leak onto a public API
 * (`partitionKey`, `idempotencyKey`, sub-workflow linkage, `metadata`, retry
 * `attempts`) and step statuses that are engine mechanics rather than display
 * states. Serving them verbatim also couples the consumer's API contract to
 * flow's record shape. This module owns the boundary instead:
 *
 * - {@link toPublicWorkflow} / {@link toPublicStep} — drop the internal
 *   fields, fold engine step statuses to the five display states.
 * - {@link PUBLIC_WORKFLOW_SCHEMA} / {@link PUBLIC_WORKFLOW_STEP_SCHEMA} —
 *   the same shapes as Zod schemas, ready for route `response` declarations
 *   (OpenAPI output, response validation, typed clients).
 *
 * The projection is deliberately closed: anything consumer-specific rides on
 * top rather than through an options bag. E.g. surfacing an `appliedAt` stamp
 * kept in workflow metadata:
 *
 * ```ts
 * const schema = PUBLIC_WORKFLOW_SCHEMA.extend({ appliedAt: z.string().nullable() });
 * const view = {
 *   ...toPublicWorkflow(workflow),
 *   appliedAt: (workflow.metadata?.appliedAt as string | undefined) ?? null,
 * };
 * ```
 */
import { z } from 'zod';
import type { StepRecord, StepStatus, WorkflowWithSteps } from './types';

// ============================================================================
// Display status fold
// ============================================================================

/** The five states a UI renders. Suspensions display as in-flight. */
export type StepDisplayStatus = 'pending' | 'running' | 'completed' | 'failed' | 'skipped';

export const STEP_DISPLAY_STATUS_SCHEMA = z.enum([
  'pending',
  'running',
  'completed',
  'failed',
  'skipped',
]);

/**
 * Engine step statuses folded to display states. `waiting` (suspended for an
 * event), `mapping` (fan-out in flight), and `compensating` (rollback running)
 * are all "still in flight" from a reader's point of view; a `compensated`
 * step's work was undone, which reads as `skipped`, with the failure that
 * triggered the rollback reported by the failed step itself. Exhaustive over
 * {@link StepStatus} so a new engine status is a compile error here, not a
 * silent hole in a consumer's API.
 */
export const STEP_DISPLAY_STATUS: Record<StepStatus, StepDisplayStatus> = {
  pending: 'pending',
  running: 'running',
  completed: 'completed',
  failed: 'failed',
  skipped: 'skipped',
  waiting: 'running',
  mapping: 'running',
  compensating: 'running',
  compensated: 'skipped',
};

/** Fold one engine step status to its display state. */
export function toDisplayStepStatus(status: StepStatus): StepDisplayStatus {
  return STEP_DISPLAY_STATUS[status];
}

// ============================================================================
// Wire shapes
// ============================================================================

/** A step as served to API consumers. Timestamps are ISO strings. */
export interface PublicWorkflowStep {
  id: number;
  key: string;
  type: string;
  status: StepDisplayStatus;
  dependencies: string[];
  input: Record<string, unknown> | null;
  output: Record<string, unknown> | null;
  error: string | null;
  startedAt: string | null;
  completedAt: string | null;
}

/** A workflow (with steps) as served to API consumers. Timestamps are ISO strings. */
export interface PublicWorkflow {
  id: number;
  type: string;
  status: WorkflowWithSteps['status'];
  input: Record<string, unknown>;
  output: Record<string, unknown> | null;
  error: string | null;
  entityRef: string | null;
  totalSteps: number;
  completedSteps: number;
  failedSteps: number;
  steps: PublicWorkflowStep[];
  createdAt: string;
  startedAt: string | null;
  completedAt: string | null;
}

export const WORKFLOW_STATUS_SCHEMA = z.enum([
  'pending',
  'running',
  'completed',
  'failed',
  'cancelled',
]);

export const PUBLIC_WORKFLOW_STEP_SCHEMA = z.object({
  id: z.number().int(),
  key: z.string(),
  type: z.string(),
  status: STEP_DISPLAY_STATUS_SCHEMA,
  dependencies: z.array(z.string()),
  input: z.record(z.string(), z.unknown()).nullable(),
  output: z.record(z.string(), z.unknown()).nullable(),
  error: z.string().nullable(),
  startedAt: z.string().nullable(),
  completedAt: z.string().nullable(),
});

export const PUBLIC_WORKFLOW_SCHEMA = z.object({
  id: z.number().int(),
  type: z.string(),
  status: WORKFLOW_STATUS_SCHEMA,
  input: z.record(z.string(), z.unknown()),
  output: z.record(z.string(), z.unknown()).nullable(),
  error: z.string().nullable(),
  entityRef: z.string().nullable(),
  totalSteps: z.number().int(),
  completedSteps: z.number().int(),
  failedSteps: z.number().int(),
  steps: z.array(PUBLIC_WORKFLOW_STEP_SCHEMA),
  createdAt: z.string(),
  startedAt: z.string().nullable(),
  completedAt: z.string().nullable(),
});

// ============================================================================
// Projections
// ============================================================================

/**
 * Project a step record to its public view. Drops `workflowId` (redundant
 * under the parent), `metadata`, `attempts`, and `parentStepId`; folds the
 * status.
 */
export function toPublicStep(step: StepRecord): PublicWorkflowStep {
  return {
    id: step.id,
    key: step.key,
    type: step.type,
    status: toDisplayStepStatus(step.status),
    dependencies: step.dependencies,
    input: step.input,
    output: step.output,
    error: step.error,
    startedAt: step.startedAt,
    completedAt: step.completedAt,
  };
}

/**
 * Project a workflow (with steps) to its public view. Drops `partitionKey`,
 * `idempotencyKey`, `parentWorkflowId`/`parentStepId`, and `metadata`.
 */
export function toPublicWorkflow(workflow: WorkflowWithSteps): PublicWorkflow {
  return {
    id: workflow.id,
    type: workflow.type,
    status: workflow.status,
    input: workflow.input,
    output: workflow.output,
    error: workflow.error,
    entityRef: workflow.entityRef,
    totalSteps: workflow.totalSteps,
    completedSteps: workflow.completedSteps,
    failedSteps: workflow.failedSteps,
    steps: workflow.steps.map(toPublicStep),
    createdAt: workflow.createdAt,
    startedAt: workflow.startedAt,
    completedAt: workflow.completedAt,
  };
}
