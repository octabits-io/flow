import type {
  WorkflowStore,
  CreateWorkflowParams,
  CreatedWorkflow,
  CompleteStepParams,
  FailStepParams,
  FinishWorkflowParams,
  ListWorkflowsFilters,
  AddChildStep,
} from './store';
import type { WorkflowId, StepId, WorkflowRecord, StepRecord, WorkflowWithSteps } from './types';

/**
 * Reference `WorkflowStore` backed by plain Maps. Intended for tests and single-
 * process experimentation — NOT durable. Partition-scoped at construction to
 * mirror real adapters (the partition value is recorded but isolation is trivial
 * since each instance owns its own maps).
 */
export function createInMemoryWorkflowStore(partitionKey = 'default'): WorkflowStore & {
  /** Test helper: read everything currently stored. */
  _dump(): { workflows: WorkflowRecord[]; steps: StepRecord[] };
} {
  const workflows = new Map<WorkflowId, WorkflowRecord>();
  const steps = new Map<StepId, StepRecord>();
  const byIdempotency = new Map<string, WorkflowId>();
  let workflowSeq = 0;
  let stepSeq = 0;

  const clone = <T>(v: T): T => (v == null ? v : JSON.parse(JSON.stringify(v)));

  function stepsOf(workflowId: WorkflowId): StepRecord[] {
    return Array.from(steps.values()).filter((s) => s.workflowId === workflowId);
  }

  async function createWorkflow(params: CreateWorkflowParams): Promise<CreatedWorkflow> {
    // Idempotency: a start whose key already exists returns the existing workflow.
    if (params.idempotencyKey != null) {
      const existingId = byIdempotency.get(params.idempotencyKey);
      if (existingId != null) {
        return { workflowId: existingId, steps: stepsOf(existingId).map(clone), alreadyExisted: true };
      }
    }

    const id = ++workflowSeq;
    const record: WorkflowRecord = {
      id,
      type: params.type,
      status: 'running',
      partitionKey,
      input: clone(params.input),
      output: null,
      error: null,
      entityRef: params.entityRef ?? null,
      idempotencyKey: params.idempotencyKey ?? null,
      parentWorkflowId: params.parentWorkflowId ?? null,
      parentStepId: params.parentStepId ?? null,
      totalSteps: params.steps.length,
      completedSteps: 0,
      failedSteps: 0,
      metadata: params.metadata ? clone(params.metadata) : null,
      createdAt: params.startedAt,
      startedAt: params.startedAt,
      completedAt: null,
    };
    workflows.set(id, record);
    if (params.idempotencyKey != null) byIdempotency.set(params.idempotencyKey, id);

    const createdSteps: StepRecord[] = params.steps.map((s) => {
      const stepId = ++stepSeq;
      const stepRecord: StepRecord = {
        id: stepId,
        workflowId: id,
        key: s.key,
        type: s.type,
        status: 'pending',
        dependencies: [...s.dependencies],
        input: s.input ? clone(s.input) : null,
        output: null,
        error: null,
        metadata: null,
        attempts: 0,
        parentStepId: null,
        startedAt: null,
        completedAt: null,
      };
      steps.set(stepId, stepRecord);
      return clone(stepRecord);
    });

    return { workflowId: id, steps: createdSteps, alreadyExisted: false };
  }

  async function getWorkflow(workflowId: WorkflowId): Promise<WorkflowRecord | null> {
    const w = workflows.get(workflowId);
    return w ? clone(w) : null;
  }

  async function getStep(stepId: StepId): Promise<StepRecord | null> {
    const s = steps.get(stepId);
    return s ? clone(s) : null;
  }

  async function listSteps(workflowId: WorkflowId): Promise<StepRecord[]> {
    return stepsOf(workflowId).map(clone);
  }

  async function markStepRunning(stepId: StepId, startedAt: string): Promise<void> {
    const s = steps.get(stepId);
    if (s) {
      s.status = 'running';
      s.startedAt = startedAt;
      s.attempts += 1;
    }
  }

  async function markStepPending(stepId: StepId): Promise<void> {
    const s = steps.get(stepId);
    if (s) s.status = 'pending';
  }

  async function markStepWaiting(stepId: StepId): Promise<void> {
    const s = steps.get(stepId);
    if (s) s.status = 'waiting';
  }

  async function markStepMapping(stepId: StepId): Promise<void> {
    const s = steps.get(stepId);
    if (s) s.status = 'mapping';
  }

  async function markStepCompensating(stepId: StepId): Promise<void> {
    const s = steps.get(stepId);
    if (s) s.status = 'compensating';
  }

  async function markStepCompensated(stepId: StepId, error?: string): Promise<void> {
    const s = steps.get(stepId);
    if (s) {
      s.status = 'compensated';
      if (error != null) s.error = error;
    }
  }

  async function addChildSteps(workflowId: WorkflowId, parentStepId: StepId, children: AddChildStep[]): Promise<StepRecord[]> {
    const created: StepRecord[] = children.map((c) => {
      const stepId = ++stepSeq;
      const stepRecord: StepRecord = {
        id: stepId,
        workflowId,
        key: c.key,
        type: c.type,
        status: 'pending',
        dependencies: [],
        input: c.input ? clone(c.input) : null,
        output: null,
        error: null,
        metadata: null,
        attempts: 0,
        parentStepId,
        startedAt: null,
        completedAt: null,
      };
      steps.set(stepId, stepRecord);
      return clone(stepRecord);
    });
    const w = workflows.get(workflowId);
    if (w) w.totalSteps += created.length;
    return created;
  }

  async function listChildSteps(parentStepId: StepId): Promise<StepRecord[]> {
    return Array.from(steps.values())
      .filter((s) => s.parentStepId === parentStepId)
      .sort((a, b) => a.id - b.id)
      .map(clone);
  }

  async function completeStep(params: CompleteStepParams): Promise<void> {
    const s = steps.get(params.stepId);
    if (s) {
      s.status = 'completed';
      s.output = clone(params.output);
      s.completedAt = params.completedAt;
    }
    const w = workflows.get(params.workflowId);
    if (w) w.completedSteps += 1;
  }

  async function failStep(params: FailStepParams): Promise<void> {
    const s = steps.get(params.stepId);
    if (s) {
      s.status = 'failed';
      s.error = params.error;
      s.completedAt = params.completedAt;
    }
    const w = workflows.get(params.workflowId);
    if (w) w.failedSteps += 1;
  }

  async function skipStep(stepId: StepId, reason: string): Promise<void> {
    const s = steps.get(stepId);
    if (s) {
      s.status = 'skipped';
      s.error = reason;
    }
  }

  async function skipPendingSteps(workflowId: WorkflowId, reason: string): Promise<void> {
    for (const s of stepsOf(workflowId)) {
      if (s.status === 'pending' || s.status === 'waiting') {
        s.status = 'skipped';
        s.error = reason;
      }
    }
  }

  async function finishWorkflow(params: FinishWorkflowParams): Promise<void> {
    const w = workflows.get(params.workflowId);
    if (!w) return;
    w.status = params.status;
    w.completedAt = params.completedAt;
    if (params.output !== undefined) w.output = clone(params.output);
    if (params.error !== undefined) w.error = params.error;
  }

  async function listWorkflows(filters: ListWorkflowsFilters): Promise<WorkflowWithSteps[]> {
    let result = Array.from(workflows.values());
    if (filters.status) result = result.filter((w) => w.status === filters.status);
    if (filters.type) result = result.filter((w) => w.type === filters.type);
    if (filters.entityRef) result = result.filter((w) => w.entityRef === filters.entityRef);
    result = result.sort((a, b) => b.id - a.id).slice(0, filters.limit ?? 50);
    return result.map((w) => ({ ...clone(w), steps: stepsOf(w.id).map(clone) }));
  }

  async function listRunningWorkflows(): Promise<WorkflowRecord[]> {
    return Array.from(workflows.values())
      .filter((w) => w.status === 'running')
      .map(clone);
  }

  async function findStuckSteps(workflowId: WorkflowId, cutoff: string): Promise<StepRecord[]> {
    return stepsOf(workflowId)
      .filter((s) => s.status === 'running' && s.startedAt != null && s.startedAt < cutoff)
      .map(clone);
  }

  return {
    createWorkflow,
    getWorkflow,
    getStep,
    listSteps,
    markStepRunning,
    markStepPending,
    markStepWaiting,
    markStepMapping,
    markStepCompensating,
    markStepCompensated,
    addChildSteps,
    listChildSteps,
    completeStep,
    failStep,
    skipStep,
    skipPendingSteps,
    finishWorkflow,
    listWorkflows,
    listRunningWorkflows,
    findStuckSteps,
    _dump() {
      return { workflows: Array.from(workflows.values()).map(clone), steps: Array.from(steps.values()).map(clone) };
    },
  };
}
