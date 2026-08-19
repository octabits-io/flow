import { describe, it, expect } from 'vitest';
import { z } from 'zod';
import type { StepRecord, StepStatus, WorkflowWithSteps } from './types';
import {
  PUBLIC_WORKFLOW_SCHEMA,
  PUBLIC_WORKFLOW_STEP_SCHEMA,
  STEP_DISPLAY_STATUS,
  toDisplayStepStatus,
  toPublicStep,
  toPublicWorkflow,
} from './public-view';

function makeStep(overrides: Partial<StepRecord> = {}): StepRecord {
  return {
    id: 11,
    workflowId: 7,
    key: 'fetch',
    type: 'demo.fetch',
    status: 'completed',
    dependencies: [],
    input: { a: 1 },
    output: { b: 2 },
    error: null,
    metadata: { internal: true },
    attempts: 3,
    parentStepId: null,
    heartbeatAt: null,
    startedAt: '2026-07-14T10:00:00.000Z',
    completedAt: '2026-07-14T10:00:01.000Z',
    ...overrides,
  };
}

function makeWorkflow(overrides: Partial<WorkflowWithSteps> = {}): WorkflowWithSteps {
  return {
    id: 7,
    type: 'demo',
    status: 'completed',
    partitionKey: 'scope-1',
    input: { x: 'y' },
    output: { fetch: { b: 2 } },
    error: null,
    entityRef: 'thing:42',
    idempotencyKey: 'dedup-1',
    deadlineAt: null,
    parentWorkflowId: null,
    parentStepId: null,
    totalSteps: 1,
    completedSteps: 1,
    failedSteps: 0,
    metadata: { keySource: 'platform' },
    createdAt: '2026-07-14T09:59:59.000Z',
    startedAt: '2026-07-14T10:00:00.000Z',
    completedAt: '2026-07-14T10:00:01.000Z',
    steps: [makeStep()],
    ...overrides,
  };
}

describe('display status fold', () => {
  it('folds suspensions to running and compensated to skipped', () => {
    expect(toDisplayStepStatus('waiting')).toBe('running');
    expect(toDisplayStepStatus('mapping')).toBe('running');
    expect(toDisplayStepStatus('compensating')).toBe('running');
    expect(toDisplayStepStatus('compensated')).toBe('skipped');
  });

  it('is identity on the five display states', () => {
    for (const s of ['pending', 'running', 'completed', 'failed', 'skipped'] as const) {
      expect(toDisplayStepStatus(s)).toBe(s);
    }
  });

  it('covers every engine status with a schema-valid display state', () => {
    for (const display of Object.values(STEP_DISPLAY_STATUS)) {
      expect(['pending', 'running', 'completed', 'failed', 'skipped']).toContain(display);
    }
  });
});

describe('toPublicStep', () => {
  it('drops workflowId, metadata, attempts, parentStepId', () => {
    const publicStep = toPublicStep(makeStep({ parentStepId: 5 }));
    expect(publicStep).not.toHaveProperty('workflowId');
    expect(publicStep).not.toHaveProperty('metadata');
    expect(publicStep).not.toHaveProperty('attempts');
    expect(publicStep).not.toHaveProperty('parentStepId');
  });

  it('parses against its wire schema for every engine status', () => {
    const statuses: StepStatus[] = [
      'pending', 'running', 'completed', 'failed', 'skipped',
      'waiting', 'mapping', 'compensating', 'compensated',
    ];
    for (const status of statuses) {
      const projected = toPublicStep(makeStep({ status, output: null, startedAt: null, completedAt: null }));
      expect(PUBLIC_WORKFLOW_STEP_SCHEMA.parse(projected)).toEqual(projected);
    }
  });
});

describe('toPublicWorkflow', () => {
  it('drops partitionKey, idempotencyKey, parent linkage, metadata', () => {
    const publicWorkflow = toPublicWorkflow(makeWorkflow({ parentWorkflowId: 3, parentStepId: 4 }));
    expect(publicWorkflow).not.toHaveProperty('partitionKey');
    expect(publicWorkflow).not.toHaveProperty('idempotencyKey');
    expect(publicWorkflow).not.toHaveProperty('parentWorkflowId');
    expect(publicWorkflow).not.toHaveProperty('parentStepId');
    expect(publicWorkflow).not.toHaveProperty('metadata');
  });

  it('keeps the reader-facing fields and projects nested steps', () => {
    const publicWorkflow = toPublicWorkflow(makeWorkflow());
    expect(publicWorkflow.entityRef).toBe('thing:42');
    expect(publicWorkflow.steps).toHaveLength(1);
    expect(publicWorkflow.steps[0]).not.toHaveProperty('attempts');
  });

  it('parses against its wire schema', () => {
    const projected = toPublicWorkflow(makeWorkflow());
    expect(PUBLIC_WORKFLOW_SCHEMA.parse(projected)).toEqual(projected);
  });

  it('extends for consumer fields (the appliedAt pattern)', () => {
    const workflow = makeWorkflow({ metadata: { appliedAt: '2026-07-14T11:00:00.000Z' } });
    const schema = PUBLIC_WORKFLOW_SCHEMA.extend({ appliedAt: z.string().nullable() });
    const view = {
      ...toPublicWorkflow(workflow),
      appliedAt: (workflow.metadata?.appliedAt as string | undefined) ?? null,
    };
    expect(schema.parse(view).appliedAt).toBe('2026-07-14T11:00:00.000Z');
  });
});
