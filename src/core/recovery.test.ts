import { describe, it, expect } from 'vitest';
import { z } from 'zod';
import { createWorkflowEngine } from './engine';
import { createInMemoryWorkflowStore } from './in-memory-store';
import { createStepHandlerRegistry } from './registry';
import { defineStep, buildWorkflow, defineWaitStep, defineMapStep } from './defineStep';
import { retryableError } from './retry';
import { createRecordingObserver } from './observability';
import type { Dispatcher, DispatchStepPayload } from './dispatcher';
import type { FlowObserver } from './observability';
import type { WorkflowEngineConfig } from './engine';

type Ctx = undefined;

function harness(opts?: { observer?: FlowObserver; config?: WorkflowEngineConfig }) {
  const store = createInMemoryWorkflowStore();
  const registry = createStepHandlerRegistry<Ctx>();
  const clock = { at: new Date('2026-01-01T00:00:00.000Z') };
  const queue: Array<{ payload: DispatchStepPayload; runAt: number }> = [];

  const dispatcher: Dispatcher = {
    async enqueueStep(payload, options) {
      queue.push({ payload, runAt: clock.at.getTime() + (options?.startAfterSeconds ?? 0) * 1000 });
      return { ok: true, value: undefined };
    },
  };

  const engine = createWorkflowEngine<Ctx>({
    store,
    registry,
    dispatcher,
    partitionKey: 'test',
    observer: opts?.observer,
    config: opts?.config,
    now: () => clock.at,
  });

  async function drain() {
    let guard = 0;
    for (;;) {
      if (++guard > 500) throw new Error('drain runaway');
      const index = queue.findIndex((j) => j.runAt <= clock.at.getTime());
      if (index < 0) return;
      const [job] = queue.splice(index, 1);
      try {
        await engine.handleStepJob(job!.payload);
      } catch {
        /* a real dispatcher would retry */
      }
    }
  }

  const advance = (ms: number) => {
    clock.at = new Date(clock.at.getTime() + ms);
  };

  const statuses = async (workflowId: number) =>
    Object.fromEntries((await store.listSteps(workflowId)).map((s) => [s.key, s.status]));

  return { store, registry, engine, queue, drain, advance, statuses, clock };
}

const noInput = z.object({});
const emptyOut = z.object({}) as unknown as z.ZodType<Record<string, never>>;

/** a → b → c, where `b` fails until `failuresLeft` is exhausted. */
function flakyChain(calls: string[], control: { failB: boolean }) {
  const a = defineStep<Record<string, never>, { a: number }, Ctx>({
    type: 'r:a',
    workflowInputSchema: noInput,
    outputSchema: z.object({ a: z.number() }),
    handler: async () => {
      calls.push('a');
      return { a: 1 };
    },
  });
  const b = defineStep<Record<string, never>, { b: number }, Ctx, { a: typeof a }>({
    type: 'r:b',
    workflowInputSchema: noInput,
    outputSchema: z.object({ b: z.number() }),
    dependencies: { a },
    handler: async (ctx) => {
      calls.push('b');
      if (control.failB) throw new Error('downstream is down');
      return { b: ctx.deps.a.a + 1 };
    },
  });
  const c = defineStep<Record<string, never>, { c: number }, Ctx, { b: typeof b }>({
    type: 'r:c',
    workflowInputSchema: noInput,
    outputSchema: z.object({ c: z.number() }),
    dependencies: { b },
    handler: async (ctx) => {
      calls.push('c');
      return { c: ctx.deps.b.b + 1 };
    },
  });
  return buildWorkflow<Record<string, never>, Ctx>({ type: 'chain', inputSchema: noInput, steps: { a, b, c } });
}

describe('retryWorkflow', () => {
  it('resumes a failed run from the failure point without re-running completed work', async () => {
    const observer = createRecordingObserver();
    const h = harness({ observer });
    const calls: string[] = [];
    const control = { failB: true };
    const wf = flakyChain(calls, control);
    wf.register(h.registry);

    const started = await wf.start(h.engine, {});
    if (!started.ok) throw new Error('start failed');
    const id = started.value.workflowId;
    await h.drain();

    expect(await h.statuses(id)).toEqual({ a: 'completed', b: 'failed', c: 'skipped' });
    expect(calls).toEqual(['a', 'b']);

    // The outage is over.
    control.failB = false;
    const retried = await h.engine.retryWorkflow(id);
    expect(retried.ok).toBe(true);
    expect(retried.ok && retried.value.resetSteps.sort()).toEqual(['b', 'c']);
    await h.drain();

    // `a` was not re-run — its side effect is not repeated.
    expect(calls).toEqual(['a', 'b', 'b', 'c']);
    const final = await h.engine.getWorkflowStatus(id);
    expect(final.ok && final.value.status).toBe('completed');
    // …and its output survived the round trip into the aggregate.
    expect(final.ok && final.value.output).toEqual({ a: { a: 1 }, b: { b: 2 }, c: { c: 3 } });
    expect(observer.events.some((e) => e.type === 'workflow.retried')).toBe(true);
  });

  it('recomputes the workflow counters', async () => {
    const h = harness();
    const calls: string[] = [];
    const control = { failB: true };
    const wf = flakyChain(calls, control);
    wf.register(h.registry);

    const started = await wf.start(h.engine, {});
    if (!started.ok) throw new Error('start failed');
    const id = started.value.workflowId;
    await h.drain();
    expect((await h.store.getWorkflow(id))?.failedSteps).toBe(1);

    control.failB = false;
    await h.engine.retryWorkflow(id);
    const reopened = await h.store.getWorkflow(id);
    expect(reopened?.status).toBe('running');
    expect(reopened?.failedSteps).toBe(0);
    expect(reopened?.completedSteps).toBe(1);
    expect(reopened?.error).toBeNull();
    expect(reopened?.completedAt).toBeNull();
  });

  it('gives the retried steps a fresh attempt budget', async () => {
    const h = harness();
    let attempts = 0;
    const step = defineStep<Record<string, never>, Record<string, never>, Ctx>({
      type: 'ra:step',
      workflowInputSchema: noInput,
      outputSchema: emptyOut,
      retry: { maxAttempts: 2, initialDelayMs: 0 },
      handler: async () => {
        attempts++;
        throw retryableError('still broken');
      },
    });
    const wf = buildWorkflow<Record<string, never>, Ctx>({ type: 'budget', inputSchema: noInput, steps: { step } });
    wf.register(h.registry);

    const started = await wf.start(h.engine, {});
    if (!started.ok) throw new Error('start failed');
    await h.drain();
    expect(attempts).toBe(2);

    await h.engine.retryWorkflow(started.value.workflowId);
    await h.drain();
    // A full budget again, not one leftover attempt.
    expect(attempts).toBe(4);
    expect((await h.store.listSteps(started.value.workflowId))[0]?.attempts).toBe(2);
  });

  it('clears a map parent’s children so a retry fans out afresh', async () => {
    const h = harness();
    const control = { fail: true };
    const each = defineMapStep<number, { doubled: number }, Record<string, never>, Ctx>({
      type: 'rm:map',
      workflowInputSchema: noInput,
      itemOutputSchema: z.object({ doubled: z.number() }),
      items: () => [1, 2, 3],
      each: (item) => {
        if (control.fail && item === 2) throw new Error('item 2 is poison');
        return { doubled: item * 2 };
      },
    });
    const wf = buildWorkflow<Record<string, never>, Ctx>({ type: 'map-retry', inputSchema: noInput, steps: { each } });
    wf.register(h.registry);

    const started = await wf.start(h.engine, {});
    if (!started.ok) throw new Error('start failed');
    const id = started.value.workflowId;
    await h.drain();
    expect((await h.engine.getWorkflowStatus(id)).ok && (await h.store.getWorkflow(id))?.status).toBe('failed');

    control.fail = false;
    await h.engine.retryWorkflow(id);
    await h.drain();

    const steps = await h.store.listSteps(id);
    // Exactly one generation of children, not two.
    expect(steps.filter((s) => s.parentStepId != null)).toHaveLength(3);
    expect((await h.store.getWorkflow(id))?.totalSteps).toBe(4);
    const final = await h.engine.getWorkflowStatus(id);
    expect(final.ok && final.value.status).toBe('completed');
    expect(final.ok && final.value.output).toEqual({ each: { items: [{ doubled: 2 }, { doubled: 4 }, { doubled: 6 }] } });
  });

  it('re-runs a step whose work was compensated away', async () => {
    const h = harness();
    const events: string[] = [];
    const control = { fail: true };
    const charge = defineStep<Record<string, never>, { charged: boolean }, Ctx>({
      type: 'rc:charge',
      workflowInputSchema: noInput,
      outputSchema: z.object({ charged: z.boolean() }),
      handler: async () => {
        events.push('charge');
        return { charged: true };
      },
      compensate: async () => {
        events.push('refund');
      },
    });
    const ship = defineStep<Record<string, never>, Record<string, never>, Ctx, { charge: typeof charge }>({
      type: 'rc:ship',
      workflowInputSchema: noInput,
      outputSchema: emptyOut,
      dependencies: { charge },
      handler: async () => {
        if (control.fail) throw new Error('warehouse offline');
        events.push('ship');
        return {} as Record<string, never>;
      },
    });
    const wf = buildWorkflow<Record<string, never>, Ctx>({ type: 'saga-retry', inputSchema: noInput, steps: { charge, ship } });
    wf.register(h.registry);

    const started = await wf.start(h.engine, {});
    if (!started.ok) throw new Error('start failed');
    const id = started.value.workflowId;
    await h.drain();
    expect(events).toEqual(['charge', 'refund']);
    expect((await h.statuses(id)).charge).toBe('compensated');

    control.fail = false;
    const retried = await h.engine.retryWorkflow(id);
    expect(retried.ok && retried.value.resetSteps.sort()).toEqual(['charge', 'ship']);
    await h.drain();

    // The refund undid the charge, so the charge has to happen again.
    expect(events).toEqual(['charge', 'refund', 'charge', 'ship']);
    const final = await h.engine.getWorkflowStatus(id);
    expect(final.ok && final.value.status).toBe('completed');
  });

  it('refuses to retry a run that is not failed', async () => {
    const h = harness();
    const only = defineStep<Record<string, never>, Record<string, never>, Ctx>({
      type: 'rn:only',
      workflowInputSchema: noInput,
      outputSchema: emptyOut,
      handler: async () => ({}) as Record<string, never>,
    });
    const wf = buildWorkflow<Record<string, never>, Ctx>({ type: 'happy', inputSchema: noInput, steps: { only } });
    wf.register(h.registry);

    const started = await wf.start(h.engine, {});
    if (!started.ok) throw new Error('start failed');
    await h.drain();

    const retried = await h.engine.retryWorkflow(started.value.workflowId);
    expect(retried.ok).toBe(false);
    expect(!retried.ok && retried.error.key).toBe('workflow_not_retryable');
    expect(!retried.ok && retried.error.message).toContain("is 'completed'");
  });

  it('reports a missing workflow rather than pretending to retry it', async () => {
    const h = harness();
    const retried = await h.engine.retryWorkflow(999);
    expect(!retried.ok && retried.error.key).toBe('workflow_not_found');
  });
});

describe('stuck-step recovery', () => {
  /** A workflow whose only step is left hanging in `running` (worker died). */
  async function stranded(h: ReturnType<typeof harness>, maxAttempts: number) {
    const step = defineStep<Record<string, never>, Record<string, never>, Ctx>({
      type: 's:step',
      workflowInputSchema: noInput,
      outputSchema: emptyOut,
      retry: { maxAttempts, initialDelayMs: 0 },
      handler: async () => ({}) as Record<string, never>,
    });
    const wf = buildWorkflow<Record<string, never>, Ctx>({ type: 'stuck', inputSchema: noInput, steps: { step } });
    wf.register(h.registry);
    const started = await wf.start(h.engine, {});
    if (!started.ok) throw new Error('start failed');
    const id = started.value.workflowId;

    // Simulate the crash: the step was claimed, then the worker vanished.
    const [row] = await h.store.listSteps(id);
    await h.store.markStepRunning(row!.id, h.clock.at.toISOString());
    h.queue.length = 0;
    h.advance((600 + 300 + 1) * 1000);
    return { id, stepId: row!.id };
  }

  it('re-queues a crashed step while its attempt budget lasts', async () => {
    const h = harness();
    const { id, stepId } = await stranded(h, 3);

    const swept = await h.engine.recoverStuckWorkflows();
    expect(swept).toMatchObject({ retriedSteps: 1, recoveredSteps: 0, recoveredWorkflows: 0 });
    expect((await h.store.getStep(stepId))?.status).toBe('pending');
    expect(h.queue).toHaveLength(1);

    // And it actually runs, taking the workflow to completion.
    h.advance(60_000);
    await h.drain();
    const final = await h.engine.getWorkflowStatus(id);
    expect(final.ok && final.value.status).toBe('completed');
  });

  it('fails a crashed step once the budget is spent', async () => {
    const h = harness();
    const { id } = await stranded(h, 1);

    const swept = await h.engine.recoverStuckWorkflows();
    expect(swept).toMatchObject({ retriedSteps: 0, recoveredSteps: 1, recoveredWorkflows: 1 });
    const final = await h.engine.getWorkflowStatus(id);
    expect(final.ok && final.value.status).toBe('failed');
    expect(final.ok && final.value.error).toContain('worker likely crashed');
  });

  it('honours onStuckStep: "fail" whatever the budget says', async () => {
    const h = harness({ config: { onStuckStep: 'fail' } });
    const { id } = await stranded(h, 5);

    const swept = await h.engine.recoverStuckWorkflows();
    expect(swept).toMatchObject({ retriedSteps: 0, recoveredSteps: 1 });
    const final = await h.engine.getWorkflowStatus(id);
    expect(final.ok && final.value.status).toBe('failed');
  });
});

describe('wait deadlines', () => {
  function approvalFlow(timeoutMs: number, onTimeout: 'fail' | { output: { approved: boolean } }) {
    const approval = defineWaitStep<{ approved: boolean }, Ctx>({
      type: 'wt:approval',
      outputSchema: z.object({ approved: z.boolean() }),
      timeoutMs,
      onTimeout,
    });
    const after = defineStep<Record<string, never>, { saw: boolean }, Ctx, { approval: typeof approval }>({
      type: 'wt:after',
      workflowInputSchema: noInput,
      outputSchema: z.object({ saw: z.boolean() }),
      dependencies: { approval },
      handler: async (ctx) => ({ saw: ctx.deps.approval.approved }),
    });
    return buildWorkflow<Record<string, never>, Ctx>({
      type: 'wait-timeout',
      inputSchema: noInput,
      steps: { approval, after },
    });
  }

  it('fails a wait that nobody answered', async () => {
    const observer = createRecordingObserver();
    const h = harness({ observer });
    approvalFlow(60_000, 'fail').register(h.registry);

    const started = await h.engine.startWorkflow(approvalFlow(60_000, 'fail').definition, {});
    if (!started.ok) throw new Error('start failed');
    const id = started.value.workflowId;
    await h.drain();
    expect((await h.statuses(id)).approval).toBe('waiting');

    h.advance(60_000);
    await h.drain();

    expect(await h.statuses(id)).toEqual({ approval: 'failed', after: 'skipped' });
    const final = await h.engine.getWorkflowStatus(id);
    expect(final.ok && final.value.status).toBe('failed');
    expect(observer.events.some((e) => e.type === 'step.timedOut')).toBe(true);
  });

  it('carries on with a stand-in answer under onTimeout: { output }', async () => {
    const h = harness();
    const wf = approvalFlow(60_000, { output: { approved: false } });
    wf.register(h.registry);

    const started = await h.engine.startWorkflow(wf.definition, {});
    if (!started.ok) throw new Error('start failed');
    const id = started.value.workflowId;
    await h.drain();

    h.advance(60_000);
    await h.drain();

    const final = await h.engine.getWorkflowStatus(id);
    expect(final.ok && final.value.status).toBe('completed');
    expect(final.ok && final.value.output).toEqual({ approval: { approved: false }, after: { saw: false } });
  });

  it('is a no-op once the event arrived', async () => {
    const h = harness();
    const wf = approvalFlow(60_000, 'fail');
    wf.register(h.registry);

    const started = await h.engine.startWorkflow(wf.definition, {});
    if (!started.ok) throw new Error('start failed');
    const id = started.value.workflowId;
    await h.drain();

    await h.engine.resumeStep(id, 'approval', { approved: true });
    await h.drain();

    // The deadline job still fires later; it must not disturb a settled step.
    h.advance(120_000);
    await h.drain();

    const final = await h.engine.getWorkflowStatus(id);
    expect(final.ok && final.value.status).toBe('completed');
    expect(final.ok && final.value.output).toEqual({ approval: { approved: true }, after: { saw: true } });
  });

  it('re-arms rather than firing early on a redelivered deadline job', async () => {
    const h = harness();
    const wf = approvalFlow(60_000, 'fail');
    wf.register(h.registry);

    const started = await h.engine.startWorkflow(wf.definition, {});
    if (!started.ok) throw new Error('start failed');
    const id = started.value.workflowId;
    await h.drain();

    const stepId = (await h.store.listSteps(id)).find((s) => s.key === 'approval')!.id;
    // A duplicate delivery, 30s into a 60s wait.
    h.advance(30_000);
    await h.engine.timeoutStep(id, stepId);
    expect((await h.statuses(id)).approval).toBe('waiting');

    // The re-armed job covers the remaining 30s.
    h.advance(30_000);
    await h.drain();
    expect((await h.statuses(id)).approval).toBe('failed');
  });

  it('leaves a wait without a deadline suspended indefinitely', async () => {
    const h = harness();
    const approval = defineWaitStep<{ approved: boolean }, Ctx>({
      type: 'wn:approval',
      outputSchema: z.object({ approved: z.boolean() }),
    });
    const wf = buildWorkflow<Record<string, never>, Ctx>({ type: 'no-deadline', inputSchema: noInput, steps: { approval } });
    wf.register(h.registry);

    const started = await wf.start(h.engine, {});
    if (!started.ok) throw new Error('start failed');
    await h.drain();

    h.advance(365 * 24 * 60 * 60 * 1000);
    await h.drain();
    expect((await h.statuses(started.value.workflowId)).approval).toBe('waiting');
    expect(h.queue).toHaveLength(0);
  });
});

describe('workflow deadlines', () => {
  function slowChain() {
    const first = defineStep<Record<string, never>, Record<string, never>, Ctx>({
      type: 'd:first',
      workflowInputSchema: noInput,
      outputSchema: emptyOut,
      handler: async () => ({}) as Record<string, never>,
    });
    const second = defineStep<Record<string, never>, Record<string, never>, Ctx, { first: typeof first }>({
      type: 'd:second',
      workflowInputSchema: noInput,
      outputSchema: emptyOut,
      dependencies: { first },
      delayMs: 60_000,
      handler: async () => ({}) as Record<string, never>,
    });
    return buildWorkflow<Record<string, never>, Ctx>({ type: 'deadline', inputSchema: noInput, steps: { first, second } });
  }

  it('fails a run that outlives its budget before the next step runs', async () => {
    const h = harness();
    const wf = slowChain();
    wf.register(h.registry);

    const started = await wf.start(h.engine, {}, { timeoutMs: 30_000 });
    if (!started.ok) throw new Error('start failed');
    const id = started.value.workflowId;
    expect((await h.store.getWorkflow(id))?.deadlineAt).toBe('2026-01-01T00:00:30.000Z');

    await h.drain();
    // `second` is held in the queue for 60s, so it is picked up past the 30s deadline.
    h.advance(61_000);
    await h.drain();

    const final = await h.engine.getWorkflowStatus(id);
    expect(final.ok && final.value.status).toBe('failed');
    expect(final.ok && final.value.error).toContain('exceeded its deadline');
    expect((await h.statuses(id)).second).toBe('skipped');
  });

  it('lets a run that finishes in time complete normally', async () => {
    const h = harness();
    const wf = slowChain();
    wf.register(h.registry);

    const started = await wf.start(h.engine, {}, { timeoutMs: 10 * 60_000 });
    if (!started.ok) throw new Error('start failed');
    await h.drain();
    h.advance(61_000);
    await h.drain();

    const final = await h.engine.getWorkflowStatus(started.value.workflowId);
    expect(final.ok && final.value.status).toBe('completed');
  });

  it('is caught by the sweeper even when nothing is trying to run', async () => {
    const h = harness();
    const approval = defineWaitStep<{ approved: boolean }, Ctx>({
      type: 'dw:approval',
      outputSchema: z.object({ approved: z.boolean() }),
    });
    const wf = buildWorkflow<Record<string, never>, Ctx>({ type: 'deadline-wait', inputSchema: noInput, steps: { approval } });
    wf.register(h.registry);

    const started = await wf.start(h.engine, {}, { timeoutMs: 30_000 });
    if (!started.ok) throw new Error('start failed');
    const id = started.value.workflowId;
    await h.drain();
    expect((await h.statuses(id)).approval).toBe('waiting');

    h.advance(31_000);
    const swept = await h.engine.recoverStuckWorkflows();
    expect(swept.expiredWorkflows).toBe(1);

    const final = await h.engine.getWorkflowStatus(id);
    expect(final.ok && final.value.status).toBe('failed');
    expect((await h.statuses(id)).approval).toBe('skipped');
  });

  it('runs saga compensation when a deadline ends the run', async () => {
    const h = harness();
    const undone: string[] = [];
    const charge = defineStep<Record<string, never>, Record<string, never>, Ctx>({
      type: 'dc:charge',
      workflowInputSchema: noInput,
      outputSchema: emptyOut,
      handler: async () => ({}) as Record<string, never>,
      compensate: async () => {
        undone.push('refund');
      },
    });
    const later = defineStep<Record<string, never>, Record<string, never>, Ctx, { charge: typeof charge }>({
      type: 'dc:later',
      workflowInputSchema: noInput,
      outputSchema: emptyOut,
      dependencies: { charge },
      delayMs: 60_000,
      handler: async () => ({}) as Record<string, never>,
    });
    const wf = buildWorkflow<Record<string, never>, Ctx>({ type: 'deadline-saga', inputSchema: noInput, steps: { charge, later } });
    wf.register(h.registry);

    const started = await wf.start(h.engine, {}, { timeoutMs: 30_000 });
    if (!started.ok) throw new Error('start failed');
    await h.drain();
    h.advance(61_000);
    await h.drain();

    expect(undone).toEqual(['refund']);
  });
});
