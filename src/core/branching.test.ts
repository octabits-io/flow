import { describe, it, expect } from 'vitest';
import { z } from 'zod';
import { createWorkflowEngine } from './engine';
import { createInMemoryWorkflowStore } from './in-memory-store';
import { createStepHandlerRegistry } from './registry';
import { defineStep, buildWorkflow, defineWaitStep } from './defineStep';
import { retryableError } from './retry';
import { createRecordingObserver } from './observability';
import type { Dispatcher, DispatchStepPayload } from './dispatcher';
import type { FlowObserver } from './observability';

// ---------------------------------------------------------------------------
// Harness: in-memory store + a queue that honours `startAfterSeconds` against a
// controllable clock, so delayed jobs only run once time has actually passed.
// ---------------------------------------------------------------------------

type Ctx = undefined;

function harness(opts?: { observer?: FlowObserver }) {
  const store = createInMemoryWorkflowStore();
  const registry = createStepHandlerRegistry<Ctx>();
  const clock = { at: new Date('2026-01-01T00:00:00.000Z') };
  const queue: Array<{ payload: DispatchStepPayload; runAt: number }> = [];

  /** Every enqueue ever made, including ones already drained. */
  const dispatched: DispatchStepPayload[] = [];

  const dispatcher: Dispatcher = {
    async enqueueStep(payload, options) {
      dispatched.push(payload);
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
    now: () => clock.at,
  });

  /** Run every job that is due at the current clock reading. */
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
        /* a real dispatcher would retry; the engine already recorded the failure */
      }
    }
  }

  const advance = (ms: number) => {
    clock.at = new Date(clock.at.getTime() + ms);
  };

  const statuses = async (workflowId: number) =>
    Object.fromEntries((await store.listSteps(workflowId)).map((s) => [s.key, s.status]));

  return { store, registry, engine, queue, dispatched, drain, advance, statuses };
}

const noInput = z.object({});

/**
 * review → { approve | escalate } → notify
 *
 * The two arms are guarded by complementary `when`s and `notify` joins on
 * whichever one ran. This is the shape the whole feature exists for.
 */
function approvalWorkflow(calls: string[], approved: boolean) {
  const review = defineStep<Record<string, never>, { approved: boolean }, Ctx>({
    type: 'b:review',
    workflowInputSchema: noInput,
    outputSchema: z.object({ approved: z.boolean() }),
    handler: async () => {
      calls.push('review');
      return { approved };
    },
  });

  const approve = defineStep<Record<string, never>, { via: string }, Ctx, { review: typeof review }>({
    type: 'b:approve',
    workflowInputSchema: noInput,
    outputSchema: z.object({ via: z.string() }),
    dependencies: { review },
    when: (ctx) => ctx.deps.review.approved,
    handler: async () => {
      calls.push('approve');
      return { via: 'approve' };
    },
  });

  const escalate = defineStep<Record<string, never>, { via: string }, Ctx, { review: typeof review }>({
    type: 'b:escalate',
    workflowInputSchema: noInput,
    outputSchema: z.object({ via: z.string() }),
    dependencies: { review },
    when: (ctx) => !ctx.deps.review.approved,
    handler: async () => {
      calls.push('escalate');
      return { via: 'escalate' };
    },
  });

  const notify = defineStep<
    Record<string, never>,
    { notified: string },
    Ctx,
    { approve: typeof approve; escalate: typeof escalate },
    'any'
  >({
    type: 'b:notify',
    workflowInputSchema: noInput,
    outputSchema: z.object({ notified: z.string() }),
    dependencies: { approve, escalate },
    join: 'any',
    handler: async (ctx) => {
      calls.push('notify');
      // Exactly one arm ran — the other is absent, and the type says so.
      const via = ctx.deps.approve?.via ?? ctx.deps.escalate?.via ?? 'none';
      return { notified: via };
    },
  });

  return buildWorkflow<Record<string, never>, Ctx>({
    type: 'branching',
    inputSchema: noInput,
    steps: { review, approve, escalate, notify },
  });
}

describe('conditional branching', () => {
  it('runs the arm whose guard holds and skips the other, then joins on the winner', async () => {
    for (const approved of [true, false]) {
      const calls: string[] = [];
      const h = harness();
      const wf = approvalWorkflow(calls, approved);
      wf.register(h.registry);

      const started = await wf.start(h.engine, {});
      expect(started.ok).toBe(true);
      if (!started.ok) return;
      await h.drain();

      const taken = approved ? 'approve' : 'escalate';
      const untaken = approved ? 'escalate' : 'approve';
      expect(calls).toEqual(['review', taken, 'notify']);

      const status = await h.statuses(started.value.workflowId);
      expect(status[taken]).toBe('completed');
      expect(status[untaken]).toBe('skipped');
      expect(status.notify).toBe('completed');

      const final = await h.engine.getWorkflowStatus(started.value.workflowId);
      expect(final.ok && final.value.status).toBe('completed');
      // The skipped arm contributes nothing to the aggregated output.
      expect(final.ok && final.value.output).toEqual({
        review: { approved },
        [taken]: { via: taken },
        notify: { notified: taken },
      });
    }
  });

  it('skips everything reachable only through a step whose guard said no', async () => {
    const h = harness();
    const gate = defineStep<Record<string, never>, { go: boolean }, Ctx>({
      type: 'c:gate',
      workflowInputSchema: noInput,
      outputSchema: z.object({ go: z.boolean() }),
      handler: async () => ({ go: false }),
    });
    const first = defineStep<Record<string, never>, Record<string, never>, Ctx, { gate: typeof gate }>({
      type: 'c:first',
      workflowInputSchema: noInput,
      outputSchema: z.object({}) as unknown as z.ZodType<Record<string, never>>,
      dependencies: { gate },
      when: (ctx) => ctx.deps.gate.go,
      handler: async () => ({}) as Record<string, never>,
    });
    const second = defineStep<Record<string, never>, Record<string, never>, Ctx, { first: typeof first }>({
      type: 'c:second',
      workflowInputSchema: noInput,
      outputSchema: z.object({}) as unknown as z.ZodType<Record<string, never>>,
      dependencies: { first },
      handler: async () => ({}) as Record<string, never>,
    });
    const wf = buildWorkflow<Record<string, never>, Ctx>({
      type: 'cascade',
      inputSchema: noInput,
      steps: { gate, first, second },
    });
    wf.register(h.registry);

    const started = await wf.start(h.engine, {});
    if (!started.ok) throw new Error('start failed');
    await h.drain();

    expect(await h.statuses(started.value.workflowId)).toEqual({
      gate: 'completed',
      first: 'skipped',
      second: 'skipped',
    });
    // A workflow that skipped its tail is still a success, not a failure.
    const final = await h.engine.getWorkflowStatus(started.value.workflowId);
    expect(final.ok && final.value.status).toBe('completed');
  });

  it('records the skip on the step and emits it', async () => {
    const observer = createRecordingObserver();
    const h = harness({ observer });
    const calls: string[] = [];
    const wf = approvalWorkflow(calls, true);
    wf.register(h.registry);

    const started = await wf.start(h.engine, {});
    if (!started.ok) throw new Error('start failed');
    await h.drain();

    const escalate = (await h.store.listSteps(started.value.workflowId)).find((s) => s.key === 'escalate');
    expect(escalate?.error).toBe('Skipped: condition not met');
    expect(observer.events.filter((e) => e.type === 'step.skipped').map((e) => e.stepKey)).toEqual(['escalate']);
  });

  it('retries a guard that throws a transient error instead of pruning the branch', async () => {
    const h = harness();
    let guardCalls = 0;
    let ran = false;

    const flaky = defineStep<Record<string, never>, { ok: boolean }, Ctx>({
      type: 'g:flaky',
      workflowInputSchema: noInput,
      outputSchema: z.object({ ok: z.boolean() }),
      retry: { maxAttempts: 3, initialDelayMs: 0 },
      when: () => {
        guardCalls++;
        if (guardCalls < 3) throw retryableError('guard lookup failed');
        return true;
      },
      handler: async () => {
        ran = true;
        return { ok: true };
      },
    });
    const wf = buildWorkflow<Record<string, never>, Ctx>({ type: 'flaky-guard', inputSchema: noInput, steps: { flaky } });
    wf.register(h.registry);

    const started = await wf.start(h.engine, {});
    if (!started.ok) throw new Error('start failed');
    await h.drain();

    expect(guardCalls).toBe(3);
    expect(ran).toBe(true);
    expect((await h.statuses(started.value.workflowId)).flaky).toBe('completed');
  });

  it('skips a guarded wait step without ever suspending it', async () => {
    const h = harness();
    const decide = defineStep<Record<string, never>, { needsApproval: boolean }, Ctx>({
      type: 'w:decide',
      workflowInputSchema: noInput,
      outputSchema: z.object({ needsApproval: z.boolean() }),
      handler: async () => ({ needsApproval: false }),
    });
    const approval = defineWaitStep<{ approved: boolean }, Ctx, { decide: typeof decide }>({
      type: 'w:approval',
      outputSchema: z.object({ approved: z.boolean() }),
      dependencies: { decide },
      when: (ctx) => ctx.deps.decide.needsApproval,
    });
    const wf = buildWorkflow<Record<string, never>, Ctx>({
      type: 'guarded-wait',
      inputSchema: noInput,
      steps: { decide, approval },
    });
    wf.register(h.registry);

    const started = await wf.start(h.engine, {});
    if (!started.ok) throw new Error('start failed');
    await h.drain();

    expect((await h.statuses(started.value.workflowId)).approval).toBe('skipped');
    const final = await h.engine.getWorkflowStatus(started.value.workflowId);
    expect(final.ok && final.value.status).toBe('completed');
  });

  it('does not re-enqueue an unrelated queued step when a branch is skipped', async () => {
    const h = harness();
    const gate = defineStep<Record<string, never>, { go: boolean }, Ctx>({
      type: 'd:gate',
      workflowInputSchema: noInput,
      outputSchema: z.object({ go: z.boolean() }),
      handler: async () => ({ go: false }),
    });
    // Held in the queue, so it is still `pending` when the guard below runs.
    const slow = defineStep<Record<string, never>, Record<string, never>, Ctx, { gate: typeof gate }>({
      type: 'd:slow',
      workflowInputSchema: noInput,
      outputSchema: z.object({}) as unknown as z.ZodType<Record<string, never>>,
      dependencies: { gate },
      delayMs: 60_000,
      handler: async () => ({}) as Record<string, never>,
    });
    const guarded = defineStep<Record<string, never>, Record<string, never>, Ctx, { gate: typeof gate }>({
      type: 'd:guarded',
      workflowInputSchema: noInput,
      outputSchema: z.object({}) as unknown as z.ZodType<Record<string, never>>,
      dependencies: { gate },
      when: (ctx) => ctx.deps.gate.go,
      handler: async () => ({}) as Record<string, never>,
    });
    const wf = buildWorkflow<Record<string, never>, Ctx>({
      type: 'skip-dispatch',
      inputSchema: noInput,
      steps: { gate, slow, guarded },
    });
    wf.register(h.registry);

    const started = await wf.start(h.engine, {});
    if (!started.ok) throw new Error('start failed');
    await h.drain(); // gate completes, both branches enqueue, `guarded` is skipped

    // A skip can only unblock a step that *depends* on it. Re-driving the whole
    // ready frontier instead would put a second job on the queue for `slow`,
    // which is still sitting there waiting out its delay.
    //
    // (This is narrower than "never enqueue twice": a completion always
    // re-dispatches every ready-and-pending step, which is safe because
    // claiming is atomic. The skip path is what must not add to that.)
    expect((await h.statuses(started.value.workflowId)).guarded).toBe('skipped');
    expect(h.dispatched.filter((job) => job.stepKey === 'slow')).toHaveLength(1);
  });

  it('still dispatches a join that the skip itself unblocked', async () => {
    const h = harness();
    const calls: string[] = [];
    const wf = approvalWorkflow(calls, true);
    wf.register(h.registry);

    const started = await wf.start(h.engine, {});
    if (!started.ok) throw new Error('start failed');
    await h.drain();

    // `notify` becomes runnable only once `escalate` settles, and `escalate`
    // settles by being skipped — so the skip is the only thing that can have
    // dispatched it.
    expect(calls).toEqual(['review', 'approve', 'notify']);
    expect(h.dispatched.some((job) => job.stepKey === 'notify')).toBe(true);
    expect((await h.statuses(started.value.workflowId)).notify).toBe('completed');
  });

  it('suspends a guarded wait step when the guard holds', async () => {
    const h = harness();
    const decide = defineStep<Record<string, never>, { needsApproval: boolean }, Ctx>({
      type: 'w2:decide',
      workflowInputSchema: noInput,
      outputSchema: z.object({ needsApproval: z.boolean() }),
      handler: async () => ({ needsApproval: true }),
    });
    const approval = defineWaitStep<{ approved: boolean }, Ctx, { decide: typeof decide }>({
      type: 'w2:approval',
      outputSchema: z.object({ approved: z.boolean() }),
      dependencies: { decide },
      when: (ctx) => ctx.deps.decide.needsApproval,
    });
    const wf = buildWorkflow<Record<string, never>, Ctx>({
      type: 'guarded-wait-taken',
      inputSchema: noInput,
      steps: { decide, approval },
    });
    wf.register(h.registry);

    const started = await wf.start(h.engine, {});
    if (!started.ok) throw new Error('start failed');
    await h.drain();
    expect((await h.statuses(started.value.workflowId)).approval).toBe('waiting');

    await h.engine.resumeStep(started.value.workflowId, 'approval', { approved: true });
    await h.drain();

    const final = await h.engine.getWorkflowStatus(started.value.workflowId);
    expect(final.ok && final.value.status).toBe('completed');
  });
});
