import { describe, it, expect, vi } from 'vitest';
import { z } from 'zod';
import { createWorkflowEngine } from './engine';
import { createInMemoryWorkflowStore } from './in-memory-store';
import { createStepHandlerRegistry } from './registry';
import { defineStep, buildWorkflow } from './defineStep';
import type { Dispatcher, DispatchStepPayload } from './dispatcher';
import type { WorkflowEngineConfig } from './engine';

type Ctx = undefined;

/**
 * The same clock-honouring harness the recovery tests use. Most cases here run
 * with `heartbeat: 'manual'` and beat explicitly, so nothing depends on real
 * timers; the one automatic-beat case uses vitest's fake timers.
 */
function harness(opts?: { config?: WorkflowEngineConfig }) {
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

  return { store, registry, engine, queue, drain, advance, clock };
}

/** A promise plus its resolver — used to hold a handler open at a known point. */
function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

const noInput = z.object({});
const emptyOut = z.object({}) as unknown as z.ZodType<Record<string, never>>;

/** Past the default 15-minute stuck threshold. */
const PAST_DEFAULT_THRESHOLD_MS = (600 + 300 + 60) * 1000;

/**
 * Start a workflow whose only step is claimed and then abandoned mid-run, the
 * way a worker that died would leave it. Returns the ids plus a way to beat on
 * its behalf.
 */
async function strandedStep(
  h: ReturnType<typeof harness>,
  options: { type: string; heartbeatTimeoutMs?: number; maxAttempts?: number },
) {
  const step = defineStep<Record<string, never>, Record<string, never>, Ctx>({
    type: options.type,
    workflowInputSchema: noInput,
    outputSchema: emptyOut,
    retry: { maxAttempts: options.maxAttempts ?? 1, initialDelayMs: 0 },
    heartbeatTimeoutMs: options.heartbeatTimeoutMs,
    heartbeat: 'manual',
    handler: async () => ({}) as Record<string, never>,
  });
  const wf = buildWorkflow<Record<string, never>, Ctx>({ type: options.type, inputSchema: noInput, steps: { step } });
  wf.register(h.registry);

  const started = await wf.start(h.engine, {});
  if (!started.ok) throw new Error('start failed');
  const row = (await h.store.listSteps(started.value.workflowId))[0]!;
  await h.store.markStepRunning(row.id, h.clock.at.toISOString());
  h.queue.length = 0;

  return { workflowId: started.value.workflowId, stepId: row.id };
}

describe('heartbeats and the stuck-step sweeper', () => {
  it('spares a step that keeps reporting in, long past the default threshold', async () => {
    const h = harness();
    const { workflowId, stepId } = await strandedStep(h, { type: 'hb:alive', heartbeatTimeoutMs: 60_000 });

    // Well past the 15 minutes that would condemn a silent step, but it has
    // spoken within its own 60s window.
    h.advance(PAST_DEFAULT_THRESHOLD_MS);
    expect(await h.store.heartbeatStep(stepId, h.clock.at.toISOString())).toBe(true);

    const swept = await h.engine.recoverStuckWorkflows();
    expect(swept).toMatchObject({ retriedSteps: 0, recoveredSteps: 0 });
    expect((await h.store.getStep(stepId))?.status).toBe('running');
    expect((await h.store.getWorkflow(workflowId))?.status).toBe('running');
  });

  it('recovers a silent step at its own short window, not the global one', async () => {
    const h = harness();
    const { workflowId } = await strandedStep(h, { type: 'hb:silent', heartbeatTimeoutMs: 60_000 });

    // Only a minute in — nowhere near the 15-minute default, but past its window.
    h.advance(61_000);
    const swept = await h.engine.recoverStuckWorkflows();
    expect(swept).toMatchObject({ retriedSteps: 0, recoveredSteps: 1 });

    const final = await h.engine.getWorkflowStatus(workflowId);
    expect(final.ok && final.value.status).toBe('failed');
    expect(final.ok && final.value.error).toContain('went silent');
  });

  it('leaves a step that declares no window on the old wall-clock behaviour', async () => {
    const h = harness();
    const { stepId } = await strandedStep(h, { type: 'hb:none' });

    h.advance(61_000);
    expect((await h.engine.recoverStuckWorkflows()).recoveredSteps).toBe(0);
    expect((await h.store.getStep(stepId))?.status).toBe('running');

    h.advance(PAST_DEFAULT_THRESHOLD_MS);
    expect((await h.engine.recoverStuckWorkflows()).recoveredSteps).toBe(1);
  });

  it('measures a beating step from its last beat, not from when it started', async () => {
    const h = harness();
    const { stepId } = await strandedStep(h, { type: 'hb:sliding', heartbeatTimeoutMs: 60_000 });

    // Three windows' worth of elapsed time, but never two windows of silence.
    for (let i = 0; i < 3; i++) {
      h.advance(50_000);
      expect(await h.store.heartbeatStep(stepId, h.clock.at.toISOString())).toBe(true);
      expect((await h.engine.recoverStuckWorkflows()).recoveredSteps).toBe(0);
    }

    h.advance(61_000);
    expect((await h.engine.recoverStuckWorkflows()).recoveredSteps).toBe(1);
  });

  it('re-queues a silent step when its attempt budget allows', async () => {
    const h = harness();
    const { stepId } = await strandedStep(h, { type: 'hb:retry', heartbeatTimeoutMs: 60_000, maxAttempts: 3 });

    h.advance(61_000);
    expect((await h.engine.recoverStuckWorkflows()).retriedSteps).toBe(1);
    expect((await h.store.getStep(stepId))?.status).toBe('pending');
  });
});

describe('the heartbeat as a cancellation channel', () => {
  it('refuses a beat once the workflow is cancelled', async () => {
    const h = harness();
    const { workflowId, stepId } = await strandedStep(h, { type: 'hb:cancel', heartbeatTimeoutMs: 60_000 });

    expect(await h.store.heartbeatStep(stepId, h.clock.at.toISOString())).toBe(true);
    await h.engine.cancelWorkflow(workflowId);
    expect(await h.store.heartbeatStep(stepId, h.clock.at.toISOString())).toBe(false);
  });

  it('refuses a beat once the sweeper has given the step to someone else', async () => {
    const h = harness();
    const { stepId } = await strandedStep(h, { type: 'hb:superseded', heartbeatTimeoutMs: 60_000, maxAttempts: 3 });

    h.advance(61_000);
    await h.engine.recoverStuckWorkflows(); // → back to `pending`, no longer ours
    expect(await h.store.heartbeatStep(stepId, h.clock.at.toISOString())).toBe(false);
  });

  it('aborts the handler and discards its outcome when the run is cancelled mid-step', async () => {
    const h = harness();
    let sawAbort = false;
    const inHandler = deferred();
    const blocked = deferred();

    const slow = defineStep<Record<string, never>, { done: boolean }, Ctx>({
      type: 'hb:abort',
      workflowInputSchema: noInput,
      outputSchema: z.object({ done: z.boolean() }),
      heartbeatTimeoutMs: 60_000,
      heartbeat: 'manual',
      handler: async (ctx) => {
        inHandler.resolve();
        await blocked.promise;
        // The beat is how a handler asks whether it still has a job to do.
        const keepGoing = await ctx.heartbeat();
        sawAbort = ctx.signal?.aborted === true;
        if (!keepGoing) return { done: false };
        return { done: true };
      },
    });
    const wf = buildWorkflow<Record<string, never>, Ctx>({ type: 'hb-abort', inputSchema: noInput, steps: { slow } });
    wf.register(h.registry);

    const started = await wf.start(h.engine, {});
    if (!started.ok) throw new Error('start failed');
    const id = started.value.workflowId;

    const running = h.drain();
    await inHandler.promise; // the step is genuinely claimed and in flight

    await h.engine.cancelWorkflow(id);
    // Past the throttle, so the handler's beat actually writes.
    h.advance(30_000);
    blocked.resolve();
    await running;

    expect(sawAbort).toBe(true);
    const final = await h.engine.getWorkflowStatus(id);
    expect(final.ok && final.value.status).toBe('cancelled');
    // The superseded invocation must not have stamped an outcome on the step.
    expect(final.ok && final.value.steps[0]?.output).toBeNull();
  });
});

describe('automatic beating', () => {
  it('beats on a timer without the handler doing anything', async () => {
    vi.useFakeTimers();
    try {
      const h = harness();
      const inHandler = deferred();
      const blocked = deferred();

      const slow = defineStep<Record<string, never>, Record<string, never>, Ctx>({
        type: 'hb:auto',
        workflowInputSchema: noInput,
        outputSchema: emptyOut,
        heartbeatTimeoutMs: 30_000, // → a 10s beat interval
        handler: async () => {
          inHandler.resolve();
          await blocked.promise;
          return {} as Record<string, never>;
        },
      });
      const wf = buildWorkflow<Record<string, never>, Ctx>({ type: 'hb-auto', inputSchema: noInput, steps: { slow } });
      wf.register(h.registry);

      const started = await wf.start(h.engine, {});
      if (!started.ok) throw new Error('start failed');
      const id = started.value.workflowId;

      const running = h.drain();
      await inHandler.promise;

      const stepId = (await h.store.listSteps(id))[0]!.id;
      expect((await h.store.getStep(stepId))?.heartbeatAt).toBeNull();

      // Move both clocks: the engine reads `now()`, the timer reads the fake one.
      h.advance(11_000);
      await vi.advanceTimersByTimeAsync(11_000);

      expect((await h.store.getStep(stepId))?.heartbeatAt).toBe(h.clock.at.toISOString());

      blocked.resolve();
      await running;

      const final = await h.engine.getWorkflowStatus(id);
      expect(final.ok && final.value.status).toBe('completed');
    } finally {
      vi.useRealTimers();
    }
  });

  it('does not beat on a timer under heartbeat: "manual"', async () => {
    vi.useFakeTimers();
    try {
      const h = harness();
      const inHandler = deferred();
      const blocked = deferred();

      const slow = defineStep<Record<string, never>, Record<string, never>, Ctx>({
        type: 'hb:manual',
        workflowInputSchema: noInput,
        outputSchema: emptyOut,
        heartbeatTimeoutMs: 30_000,
        heartbeat: 'manual',
        handler: async () => {
          inHandler.resolve();
          await blocked.promise;
          return {} as Record<string, never>;
        },
      });
      const wf = buildWorkflow<Record<string, never>, Ctx>({ type: 'hb-manual', inputSchema: noInput, steps: { slow } });
      wf.register(h.registry);

      const started = await wf.start(h.engine, {});
      if (!started.ok) throw new Error('start failed');
      const id = started.value.workflowId;

      const running = h.drain();
      await inHandler.promise;

      h.advance(11_000);
      await vi.advanceTimersByTimeAsync(11_000);

      // Silence is the point: nothing beat on this step's behalf.
      const stepId = (await h.store.listSteps(id))[0]!.id;
      expect((await h.store.getStep(stepId))?.heartbeatAt).toBeNull();

      blocked.resolve();
      await running;
    } finally {
      vi.useRealTimers();
    }
  });
});

describe('a fresh claim', () => {
  it('clears the previous attempt’s beat', async () => {
    const h = harness();
    const { stepId } = await strandedStep(h, { type: 'hb:reclaim', heartbeatTimeoutMs: 60_000, maxAttempts: 3 });

    h.advance(30_000);
    await h.store.heartbeatStep(stepId, h.clock.at.toISOString());
    expect((await h.store.getStep(stepId))?.heartbeatAt).not.toBeNull();

    // The sweeper hands it back; the next claim must not inherit a stamp that is
    // older than its own start, or it would look stale the moment it begins.
    h.advance(61_000);
    await h.engine.recoverStuckWorkflows();
    await h.store.markStepRunning(stepId, h.clock.at.toISOString());

    const reclaimed = await h.store.getStep(stepId);
    expect(reclaimed?.heartbeatAt).toBeNull();
    expect((await h.engine.recoverStuckWorkflows()).recoveredSteps).toBe(0);
  });
});
