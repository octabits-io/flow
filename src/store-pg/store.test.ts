import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { z } from 'zod';
import { Pool } from 'pg';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import {
  createWorkflowEngine,
  createStepHandlerRegistry,
  defineStep,
  defineWaitStep,
  defineMapStep,
  defineSubWorkflowStep,
  buildWorkflow,
  type Dispatcher,
  type DispatchStepPayload,
} from '../core';
import type { FlowObserver } from '../core';
import { createPgWorkflowStore, applySchema, FLOW_STORE_DDL, FLOW_EVENT_DDL, createPgEventSink, readFlowEvents } from './index';

let container: StartedPostgreSqlContainer;
let pool: Pool;

beforeAll(async () => {
  container = await new PostgreSqlContainer('postgres:17-alpine').start();
  pool = new Pool({ connectionString: container.getConnectionUri() });
  await applySchema(pool, FLOW_STORE_DDL);
  await applySchema(pool, FLOW_EVENT_DDL);
});

afterAll(async () => {
  await pool?.end();
  await container?.stop();
});

type Ctx = undefined;

function harness(now?: () => Date, observer?: FlowObserver) {
  const store = createPgWorkflowStore({ pool, partitionKey: 'test' });
  const registry = createStepHandlerRegistry<Ctx>();
  const queue: DispatchStepPayload[] = [];
  const dispatcher: Dispatcher = { async enqueueStep(p) { queue.push(p); return { ok: true, value: undefined }; } };
  const engine = createWorkflowEngine<Ctx>({ store, registry, dispatcher, partitionKey: 'test', now, observer });
  async function drain() {
    let guard = 0;
    while (queue.length > 0) {
      if (++guard > 1000) throw new Error('runaway');
      const p = queue.shift()!;
      try {
        await engine.executeStep(p.workflowId, p.stepId);
      } catch {
        /* dispatcher would retry */
      }
    }
  }
  return { store, registry, engine, queue, drain };
}

describe('createPgWorkflowStore (integration)', () => {
  it('runs a linear DAG and aggregates output against real Postgres', async () => {
    const input = z.object({ seed: z.string() });
    const a = defineStep<{ seed: string }, { a: string }, Ctx>({
      type: 'pg:a', workflowInputSchema: input, outputSchema: z.object({ a: z.string() }),
      handler: async (ctx) => ({ a: ctx.workflowInput.seed + '-a' }),
    });
    const b = defineStep<{ seed: string }, { b: string }, Ctx, { a: typeof a }>({
      type: 'pg:b', workflowInputSchema: input, outputSchema: z.object({ b: z.string() }), dependencies: { a },
      handler: async (ctx) => ({ b: ctx.deps.a.a + '-b' }),
    });
    const wf = buildWorkflow<{ seed: string }, Ctx>({ type: 'pg-linear', inputSchema: input, steps: { a, b } });

    const h = harness();
    wf.register(h.registry);
    const started = await wf.start(h.engine, { seed: 'x' });
    expect(started.ok).toBe(true);
    if (!started.ok) return;
    expect(h.queue.length).toBe(1); // only root enqueued
    await h.drain();

    const status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('completed');
    expect(status.value.completedSteps).toBe(2);
    expect(status.value.output).toEqual({ a: { a: 'x-a' }, b: { b: 'x-a-b' } });
  });

  it('parallelizes roots and joins at a fan-in', async () => {
    const input = z.object({});
    const a = defineStep<{}, { v: number }, Ctx>({ type: 'pgf:a', workflowInputSchema: input, outputSchema: z.object({ v: z.number() }), handler: async () => ({ v: 1 }) });
    const b = defineStep<{}, { v: number }, Ctx>({ type: 'pgf:b', workflowInputSchema: input, outputSchema: z.object({ v: z.number() }), handler: async () => ({ v: 2 }) });
    const join = defineStep<{}, { sum: number }, Ctx, { a: typeof a; b: typeof b }>({
      type: 'pgf:join', workflowInputSchema: input, outputSchema: z.object({ sum: z.number() }), dependencies: { a, b },
      handler: async (ctx) => ({ sum: ctx.deps.a.v + ctx.deps.b.v }),
    });
    const wf = buildWorkflow<{}, Ctx>({ type: 'pg-fan', inputSchema: input, steps: { a, b, join } });

    const h = harness();
    wf.register(h.registry);
    const started = await wf.start(h.engine, {});
    if (!started.ok) return;
    expect(h.queue.length).toBe(2);
    await h.drain();

    const status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('completed');
    expect(status.value.output).toMatchObject({ join: { sum: 3 } });
  });

  it('cascades a failed step and fails the workflow', async () => {
    const input = z.object({});
    const a = defineStep<{}, { ok: boolean }, Ctx>({
      type: 'pgfail:a', workflowInputSchema: input, outputSchema: z.object({ ok: z.boolean() }),
      handler: async () => Promise.reject(new Error('kaboom')),
    });
    const b = defineStep<{}, { ok: boolean }, Ctx, { a: typeof a }>({
      type: 'pgfail:b', workflowInputSchema: input, outputSchema: z.object({ ok: z.boolean() }), dependencies: { a },
      handler: async () => ({ ok: true }),
    });
    const wf = buildWorkflow<{}, Ctx>({ type: 'pg-fail', inputSchema: input, steps: { a, b } });

    const h = harness();
    wf.register(h.registry);
    const started = await wf.start(h.engine, {});
    if (!started.ok) return;
    await h.drain();

    const status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('failed');
    expect(status.value.steps.find((s) => s.key === 'a')!.status).toBe('failed');
    expect(status.value.steps.find((s) => s.key === 'b')!.status).toBe('skipped');
  });

  it('recovers a stuck running step past the expiry', async () => {
    let current = new Date('2026-03-01T00:00:00.000Z');
    const input = z.object({});
    const a = defineStep<{}, { v: number }, Ctx>({ type: 'pgstuck:a', workflowInputSchema: input, outputSchema: z.object({ v: z.number() }), handler: async () => ({ v: 1 }) });
    const wf = buildWorkflow<{}, Ctx>({ type: 'pg-stuck', inputSchema: input, steps: { a } });

    const h = harness(() => current);
    wf.register(h.registry);
    const started = await wf.start(h.engine, {});
    if (!started.ok) return;

    // simulate a worker that marked running then crashed
    const job = h.queue.shift()!;
    await h.store.markStepRunning(job.stepId, current.toISOString());

    current = new Date(current.getTime() + (600 + 300 + 60) * 1000);
    const recovered = await h.engine.recoverStuckWorkflows();
    expect(recovered.recoveredSteps).toBe(1);

    const status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('failed');
  });

  it('lists workflows with their steps and filters by type', async () => {
    const list = await createPgWorkflowStore({ pool, partitionKey: 'test' }).listWorkflows({ type: 'pg-linear', limit: 10 });
    expect(list.length).toBeGreaterThan(0);
    expect(list[0]!.type).toBe('pg-linear');
    expect(list[0]!.steps.length).toBe(2);
  });

  it('cancels a workflow: pending steps become skipped in Postgres', async () => {
    const input = z.object({});
    const a = defineStep<{}, { v: number }, Ctx>({ type: 'pgcancel:a', workflowInputSchema: input, outputSchema: z.object({ v: z.number() }), handler: async () => ({ v: 1 }) });
    const b = defineStep<{}, { v: number }, Ctx, { a: typeof a }>({ type: 'pgcancel:b', workflowInputSchema: input, outputSchema: z.object({ v: z.number() }), dependencies: { a }, handler: async () => ({ v: 2 }) });
    const wf = buildWorkflow<{}, Ctx>({ type: 'pg-cancel', inputSchema: input, steps: { a, b } });

    const h = harness();
    wf.register(h.registry);
    const started = await wf.start(h.engine, {});
    if (!started.ok) return;
    // cancel before draining → all steps still pending
    const res = await h.engine.cancelWorkflow(started.value.workflowId);
    expect(res.ok).toBe(true);

    const status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('cancelled');
    expect(status.value.steps.every((s) => s.status === 'skipped')).toBe(true);
  });

  it('persists attempts and retries a flaky step to completion', async () => {
    const calls = { n: 0 };
    const input = z.object({});
    const flaky = defineStep<{}, { ok: boolean }, Ctx>({
      type: 'pgretry:flaky',
      workflowInputSchema: input,
      outputSchema: z.object({ ok: z.boolean() }),
      retry: { maxAttempts: 3, initialDelayMs: 1 },
      handler: async () => {
        calls.n += 1;
        if (calls.n < 2) throw new Error('429 rate limit');
        return { ok: true };
      },
    });
    const wf = buildWorkflow<{}, Ctx>({ type: 'pg-retry', inputSchema: input, steps: { flaky } });

    const h = harness();
    wf.register(h.registry);
    const started = await wf.start(h.engine, {});
    if (!started.ok) return;
    await h.drain();

    expect(calls.n).toBe(2);
    const status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('completed');
    expect(status.value.steps[0]!.attempts).toBe(2); // persisted across the retry re-delivery
  });

  it('dedupes a workflow start by idempotencyKey', async () => {
    const input = z.object({});
    const a = defineStep<{}, { v: number }, Ctx>({
      type: 'pgidem:a', workflowInputSchema: input, outputSchema: z.object({ v: z.number() }), handler: async () => ({ v: 1 }),
    });
    const wf = buildWorkflow<{}, Ctx>({ type: 'pg-idem', inputSchema: input, steps: { a } });

    const h = harness();
    wf.register(h.registry);
    const first = await wf.start(h.engine, {}, { idempotencyKey: 'k-1' });
    if (!first.ok) return;
    const second = await wf.start(h.engine, {}, { idempotencyKey: 'k-1' });
    if (!second.ok) return;

    expect(second.value.workflowId).toBe(first.value.workflowId); // same workflow
    expect(second.value.enqueuedSteps).toEqual([]); // roots not re-enqueued

    // exactly one workflow exists for that key
    const store = createPgWorkflowStore({ pool, partitionKey: 'test' });
    const list = await store.listWorkflows({ type: 'pg-idem', limit: 10 });
    expect(list.filter((w) => w.idempotencyKey === 'k-1').length).toBe(1);

    await h.drain();
  });

  it('suspends a wait step (status waiting) and resumes it on real Postgres', async () => {
    const input = z.object({});
    const a = defineStep<{}, { ok: boolean }, Ctx>({
      type: 'pgwait:a', workflowInputSchema: input, outputSchema: z.object({ ok: z.boolean() }), handler: async () => ({ ok: true }),
    });
    const gate = defineWaitStep<{ approved: boolean }, Ctx, { a: typeof a }>({
      type: 'pgwait:gate', outputSchema: z.object({ approved: z.boolean() }), dependencies: { a },
    });
    const done = defineStep<{}, { done: boolean }, Ctx, { gate: typeof gate }>({
      type: 'pgwait:done', workflowInputSchema: input, outputSchema: z.object({ done: z.boolean() }), dependencies: { gate }, handler: async () => ({ done: true }),
    });
    const wf = buildWorkflow<{}, Ctx>({ type: 'pg-wait', inputSchema: input, steps: { a, gate, done } });

    const h = harness();
    wf.register(h.registry);
    const started = await wf.start(h.engine, {});
    if (!started.ok) return;
    await h.drain();

    // gate is suspended (persisted as 'waiting'); workflow not yet complete
    let status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('running');
    expect(status.value.steps.find((s) => s.key === 'gate')!.status).toBe('waiting');

    const resumed = await h.engine.resumeStep(started.value.workflowId, 'gate', { approved: true });
    expect(resumed.ok).toBe(true);
    await h.drain();

    status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('completed');
    expect(status.value.output).toMatchObject({ gate: { approved: true }, done: { done: true } });
  });

  it('materializes map children at runtime and aggregates them on real Postgres', async () => {
    const input = z.object({});
    const mapper = defineMapStep<number, { sq: number }, {}, Ctx>({
      type: 'pgmap:m', workflowInputSchema: input, itemOutputSchema: z.object({ sq: z.number() }),
      items: () => [2, 3, 4],
      each: async (n) => ({ sq: n * n }),
    });
    const sum = defineStep<{}, { total: number }, Ctx, { mapper: typeof mapper }>({
      type: 'pgmap:sum', workflowInputSchema: input, outputSchema: z.object({ total: z.number() }), dependencies: { mapper },
      handler: async (ctx) => ({ total: ctx.deps.mapper.items.reduce((a, x) => a + x.sq, 0) }),
    });
    const wf = buildWorkflow<{}, Ctx>({ type: 'pg-map', inputSchema: input, steps: { mapper, sum } });

    const h = harness();
    wf.register(h.registry);
    const started = await wf.start(h.engine, {});
    if (!started.ok) return;
    await h.drain();

    const status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('completed');
    // child rows persisted with parent_step_id set
    expect(status.value.steps.filter((s) => s.parentStepId != null).length).toBe(3);
    expect(status.value.output).toMatchObject({
      mapper: { items: [{ sq: 4 }, { sq: 9 }, { sq: 16 }] },
      sum: { total: 29 },
    });
  });

  it('compensates completed steps in reverse order on failure (Postgres)', async () => {
    const input = z.object({});
    const events: string[] = [];
    const a = defineStep<{}, { a: number }, Ctx>({
      type: 'pgsaga:a', workflowInputSchema: input, outputSchema: z.object({ a: z.number() }),
      handler: async () => ({ a: 1 }), compensate: (ctx) => { events.push(`undo:a:${ctx.output.a}`); },
    });
    const b = defineStep<{}, { b: number }, Ctx, { a: typeof a }>({
      type: 'pgsaga:b', workflowInputSchema: input, outputSchema: z.object({ b: z.number() }), dependencies: { a },
      handler: async () => ({ b: 2 }), compensate: (ctx) => { events.push(`undo:b:${ctx.output.b}`); },
    });
    const c = defineStep<{}, { c: number }, Ctx, { b: typeof b }>({
      type: 'pgsaga:c', workflowInputSchema: input, outputSchema: z.object({ c: z.number() }), dependencies: { b },
      handler: async () => { throw new Error('c boom'); },
    });
    const wf = buildWorkflow<{}, Ctx>({ type: 'pg-saga', inputSchema: input, steps: { a, b, c } });

    const h = harness();
    wf.register(h.registry);
    const started = await wf.start(h.engine, {});
    if (!started.ok) return;
    await h.drain();

    const status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('failed');
    expect(events).toEqual(['undo:b:2', 'undo:a:1']);
    expect(status.value.steps.find((s) => s.key === 'a')!.status).toBe('compensated');
    expect(status.value.steps.find((s) => s.key === 'b')!.status).toBe('compensated');
  });

  it('persists a run-history event stream via the pg event sink', async () => {
    const input = z.object({});
    const a = defineStep<{}, { a: number }, Ctx>({
      type: 'pgobs:a', workflowInputSchema: input, outputSchema: z.object({ a: z.number() }), handler: async () => ({ a: 1 }),
    });
    const b = defineStep<{}, { b: number }, Ctx, { a: typeof a }>({
      type: 'pgobs:b', workflowInputSchema: input, outputSchema: z.object({ b: z.number() }), dependencies: { a }, handler: async () => ({ b: 2 }),
    });
    const wf = buildWorkflow<{}, Ctx>({ type: 'pg-obs', inputSchema: input, steps: { a, b } });

    const sink = createPgEventSink({ pool, partitionKey: 'test' });
    const h = harness(undefined, sink);
    wf.register(h.registry);
    const started = await wf.start(h.engine, {});
    if (!started.ok) return;
    await h.drain();
    await sink.flush();

    const events = await readFlowEvents(pool, { workflowId: started.value.workflowId, partitionKey: 'test' });
    const types = events.map((e) => e.type);
    expect(types[0]).toBe('workflow.started');
    expect(types[types.length - 1]).toBe('workflow.completed');
    expect(types.filter((t) => t === 'step.completed').length).toBe(2);
    const aDone = events.find((e) => e.type === 'step.completed' && e.stepKey === 'a')!;
    expect(aDone.attempt).toBe(1);
    expect(aDone.durationMs).not.toBeUndefined();
  });

  it('runs a sub-workflow child and bridges its output to the parent step on Postgres', async () => {
    const input = z.object({});
    const childInput = z.object({ n: z.number() });
    const triple = defineStep<{ n: number }, { tripled: number }, Ctx>({
      type: 'pgsub:triple', workflowInputSchema: childInput, outputSchema: z.object({ tripled: z.number() }),
      handler: async (ctx) => ({ tripled: ctx.workflowInput.n * 3 }),
    });
    const childWf = buildWorkflow<{ n: number }, Ctx>({ type: 'pgsub-child', inputSchema: childInput, steps: { triple } });

    const call = defineSubWorkflowStep<{ triple: { tripled: number } }, {}, Ctx>({
      type: 'pgsub:call', workflowInputSchema: input, childWorkflow: childWf, input: () => ({ n: 7 }),
      outputSchema: z.object({ triple: z.object({ tripled: z.number() }) }),
    });
    const after = defineStep<{}, { result: number }, Ctx, { call: typeof call }>({
      type: 'pgsub:after', workflowInputSchema: input, outputSchema: z.object({ result: z.number() }), dependencies: { call },
      handler: async (ctx) => ({ result: ctx.deps.call.triple.tripled }),
    });
    const parentWf = buildWorkflow<{}, Ctx>({ type: 'pgsub-parent', inputSchema: input, steps: { call, after } });

    const h = harness();
    parentWf.register(h.registry);
    const started = await parentWf.start(h.engine, {});
    if (!started.ok) return;
    await h.drain();

    const status = await h.engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('completed');
    expect(status.value.steps.find((s) => s.key === 'call')!.status).toBe('completed');
    expect(status.value.output).toMatchObject({ call: { triple: { tripled: 21 } }, after: { result: 21 } });

    // the child workflow row persisted with parent linkage
    const childList = await h.store.listWorkflows({ type: 'pgsub-child' });
    expect(childList.length).toBe(1);
    const child = childList[0]!;
    expect(child.parentWorkflowId).toBe(started.value.workflowId);
    expect(child.status).toBe('completed');
  });

  it('returns null for missing rows and filters by entityRef + status', async () => {
    const store = createPgWorkflowStore({ pool, partitionKey: 'test' });
    expect(await store.getStep(99999)).toBeNull();
    expect(await store.getWorkflow(99999)).toBeNull();

    const input = z.object({});
    const a = defineStep<{}, { v: number }, Ctx>({ type: 'pgentity:a', workflowInputSchema: input, outputSchema: z.object({ v: z.number() }), handler: async () => ({ v: 1 }) });
    const wf = buildWorkflow<{}, Ctx>({ type: 'pg-entity', inputSchema: input, steps: { a } });
    const h = harness();
    wf.register(h.registry);
    const started = await wf.start(h.engine, {}, { entityRef: 'listing:abc' });
    if (!started.ok) return;
    await h.drain();

    const byEntity = await store.listWorkflows({ entityRef: 'listing:abc', limit: 10 });
    expect(byEntity.map((w) => w.entityRef)).toContain('listing:abc');
    const byStatusType = await store.listWorkflows({ status: 'completed', type: 'pg-entity', limit: 10 });
    expect(byStatusType.length).toBeGreaterThan(0);
  });
});
