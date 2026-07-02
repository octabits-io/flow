import { describe, it, expect } from 'vitest';
import { z } from 'zod';
import type { LanguageModelV4 } from '@ai-sdk/provider';
import {
  createWorkflowEngine,
  createInMemoryWorkflowStore,
  createStepHandlerRegistry,
  type Dispatcher,
  type DispatchStepPayload,
} from '../core';
import { createCostEstimator, estimateCostMicros } from './cost';
import { createInstrumentedModel, createUsageAccumulator, type AccumulatedUsage } from './instrumented-model';
import { createAiWorkflowHooks, type AiUsageRecorder, type AiQuotaPolicy } from './hooks';
import { defineAiStep, buildAiWorkflow } from './define-ai-step';
import type { AiContext } from './context';

// A minimal fake model that reports fixed usage from doGenerate.
function fakeModel(modelId: string, usage = { input: 100, output: 50 }): LanguageModelV4 {
  return {
    specificationVersion: 'v4',
    provider: 'fake',
    modelId,
    supportedUrls: {},
    async doGenerate() {
      return {
        content: [{ type: 'text', text: 'ok' }],
        finishReason: 'stop',
        usage: { inputTokens: { total: usage.input }, outputTokens: { total: usage.output } },
        warnings: [],
      };
    },
    async doStream() {
      throw new Error('not used in tests');
    },
  } as unknown as LanguageModelV4;
}

describe('cost', () => {
  it('estimates known-model cost in microdollars', () => {
    // claude-sonnet-4-6: $3/M input, $15/M output → 100*3/1e6 + 50*15/1e6 = $0.00105 → 1050 micros
    const micros = estimateCostMicros({ inputTokens: 100, outputTokens: 50, cacheReadTokens: 0, cacheWriteTokens: 0 }, 'claude-sonnet-4-6');
    expect(micros).toBe(1050);
  });

  it('falls back to the priciest entry for unknown models', () => {
    const known = estimateCostMicros({ inputTokens: 1000, outputTokens: 0, cacheReadTokens: 0, cacheWriteTokens: 0 }, 'totally-unknown');
    // priciest default is opus-4-6 at $15/M input → 1000*15/1e6 = $0.015 → 15000 micros
    expect(known).toBe(15000);
  });

  it('honors an injected pricing table', () => {
    const estimate = createCostEstimator({
      pricing: { 'my-model': { inputPerMillion: 1, outputPerMillion: 2, cacheReadPerMillion: 0, cacheWritePerMillion: 0 } },
    });
    expect(estimate({ inputTokens: 1_000_000, outputTokens: 0, cacheReadTokens: 0, cacheWriteTokens: 0 }, 'my-model')).toBe(1_000_000);
  });
});

describe('instrumented model', () => {
  it('captures token usage transparently from doGenerate', async () => {
    const acc = createUsageAccumulator();
    const model = createInstrumentedModel(fakeModel('claude-haiku-4-5-20251001', { input: 200, output: 80 }), acc);

    // Call through the wrapped model as the AI SDK would.
    await (model as any).doGenerate({ prompt: [] });

    expect(acc.hasUsage()).toBe(true);
    expect(acc.get()).toMatchObject({ inputTokens: 200, outputTokens: 80, modelId: 'claude-haiku-4-5-20251001' });
  });

  it('accumulates across multiple calls', async () => {
    const acc = createUsageAccumulator();
    const model = createInstrumentedModel(fakeModel('claude-sonnet-4-6', { input: 10, output: 5 }), acc);
    await (model as any).doGenerate({ prompt: [] });
    await (model as any).doGenerate({ prompt: [] });
    expect(acc.get()).toMatchObject({ inputTokens: 20, outputTokens: 10 });
  });
});

describe('createAiWorkflowHooks', () => {
  function recorder() {
    const stepCalls: Array<{ workflowId: number; stepId: number; usage: AccumulatedUsage; costMicros: number }> = [];
    const wfCalls: Array<{ workflowId: number; usage: AccumulatedUsage; costMicros: number }> = [];
    const dailyCalls: Array<{ workflowId: number; workflowType: string; keySource: string; date: string }> = [];
    const rec: AiUsageRecorder = {
      async recordStepUsage(a) {
        stepCalls.push(a);
      },
      async incrementWorkflowUsage(a) {
        wfCalls.push(a);
      },
      async recordWorkflowDaily(a) {
        dailyCalls.push(a);
      },
    };
    return { rec, stepCalls, wfCalls, dailyCalls };
  }

  it('rejects start when the quota policy rejects', async () => {
    const quotaPolicy: AiQuotaPolicy = {
      async checkQuota() {
        return { ok: false, error: { key: 'ai_quota_exceeded', message: 'too many' } };
      },
    };
    const hooks = createAiWorkflowHooks({
      modelResolver: { resolveModel: () => fakeModel('claude-sonnet-4-6') },
      usageRecorder: recorder().rec,
      quotaPolicy,
    });
    const res = await hooks.onBeforeStart!({ definition: { type: 't', steps: [] }, input: {} });
    expect(res.ok).toBe(false);
    if (res.ok) return;
    expect(res.error.key).toBe('ai_quota_exceeded');
  });

  it('stamps keySource metadata on start', async () => {
    const hooks = createAiWorkflowHooks({
      modelResolver: { resolveModel: () => fakeModel('claude-sonnet-4-6'), resolveKeySource: () => 'tenant' },
      usageRecorder: recorder().rec,
    });
    const res = await hooks.onBeforeStart!({ definition: { type: 't', steps: [] }, input: {} });
    expect(res.ok).toBe(true);
    if (!res.ok) return;
    expect(res.value.metadata).toEqual({ keySource: 'tenant' });
  });

  it('records usage + cost in onAfterStep', async () => {
    const r = recorder();
    const hooks = createAiWorkflowHooks({
      modelResolver: { resolveModel: () => fakeModel('claude-sonnet-4-6') },
      usageRecorder: r.rec,
    });
    const acc = createUsageAccumulator();
    acc.record({ inputTokens: 100, outputTokens: 50, cacheReadTokens: 0, cacheWriteTokens: 0, modelId: 'claude-sonnet-4-6' });
    const context: AiContext = { model: fakeModel('claude-sonnet-4-6'), host: undefined, usage: acc };

    await hooks.onAfterStep!({
      workflowId: 1,
      stepId: 2,
      partitionKey: 'p',
      workflow: {} as any,
      step: {} as any,
      output: {},
      context,
    });

    expect(r.stepCalls).toHaveLength(1);
    expect(r.stepCalls[0]).toMatchObject({ workflowId: 1, stepId: 2, costMicros: 1050 });
    expect(r.wfCalls[0]).toMatchObject({ costMicros: 1050 });
  });

  it('rolls up daily usage on completion using keySource from metadata', async () => {
    const r = recorder();
    const hooks = createAiWorkflowHooks({
      modelResolver: { resolveModel: () => fakeModel('claude-sonnet-4-6') },
      usageRecorder: r.rec,
      now: () => new Date('2026-02-03T10:00:00.000Z'),
    });
    await hooks.onWorkflowCompleted!({
      workflowId: 7,
      partitionKey: 'p',
      workflow: { type: 'listing-gen', metadata: { keySource: 'platform' } } as any,
    });
    expect(r.dailyCalls[0]).toEqual({ workflowId: 7, workflowType: 'listing-gen', keySource: 'platform', date: '2026-02-03' });
  });
});

describe('end-to-end: AI workflow through flow-core', () => {
  it('instruments the model, records step usage, and rolls up on completion', async () => {
    const stepUsage: Array<{ stepId: number; costMicros: number }> = [];
    const daily: Array<{ workflowType: string }> = [];
    const usageRecorder: AiUsageRecorder = {
      async recordStepUsage(a) {
        stepUsage.push({ stepId: a.stepId, costMicros: a.costMicros });
      },
      async incrementWorkflowUsage() {},
      async recordWorkflowDaily(a) {
        daily.push({ workflowType: a.workflowType });
      },
    };

    type Host = { tenantName: string };
    const hooks = createAiWorkflowHooks<Host>({
      modelResolver: {
        resolveModel: () => fakeModel('claude-sonnet-4-6'),
        resolveHost: () => ({ tenantName: 'acme' }),
        resolveKeySource: () => 'platform',
      },
      usageRecorder,
    });

    const input = z.object({ topic: z.string() });
    let seenHost = '';
    const generate = defineAiStep<{ topic: string }, { text: string }, Host>({
      type: 'ai:generate',
      workflowInputSchema: input,
      outputSchema: z.object({ text: z.string() }),
      handler: async (ctx) => {
        seenHost = ctx.context.host.tenantName;
        // use the instrumented model exactly as a real handler would
        await (ctx.context.model as any).doGenerate({ prompt: [] });
        return { text: `about ${ctx.workflowInput.topic}` };
      },
    });
    const wf = buildAiWorkflow<{ topic: string }, Host>({ type: 'gen', inputSchema: input, steps: { generate } });

    const store = createInMemoryWorkflowStore();
    const registry = createStepHandlerRegistry<AiContext<Host>>();
    wf.register(registry);
    const queue: DispatchStepPayload[] = [];
    const dispatcher: Dispatcher = { async enqueueStep(p) { queue.push(p); return { ok: true, value: undefined }; } };
    const engine = createWorkflowEngine<AiContext<Host>>({ store, registry, dispatcher, partitionKey: 'acme', hooks });

    const started = await wf.start(engine, { topic: 'rentals' });
    expect(started.ok).toBe(true);
    if (!started.ok) return;

    // drain
    while (queue.length > 0) {
      const p = queue.shift()!;
      await engine.executeStep(p.workflowId, p.stepId);
    }

    const status = await engine.getWorkflowStatus(started.value.workflowId);
    if (!status.ok) return;
    expect(status.value.status).toBe('completed');
    expect(seenHost).toBe('acme');
    expect(stepUsage).toHaveLength(1);
    expect(stepUsage[0]!.costMicros).toBe(1050); // 100 in / 50 out on sonnet-4-6
    expect(daily).toEqual([{ workflowType: 'gen' }]);
    // keySource was stamped at start
    expect(status.value.metadata).toEqual({ keySource: 'platform' });
  });
});

// A fake model whose doStream emits a `finish` chunk carrying usage.
function streamingModel(modelId: string, usage = { input: 30, output: 12 }): LanguageModelV4 {
  return {
    specificationVersion: 'v4',
    provider: 'fake',
    modelId,
    supportedUrls: {},
    async doGenerate() {
      throw new Error('not used');
    },
    async doStream() {
      const stream = new ReadableStream({
        start(c) {
          c.enqueue({ type: 'text-delta', id: '1', delta: 'hi' });
          c.enqueue({ type: 'finish', finishReason: 'stop', usage: { inputTokens: { total: usage.input }, outputTokens: { total: usage.output } } });
          c.close();
        },
      });
      return { stream };
    },
  } as unknown as LanguageModelV4;
}

describe('instrumented model — streaming', () => {
  it('captures usage from the finish chunk of a stream', async () => {
    const acc = createUsageAccumulator();
    const model = createInstrumentedModel(streamingModel('claude-sonnet-4-6', { input: 30, output: 12 }), acc);
    const res = await (model as any).doStream({ prompt: [] });
    const reader = res.stream.getReader();
    // drain the wrapped stream so the middleware's transform sees the finish chunk
    for (;;) {
      const { done } = await reader.read();
      if (done) break;
    }
    expect(acc.hasUsage()).toBe(true);
    expect(acc.get()).toMatchObject({ inputTokens: 30, outputTokens: 12, modelId: 'claude-sonnet-4-6' });
  });
});

describe('createAiWorkflowHooks — optional branches', () => {
  const minimalRecorder = (): AiUsageRecorder => ({ async recordStepUsage() {}, async incrementWorkflowUsage() {} });

  it('onAfterStep does nothing when no usage was recorded', async () => {
    const calls: string[] = [];
    const recorder: AiUsageRecorder = {
      async recordStepUsage() { calls.push('step'); },
      async incrementWorkflowUsage() { calls.push('wf'); },
    };
    const hooks = createAiWorkflowHooks({ modelResolver: { resolveModel: () => fakeModel('claude-sonnet-4-6') }, usageRecorder: recorder });
    const acc = createUsageAccumulator(); // empty
    await hooks.onAfterStep!({ workflowId: 1, stepId: 1, partitionKey: 'p', workflow: {} as any, step: {} as any, output: {}, context: { model: fakeModel('claude-sonnet-4-6'), host: undefined, usage: acc } });
    expect(calls).toEqual([]);
  });

  it('onWorkflowCompleted is a no-op without recordWorkflowDaily', async () => {
    const hooks = createAiWorkflowHooks({ modelResolver: { resolveModel: () => fakeModel('claude-sonnet-4-6') }, usageRecorder: minimalRecorder() });
    await expect(
      hooks.onWorkflowCompleted!({ workflowId: 1, partitionKey: 'p', workflow: { type: 't', metadata: null } as any }),
    ).resolves.toBeUndefined();
  });

  it('onBeforeStart yields empty metadata without quota policy or keySource', async () => {
    const hooks = createAiWorkflowHooks({ modelResolver: { resolveModel: () => fakeModel('claude-sonnet-4-6') }, usageRecorder: minimalRecorder() });
    const res = await hooks.onBeforeStart!({ definition: { type: 't', steps: [] }, input: {} });
    expect(res.ok).toBe(true);
    if (!res.ok) return;
    expect(res.value.metadata).toBeUndefined();
  });

  it('buildStepContext defaults host to undefined without resolveHost', async () => {
    const hooks = createAiWorkflowHooks({ modelResolver: { resolveModel: () => fakeModel('claude-sonnet-4-6') }, usageRecorder: minimalRecorder() });
    const c = await hooks.buildStepContext!({ workflowId: 1, stepId: 1, stepKey: 'k', partitionKey: 'p', workflow: {} as any, step: {} as any });
    expect(c.host).toBeUndefined();
    expect(c.model).toBeDefined();
    expect(c.usage.hasUsage()).toBe(false);
  });
});
