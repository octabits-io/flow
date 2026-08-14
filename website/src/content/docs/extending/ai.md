---
title: The AI add-on
description: Instrumented models, token and cost capture, quota enforcement and usage rollups.
---

`octaflow/ai` wires model instrumentation, token/cost capture, quota, and daily usage
rollups into the engine's lifecycle hooks — the core stays AI-free.

```ts
import { z } from 'zod';
import { generateText } from 'ai';
import { createWorkflowEngine, createStepHandlerRegistry, createInMemoryWorkflowStore } from 'octaflow';
import { defineAiStep, buildAiWorkflow, createAiWorkflowHooks } from 'octaflow/ai';

const summarize = defineAiStep({
  type: 'summarize',
  workflowInputSchema: z.object({ text: z.string() }),
  outputSchema: z.object({ summary: z.string() }),
  retry: { maxAttempts: 3 }, // provider 429s
  handler: async (ctx) => {
    const { text } = await generateText({ model: ctx.context.model, prompt: `Summarize: ${ctx.workflowInput.text}` });
    return { summary: text };
  },
});

const hooks = createAiWorkflowHooks({
  modelResolver: {
    resolveModel: () => myModel,          // your LanguageModelV4
    // resolveHost: (args) => container.scope(args),   // optional → ctx.context.host
    // resolveKeySource: () => 'platform',             // optional → stamped on workflow metadata
  },
  usageRecorder: { recordStepUsage: async () => {}, incrementWorkflowUsage: async () => {} },
  // quotaPolicy: { checkQuota: async () => ({ ok: true, value: undefined }) },
  // costEstimator: createCostEstimator({ … }),        // defaults to DEFAULT_MODEL_PRICING
});

const store = createInMemoryWorkflowStore();
const registry = createStepHandlerRegistry();
const engine = createWorkflowEngine({ store, dispatcher, registry, partitionKey: 'tenant-1', hooks });
```

`ctx.context.model` is an **instrumented** model — token usage is captured automatically and the
`onAfterStep` hook turns it into cost via a pluggable pricing table (`createCostEstimator`).
`ctx.context.host` is whatever `modelResolver.resolveHost` returns (a DI scope, domain
services), or `undefined` if you don't supply one.
→ [`examples/13-ai-workflow.ts`](https://github.com/octabits-io/octaflow/blob/main/examples/13-ai-workflow.ts)

## Quota enforcement & usage aggregation

Two **store-agnostic** engines complete the add-on. They own the window math, the enforcement
order, and the rollup shapes; the raw counts and aggregate reads come through narrow store seams
(`AiQuotaStore` / `AiUsageStore`) that you implement with your own SQL — flow never touches a
database (and the `ai` layer never depends on `pg`). Scoping is generic (`partitionKey`) and
`keySource` is a free-form string, so both engines stay tenancy-agnostic.

```ts
import { createAiQuotaService, createAiUsageAggregationService, DEFAULT_AI_QUOTA } from 'octaflow/ai';

// Quota: limits come from an injected getQuota(partitionKey) callback — return
// null to exempt a scope entirely (e.g. bring-your-own-key), or null on any
// single window to leave it unlimited.
const quota = createAiQuotaService({
  store,                                   // your AiQuotaStore (count reads)
  getQuota: (partitionKey) => DEFAULT_AI_QUOTA,
});
const gate = await quota.checkQuota('tenant-1');  // Result<void, AiQuotaExceededError>
// wire it into the hooks: quotaPolicy: { checkQuota: () => quota.checkQuota('tenant-1') }

// Usage aggregation: roll completed workflows (and embedding batches) into a
// daily table, then read summaries back.
const usage = createAiUsageAggregationService({ store });  // your AiUsageStore (UPSERT + aggregate reads)
await usage.recordWorkflowCompletion({ partitionKey: 'tenant-1', date, workflowType: 'gen', keySource: 'platform', usage: totals, estimatedCostMicros });
const byDay = await usage.getUsageSummary({ partitionKey: 'tenant-1', startDate, endDate });
```

`checkQuota` enforces three windows in order — **concurrent → per-day → per-month** (UTC) — and the
day/month counts include currently-running (not-yet-aggregated) workflows. `AiUsageStore` extends
`AiQuotaStore`, so one implementation backs both engines.
