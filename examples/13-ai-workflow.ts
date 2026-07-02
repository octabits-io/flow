/**
 * 13 — AI workflow add-on (instrumented model + token/cost capture)
 *
 * `@octabits-io/flow/ai` wires model instrumentation, token/cost capture, optional quota, and
 * daily usage rollups into the engine's lifecycle hooks — the core stays AI-free. In an AI step,
 * `ctx.context.model` is an INSTRUMENTED `LanguageModelV4`: token usage is captured automatically
 * and turned into cost by a pluggable pricing table. `ctx.context.host` is whatever your
 * `resolveHost` returns (a DI scope, domain services).
 *
 * Requires the optional peers: `ai`, `@ai-sdk/provider` (+ a provider like `@ai-sdk/anthropic`).
 */
import { z } from 'zod';
import { generateText } from 'ai';
import {
  createWorkflowEngine,
  createStepHandlerRegistry,
  createInMemoryWorkflowStore,
} from '@octabits-io/flow';
import type { Dispatcher, DispatchStepPayload } from '@octabits-io/flow';
import { defineAiStep, buildAiWorkflow, createAiWorkflowHooks } from '@octabits-io/flow/ai';
import type { LanguageModelV4 } from '@ai-sdk/provider';

// Your host context (DI scope, services) — exposed to handlers as `ctx.context.host`.
interface Host {
  logger: { info: (m: string) => void };
}

const inputSchema = z.object({ text: z.string() });

const summarize = defineAiStep<{ text: string }, { summary: string }, Host>({
  type: 'summarize',
  workflowInputSchema: inputSchema,
  outputSchema: z.object({ summary: z.string() }),
  retry: { maxAttempts: 3 }, // ride out provider 429s
  handler: async (ctx) => {
    ctx.context.host.logger.info('summarizing…');
    const { text } = await generateText({
      model: ctx.context.model, // instrumented — usage captured behind the scenes
      prompt: `Summarize in one sentence:\n${ctx.workflowInput.text}`,
    });
    return { summary: text };
  },
});

const wf = buildAiWorkflow<{ text: string }, Host>({ type: 'summarizer', inputSchema, steps: { summarize } });

async function main() {
  // Bring your own model. e.g. import { anthropic } from '@ai-sdk/anthropic'; const model = anthropic('claude-haiku-4-5');
  const model: LanguageModelV4 = /* your provider model */ undefined as unknown as LanguageModelV4;

  // The hooks turn the engine AI-aware: resolve + instrument the model, capture usage/cost,
  // (optionally) enforce quota, and roll up daily usage.
  const hooks = createAiWorkflowHooks<Host>({
    modelResolver: {
      resolveModel: () => model,
      resolveHost: () => ({ logger: console }),
      resolveKeySource: () => 'platform', // stamped into metadata for usage rollups
    },
    usageRecorder: {
      // Persist however you like (a step row, a usage table, …).
      recordStepUsage: async ({ stepId, usage, costMicros }) =>
        console.log(`step ${stepId}: in=${usage.inputTokens} out=${usage.outputTokens} tokens, ${costMicros}µ$`),
      incrementWorkflowUsage: async () => {},
    },
    // quotaPolicy: { checkQuota: async () => ({ ok: true, value: undefined }) }, // reject a start past the limit
    // costEstimator: createCostEstimator({
    //   pricing: { 'my-model': { inputPerMillion: 1, outputPerMillion: 3, cacheReadPerMillion: 0.3, cacheWritePerMillion: 3.75 } },
    // }),
  });

  // Standard core runtime (here in-memory) — just pass `hooks`.
  const store = createInMemoryWorkflowStore('tenant-1');
  const registry = createStepHandlerRegistry();
  const queue: DispatchStepPayload[] = [];
  const dispatcher: Dispatcher = {
    async enqueueStep(p) { queue.push(p); return { ok: true, value: undefined }; },
  };
  const engine = createWorkflowEngine({ store, registry, dispatcher, partitionKey: 'tenant-1', hooks });
  wf.register(registry);

  const started = await wf.start(engine, { text: 'Flow is a durable DAG engine for TypeScript.' });
  if (!started.ok) throw new Error(started.error.message);
  while (queue.length) {
    const job = queue.shift()!;
    await engine.executeStep(job.workflowId, job.stepId);
  }

  const status = await engine.getWorkflowStatus(started.value.workflowId);
  if (status.ok) console.log(status.value.status, JSON.stringify(status.value.output));
}

main();
