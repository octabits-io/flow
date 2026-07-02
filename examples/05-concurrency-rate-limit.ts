/**
 * 05 — Concurrency & rate limiting (gap 03)
 *
 * A `StepGate` is consulted before each step runs. It can cap how many steps of a given *type*
 * run at once (concurrency) and/or throttle them (token-bucket rate limit). A step that isn't
 * admitted is **deferred** — re-enqueued with a small delay — WITHOUT consuming a retry attempt.
 *
 * Here we cap `ai:generate` at 2 concurrent and 5/sec. Use `createInMemoryStepGate` for a single
 * process; `createPgStepGate` (store-pg) for crash-safe, cross-process caps.
 */
import { z } from 'zod';
import { defineStep, buildWorkflow, createInMemoryStepGate } from '@octabits-io/flow';
import { createInMemoryRuntime } from './runtime';

const inputSchema = z.object({});

const generate = defineStep({
  type: 'ai:generate',
  workflowInputSchema: inputSchema,
  outputSchema: z.object({ token: z.number() }),
  handler: async (ctx) => ({ token: ctx.stepId }),
});

// Three independent roots of the same step type — without a gate all three run at once;
// with the gate at most 2 run concurrently and they're throttled to 5/sec.
const wf = buildWorkflow({
  type: 'gated',
  inputSchema,
  steps: { gen1: generate, gen2: generate, gen3: generate },
});

async function main() {
  const gate = createInMemoryStepGate({
    concurrency: { 'ai:generate': { maxConcurrent: 2 } },
    rateLimit: { 'ai:generate': { perSecond: 5, burst: 5 } },
  });

  const { engine, registry, drain } = createInMemoryRuntime({ gate });
  wf.register(registry);

  const started = await wf.start(engine, {});
  if (!started.ok) throw new Error(started.error.message);
  await drain(); // deferred steps re-enqueue and eventually run within the caps

  const status = await engine.getWorkflowStatus(started.value.workflowId);
  if (status.ok) console.log('status =', status.value.status); // → completed
}

main();
