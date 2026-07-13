/**
 * 03 — Per-step retry & timeout
 *
 * A step can declare a `retry` policy and a `timeoutMs`. A *retryable* failure (thrown error
 * whose message looks transient — "429", "rate limit", "timeout", "503", …) is retried with
 * backoff up to `maxAttempts`; after that it fails terminally. A timeout aborts the handler
 * (via `ctx.signal`) and counts as a retryable failure.
 *
 * Non-retryable errors (a plain bug) fail immediately without burning the retry budget.
 */
import { z } from 'zod';
import { defineStep, buildWorkflow } from '@octabits-io/flow';
import { createInMemoryRuntime } from './runtime';

const inputSchema = z.object({});
let attempts = 0;

const flaky = defineStep({
  type: 'call-api',
  workflowInputSchema: inputSchema,
  outputSchema: z.object({ ok: z.boolean() }),
  // 3 total attempts, exponential backoff starting at 200ms (capped at 5s).
  retry: { maxAttempts: 3, backoff: 'exponential', initialDelayMs: 200, maxDelayMs: 5_000 },
  timeoutMs: 10_000,
  handler: async () => {
    attempts++;
    if (attempts < 3) throw new Error('429 rate limit — retry me'); // retryable
    return { ok: true };
  },
});

const wf = buildWorkflow({ type: 'retrying', inputSchema, steps: { flaky } });

async function main() {
  const { engine, registry, drain } = createInMemoryRuntime();
  wf.register(registry);

  const started = await wf.start(engine, {});
  if (!started.ok) throw new Error(started.error.message);
  await drain(); // the in-memory driver collapses backoff delays; a real queue waits them out

  const status = await engine.getWorkflowStatus(started.value.workflowId);
  if (status.ok) {
    const step = status.value.steps.find((s) => s.key === 'flaky')!;
    console.log(`status=${status.value.status} attempts=${step.attempts}`);
    // → status=completed attempts=3
  }
}

main();
