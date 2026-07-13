/**
 * 08 — Signals / waitForEvent
 *
 * `defineWaitStep` suspends once its dependencies complete (status `waiting`) — its handler
 * never runs. It resumes only when the host calls `engine.resumeStep(workflowId, stepKey,
 * payload)`, e.g. from a webhook or a human approval. The resume payload becomes the step's
 * output (validated against `outputSchema`) and the DAG advances. Re-delivery is a safe no-op.
 */
import { z } from 'zod';
import { defineStep, defineWaitStep, buildWorkflow } from '@octabits-io/flow';
import { createInMemoryRuntime } from './runtime';

const inputSchema = z.object({});

const draft = defineStep({
  type: 'draft',
  workflowInputSchema: inputSchema,
  outputSchema: z.object({ text: z.string() }),
  handler: async () => ({ text: 'please review me' }),
});

// Suspends until an external "approved/rejected" event arrives.
const approval = defineWaitStep({
  type: 'await-approval',
  outputSchema: z.object({ approved: z.boolean(), by: z.string() }),
  dependencies: { draft },
});

const publish = defineStep({
  type: 'publish',
  workflowInputSchema: inputSchema,
  outputSchema: z.object({ published: z.boolean() }),
  dependencies: { approval },
  handler: async (ctx) => ({ published: ctx.deps.approval.approved }),
});

const wf = buildWorkflow({ type: 'review-flow', inputSchema, steps: { draft, approval, publish } });

async function main() {
  const { engine, registry, drain } = createInMemoryRuntime();
  wf.register(registry);

  const started = await wf.start(engine, {});
  if (!started.ok) throw new Error(started.error.message);
  await drain();

  // The workflow is now parked on `approval` (waiting) — nothing left in the queue.
  let status = await engine.getWorkflowStatus(started.value.workflowId);
  if (status.ok) console.log('after draft:', status.value.steps.find((s) => s.key === 'approval')!.status); // → waiting

  // …later, a human/webhook approves. Deliver the event:
  await engine.resumeStep(started.value.workflowId, 'approval', { approved: true, by: 'editor@acme.com' });
  await drain();

  status = await engine.getWorkflowStatus(started.value.workflowId);
  if (status.ok) console.log('final:', status.value.status, JSON.stringify(status.value.output.publish));
  // → final: completed {"published":true}
}

main();
