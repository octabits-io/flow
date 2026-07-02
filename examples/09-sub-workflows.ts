/**
 * 09 — Child / sub-workflows (gap 08)
 *
 * `defineSubWorkflowStep` starts another (built) workflow and awaits its result. `input(ctx)`
 * maps the parent context to the child's input; the engine starts the child (same partition),
 * suspends the parent step, and resumes it with the child's output once the child terminates.
 * A failed/cancelled child fails the parent step (and cascades). Sub-workflows compose + nest.
 */
import { z } from 'zod';
import { defineStep, defineSubWorkflowStep, buildWorkflow } from '@octabits-io/flow';
import { createInMemoryRuntime } from './runtime';

// --- Child workflow: takes { n } and triples it ---
const childInput = z.object({ n: z.number() });
const triple = defineStep({
  type: 'triple',
  workflowInputSchema: childInput,
  outputSchema: z.object({ tripled: z.number() }),
  handler: async (ctx) => ({ tripled: ctx.workflowInput.n * 3 }),
});
const childWf = buildWorkflow({ type: 'tripler', inputSchema: childInput, steps: { triple } });

// --- Parent workflow: calls the child, then uses its output ---
const parentInput = z.object({ value: z.number() });

const callChild = defineSubWorkflowStep({
  type: 'call-tripler',
  workflowInputSchema: parentInput,
  childWorkflow: childWf,
  input: (ctx) => ({ n: ctx.workflowInput.value }),
  // The child's output is keyed by its step keys: { triple: { tripled: number } }
  outputSchema: z.object({ triple: z.object({ tripled: z.number() }) }),
});

const report = defineStep({
  type: 'report',
  workflowInputSchema: parentInput,
  outputSchema: z.object({ result: z.number() }),
  dependencies: { callChild },
  handler: async (ctx) => ({ result: ctx.deps.callChild.triple.tripled }),
});

const parentWf = buildWorkflow({ type: 'parent', inputSchema: parentInput, steps: { callChild, report } });

async function main() {
  const { engine, registry, drain } = createInMemoryRuntime();
  // Registering the parent also registers the child workflow's handlers automatically.
  parentWf.register(registry);

  const started = await parentWf.start(engine, { value: 7 });
  if (!started.ok) throw new Error(started.error.message);
  await drain();

  const status = await engine.getWorkflowStatus(started.value.workflowId);
  if (status.ok) console.log(status.value.status, JSON.stringify(status.value.output.report));
  // → completed {"result":21}
}

main();
