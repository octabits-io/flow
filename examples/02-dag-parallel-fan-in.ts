/**
 * 02 — Parallel branches + fan-in (a diamond DAG)
 *
 *        ┌── enrichA ──┐
 *  fetch ┤              ├── merge
 *        └── enrichB ──┘
 *
 * `enrichA` and `enrichB` both depend only on `fetch`, so they run in parallel. `merge` depends
 * on both, so it waits for the slower one (automatic fan-in). No scheduling code required.
 */
import { z } from 'zod';
import { defineStep, buildWorkflow } from '@octabits-io/flow';
import { createInMemoryRuntime } from './runtime';

const inputSchema = z.object({ id: z.string() });

const fetch = defineStep({
  type: 'fetch',
  workflowInputSchema: inputSchema,
  outputSchema: z.object({ raw: z.string() }),
  handler: async (ctx) => ({ raw: `record:${ctx.workflowInput.id}` }),
});

const enrichA = defineStep({
  type: 'enrichA',
  workflowInputSchema: inputSchema,
  outputSchema: z.object({ a: z.string() }),
  dependencies: { fetch },
  handler: async (ctx) => ({ a: `${ctx.deps.fetch.raw}+A` }),
});

const enrichB = defineStep({
  type: 'enrichB',
  workflowInputSchema: inputSchema,
  outputSchema: z.object({ b: z.string() }),
  dependencies: { fetch },
  handler: async (ctx) => ({ b: `${ctx.deps.fetch.raw}+B` }),
});

const merge = defineStep({
  type: 'merge',
  workflowInputSchema: inputSchema,
  outputSchema: z.object({ merged: z.string() }),
  dependencies: { enrichA, enrichB },
  handler: async (ctx) => ({ merged: `${ctx.deps.enrichA.a} & ${ctx.deps.enrichB.b}` }),
});

const wf = buildWorkflow({ type: 'diamond', inputSchema, steps: { fetch, enrichA, enrichB, merge } });

async function main() {
  const { engine, registry, drain } = createInMemoryRuntime();
  wf.register(registry);

  const started = await wf.start(engine, { id: '7' });
  if (!started.ok) throw new Error(started.error.message);
  await drain();

  const status = await engine.getWorkflowStatus(started.value.workflowId);
  if (status.ok) console.log(status.value.status, JSON.stringify(status.value.output.merge));
  // → completed {"merged":"record:7+A & record:7+B"}
}

main();
