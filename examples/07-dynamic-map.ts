/**
 * 07 — Dynamic fan-out / map (gap 06)
 *
 * `defineMapStep` fans out over a list whose size is only known at run time. `items(ctx)`
 * produces the list (from deps/input); the engine spawns one child step per item, each running
 * `each(item, info)` with its own retry/gate. The map step completes with the aggregated outputs
 * (in item order) once all children finish — a failed item fails the whole map.
 *
 * Downstream steps read the map step's output as `{ items: TItemOutput[] }`.
 */
import { z } from 'zod';
import { defineStep, defineMapStep, buildWorkflow } from '@octabits-io/flow';
import { createInMemoryRuntime } from './runtime';

const inputSchema = z.object({});

const listImages = defineStep({
  type: 'list-images',
  workflowInputSchema: inputSchema,
  outputSchema: z.object({ urls: z.array(z.string()) }),
  handler: async () => ({ urls: ['a.png', 'b.png', 'c.png'] }),
});

const resizeAll = defineMapStep({
  type: 'resize-all',
  workflowInputSchema: inputSchema,
  itemOutputSchema: z.object({ thumb: z.string() }),
  dependencies: { listImages },
  items: (ctx) => ctx.deps.listImages.urls, // runtime-sized list
  each: async (url, info) => ({ thumb: `${url}@${info.index}.thumb` }),
  itemRetry: { maxAttempts: 2 }, // per-item retry
});

const report = defineStep({
  type: 'report',
  workflowInputSchema: inputSchema,
  outputSchema: z.object({ count: z.number() }),
  dependencies: { resizeAll },
  handler: async (ctx) => ({ count: ctx.deps.resizeAll.items.length }),
});

const wf = buildWorkflow({ type: 'thumbnails', inputSchema, steps: { listImages, resizeAll, report } });

async function main() {
  const { engine, registry, drain } = createInMemoryRuntime();
  wf.register(registry);

  const started = await wf.start(engine, {});
  if (!started.ok) throw new Error(started.error.message);
  await drain();

  const status = await engine.getWorkflowStatus(started.value.workflowId);
  if (status.ok) {
    console.log('status =', status.value.status);
    console.log('thumbs =', JSON.stringify(status.value.output.resizeAll));
    // → thumbs = {"items":[{"thumb":"a.png@0.thumb"},{"thumb":"b.png@1.thumb"},{"thumb":"c.png@2.thumb"}]}
    const children = status.value.steps.filter((s) => s.parentStepId != null).length;
    console.log('child steps materialized =', children); // → 3
  }
}

main();
