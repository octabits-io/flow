/**
 * 06 — Workflow-start idempotency
 *
 * Pass an `idempotencyKey` when starting. A second start with the same key (per partition)
 * returns the EXISTING workflow instead of creating a duplicate — so a double-click, a retried
 * request, or two overlapping cron ticks can't start the same work twice.
 */
import { z } from 'zod';
import { defineStep, buildWorkflow } from '@octabits-io/flow';
import { createInMemoryRuntime } from './runtime';

const inputSchema = z.object({ fileId: z.string() });

const importFile = defineStep({
  type: 'import-file',
  workflowInputSchema: inputSchema,
  outputSchema: z.object({ rows: z.number() }),
  handler: async () => ({ rows: 100 }),
});

const wf = buildWorkflow({ type: 'import', inputSchema, steps: { importFile } });

async function main() {
  const { engine, registry, drain } = createInMemoryRuntime();
  wf.register(registry);

  const key = 'import:file-42';
  const first = await wf.start(engine, { fileId: 'file-42' }, { idempotencyKey: key });
  const second = await wf.start(engine, { fileId: 'file-42' }, { idempotencyKey: key });
  if (!first.ok || !second.ok) throw new Error('start failed');

  console.log('same workflow?', first.value.workflowId === second.value.workflowId);
  // → same workflow? true   (the second start enqueued nothing new)
  console.log('second enqueued', second.value.enqueuedSteps.length); // → 0

  await drain();
}

main();
