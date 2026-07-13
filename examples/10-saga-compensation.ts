/**
 * 10 — Saga compensation
 *
 * Add an optional `compensate` to a step to undo its side effects. When the workflow fails, the
 * engine runs `compensate` for each COMPLETED step in REVERSE dependency order (statuses
 * `compensating` → `compensated`). The handler receives the typed context plus the step's own
 * `output` (what to roll back). Best-effort: a throwing rollback is logged + surfaced on the
 * step, and the remaining compensations still run.
 *
 * Classic example: reserve → charge → confirm. If `confirm` fails, refund the charge and
 * release the reservation — in that order.
 */
import { z } from 'zod';
import { defineStep, buildWorkflow } from '@octabits-io/flow';
import { createInMemoryRuntime } from './runtime';

const inputSchema = z.object({});
const log: string[] = [];

const reserve = defineStep({
  type: 'reserve',
  workflowInputSchema: inputSchema,
  outputSchema: z.object({ seatId: z.string() }),
  handler: async () => { log.push('reserve'); return { seatId: 'A1' }; },
  compensate: async (ctx) => { log.push(`release ${ctx.output.seatId}`); },
});

const charge = defineStep({
  type: 'charge',
  workflowInputSchema: inputSchema,
  outputSchema: z.object({ chargeId: z.string() }),
  dependencies: { reserve },
  handler: async () => { log.push('charge'); return { chargeId: 'ch_1' }; },
  compensate: async (ctx) => { log.push(`refund ${ctx.output.chargeId}`); },
});

const confirm = defineStep({
  type: 'confirm',
  workflowInputSchema: inputSchema,
  outputSchema: z.object({ ok: z.boolean() }),
  dependencies: { charge },
  handler: async () => { log.push('confirm'); throw new Error('inventory gone — confirm failed'); },
});

const wf = buildWorkflow({ type: 'booking', inputSchema, steps: { reserve, charge, confirm } });

async function main() {
  const { engine, registry, drain } = createInMemoryRuntime();
  wf.register(registry);

  const started = await wf.start(engine, {});
  if (!started.ok) throw new Error(started.error.message);
  await drain();

  const status = await engine.getWorkflowStatus(started.value.workflowId);
  if (status.ok) console.log('status =', status.value.status); // → failed
  console.log('events =', log.join(' → '));
  // → reserve → charge → confirm → refund ch_1 → release A1   (rollback in reverse order)
}

main();
