/**
 * 04 — Durable sleep / timers (gap 02)
 *
 * `defineSleepStep` is a no-op step that, once its dependencies complete, is held in the queue
 * for `sleepMs` before completing — then the DAG advances. The delay is **durable**: it lives
 * in the queue (`startAfterSeconds`), so it survives a process restart. Use it for cooldowns,
 * "remind in 24h", or spacing out downstream work.
 *
 * (The in-memory driver here runs the sleep immediately; a real pg-boss dispatcher waits.)
 */
import { z } from 'zod';
import { defineStep, defineSleepStep, buildWorkflow } from '@octabits-io/flow';
import { createInMemoryRuntime } from './runtime';

const inputSchema = z.object({ email: z.string() });

const sendWelcome = defineStep({
  type: 'send-welcome',
  workflowInputSchema: inputSchema,
  outputSchema: z.object({ sent: z.boolean() }),
  handler: async (ctx) => {
    console.log(`welcome → ${ctx.workflowInput.email}`);
    return { sent: true };
  },
});

// Wait 3 days after the welcome before the follow-up.
const wait = defineSleepStep({
  type: 'wait-3-days',
  sleepMs: 3 * 24 * 60 * 60 * 1000,
  dependencies: { sendWelcome },
});

const sendFollowUp = defineStep({
  type: 'send-followup',
  workflowInputSchema: inputSchema,
  outputSchema: z.object({ sent: z.boolean() }),
  dependencies: { wait },
  handler: async (ctx) => {
    console.log(`follow-up → ${ctx.workflowInput.email}`);
    return { sent: true };
  },
});

const wf = buildWorkflow({ type: 'onboarding', inputSchema, steps: { sendWelcome, wait, sendFollowUp } });

async function main() {
  const { engine, registry, drain } = createInMemoryRuntime();
  wf.register(registry);

  const started = await wf.start(engine, { email: 'ada@example.com' });
  if (!started.ok) throw new Error(started.error.message);
  await drain();

  const status = await engine.getWorkflowStatus(started.value.workflowId);
  if (status.ok) console.log('status =', status.value.status); // → completed
}

main();
