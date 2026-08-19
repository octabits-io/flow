/**
 * Conditional branching — `when` guards and a `join: 'any'` convergence.
 *
 * A DAG is static, but which parts of it *run* need not be. A `when` guard
 * decides whether a step executes at all; a step whose guard says no is skipped,
 * and so is everything reachable only through it. That gives you if/else without
 * giving up the inspectable graph.
 *
 * The catch is the convergence: under the default rule (`join: 'all'`) a step
 * needs *every* dependency to have completed, so the branch that wasn't taken
 * would skip the join along with it. `join: 'any'` is the fix — run once all the
 * branches have settled and at least one completed.
 *
 *      ┌── expedite ──┐
 *  triage             ├── notify        (notify joins on whichever arm ran)
 *      └── standard ──┘
 *
 * Run: npx tsx examples/15-conditional-branching.ts
 */
import { z } from 'zod';
import { defineStep, buildWorkflow } from 'octaflow';
import { createInMemoryRuntime } from './runtime.ts';

const input = z.object({ orderId: z.string(), amount: z.number() });
type Input = z.infer<typeof input>;

const triage = defineStep<Input, { priority: 'high' | 'normal' }>({
  type: 'ex15:triage',
  workflowInputSchema: input,
  outputSchema: z.object({ priority: z.enum(['high', 'normal']) }),
  handler: async (ctx) => {
    const priority = ctx.workflowInput.amount >= 1000 ? 'high' : 'normal';
    console.log(`triage: ${ctx.workflowInput.orderId} is ${priority} (${ctx.workflowInput.amount})`);
    return { priority };
  },
});

const expedite = defineStep<Input, { handledBy: string }, unknown, { triage: typeof triage }>({
  type: 'ex15:expedite',
  workflowInputSchema: input,
  outputSchema: z.object({ handledBy: z.string() }),
  dependencies: { triage },
  // The guard sees exactly what the handler would: validated input and deps.
  when: (ctx) => ctx.deps.triage.priority === 'high',
  handler: async () => {
    console.log('expedite: routed to the priority desk');
    return { handledBy: 'priority-desk' };
  },
});

const standard = defineStep<Input, { handledBy: string }, unknown, { triage: typeof triage }>({
  type: 'ex15:standard',
  workflowInputSchema: input,
  outputSchema: z.object({ handledBy: z.string() }),
  dependencies: { triage },
  when: (ctx) => ctx.deps.triage.priority === 'normal',
  handler: async () => {
    console.log('standard: routed to the normal queue');
    return { handledBy: 'normal-queue' };
  },
});

const notify = defineStep<
  Input,
  { message: string },
  unknown,
  { expedite: typeof expedite; standard: typeof standard },
  'any'
>({
  type: 'ex15:notify',
  workflowInputSchema: input,
  outputSchema: z.object({ message: z.string() }),
  dependencies: { expedite, standard },
  // Without this the skipped arm would skip `notify` too.
  join: 'any',
  handler: async (ctx) => {
    // Exactly one arm ran, so both are typed as possibly-absent.
    const handledBy = ctx.deps.expedite?.handledBy ?? ctx.deps.standard?.handledBy ?? 'nobody';
    const message = `${ctx.workflowInput.orderId} handled by ${handledBy}`;
    console.log(`notify: ${message}`);
    return { message };
  },
});

const wf = buildWorkflow({
  type: 'ex15:order-routing',
  inputSchema: input,
  steps: { triage, expedite, standard, notify },
});

async function run(amount: number) {
  const { engine, registry, drain } = createInMemoryRuntime();
  wf.register(registry);

  const started = await wf.start(engine, { orderId: `order-${amount}`, amount });
  if (!started.ok) throw new Error(started.error.message);
  await drain();

  const status = await engine.getWorkflowStatus(started.value.workflowId);
  if (!status.ok) throw new Error(status.error.message);

  const shape = status.value.steps.map((s) => `${s.key}=${s.status}`).join(' ');
  console.log(`→ ${status.value.status}: ${shape}\n`);
}

// Same DAG, two different paths through it.
await run(2500);
await run(40);
