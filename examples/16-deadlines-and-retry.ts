/**
 * Deadlines and operator retry — the two things that keep a run from getting stuck
 * or being lost.
 *
 * 1. A `defineWaitStep` with `timeoutMs` cannot wait forever. `onTimeout: { output }`
 *    ends the wait with a stand-in answer and lets the DAG carry on, which — paired
 *    with a `when` guard — is how "approve within the window, otherwise escalate"
 *    is expressed. (`onTimeout: 'fail'`, the default, ends the run instead.)
 *
 * 2. `engine.retryWorkflow(id)` puts a *failed* run back in flight from where it
 *    stopped: the steps that failed or were skipped in the fallout run again with a
 *    fresh attempt budget, while the steps that completed keep their output and are
 *    not re-run. That is the difference between fixing an outage and repeating every
 *    side effect the first attempt already committed.
 *
 * 3. `StartOptions.timeoutMs` puts a wall-clock budget on the whole run.
 *
 * The in-memory runtime here uses a virtual clock, so the 48-hour wait below
 * resolves instantly while still behaving exactly as it would on a real queue.
 *
 * Run: npx tsx examples/16-deadlines-and-retry.ts
 */
import { z } from 'zod';
import { defineStep, defineWaitStep, buildWorkflow } from 'octaflow';
import { createInMemoryRuntime } from './runtime.ts';

const HOURS = 60 * 60 * 1000;

// ---------------------------------------------------------------------------
// 1. A wait that expires into a decision instead of hanging
// ---------------------------------------------------------------------------

const input = z.object({ docId: z.string() });
type Input = z.infer<typeof input>;

const approval = defineWaitStep<{ approved: boolean }>({
  type: 'ex16:approval',
  outputSchema: z.object({ approved: z.boolean() }),
  timeoutMs: 48 * HOURS,
  // Nobody answered in time → treat it as "not approved" and keep going.
  onTimeout: { output: { approved: false } },
});

const publish = defineStep<Input, { done: string }, unknown, { approval: typeof approval }>({
  type: 'ex16:publish',
  workflowInputSchema: input,
  outputSchema: z.object({ done: z.string() }),
  dependencies: { approval },
  when: (ctx) => ctx.deps.approval.approved,
  handler: async (ctx) => {
    console.log(`  publish: ${ctx.workflowInput.docId} is live`);
    return { done: 'published' };
  },
});

const escalate = defineStep<Input, { done: string }, unknown, { approval: typeof approval }>({
  type: 'ex16:escalate',
  workflowInputSchema: input,
  outputSchema: z.object({ done: z.string() }),
  dependencies: { approval },
  when: (ctx) => !ctx.deps.approval.approved,
  handler: async (ctx) => {
    console.log(`  escalate: nobody approved ${ctx.workflowInput.docId} — paging the editor`);
    return { done: 'escalated' };
  },
});

const approvalFlow = buildWorkflow({
  type: 'ex16:approval-flow',
  inputSchema: input,
  steps: { approval, publish, escalate },
});

async function waitThatExpires() {
  console.log('— a wait nobody answers —');
  const { engine, registry, drain } = createInMemoryRuntime();
  approvalFlow.register(registry);

  const started = await approvalFlow.start(engine, { docId: 'doc-1' });
  if (!started.ok) throw new Error(started.error.message);

  // Nothing resumes the step; draining fast-forwards to its 48-hour deadline.
  await drain();

  const status = await engine.getWorkflowStatus(started.value.workflowId);
  if (!status.ok) throw new Error(status.error.message);
  console.log(`  → ${status.value.status}:`, status.value.steps.map((s) => `${s.key}=${s.status}`).join(' '), '\n');
}

async function waitThatIsAnswered() {
  console.log('— the same wait, answered in time —');
  const { engine, registry, drain } = createInMemoryRuntime();
  approvalFlow.register(registry);

  const started = await approvalFlow.start(engine, { docId: 'doc-2' });
  if (!started.ok) throw new Error(started.error.message);
  // `advanceClock: false` stops the drain from skipping ahead to the deadline,
  // leaving the step suspended so the event can actually arrive first.
  await drain({ advanceClock: false });

  await engine.resumeStep(started.value.workflowId, 'approval', { approved: true });
  await drain();

  const status = await engine.getWorkflowStatus(started.value.workflowId);
  if (!status.ok) throw new Error(status.error.message);
  console.log(`  → ${status.value.status}:`, status.value.steps.map((s) => `${s.key}=${s.status}`).join(' '), '\n');
}

// ---------------------------------------------------------------------------
// 2. Retrying a failed run without repeating what already succeeded
// ---------------------------------------------------------------------------

const outage = { down: true };

const charge = defineStep<Record<string, never>, { chargeId: string }>({
  type: 'ex16:charge',
  workflowInputSchema: z.object({}),
  outputSchema: z.object({ chargeId: z.string() }),
  handler: async () => {
    console.log('  charge: taking payment (a side effect you do NOT want twice)');
    return { chargeId: 'ch_1' };
  },
});

const fulfil = defineStep<Record<string, never>, { shipped: boolean }, unknown, { charge: typeof charge }>({
  type: 'ex16:fulfil',
  workflowInputSchema: z.object({}),
  outputSchema: z.object({ shipped: z.boolean() }),
  dependencies: { charge },
  handler: async () => {
    if (outage.down) {
      console.log('  fulfil: warehouse API is down');
      throw new Error('warehouse unreachable');
    }
    console.log('  fulfil: shipped');
    return { shipped: true };
  },
});

const orderFlow = buildWorkflow({
  type: 'ex16:order',
  inputSchema: z.object({}),
  steps: { charge, fulfil },
});

async function retryAfterAnOutage() {
  console.log('— a failed run, retried after the outage —');
  const { engine, registry, drain } = createInMemoryRuntime();
  orderFlow.register(registry);

  const started = await orderFlow.start(engine, {});
  if (!started.ok) throw new Error(started.error.message);
  const id = started.value.workflowId;
  await drain();

  const failed = await engine.getWorkflowStatus(id);
  if (!failed.ok) throw new Error(failed.error.message);
  console.log(`  → ${failed.value.status}:`, failed.value.steps.map((s) => `${s.key}=${s.status}`).join(' '));

  // The warehouse is back.
  outage.down = false;
  const retried = await engine.retryWorkflow(id);
  if (!retried.ok) throw new Error(retried.error.message);
  console.log(`  retryWorkflow reset: ${retried.value.resetSteps.join(', ')} (note: not 'charge')`);
  await drain();

  const status = await engine.getWorkflowStatus(id);
  if (!status.ok) throw new Error(status.error.message);
  console.log(`  → ${status.value.status}:`, JSON.stringify(status.value.output), '\n');
}

// ---------------------------------------------------------------------------
// 3. A budget for the whole run
// ---------------------------------------------------------------------------

async function runDeadline() {
  console.log('— a run that outlives its budget —');
  const { engine, registry, drain, advance } = createInMemoryRuntime();
  orderFlow.register(registry);
  outage.down = false;

  const started = await orderFlow.start(engine, {}, { timeoutMs: 1 * HOURS });
  if (!started.ok) throw new Error(started.error.message);

  // Nothing ran yet, and an hour has gone by.
  advance(2 * HOURS);
  await drain();

  const status = await engine.getWorkflowStatus(started.value.workflowId);
  if (!status.ok) throw new Error(status.error.message);
  console.log(`  → ${status.value.status}: ${status.value.error}`);
}

await waitThatExpires();
await waitThatIsAnswered();
await retryAfterAnOutage();
await runDeadline();
