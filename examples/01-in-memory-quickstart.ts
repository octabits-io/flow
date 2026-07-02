/**
 * 01 — In-memory quick start
 *
 * The smallest complete workflow: two steps, an in-memory store, and an in-process queue you
 * drain by hand. This is the full manual wiring — examples 02+ hide the boilerplate behind
 * `runtime.ts`.
 */
import { z } from 'zod';
import {
  createWorkflowEngine,
  createStepHandlerRegistry,
  createInMemoryWorkflowStore,
  defineStep,
  buildWorkflow,
} from '@octabits-io/flow';
import type { Dispatcher, DispatchStepPayload } from '@octabits-io/flow';

const inputSchema = z.object({ name: z.string() });

// A step declares its input/output schemas and a handler. Dependencies make their outputs
// available, typed, as `ctx.deps`.
const greet = defineStep({
  type: 'greet',
  workflowInputSchema: inputSchema,
  outputSchema: z.object({ greeting: z.string() }),
  handler: async (ctx) => ({ greeting: `Hello, ${ctx.workflowInput.name}` }),
});

const shout = defineStep({
  type: 'shout',
  workflowInputSchema: inputSchema,
  outputSchema: z.object({ loud: z.string() }),
  dependencies: { greet },
  handler: async (ctx) => ({ loud: `${ctx.deps.greet.greeting.toUpperCase()}!` }),
});

// A workflow is a DAG derived from the steps' dependency metadata.
const wf = buildWorkflow({ type: 'hello', inputSchema, steps: { greet, shout } });

async function main() {
  // Runtime: store + registry + a dispatcher. In-memory, the dispatcher is a plain array.
  const store = createInMemoryWorkflowStore();
  const registry = createStepHandlerRegistry();
  const queue: DispatchStepPayload[] = [];
  const dispatcher: Dispatcher = {
    async enqueueStep(payload) {
      queue.push(payload);
      return { ok: true, value: undefined };
    },
  };
  const engine = createWorkflowEngine({ store, registry, dispatcher, partitionKey: 'default' });

  wf.register(registry);

  // Start enqueues the dependency-free roots; draining runs them and the steps they unblock.
  const started = await wf.start(engine, { name: 'Ada' });
  if (!started.ok) throw new Error(started.error.message);

  while (queue.length) {
    const job = queue.shift()!;
    await engine.executeStep(job.workflowId, job.stepId);
  }

  const status = await engine.getWorkflowStatus(started.value.workflowId);
  if (status.ok) console.log(status.value.status, JSON.stringify(status.value.output));
  // → completed {"greet":{"greeting":"Hello, Ada"},"shout":{"loud":"HELLO, ADA!"}}
}

main();
