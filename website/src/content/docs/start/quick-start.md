---
title: Quick start
description: A runnable in-memory workflow in one file — no database, no queue.
---

A complete, runnable single-process workflow — no database, no queue. The engine self-advances
through a `Dispatcher`; in-memory you supply a tiny in-process queue and drain it.

```ts
import { z } from 'zod';
import {
  createWorkflowEngine,
  createStepHandlerRegistry,
  createInMemoryWorkflowStore,
  defineStep,
  buildWorkflow,
  type Dispatcher,
  type DispatchStepPayload,
} from 'octaflow';

// 1. Define typed steps. A step's `dependencies` make its deps' outputs available as `ctx.deps`.
const inputSchema = z.object({ name: z.string() });

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
  handler: async (ctx) => ({ loud: ctx.deps.greet.greeting.toUpperCase() + '!' }),
});

// 2. Build the workflow (a DAG derived from the steps' dependency metadata).
const wf = buildWorkflow({ type: 'hello', inputSchema, steps: { greet, shout } });

// 3. Wire the runtime: store + registry + an in-process dispatcher you drain yourself.
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

// 4. Start and drain. (A real dispatcher like pg-boss does this for you across processes.)
const started = await wf.start(engine, { name: 'Ada' });
if (!started.ok) throw new Error(started.error.message);

while (queue.length) {
  const job = queue.shift()!;
  await engine.executeStep(job.workflowId, job.stepId);
}

// 5. Read the result.
const status = await engine.getWorkflowStatus(started.value.workflowId);
if (status.ok) console.log(status.value.status, status.value.output);
// → 'completed' { greet: { greeting: 'Hello, Ada' }, shout: { loud: 'HELLO, ADA!' } }
```

See [`examples/01-in-memory-quickstart.ts`](https://github.com/octabits-io/octaflow/blob/main/examples/01-in-memory-quickstart.ts).
