/**
 * Shared in-memory runtime for the in-process examples (01–11).
 *
 * The engine self-advances through a `Dispatcher`. In a real deployment that's a durable
 * queue (pg-boss) drained by worker processes; here it's a plain array you drain in-process.
 * `drain()` runs every enqueued (and re-enqueued) step until the queue is empty.
 *
 * Note: this naive driver ignores `startAfterSeconds` (retry backoff / durable sleep delays
 * collapse to "immediate, in order"). A real dispatcher honors the delay. The *behavior*
 * (retry happens, the sleep step runs) is identical — only wall-clock timing differs.
 */
import {
  createWorkflowEngine,
  createStepHandlerRegistry,
  createInMemoryWorkflowStore,
} from '@octabits-io/flow';
import type {
  Dispatcher,
  DispatchStepPayload,
  StepGate,
  FlowObserver,
  FlowTracer,
  WorkflowHooks,
} from '@octabits-io/flow';

export interface RuntimeOptions {
  partitionKey?: string;
  gate?: StepGate;
  observer?: FlowObserver;
  tracer?: FlowTracer;
  hooks?: WorkflowHooks<any>;
}

export function createInMemoryRuntime(opts: RuntimeOptions = {}) {
  const partitionKey = opts.partitionKey ?? 'default';
  const store = createInMemoryWorkflowStore(partitionKey);
  const registry = createStepHandlerRegistry();
  const queue: DispatchStepPayload[] = [];

  const dispatcher: Dispatcher = {
    async enqueueStep(payload) {
      queue.push(payload);
      return { ok: true, value: undefined };
    },
  };

  const engine = createWorkflowEngine({
    store,
    registry,
    dispatcher,
    partitionKey,
    gate: opts.gate,
    observer: opts.observer,
    tracer: opts.tracer,
    hooks: opts.hooks,
  });

  async function drain() {
    let guard = 0;
    while (queue.length) {
      if (++guard > 10_000) throw new Error('drain runaway — a step keeps re-enqueueing');
      const job = queue.shift()!;
      try {
        await engine.executeStep(job.workflowId, job.stepId);
      } catch {
        // A real dispatcher would retry; the engine has already marked the step failed
        // and cascaded before re-throwing, so swallowing here is faithful.
      }
    }
  }

  return { store, registry, engine, queue, drain };
}
