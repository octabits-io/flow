/**
 * Shared in-memory runtime for the in-process examples (01–11).
 *
 * The engine self-advances through a `Dispatcher`. In a real deployment that's a durable
 * queue (pg-boss) drained by worker processes; here it's a plain array you drain in-process.
 * `drain()` runs every enqueued (and re-enqueued) step until the queue is empty.
 *
 * The queue runs on a **virtual clock**: `startAfterSeconds` is honoured against it, and
 * when nothing is due `drain()` fast-forwards to the next scheduled job. So retry backoff,
 * durable sleeps and wait deadlines all behave exactly as they would on a real queue —
 * they just don't cost you the wall-clock wait.
 */
import {
  createWorkflowEngine,
  createStepHandlerRegistry,
  createInMemoryWorkflowStore,
} from 'octaflow';
import type {
  Dispatcher,
  DispatchStepPayload,
  StepGate,
  FlowObserver,
  FlowTracer,
  WorkflowHooks,
} from 'octaflow';

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
  const clock = { at: new Date() };
  const queue: Array<{ payload: DispatchStepPayload; runAt: number }> = [];

  const dispatcher: Dispatcher = {
    async enqueueStep(payload, options) {
      queue.push({ payload, runAt: clock.at.getTime() + (options?.startAfterSeconds ?? 0) * 1000 });
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
    now: () => clock.at,
  });

  /**
   * Run queued jobs until nothing is left.
   *
   * By default the clock fast-forwards to the next job when nothing is due yet,
   * so retry backoff, durable sleeps and wait deadlines all resolve without the
   * wall-clock wait. Pass `{ advanceClock: false }` to run only what is due
   * *now* — which is how you let a step suspend and stay suspended long enough
   * to deliver an event to it.
   */
  async function drain(options: { advanceClock?: boolean } = {}) {
    const advanceClock = options.advanceClock ?? true;
    let guard = 0;
    while (queue.length) {
      if (++guard > 10_000) throw new Error('drain runaway — a step keeps re-enqueueing');
      let next = 0;
      for (let i = 1; i < queue.length; i++) if (queue[i]!.runAt < queue[next]!.runAt) next = i;
      if (!advanceClock && queue[next]!.runAt > clock.at.getTime()) return;
      const [job] = queue.splice(next, 1);
      if (job!.runAt > clock.at.getTime()) clock.at = new Date(job!.runAt);
      try {
        // `handleStepJob`, not `executeStep`: a queue carries step runs *and* wait
        // deadlines, and only the payload's `kind` tells them apart.
        await engine.handleStepJob(job!.payload);
      } catch {
        // A real dispatcher would retry; the engine has already marked the step failed
        // and cascaded before re-throwing, so swallowing here is faithful.
      }
    }
  }

  /** Move the virtual clock forward without running anything (e.g. to blow a deadline). */
  function advance(ms: number) {
    clock.at = new Date(clock.at.getTime() + ms);
  }

  return { store, registry, engine, queue, drain, advance, clock };
}
