/**
 * The README demo. Run it directly:
 *
 *   npx tsx scripts/demo.ts
 *
 * To re-record `docs/demo.svg` after changing this file (needs asciinema; the
 * converter is fetched by npx, nothing to install):
 *
 *   asciinema rec --overwrite --window-size 96x40 --idle-time-limit 2 \
 *     --output-format asciicast-v2 --command "npx tsx scripts/demo.ts" docs/demo.cast
 *   npx svg-term-cli --in docs/demo.cast --out docs/demo.svg --window --width 96 --height 40
 *
 * One realistic pipeline, exercising most of the engine, then a saga rollback:
 *
 *   fetchDraft ─┬─ detectLanguage ── translateAll   (map: fan out over 4 locales)
 *               └─ extractImages  ── optimizeImages (map: 6 images, concurrency-capped,
 *                                    │               one of them flaky → retried)
 *                                    ├─ renderPdf   (sub-workflow: layout → rasterize)
 *                                    └─ awaitReview (suspends for an event, with a deadline)
 *                                         ├─ embargo (durable sleep) ── publish ─┐
 *                                         └─ escalate ───────────────────────────┴─ notifyAuthor
 *
 * `publish` and `escalate` are guarded by `when`, so only the branch the review chose
 * runs; `notifyAuthor` joins with `join: 'any'` so the arm that was skipped doesn't
 * skip the join along with it.
 *
 * The trace is printed from a `FlowObserver` — the same seam you'd wire to OpenTelemetry
 * or an events table — so every line below is an engine transition, not a console.log in
 * a handler.
 *
 * Handler bodies are `await sleep(...)`. This demonstrates scheduling and durability,
 * NOT throughput. It is a demo, not a benchmark.
 */
import { z } from 'zod';
import {
  createWorkflowEngine,
  createStepHandlerRegistry,
  createInMemoryWorkflowStore,
  createInMemoryStepGate,
  defineStep,
  defineMapStep,
  defineWaitStep,
  defineSleepStep,
  defineSubWorkflowStep,
  buildWorkflow,
  retryableError,
} from 'octaflow';
import type { Dispatcher, DispatchStepPayload, FlowEvent, FlowObserver } from 'octaflow';

// ---------------------------------------------------------------------------
// presentation
// ---------------------------------------------------------------------------

const c = {
  d: (s: string) => `\x1b[2m${s}\x1b[0m`,
  b: (s: string) => `\x1b[1m${s}\x1b[0m`,
  cy: (s: string) => `\x1b[36m${s}\x1b[0m`,
  gr: (s: string) => `\x1b[32m${s}\x1b[0m`,
  ye: (s: string) => `\x1b[33m${s}\x1b[0m`,
  re: (s: string) => `\x1b[31m${s}\x1b[0m`,
  ma: (s: string) => `\x1b[35m${s}\x1b[0m`,
};

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
let t0 = Date.now();
const elapsed = () => c.d(`${((Date.now() - t0) / 1000).toFixed(1)}s`.padStart(6));

function banner(title: string) {
  console.log(`\n${c.b(`  ${title}`)}\n`);
}

/** Render one engine transition as a trace line. */
function line(sym: string, key: string, detail = '') {
  console.log(`${elapsed()}  ${sym} ${key.padEnd(24)} ${c.d(detail)}`);
}

/** A FlowObserver that prints the transitions worth watching. */
function tracer(): FlowObserver {
  // `workflow.completed` carries no type, so remember it from the start event.
  const names = new Map<number, string>();
  return {
    record(e: FlowEvent) {
      const key = e.stepKey ?? '';
      const wf = () => names.get(e.workflowId) ?? '';
      switch (e.type) {
        case 'workflow.started':
          if (e.workflowType) names.set(e.workflowId, e.workflowType);
          line(c.ma('◆'), wf(), 'workflow started');
          break;
        case 'step.started':
          line(c.cy('▶'), key, e.attempt && e.attempt > 1 ? `attempt ${e.attempt}` : '');
          break;
        case 'step.completed':
          line(c.gr('✔'), key, `${e.durationMs}ms`);
          break;
        case 'step.retrying':
          line(c.ye('↻'), key, `${e.error} — retrying`);
          break;
        case 'step.failed':
          line(c.re('✘'), key, e.error ?? '');
          break;
        case 'step.mapping':
          line(c.ma('⑂'), key, 'fanned out — parent suspended');
          break;
        case 'step.waiting':
          // Emitted both for wait steps and for a sub-workflow parent awaiting its child.
          line(c.ye('⏸'), key, key === 'awaitReview' ? 'suspended — awaiting external event' : 'suspended — awaiting child workflow');
          break;
        case 'step.resumed':
          line(c.gr('⏵'), key, 'event delivered');
          break;
        case 'step.skipped':
          line(c.d('⊘'), key, 'skipped — its branch was not taken');
          break;
        case 'step.timedOut':
          line(c.ye('⏱'), key, 'wait deadline reached');
          break;
        case 'step.compensating':
          line(c.ye('↶'), key, 'rolling back');
          break;
        case 'step.compensated':
          line(c.gr('✔'), key, e.error ? `rollback error: ${e.error}` : 'rolled back');
          break;
        case 'workflow.completed':
          line(c.gr('◆'), wf(), 'workflow completed');
          break;
        case 'workflow.failed':
          line(c.re('◆'), wf(), 'workflow failed');
          break;
      }
    },
  };
}

// ---------------------------------------------------------------------------
// a runtime that honours delays and runs ready steps concurrently
// ---------------------------------------------------------------------------

function runtime(opts: { gate?: ReturnType<typeof createInMemoryStepGate> } = {}) {
  const store = createInMemoryWorkflowStore();
  const registry = createStepHandlerRegistry();
  let jobs: Array<{ p: DispatchStepPayload; runAt: number }> = [];

  const dispatcher: Dispatcher = {
    async enqueueStep(p, options) {
      const delay = options?.startAfterSeconds ?? 0;
      // Surface long durable delays so the pause in the trace reads as intent, not a hang.
      if (delay >= 2) {
        if (p.kind === 'timeout') line(c.ye('⏳'), p.stepKey, `deadline armed — ${delay}s to respond`);
        else line(c.ye('⏲'), p.stepKey, `held in the queue for ${delay}s — durable`);
      }
      jobs.push({ p, runAt: Date.now() + delay * 1000 });
      return { ok: true, value: undefined };
    },
  };

  const engine = createWorkflowEngine({
    store,
    registry,
    dispatcher,
    partitionKey: 'demo',
    gate: opts.gate,
    observer: tracer(),
  });

  /** Run everything that is due, concurrently; wait for the next due job otherwise. */
  async function drain() {
    let guard = 0;
    while (jobs.length) {
      if (++guard > 5000) throw new Error('runaway');
      const now = Date.now();
      const due = jobs.filter((j) => j.runAt <= now);
      if (!due.length) {
        // Everything left is a wait deadline: either the run is parked on an
        // external event (only the demo can move it) or it has already finished
        // and the deadline is moot. Either way, waiting it out is dead air.
        if (jobs.every((j) => j.p.kind === 'timeout')) {
          jobs = [];
          return;
        }
        await sleep(Math.max(10, Math.min(...jobs.map((j) => j.runAt)) - now));
        continue;
      }
      jobs = jobs.filter((j) => j.runAt > now);
      await Promise.all(due.map((j) => engine.handleStepJob(j.p).catch(() => undefined)));
    }
  }

  return { store, registry, engine, drain };
}

// ---------------------------------------------------------------------------
// act one — the pipeline
// ---------------------------------------------------------------------------

const input = z.object({ draftId: z.string() });

function plain<O extends Record<string, unknown>>(
  type: string,
  ms: number,
  out: z.ZodType<O>,
  value: O,
  deps?: Record<string, any>,
  wfInput: z.ZodType<any> = input,
) {
  return defineStep<any, O, unknown, any>({
    type,
    workflowInputSchema: wfInput,
    outputSchema: out,
    ...(deps ? { dependencies: deps } : {}),
    handler: async () => {
      await sleep(ms);
      return value;
    },
  });
}

async function actOne() {
  banner('A publishing pipeline — fan-out, sub-workflow, signal, deadline, branch');
  console.log(
    c.d(
      '   fetchDraft ─┬─ detectLanguage ── translateAll   (4 locales)\n' +
        '               └─ extractImages  ── optimizeImages (6 images, max 2 at once)\n' +
        '                                     ├─ renderPdf   (sub-workflow)\n' +
        '                                     └─ awaitReview (external event, 8s deadline)\n' +
        '                                          ├─ embargo (sleep) ─ publish ─┐\n' +
        '                                          └─ escalate ─────────────────┴─ notifyAuthor\n',
    ),
  );

  const fetchDraft = plain('fetchDraft', 700, z.object({ text: z.string() }), { text: '…' });
  const detectLanguage = plain('detectLanguage', 600, z.object({ lang: z.string() }), { lang: 'en' }, { fetchDraft });
  const extractImages = plain('extractImages', 900, z.object({ n: z.number() }), { n: 6 }, { fetchDraft });

  const translateAll = defineMapStep<string, { locale: string }, { draftId: string }, unknown, any>({
    type: 'translateAll',
    workflowInputSchema: input,
    itemOutputSchema: z.object({ locale: z.string() }),
    dependencies: { detectLanguage },
    items: () => ['de', 'fr', 'es', 'ja'],
    each: async (locale) => {
      await sleep(800);
      return { locale };
    },
  });

  // One image fails on its first attempt, so the per-item retry budget is visible.
  const seen = new Map<number, number>();
  const optimizeImages = defineMapStep<number, { bytes: number }, { draftId: string }, unknown, any>({
    type: 'optimizeImages',
    workflowInputSchema: input,
    itemOutputSchema: z.object({ bytes: z.number() }),
    dependencies: { extractImages },
    itemRetry: { maxAttempts: 3, backoff: 'fixed', initialDelayMs: 400 },
    items: () => [0, 1, 2, 3, 4, 5],
    each: async (i) => {
      const n = (seen.get(i) ?? 0) + 1;
      seen.set(i, n);
      await sleep(900);
      // Marked explicitly: "encoder busy" is not in the default heuristic's vocabulary,
      // so without the marker this would fail terminally and take the whole map with it.
      if (i === 3 && n === 1) throw retryableError('encoder busy');
      return { bytes: 1000 + i };
    },
  });

  const childInput = z.object({});
  const layout = plain('layout', 700, z.object({ pages: z.number() }), { pages: 12 }, undefined, childInput);
  const rasterize = plain('rasterize', 700, z.object({ url: z.string() }), { url: 'a.pdf' }, { layout }, childInput);
  const pdfChild = buildWorkflow({ type: 'render-pdf', inputSchema: childInput, steps: { layout, rasterize } });

  const renderPdf = defineSubWorkflowStep<{ url: string }, { draftId: string }, unknown, any>({
    type: 'renderPdf',
    workflowInputSchema: input,
    childWorkflow: pdfChild,
    outputSchema: z.object({ url: z.string() }),
    dependencies: { optimizeImages, translateAll },
    input: () => ({}),
  });

  // The wait cannot hang forever: if no editor answers in 8s the step completes
  // itself with `approved: false`, and the branch below routes on that instead.
  const awaitReview = defineWaitStep({
    type: 'awaitReview',
    outputSchema: z.object({ approved: z.boolean(), by: z.string() }),
    dependencies: { renderPdf },
    timeoutMs: 8000,
    onTimeout: { output: { approved: false, by: 'nobody' } },
  });

  const embargo = defineSleepStep({ type: 'embargo', sleepMs: 4000, dependencies: { awaitReview } });

  // Two arms, complementary guards. Whichever one the review didn't choose is
  // skipped — along with anything reachable only through it.
  const publish = defineStep<any, { url: string }, unknown, any>({
    type: 'publish',
    workflowInputSchema: input,
    outputSchema: z.object({ url: z.string() }),
    dependencies: { embargo, awaitReview },
    when: (ctx) => ctx.deps.awaitReview.approved,
    handler: async () => {
      await sleep(600);
      return { url: '/a' };
    },
  });

  const escalate = defineStep<any, { paged: string }, unknown, any>({
    type: 'escalate',
    workflowInputSchema: input,
    outputSchema: z.object({ paged: z.string() }),
    dependencies: { awaitReview },
    when: (ctx) => !ctx.deps.awaitReview.approved,
    handler: async () => {
      await sleep(600);
      return { paged: 'editor-on-call' };
    },
  });

  // `join: 'any'` is what lets this run at all: under the default rule the
  // skipped arm would skip the join with it.
  const notifyAuthor = defineStep<any, { told: string }, unknown, any, 'any'>({
    type: 'notifyAuthor',
    workflowInputSchema: input,
    outputSchema: z.object({ told: z.string() }),
    dependencies: { publish, escalate },
    join: 'any',
    handler: async (ctx) => {
      await sleep(500);
      return { told: ctx.deps.publish ? 'published' : 'escalated' };
    },
  });

  const wf = buildWorkflow({
    type: 'publish-article',
    inputSchema: input,
    steps: {
      fetchDraft, detectLanguage, extractImages, translateAll, optimizeImages, renderPdf,
      awaitReview, embargo, publish, escalate, notifyAuthor,
    },
  });

  // Cap the image workers: at most 2 of the 6 children run at a time.
  const gate = createInMemoryStepGate({
    concurrency: { optimizeImages__item: { maxConcurrent: 2 } },
    concurrencyRetrySeconds: 1,
  });

  const rt = runtime({ gate });
  wf.register(rt.registry);
  t0 = Date.now();

  const started = await wf.start(rt.engine, { draftId: 'draft-42' });
  if (!started.ok) throw new Error('start failed');
  await rt.drain();

  // Parked on awaitReview — nothing queued. A human (or a webhook) approves.
  console.log(c.d('\n         … an editor reviews it out of band …\n'));
  await sleep(1800);
  await rt.engine.resumeStep(started.value.workflowId, 'awaitReview', { approved: true, by: 'editor@acme' });
  await rt.drain();

  const status = await rt.engine.getWorkflowStatus(started.value.workflowId);
  if (status.ok) {
    console.log(
      `\n  ${c.b(status.value.status)} in ${c.b(`${((Date.now() - t0) / 1000).toFixed(1)}s`)}` +
        c.d(` — ${status.value.steps.length} steps persisted, every transition durable\n`),
    );
  }
}

// ---------------------------------------------------------------------------
// act two — saga rollback
// ---------------------------------------------------------------------------

async function actTwo() {
  banner('When a step fails, completed steps roll back in reverse');

  const empty = z.object({});
  const reserve = defineStep({
    type: 'reserveSeat',
    workflowInputSchema: empty,
    outputSchema: z.object({ seatId: z.string() }),
    handler: async () => {
      await sleep(600);
      return { seatId: 'A1' };
    },
    compensate: async () => {
      await sleep(400); // releasing the seat
    },
  });
  const charge = defineStep({
    type: 'chargeCard',
    workflowInputSchema: empty,
    outputSchema: z.object({ chargeId: z.string() }),
    dependencies: { reserve },
    handler: async () => {
      await sleep(700);
      return { chargeId: 'ch_1' };
    },
    compensate: async () => {
      await sleep(400); // refunding the charge
    },
  });
  const confirm = defineStep({
    type: 'confirmBooking',
    workflowInputSchema: empty,
    outputSchema: z.object({ ok: z.boolean() }),
    dependencies: { charge },
    handler: async () => {
      await sleep(600);
      throw new Error('inventory gone');
    },
  });

  const wf = buildWorkflow({ type: 'booking', inputSchema: empty, steps: { reserve, charge, confirm } });
  const rt = runtime();
  wf.register(rt.registry);
  t0 = Date.now();
  const started = await wf.start(rt.engine, {});
  if (!started.ok) throw new Error('start failed');
  await rt.drain();
  await sleep(300);
  console.log(c.d('\n  charge refunded before the seat was released — reverse order\n'));
}

await actOne();
await actTwo();
console.log(
  c.d('  no workflow server, no control plane — this is a library over Postgres\n'),
);
