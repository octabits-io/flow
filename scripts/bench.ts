/**
 * Throughput / latency benchmark against real Postgres.
 *
 *   npx tsx scripts/bench.ts              # default sizes
 *   BENCH_WORKFLOWS=500 npx tsx scripts/bench.ts
 *   BENCH_SKIP_PGBOSS=1 npx tsx scripts/bench.ts
 *
 * Docker must be running — Postgres is started via Testcontainers so the numbers
 * come from a real database rather than a mock.
 *
 * WHAT IS MEASURED
 *
 * Handlers are no-ops. This measures the *engine's* cost per step — claiming the
 * step, reading dependency outputs, persisting the transition, recomputing
 * readiness and enqueueing what became ready — not how fast your business logic
 * is. Two suites:
 *
 *   A. engine + Postgres store, in-process dispatcher.  Isolates engine + DB cost.
 *   B. engine + Postgres store + pg-boss workers.       The full production path.
 *
 * Suite B's *latency* is dominated by the queue's polling interval, not by the
 * engine; its throughput is the number that means something. Suite A tells you
 * what the engine costs; suite B tells you what a deployment does.
 *
 * READ THE NUMBERS WITH CARE
 *
 * They are specific to one machine, one Postgres, and this DAG shape. Docker on
 * macOS in particular has far slower disk I/O than a Linux host. Treat them as an
 * order of magnitude and a scaling curve, not a score to compare against a queue
 * that does less work.
 */
import { z } from 'zod';
import { Pool } from 'pg';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import {
  createWorkflowEngine,
  createStepHandlerRegistry,
  defineStep,
  buildWorkflow,
} from 'octaflow';
import type { Dispatcher, DispatchStepPayload } from 'octaflow';
import { createPgWorkflowStore, applySchema, FLOW_STORE_DDL } from 'octaflow/store-pg';

// ---------------------------------------------------------------------------
// config
// ---------------------------------------------------------------------------

const WORKFLOWS = Number(process.env.BENCH_WORKFLOWS ?? 200);
const CONCURRENCIES = (process.env.BENCH_CONCURRENCY ?? '1,4,16,64').split(',').map(Number);
const SKIP_PGBOSS = process.env.BENCH_SKIP_PGBOSS === '1';
const STEPS_PER_WORKFLOW = 6; // root → 4 parallel → join

const c = {
  d: (s: string) => `\x1b[2m${s}\x1b[0m`,
  b: (s: string) => `\x1b[1m${s}\x1b[0m`,
  gr: (s: string) => `\x1b[32m${s}\x1b[0m`,
};

// ---------------------------------------------------------------------------
// stats
// ---------------------------------------------------------------------------

function percentile(sorted: number[], p: number): number {
  if (!sorted.length) return 0;
  const idx = Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1);
  return sorted[idx]!;
}

function summarise(durations: number[]) {
  const sorted = [...durations].sort((a, b) => a - b);
  return {
    n: sorted.length,
    p50: percentile(sorted, 50),
    p95: percentile(sorted, 95),
    p99: percentile(sorted, 99),
    max: sorted[sorted.length - 1] ?? 0,
  };
}

const ms = (v: number) => `${v.toFixed(1)}ms`;

// ---------------------------------------------------------------------------
// workload — one workflow is a small diamond: root → a,b,c,d → join
// ---------------------------------------------------------------------------

const input = z.object({ n: z.number() });
const out = z.object({ ok: z.literal(true) });
const noop = async () => ({ ok: true as const });

function buildBenchWorkflow() {
  const root = defineStep<{ n: number }, { ok: true }, unknown, any>({
    type: 'bench:root', workflowInputSchema: input, outputSchema: out, handler: noop,
  });
  const fan = ['a', 'b', 'c', 'd'].map((k) =>
    defineStep<{ n: number }, { ok: true }, unknown, any>({
      type: `bench:${k}`, workflowInputSchema: input, outputSchema: out,
      dependencies: { root }, handler: noop,
    }),
  );
  const [a, b, d, e] = fan as [any, any, any, any];
  const join = defineStep<{ n: number }, { ok: true }, unknown, any>({
    type: 'bench:join', workflowInputSchema: input, outputSchema: out,
    dependencies: { a, b, c: d, d: e }, handler: noop,
  });
  return buildWorkflow({ type: 'bench', inputSchema: input, steps: { root, a, b, c: d, d: e, join } });
}

// ---------------------------------------------------------------------------
// suite A — engine + Postgres store, in-process dispatcher
// ---------------------------------------------------------------------------

async function suiteA(uri: string) {
  console.log(c.b('\n  A. engine + Postgres store (in-process dispatcher)\n'));
  console.log(
    c.d(`     ${WORKFLOWS} workflows × ${STEPS_PER_WORKFLOW} steps = ${WORKFLOWS * STEPS_PER_WORKFLOW} steps per run\n`),
  );
  console.log(c.d('     conc   steps/sec    p50      p95      p99      max'));
  console.log(c.d('     ' + '─'.repeat(56)));

  const rows: Array<{ conc: number; rate: number; p50: number; p95: number; p99: number }> = [];

  for (const conc of CONCURRENCIES) {
    // Pool must exceed the in-flight step count, or we measure pool contention
    // rather than the engine.
    const pool = new Pool({ connectionString: uri, max: conc + 4 });
    const store = createPgWorkflowStore({ pool, partitionKey: `bench-${conc}` });
    const registry = createStepHandlerRegistry();
    const queue: DispatchStepPayload[] = [];
    const dispatcher: Dispatcher = {
      async enqueueStep(p) { queue.push(p); return { ok: true, value: undefined }; },
    };
    const engine = createWorkflowEngine({ store, registry, dispatcher, partitionKey: `bench-${conc}` });
    const wf = buildBenchWorkflow();
    wf.register(registry);

    // warm up connections + query plans so the first rows don't skew p99
    const warm = await wf.start(engine, { n: -1 });
    if (warm.ok) while (queue.length) { const j = queue.shift()!; await engine.executeStep(j.workflowId, j.stepId); }

    for (let i = 0; i < WORKFLOWS; i++) await wf.start(engine, { n: i });

    const durations: number[] = [];
    const started = performance.now();
    // `conc` consumers pulling from the shared queue, like `conc` workers would.
    await Promise.all(
      Array.from({ length: conc }, async () => {
        for (;;) {
          const job = queue.shift();
          if (!job) break;
          const t = performance.now();
          try { await engine.executeStep(job.workflowId, job.stepId); } catch { /* counted anyway */ }
          durations.push(performance.now() - t);
        }
      }),
    );
    const elapsed = performance.now() - started;
    const rate = (durations.length / elapsed) * 1000;
    const s = summarise(durations);
    rows.push({ conc, rate, p50: s.p50, p95: s.p95, p99: s.p99 });

    console.log(
      `     ${String(conc).padStart(4)}   ${c.gr(rate.toFixed(0).padStart(9))}   ` +
        `${ms(s.p50).padStart(7)}  ${ms(s.p95).padStart(7)}  ${ms(s.p99).padStart(7)}  ${ms(s.max).padStart(7)}`,
    );
    await pool.end();
  }

  const best = rows.reduce((m, r) => (r.rate > m.rate ? r : m), rows[0]!);
  console.log(c.d(`\n     peak ${best.rate.toFixed(0)} steps/sec at concurrency ${best.conc}`));
  return rows;
}

// ---------------------------------------------------------------------------
// suite B — the full path: engine + store + pg-boss workers
// ---------------------------------------------------------------------------

async function suiteB(uri: string) {
  const { PgBoss } = await import('pg-boss');
  const { createPgBossDispatcher, createPgBossStepWorker } = await import(
    'octaflow/dispatcher-pgboss'
  );

  console.log(c.b('\n  B. end-to-end through pg-boss workers\n'));
  console.log(
    c.d('     A single worker is poll-bound: it fetches a batch, runs it, then waits out\n' +
        '     the polling interval. Throughput comes from running several workers.\n'),
  );
  console.log(c.d('     workers  batch  burst  conc   steps/sec   wall'));
  console.log(c.d('     ' + '─'.repeat(56)));

  const configs = [
    { workers: 1, batch: 25, burst: false, concurrency: 1 },
    { workers: 1, batch: 25, burst: true, concurrency: 1 },
    { workers: 1, batch: 25, burst: true, concurrency: 8 },
    { workers: 4, batch: 25, burst: true, concurrency: 8 },
  ];
  const rows: Array<{ workers: number; batch: number; burst: boolean; concurrency: number; rate: number }> = [];

  for (const [runIdx, cfg] of configs.entries()) {
    const queueName = `bench-steps-${runIdx}`;
    const partitionKey = `bench-pgboss-${runIdx}`;
    const boss = new PgBoss({ connectionString: uri, schema: `pgboss_bench_${runIdx}` });
    await boss.start();

    const pool = new Pool({ connectionString: uri, max: cfg.workers * 4 + 8 });
    const store = createPgWorkflowStore({ pool, partitionKey });
    const registry = createStepHandlerRegistry();
    const dispatcher = createPgBossDispatcher({ boss, queueName, partitionKey });
    const engine = createWorkflowEngine({ store, registry, dispatcher, partitionKey });
    const wf = buildBenchWorkflow();
    wf.register(registry);

    let done = 0;
    const target = WORKFLOWS * STEPS_PER_WORKFLOW;
    let settle!: () => void;
    const finished = new Promise<void>((r) => (settle = r));

    const workers = await Promise.all(
      Array.from({ length: cfg.workers }, async () => {
        const w = createPgBossStepWorker({
          boss,
          queueName,
          workerOptions: {
            pollingIntervalSeconds: 0.5,
            batchSize: cfg.batch,
            burstWhenBatchFull: cfg.burst,
            concurrency: cfg.concurrency,
          },
        });
        await w.start(async (payload) => {
          await engine.executeStep(payload.workflowId, payload.stepId);
          if (++done >= target) settle();
        });
        return w;
      }),
    );

    const started = performance.now();
    for (let i = 0; i < WORKFLOWS; i++) await wf.start(engine, { n: i });

    const timedOut = await Promise.race([
      finished.then(() => false),
      new Promise<boolean>((r) => setTimeout(() => r(true), 180_000).unref()),
    ]);
    const elapsed = performance.now() - started;
    const rate = (done / elapsed) * 1000;
    rows.push({ workers: cfg.workers, batch: cfg.batch, burst: cfg.burst, concurrency: cfg.concurrency, rate });

    console.log(
      `     ${String(cfg.workers).padStart(7)}  ${String(cfg.batch).padStart(5)}  ` +
        `${(cfg.burst ? 'yes' : 'no').padStart(5)}  ${String(cfg.concurrency).padStart(4)}   ` +
        `${c.gr(rate.toFixed(0).padStart(9))}   ${c.d(`${(elapsed / 1000).toFixed(1)}s`)}` +
        (timedOut ? c.d(`  (timed out at ${done}/${target})`) : ''),
    );

    await Promise.all(workers.map((w) => w.stop()));
    await boss.stop({ graceful: false });
    await pool.end();
  }

  const best = rows.reduce((m, r) => (r.rate > m.rate ? r : m), rows[0]!);
  console.log(
    c.d(`\n     peak ${best.rate.toFixed(0)} steps/sec — ${best.workers} workers, batch ${best.batch}, burst ${best.burst ? 'on' : 'off'}, concurrency ${best.concurrency}`),
  );
  console.log(
    c.d('     Latency here is queue polling, not engine cost — suite A is the engine.\n' +
        '     Without burst a worker is poll-bound: it drains a batch in milliseconds, then\n' +
        '     waits out the interval. Concurrency only pays once burst removes that wait —\n' +
        '     on its own it moves nothing.'),
  );
  return rows;
}

// ---------------------------------------------------------------------------

async function main() {
  console.log(c.d('\n  starting postgres:17-alpine via testcontainers…'));
  let container: StartedPostgreSqlContainer | undefined;
  try {
    container = await new PostgreSqlContainer('postgres:17-alpine').start();
    const uri = container.getConnectionUri();
    const setup = new Pool({ connectionString: uri });
    await applySchema(setup, FLOW_STORE_DDL);
    await setup.end();

    console.log(c.d(`  node ${process.version} · ${process.platform}/${process.arch}\n`));

    await suiteA(uri);
    if (!SKIP_PGBOSS) await suiteB(uri);

    console.log(
      c.d(
        '\n  Handlers are no-ops: this is engine overhead per step, not your workload.\n' +
          '  Numbers are machine-specific — Docker on macOS is markedly slower than a Linux host.\n',
      ),
    );
  } finally {
    await container?.stop();
  }
}

await main();
