---
title: Postgres & pg-boss
description: "Production wiring: the Postgres store, the pg-boss dispatcher, workers, DLQ and cron."
---

The durable setup swaps the in-memory store for Postgres and the in-process queue for pg-boss.
**One-time:** apply the DDL. **Per process:** build the engine, start a step worker (drives
`executeStep`), a DLQ worker (handles exhausted jobs), and optionally a cron scheduler.

```ts
import { Pool } from 'pg';
import PgBoss from 'pg-boss';
import {
  createWorkflowEngine,
  createStepHandlerRegistry,
} from 'octaflow';
import {
  createPgWorkflowStore,
  createPgStepGate,
  createPgEventSink,
  applySchema,
  FLOW_STORE_DDL,
  FLOW_GATE_DDL,
  FLOW_EVENT_DDL,
} from 'octaflow/store-pg';
import {
  createPgBossDispatcher,
  createPgBossStepWorker,
  createPgBossDlqWorker,
} from 'octaflow/dispatcher-pgboss';

const partitionKey = 'tenant-42';
const queueName = 'flow-steps';

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const boss = new PgBoss({ connectionString: process.env.DATABASE_URL });
await boss.start();

// One-time schema setup (idempotent CREATE TABLE IF NOT EXISTS …).
// `applySchema` is a dev/test convenience — in production, paste the DDL into your
// own migration system instead so schema changes stay reviewed and versioned.
await applySchema(pool, FLOW_STORE_DDL);
await applySchema(pool, FLOW_GATE_DDL);
await applySchema(pool, FLOW_EVENT_DDL);

// Recommended for shared databases: keep the flow tables in their own Postgres
// schema, and grant access on it only to the worker's role. That isolates the
// engine's state from app tables without any row-level-security choreography.
//   await applySchema(pool, flowStoreDdl('flow'));           // emits CREATE SCHEMA IF NOT EXISTS
//   await applySchema(pool, flowGateDdl({ schema: 'flow' }));
//   await applySchema(pool, flowEventDdl('flow'));
//   … then pass `schema: 'flow'` to createPgWorkflowStore / createPgStepGate / createPgEventSink.

// Per-partition engine.
const store = createPgWorkflowStore({ pool, partitionKey });
const dispatcher = createPgBossDispatcher({ boss, queueName, partitionKey });
const gate = createPgStepGate({ pool, partitionKey, concurrency: { 'ai:generate': { maxConcurrent: 3 } } });
const observer = createPgEventSink({ pool, partitionKey }); // run history → flow_step_event
const registry = createStepHandlerRegistry();
const engine = createWorkflowEngine({ store, dispatcher, registry, partitionKey, gate, observer });

myWorkflow.register(registry);

// Step worker: pull a job, run it. Throwing triggers a pg-boss retry; exhaustion → DLQ.
// Each job settles on its own outcome, so one failing step never drags its batch along.
const worker = createPgBossStepWorker({
  boss,
  queueName,
  workerOptions: {
    batchSize: 25,
    burstWhenBatchFull: true, // keep fetching while batches come back full — see Performance
    concurrency: 8,           // steps run at once from one batch; each holds a store connection
  },
});
await worker.start(async (payload) => {
  await engine.executeStep(payload.workflowId, payload.stepId);
});

// DLQ worker: a job that exhausted retries — mark the step terminally failed.
const dlq = createPgBossDlqWorker({ boss, queueName });
await dlq.start(async (payload) => {
  await engine.handleStepExhausted(payload.workflowId, payload.stepId, 'retries exhausted');
});

// Start work — the dispatcher enqueues, the worker drives it. No manual drain.
await myWorkflow.start(engine, { /* input */ });
```

Multi-tenant: build **one engine + store + dispatcher per partition**, all sharing the same
pool/boss. The step worker reads `payload.partitionKey` and routes to that partition's engine.

See [`examples/12-postgres-pgboss-production.ts`](https://github.com/octabits-io/octaflow/blob/main/examples/12-postgres-pgboss-production.ts).

## Cron / scheduled starts

```ts
import { createPgBossScheduler, createPgBossStartWorker } from 'octaflow/dispatcher-pgboss';

const scheduler = createPgBossScheduler({ boss, queueName: 'flow-starts', partitionKey });
await scheduler.schedule({ key: 'nightly', cron: '0 3 * * *', workflowType: 'enrichment', input: { full: true } });

// A start worker turns each cron tick into a workflow start (host maps type → definition).
const starter = createPgBossStartWorker({ boss, queueName: 'flow-starts' });
await starter.start(async (payload) => {
  const wf = workflowsByType[payload.workflowType];
  await engine.startWorkflow(wf.definition, payload.input ?? {}, { idempotencyKey: payload.idempotencyKey });
});
```
