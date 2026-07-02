/**
 * 12 — Production wiring: Postgres store + gate + event sink + pg-boss dispatcher + cron
 *
 * Reference setup for a durable, multi-process deployment. Unlike the in-memory examples there's
 * no manual drain: the pg-boss step worker pulls jobs and drives `engine.executeStep`, retries
 * and durable sleeps are real (queue `startAfterSeconds`), and the cron scheduler starts
 * workflows on a schedule.
 *
 * Needs a running Postgres (DATABASE_URL). Requires the optional peers: `pg`, `pg-boss`.
 */
import { Pool } from 'pg';
import PgBoss from 'pg-boss';
import { z } from 'zod';
import {
  createWorkflowEngine,
  createStepHandlerRegistry,
  defineStep,
  buildWorkflow,
} from '@octabits-io/flow';
import {
  createPgWorkflowStore,
  createPgStepGate,
  createPgEventSink,
  applySchema,
  readFlowEvents,
  FLOW_STORE_DDL,
  FLOW_GATE_DDL,
  FLOW_EVENT_DDL,
} from '@octabits-io/flow/store-pg';
import {
  createPgBossDispatcher,
  createPgBossStepWorker,
  createPgBossDlqWorker,
  createPgBossScheduler,
  createPgBossStartWorker,
} from '@octabits-io/flow/dispatcher-pgboss';

// --- A trivial workflow ---
const inputSchema = z.object({ x: z.number() });
const double = defineStep({
  type: 'double',
  workflowInputSchema: inputSchema,
  outputSchema: z.object({ y: z.number() }),
  handler: async (ctx) => ({ y: ctx.workflowInput.x * 2 }),
});
const wf = buildWorkflow({ type: 'doubler', inputSchema, steps: { double } });
const workflowsByType = { [wf.type]: wf };

async function main() {
  const connectionString = process.env.DATABASE_URL!;
  const partitionKey = 'tenant-42';
  const stepQueue = 'flow-steps';
  const startQueue = 'flow-starts';

  const pool = new Pool({ connectionString });
  const boss = new PgBoss({ connectionString });
  await boss.start();

  // 1. One-time schema (idempotent). Run in a migration in real apps.
  await applySchema(pool, FLOW_STORE_DDL);
  await applySchema(pool, FLOW_GATE_DDL);
  await applySchema(pool, FLOW_EVENT_DDL);

  // 2. Build the per-partition engine over Postgres + pg-boss.
  const store = createPgWorkflowStore({ pool, partitionKey });
  const dispatcher = createPgBossDispatcher({ boss, queueName: stepQueue, partitionKey });
  const gate = createPgStepGate({ pool, partitionKey, concurrency: { double: { maxConcurrent: 5 } } });
  const observer = createPgEventSink({ pool, partitionKey }); // run history → flow_step_event
  const registry = createStepHandlerRegistry();
  const engine = createWorkflowEngine({ store, dispatcher, registry, partitionKey, gate, observer });
  wf.register(registry);

  // 3. Step worker — pull a job and execute it. Throwing → pg-boss retry; exhaustion → DLQ.
  const worker = createPgBossStepWorker({ boss, queueName: stepQueue });
  await worker.start(async (payload) => {
    await engine.executeStep(payload.workflowId, payload.stepId);
  });

  // 4. DLQ worker — a job that exhausted its retries → mark the step terminally failed.
  const dlq = createPgBossDlqWorker({ boss, queueName: stepQueue });
  await dlq.start(async (payload) => {
    await engine.handleStepExhausted(payload.workflowId, payload.stepId, 'retries exhausted');
  });

  // 5. (Optional) cron — start a workflow on a schedule. A start worker turns each tick into a start.
  const scheduler = createPgBossScheduler({ boss, queueName: startQueue, partitionKey });
  await scheduler.schedule({ key: 'nightly', cron: '0 3 * * *', workflowType: wf.type, input: { x: 21 } });

  const starter = createPgBossStartWorker({ boss, queueName: startQueue });
  await starter.start(async (payload) => {
    const target = workflowsByType[payload.workflowType];
    if (!target) return;
    await engine.startWorkflow(target.definition, payload.input ?? {}, { idempotencyKey: payload.idempotencyKey });
  });

  // 6. Start work now — the worker drives it; poll for completion.
  const started = await wf.start(engine, { x: 21 });
  if (!started.ok) throw new Error(started.error.message);

  // (In a real service you wouldn't poll — you'd react to it. Shown here for the example.)
  for (let i = 0; i < 50; i++) {
    const status = await engine.getWorkflowStatus(started.value.workflowId);
    if (status.ok && (status.value.status === 'completed' || status.value.status === 'failed')) {
      console.log('status =', status.value.status, JSON.stringify(status.value.output));
      const history = await readFlowEvents(pool, { workflowId: started.value.workflowId, partitionKey });
      console.log('run history:', history.map((e) => e.type).join(', '));
      break;
    }
    await new Promise((r) => setTimeout(r, 100));
  }

  await worker.stop();
  await dlq.stop();
  await starter.stop();
  await boss.stop();
  await pool.end();
}

main();
