import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { z } from 'zod';
import { Pool } from 'pg';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import {
  createWorkflowEngine,
  createStepHandlerRegistry,
  defineStep,
  buildWorkflow,
  type Dispatcher,
  type DispatchStepPayload,
} from '../core';
import {
  createPgWorkflowStore,
  createPgStepGate,
  createPgEventSink,
  readFlowEvents,
  applySchema,
  flowStoreDdl,
  flowGateDdl,
  flowEventDdl,
} from './index';

const SCHEMA = 'flow';

let container: StartedPostgreSqlContainer;
let pool: Pool;

beforeAll(async () => {
  container = await new PostgreSqlContainer('postgres:17-alpine').start();
  pool = new Pool({ connectionString: container.getConnectionUri() });
  await applySchema(pool, flowStoreDdl(SCHEMA));
  await applySchema(pool, flowGateDdl({ schema: SCHEMA }));
  await applySchema(pool, flowEventDdl(SCHEMA));
});

afterAll(async () => {
  await pool?.end();
  await container?.stop();
});

describe('store-pg in a dedicated schema (integration)', () => {
  it('creates all five tables in the target schema and none in public', async () => {
    const res = await pool.query(
      `SELECT table_schema, table_name FROM information_schema.tables
       WHERE table_name LIKE 'flow\\_%' ORDER BY table_name`,
    );
    const byName = Object.fromEntries(res.rows.map((r) => [r.table_name, r.table_schema]));
    expect(byName).toEqual({
      flow_workflow: SCHEMA,
      flow_workflow_step: SCHEMA,
      flow_step_event: SCHEMA,
      flow_rate_bucket: SCHEMA,
      flow_step_lease: SCHEMA,
    });
  });

  it('runs a workflow end-to-end with store, gate, and event sink all schema-bound', async () => {
    const store = createPgWorkflowStore({ pool, partitionKey: 'iso', schema: SCHEMA });
    const gate = createPgStepGate({
      pool,
      partitionKey: 'iso',
      schema: SCHEMA,
      concurrency: { work: { maxConcurrent: 1 } },
      rateLimit: { work: { perSecond: 100, burst: 100 } },
    });
    const observer = createPgEventSink({ pool, partitionKey: 'iso', schema: SCHEMA });

    const input = z.object({ n: z.number() });
    const work = defineStep<{ n: number }, { doubled: number }, undefined>({
      type: 'work',
      workflowInputSchema: input,
      outputSchema: z.object({ doubled: z.number() }),
      handler: async (ctx) => ({ doubled: ctx.workflowInput.n * 2 }),
    });
    const wf = buildWorkflow<{ n: number }, undefined>({
      type: 'iso-test',
      inputSchema: input,
      steps: { work },
    });

    const registry = createStepHandlerRegistry<undefined>();
    wf.register(registry);
    const queue: DispatchStepPayload[] = [];
    const dispatcher: Dispatcher = {
      async enqueueStep(p) {
        queue.push(p);
        return { ok: true, value: undefined };
      },
    };
    const engine = createWorkflowEngine<undefined>({
      store,
      registry,
      dispatcher,
      partitionKey: 'iso',
      gate,
      observer,
    });

    const started = await wf.start(engine, { n: 21 });
    expect(started.ok).toBe(true);
    if (!started.ok) return;
    while (queue.length > 0) {
      const p = queue.shift()!;
      await engine.executeStep(p.workflowId, p.stepId);
    }
    await observer.flush();

    const status = await engine.getWorkflowStatus(started.value.workflowId);
    expect(status.ok).toBe(true);
    if (!status.ok) return;
    expect(status.value.status).toBe('completed');
    expect(status.value.output).toEqual({ work: { doubled: 42 } });

    const events = await readFlowEvents(pool, {
      workflowId: started.value.workflowId,
      partitionKey: 'iso',
      schema: SCHEMA,
    });
    expect(events.length).toBeGreaterThan(0);
    expect(events.at(-1)?.type).toBe('workflow.completed');
  });
});
