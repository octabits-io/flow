import type { Pool } from 'pg';
import type { FlowObserver, FlowEvent, FlowEventType, WorkflowId } from '../core';
import { createSchemaDdl } from './ddl';

/**
 * DDL for the append-only run-history table (gap 10). Apply once at deploy time (alongside
 * `flowStoreDdl`), or call `applySchema(pool, flowEventDdl())`.
 */
export function flowEventDdl(schema = 'public'): string {
  const evt = `${schema}.flow_step_event`;
  return `
${createSchemaDdl(schema)}CREATE TABLE IF NOT EXISTS ${evt} (
  id            bigserial PRIMARY KEY,
  partition_key text        NOT NULL,
  workflow_id   bigint      NOT NULL,
  workflow_type text,
  step_id       bigint,
  step_key      text,
  step_type     text,
  event_type    text        NOT NULL,
  attempt       integer,
  duration_ms   integer,
  error         text,
  at            timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS flow_step_event_workflow_idx ON ${evt} (partition_key, workflow_id, id);
`;
}

/** Default-schema event-table DDL string. */
export const FLOW_EVENT_DDL = flowEventDdl();

export interface PgEventSinkDeps {
  pool: Pool;
  /** Partition this sink is bound to (e.g. a tenant id). */
  partitionKey: string;
  /** Schema the flow tables live in. Default 'public'. */
  schema?: string;
}

/**
 * A `FlowObserver` that appends every engine event to `flow_step_event` (gap 10). `record` is
 * fire-and-forget (it never throws and never blocks the engine); call `flush()` to await the
 * in-flight inserts — useful in tests and before shutdown.
 */
export interface PgEventSink extends FlowObserver {
  flush(): Promise<void>;
}

export function createPgEventSink(deps: PgEventSinkDeps): PgEventSink {
  const { pool, partitionKey } = deps;
  const schema = deps.schema ?? 'public';
  const EVT = `${schema}.flow_step_event`;
  const pending = new Set<Promise<unknown>>();

  return {
    record(event: FlowEvent) {
      // Guard the host's partition: only persist events for this sink's partition.
      if (event.partitionKey !== partitionKey) return;
      const p = pool
        .query(
          `INSERT INTO ${EVT}
             (partition_key, workflow_id, workflow_type, step_id, step_key, step_type, event_type, attempt, duration_ms, error, at)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
          [
            partitionKey,
            event.workflowId,
            event.workflowType ?? null,
            event.stepId ?? null,
            event.stepKey ?? null,
            event.stepType ?? null,
            event.type,
            event.attempt ?? null,
            event.durationMs ?? null,
            event.error ?? null,
            event.at,
          ],
        )
        .catch(() => {
          /* best-effort: a history-write failure must never break a run */
        })
        .finally(() => {
          pending.delete(p);
        });
      pending.add(p);
    },
    async flush() {
      await Promise.all(Array.from(pending));
    },
  };
}

/** Read a workflow's run history (gap 10), ordered oldest-first. */
export async function readFlowEvents(
  pool: Pool,
  params: { workflowId: WorkflowId; partitionKey: string; schema?: string },
): Promise<FlowEvent[]> {
  const EVT = `${params.schema ?? 'public'}.flow_step_event`;
  const res = await pool.query(
    `SELECT * FROM ${EVT} WHERE partition_key = $1 AND workflow_id = $2 ORDER BY id`,
    [params.partitionKey, params.workflowId],
  );
  return res.rows.map((r) => ({
    type: r.event_type as FlowEventType,
    at: r.at instanceof Date ? r.at.toISOString() : String(r.at),
    partitionKey: r.partition_key,
    workflowId: Number(r.workflow_id),
    workflowType: r.workflow_type ?? undefined,
    stepId: r.step_id == null ? undefined : Number(r.step_id),
    stepKey: r.step_key ?? undefined,
    stepType: r.step_type ?? undefined,
    attempt: r.attempt == null ? undefined : Number(r.attempt),
    durationMs: r.duration_ms == null ? undefined : Number(r.duration_ms),
    error: r.error ?? undefined,
  }));
}
