import type { Pool } from 'pg';
import type { FlowObserver, FlowEvent, FlowEventType, WorkflowId } from '../core';
import { createSchemaDdl } from './ddl';
import { type SqlExecutor, poolExecutor, toExecutor } from './executor';

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

export interface EventSinkDeps {
  /** How the sink talks to Postgres (pool-backed, RLS-scoped, …). */
  exec: SqlExecutor;
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

/**
 * A `FlowObserver` that appends engine events to `flow_step_event` through an
 * injected {@link SqlExecutor}, so a host can run the write under Row Level
 * Security (inject an executor that sets the tenant GUC) instead of a plain pool.
 * `record` is fire-and-forget (never throws, never blocks the engine); `flush()`
 * awaits the in-flight inserts.
 */
export function createEventSink(deps: EventSinkDeps): PgEventSink {
  const { exec, partitionKey } = deps;
  const schema = deps.schema ?? 'public';
  const EVT = `${schema}.flow_step_event`;
  const pending = new Set<Promise<unknown>>();

  return {
    record(event: FlowEvent) {
      // Guard the host's partition: only persist events for this sink's partition.
      if (event.partitionKey !== partitionKey) return;
      const p = exec
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

/**
 * A `FlowObserver` backed by a `pg` {@link Pool} — the batteries-included sink
 * (via {@link poolExecutor}). Hosts that need Row Level Security should build an
 * executor and call {@link createEventSink} instead.
 */
export function createPgEventSink(deps: PgEventSinkDeps): PgEventSink {
  return createEventSink({ exec: poolExecutor(deps.pool), partitionKey: deps.partitionKey, schema: deps.schema });
}

/**
 * Read a workflow's run history (gap 10), ordered oldest-first. Accepts either a
 * `pg` {@link Pool} (batteries-included) or a host's {@link SqlExecutor} (e.g.
 * RLS-scoped) — both route through the same seam.
 */
export async function readFlowEvents(
  db: Pool | SqlExecutor,
  params: { workflowId: WorkflowId; partitionKey: string; schema?: string },
): Promise<FlowEvent[]> {
  const EVT = `${params.schema ?? 'public'}.flow_step_event`;
  const res = await toExecutor(db).query<FlowEventRow>(
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

/** Raw `flow_step_event` row as returned by `pg` (bigint→string, integer→number, timestamptz→Date). */
type FlowEventRow = {
  partition_key: string;
  workflow_id: string;
  workflow_type: string | null;
  step_id: string | null;
  step_key: string | null;
  step_type: string | null;
  event_type: string;
  attempt: number | null;
  duration_ms: number | null;
  error: string | null;
  at: Date | string;
};
