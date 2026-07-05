/**
 * Schema for the Postgres WorkflowStore adapter. Two tables mirror flow-core's
 * record shapes. Apply once at deploy time (or call `applySchema`).
 *
 * `partition_key` is the generic multi-tenancy column (the engine/store are bound
 * to one partition). Add Row Level Security on top if you need hard isolation.
 */
export function flowStoreDdl(schema = 'public'): string {
  const wf = `${schema}.flow_workflow`;
  const step = `${schema}.flow_workflow_step`;
  return `
${createSchemaDdl(schema)}CREATE TABLE IF NOT EXISTS ${wf} (
  id              bigserial PRIMARY KEY,
  partition_key   text        NOT NULL,
  type            text        NOT NULL,
  status          text        NOT NULL,
  input           jsonb       NOT NULL DEFAULT '{}'::jsonb,
  output          jsonb,
  error           text,
  entity_ref      text,
  total_steps     integer     NOT NULL,
  completed_steps integer     NOT NULL DEFAULT 0,
  failed_steps    integer     NOT NULL DEFAULT 0,
  metadata        jsonb,
  idempotency_key text,
  parent_workflow_id bigint,
  parent_step_id     bigint,
  created_at      timestamptz NOT NULL DEFAULT now(),
  started_at      timestamptz,
  completed_at    timestamptz
);

CREATE INDEX IF NOT EXISTS flow_workflow_partition_status_idx ON ${wf} (partition_key, status);
CREATE INDEX IF NOT EXISTS flow_workflow_parent_idx          ON ${wf} (parent_workflow_id);
CREATE INDEX IF NOT EXISTS flow_workflow_partition_type_idx   ON ${wf} (partition_key, type);
CREATE INDEX IF NOT EXISTS flow_workflow_partition_entity_idx ON ${wf} (partition_key, entity_ref);
CREATE UNIQUE INDEX IF NOT EXISTS flow_workflow_idempotency_idx ON ${wf} (partition_key, idempotency_key) WHERE idempotency_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS ${step} (
  id            bigserial PRIMARY KEY,
  workflow_id   bigint      NOT NULL REFERENCES ${wf}(id) ON DELETE CASCADE,
  partition_key text        NOT NULL,
  key           text        NOT NULL,
  type          text        NOT NULL,
  status        text        NOT NULL,
  dependencies  jsonb       NOT NULL DEFAULT '[]'::jsonb,
  input         jsonb,
  output        jsonb,
  error         text,
  metadata      jsonb,
  attempts      integer     NOT NULL DEFAULT 0,
  parent_step_id bigint     REFERENCES ${step}(id) ON DELETE CASCADE,
  started_at    timestamptz,
  completed_at  timestamptz,
  UNIQUE (workflow_id, key)
);

CREATE INDEX IF NOT EXISTS flow_workflow_step_workflow_idx ON ${step} (workflow_id);
CREATE INDEX IF NOT EXISTS flow_workflow_step_status_idx   ON ${step} (workflow_id, status);
CREATE INDEX IF NOT EXISTS flow_workflow_step_parent_idx   ON ${step} (parent_step_id);
`;
}

/** Default-schema DDL string. */
export const FLOW_STORE_DDL = flowStoreDdl();

/** `CREATE SCHEMA IF NOT EXISTS` line for non-default schemas; empty for 'public'. */
export function createSchemaDdl(schema: string): string {
  return schema === 'public' ? '' : `CREATE SCHEMA IF NOT EXISTS ${schema};\n\n`;
}
