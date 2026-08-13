// ============================================================================
// octaflow — root entry = the generic durable DAG engine (core layer)
// ============================================================================
//
// The default import pulls in NO heavy dependencies (no AI SDK, no pg, no
// pg-boss). Opt into a layer via its subpath export:
//   octaflow                     → core engine (this file)
//   octaflow/ai                  → AI add-on (token/cost/quota)
//   octaflow/store-pg            → Postgres WorkflowStore adapter
//   octaflow/dispatcher-pgboss   → pg-boss Dispatcher adapter

export * from './core';
