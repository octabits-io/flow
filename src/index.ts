// ============================================================================
// @octabits-io/flow — root entry = the generic durable DAG engine (core layer)
// ============================================================================
//
// The default import pulls in NO heavy dependencies (no AI SDK, no pg, no
// pg-boss). Opt into a layer via its subpath export:
//   @octabits-io/flow                     → core engine (this file)
//   @octabits-io/flow/ai                  → AI add-on (token/cost/quota)
//   @octabits-io/flow/store-pg            → Postgres WorkflowStore adapter
//   @octabits-io/flow/dispatcher-pgboss   → pg-boss Dispatcher adapter

export * from './core';
