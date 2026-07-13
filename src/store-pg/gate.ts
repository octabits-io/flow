import type { Pool } from 'pg';
import type { StepGate, StepGateRequest, StepGateDecision, ConcurrencyRule, RateRule } from '../core';
import { createSchemaDdl } from './ddl';
import { type SqlExecutor, poolExecutor } from './executor';

// ============================================================================
// Postgres StepGate adapter (shared, multi-worker)
// ============================================================================
//
// A shared-store implementation of the core `StepGate`, so concurrency caps and
// rate limits are *global* across every worker process (unlike the per-process
// in-memory gate). Two tables:
//
//   flow_rate_bucket  — one token-bucket row per (partition, step_type); refill +
//                       decrement happen atomically in a single UPDATE (row lock).
//   flow_step_lease   — one row per held concurrency slot; crash-safe via expires_at
//                       (a worker that dies holding a slot has its lease time out).
//
// Acquire reserves a concurrency lease first (advisory-locked, so count-then-insert
// is race-free), then consumes a rate token; a rate denial releases the lease.

const DEFAULT_RATE_TABLE = 'flow_rate_bucket';
const DEFAULT_LEASE_TABLE = 'flow_step_lease';

/** DDL for the gate's two tables. Apply once at deploy time (or via `applySchema`). */
export function flowGateDdl(
  opts: { schema?: string; rateBucketTable?: string; leaseTable?: string } = {},
): string {
  const schema = opts.schema ?? 'public';
  const rateName = opts.rateBucketTable ?? DEFAULT_RATE_TABLE;
  const leaseName = opts.leaseTable ?? DEFAULT_LEASE_TABLE;
  const rate = `${schema}.${rateName}`;
  const lease = `${schema}.${leaseName}`;
  return `
${createSchemaDdl(schema)}CREATE TABLE IF NOT EXISTS ${rate} (
  partition_key text             NOT NULL,
  step_type     text             NOT NULL,
  tokens        double precision NOT NULL,
  updated_at    timestamptz      NOT NULL DEFAULT now(),
  PRIMARY KEY (partition_key, step_type)
);

CREATE TABLE IF NOT EXISTS ${lease} (
  partition_key text        NOT NULL,
  step_type     text        NOT NULL,
  step_id       bigint      NOT NULL,
  acquired_at   timestamptz NOT NULL DEFAULT now(),
  expires_at    timestamptz NOT NULL,
  PRIMARY KEY (partition_key, step_type, step_id)
);
CREATE INDEX IF NOT EXISTS ${leaseName}_active_idx ON ${lease} (partition_key, step_type, expires_at);
`;
}

/** Default-schema gate DDL string. */
export const FLOW_GATE_DDL = flowGateDdl();

export interface PgStepGateConfig {
  pool: Pool;
  /** Partition this gate is bound to (matches the engine/store partition). */
  partitionKey: string;
  /** Schema the gate tables live in. Default 'public'. */
  schema?: string;
  /** Per-stepType concurrency caps (held leases). */
  concurrency?: Record<string, ConcurrencyRule>;
  /** Per-stepType token-bucket rate limits. */
  rateLimit?: Record<string, RateRule>;
  /** Lease TTL in seconds — crash-safety for held concurrency slots. Default 600. */
  leaseTtlSeconds?: number;
  /** Retry delay (seconds) returned when a concurrency cap is hit. Default 1. */
  concurrencyRetrySeconds?: number;
  /** Table name overrides. */
  tables?: { rateBucket?: string; lease?: string };
}

export interface StepGateConfig extends Omit<PgStepGateConfig, 'pool'> {
  /** How the gate talks to Postgres (pool-backed, RLS-scoped, …). */
  exec: SqlExecutor;
}

/** Sentinel thrown to roll back the acquire transaction when the concurrency cap is hit. */
const CAP_HIT = Symbol('flow.gate.capHit');

/**
 * A Postgres-backed {@link StepGate} addressing all SQL through an injected
 * {@link SqlExecutor}, so a host can run the concurrency/rate SQL under Row Level
 * Security (inject an executor that sets the tenant GUC) instead of a plain pool.
 * Global concurrency caps + rate limits shared across all workers; drop-in for
 * the in-memory gate via `WorkflowEngineDeps.gate`.
 */
export function createStepGate(config: StepGateConfig): StepGate {
  const { exec, partitionKey } = config;
  const schema = config.schema ?? 'public';
  const RATE = `${schema}.${config.tables?.rateBucket ?? DEFAULT_RATE_TABLE}`;
  const LEASE = `${schema}.${config.tables?.lease ?? DEFAULT_LEASE_TABLE}`;
  const leaseTtl = Math.max(1, config.leaseTtlSeconds ?? 600);
  const concurrencyRetry = Math.max(1, config.concurrencyRetrySeconds ?? 1);

  async function releaseLease(req: StepGateRequest): Promise<void> {
    await exec.query(
      `DELETE FROM ${LEASE} WHERE partition_key = $1 AND step_type = $2 AND step_id = $3`,
      [partitionKey, req.stepType, req.stepId],
    );
  }

  /** Reserve a concurrency slot atomically. Returns whether a slot was granted. */
  async function acquireLease(req: StepGateRequest, cap: number): Promise<boolean> {
    try {
      return await exec.transaction(async (tx) => {
        // Serialize acquire for this (partition, step_type) so count-then-insert is race-free.
        // The advisory lock is transaction-scoped — released on COMMIT/ROLLBACK.
        await tx.query('SELECT pg_advisory_xact_lock(hashtext($1))', [`${partitionKey}:${req.stepType}`]);
        await tx.query(
          `DELETE FROM ${LEASE} WHERE partition_key = $1 AND step_type = $2 AND expires_at < now()`,
          [partitionKey, req.stepType],
        );
        // Exclude this step's own lease so a re-delivery re-acquires its slot idempotently.
        const countRes = await tx.query<{ n: number }>(
          `SELECT count(*)::int AS n FROM ${LEASE} WHERE partition_key = $1 AND step_type = $2 AND step_id <> $3`,
          [partitionKey, req.stepType, req.stepId],
        );
        const active = countRes.rows[0]?.n ?? 0;
        // Throw the sentinel to roll back (dropping the expired-lease cleanup too),
        // preserving the prior pool-based behavior exactly.
        if (active >= cap) throw CAP_HIT;
        await tx.query(
          `INSERT INTO ${LEASE} (partition_key, step_type, step_id, expires_at)
           VALUES ($1, $2, $3, now() + ($4 || ' seconds')::interval)
           ON CONFLICT (partition_key, step_type, step_id)
           DO UPDATE SET acquired_at = now(), expires_at = EXCLUDED.expires_at`,
          [partitionKey, req.stepType, req.stepId, String(leaseTtl)],
        );
        return true;
      });
    } catch (e) {
      if (e === CAP_HIT) return false;
      throw e;
    }
  }

  /** Consume a rate token atomically (refill-then-decrement in one UPDATE). */
  async function consumeToken(req: StepGateRequest, rule: RateRule): Promise<boolean> {
    const capacity = rule.burst ?? Math.max(1, Math.ceil(rule.perSecond));
    const res = await exec.query(
      `INSERT INTO ${RATE} AS bucket (partition_key, step_type, tokens, updated_at)
       VALUES ($1, $2, $3, now())
       ON CONFLICT (partition_key, step_type) DO UPDATE
       SET tokens = LEAST($4, bucket.tokens + EXTRACT(EPOCH FROM (now() - bucket.updated_at)) * $5) - 1,
           updated_at = now()
       WHERE LEAST($4, bucket.tokens + EXTRACT(EPOCH FROM (now() - bucket.updated_at)) * $5) >= 1
       RETURNING tokens`,
      [partitionKey, req.stepType, capacity - 1, capacity, rule.perSecond],
    );
    return (res.rowCount ?? 0) > 0;
  }

  return {
    async acquire(req): Promise<StepGateDecision> {
      const cRule = config.concurrency?.[req.stepType];
      const rRule = config.rateLimit?.[req.stepType];

      // 1) concurrency lease (reserved first so a rate denial doesn't waste a slot)
      let leaseHeld = false;
      if (cRule) {
        const granted = await acquireLease(req, cRule.maxConcurrent);
        if (!granted) return { admitted: false, retryAfterSeconds: concurrencyRetry };
        leaseHeld = true;
      }

      // 2) rate token (throughput)
      if (rRule) {
        const ok = await consumeToken(req, rRule);
        if (!ok) {
          if (leaseHeld) await releaseLease(req);
          return { admitted: false, retryAfterSeconds: Math.max(1, Math.ceil(1 / rRule.perSecond)) };
        }
      }

      return {
        admitted: true,
        release: async () => {
          if (leaseHeld) await releaseLease(req);
        },
      };
    },
  };
}

/**
 * A Postgres-backed {@link StepGate} over a `pg` {@link Pool} — the
 * batteries-included adapter (via {@link poolExecutor}). Hosts that need Row
 * Level Security should build an executor and call {@link createStepGate}.
 */
export function createPgStepGate(config: PgStepGateConfig): StepGate {
  const { pool, ...rest } = config;
  return createStepGate({ exec: poolExecutor(pool), ...rest });
}
