import type { Pool, PoolClient } from 'pg';

// ============================================================================
// SQL executor seam — shared by the WorkflowStore, StepGate and event sink
// ============================================================================
//
// The Postgres adapters never open their own connections; they address all SQL
// through an injected `SqlExecutor`. The *host* decides how queries run: a plain
// pool ({@link poolExecutor}), or a connection that first sets transaction-local
// GUCs for Row Level Security. Because every adapter shares this one seam, a host
// can wire a single RLS-scoped executor across the store, gate and sink.

/** Minimal query result shape the adapters consume (a structural subset of `pg`'s `QueryResult`). */
export interface SqlResult<R> {
  rows: R[];
  rowCount: number | null;
}

/**
 * A Postgres adapter's only contact with the database. Adapters issue raw
 * parameterized SQL through this seam and never open their own connections, so
 * the *host* decides how queries run: a plain pool ({@link poolExecutor}), or a
 * connection that first sets transaction-local GUCs for Row Level Security.
 *
 * `transaction(fn)` MUST run every `fn` query on a single connection inside one
 * DB transaction; the passed executor is that same-connection handle. Adapters
 * never nest transactions.
 */
export interface SqlExecutor {
  query<R = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<SqlResult<R>>;
  transaction<T>(fn: (tx: SqlExecutor) => Promise<T>): Promise<T>;
}

/**
 * Build a {@link SqlExecutor} over a `pg` {@link Pool}: top-level queries run on
 * the pool (autocommit); `transaction` checks out a client and wraps `fn` in
 * `BEGIN`/`COMMIT` (rolling back and always releasing on error). This is the
 * batteries-included executor used by the `createPg*` adapters.
 */
export function poolExecutor(pool: Pool): SqlExecutor {
  const fromClient = (client: PoolClient): SqlExecutor => ({
    async query(sql, params) {
      const res = await client.query(sql, params as unknown[]);
      return { rows: res.rows, rowCount: res.rowCount };
    },
    // Already inside a transaction — reuse the same connection; adapters never nest.
    transaction(fn) {
      return fn(fromClient(client));
    },
  });

  return {
    async query(sql, params) {
      const res = await pool.query(sql, params as unknown[]);
      return { rows: res.rows, rowCount: res.rowCount };
    },
    async transaction(fn) {
      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        const result = await fn(fromClient(client));
        await client.query('COMMIT');
        return result;
      } catch (e) {
        await client.query('ROLLBACK').catch(() => {});
        throw e;
      } finally {
        client.release();
      }
    },
  };
}

/**
 * Normalize a `Pool | SqlExecutor` argument to a {@link SqlExecutor}. Lets the
 * read helpers accept either the batteries-included pool or a host's RLS-scoped
 * executor without a breaking signature change. `SqlExecutor` is discriminated by
 * its `transaction` method (a `pg` `Pool` has none).
 */
export function toExecutor(db: Pool | SqlExecutor): SqlExecutor {
  return 'transaction' in db ? db : poolExecutor(db);
}
