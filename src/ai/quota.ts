/**
 * AI workflow quota enforcement — a store-agnostic engine.
 *
 * Guards a workflow start against three windows: max concurrent in-flight
 * workflows, a per-day cap, and a per-month cap. The engine owns the window
 * math and the enforcement order; the raw counts come through a narrow
 * {@link AiQuotaStore} seam (the consumer implements it with SQL — this layer
 * never touches a database), and the limits come from an injected
 * {@link AiQuotaConfigResolver} callback (not a tenant service).
 *
 * Scoping is generic: everything is keyed by `partitionKey`, the same partition
 * vocabulary flow-core uses. Tenancy is entirely the consumer's concern — a
 * multi-tenant host passes a tenant id as the partition key; a single-tenant
 * host passes a constant.
 */
import type { Result } from '../core';
import { err, ok } from '../core';
import { toIsoDate, monthStartOf } from './usage-window';

// ============================================================================
// Config
// ============================================================================

/**
 * Resolved quota limits for a scope. `null` on any window means **unlimited**
 * for that window (the check is skipped). The {@link AiQuotaConfigResolver} may
 * return `null` for the whole config to exempt a scope entirely (e.g. a
 * bring-your-own-key caller that shouldn't be metered against platform quota).
 */
export interface AiQuotaConfig {
  /** Max simultaneously-running workflows. `null` = unlimited. */
  maxConcurrentWorkflows: number | null;
  /** Max workflows started per calendar day (UTC). `null` = unlimited. */
  maxWorkflowsPerDay: number | null;
  /** Max workflows started per calendar month (UTC). `null` = unlimited. */
  maxWorkflowsPerMonth: number | null;
}

/**
 * Convenience defaults a consumer can spread over partial per-scope limits.
 * flow does **not** apply these automatically — resolution policy lives in the
 * injected {@link AiQuotaConfigResolver}.
 */
export const DEFAULT_AI_QUOTA: AiQuotaConfig = {
  maxConcurrentWorkflows: 3,
  maxWorkflowsPerDay: 50,
  maxWorkflowsPerMonth: 500,
};

/**
 * Resolves the quota limits for a scope. Returning `null` exempts the scope
 * from all quota enforcement (the check short-circuits to success).
 */
export type AiQuotaConfigResolver = (
  partitionKey: string,
) => AiQuotaConfig | null | Promise<AiQuotaConfig | null>;

// ============================================================================
// Store seam
// ============================================================================

/** A `[startDate, endDate]` inclusive query over the daily-usage rollup (dates are `YYYY-MM-DD`). */
export interface AiUsageCountQuery {
  partitionKey: string;
  /** Inclusive lower bound, `YYYY-MM-DD`. */
  startDate: string;
  /** Inclusive upper bound, `YYYY-MM-DD`. */
  endDate: string;
}

/**
 * The reads quota enforcement needs from storage. The consumer implements these
 * with its own queries (over the in-flight workflow table and the daily-usage
 * rollup); flow supplies no implementation. Any workflow-type filtering policy
 * (e.g. excluding synthetic embedding rows from the count) lives in the
 * consumer's SQL, keeping this layer policy-agnostic.
 */
export interface AiQuotaStore {
  /** Number of workflows currently `running` for the scope. */
  countRunningWorkflows(partitionKey: string): Promise<number>;
  /** Sum of `workflowCount` in the daily rollup over the inclusive date range. */
  sumWorkflowCount(query: AiUsageCountQuery): Promise<number>;
}

// ============================================================================
// Error
// ============================================================================

/** Reason a start was rejected — which window tripped. */
export type AiQuotaExceededReason = 'concurrent_limit' | 'daily_limit' | 'monthly_limit';

/** Expected error value when a start is rejected by quota. Extends flow's structural error. */
export interface AiQuotaExceededError {
  key: 'ai_quota_exceeded';
  reason: AiQuotaExceededReason;
  message: string;
  /** The limit that was hit. */
  limit: number;
  /** The observed count at rejection (includes in-flight runs for the day/month windows). */
  current: number;
}

// ============================================================================
// Factory
// ============================================================================

export interface CreateAiQuotaServiceDeps {
  /** Count reads over the in-flight workflow table + daily rollup. */
  store: AiQuotaStore;
  /** Per-scope limit resolver. Return `null` to exempt the scope. */
  getQuota: AiQuotaConfigResolver;
  /** Clock injection for window computation (tests). Defaults to `() => new Date()`. */
  now?: () => Date;
}

export interface AiQuotaService {
  /**
   * Check all quota windows for the scope. Resolves `ok` when the start is
   * allowed, or an {@link AiQuotaExceededError} naming the window that tripped.
   * The day and month counts include currently-running (not-yet-aggregated)
   * workflows, matching the source semantics.
   */
  checkQuota(partitionKey: string): Promise<Result<void, AiQuotaExceededError>>;
}

/**
 * Build a store-agnostic AI quota service. Enforcement order is concurrent →
 * day → month; the first tripped window short-circuits with its error.
 */
export function createAiQuotaService(deps: CreateAiQuotaServiceDeps): AiQuotaService {
  const now = deps.now ?? (() => new Date());

  async function checkQuota(partitionKey: string): Promise<Result<void, AiQuotaExceededError>> {
    const config = await deps.getQuota(partitionKey);
    // Null config → scope is fully exempt from quota (e.g. BYOK).
    if (!config) return ok(undefined);

    const running = await deps.store.countRunningWorkflows(partitionKey);

    // 1. Concurrent workflows.
    if (config.maxConcurrentWorkflows !== null && running >= config.maxConcurrentWorkflows) {
      return err({
        key: 'ai_quota_exceeded',
        reason: 'concurrent_limit',
        message: `Maximum concurrent AI workflows reached (${config.maxConcurrentWorkflows}). Please wait for running workflows to complete.`,
        limit: config.maxConcurrentWorkflows,
        current: running,
      });
    }

    const today = toIsoDate(now());

    // 2. Daily limit. Include running workflows (not yet rolled up).
    if (config.maxWorkflowsPerDay !== null) {
      const todaySum = await deps.store.sumWorkflowCount({ partitionKey, startDate: today, endDate: today });
      const todayCount = todaySum + running;
      if (todayCount >= config.maxWorkflowsPerDay) {
        return err({
          key: 'ai_quota_exceeded',
          reason: 'daily_limit',
          message: `Daily AI workflow limit reached (${config.maxWorkflowsPerDay}). Try again tomorrow.`,
          limit: config.maxWorkflowsPerDay,
          current: todayCount,
        });
      }
    }

    // 3. Monthly limit. Include running workflows (not yet rolled up).
    if (config.maxWorkflowsPerMonth !== null) {
      const monthStart = monthStartOf(today);
      const monthSum = await deps.store.sumWorkflowCount({ partitionKey, startDate: monthStart, endDate: today });
      const monthCount = monthSum + running;
      if (monthCount >= config.maxWorkflowsPerMonth) {
        return err({
          key: 'ai_quota_exceeded',
          reason: 'monthly_limit',
          message: `Monthly AI workflow limit reached (${config.maxWorkflowsPerMonth}). Contact support to increase your limit.`,
          limit: config.maxWorkflowsPerMonth,
          current: monthCount,
        });
      }
    }

    return ok(undefined);
  }

  return { checkQuota };
}
