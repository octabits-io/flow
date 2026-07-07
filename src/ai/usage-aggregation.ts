/**
 * AI token-usage + cost aggregation — a store-agnostic engine.
 *
 * Owns the shapes, the calendar-window math, and the "increment by one run"
 * derivation for daily usage rollups; the raw row access (the UPSERT and the
 * GROUP BY aggregate reads) goes through a narrow {@link AiUsageStore} seam that
 * the consumer implements with SQL. flow ships no database code here.
 *
 * Scoping is generic (`partitionKey`), and `keySource` is a free-form string —
 * the same `'platform' | 'tenant' | …` convention flow's AI hooks already use
 * for "which key paid for the run", but not fixed to any union.
 */
import type { Result, Logger } from '../core';
import { err, ok, noopLogger } from '../core';
import type { TokenUsage } from './cost';
import type { AiQuotaStore, AiUsageCountQuery } from './quota';
import { toIsoDate, monthStartOf } from './usage-window';

// ============================================================================
// Store seam
// ============================================================================

/** An additive write into the daily-usage rollup for one run (workflow completion or embedding batch). */
export interface DailyUsageDelta {
  partitionKey: string;
  /** Calendar day of the run, `YYYY-MM-DD`. */
  date: string;
  workflowType: string;
  /** Which key paid for the run (free-form; e.g. `'platform' | 'tenant'`). */
  keySource: string;
  /** How much to add to `workflowCount` — always `1` for the built-in recorders. */
  workflowCount: number;
  /** Token totals to add. Embeddings set only `inputTokens` (the rest are `0`). */
  usage: TokenUsage;
  /** Cost to add, in microdollars (1 USD = 1,000,000). */
  estimatedCostMicros: number;
}

/** Inclusive `[startDate, endDate]` range query for the aggregate reads (`YYYY-MM-DD`). */
export interface AiUsageRangeQuery {
  partitionKey: string;
  startDate: string;
  endDate: string;
}

/** One row of the per-day aggregate (GROUP BY date). */
export interface UsageSummaryRow {
  date: string;
  workflowCount: number;
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  cacheWriteTokens: number;
  estimatedCostMicros: number;
}

/** One row of the per-type aggregate (GROUP BY workflowType, keySource). */
export interface UsageByTypeRow {
  workflowType: string;
  keySource: string;
  workflowCount: number;
  inputTokens: number;
  outputTokens: number;
  estimatedCostMicros: number;
}

/**
 * The reads/writes usage aggregation needs from storage. Extends
 * {@link AiQuotaStore} so a single consumer implementation backs both the quota
 * and aggregation engines. The `addDailyUsage` UPSERT and the two aggregate
 * reads are the consumer's SQL; flow owns everything above the seam.
 */
export interface AiUsageStore extends AiQuotaStore {
  /** UPSERT-add a run's usage into the daily rollup (unique on partition+date+type+keySource). */
  addDailyUsage(delta: DailyUsageDelta): Promise<void>;
  /** Per-day totals over the range, ascending by date. */
  aggregateByDate(query: AiUsageRangeQuery): Promise<UsageSummaryRow[]>;
  /** Per-(type, keySource) totals over the range. */
  aggregateByWorkflowType(query: AiUsageRangeQuery): Promise<UsageByTypeRow[]>;
}

// ============================================================================
// Inputs / outputs
// ============================================================================

/** A completed LLM workflow's usage to roll up. */
export interface WorkflowUsageInput {
  partitionKey: string;
  /** Calendar day of completion, `YYYY-MM-DD`. */
  date: string;
  workflowType: string;
  keySource: string;
  usage: TokenUsage;
  estimatedCostMicros: number;
}

/**
 * A flushed embedding batch's usage to roll up. Embeddings have only input
 * tokens (no output, no cache); `workflowCount` increments once per flushed
 * batch, not per inner embed call.
 */
export interface EmbeddingUsageInput {
  partitionKey: string;
  /** Calendar day of the flush, `YYYY-MM-DD`. */
  date: string;
  /** Synthetic type, e.g. `embedding:index-sync`. */
  workflowType: string;
  keySource: string;
  inputTokens: number;
  estimatedCostMicros: number;
}

/** Current-window usage snapshot for a scope. */
export interface CurrentQuotaUsage {
  today: { workflowCount: number };
  thisMonth: { workflowCount: number };
  running: { count: number };
}

/** Expected error value from aggregation reads/writes. Extends flow's structural error. */
export interface AiUsageError {
  key: 'ai_usage_error';
  message: string;
}

// ============================================================================
// Factory
// ============================================================================

export interface CreateAiUsageAggregationServiceDeps {
  store: AiUsageStore;
  /** Clock injection for the current-window computation (tests). Defaults to `() => new Date()`. */
  now?: () => Date;
  /** Optional logger for write failures. Defaults to a no-op. */
  logger?: Logger;
}

export interface AiUsageAggregationService {
  /** Roll a completed LLM workflow's totals into the daily aggregate (UPSERT +1). */
  recordWorkflowCompletion(data: WorkflowUsageInput): Promise<Result<void, AiUsageError>>;
  /** Roll a flushed embedding batch into the daily aggregate (output/cache tokens = 0). */
  recordEmbeddingUsage(data: EmbeddingUsageInput): Promise<Result<void, AiUsageError>>;
  /** Per-day usage totals over an inclusive date range. */
  getUsageSummary(params: AiUsageRangeQuery): Promise<Result<UsageSummaryRow[], AiUsageError>>;
  /** Per-(type, keySource) usage totals over an inclusive date range. */
  getUsageByWorkflowType(params: AiUsageRangeQuery): Promise<Result<UsageByTypeRow[], AiUsageError>>;
  /** Today's / this-month's workflow counts and the current running count. */
  getCurrentQuotaUsage(params: { partitionKey: string }): Promise<Result<CurrentQuotaUsage, AiUsageError>>;
}

function messageOf(error: unknown, fallback: string): string {
  return error instanceof Error ? error.message : fallback;
}

/** Build a store-agnostic AI usage-aggregation service. */
export function createAiUsageAggregationService(
  deps: CreateAiUsageAggregationServiceDeps,
): AiUsageAggregationService {
  const { store } = deps;
  const now = deps.now ?? (() => new Date());
  const logger = deps.logger ?? noopLogger;

  async function recordWorkflowCompletion(data: WorkflowUsageInput): Promise<Result<void, AiUsageError>> {
    try {
      await store.addDailyUsage({
        partitionKey: data.partitionKey,
        date: data.date,
        workflowType: data.workflowType,
        keySource: data.keySource,
        workflowCount: 1,
        usage: data.usage,
        estimatedCostMicros: data.estimatedCostMicros,
      });
      return ok(undefined);
    } catch (error) {
      const message = messageOf(error, 'Unknown aggregation error');
      logger.error('Failed to record workflow completion', error instanceof Error ? error : new Error(message));
      return err({ key: 'ai_usage_error', message });
    }
  }

  async function recordEmbeddingUsage(data: EmbeddingUsageInput): Promise<Result<void, AiUsageError>> {
    try {
      await store.addDailyUsage({
        partitionKey: data.partitionKey,
        date: data.date,
        workflowType: data.workflowType,
        keySource: data.keySource,
        workflowCount: 1,
        usage: { inputTokens: data.inputTokens, outputTokens: 0, cacheReadTokens: 0, cacheWriteTokens: 0 },
        estimatedCostMicros: data.estimatedCostMicros,
      });
      return ok(undefined);
    } catch (error) {
      const message = messageOf(error, 'Unknown aggregation error');
      logger.error('Failed to record embedding usage', error instanceof Error ? error : new Error(message));
      return err({ key: 'ai_usage_error', message });
    }
  }

  async function getUsageSummary(params: AiUsageRangeQuery): Promise<Result<UsageSummaryRow[], AiUsageError>> {
    try {
      return ok(await store.aggregateByDate(params));
    } catch (error) {
      return err({ key: 'ai_usage_error', message: messageOf(error, 'Unknown query error') });
    }
  }

  async function getUsageByWorkflowType(
    params: AiUsageRangeQuery,
  ): Promise<Result<UsageByTypeRow[], AiUsageError>> {
    try {
      return ok(await store.aggregateByWorkflowType(params));
    } catch (error) {
      return err({ key: 'ai_usage_error', message: messageOf(error, 'Unknown query error') });
    }
  }

  async function getCurrentQuotaUsage(params: {
    partitionKey: string;
  }): Promise<Result<CurrentQuotaUsage, AiUsageError>> {
    try {
      const today = toIsoDate(now());
      const monthStart = monthStartOf(today);
      const { partitionKey } = params;

      const dayQuery: AiUsageCountQuery = { partitionKey, startDate: today, endDate: today };
      const monthQuery: AiUsageCountQuery = { partitionKey, startDate: monthStart, endDate: today };

      const [todayCount, monthCount, runningCount] = await Promise.all([
        store.sumWorkflowCount(dayQuery),
        store.sumWorkflowCount(monthQuery),
        store.countRunningWorkflows(partitionKey),
      ]);

      return ok({
        today: { workflowCount: todayCount },
        thisMonth: { workflowCount: monthCount },
        running: { count: runningCount },
      });
    } catch (error) {
      return err({ key: 'ai_usage_error', message: messageOf(error, 'Unknown query error') });
    }
  }

  return {
    recordWorkflowCompletion,
    recordEmbeddingUsage,
    getUsageSummary,
    getUsageByWorkflowType,
    getCurrentQuotaUsage,
  };
}
