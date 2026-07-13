import type { WorkflowId, StepId } from './types';

// ============================================================================
// Step admission gate (concurrency & rate limiting)
// ============================================================================

export interface StepGateRequest {
  partitionKey: string;
  workflowId: WorkflowId;
  stepId: StepId;
  stepKey: string;
  stepType: string;
}

export type StepGateDecision =
  | { admitted: true; release(): void | Promise<void> }
  | { admitted: false; retryAfterSeconds: number };

/**
 * Admission control consulted by the engine **before** a step runs. Return
 * `{ admitted: false, retryAfterSeconds }` to defer the step — it is re-enqueued with
 * that delay and **no attempt is consumed** — or `{ admitted: true, release }` with a
 * `release` the engine calls when the step finishes (success, terminal failure, or
 * retry-reschedule).
 *
 * The shipped {@link createInMemoryStepGate} is **per-process** — correct for a single
 * worker and for tests. For a multi-worker concurrency cap, implement this against a
 * shared store (e.g. a leases table); the contract is identical.
 */
export interface StepGate {
  acquire(req: StepGateRequest): Promise<StepGateDecision>;
}

/** Max concurrently-running steps of a given type (per process). */
export interface ConcurrencyRule {
  maxConcurrent: number;
}

/** Token-bucket throughput limit for a given step type. */
export interface RateRule {
  /** Sustained rate, tokens per second. */
  perSecond: number;
  /** Bucket capacity (max burst). Defaults to `max(1, ceil(perSecond))`. */
  burst?: number;
}

export interface InMemoryStepGateConfig {
  /** Per-stepType concurrency caps. */
  concurrency?: Record<string, ConcurrencyRule>;
  /** Per-stepType token-bucket rate limits. */
  rateLimit?: Record<string, RateRule>;
  /** Retry delay (seconds) returned when a concurrency cap is hit. Default 1. */
  concurrencyRetrySeconds?: number;
  /** Clock injection for tests (ms epoch). Defaults to `Date.now`. */
  now?: () => number;
}

/**
 * A per-process {@link StepGate}: per-stepType concurrency caps (held slots) plus
 * per-stepType token-bucket rate limits (throughput). Both are optional and checked
 * per step type; a step type with no rule is always admitted.
 *
 * Concurrency is reserved first, then the rate token — so a rate denial does not waste
 * a concurrency slot. Good for single-worker deployments; swap in a shared-store gate
 * for multi-worker concurrency control.
 */
export function createInMemoryStepGate(config: InMemoryStepGateConfig = {}): StepGate {
  const now = config.now ?? (() => Date.now());
  const concurrencyRetry = Math.max(1, config.concurrencyRetrySeconds ?? 1);
  const running = new Map<string, number>();
  const buckets = new Map<string, { tokens: number; updatedMs: number }>();

  function rateCheck(stepType: string): { ok: true } | { ok: false; retryAfterSeconds: number } {
    const rule = config.rateLimit?.[stepType];
    if (!rule) return { ok: true };
    const capacity = rule.burst ?? Math.max(1, Math.ceil(rule.perSecond));
    const tNow = now();
    let b = buckets.get(stepType);
    if (!b) {
      b = { tokens: capacity, updatedMs: tNow };
      buckets.set(stepType, b);
    }
    const elapsedSec = Math.max(0, (tNow - b.updatedMs) / 1000);
    b.tokens = Math.min(capacity, b.tokens + elapsedSec * rule.perSecond);
    b.updatedMs = tNow;
    if (b.tokens >= 1) {
      b.tokens -= 1;
      return { ok: true };
    }
    const needed = 1 - b.tokens;
    return { ok: false, retryAfterSeconds: Math.max(1, Math.ceil(needed / rule.perSecond)) };
  }

  return {
    async acquire(req) {
      // 1) concurrency slot (reserved first so a rate denial doesn't waste it)
      let release: () => void = () => {};
      const cRule = config.concurrency?.[req.stepType];
      if (cRule) {
        const current = running.get(req.stepType) ?? 0;
        if (current >= cRule.maxConcurrent) {
          return { admitted: false, retryAfterSeconds: concurrencyRetry };
        }
        running.set(req.stepType, current + 1);
        let released = false;
        release = () => {
          if (released) return;
          released = true;
          running.set(req.stepType, Math.max(0, (running.get(req.stepType) ?? 1) - 1));
        };
      }

      // 2) rate-limit token (throughput)
      const rate = rateCheck(req.stepType);
      if (!rate.ok) {
        release();
        return { admitted: false, retryAfterSeconds: rate.retryAfterSeconds };
      }

      return { admitted: true, release };
    },
  };
}
