/**
 * Token → cost estimation. Cost is computed in **microdollars**
 * (1 USD = 1,000,000 microdollars) to avoid float accumulation.
 *
 * The pricing table is injectable: pass your own to `createCostEstimator` for a
 * different provider/model set. A default Anthropic/OpenAI-embedding table ships.
 */

export interface ModelPricing {
  inputPerMillion: number;
  outputPerMillion: number;
  cacheReadPerMillion: number;
  cacheWritePerMillion: number;
}

export interface TokenUsage {
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  cacheWriteTokens: number;
}

/** Default pricing (USD per million tokens), as of 2026-04. */
export const DEFAULT_MODEL_PRICING: Record<string, ModelPricing> = {
  'text-embedding-3-small': { inputPerMillion: 0.02, outputPerMillion: 0, cacheReadPerMillion: 0, cacheWritePerMillion: 0 },
  'text-embedding-3-large': { inputPerMillion: 0.13, outputPerMillion: 0, cacheReadPerMillion: 0, cacheWritePerMillion: 0 },
  'claude-opus-4-7': { inputPerMillion: 5, outputPerMillion: 25, cacheReadPerMillion: 0.5, cacheWritePerMillion: 6.25 },
  'claude-opus-4-6': { inputPerMillion: 15, outputPerMillion: 75, cacheReadPerMillion: 1.5, cacheWritePerMillion: 18.75 },
  'claude-sonnet-4-6': { inputPerMillion: 3, outputPerMillion: 15, cacheReadPerMillion: 0.3, cacheWritePerMillion: 3.75 },
  'claude-sonnet-4-5-20250929': { inputPerMillion: 3, outputPerMillion: 15, cacheReadPerMillion: 0.3, cacheWritePerMillion: 3.75 },
  'claude-haiku-4-5-20251001': { inputPerMillion: 0.8, outputPerMillion: 4, cacheReadPerMillion: 0.08, cacheWritePerMillion: 1 },
};

export interface CostEstimatorOptions {
  /** Pricing table keyed by model id. Defaults to `DEFAULT_MODEL_PRICING`. */
  pricing?: Record<string, ModelPricing>;
  /** Pricing used for unknown model ids. Defaults to the priciest entry in the table (safe overcounting). */
  fallback?: ModelPricing;
}

export type CostEstimator = (usage: TokenUsage, modelId: string) => number;

function priciest(pricing: Record<string, ModelPricing>): ModelPricing {
  let best: ModelPricing | undefined;
  for (const p of Object.values(pricing)) {
    if (!best || p.outputPerMillion > best.outputPerMillion) best = p;
  }
  return best ?? { inputPerMillion: 0, outputPerMillion: 0, cacheReadPerMillion: 0, cacheWritePerMillion: 0 };
}

/** Build a cost estimator returning microdollars. */
export function createCostEstimator(options: CostEstimatorOptions = {}): CostEstimator {
  const pricing = options.pricing ?? DEFAULT_MODEL_PRICING;
  const fallback = options.fallback ?? priciest(pricing);

  return (usage, modelId) => {
    const p = pricing[modelId] ?? fallback;
    const input = (usage.inputTokens / 1_000_000) * p.inputPerMillion;
    const output = (usage.outputTokens / 1_000_000) * p.outputPerMillion;
    const cacheRead = (usage.cacheReadTokens / 1_000_000) * p.cacheReadPerMillion;
    const cacheWrite = (usage.cacheWriteTokens / 1_000_000) * p.cacheWritePerMillion;
    return Math.round((input + output + cacheRead + cacheWrite) * 1_000_000);
  };
}

/** Convenience estimator using the default pricing table. */
export const estimateCostMicros: CostEstimator = createCostEstimator();
