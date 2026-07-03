/**
 * Instrumented embedding-model wrapper that transparently captures token usage
 * from every embed/embedMany call, without call sites doing anything.
 *
 * Uses the Vercel AI SDK's `wrapEmbeddingModel` middleware. Provider-agnostic —
 * works with any `EmbeddingModelV4` (OpenAI, Mistral, …). Embeddings only have
 * input tokens (no output, no cache), so the accumulator is narrower than the
 * language-model `UsageAccumulator`. Feed its `inputTokens` into `estimateCostMicros`
 * (with the other token fields as 0) to price a batch.
 */
import type { EmbeddingModelV4 } from '@ai-sdk/provider';
import { wrapEmbeddingModel } from 'ai';

export interface EmbeddingAccumulatedUsage {
  inputTokens: number;
  callCount: number;
  modelId: string;
}

export interface EmbeddingUsageAccumulator {
  /** Record usage from a single doEmbed call (additive — supports multiple calls per batch). */
  record(tokens: number, modelId: string): void;
  /** Total accumulated usage across all calls. */
  get(): EmbeddingAccumulatedUsage;
  /** Whether any usage was recorded. */
  hasUsage(): boolean;
  /** Clear all accumulated state — used when a long-lived accumulator is reused across flushes. */
  reset(): void;
}

export function createEmbeddingUsageAccumulator(): EmbeddingUsageAccumulator {
  let inputTokens = 0;
  let callCount = 0;
  let modelId = '';

  return {
    record(tokens, recordedModelId) {
      inputTokens += tokens;
      callCount += 1;
      // Keep the last model id (all calls in a batch typically use the same model).
      if (recordedModelId) modelId = recordedModelId;
    },
    get() {
      return { inputTokens, callCount, modelId };
    },
    hasUsage() {
      return callCount > 0;
    },
    reset() {
      inputTokens = 0;
      callCount = 0;
      modelId = '';
    },
  };
}

/**
 * Wrap an embedding model with usage-tracking middleware. The accumulator collects
 * token usage from every doEmbed call routed through the wrapped model — both
 * `embed()` (single value) and `embedMany()` (batched values) invoke doEmbed one or
 * more times, and each invocation is recorded.
 */
export function createInstrumentedEmbeddingModel(
  model: EmbeddingModelV4,
  accumulator: EmbeddingUsageAccumulator,
): EmbeddingModelV4 {
  return wrapEmbeddingModel({
    model,
    middleware: {
      specificationVersion: 'v4',

      wrapEmbed: async ({ doEmbed }) => {
        const result = await doEmbed();
        accumulator.record(result.usage?.tokens ?? 0, model.modelId);
        return result;
      },
    },
  });
}
