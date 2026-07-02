/**
 * Instrumented language-model wrapper that transparently captures token usage
 * from every generateText/streamText call, without step handlers doing anything.
 *
 * Uses the Vercel AI SDK's `wrapLanguageModel` middleware. Provider-agnostic —
 * works with any `LanguageModelV4` (Anthropic, OpenAI, Mistral, …).
 */
import type { LanguageModelV4 } from '@ai-sdk/provider';
import { wrapLanguageModel } from 'ai';

export interface AccumulatedUsage {
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  cacheWriteTokens: number;
  modelId: string;
}

export interface UsageAccumulator {
  /** Record usage from a single LLM call (additive — supports multiple calls per step). */
  record(usage: AccumulatedUsage): void;
  /** Total accumulated usage across all calls. */
  get(): AccumulatedUsage;
  /** Whether any usage was recorded. */
  hasUsage(): boolean;
}

export function createUsageAccumulator(): UsageAccumulator {
  const total: AccumulatedUsage = {
    inputTokens: 0,
    outputTokens: 0,
    cacheReadTokens: 0,
    cacheWriteTokens: 0,
    modelId: '',
  };
  let hasAny = false;

  return {
    record(usage) {
      total.inputTokens += usage.inputTokens;
      total.outputTokens += usage.outputTokens;
      total.cacheReadTokens += usage.cacheReadTokens;
      total.cacheWriteTokens += usage.cacheWriteTokens;
      // Keep the last model id (all calls in a step typically use the same model).
      if (usage.modelId) total.modelId = usage.modelId;
      hasAny = true;
    },
    get() {
      return { ...total };
    },
    hasUsage() {
      return hasAny;
    },
  };
}

/**
 * Wrap a language model with usage-tracking middleware. The accumulator collects
 * token usage from every doGenerate/doStream call routed through the wrapped model.
 */
export function createInstrumentedModel(model: LanguageModelV4, accumulator: UsageAccumulator): LanguageModelV4 {
  return wrapLanguageModel({
    model,
    middleware: {
      specificationVersion: 'v4',

      wrapGenerate: async ({ doGenerate }) => {
        const result = await doGenerate();
        const usage = result.usage;
        if (usage) {
          accumulator.record({
            inputTokens: usage.inputTokens?.total ?? 0,
            outputTokens: usage.outputTokens?.total ?? 0,
            cacheReadTokens: usage.inputTokens?.cacheRead ?? 0,
            cacheWriteTokens: usage.inputTokens?.cacheWrite ?? 0,
            modelId: model.modelId,
          });
        }
        return result;
      },

      wrapStream: async ({ doStream }) => {
        const result = await doStream();
        const wrappedStream = result.stream.pipeThrough(
          new TransformStream({
            transform(chunk, controller) {
              controller.enqueue(chunk);
              if (chunk.type === 'finish' && chunk.usage) {
                const u = chunk.usage;
                accumulator.record({
                  inputTokens: u.inputTokens?.total ?? 0,
                  outputTokens: u.outputTokens?.total ?? 0,
                  cacheReadTokens: u.inputTokens?.cacheRead ?? 0,
                  cacheWriteTokens: u.inputTokens?.cacheWrite ?? 0,
                  modelId: model.modelId,
                });
              }
            },
          }),
        );
        return { ...result, stream: wrappedStream };
      },
    },
  });
}
