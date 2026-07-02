import type { LanguageModelV4 } from '@ai-sdk/provider';
import type { UsageAccumulator } from './instrumented-model';

/**
 * The `TContext` flow-core threads through AI workflows. Produced by the
 * `buildStepContext` hook in `createAiWorkflowHooks` and consumed by AI step
 * handlers (which read `ctx.context.model` / `ctx.context.host`).
 */
export interface AiContext<THost = unknown> {
  /** Instrumented language model — token usage is captured automatically. */
  model: LanguageModelV4;
  /** Host-provided per-step context (DI scope, domain services, …). */
  host: THost;
  /**
   * Internal: the per-step usage accumulator. The `onAfterStep` hook reads this
   * back off the context to persist token/cost. Handlers should not touch it.
   */
  readonly usage: UsageAccumulator;
}
