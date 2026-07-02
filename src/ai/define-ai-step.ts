import type { z } from 'zod';
import {
  defineStep,
  buildWorkflow,
  type TypedStep,
  type TypedWorkflow,
  type TypedStepContext,
  type RetryPolicy,
} from '../core';
import type { AiContext } from './context';

/** An AI workflow step — a flow-core `TypedStep` whose context is an `AiContext`. */
export type AiTypedStep<
  TInput extends Record<string, unknown> = Record<string, unknown>,
  TOutput extends Record<string, unknown> = Record<string, unknown>,
  THost = unknown,
  TDeps extends Record<string, TypedStep<any, any, any, AiContext<THost>>> = {},
> = TypedStep<TInput, TOutput, TDeps, AiContext<THost>>;

interface DefineAiStepConfig<
  TInput extends Record<string, unknown>,
  TOutput extends Record<string, unknown>,
  THost,
  TDeps extends Record<string, TypedStep<any, any, any, AiContext<THost>>>,
> {
  type: string;
  workflowInputSchema: z.ZodType<TInput>;
  outputSchema: z.ZodType<TOutput>;
  dependencies?: TDeps;
  handler: (ctx: TypedStepContext<TInput, TDeps, AiContext<THost>>) => Promise<TOutput>;
  /** Optional retry policy (max attempts, backoff) — e.g. for provider 429s. */
  retry?: RetryPolicy;
  /** Optional per-step wall-clock timeout in ms. */
  timeoutMs?: number;
  /** Optional durable start delay in ms — held in the queue once the step is ready. */
  delayMs?: number;
}

/**
 * Define a type-safe AI workflow step. Identical to flow-core's `defineStep`
 * except the handler context is fixed to `AiContext<THost>`, so handlers get a
 * ready-to-use `ctx.context.model` (instrumented) and `ctx.context.host`.
 */
export function defineAiStep<
  TInput extends Record<string, unknown>,
  TOutput extends Record<string, unknown>,
  THost = unknown,
  TDeps extends Record<string, TypedStep<any, any, any, AiContext<THost>>> = {},
>(config: DefineAiStepConfig<TInput, TOutput, THost, TDeps>): AiTypedStep<TInput, TOutput, THost, TDeps> {
  return defineStep<TInput, TOutput, AiContext<THost>, TDeps>(config);
}

/** Build a typed AI workflow from `defineAiStep` definitions. */
export function buildAiWorkflow<
  TInput extends Record<string, unknown>,
  THost = unknown,
  TSteps extends Record<string, TypedStep<any, any, any, AiContext<THost>>> = {},
>(config: {
  type: string;
  inputSchema: z.ZodType<TInput>;
  steps: TSteps;
}): TypedWorkflow<TInput, AiContext<THost>> {
  return buildWorkflow<TInput, AiContext<THost>, TSteps>(config);
}
