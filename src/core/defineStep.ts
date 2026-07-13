import { z } from 'zod';
import type { Result } from './result';
import type {
  StepHandler,
  StepHandlerRegistry,
  StepExecutionContext,
  StepError,
  StepCompensateHandler,
  StepCompensationContext,
  RetryPolicy,
  WorkflowDefinition,
  StartOptions,
  WorkflowCreatedResult,
  FlowError,
} from './types';
import { isRetryableError } from './retry';

/**
 * `z.object({})` typed as the empty-record schema. Zod infers `{}` for an empty object schema,
 * but we want `Record<string, never>` for input/output-less steps — hence the bridge cast.
 * Shared by `defineWaitStep`, `defineMapStep`'s child, and `defineSleepStep`.
 */
const emptyObjectSchema = z.object({}) as unknown as z.ZodType<Record<string, never>>;

// ============================================================================
// Typed step
// ============================================================================

/**
 * A type-safe step definition with Zod validation schemas, parameterized by the
 * host `TContext`. Created by `defineStep()`, consumed by `buildWorkflow()`.
 */
export interface TypedStep<
  TInput extends Record<string, unknown> = Record<string, unknown>,
  TOutput extends Record<string, unknown> = Record<string, unknown>,
  TDeps extends Record<string, TypedStep<any, any, any, TContext>> = {},
  TContext = unknown,
> {
  readonly type: string;
  readonly workflowInputSchema: z.ZodType<TInput>;
  readonly outputSchema: z.ZodType<TOutput>;
  readonly dependencies: TDeps;
  /** Untyped handler for registry compatibility. */
  readonly handler: StepHandler<TContext>;
  /** Optional retry policy for this step type. */
  readonly retry?: RetryPolicy;
  /** Optional per-step wall-clock timeout in ms. */
  readonly timeoutMs?: number;
  /** Optional durable start delay in ms (held in the queue once the step is ready). */
  readonly delayMs?: number;
  /** When true, the step suspends (waiting) until `engine.resumeStep` delivers an event. */
  readonly waitForEvent?: boolean;
  /** When true, this is a map parent — see `defineMapStep`. */
  readonly map?: boolean;
  /** For a map parent: the step type registered for per-item children. */
  readonly childType?: string;
  /** For a map parent: the child step's registration, registered alongside the parent. */
  readonly childRegistration?: { type: string; handler: StepHandler<TContext>; retry?: RetryPolicy; timeoutMs?: number };
  /** For a sub-workflow step: the child workflow definition the engine starts. */
  readonly subWorkflowDefinition?: WorkflowDefinition;
  /** For a sub-workflow step: registers the child workflow's step handlers alongside the parent. */
  readonly subWorkflowRegister?: (registry: StepHandlerRegistry<TContext>) => void;
  /** Untyped saga rollback handler, built from the typed `compensate` config. */
  readonly compensate?: StepCompensateHandler<TContext>;
}

/** Extract the output type of a typed step. */
export type StepOutput<T> = T extends TypedStep<any, infer O, any, any> ? O : never;

/** Derive the aggregated workflow output from a steps map: `{ [stepKey]: stepOutput }`. */
export type WorkflowOutput<TSteps extends Record<string, TypedStep<any, any, any, any>>> = {
  [K in keyof TSteps]: StepOutput<TSteps[K]>;
};

/**
 * Typed execution context passed to step handlers. `workflowInput` is validated
 * against the step's `workflowInputSchema`; each `deps` entry is validated against
 * its dependency's `outputSchema`.
 */
export interface TypedStepContext<
  TInput extends Record<string, unknown>,
  TDeps extends Record<string, TypedStep<any, any, any, TContext>>,
  TContext,
> {
  workflowId: number;
  stepId: number;
  stepKey: string;
  partitionKey: string;
  workflowInput: TInput;
  stepInput: Record<string, unknown>;
  deps: { [K in keyof TDeps]: StepOutput<TDeps[K]> };
  signal?: AbortSignal;
  context: TContext;
}

interface DefineStepConfig<
  TInput extends Record<string, unknown>,
  TOutput extends Record<string, unknown>,
  TDeps extends Record<string, TypedStep<any, any, any, TContext>>,
  TContext,
> {
  type: string;
  workflowInputSchema: z.ZodType<TInput>;
  outputSchema: z.ZodType<TOutput>;
  dependencies?: TDeps;
  handler: (ctx: TypedStepContext<TInput, TDeps, TContext>) => Promise<TOutput>;
  /** Optional retry policy (max attempts, backoff). */
  retry?: RetryPolicy;
  /** Optional per-step wall-clock timeout in ms. */
  timeoutMs?: number;
  /** Optional durable start delay in ms — held in the queue once the step is ready. */
  delayMs?: number;
  /** When true, the step suspends (waiting) until `engine.resumeStep` delivers an event. */
  waitForEvent?: boolean;
  /**
   * Optional saga rollback handler: on workflow failure the engine runs it once for
   * this step (if it completed), in reverse dependency order. Receives the typed context plus
   * the step's own `output` (what to undo). Best-effort — a throw is logged + surfaced.
   */
  compensate?: (ctx: TypedStepContext<TInput, TDeps, TContext> & { output: TOutput }) => Promise<void> | void;
}

/**
 * Define a type-safe workflow step with Zod validation. Wraps a typed handler
 * into an untyped `StepHandler` with a 5-phase pipeline:
 *
 *   1. Parse `workflowInput` against `workflowInputSchema`
 *   2. Parse each dependency output against its `outputSchema`
 *   3. Build the typed context
 *   4. Call the handler
 *   5. Parse the output against `outputSchema`
 *
 * Validation errors are non-retryable (a schema mismatch is a programming error).
 */
export function defineStep<
  TInput extends Record<string, unknown>,
  TOutput extends Record<string, unknown>,
  TContext = unknown,
  TDeps extends Record<string, TypedStep<any, any, any, TContext>> = {},
>(config: DefineStepConfig<TInput, TOutput, TDeps, TContext>): TypedStep<TInput, TOutput, TDeps, TContext> {
  const { type, workflowInputSchema, outputSchema, dependencies, handler, retry, timeoutMs, delayMs, waitForEvent } = config;
  const deps = (dependencies ?? {}) as TDeps;

  const wrappedHandler: StepHandler<TContext> = async (
    ctx: StepExecutionContext<TContext>,
  ): Promise<Result<Record<string, unknown>, StepError>> => {
    try {
      // Phase 1: workflow input
      const inputResult = workflowInputSchema.safeParse(ctx.workflowInput);
      if (!inputResult.success) {
        return {
          ok: false,
          error: { key: 'step_error', message: `[${type}] Invalid workflow input: ${inputResult.error.message}`, retryable: false },
        };
      }

      // Phase 2: dependency outputs
      const parsedDeps: Record<string, unknown> = {};
      for (const [depKey, depStep] of Object.entries(deps)) {
        const depResult = depStep.outputSchema.safeParse(ctx.dependencyOutputs[depKey]);
        if (!depResult.success) {
          return {
            ok: false,
            error: { key: 'step_error', message: `[${type}] Invalid dependency output '${depKey}': ${depResult.error.message}`, retryable: false },
          };
        }
        parsedDeps[depKey] = depResult.data;
      }

      // Phase 3: typed context
      const typedCtx: TypedStepContext<TInput, TDeps, TContext> = {
        workflowId: ctx.workflowId,
        stepId: ctx.stepId,
        stepKey: ctx.stepKey,
        partitionKey: ctx.partitionKey,
        workflowInput: inputResult.data,
        stepInput: ctx.stepInput,
        deps: parsedDeps as TypedStepContext<TInput, TDeps, TContext>['deps'],
        signal: ctx.signal,
        context: ctx.context,
      };

      // Phase 4: handler
      const output = await handler(typedCtx);

      // Phase 5: output
      const outputResult = outputSchema.safeParse(output);
      if (!outputResult.success) {
        return {
          ok: false,
          error: { key: 'step_error', message: `[${type}] Invalid step output: ${outputResult.error.message}`, retryable: false },
        };
      }

      return { ok: true, value: outputResult.data };
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown step error';
      return { ok: false, error: { key: 'step_error', message, retryable: isRetryableError(error) } };
    }
  };

  // Wrap the typed compensate (if any) into an untyped handler: re-derive the typed context
  // best-effort (don't hard-fail rollback on a stale schema), then add the step's own output.
  let wrappedCompensate: StepCompensateHandler<TContext> | undefined;
  if (config.compensate) {
    const compensate = config.compensate;
    wrappedCompensate = async (ctx: StepCompensationContext<TContext>) => {
      const input = workflowInputSchema.safeParse(ctx.workflowInput);
      const parsedDeps: Record<string, unknown> = {};
      for (const [depKey, depStep] of Object.entries(deps)) {
        const r = depStep.outputSchema.safeParse(ctx.dependencyOutputs[depKey]);
        parsedDeps[depKey] = r.success ? r.data : ctx.dependencyOutputs[depKey];
      }
      const out = outputSchema.safeParse(ctx.output);
      await compensate({
        workflowId: ctx.workflowId,
        stepId: ctx.stepId,
        stepKey: ctx.stepKey,
        partitionKey: ctx.partitionKey,
        workflowInput: (input.success ? input.data : ctx.workflowInput) as TInput,
        stepInput: ctx.stepInput,
        deps: parsedDeps as TypedStepContext<TInput, TDeps, TContext>['deps'],
        signal: ctx.signal,
        context: ctx.context,
        output: (out.success ? out.data : ctx.output) as TOutput,
      });
    };
  }

  return { type, workflowInputSchema, outputSchema, dependencies: deps, handler: wrappedHandler, retry, timeoutMs, delayMs, waitForEvent, compensate: wrappedCompensate };
}

/**
 * Define a **wait-for-event** step: once its dependencies complete, it suspends
 * (status `waiting`) instead of running, until `engine.resumeStep(workflowId, stepKey,
 * payload)` delivers an external event. The resume payload becomes the step's output
 * (validated by dependents against `outputSchema`), then the DAG advances.
 *
 * Use for human-in-the-loop approvals, webhooks, or any async external dependency.
 */
export function defineWaitStep<
  TOutput extends Record<string, unknown> = Record<string, unknown>,
  TContext = unknown,
  TDeps extends Record<string, TypedStep<any, any, any, TContext>> = {},
>(config: {
  type: string;
  /** Shape of the event payload delivered via `resumeStep`. */
  outputSchema: z.ZodType<TOutput>;
  dependencies?: TDeps;
}): TypedStep<Record<string, never>, TOutput, TDeps, TContext> {
  return defineStep<Record<string, never>, TOutput, TContext, TDeps>({
    type: config.type,
    workflowInputSchema: emptyObjectSchema,
    outputSchema: config.outputSchema,
    dependencies: config.dependencies,
    waitForEvent: true,
    // Never invoked — a wait step is completed by resumeStep, not by running a handler.
    handler: async () => ({}) as TOutput,
  });
}

/**
 * Define a **map** step: dynamic fan-out. `items(ctx)` produces a runtime-sized
 * list from the step's deps/input; the engine spawns one child per item (each running
 * `each(item, …)` with its own retry/timeout), suspends the parent as `mapping`, and
 * completes it with `{ items: childOutputs[] }` (ordered) once all children finish. A
 * failed item fails the whole map. Use for "do X for each of N images/sections/locales".
 *
 * The map step's output type is `{ items: TItemOutput[] }`; dependents read `deps.X.items`.
 */
export function defineMapStep<
  TItem,
  TItemOutput extends Record<string, unknown>,
  TWorkflowInput extends Record<string, unknown> = Record<string, unknown>,
  TContext = unknown,
  TDeps extends Record<string, TypedStep<any, any, any, TContext>> = {},
>(config: {
  type: string;
  workflowInputSchema: z.ZodType<TWorkflowInput>;
  /** Schema for one item's output. */
  itemOutputSchema: z.ZodType<TItemOutput>;
  dependencies?: TDeps;
  /** Produce the list of items to fan out over. */
  items: (ctx: TypedStepContext<TWorkflowInput, TDeps, TContext>) => Promise<TItem[]> | TItem[];
  /** Process one item. */
  each: (
    item: TItem,
    info: { index: number; context: TContext; workflowId: number; partitionKey: string },
  ) => Promise<TItemOutput> | TItemOutput;
  /** Optional per-item retry policy. */
  itemRetry?: RetryPolicy;
  /** Optional per-item wall-clock timeout in ms. */
  itemTimeoutMs?: number;
}): TypedStep<TWorkflowInput, { items: TItemOutput[] }, TDeps, TContext> {
  const childType = `${config.type}__item`;

  // Parent: validates input + deps like a normal step, but its "output" is the raw item
  // list consumed by the engine (not validated against the parent's real output schema).
  const parent = defineStep<TWorkflowInput, { items: unknown[] }, TContext, TDeps>({
    type: config.type,
    workflowInputSchema: config.workflowInputSchema,
    outputSchema: z.object({ items: z.array(z.unknown()) }) as unknown as z.ZodType<{ items: unknown[] }>,
    dependencies: config.dependencies,
    handler: async (ctx) => ({ items: await config.items(ctx) }),
  });

  // Child: runs `each` for one item (read from its input) and validates against itemOutputSchema.
  const child = defineStep<Record<string, never>, TItemOutput, TContext>({
    type: childType,
    workflowInputSchema: emptyObjectSchema,
    outputSchema: config.itemOutputSchema,
    retry: config.itemRetry,
    timeoutMs: config.itemTimeoutMs,
    handler: async (ctx) =>
      config.each(ctx.stepInput.item as TItem, {
        index: Number(ctx.stepInput.index ?? 0),
        context: ctx.context,
        workflowId: ctx.workflowId,
        partitionKey: ctx.partitionKey,
      }),
  });

  return {
    ...parent,
    // The real output contract dependents validate against: { items: TItemOutput[] }.
    outputSchema: z.object({ items: z.array(config.itemOutputSchema) }) as unknown as z.ZodType<{ items: TItemOutput[] }>,
    map: true,
    childType,
    childRegistration: { type: childType, handler: child.handler, retry: child.retry, timeoutMs: child.timeoutMs },
  } as TypedStep<TWorkflowInput, { items: TItemOutput[] }, TDeps, TContext>;
}

/**
 * Define a **sub-workflow** step: once its deps complete, `input(ctx)` maps the
 * parent context to the child workflow's input; the engine **starts the child workflow**
 * (sharing the partition), suspends this step as `waiting`, and resumes it with the child's
 * output once the child terminates. A failed/cancelled child fails this step (and cascades).
 *
 * Pass the built child workflow (`buildWorkflow(...)`); its step handlers are registered
 * automatically alongside the parent. The step's output type is the child's output shape —
 * give `outputSchema` to type it for dependents (defaults to an opaque record).
 */
export function defineSubWorkflowStep<
  TOutput extends Record<string, unknown> = Record<string, unknown>,
  TWorkflowInput extends Record<string, unknown> = Record<string, unknown>,
  TContext = unknown,
  TDeps extends Record<string, TypedStep<any, any, any, TContext>> = {},
>(config: {
  type: string;
  workflowInputSchema: z.ZodType<TWorkflowInput>;
  /** The built child workflow to start (`buildWorkflow(...)`) — must share the parent's `TContext`. */
  childWorkflow: { definition: WorkflowDefinition; register: (registry: StepHandlerRegistry<TContext>) => void };
  /** Map the parent step's context to the child workflow's input. */
  input: (ctx: TypedStepContext<TWorkflowInput, TDeps, TContext>) => Promise<Record<string, unknown>> | Record<string, unknown>;
  /** Shape of the child workflow's output (for typing dependents). Defaults to an opaque record. */
  outputSchema?: z.ZodType<TOutput>;
  dependencies?: TDeps;
}): TypedStep<TWorkflowInput, TOutput, TDeps, TContext> {
  const recordSchema = z.record(z.string(), z.unknown());
  // Parent: validates input + deps like a normal step; its "output" is the child input,
  // which the engine consumes (not validated against the real output schema).
  const parent = defineStep<TWorkflowInput, Record<string, unknown>, TContext, TDeps>({
    type: config.type,
    workflowInputSchema: config.workflowInputSchema,
    outputSchema: recordSchema as unknown as z.ZodType<Record<string, unknown>>,
    dependencies: config.dependencies,
    handler: async (ctx) => config.input(ctx),
  });

  return {
    ...parent,
    // The real output contract dependents validate against: the child workflow's output.
    outputSchema: (config.outputSchema ?? recordSchema) as unknown as z.ZodType<TOutput>,
    subWorkflowDefinition: config.childWorkflow.definition,
    subWorkflowRegister: config.childWorkflow.register,
  } as TypedStep<TWorkflowInput, TOutput, TDeps, TContext>;
}

/**
 * Define a durable **sleep** step: a no-op step that, once its dependencies complete,
 * is held in the queue for `sleepMs` before completing — advancing the DAG afterwards.
 * The delay is durable (survives restarts) because it lives in the queue, not in memory.
 *
 * A sleep step is just `defineStep` with `delayMs` and an empty handler; use this for
 * cooldowns, "continue in 1 hour", or spacing out downstream work.
 */
export function defineSleepStep<
  TContext = unknown,
  TDeps extends Record<string, TypedStep<any, any, any, TContext>> = {},
>(config: {
  type: string;
  /** How long to sleep once the step becomes ready, in ms. */
  sleepMs: number;
  dependencies?: TDeps;
}): TypedStep<Record<string, never>, Record<string, never>, TDeps, TContext> {
  // A sleep step ignores input; z.object({}) strips any extra keys without erroring.
  return defineStep<Record<string, never>, Record<string, never>, TContext, TDeps>({
    type: config.type,
    workflowInputSchema: emptyObjectSchema,
    outputSchema: emptyObjectSchema,
    dependencies: config.dependencies,
    delayMs: config.sleepMs,
    handler: async () => ({}),
  });
}

// ============================================================================
// Typed workflow
// ============================================================================

/** Minimal engine surface `buildWorkflow().start()` needs — avoids a circular import. */
interface StartableEngine {
  startWorkflow(
    definition: WorkflowDefinition,
    input: Record<string, unknown>,
    options?: StartOptions,
  ): Promise<Result<WorkflowCreatedResult, FlowError>>;
}

export interface TypedWorkflow<TInput extends Record<string, unknown>, TContext> {
  readonly type: string;
  readonly inputSchema: z.ZodType<TInput>;
  readonly definition: WorkflowDefinition;
  /** Register every step handler with a registry. */
  register(registry: StepHandlerRegistry<TContext>): void;
  /** Type-safe start with input validation. */
  start(engine: StartableEngine, input: TInput, options?: StartOptions): Promise<Result<WorkflowCreatedResult, FlowError>>;
}

interface BuildWorkflowConfig<
  TInput extends Record<string, unknown>,
  TSteps extends Record<string, TypedStep<any, any, any, TContext>>,
  TContext,
> {
  type: string;
  inputSchema: z.ZodType<TInput>;
  steps: TSteps;
}

/**
 * Build a typed workflow from step definitions. Derives the `WorkflowDefinition`
 * DAG from each step's dependency metadata and validates at construction time
 * that every dependency key references a real step.
 */
export function buildWorkflow<
  TInput extends Record<string, unknown>,
  TContext = unknown,
  TSteps extends Record<string, TypedStep<any, any, any, TContext>> = {},
>(config: BuildWorkflowConfig<TInput, TSteps, TContext>): TypedWorkflow<TInput, TContext> {
  const { type, inputSchema, steps } = config;
  const stepKeys = new Set(Object.keys(steps));

  for (const [stepKey, step] of Object.entries(steps)) {
    for (const depKey of Object.keys(step.dependencies)) {
      if (!stepKeys.has(depKey)) {
        throw new Error(
          `[buildWorkflow] Step '${stepKey}' depends on '${depKey}', which is not a valid step key. Valid keys: ${[...stepKeys].join(', ')}`,
        );
      }
    }
  }

  const definition: WorkflowDefinition = {
    type,
    steps: Object.entries(steps).map(([key, step]) => {
      const depKeys = Object.keys(step.dependencies);
      return { key, type: step.type, ...(depKeys.length > 0 ? { dependencies: depKeys } : {}) };
    }),
  };

  return {
    type,
    inputSchema,
    definition,
    register(registry) {
      for (const step of Object.values(steps)) {
        registry.register(step.type, step.handler, {
          retry: step.retry,
          timeoutMs: step.timeoutMs,
          delayMs: step.delayMs,
          waitForEvent: step.waitForEvent,
          map: step.map,
          childType: step.childType,
          subWorkflowDefinition: step.subWorkflowDefinition,
          compensate: step.compensate,
        });
        // A map step also registers its per-item child handler.
        if (step.childRegistration) {
          const c = step.childRegistration;
          registry.register(c.type, c.handler, { retry: c.retry, timeoutMs: c.timeoutMs });
        }
        // A sub-workflow step also registers the child workflow's step handlers.
        if (step.subWorkflowRegister) step.subWorkflowRegister(registry);
      }
    },
    start(engine, input, options) {
      const parsed = inputSchema.parse(input);
      return engine.startWorkflow(definition, parsed, options);
    },
  };
}
