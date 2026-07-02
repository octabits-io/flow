import type { Result } from './result';
import type { Logger } from './logger';
import { noopLogger } from './logger';
import type { WorkflowStore } from './store';
import type { Dispatcher } from './dispatcher';
import type { StepGate } from './gate';
import type { WorkflowHooks } from './hooks';
import type { FlowObserver, FlowTracer, FlowEvent } from './observability';
import { noopObserver, noopTracer } from './observability';
import type {
  WorkflowId,
  StepId,
  WorkflowDefinition,
  WorkflowStatus,
  WorkflowRecord,
  StepRecord,
  WorkflowWithSteps,
  StepHandlerRegistry,
  StepHandler,
  StepExecutionContext,
  StepError,
  RetryPolicy,
  StartOptions,
  WorkflowCreatedResult,
  FlowError,
} from './types';

// ============================================================================
// Retry / timeout helpers
// ============================================================================

/**
 * Run a handler under an optional wall-clock timeout. On expiry the abort signal
 * is fired (cooperative handlers stop) and a retryable timeout error is returned —
 * the engine then applies the step's retry policy.
 */
async function runWithTimeout<TContext>(
  handler: StepHandler<TContext>,
  ctx: StepExecutionContext<TContext>,
  timeoutMs: number | undefined,
  abort: AbortController,
): Promise<Result<Record<string, unknown>, StepError>> {
  if (!timeoutMs || timeoutMs <= 0) return handler(ctx);
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<Result<Record<string, unknown>, StepError>>((resolve) => {
    timer = setTimeout(() => {
      abort.abort();
      resolve({ ok: false, error: { key: 'step_error', message: `Step timed out after ${timeoutMs}ms`, retryable: true } });
    }, timeoutMs);
  });
  try {
    return await Promise.race([handler(ctx), timeout]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

/** Backoff delay (in whole seconds) before the next attempt, given the just-failed attempt number. */
function backoffDelaySeconds(retry: RetryPolicy | undefined, failedAttemptNo: number): number {
  const initialMs = retry?.initialDelayMs ?? 1000;
  const maxMs = retry?.maxDelayMs ?? 60_000;
  const baseMs = retry?.backoff === 'exponential' ? initialMs * 2 ** (failedAttemptNo - 1) : initialMs;
  return Math.max(0, Math.ceil(Math.min(baseMs, maxMs) / 1000));
}

/**
 * Reverse topological order (gap 09): a step appears before its dependencies, so saga
 * compensation undoes effects in the opposite order they were produced. Pure / deterministic.
 */
function reverseTopologicalOrder(steps: StepRecord[]): StepRecord[] {
  const inSet = new Set(steps.map((s) => s.key));
  const placed = new Set<string>();
  const topo: StepRecord[] = [];
  let guard = 0;
  while (placed.size < steps.length && guard++ <= steps.length) {
    for (const s of steps) {
      if (placed.has(s.key)) continue;
      const deps = (s.dependencies ?? []).filter((d) => inSet.has(d));
      if (deps.every((d) => placed.has(d))) {
        topo.push(s);
        placed.add(s.key);
      }
    }
  }
  // Any leftover (a cycle — shouldn't happen for a validated DAG) appended as-is.
  for (const s of steps) if (!placed.has(s.key)) topo.push(s);
  return topo.reverse();
}

// ============================================================================
// Dependencies
// ============================================================================

export interface WorkflowEngineConfig {
  /** The dispatcher's per-step expiry, in seconds. Used by the stuck-step sweeper. Default 600. */
  stepExpirySeconds?: number;
  /** Extra grace added to the stuck threshold beyond `stepExpirySeconds`. Default 300. */
  stuckStepBufferSeconds?: number;
}

export interface WorkflowEngineDeps<TContext = unknown> {
  store: WorkflowStore;
  dispatcher: Dispatcher;
  registry: StepHandlerRegistry<TContext>;
  /** Partition this engine instance is bound to (e.g. a tenant id). */
  partitionKey: string;
  logger?: Logger;
  hooks?: WorkflowHooks<TContext>;
  config?: WorkflowEngineConfig;
  /**
   * Optional admission gate (concurrency caps / rate limiting). Consulted before each
   * step runs; a denied step is deferred (re-enqueued) without consuming an attempt.
   */
  gate?: StepGate;
  /** Run-history / metrics sink (gap 10). Receives a `FlowEvent` per transition. Default: no-op. */
  observer?: FlowObserver;
  /** Tracer (gap 10): the engine wraps each `executeStep` in a span. Default: no-op. */
  tracer?: FlowTracer;
  /** Clock injection point for testability. Defaults to `() => new Date()`. */
  now?: () => Date;
}

// ============================================================================
// Factory
// ============================================================================

export function createWorkflowEngine<TContext = unknown>(deps: WorkflowEngineDeps<TContext>) {
  const { store, dispatcher, registry, partitionKey } = deps;
  const gate = deps.gate;
  const logger = deps.logger ?? noopLogger;
  const hooks = deps.hooks ?? {};
  const observer = deps.observer ?? noopObserver;
  const tracer = deps.tracer ?? noopTracer;
  const now = deps.now ?? (() => new Date());
  const stepExpirySeconds = deps.config?.stepExpirySeconds ?? 600;
  const stuckStepBufferSeconds = deps.config?.stuckStepBufferSeconds ?? 300;

  const nowIso = () => now().toISOString();

  /** Emit a run-history / metrics event (gap 10). Guarded — a faulty observer never breaks a run. */
  function emit(event: Omit<FlowEvent, 'at' | 'partitionKey'>): void {
    try {
      observer.record({ ...event, partitionKey, at: nowIso() });
    } catch (e) {
      logger.error('Observer threw', e instanceof Error ? e : new Error(String(e)), { workflowId: event.workflowId });
    }
  }

  // --------------------------------------------------------------------------
  // Dispatch helpers
  // --------------------------------------------------------------------------

  /** Durable start delay for a ready step, from its registered `delayMs` (gap 02). */
  function readyStepDelay(stepType: string): { startAfterSeconds: number } | undefined {
    const delayMs = registry.getRegistration(stepType)?.delayMs;
    if (!delayMs || delayMs <= 0) return undefined;
    return { startAfterSeconds: Math.ceil(delayMs / 1000) };
  }

  /** Whether a step type suspends on readiness instead of dispatching (gap 07). */
  function isWaitStep(stepType: string): boolean {
    return registry.getRegistration(stepType)?.waitForEvent === true;
  }

  /**
   * Make a newly-ready step runnable: a `waitForEvent` step suspends (status `waiting`)
   * and awaits `resumeStep`; everything else is enqueued (with any durable delay).
   * Returns whether the step was enqueued.
   */
  async function dispatchReadyStep(workflowId: WorkflowId, stepId: StepId, stepKey: string, stepType: string): Promise<boolean> {
    if (isWaitStep(stepType)) {
      await store.markStepWaiting(stepId);
      logger.info('Step is waiting for an event', { workflowId, stepId, stepKey });
      emit({ type: 'step.waiting', workflowId, stepId, stepKey, stepType });
      return false;
    }
    const result = await dispatcher.enqueueStep({ workflowId, stepId, stepKey, stepType }, readyStepDelay(stepType));
    if (!result.ok) {
      logger.error('Failed to enqueue step', new Error(result.error.message), { workflowId, stepKey });
      return false;
    }
    logger.info('Enqueued step', { workflowId, stepKey });
    return true;
  }

  // --------------------------------------------------------------------------
  // Validation (pure)
  // --------------------------------------------------------------------------

  function validateDefinition(definition: WorkflowDefinition): Result<void, FlowError> {
    const { steps } = definition;

    if (steps.length === 0) {
      return { ok: false, error: { key: 'invalid_workflow_definition', message: 'Workflow must have at least one step' } };
    }

    const keys = new Set<string>();
    for (const step of steps) {
      if (keys.has(step.key)) {
        return { ok: false, error: { key: 'invalid_workflow_definition', message: `Duplicate step key: ${step.key}` } };
      }
      keys.add(step.key);
    }

    for (const step of steps) {
      for (const dep of step.dependencies ?? []) {
        if (!keys.has(dep)) {
          return { ok: false, error: { key: 'invalid_workflow_definition', message: `Step '${step.key}' depends on unknown step '${dep}'` } };
        }
        if (dep === step.key) {
          return { ok: false, error: { key: 'invalid_workflow_definition', message: `Step '${step.key}' cannot depend on itself` } };
        }
      }
    }

    // Cycle detection via Kahn's algorithm
    const inDegree = new Map<string, number>();
    const adjacency = new Map<string, string[]>();
    for (const step of steps) {
      inDegree.set(step.key, (step.dependencies ?? []).length);
      adjacency.set(step.key, []);
    }
    for (const step of steps) {
      for (const dep of step.dependencies ?? []) {
        adjacency.get(dep)!.push(step.key);
      }
    }
    const queue: string[] = [];
    for (const [key, deg] of inDegree) {
      if (deg === 0) queue.push(key);
    }
    let visited = 0;
    while (queue.length > 0) {
      const current = queue.shift()!;
      visited++;
      for (const neighbor of adjacency.get(current) ?? []) {
        const newDeg = inDegree.get(neighbor)! - 1;
        inDegree.set(neighbor, newDeg);
        if (newDeg === 0) queue.push(neighbor);
      }
    }
    if (visited !== steps.length) {
      return { ok: false, error: { key: 'invalid_workflow_definition', message: 'Workflow contains circular dependencies' } };
    }

    for (const step of steps) {
      if (!registry.has(step.type)) {
        return { ok: false, error: { key: 'step_handler_not_found', message: `No handler registered for step type '${step.type}'`, stepType: step.type } };
      }
    }

    return { ok: true, value: undefined };
  }

  // --------------------------------------------------------------------------
  // Start
  // --------------------------------------------------------------------------

  async function startWorkflow(
    definition: WorkflowDefinition,
    input: Record<string, unknown>,
    options?: StartOptions,
  ): Promise<Result<WorkflowCreatedResult, FlowError>> {
    const validation = validateDefinition(definition);
    if (!validation.ok) return validation;

    let metadata = options?.metadata;
    if (hooks.onBeforeStart) {
      const guard = await hooks.onBeforeStart({ definition, input, options });
      if (!guard.ok) return guard;
      if (guard.value.metadata) {
        metadata = { ...(metadata ?? {}), ...guard.value.metadata };
      }
    }

    const { steps } = definition;

    const created = await store.createWorkflow({
      type: definition.type,
      input,
      entityRef: options?.entityRef,
      metadata,
      idempotencyKey: options?.idempotencyKey,
      parentWorkflowId: options?.parentWorkflowId,
      parentStepId: options?.parentStepId,
      startedAt: nowIso(),
      steps: steps.map((s) => ({
        key: s.key,
        type: s.type,
        dependencies: s.dependencies ?? [],
        input: s.input ?? null,
      })),
    });

    const workflowId = created.workflowId;

    // Idempotency hit: the workflow already exists — return it without re-enqueuing.
    if (created.alreadyExisted) {
      logger.info('Idempotent start: returning existing workflow', { workflowId, idempotencyKey: options?.idempotencyKey });
      return { ok: true, value: { workflowId, totalSteps: created.steps.length, enqueuedSteps: [] } };
    }

    emit({ type: 'workflow.started', workflowId, workflowType: definition.type });

    // Enqueue dependency-free roots (run immediately / in parallel)
    const readySteps = steps.filter((s) => !s.dependencies || s.dependencies.length === 0);
    const enqueuedStepKeys: string[] = [];
    for (const readyStep of readySteps) {
      const dbStep = created.steps.find((s) => s.key === readyStep.key);
      if (!dbStep) continue;
      const enqueued = await dispatchReadyStep(workflowId, dbStep.id, readyStep.key, readyStep.type);
      if (enqueued) enqueuedStepKeys.push(readyStep.key);
    }

    logger.info('Workflow started', { workflowId, type: definition.type, totalSteps: steps.length, enqueuedSteps: enqueuedStepKeys });

    return { ok: true, value: { workflowId, totalSteps: steps.length, enqueuedSteps: enqueuedStepKeys } };
  }

  /** Type-safe start: duck-types on `{ inputSchema, definition }`. */
  function start<TInput extends Record<string, unknown>>(
    workflow: { inputSchema: { parse(v: unknown): TInput }; definition: WorkflowDefinition },
    input: TInput,
    options?: StartOptions,
  ): Promise<Result<WorkflowCreatedResult, FlowError>> {
    return startWorkflow(workflow.definition, workflow.inputSchema.parse(input), options);
  }

  // --------------------------------------------------------------------------
  // Execute
  // --------------------------------------------------------------------------

  async function executeStep(workflowId: WorkflowId, stepId: StepId): Promise<Result<void, FlowError>> {
    const workflow = await store.getWorkflow(workflowId);
    if (!workflow) return { ok: false, error: { key: 'workflow_not_found', message: `Workflow ${workflowId} not found` } };

    if (workflow.status === 'cancelled' || workflow.status === 'failed') {
      logger.info('Skipping step for non-active workflow', { workflowId, stepId, status: workflow.status });
      return { ok: true, value: undefined };
    }

    const step = await store.getStep(stepId);
    if (!step || step.workflowId !== workflowId) {
      return { ok: false, error: { key: 'workflow_not_found', message: `Step ${stepId} not found` } };
    }

    // Idempotency: a re-delivered job for an already-handled step is a no-op
    if (step.status !== 'pending') {
      logger.info('Skipping already-processed step', { workflowId, stepId, stepKey: step.key, status: step.status });
      return { ok: true, value: undefined };
    }

    // Admission gate (gap 03): if not admitted, defer by re-enqueueing with the gate's
    // delay. The step stays `pending` and no attempt is consumed.
    let releaseSlot: (() => void | Promise<void>) | undefined;
    if (gate) {
      const decision = await gate.acquire({ partitionKey, workflowId, stepId, stepKey: step.key, stepType: step.type });
      if (!decision.admitted) {
        logger.info('Step deferred by gate', { workflowId, stepId, stepKey: step.key, retryAfterSeconds: decision.retryAfterSeconds });
        const re = await dispatcher.enqueueStep(
          { workflowId, stepId, stepKey: step.key, stepType: step.type },
          { startAfterSeconds: decision.retryAfterSeconds },
        );
        if (!re.ok) logger.error('Failed to re-enqueue gated step', new Error(re.error.message), { workflowId, stepId });
        return { ok: true, value: undefined };
      }
      releaseSlot = decision.release;
    }

    const startMs = now().getTime();
    try {
      await store.markStepRunning(stepId, nowIso());

      // Resolve dependency outputs
      const dependencyOutputs: Record<string, unknown> = {};
      const stepDeps = step.dependencies ?? [];
      if (stepDeps.length > 0) {
        const allSteps = await store.listSteps(workflowId);
        for (const depKey of stepDeps) {
          const depStep = allSteps.find((s) => s.key === depKey);
          if (!depStep || depStep.status !== 'completed') {
            logger.error('Step dependency not completed', new Error(`Dependency '${depKey}' is ${depStep?.status ?? 'missing'}`), { workflowId, stepId });
            return { ok: false, error: { key: 'step_error', message: `Dependency '${depKey}' is not completed` } };
          }
          dependencyOutputs[depKey] = depStep.output ?? undefined;
        }
      }

      const registration = registry.getRegistration(step.type);
      if (!registration) {
        await markStepFailed(stepId, workflowId, `No handler for step type '${step.type}'`);
        return { ok: false, error: { key: 'step_handler_not_found', message: `No handler for '${step.type}'`, stepType: step.type } };
      }
      const { handler, retry, timeoutMs } = registration;
      const maxAttempts = retry?.maxAttempts ?? 1;
      // markStepRunning already bumped the persisted counter; this run is attempt N.
      const attemptNo = step.attempts + 1;

      emit({ type: 'step.started', workflowId, workflowType: workflow.type, stepId, stepKey: step.key, stepType: step.type, attempt: attemptNo });
      const span = tracer.startSpan('flow.step', {
        'flow.workflow_id': workflowId,
        'flow.workflow_type': workflow.type,
        'flow.step_key': step.key,
        'flow.step_type': step.type,
        'flow.attempt': attemptNo,
        'flow.partition': partitionKey,
      });

      try {
        const context = hooks.buildStepContext
          ? await hooks.buildStepContext({ workflowId, stepId, stepKey: step.key, partitionKey, workflow, step })
          : (undefined as TContext);

        const abort = new AbortController();
        const ctx: StepExecutionContext<TContext> = {
          workflowId,
          stepId,
          stepKey: step.key,
          partitionKey,
          workflowInput: workflow.input ?? {},
          stepInput: step.input ?? {},
          dependencyOutputs,
          signal: abort.signal,
          context,
        };

        const handlerResult = await runWithTimeout(handler, ctx, timeoutMs, abort);

        if (handlerResult.ok) {
          // Map parent: the handler returned the item list — spawn children, don't complete.
          if (registration.map) {
            emit({ type: 'step.mapping', workflowId, workflowType: workflow.type, stepId, stepKey: step.key, stepType: step.type, attempt: attemptNo });
            await spawnMapChildren(workflowId, step, registration.childType ?? `${step.type}__item`, handlerResult.value);
            return { ok: true, value: undefined };
          }
          // Sub-workflow parent: the handler returned the child input — start it, then suspend.
          if (registration.subWorkflowDefinition) {
            await startSubWorkflow(workflowId, step, registration.subWorkflowDefinition, handlerResult.value);
            return { ok: true, value: undefined };
          }
          if (hooks.onAfterStep) {
            await hooks.onAfterStep({ workflowId, stepId, partitionKey, workflow, step, output: handlerResult.value, context });
          }
          emit({ type: 'step.completed', workflowId, workflowType: workflow.type, stepId, stepKey: step.key, stepType: step.type, attempt: attemptNo, durationMs: now().getTime() - startMs });
          if (step.parentStepId != null) {
            await onChildCompleted(workflowId, step, handlerResult.value);
          } else {
            await onStepCompleted(workflowId, stepId, handlerResult.value);
          }
          return { ok: true, value: undefined };
        }

        // Retry transient failures within the attempt budget (re-enqueue with backoff).
        if (handlerResult.error.retryable && attemptNo < maxAttempts) {
          const delaySeconds = backoffDelaySeconds(retry, attemptNo);
          logger.info('Retrying step', { workflowId, stepId, stepKey: step.key, attempt: attemptNo, maxAttempts, delaySeconds, reason: handlerResult.error.message });
          emit({ type: 'step.retrying', workflowId, workflowType: workflow.type, stepId, stepKey: step.key, stepType: step.type, attempt: attemptNo, durationMs: now().getTime() - startMs, error: handlerResult.error.message });
          await store.markStepPending(stepId);
          const re = await dispatcher.enqueueStep(
            { workflowId, stepId, stepKey: step.key, stepType: step.type },
            { startAfterSeconds: delaySeconds },
          );
          if (!re.ok) {
            await failStepTerminal(workflowId, step, `${handlerResult.error.message} (retry enqueue failed: ${re.error.message})`);
          }
          return { ok: true, value: undefined };
        }

        span.recordError(new Error(handlerResult.error.message));
        emit({ type: 'step.failed', workflowId, workflowType: workflow.type, stepId, stepKey: step.key, stepType: step.type, attempt: attemptNo, durationMs: now().getTime() - startMs, error: handlerResult.error.message });
        await failStepTerminal(workflowId, step, handlerResult.error.message);
        return { ok: true, value: undefined };
      } catch (error) {
        const message = error instanceof Error ? error.message : 'Unknown step execution error';
        logger.error('Step execution threw', error instanceof Error ? error : new Error(message), { workflowId, stepId });
        span.recordError(error instanceof Error ? error : new Error(message));
        emit({ type: 'step.failed', workflowId, workflowType: workflow.type, stepId, stepKey: step.key, stepType: step.type, attempt: attemptNo, durationMs: now().getTime() - startMs, error: message });
        await failStepTerminal(workflowId, step, message);
        // Re-throw so the dispatcher can retry
        throw error;
      } finally {
        span.end();
      }
    } finally {
      if (releaseSlot) await releaseSlot();
    }
  }

  /**
   * Deliver an external event to a `waiting` step (gap 07): completes it with `payload`
   * as its output and advances the DAG. Idempotent — resuming a non-waiting step (e.g. a
   * re-delivered event) is a logged no-op. The host correlates the event to `stepKey`.
   */
  async function resumeStep(
    workflowId: WorkflowId,
    stepKey: string,
    payload: Record<string, unknown> = {},
  ): Promise<Result<void, FlowError>> {
    const workflow = await store.getWorkflow(workflowId);
    if (!workflow) return { ok: false, error: { key: 'workflow_not_found', message: `Workflow ${workflowId} not found` } };

    if (workflow.status !== 'running' && workflow.status !== 'pending') {
      logger.info('Ignoring resume for non-active workflow', { workflowId, stepKey, status: workflow.status });
      return { ok: true, value: undefined };
    }

    const step = (await store.listSteps(workflowId)).find((s) => s.key === stepKey);
    if (!step) return { ok: false, error: { key: 'workflow_not_found', message: `Step '${stepKey}' not found in workflow ${workflowId}` } };

    if (step.status !== 'waiting') {
      logger.info('Ignoring resume for non-waiting step', { workflowId, stepKey, status: step.status });
      return { ok: true, value: undefined };
    }

    emit({ type: 'step.resumed', workflowId, stepId: step.id, stepKey, stepType: step.type });
    await onStepCompleted(workflowId, step.id, payload);
    logger.info('Resumed waiting step', { workflowId, stepKey });
    return { ok: true, value: undefined };
  }

  // --------------------------------------------------------------------------
  // Advancement
  // --------------------------------------------------------------------------

  async function onStepCompleted(workflowId: WorkflowId, stepId: StepId, output: Record<string, unknown>): Promise<void> {
    const completedAt = nowIso();
    await store.completeStep({ workflowId, stepId, output, completedAt });

    const allSteps = await store.listSteps(workflowId);
    // Map children are internal — only keyed DAG steps drive readiness/termination (gap 06).
    const keyedSteps = allSteps.filter((s) => s.parentStepId == null);
    const completedKeys = new Set(keyedSteps.filter((s) => s.status === 'completed').map((s) => s.key));

    // Steps whose dependencies are now all complete
    const newlyReady = keyedSteps.filter((s) => {
      if (s.status !== 'pending') return false;
      return (s.dependencies ?? []).every((dep) => completedKeys.has(dep));
    });

    for (const readyStep of newlyReady) {
      await dispatchReadyStep(workflowId, readyStep.id, readyStep.key, readyStep.type);
    }

    // All terminal (treat the just-completed step as completed even though listSteps may be stale)
    const allTerminal = keyedSteps.every((s) => s.id === stepId || s.status === 'completed' || s.status === 'skipped');
    if (allTerminal && newlyReady.length === 0) {
      const aggregatedOutput: Record<string, unknown> = {};
      for (const s of keyedSteps) {
        if (s.status === 'completed' || s.id === stepId) {
          aggregatedOutput[s.key] = s.id === stepId ? output : (s.output ?? null);
        }
      }
      await store.finishWorkflow({ workflowId, status: 'completed', output: aggregatedOutput, completedAt });
      logger.info('Workflow completed', { workflowId });
      emit({ type: 'workflow.completed', workflowId });

      if (hooks.onWorkflowCompleted) {
        const finalWorkflow = await store.getWorkflow(workflowId);
        if (finalWorkflow) {
          // Fire-and-forget: never block / fail completion on bookkeeping
          Promise.resolve(hooks.onWorkflowCompleted({ workflowId, partitionKey, workflow: finalWorkflow })).catch((e) =>
            logger.error('onWorkflowCompleted hook failed', e instanceof Error ? e : new Error(String(e)), { workflowId }),
          );
        }
      }

      // If this workflow is a sub-workflow child, settle its parent step (gap 08).
      await bridgeSubWorkflow(workflowId);
    }
  }

  // --------------------------------------------------------------------------
  // Dynamic fan-out / map (gap 06)
  // --------------------------------------------------------------------------

  /** A map parent produced its item list — spawn one child per item and suspend it. */
  async function spawnMapChildren(
    workflowId: WorkflowId,
    parentStep: StepRecord,
    childType: string,
    itemsOutput: Record<string, unknown>,
  ): Promise<void> {
    const items = Array.isArray(itemsOutput.items) ? (itemsOutput.items as unknown[]) : [];
    if (items.length === 0) {
      // Nothing to fan out over — complete the map immediately with an empty result.
      await onStepCompleted(workflowId, parentStep.id, { items: [] });
      return;
    }
    const children = items.map((item, index) => ({
      key: `${parentStep.key}#${index}`,
      type: childType,
      input: { item, index },
    }));
    const created = await store.addChildSteps(workflowId, parentStep.id, children);
    await store.markStepMapping(parentStep.id);
    for (const child of created) {
      const result = await dispatcher.enqueueStep(
        { workflowId, stepId: child.id, stepKey: child.key, stepType: child.type },
        readyStepDelay(child.type),
      );
      if (!result.ok) logger.error('Failed to enqueue map child', new Error(result.error.message), { workflowId, stepKey: child.key });
    }
    logger.info('Map step spawned children', { workflowId, parentKey: parentStep.key, count: created.length });
  }

  /** A map child completed — complete the parent (with aggregated outputs) once all siblings finish. */
  async function onChildCompleted(workflowId: WorkflowId, childStep: StepRecord, output: Record<string, unknown>): Promise<void> {
    await store.completeStep({ workflowId, stepId: childStep.id, output, completedAt: nowIso() });
    const parentId = childStep.parentStepId!;
    const children = await store.listChildSteps(parentId);

    if (children.some((c) => c.status === 'failed')) {
      // The map already failed (a sibling errored) — re-check so the failure finalizes
      // the workflow once the remaining in-flight children settle.
      await checkWorkflowFailure(workflowId);
      return;
    }
    if (children.every((c) => c.status === 'completed')) {
      const aggregated = children.map((c) => c.output ?? null);
      await onStepCompleted(workflowId, parentId, { items: aggregated });
    }
  }

  /** Fail a map parent and skip its still-pending children (a sibling item errored). */
  async function failMapParent(workflowId: WorkflowId, parentStepId: StepId, message: string): Promise<void> {
    const parent = await store.getStep(parentStepId);
    if (!parent || parent.status === 'failed' || parent.status === 'completed') return;
    await markStepFailed(parentStepId, workflowId, message);
    for (const child of await store.listChildSteps(parentStepId)) {
      if (child.status === 'pending') await store.skipStep(child.id, 'Skipped: sibling map item failed');
    }
  }

  /** Terminal failure of a step — fails the parent map too if the step is a map child. */
  async function failStepTerminal(workflowId: WorkflowId, step: StepRecord, message: string): Promise<void> {
    await markStepFailed(step.id, workflowId, message);
    if (step.parentStepId != null) {
      await failMapParent(workflowId, step.parentStepId, `Map item failed: ${message}`);
    }
    await checkWorkflowFailure(workflowId);
  }

  // --------------------------------------------------------------------------
  // Child / sub-workflows (gap 08)
  // --------------------------------------------------------------------------

  /** A sub-workflow step ran — start the child workflow and suspend the parent step. */
  async function startSubWorkflow(
    workflowId: WorkflowId,
    parentStep: StepRecord,
    childDefinition: WorkflowDefinition,
    childInput: Record<string, unknown>,
  ): Promise<void> {
    // Suspend first so the child's terminal bridge always finds the parent step `waiting`.
    await store.markStepWaiting(parentStep.id);
    emit({ type: 'step.waiting', workflowId, stepId: parentStep.id, stepKey: parentStep.key, stepType: parentStep.type });
    const started = await startWorkflow(childDefinition, childInput, {
      parentWorkflowId: workflowId,
      parentStepId: parentStep.id,
    });
    if (!started.ok) {
      logger.error('Failed to start sub-workflow', new Error(started.error.message), { workflowId, stepKey: parentStep.key });
      await failStepTerminal(workflowId, parentStep, `Sub-workflow start failed: ${started.error.message}`);
      return;
    }
    logger.info('Sub-workflow started', { workflowId, parentKey: parentStep.key, childWorkflowId: started.value.workflowId });
  }

  /**
   * A workflow reached a terminal state — if it's a sub-workflow child, settle its parent
   * step: complete it with the child's output, or fail it (and cascade) on failure/cancel.
   * Idempotent — a no-op unless the parent step is still `waiting`.
   */
  async function bridgeSubWorkflow(childWorkflowId: WorkflowId): Promise<void> {
    const child = await store.getWorkflow(childWorkflowId);
    if (!child || child.parentWorkflowId == null || child.parentStepId == null) return;

    const parentStep = await store.getStep(child.parentStepId);
    if (!parentStep || parentStep.status !== 'waiting') return;

    if (child.status === 'completed') {
      await onStepCompleted(child.parentWorkflowId, child.parentStepId, child.output ?? {});
    } else {
      await markStepFailed(child.parentStepId, child.parentWorkflowId, `Sub-workflow ${child.status}${child.error ? `: ${child.error}` : ''}`);
      await checkWorkflowFailure(child.parentWorkflowId);
    }
    logger.info('Bridged sub-workflow to parent step', { childWorkflowId, parentWorkflowId: child.parentWorkflowId, status: child.status });
  }

  // --------------------------------------------------------------------------
  // Compensation / saga (gap 09)
  // --------------------------------------------------------------------------

  /**
   * Run each completed step's `compensate` handler in reverse dependency order to undo side
   * effects after the workflow failed. Best-effort: one attempt per step, throws are logged and
   * surfaced on the step (status `compensated` with the error). Steps without a handler are left.
   */
  async function compensateWorkflow(workflowId: WorkflowId): Promise<void> {
    const workflow = await store.getWorkflow(workflowId);
    if (!workflow) return;
    const steps = await store.listSteps(workflowId);
    const compensatable = steps.filter((s) => s.status === 'completed' && registry.getRegistration(s.type)?.compensate);
    if (compensatable.length === 0) return;

    const byKey = new Map(steps.map((s) => [s.key, s] as const));
    for (const step of reverseTopologicalOrder(compensatable)) {
      const compensate = registry.getRegistration(step.type)?.compensate;
      if (!compensate) continue;
      await store.markStepCompensating(step.id);
      emit({ type: 'step.compensating', workflowId, workflowType: workflow.type, stepId: step.id, stepKey: step.key, stepType: step.type });
      try {
        const dependencyOutputs: Record<string, unknown> = {};
        for (const depKey of step.dependencies ?? []) dependencyOutputs[depKey] = byKey.get(depKey)?.output ?? undefined;
        const context = hooks.buildStepContext
          ? await hooks.buildStepContext({ workflowId, stepId: step.id, stepKey: step.key, partitionKey, workflow, step })
          : (undefined as TContext);
        await compensate({
          workflowId,
          stepId: step.id,
          stepKey: step.key,
          partitionKey,
          workflowInput: workflow.input ?? {},
          stepInput: step.input ?? {},
          dependencyOutputs,
          context,
          output: step.output ?? {},
        });
        await store.markStepCompensated(step.id);
        logger.info('Compensated step', { workflowId, stepKey: step.key });
        emit({ type: 'step.compensated', workflowId, workflowType: workflow.type, stepId: step.id, stepKey: step.key, stepType: step.type });
      } catch (error) {
        const message = error instanceof Error ? error.message : 'Unknown compensation error';
        logger.error('Compensation failed', error instanceof Error ? error : new Error(message), { workflowId, stepKey: step.key });
        await store.markStepCompensated(step.id, `Compensation failed: ${message}`);
        emit({ type: 'step.compensated', workflowId, workflowType: workflow.type, stepId: step.id, stepKey: step.key, stepType: step.type, error: message });
      }
    }
  }

  async function markStepFailed(stepId: StepId, workflowId: WorkflowId, errorMessage: string): Promise<void> {
    await store.failStep({ workflowId, stepId, error: errorMessage, completedAt: nowIso() });
  }

  async function checkWorkflowFailure(workflowId: WorkflowId): Promise<void> {
    // Skip pending steps blocked by a failed OR already-skipped dependency,
    // iterating to a fixpoint so failures cascade transitively through a chain
    // (a→b→c: failing `a` skips `b`, then `c` which depends on the skipped `b`).
    for (;;) {
      const steps = await store.listSteps(workflowId);
      const blockedKeys = new Set(
        steps.filter((s) => s.status === 'failed' || s.status === 'skipped').map((s) => s.key),
      );
      const toSkip = steps.filter(
        (s) => s.status === 'pending' && (s.dependencies ?? []).some((dep) => blockedKeys.has(dep)),
      );
      if (toSkip.length === 0) break;
      for (const s of toSkip) {
        await store.skipStep(s.id, 'Skipped due to failed dependency');
        emit({ type: 'step.skipped', workflowId, stepId: s.id, stepKey: s.key, stepType: s.type });
      }
    }

    const refreshed = await store.listSteps(workflowId);
    const allTerminal = refreshed.every((s) => ['completed', 'failed', 'skipped'].includes(s.status));
    if (allTerminal) {
      const firstFailed = refreshed.find((s) => s.status === 'failed');
      await store.finishWorkflow({ workflowId, status: 'failed', error: firstFailed?.error ?? 'One or more steps failed', completedAt: nowIso() });
      logger.info('Workflow failed', { workflowId });
      emit({ type: 'workflow.failed', workflowId, error: firstFailed?.error ?? 'One or more steps failed' });
      // Undo completed steps' side effects in reverse order (gap 09), then…
      await compensateWorkflow(workflowId);
      // …propagate failure to the parent step if this is a sub-workflow child (gap 08).
      await bridgeSubWorkflow(workflowId);
    }
  }

  // --------------------------------------------------------------------------
  // Cancel / status / recovery
  // --------------------------------------------------------------------------

  async function cancelWorkflow(workflowId: WorkflowId): Promise<Result<void, FlowError>> {
    const workflow = await store.getWorkflow(workflowId);
    if (!workflow) return { ok: false, error: { key: 'workflow_not_found', message: `Workflow ${workflowId} not found` } };
    if (workflow.status !== 'pending' && workflow.status !== 'running') return { ok: true, value: undefined };

    await store.skipPendingSteps(workflowId, 'Workflow cancelled');
    await store.finishWorkflow({ workflowId, status: 'cancelled', completedAt: nowIso() });
    logger.info('Workflow cancelled', { workflowId });
    emit({ type: 'workflow.cancelled', workflowId });
    // A cancelled sub-workflow child fails its parent step (gap 08).
    await bridgeSubWorkflow(workflowId);
    return { ok: true, value: undefined };
  }

  async function getWorkflowStatus(workflowId: WorkflowId): Promise<Result<WorkflowWithSteps, FlowError>> {
    const workflow = await store.getWorkflow(workflowId);
    if (!workflow) return { ok: false, error: { key: 'workflow_not_found', message: `Workflow ${workflowId} not found` } };
    const steps = await store.listSteps(workflowId);
    return { ok: true, value: { ...workflow, steps } };
  }

  async function listWorkflows(filters?: {
    status?: WorkflowStatus;
    type?: string;
    entityRef?: string;
    limit?: number;
  }): Promise<Result<WorkflowWithSteps[], FlowError>> {
    const workflows = await store.listWorkflows({ ...filters, limit: filters?.limit ?? 50 });
    return { ok: true, value: workflows };
  }

  async function handleStepExhausted(workflowId: WorkflowId, stepId: StepId, errorMessage: string): Promise<void> {
    const step = await store.getStep(stepId);
    if (!step || step.workflowId !== workflowId || ['completed', 'failed', 'skipped'].includes(step.status)) return;
    await markStepFailed(stepId, workflowId, errorMessage);
    await checkWorkflowFailure(workflowId);
  }

  /**
   * Recover workflows whose step is stuck in `running` past the dispatcher expiry
   * (worker crashed between marking `running` and completing). Marks such steps
   * failed and cascades the workflow to `failed`.
   */
  async function recoverStuckWorkflows(): Promise<{ recoveredSteps: number; recoveredWorkflows: number }> {
    const stuckThresholdSeconds = stepExpirySeconds + stuckStepBufferSeconds;
    const cutoff = new Date(now().getTime() - stuckThresholdSeconds * 1000).toISOString();

    const runningWorkflows = await store.listRunningWorkflows();
    if (runningWorkflows.length === 0) return { recoveredSteps: 0, recoveredWorkflows: 0 };

    let recoveredSteps = 0;
    const affectedWorkflowIds = new Set<WorkflowId>();

    for (const workflow of runningWorkflows) {
      const stuckSteps = await store.findStuckSteps(workflow.id, cutoff);
      for (const step of stuckSteps) {
        logger.warn('Recovering stuck workflow step', { workflowId: workflow.id, stepId: step.id, stepKey: step.key, startedAt: step.startedAt });
        await markStepFailed(step.id, workflow.id, `Step exceeded ${stuckThresholdSeconds}s without completion (worker likely crashed)`);
        recoveredSteps++;
        affectedWorkflowIds.add(workflow.id);
      }
    }

    for (const workflowId of affectedWorkflowIds) {
      await checkWorkflowFailure(workflowId);
    }

    return { recoveredSteps, recoveredWorkflows: affectedWorkflowIds.size };
  }

  return {
    validateDefinition,
    startWorkflow,
    start,
    executeStep,
    resumeStep,
    handleStepExhausted,
    recoverStuckWorkflows,
    cancelWorkflow,
    getWorkflowStatus,
    listWorkflows,
  };
}

export type WorkflowEngine<TContext = unknown> = ReturnType<typeof createWorkflowEngine<TContext>>;

// Re-export for convenience: a record shape used by status helpers
export type { WorkflowRecord, StepRecord };
