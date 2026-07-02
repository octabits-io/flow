import type { WorkflowHooks, BuildStepContextArgs, Result, FlowErrorShape } from '../core';
import type { LanguageModelV4 } from '@ai-sdk/provider';
import { createInstrumentedModel, createUsageAccumulator, type AccumulatedUsage } from './instrumented-model';
import { createCostEstimator, type CostEstimator } from './cost';
import type { AiContext } from './context';

// ============================================================================
// Injected collaborators (host implements these against its own schema/services)
// ============================================================================

export interface AiModelResolver<THost = unknown> {
  /** Resolve the base (uninstrumented) model for a step. */
  resolveModel(args: BuildStepContextArgs): Promise<LanguageModelV4> | LanguageModelV4;
  /** Optional host context (DI scope, services) exposed to handlers as `ctx.context.host`. */
  resolveHost?(args: BuildStepContextArgs): Promise<THost> | THost;
  /** Which key paid for the run — stamped into workflow metadata for usage rollups. */
  resolveKeySource?(): Promise<'platform' | 'tenant'> | 'platform' | 'tenant';
}

export interface AiUsageRecorder {
  /** Persist per-step token usage + cost (e.g. onto the step row). */
  recordStepUsage(args: { workflowId: number; stepId: number; usage: AccumulatedUsage; costMicros: number }): Promise<void>;
  /** Atomically add this step's usage + cost to the workflow's running totals. */
  incrementWorkflowUsage(args: { workflowId: number; usage: AccumulatedUsage; costMicros: number }): Promise<void>;
  /**
   * Roll the completed workflow's totals into a daily aggregate. The host reads
   * the workflow's accumulated totals from its own store and UPSERTs the rollup.
   */
  recordWorkflowDaily?(args: { workflowId: number; workflowType: string; keySource: 'platform' | 'tenant'; date: string }): Promise<void>;
}

export interface AiQuotaPolicy {
  /** Return an error (e.g. `ai_quota_exceeded`) to reject a workflow start. */
  checkQuota(): Promise<Result<void, FlowErrorShape>>;
}

// ============================================================================
// Hook factory
// ============================================================================

export interface CreateAiWorkflowHooksDeps<THost = unknown> {
  modelResolver: AiModelResolver<THost>;
  usageRecorder: AiUsageRecorder;
  /** Optional quota guard. BYOK exemption etc. is the policy's concern. */
  quotaPolicy?: AiQuotaPolicy;
  /** Defaults to `createCostEstimator()` (the built-in pricing table). */
  costEstimator?: CostEstimator;
  /** Clock injection for tests. */
  now?: () => Date;
}

/**
 * Wire the AI behaviors (model instrumentation, token/cost capture, quota,
 * usage rollups) into a set of flow-core lifecycle hooks. Pass the result as the
 * engine's `hooks`. flow-core stays AI-free; everything AI lives here.
 */
export function createAiWorkflowHooks<THost = unknown>(
  deps: CreateAiWorkflowHooksDeps<THost>,
): WorkflowHooks<AiContext<THost>> {
  const estimate = deps.costEstimator ?? createCostEstimator();
  const now = deps.now ?? (() => new Date());

  return {
    async onBeforeStart() {
      if (deps.quotaPolicy) {
        const q = await deps.quotaPolicy.checkQuota();
        if (!q.ok) return q;
      }
      let metadata: Record<string, unknown> | undefined;
      if (deps.modelResolver.resolveKeySource) {
        metadata = { keySource: await deps.modelResolver.resolveKeySource() };
      }
      return { ok: true, value: { metadata } };
    },

    async buildStepContext(args) {
      const base = await deps.modelResolver.resolveModel(args);
      const usage = createUsageAccumulator();
      const model = createInstrumentedModel(base, usage);
      const host = deps.modelResolver.resolveHost
        ? await deps.modelResolver.resolveHost(args)
        : (undefined as THost);
      return { model, host, usage };
    },

    async onAfterStep({ workflowId, stepId, context }) {
      const acc = context.usage;
      if (!acc.hasUsage()) return;
      const usage = acc.get();
      const costMicros = estimate(usage, usage.modelId);
      await deps.usageRecorder.recordStepUsage({ workflowId, stepId, usage, costMicros });
      await deps.usageRecorder.incrementWorkflowUsage({ workflowId, usage, costMicros });
    },

    async onWorkflowCompleted({ workflowId, workflow }) {
      if (!deps.usageRecorder.recordWorkflowDaily) return;
      const keySource = (workflow.metadata?.keySource as 'platform' | 'tenant') ?? 'platform';
      const date = now().toISOString().split('T')[0]!;
      await deps.usageRecorder.recordWorkflowDaily({ workflowId, workflowType: workflow.type, keySource, date });
    },
  };
}
