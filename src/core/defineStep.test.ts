import { describe, it, expect } from 'vitest';
import { z } from 'zod';
import { defineStep, buildWorkflow } from './defineStep';
import type { StepExecutionContext } from './types';

function ctx(over: Partial<StepExecutionContext> = {}): StepExecutionContext {
  return {
    workflowId: 1,
    stepId: 1,
    stepKey: 'k',
    partitionKey: 'p',
    workflowInput: {},
    stepInput: {},
    dependencyOutputs: {},
    context: undefined,
    ...over,
  };
}

describe('defineStep validation pipeline', () => {
  it('rejects invalid workflow input as non-retryable', async () => {
    const step = defineStep({
      type: 't',
      workflowInputSchema: z.object({ n: z.number() }),
      outputSchema: z.object({}),
      handler: async () => ({}),
    });
    const res = await step.handler(ctx({ workflowInput: { n: 'not-a-number' } }));
    expect(res.ok).toBe(false);
    if (res.ok) return;
    expect(res.error.message).toMatch(/Invalid workflow input/);
    expect(res.error.retryable).toBe(false);
  });

  it('rejects an invalid dependency output', async () => {
    const dep = defineStep({
      type: 'dep',
      workflowInputSchema: z.object({}),
      outputSchema: z.object({ v: z.number() }),
      handler: async () => ({ v: 1 }),
    });
    const step = defineStep({
      type: 't',
      workflowInputSchema: z.object({}),
      outputSchema: z.object({}),
      dependencies: { dep },
      handler: async () => ({}),
    });
    const res = await step.handler(ctx({ dependencyOutputs: { dep: { v: 'bad' } } }));
    expect(res.ok).toBe(false);
    if (res.ok) return;
    expect(res.error.message).toMatch(/Invalid dependency output 'dep'/);
  });

  it('rejects an invalid handler output', async () => {
    const step = defineStep({
      type: 't',
      workflowInputSchema: z.object({}),
      outputSchema: z.object({ v: z.number() }),
      handler: async () => ({ v: 'nope' } as unknown as { v: number }),
    });
    const res = await step.handler(ctx());
    expect(res.ok).toBe(false);
    if (res.ok) return;
    expect(res.error.message).toMatch(/Invalid step output/);
  });

  it('marks thrown rate-limit errors as retryable', async () => {
    const step = defineStep({
      type: 't',
      workflowInputSchema: z.object({}),
      outputSchema: z.object({}),
      handler: async () => {
        throw new Error('429 rate limit');
      },
    });
    const res = await step.handler(ctx());
    expect(res.ok).toBe(false);
    if (res.ok) return;
    expect(res.error.retryable).toBe(true);
  });
});

describe('buildWorkflow', () => {
  it('throws when a step depends on an unknown step key', () => {
    const a = defineStep({ type: 'a', workflowInputSchema: z.object({}), outputSchema: z.object({}), handler: async () => ({}) });
    // 'ghost' is not a key in the steps map below
    const bad = defineStep({ type: 'b', workflowInputSchema: z.object({}), outputSchema: z.object({}), dependencies: { ghost: a }, handler: async () => ({}) });
    expect(() => buildWorkflow({ type: 'w', inputSchema: z.object({}), steps: { b: bad } })).toThrow(/not a valid step key/);
  });

  it('derives the DAG definition from step metadata', () => {
    const a = defineStep({ type: 'a', workflowInputSchema: z.object({}), outputSchema: z.object({}), handler: async () => ({}) });
    const b = defineStep({ type: 'b', workflowInputSchema: z.object({}), outputSchema: z.object({}), dependencies: { a }, handler: async () => ({}) });
    const wf = buildWorkflow({ type: 'w', inputSchema: z.object({}), steps: { a, b } });
    expect(wf.definition).toEqual({
      type: 'w',
      steps: [
        { key: 'a', type: 'a' },
        { key: 'b', type: 'b', dependencies: ['a'] },
      ],
    });
  });
});
