import { describe, it, expect } from 'vitest';
import { computeReadiness } from './readiness';
import type { JoinRule } from './readiness';
import type { StepRecord, StepStatus } from './types';

let seq = 0;
function step(key: string, status: StepStatus, dependencies: string[] = [], parentStepId: number | null = null): StepRecord {
  return {
    id: ++seq,
    workflowId: 1,
    key,
    type: `t:${key}`,
    status,
    dependencies,
    input: null,
    output: null,
    error: null,
    metadata: null,
    attempts: 0,
    parentStepId,
    heartbeatAt: null,
    startedAt: null,
    completedAt: null,
  };
}

/** Join rules keyed by the step key they were declared for (types are `t:<key>`). */
const joins = (byKey: Record<string, JoinRule>) => (stepType: string) => byKey[stepType.slice(2)] ?? 'all';
const allJoin = () => 'all' as const;

const keys = (steps: StepRecord[]) => steps.map((s) => s.key);

describe('computeReadiness', () => {
  it('makes dependency-free steps ready immediately', () => {
    const plan = computeReadiness([step('a', 'pending'), step('b', 'pending')], allJoin);
    expect(keys(plan.ready)).toEqual(['a', 'b']);
    expect(plan.skip).toEqual([]);
  });

  it('holds a step until every dependency has completed', () => {
    const steps = [step('a', 'completed'), step('b', 'running'), step('c', 'pending', ['a', 'b'])];
    expect(keys(computeReadiness(steps, allJoin).ready)).toEqual([]);

    steps[1]!.status = 'completed';
    expect(keys(computeReadiness(steps, allJoin).ready)).toEqual(['c']);
  });

  it('cascades a failure through a chain in one pass', () => {
    const steps = [
      step('a', 'failed'),
      step('b', 'pending', ['a']),
      step('c', 'pending', ['b']),
      step('unrelated', 'pending'),
    ];
    const plan = computeReadiness(steps, allJoin);
    expect(keys(plan.skip.map((s) => s.step))).toEqual(['b', 'c']);
    expect(plan.skip[0]!.reason).toBe('Skipped due to failed dependency');
    // An independent branch is untouched by the failure.
    expect(keys(plan.ready)).toEqual(['unrelated']);
  });

  it('treats a skipped or compensated dependency as unreachable under join "all"', () => {
    for (const status of ['skipped', 'compensated'] as const) {
      const plan = computeReadiness([step('a', status), step('b', 'pending', ['a'])], allJoin);
      expect(keys(plan.skip.map((s) => s.step))).toEqual(['b']);
    }
  });

  // --- join: 'any' ---------------------------------------------------------

  it('holds a join: "any" step while a branch is still in flight', () => {
    const steps = [step('yes', 'completed'), step('no', 'running'), step('join', 'pending', ['yes', 'no'])];
    const plan = computeReadiness(steps, joins({ join: 'any' }));
    expect(keys(plan.ready)).toEqual([]);
    expect(plan.skip).toEqual([]);
  });

  it('runs a join: "any" step once the untaken branches are skipped', () => {
    const steps = [step('yes', 'completed'), step('no', 'skipped'), step('join', 'pending', ['yes', 'no'])];
    const plan = computeReadiness(steps, joins({ join: 'any' }));
    expect(keys(plan.ready)).toEqual(['join']);
    expect(plan.skip).toEqual([]);
  });

  it('skips a join: "any" step when no branch completed', () => {
    const steps = [step('yes', 'skipped'), step('no', 'skipped'), step('join', 'pending', ['yes', 'no'])];
    const plan = computeReadiness(steps, joins({ join: 'any' }));
    expect(keys(plan.ready)).toEqual([]);
    expect(plan.skip[0]!.reason).toBe('Skipped: no dependency branch completed');
  });

  it('still treats a failed dependency as poison under join: "any"', () => {
    const steps = [step('yes', 'completed'), step('no', 'failed'), step('join', 'pending', ['yes', 'no'])];
    const plan = computeReadiness(steps, joins({ join: 'any' }));
    expect(keys(plan.ready)).toEqual([]);
    expect(keys(plan.skip.map((s) => s.step))).toEqual(['join']);
  });

  it('does not let a skipped branch prune the join it feeds', () => {
    // The whole point: under the default rule this join would be skipped too.
    const steps = [step('yes', 'completed'), step('no', 'skipped'), step('join', 'pending', ['yes', 'no'])];
    expect(keys(computeReadiness(steps, allJoin).skip.map((s) => s.step))).toEqual(['join']);
    expect(keys(computeReadiness(steps, joins({ join: 'any' })).ready)).toEqual(['join']);
  });

  // --- map children --------------------------------------------------------

  it('ignores map children entirely', () => {
    const parent = step('map', 'mapping');
    const child = step('map#0', 'pending', [], parent.id);
    const plan = computeReadiness([parent, child, step('after', 'pending', ['map'])], allJoin);
    expect(keys(plan.ready)).toEqual([]);
    expect(plan.skip).toEqual([]);
  });
});
