import type { StepRecord } from './types';

// ============================================================================
// Readiness — which steps may run, which are unreachable
// ============================================================================
//
// One pure function answers both questions the engine asks after every
// transition: *what just became runnable* and *what can never run any more*.
// It used to be two inline loops (one in the completion path, one in the
// failure path) that disagreed about join rules; keeping it here makes the
// answer a value the engine can test, and the only place a new edge rule has
// to be taught.

/**
 * How a step's dependencies gate it.
 *
 * - `'all'` (default) — every dependency must have **completed**. A dependency
 *   that failed or was skipped makes the step unreachable, so it is skipped
 *   too, and that verdict cascades down the DAG.
 * - `'any'` — the step runs once every dependency has **settled** and **at
 *   least one completed**. Skipped dependencies are tolerated. This is the
 *   join for a conditional branch: exactly one arm runs, the others are
 *   skipped, and the join still fires — once. If every arm was skipped, so is
 *   the join; a *failed* dependency still poisons it, because the workflow is
 *   failing regardless.
 */
export type JoinRule = 'all' | 'any';

/** Statuses a step can no longer move out of on its own. */
const TERMINAL_STATUSES = new Set(['completed', 'failed', 'skipped', 'compensated']);

export function isTerminalStepStatus(status: string): boolean {
  return TERMINAL_STATUSES.has(status);
}

/** A step the plan says can never run, with the reason to record on it. */
export interface PlannedSkip {
  step: StepRecord;
  reason: string;
}

export interface ReadinessPlan {
  /** `pending` steps whose dependencies are satisfied — dispatch these. */
  ready: StepRecord[];
  /** `pending` steps that are now unreachable — skip these (already cascaded). */
  skip: PlannedSkip[];
}

const BLOCKED_REASON = 'Skipped due to failed dependency';
const NO_BRANCH_REASON = 'Skipped: no dependency branch completed';

type Verdict = 'ready' | 'wait' | 'skip';

/**
 * Decide, for one snapshot of a workflow's steps, which are runnable and which
 * are unreachable. Pure and deterministic — no store, no clock.
 *
 * Only **keyed** steps participate: map children (`parentStepId != null`) are
 * internal to their parent and never drive DAG readiness.
 *
 * The unreachable set is computed to a fixpoint, so a failure cascades through
 * a chain in one call (a→b→c: failing `a` skips `b`, which then skips `c`).
 *
 * @param steps every step of the workflow, as read from the store
 * @param joinOf the join rule for a step type, from its registration
 */
export function computeReadiness(
  steps: StepRecord[],
  joinOf: (stepType: string) => JoinRule,
): ReadinessPlan {
  const keyed = steps.filter((s) => s.parentStepId == null);
  const statusByKey = new Map(keyed.map((s) => [s.key, s.status as string] as const));

  // Keys this plan has decided to skip. Folded into the status lookup so a
  // verdict reached in one pass feeds the next.
  const planned = new Map<string, string>();
  const statusOf = (key: string): string | undefined => planned.get(key) ?? statusByKey.get(key);

  function evaluate(step: StepRecord): Verdict {
    const deps = step.dependencies ?? [];
    if (deps.length === 0) return 'ready';

    const states = deps.map(statusOf);
    // A dependency the snapshot doesn't know about can't be judged — wait
    // rather than guess. (A validated DAG never gets here.)
    if (states.some((s) => s === undefined)) return 'wait';

    if (joinOf(step.type) === 'all') {
      if (states.some((s) => s !== 'completed' && isTerminalStepStatus(s!))) return 'skip';
      return states.every((s) => s === 'completed') ? 'ready' : 'wait';
    }

    // 'any': one arm is enough, but only once the others have stopped moving.
    if (states.some((s) => s === 'failed')) return 'skip';
    if (!states.every((s) => isTerminalStepStatus(s!))) return 'wait';
    return states.some((s) => s === 'completed') ? 'ready' : 'skip';
  }

  const pending = keyed.filter((s) => s.status === 'pending');

  const skip: PlannedSkip[] = [];
  for (;;) {
    let changed = false;
    for (const step of pending) {
      if (planned.has(step.key)) continue;
      if (evaluate(step) !== 'skip') continue;
      const anyJoin = joinOf(step.type) === 'any';
      skip.push({ step, reason: anyJoin ? NO_BRANCH_REASON : BLOCKED_REASON });
      planned.set(step.key, 'skipped');
      changed = true;
    }
    if (!changed) break;
  }

  const ready = pending.filter((s) => !planned.has(s.key) && evaluate(s) === 'ready');

  return { ready, skip };
}
