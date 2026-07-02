import { describe, it, expect } from 'vitest';
import { createInMemoryStepGate, type StepGate, type StepGateRequest } from './gate';

const req = (stepType: string): StepGateRequest => ({
  partitionKey: 'test',
  workflowId: 1,
  stepId: 1,
  stepKey: 'k',
  stepType,
});

async function admit(gate: StepGate, stepType: string) {
  const d = await gate.acquire(req(stepType));
  return d;
}

describe('createInMemoryStepGate', () => {
  it('admits any step type with no rule', async () => {
    const gate = createInMemoryStepGate();
    const d = await admit(gate, 'free');
    expect(d.admitted).toBe(true);
  });

  it('enforces a per-stepType concurrency cap and frees on release', async () => {
    const gate = createInMemoryStepGate({ concurrency: { x: { maxConcurrent: 2 } }, concurrencyRetrySeconds: 5 });

    const a = await admit(gate, 'x');
    const b = await admit(gate, 'x');
    const c = await admit(gate, 'x');
    expect(a.admitted && b.admitted).toBe(true);
    expect(c.admitted).toBe(false);
    if (!c.admitted) expect(c.retryAfterSeconds).toBe(5);

    // releasing a slot lets the next one in
    if (a.admitted) a.release();
    const d = await admit(gate, 'x');
    expect(d.admitted).toBe(true);

    // a different step type is unaffected
    const other = await admit(gate, 'y');
    expect(other.admitted).toBe(true);
  });

  it('enforces a token-bucket rate limit and refills over time', async () => {
    let t = 1_000_000;
    const gate = createInMemoryStepGate({ rateLimit: { llm: { perSecond: 2, burst: 2 } }, now: () => t });

    // burst of 2 is admitted, the 3rd is denied
    expect((await admit(gate, 'llm')).admitted).toBe(true);
    expect((await admit(gate, 'llm')).admitted).toBe(true);
    const denied = await admit(gate, 'llm');
    expect(denied.admitted).toBe(false);
    if (!denied.admitted) expect(denied.retryAfterSeconds).toBeGreaterThanOrEqual(1);

    // after 1s, 2 tokens refilled → admitted again
    t += 1000;
    expect((await admit(gate, 'llm')).admitted).toBe(true);
  });

  it('does not consume a concurrency slot when the rate limit denies', async () => {
    let t = 0;
    const gate = createInMemoryStepGate({
      concurrency: { z: { maxConcurrent: 1 } },
      rateLimit: { z: { perSecond: 1, burst: 1 } },
      now: () => t,
    });

    // first admit consumes the single token AND the single slot, then releases the slot
    const first = await admit(gate, 'z');
    expect(first.admitted).toBe(true);
    if (first.admitted) await first.release();

    // token bucket is empty → denied; crucially the slot was freed, so it's a rate denial
    const second = await admit(gate, 'z');
    expect(second.admitted).toBe(false);

    // refill the token → the slot is still free, so admitted
    t += 1000;
    const third = await admit(gate, 'z');
    expect(third.admitted).toBe(true);
  });
});
