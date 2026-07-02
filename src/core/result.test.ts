import { describe, it, expect } from 'vitest';
import { ok, err } from './result';

describe('Result helpers', () => {
  it('ok wraps a value', () => {
    expect(ok(42)).toEqual({ ok: true, value: 42 });
  });

  it('err wraps an error', () => {
    expect(err({ key: 'boom', message: 'failed' })).toEqual({ ok: false, error: { key: 'boom', message: 'failed' } });
  });
});
