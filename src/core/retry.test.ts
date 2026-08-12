import { describe, it, expect } from 'vitest';
import {
  isRetryableError,
  explicitRetryability,
  markRetryable,
  retryableError,
  nonRetryableError,
} from './retry';

describe('isRetryableError', () => {
  it.each([
    ['rate limit exceeded', true],
    ['HTTP 429 returned', true],
    ['Too Many Requests', true],
    ['request timeout after 30s', true],
    ['read ECONNRESET', true],
    ['fetch failed', true],
    ['503 from upstream', true],
    ['service unavailable', true],
    ['bad request', false],
    ['validation failed', false],
    ['some other error', false],
  ])('classifies %j as retryable=%s', (message, expected) => {
    expect(isRetryableError(new Error(message))).toBe(expected);
  });

  it('returns false for non-Error values', () => {
    expect(isRetryableError('rate limit')).toBe(false);
    expect(isRetryableError(null)).toBe(false);
    expect(isRetryableError(undefined)).toBe(false);
    expect(isRetryableError({ message: '429' })).toBe(false);
  });
});

describe('explicit retryability', () => {
  it('overrides the heuristic in both directions', () => {
    // the heuristic would say false — "connection refused" is transient but unrecognised
    expect(isRetryableError(new Error('connection refused'))).toBe(false);
    expect(isRetryableError(retryableError('connection refused'))).toBe(true);

    // the heuristic would say true — a permanent bug that merely mentions a timeout
    expect(isRetryableError(new Error('bug: timeout config must be positive'))).toBe(true);
    expect(isRetryableError(nonRetryableError('bug: timeout config must be positive'))).toBe(false);
  });

  it('markRetryable tags an existing error in place and returns it', () => {
    const err = new Error('encoder busy');
    expect(isRetryableError(err)).toBe(false);
    expect(markRetryable(err)).toBe(err); // same reference, so it can wrap a throw
    expect(isRetryableError(err)).toBe(true);
    expect(isRetryableError(markRetryable(err, false))).toBe(false);
  });

  it('does not make the marker enumerable (it must not leak into serialisation)', () => {
    const err = retryableError('boom');
    expect(Object.keys(err)).toEqual([]);
    expect(JSON.stringify({ ...err })).toBe('{}');
  });

  it('reports whether an error carries an explicit decision', () => {
    expect(explicitRetryability(new Error('rate limit'))).toBeUndefined();
    expect(explicitRetryability(retryableError('x'))).toBe(true);
    expect(explicitRetryability(nonRetryableError('x'))).toBe(false);
    expect(explicitRetryability('not an object')).toBeUndefined();
    expect(explicitRetryability(null)).toBeUndefined();
  });

  it('marks non-Error objects too, and ignores primitives', () => {
    const obj = { code: 'E_BUSY' };
    expect(isRetryableError(markRetryable(obj))).toBe(true);
    expect(markRetryable('str')).toBe('str'); // no throw, unchanged
  });

  it('preserves the cause when constructing', () => {
    const cause = new Error('root');
    expect(retryableError('wrapped', { cause }).cause).toBe(cause);
  });
});
