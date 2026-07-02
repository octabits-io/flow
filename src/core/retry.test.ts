import { describe, it, expect } from 'vitest';
import { isRetryableError } from './retry';

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
