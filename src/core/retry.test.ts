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

  it('never throws when the error cannot carry a property', () => {
    // Some SDKs hand out frozen errors. Marking is usually evaluated inside a
    // `throw`, so a TypeError here would replace the caller's actual failure.
    for (const harden of [Object.freeze, Object.seal, Object.preventExtensions]) {
      const err = harden(new Error('encoder busy'));
      expect(() => markRetryable(err)).not.toThrow();
      expect(isRetryableError(err)).toBe(true);
      expect(isRetryableError(markRetryable(err, false))).toBe(false);
    }
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

  it('follows the cause chain, so wrapping does not lose the decision', () => {
    // wrapping in a catch-and-rethrow is the norm, not the exception
    const wrapped = new Error('upstream failed', { cause: retryableError('busy') });
    expect(isRetryableError(wrapped)).toBe(true);

    const twice = new Error('outer', { cause: new Error('mid', { cause: nonRetryableError('bad config') }) });
    expect(isRetryableError(twice)).toBe(false);
  });

  it('lets the outermost decision win over a deeper one', () => {
    const inner = retryableError('busy');
    expect(isRetryableError(nonRetryableError('giving up', { cause: inner }))).toBe(false);
  });

  it('survives a cyclic cause chain', () => {
    const a = new Error('a') as Error & { cause?: unknown };
    const b = new Error('b') as Error & { cause?: unknown };
    a.cause = b;
    b.cause = a;
    expect(() => isRetryableError(a)).not.toThrow();
    expect(isRetryableError(a)).toBe(false);
  });
});

describe('structured-field classification', () => {
  it.each([
    ['ECONNRESET', true],
    ['ECONNREFUSED', true],
    ['ETIMEDOUT', true],
    ['EAI_AGAIN', true],
    ['UND_ERR_CONNECT_TIMEOUT', true],
    ['ENOTFOUND', false], // bad hostname — will not fix itself
    ['ERR_INVALID_ARG_TYPE', false], // a bug
  ])('classifies code %s as retryable=%s', (code, expected) => {
    expect(isRetryableError(Object.assign(new Error('boom'), { code }))).toBe(expected);
  });

  it.each([
    [408, true],
    [425, true],
    [429, true],
    [500, true],
    [502, true],
    [503, true],
    [504, true],
    [501, false], // not implemented — permanent
    [505, false],
    [400, false],
    [404, false],
    [422, false],
  ])('classifies HTTP %s as retryable=%s', (status, expected) => {
    expect(isRetryableError(Object.assign(new Error('http'), { status }))).toBe(expected);
  });

  it('reads status from the shapes clients actually use', () => {
    expect(isRetryableError(Object.assign(new Error('x'), { statusCode: 503 }))).toBe(true);
    expect(isRetryableError(Object.assign(new Error('x'), { httpStatusCode: 503 }))).toBe(true);
    expect(isRetryableError(Object.assign(new Error('x'), { response: { status: 503 } }))).toBe(true);
  });

  it('lets a status outrank a misleading message', () => {
    // "timeout" in the message would otherwise say retry; a 400 is definitive
    const err = Object.assign(new Error('timeout value rejected'), { status: 400 });
    expect(isRetryableError(err)).toBe(false);
  });

  it('still defers to an explicit marker over structured fields', () => {
    expect(isRetryableError(markRetryable(Object.assign(new Error('x'), { status: 503 }), false))).toBe(false);
  });
});
