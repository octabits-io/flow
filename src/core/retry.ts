// ============================================================================
// Retryability
// ============================================================================
//
// Whether a failed step is retried is decided in this order:
//
//   1. An **explicit marker** on the thrown error ({@link markRetryable},
//      {@link retryableError}, {@link nonRetryableError}) — always wins.
//   2. The step's own `isRetryable` predicate, if `defineStep` was given one.
//   3. {@link isRetryableError} — a message heuristic, the zero-config default.
//
// The heuristic is a convenience for the common transient failures, not a
// classifier. It reads the message, so `'connection refused'` is treated as
// permanent while any error mentioning `'timeout'` is treated as transient.
// When it matters, say so explicitly rather than phrasing the message to suit it.

/**
 * Realm-safe marker for an explicit retryability decision. `Symbol.for` so a
 * duplicated copy of this module still reads markers set by the other copy.
 */
const RETRYABLE = Symbol.for('@octabits-io/flow.retryable');

/**
 * Tag an error with an explicit retryability decision, overriding both the
 * step predicate and the message heuristic. Returns the same error, so it can
 * wrap a throw:
 *
 * ```ts
 * throw markRetryable(await client.readError(), true);
 * ```
 */
export function markRetryable<E>(error: E, retryable = true): E {
  if (typeof error === 'object' && error !== null) {
    Object.defineProperty(error, RETRYABLE, { value: retryable, enumerable: false, configurable: true });
  }
  return error;
}

/** An error the engine will always retry (within the step's attempt budget). */
export function retryableError(message: string, options?: ErrorOptions): Error {
  return markRetryable(new Error(message, options), true);
}

/** An error the engine will never retry, whatever its message says. */
export function nonRetryableError(message: string, options?: ErrorOptions): Error {
  return markRetryable(new Error(message, options), false);
}

/**
 * The explicit decision carried by an error, or `undefined` if it carries none.
 * Callers deciding retryability themselves should consult this first.
 */
export function explicitRetryability(error: unknown): boolean | undefined {
  if (typeof error !== 'object' || error === null) return undefined;
  const value = (error as Record<symbol, unknown>)[RETRYABLE];
  return typeof value === 'boolean' ? value : undefined;
}

/**
 * Heuristic for whether a thrown error should be retried (rate limits, transient
 * network failures, service-unavailable). Schema and programming errors are
 * intentionally NOT retryable.
 *
 * This is the **fallback** — an explicit marker on the error wins over it. It
 * classifies by message substring, so it will misjudge anything whose wording it
 * doesn't recognise; reach for {@link retryableError} / {@link nonRetryableError}
 * (or `defineStep`'s `isRetryable`) when the answer matters.
 */
export function isRetryableError(error: unknown): boolean {
  const explicit = explicitRetryability(error);
  if (explicit !== undefined) return explicit;

  if (!(error instanceof Error)) return false;

  const message = error.message.toLowerCase();

  // Rate limiting
  if (message.includes('rate limit') || message.includes('429') || message.includes('too many requests')) {
    return true;
  }

  // Network / timeout
  if (message.includes('timeout') || message.includes('econnreset') || message.includes('fetch failed')) {
    return true;
  }

  // Service unavailable
  if (message.includes('503') || message.includes('service unavailable')) {
    return true;
  }

  return false;
}
