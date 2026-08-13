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
const RETRYABLE = Symbol.for('octaflow.retryable');

/**
 * Fallback store for errors that cannot carry a property — frozen, sealed, or
 * non-extensible objects, which some SDKs hand out. Keyed weakly, so marking an
 * error never keeps it alive.
 */
const sideTable = new WeakMap<object, boolean>();

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
    try {
      Object.defineProperty(error, RETRYABLE, { value: retryable, enumerable: false, configurable: true });
    } catch {
      // Frozen / sealed / non-extensible: keep the decision beside the error instead.
      // Marking must never throw — it is typically evaluated inside a `throw`, and a
      // TypeError here would replace the failure the caller was trying to report.
      sideTable.set(error, retryable);
    }
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

/** How deep to follow `cause` before giving up. Guards against pathological chains. */
const MAX_CAUSE_DEPTH = 8;

/**
 * The explicit decision carried by an error, or `undefined` if it carries none.
 * Callers deciding retryability themselves should consult this first.
 *
 * Follows the `cause` chain, so a marked error keeps its decision when a caller
 * wraps it — `new Error('upstream failed', { cause: retryableError('busy') })`
 * still reports `true`. The outermost decision wins; the walk is depth-capped and
 * cycle-safe.
 */
export function explicitRetryability(error: unknown): boolean | undefined {
  const seen = new Set<unknown>();
  let current = error;
  for (let depth = 0; depth < MAX_CAUSE_DEPTH; depth++) {
    if (typeof current !== 'object' || current === null || seen.has(current)) return undefined;
    seen.add(current);

    const value = (current as Record<symbol, unknown>)[RETRYABLE];
    if (typeof value === 'boolean') return value;
    const stored = sideTable.get(current);
    if (stored !== undefined) return stored;

    current = (current as { cause?: unknown }).cause;
  }
  return undefined;
}

/**
 * Transient `code` values, as set by Node's networking stack and by most clients
 * built on it. Deliberately excludes permanent ones like `ENOTFOUND` (bad host)
 * and `ERR_INVALID_ARG_TYPE` (a bug).
 */
const RETRYABLE_CODES = new Set([
  'ECONNRESET',
  'ECONNREFUSED',
  'ECONNABORTED',
  'ETIMEDOUT',
  'EPIPE',
  'EHOSTUNREACH',
  'ENETUNREACH',
  'ENETRESET',
  'EAI_AGAIN',
  'EBUSY',
  'UND_ERR_CONNECT_TIMEOUT',
  'UND_ERR_HEADERS_TIMEOUT',
  'UND_ERR_BODY_TIMEOUT',
]);

/** Transient HTTP statuses. 5xx is transient except the ones that mean "never".  */
function isRetryableStatus(status: number): boolean {
  if (status === 408 || status === 425 || status === 429) return true;
  // 501 Not Implemented and 505 Version Not Supported will not change on retry.
  return status >= 500 && status < 600 && status !== 501 && status !== 505;
}

/** Read a numeric HTTP status off the shapes clients actually use. */
function statusOf(error: object): number | undefined {
  for (const key of ['status', 'statusCode', 'httpStatusCode'] as const) {
    const v = (error as Record<string, unknown>)[key];
    if (typeof v === 'number' && Number.isFinite(v)) return v;
  }
  const response = (error as { response?: { status?: unknown } }).response;
  if (response && typeof response.status === 'number') return response.status;
  return undefined;
}

/**
 * Default classifier for a thrown error, used when nothing more specific applies.
 * Checked in order:
 *
 *   1. An explicit marker ({@link markRetryable}), including through `cause`.
 *   2. **Structured fields** — `code` (`ECONNRESET`, `ETIMEDOUT`, …) and HTTP
 *      status (`status` / `statusCode` / `response.status`): 408, 425, 429, 5xx.
 *   3. **The message**, matched against a small vocabulary.
 *
 * Steps 2–3 are a convenience, not a classifier. The message check especially will
 * misjudge anything whose wording it doesn't recognise — a permanent bug reading
 * `'timeout must be > 0'` looks transient. Reach for {@link retryableError} /
 * {@link nonRetryableError}, `defineStep`'s `isRetryable`, or the engine's
 * `defaultRetryable` when the answer matters.
 */
export function isRetryableError(error: unknown): boolean {
  const explicit = explicitRetryability(error);
  if (explicit !== undefined) return explicit;

  // Structured signals first: `code` and HTTP status are contractual, whereas a
  // message is prose a library may reword in a patch release.
  if (typeof error === 'object' && error !== null) {
    const code = (error as { code?: unknown }).code;
    if (typeof code === 'string' && RETRYABLE_CODES.has(code.toUpperCase())) return true;
    const status = statusOf(error);
    if (status !== undefined) return isRetryableStatus(status);
  }

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
