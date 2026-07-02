/**
 * Heuristic for whether a thrown error should be retried by the dispatcher
 * (rate limits, transient network failures, service-unavailable). Schema and
 * programming errors are intentionally NOT retryable.
 */
export function isRetryableError(error: unknown): boolean {
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
