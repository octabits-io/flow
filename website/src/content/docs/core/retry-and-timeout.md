---
title: Retry & timeout
description: Attempt budgets, backoff, wall-clock timeouts — and how a failure is classified as transient.
---

```ts
const flaky = defineStep({
  type: 'call-api', workflowInputSchema, outputSchema,
  retry: { maxAttempts: 4, backoff: 'exponential', initialDelayMs: 500, maxDelayMs: 30_000 },
  timeoutMs: 10_000, // aborts + retries on expiry
  handler: async (ctx) => { /* throw a retryable error to retry within budget */ },
});
```
A failure is retried (with backoff via the dispatcher's `startAfterSeconds`) up to `maxAttempts`;
after that the step fails terminally. → [`examples/03-retry-timeout.ts`](https://github.com/octabits-io/flow/blob/main/examples/03-retry-timeout.ts)

**Which failures count as retryable** is decided in this order:

1. **An explicit marker on the error** — always wins.
   ```ts
   import { retryableError, nonRetryableError, markRetryable } from '@octabits-io/flow';

   throw retryableError('encoder busy');              // retry, whatever the message says
   throw nonRetryableError('timeout must be > 0');    // never retry — a bug, not a blip
   throw markRetryable(await client.readError(), true); // tag an error you didn't construct
   ```
   A marker is found **through `cause`**, so wrapping doesn't lose it:
   `new Error('upstream failed', { cause: retryableError('busy') })` still retries.
2. **The step's own predicate**, for classifying a whole family of errors at once:
   ```ts
   isRetryable: (e) => e instanceof HttpError && e.status >= 500,
   ```
   (`defineMapStep` takes `itemIsRetryable` for its per-item children.)
3. **`isRetryableError`** — the zero-config default. It reads **structured fields first**:
   `code` (`ECONNRESET`, `ECONNREFUSED`, `ETIMEDOUT`, `EAI_AGAIN`, …) and HTTP status from
   `status` / `statusCode` / `response.status` (408, 425, 429 and 5xx except 501/505). Only
   then does it fall back to matching the **message** against a small vocabulary
   (`rate limit`, `timeout`, `fetch failed`, `service unavailable`, …).

That last fallback is a convenience, not a classifier — it can only judge wording. A genuine
bug reading `'timeout must be > 0'` looks transient to it. When the answer matters, mark the
error rather than phrasing it to suit the heuristic.

To stop guessing entirely, give the engine a `defaultRetryable`:

```ts
createWorkflowEngine({ …, config: { defaultRetryable: false } }); // strict: never guess
```

It replaces the classifier's answer **only where the classifier guessed**. Explicitly marked
errors, steps with their own `isRetryable`, and engine-generated failures like a step timeout
are unaffected.
