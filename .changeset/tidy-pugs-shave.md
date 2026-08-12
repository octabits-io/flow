---
"@octabits-io/flow": minor
---

Add an explicit escape hatch for retryability.

Whether a failed step was retried was decided solely by `isRetryableError`, which
matches the error *message* against a small vocabulary (`rate limit`, `429`,
`timeout`, `ECONNRESET`, `503`, …). That silently misjudges both directions:
`'connection refused'` is transient but failed terminally, while a permanent bug
whose message happened to contain `'timeout'` was retried until the budget ran out.

Retryability is now decided in this order:

1. **An explicit marker on the error** — `retryableError(msg)`, `nonRetryableError(msg)`,
   or `markRetryable(err, bool)` to tag an error you didn't construct. Also
   `explicitRetryability(err)` to read the decision back.
2. **The step's own predicate** — `defineStep({ isRetryable: (e) => … })`, and
   `defineMapStep({ itemIsRetryable })` for per-item children.
3. **`isRetryableError`** — unchanged, still the zero-config default.

Nothing breaks: steps that mark nothing and define no predicate behave exactly as
before. The marker is a non-enumerable `Symbol.for` property, so it does not leak
into `JSON.stringify` or spread, and survives a duplicated copy of the module.
