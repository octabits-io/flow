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
   `explicitRetryability(err)` to read the decision back. Markers are found through
   the `cause` chain, so wrapping an error doesn't lose its decision.
2. **The step's own predicate** — `defineStep({ isRetryable: (e) => … })`, and
   `defineMapStep({ itemIsRetryable })` for per-item children.
3. **`isRetryableError`** — now reads structured fields before the message: `code`
   (`ECONNRESET`, `ECONNREFUSED`, `ETIMEDOUT`, `EAI_AGAIN`, undici timeouts, …) and
   HTTP status from `status` / `statusCode` / `httpStatusCode` / `response.status`
   (408, 425, 429, 5xx except 501/505). The message vocabulary is unchanged and is
   now the last resort.

Also adds `defaultRetryable` to the engine config, for hosts that would rather not
guess at all:

```ts
createWorkflowEngine({ …, config: { defaultRetryable: false } });
```

It applies **only where the classifier guessed** — explicit markers, per-step
predicates and engine-generated failures (a step timeout) are unaffected. `StepError`
gains `retryableFrom: 'explicit' | 'predicate' | 'heuristic'` to make that distinction
available to custom dispatchers.

The marker is a non-enumerable `Symbol.for` property, so it does not leak into
`JSON.stringify` or spread, and survives a duplicated copy of the module. Marking
never throws — errors that are frozen, sealed or non-extensible fall back to a
`WeakMap`, since marking is usually evaluated inside a `throw` and a `TypeError`
there would replace the failure being reported.

**Behaviour change**: errors carrying a transient `code` or a 5xx/429 status now
retry where previously they failed terminally (their message was never consulted).
Steps that mark nothing, define no predicate, and throw plain message-only errors
behave exactly as before.
