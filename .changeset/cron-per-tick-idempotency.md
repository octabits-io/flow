---
'octaflow': minor
---

Resolve a scheduled start's idempotency key **per delivery**, so a cron schedule no longer
collapses every tick into one workflow.

A pg-boss schedule stores its payload once and redelivers that same payload on every tick. The
scheduler baked `idempotencyKey` into that payload at schedule time, so every tick carried an
identical key — and because start idempotency keys never expire, a nightly cron created with a
key started **exactly one workflow, ever**. Every subsequent tick found the existing workflow
and returned it. The failure is silent: the schedule looks healthy and the job succeeds.

`ScheduleStartInput` and `WireStartPayload` now take `idempotencyKeyPrefix`, and the start
worker resolves it against the pg-boss job id — unique per cron tick, stable across that job's
retries:

```ts
await scheduler.schedule({
  key: 'nightly', cron: '0 3 * * *', workflowType: 'enrichment',
  idempotencyKeyPrefix: 'nightly',        // → 'nightly:<jobId>' per tick
});

await starter.start(async (job) => {
  await engine.startWorkflow(wf.definition, job.input, { idempotencyKey: job.idempotencyKey });
});
```

A redelivered tick reuses its key (deduped); the next tick gets a new one (starts fresh).

`idempotencyKey` is still accepted and still used verbatim, which is the right behaviour for an
ad-hoc start and for a deliberate "run this once and never again" schedule. An explicit key wins
over a prefix.

**Breaking for `StartJobProcessor` implementations only in shape, not usage**: the processor now
receives a `StartJobContext` — the wire payload plus `jobId` and the resolved `idempotencyKey` —
instead of a bare `WireStartPayload`. Code that already forwarded `payload.idempotencyKey` keeps
compiling and starts behaving correctly. Also exports `resolveStartIdempotencyKey` so a host can
reproduce the derivation.

Unrelated fix in the same area: `examples/12` and the Postgres docs imported pg-boss as
`import PgBoss from 'pg-boss'`, but pg-boss has no default export — `new PgBoss(...)` threw at
runtime. Both now use the named import.
