---
"@octabits-io/flow": minor
---

Make the pg-boss step worker settle jobs individually, and expose the throughput
knobs that were previously unreachable.

**Per-job settlement.** pg-boss fails an entire batch when the handler throws, so
one bad step dragged its batch neighbours into a retry — wasteful (the engine's
atomic claim made re-execution a no-op) and it obscured which job actually
dead-lettered. The worker now reports each job's own outcome: a step that throws
fails alone under the queue's retry policy, and a payload that fails schema
validation is dead-lettered directly rather than burning attempts it can never pass.

**New `workerOptions`** on `createPgBossStepWorker`, all optional and defaulting to
today's behaviour:

- `burstWhenBatchFull` — keep fetching with no delay while batches come back full.
- `burstWhenReadyExceeds` — burst while the queue's ready count exceeds a threshold.
- `notifyPollingIntervalSeconds` — poll interval used while LISTEN/NOTIFY is active.
- `concurrency` — steps run at once from one fetched batch (default 1, i.e. serial).

Measured on the repo's benchmark (200 workflows × 6 steps, 1 worker, batch 25):
50 → 274 steps/sec with `burstWhenBatchFull`, and 646 with `concurrency: 8` on top.
`concurrency` alone, without burst, changes nothing — a poll-bound worker drains its
batch in milliseconds and then waits, so the wait is the bottleneck, not the work.
Budget connections before raising it: each in-flight step holds one, so
`workers × concurrency` must fit the pool and Postgres `max_connections`.

**Peer range**: the optional `pg-boss` peer moves from `^12.0.0` to `^12.21.0`, the
release that introduced `perJobResults` and the burst options.
