---
title: Performance
description: Measured throughput and latency against real Postgres, with the caveats stated.
---

Reproduce with `npx tsx scripts/bench.ts` (Docker required — it starts Postgres 17 via
Testcontainers). Workload: 200 workflows × 6 steps in a `root → 4 parallel → join` diamond.
**Handlers are no-ops**, so this measures what the *engine* costs per step — claiming it,
reading dependency outputs, persisting the transition, recomputing readiness — not your work.

**Engine + Postgres store** (in-process dispatcher), per-step latency:

| concurrency | steps/sec | p50 | p95 | p99 |
|---|---|---|---|---|
| 1 | 1,031 | 1.0 ms | 2.1 ms | 2.9 ms |
| 4 | 1,932 | 2.1 ms | 3.9 ms | 4.8 ms |
| 16 | 2,108 | 7.1 ms | 12.7 ms | 15.9 ms |
| 64 | 2,270 | 26.8 ms | 46.8 ms | 64.0 ms |

**End-to-end through pg-boss workers** — the full production path, batch 25:

| workers | `burstWhenBatchFull` | `concurrency` | steps/sec |
|---|---|---|---|
| 1 | off | 1 | 50 |
| 1 | **on** | 1 | 274 |
| 1 | **on** | 8 | 646 |
| 4 | **on** | 8 | 902 |

That first row is not a ceiling, it's a *polling artifact* — and the fix is configuration, not
architecture. A worker drains a batch in milliseconds, then waits out the 0.5 s interval, so
**`burstWhenBatchFull` is the setting that matters**: it keeps fetching while batches come back
full. `concurrency` (steps run at once from one batch) then compounds on top — but on its own,
without burst, it changes nothing at all, because the wait, not the work, is the bottleneck.

Budget connections before raising `concurrency`: each in-flight step holds one, so
`workers × concurrency` must fit your pool and Postgres `max_connections`.

**How to read this.** Measured on an M-series Mac with Postgres in Docker, which has markedly
slower disk I/O than a Linux host — expect better on a real server. These are an order of
magnitude and a scaling shape, not a score. A Redis-backed job queue will beat these numbers
outright, because it isn't writing a durable transition per step to a relational database;
that write is the feature. And in any real workflow, handler time dwarfs the 1–3 ms of engine
overhead, so the practical question is usually whether ~1 ms per transition is acceptable
next to what your steps actually do.
