---
title: Durable sleep
description: Hold a step in the queue so the delay survives a restart.
---

```ts
const cooldown = defineSleepStep({ type: 'cooldown', sleepMs: 60 * 60 * 1000, dependencies: { charge } });
```
A no-op step held in the queue for `sleepMs` once ready — durable across restarts (the delay lives
in the queue, not in memory). → [`examples/04-durable-sleep.ts`](https://github.com/octabits-io/octaflow/blob/main/examples/04-durable-sleep.ts)
