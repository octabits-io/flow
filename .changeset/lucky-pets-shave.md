---
'octaflow': minor
---

Conditional branching, deadlines, and recovery for failed runs.

Three gaps where the engine handled the happy path and the first branch of the unhappy
path, then stopped.

**Conditional branching.** A `when` guard on a step decides whether it runs at all; a
step whose guard says no is skipped, and so is everything reachable only through it.
`join: 'any'` lets the branches converge again — it runs once every dependency has
settled and at least one completed, so a skipped arm no longer skips the join with it.
Under `'any'` the `deps` type becomes possibly-absent per branch. A guard that throws is
classified like a failing handler, so a transient error is retried rather than quietly
pruning the branch. Available on `defineStep`, `defineWaitStep`, `defineMapStep` and
`defineSubWorkflowStep`.

**Deadlines.** `defineWaitStep` now takes `timeoutMs` plus `onTimeout`: `'fail'` (the
default) ends the run, `{ output }` completes the step with a stand-in answer and lets
the DAG carry on — which, paired with a `when` guard, is "approve within 48 hours,
otherwise escalate". `StartOptions.timeoutMs` puts a wall-clock budget on a whole run,
enforced when a step is picked up and by `recoverStuckWorkflows`, so it also catches a
run suspended on an event that never came.

**Recovering a failed run.** `engine.retryWorkflow(id)` resumes a `failed` workflow from
where it stopped: steps that failed, were skipped in the fallout, or had their work
compensated away go back to `pending` with a fresh attempt budget, while completed steps
keep their output and do not run again. Emits `workflow.retried`.

### Breaking changes

**Workers must call `engine.handleStepJob(payload)` instead of `executeStep`.** The queue
now carries wait-deadline jobs alongside step runs, and only the payload's `kind` tells
them apart. A worker still calling `executeStep` keeps working but will never time out a
suspended step.

```diff
  await worker.start(async (payload) => {
-   await engine.executeStep(payload.workflowId, payload.stepId);
+   await engine.handleStepJob(payload);
  });
```

**A crashed step is now re-queued rather than failed.** `recoverStuckWorkflows` puts a
step whose worker died back on the queue while its attempt budget has room, and only
fails it once that budget is spent — a dead pod costs an attempt, not the whole run.
Handlers must therefore tolerate re-execution after a partial run, which is the contract
retries already imposed. Set `config.onStuckStep: 'fail'` for the previous behaviour. Its
return type gained `retriedSteps` and `expiredWorkflows`.

**`WorkflowStore` gained three methods and changed one.** Custom stores must implement
`resetStep`, `reopenWorkflow` and `deleteChildSteps`, and `markStepWaiting` now takes a
`waitingAt` timestamp to stamp on `started_at` (a wait deadline is measured from it). The
bundled `store-pg` and in-memory stores are updated; see CONTRIBUTING for the contract.

**`WorkflowRecord` gained `deadlineAt`**, backed by a new `deadline_at` column.
`flowStoreDdl()` emits an `ALTER TABLE … ADD COLUMN IF NOT EXISTS`, so re-applying the
DDL migrates an existing database. If you host the tables in your own migrations, add:

```sql
ALTER TABLE flow_workflow ADD COLUMN IF NOT EXISTS deadline_at timestamptz;
CREATE INDEX IF NOT EXISTS flow_workflow_deadline_idx
  ON flow_workflow (deadline_at) WHERE deadline_at IS NOT NULL;
```

**The pg-boss wire payload gained `kind`**, defaulted to `'execute'` so jobs enqueued by
an older version still parse and still mean "run the step".

### Internals

Completion, failure, conditional skip and operator retry now funnel through one `settle()`
pass — prune what can no longer run, dispatch what became runnable, finish the workflow
once nothing is left moving. The readiness rules it uses are a pure, exported function
(`computeReadiness`), so join rules and skip cascades are decided in one place instead of
two that could disagree.
