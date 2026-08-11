---
"@octabits-io/flow": minor
---

Fix a double-execution window in the step claim.

`executeStep` read a step, checked it was `pending`, and then wrote `running` as a
separate statement. Two workers handed the same job by an at-least-once dispatcher
could both pass the read and both run the handler — with the step's `attempts`
double-incremented.

The claim is now atomic: `markStepRunning` flips `pending` → `running` only if the
step is still `pending`, and reports whether the caller won. The Postgres store does
this with `UPDATE … WHERE id = $1 AND status = 'pending'` and checks `rowCount`; the
engine bails out (releasing its gate slot) when it loses the race.

**Breaking for custom `WorkflowStore` implementations**: `markStepRunning` now returns
`Promise<boolean>` instead of `Promise<void>`, and MUST perform the status check and
the write as one atomic operation. Implementations that unconditionally write the row
will reintroduce the double-execution window. The bundled Postgres and in-memory
stores are already updated; consumers using them need no changes.
