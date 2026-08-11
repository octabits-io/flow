## What and why

<!-- What changes, and the problem it solves. Link an issue if there is one. -->

## Checks

- [ ] `pnpm lint && pnpm typecheck && pnpm test` pass locally (Docker running for the integration suite)
- [ ] A test covers the change — and for a bug fix, it fails when the fix is reverted
- [ ] A changeset is included (`pnpm changeset`) if the change is user-visible
- [ ] Layer boundaries respected — no heavy dependency reached `core`

## Breaking changes

<!-- Interface changes, especially to WorkflowStore / Dispatcher / StepGate.
     Describe the migration; leave "none" if there are none. -->

none
