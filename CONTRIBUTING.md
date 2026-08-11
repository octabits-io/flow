# Contributing to `@octabits-io/flow`

Thanks for taking a look. Bug reports with a runnable reproduction are the most
useful thing you can send; PRs are welcome too.

## Getting set up

```bash
pnpm install
pnpm build
```

**Docker must be running** for the integration suite — the Postgres and pg-boss
tests spin up real containers via Testcontainers.

## The checks

```bash
pnpm lint         # dependency-boundary check (scripts/check-boundaries.mjs)
pnpm typecheck    # tsc --noEmit
pnpm test:unit    # core + ai, no services, ~1s
pnpm test         # everything, including containers
```

CI runs all four. `pnpm test:unit` is the fast loop while you work; run the full
suite before opening a PR.

To run one file:

```bash
npx vitest run src/core/engine.test.ts
```

## Architecture you need to know

One package, **four layers**, with the boundaries enforced by `pnpm lint`:

| Entry | Layer | May import | Never imports |
|---|---|---|---|
| `.` | `core` — the engine | nothing internal | any heavy dep |
| `./ai` | instrumented models, quota, usage | `core` | `pg`, `pg-boss` |
| `./store-pg` | Postgres `WorkflowStore`, gate, event sink | `core` | `ai`/`@ai-sdk`, `pg-boss` |
| `./dispatcher-pgboss` | pg-boss dispatcher, workers, cron | `core` | `ai`/`@ai-sdk`, `pg` |

`ai` and the two adapters may never depend on each other. The default `.` entry
re-exports **only `core`**, so `import '@octabits-io/flow'` never pulls in the AI
SDK, `pg`, or `pg-boss` — all of which are optional peers. Keep it that way: new
capabilities that need a heavy dependency belong in a layer, behind an optional
peer, not in core.

## Conventions

- **Factory functions over classes** — `createXxxService(deps)` returning a plain object.
- **Result pattern** — expected errors are *values*: return `Result<T, E>` discriminated
  on `ok`. Only unexpected/programming errors throw.
- **Zod** for runtime validation of step I/O.
- **ESM-only**, `.ts`-extension imports, `verbatimModuleSyntax` (so `import type { … }`
  for type-only imports).
- **`noUncheckedIndexedAccess`** is on — indexed access yields `T | undefined`.
- **Tenancy-agnostic** — no tenant vocabulary in the API surface. Work is scoped by the
  generic `partitionKey`; consumers bind their own names.
- The package is deliberately **standalone**: structural `Result`/`Logger` types, no
  `@octabits-io/foundation` dependency.

## Implementing a custom store or dispatcher

Both are plain interfaces, but they carry correctness requirements that the type
signature alone doesn't express — they're documented on the interfaces in
`src/core/store.ts` and `src/core/dispatcher.ts`. The one most easily gotten wrong:

> `markStepRunning` is the step **claim**. It must flip `pending` → `running`
> atomically and report whether the caller won. A read-then-write lets two workers
> handed the same job by an at-least-once dispatcher both run the handler.

If you add a store, port the behavioural tests in `src/store-pg/store.test.ts`.

## Commits

**Conventional Commits**, enforced by a `commit-msg` hook (commitlint):

```
fix(core): claim steps atomically to prevent double execution
feat(store-pg): expose a schema-qualified DDL helper
docs: clarify the declarative vs imperative tradeoff
```

Please **don't** add `Co-Authored-By` trailers.

## Changesets

Any user-visible change needs one:

```bash
pnpm changeset
```

Pre-1.0, consumers pin on caret ranges (`^0.12.0`), so the minor is the effective
breaking-change lever — use `minor` for breaking changes and `patch` for fixes.
Describe the migration in the changeset body when you change an interface.

Maintainers publish; contributors never need to run `changeset:publish`.

## PRs

Small and focused beats large and comprehensive. Include a test that fails without
your change — for a bug fix, the most convincing thing you can do is confirm the
test fails when the fix is reverted.
