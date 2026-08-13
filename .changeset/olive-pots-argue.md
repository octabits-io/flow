---
"octaflow": minor
---

**Renamed: `@octabits-io/flow` is now `octaflow`.**

"Flow" already means Facebook's type checker to most JavaScript developers, and
the scope added friction without adding meaning — nobody searching for a workflow
engine types `@octabits-io/`. `octaflow` is unscoped, unambiguous in search, and
still on-brand.

To migrate, change the dependency and every import:

```diff
-import { buildWorkflow } from '@octabits-io/flow';
-import { createPgWorkflowStore } from '@octabits-io/flow/store-pg';
+import { buildWorkflow } from 'octaflow';
+import { createPgWorkflowStore } from 'octaflow/store-pg';
```

```bash
npm remove @octabits-io/flow && npm install octaflow
```

Nothing else changed: the subpath exports (`.`, `./ai`, `./store-pg`,
`./dispatcher-pgboss`), every export name, and all behaviour are identical. A
find-and-replace of the package string is the whole migration.

The repository moved to `octabits-io/octaflow` (GitHub redirects the old URLs)
and the docs are now at https://octabits-io.github.io/octaflow/.
`@octabits-io/flow` is deprecated on npm at its last version, 0.13.0.
