import { defineConfig } from "tsdown";

export default defineConfig({
  entry: {
    index: "src/index.ts",
    "ai/index": "src/ai/index.ts",
    "store-pg/index": "src/store-pg/index.ts",
    "dispatcher-pgboss/index": "src/dispatcher-pgboss/index.ts",
  },
  format: "esm",
  // Rolldown code-splits ESM entries automatically, so the core layer is
  // shared between entries without an explicit `splitting` flag.
  // Emit .js/.d.ts (matching the exports map) instead of tsdown's default .mjs/.d.mts.
  fixedExtension: false,
  dts: true,
  clean: true,
});
