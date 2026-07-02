import { defineConfig } from "tsup";

export default defineConfig({
  entry: {
    index: "src/index.ts",
    "ai/index": "src/ai/index.ts",
    "store-pg/index": "src/store-pg/index.ts",
    "dispatcher-pgboss/index": "src/dispatcher-pgboss/index.ts",
  },
  format: "esm",
  // Share the core layer between entries instead of duplicating it per bundle.
  splitting: true,
  dts: true,
  clean: true,
});
