import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    coverage: { reporter: ['text'] },
    projects: [
      {
        // Fast, no external services: core + ai layers.
        test: {
          name: 'unit',
          include: ['src/core/**/*.{test,spec}.ts', 'src/ai/**/*.{test,spec}.ts'],
          environment: 'node',
        },
      },
      {
        // Adapters: real Postgres / pg-boss via testcontainers (Docker required).
        test: {
          name: 'integration',
          include: ['src/store-pg/**/*.{test,spec}.ts', 'src/dispatcher-pgboss/**/*.{test,spec}.ts'],
          environment: 'node',
          fileParallelism: false,
          testTimeout: 120_000,
          hookTimeout: 120_000,
        },
      },
    ],
  },
});
