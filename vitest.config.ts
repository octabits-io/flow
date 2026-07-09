import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    coverage: { reporter: ['text'] },
    projects: [
      {
        // Fast, no external services: core + ai layers, plus adapter unit tests
        // (mock-backed, no Docker) named `*.unit.test.ts`.
        test: {
          name: 'unit',
          include: [
            'src/core/**/*.{test,spec}.ts',
            'src/ai/**/*.{test,spec}.ts',
            'src/**/*.unit.test.ts',
          ],
          environment: 'node',
        },
      },
      {
        // Adapters: real Postgres / pg-boss via testcontainers (Docker required).
        test: {
          name: 'integration',
          include: ['src/store-pg/**/*.{test,spec}.ts', 'src/dispatcher-pgboss/**/*.{test,spec}.ts'],
          // `*.unit.test.ts` are mock-backed and run in the fast lane instead.
          exclude: ['src/**/*.unit.test.ts'],
          environment: 'node',
          fileParallelism: false,
          testTimeout: 120_000,
          hookTimeout: 120_000,
        },
      },
    ],
  },
});
