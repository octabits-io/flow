// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

// Served from GitHub Pages at https://octabits-io.github.io/flow/, so every
// internal link has to carry the /flow base. Override with SITE / BASE when
// hosting elsewhere (a custom domain wants base: '/').
const site = process.env.SITE ?? 'https://octabits-io.github.io';
const base = process.env.BASE ?? '/flow';

export default defineConfig({
  site,
  base,
  trailingSlash: 'always',
  integrations: [
    starlight({
      title: 'Flow',
      description:
        'Durable workflows for TypeScript that run on the Postgres you already have. A declarative, inspectable DAG — not imperative durable functions.',
      logo: { src: './src/assets/logo.svg', replacesTitle: false },
      social: [
        { icon: 'github', label: 'GitHub', href: 'https://github.com/octabits-io/flow' },
        { icon: 'npm', label: 'npm', href: 'https://www.npmjs.com/package/@octabits-io/flow' },
      ],
      editLink: {
        baseUrl: 'https://github.com/octabits-io/flow/edit/main/website/',
      },
      // Share the repo's social card, so a link to the docs previews the same
      // artwork as a link to GitHub.
      head: [
        { tag: 'meta', attrs: { property: 'og:image', content: `${site}${base}/flow-social-preview.png` } },
        { tag: 'meta', attrs: { name: 'twitter:image', content: `${site}${base}/flow-social-preview.png` } },
        { tag: 'meta', attrs: { name: 'twitter:card', content: 'summary_large_image' } },
      ],
      lastUpdated: true,
      customCss: ['./src/styles/custom.css'],
      components: {
        // Suppresses the automatic <h1> on the splash page; see the component.
        PageTitle: './src/components/PageTitle.astro',
      },
      sidebar: [
        {
          label: 'Start here',
          items: [
            { label: 'What is Flow?', slug: 'start/what-is-flow' },
            { label: 'How it compares', slug: 'start/how-it-compares' },
            { label: 'Installation', slug: 'start/installation' },
            { label: 'Quick start', slug: 'start/quick-start' },
          ],
        },
        {
          label: 'Core',
          items: [
            { label: 'Concepts', slug: 'core/concepts' },
            { label: 'Defining steps', slug: 'core/defining-steps' },
            { label: 'Retry & timeout', slug: 'core/retry-and-timeout' },
            { label: 'Durable sleep', slug: 'core/durable-sleep' },
            { label: 'Concurrency & rate limits', slug: 'core/concurrency-and-rate-limits' },
            { label: 'Idempotency', slug: 'core/idempotency' },
            { label: 'Fan-out & map', slug: 'core/fan-out-and-map' },
            { label: 'Signals', slug: 'core/signals' },
            { label: 'Sub-workflows', slug: 'core/sub-workflows' },
            { label: 'Saga compensation', slug: 'core/saga-compensation' },
          ],
        },
        {
          label: 'Running it',
          items: [
            { label: 'Postgres & pg-boss', slug: 'running/postgres-and-pg-boss' },
            { label: 'Observability', slug: 'running/observability' },
            { label: 'Performance', slug: 'running/performance' },
            { label: 'Testing', slug: 'running/testing' },
          ],
        },
        {
          label: 'Extending',
          items: [
            { label: 'Stores, dispatchers, gates', slug: 'extending/interfaces' },
            { label: 'Serving over HTTP', slug: 'extending/http' },
            { label: 'The AI add-on', slug: 'extending/ai' },
          ],
        },
        {
          label: 'Reference',
          items: [
            { label: 'API', slug: 'reference/api' },
            { label: 'Examples', slug: 'reference/examples' },
          ],
        },
      ],
    }),
  ],
});
