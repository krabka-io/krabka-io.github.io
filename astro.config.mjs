import { defineConfig } from 'astro/config';
import tailwindcss from '@tailwindcss/vite';
import sitemap from '@astrojs/sitemap';
import { satteri } from '@astrojs/markdown-satteri';
import wrapTables from './src/utils/satteri-wrap-tables.mjs';

export default defineConfig({
  site: 'https://krabka.io',
  // Pages that left this site. Benchmarks were retired in favor of the
  // verification story; observability and gres moved to their own repositories
  // as part of the broader Krabka ecosystem.
  redirects: {
    '/benchmarks': '/verification',
    '/docs/benchmarks': '/verification',
    '/features/observability': 'https://github.com/krabka-io/krabka-o11y',
    '/docs/observability': 'https://github.com/krabka-io/krabka-o11y/blob/main/docs/observing_krabka_clusters.md',
    '/whitepapers/gres-scaling': 'https://github.com/krabka-io/gres/blob/main/docs/gres-scaling-whitepaper.md',
  },
  integrations: [sitemap()],
  markdown: {
    // Astro's default Sätteri pipeline, with one plugin: synced guides carry
    // wide tables, and each gets its own scroll container.
    processor: satteri({ hastPlugins: [wrapTables] }),
  },
  vite: {
    plugins: [tailwindcss()],
  },
});
