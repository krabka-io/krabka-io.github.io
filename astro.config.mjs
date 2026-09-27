import { defineConfig } from 'astro/config';
import tailwindcss from '@tailwindcss/vite';
import sitemap from '@astrojs/sitemap';

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
  vite: {
    plugins: [tailwindcss()],
  },
});
