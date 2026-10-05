import { defineConfig } from 'astro/config';
import tailwindcss from '@tailwindcss/vite';
import sitemap from '@astrojs/sitemap';
import { satteri } from '@astrojs/markdown-satteri';
import wrapTables from './src/utils/satteri-wrap-tables.mjs';
import krabkaTheme from './src/utils/krabka-shiki-theme.mjs';

export default defineConfig({
  site: 'https://krabka.io',
  // Astro's HTML compression drops the whitespace between an inline element
  // and the text or element beside it when a line break separates them, which
  // glued words together (`<strong>WebAssembly</strong>` + newline + `and`
  // rendered as "WebAssemblyand").
  compressHTML: false,
  // Fetch a page when its link is hovered or focused, so moving between docs is quick.
  prefetch: true,
  // Legacy routes; observability and gres moved to their own repositories.
  redirects: {
    '/docs/benchmarks': '/benchmarks',
    // The quickstart files are served from /quickstart/ (docker-compose.yml and
    // friends); the guide is under /docs, which is where trimming a file URL lands.
    '/quickstart': '/docs/quickstart',
    '/features/observability': 'https://github.com/krabka-io/krabka-o11y',
    '/docs/observability': 'https://github.com/krabka-io/krabka-o11y/blob/main/docs/observing_krabka_clusters.md',
    // The API reference used to be built into this site; each project now
    // publishes its own, so the old `latest` routes forward to those.
    '/api/broker/latest': 'https://krabka.io/krabka-broker/',
    '/api/streams-java/latest': 'https://krabka.io/krabka-streams-java/api/',
    '/api/streams-go/latest': 'https://krabka.io/krabka-streams-go/',
    '/whitepapers/gres-scaling': 'https://github.com/krabka-io/gres/blob/main/docs/gres-scaling-whitepaper.md',
  },
  integrations: [sitemap()],
  markdown: {
    // Astro's default Sätteri pipeline, with one plugin and the site's code
    // theme: synced guides carry wide tables, and each gets its own scroll
    // container.
    processor: satteri({ hastPlugins: [wrapTables] }),
    shikiConfig: { theme: krabkaTheme },
  },
  vite: {
    plugins: [tailwindcss()],
  },
});
