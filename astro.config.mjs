import { defineConfig } from 'astro/config';
import tailwindcss from '@tailwindcss/vite';
import sitemap from '@astrojs/sitemap';
import { satteri } from '@astrojs/markdown-satteri';
import wrapTables from './src/utils/satteri-wrap-tables.mjs';
import baseLinks from './src/utils/satteri-base-links.mjs';
import krabkaTheme from './src/utils/krabka-shiki-theme.mjs';
import headingIds from './src/utils/heading-ids.mjs';

// The path the site is served below. getPath() reads it as BASE_URL, and
// baseLinks applies it to root-relative links in Markdown pages.
const base = '/';

export default defineConfig({
  site: 'https://krabka.io',
  base,
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
    '/features/observability': 'https://krabka.io/krabka-o11y/',
    '/docs/observability': 'https://krabka.io/krabka-o11y/docs/observing_krabka_clusters/',
    // The API reference used to be built into this site; each project now
    // publishes its own, so the old `latest` routes forward to those.
    '/api/broker/latest': 'https://krabka.io/krabka-broker/',
    '/api/streams-java/latest': 'https://krabka.io/krabka-streams-java/api/',
    '/api/streams-go/latest': 'https://krabka.io/krabka-streams-go/',
    '/whitepapers/gres-scaling': 'https://github.com/krabka-io/gres/blob/main/docs/gres-scaling-whitepaper.md',
  },
  integrations: [
    // Exclude noindex pages and the authored /docs/api redirect to /api.
    // check-sitemap verifies this list against the final HTML on every build.
    sitemap({ filter: (page) => !/\/(404|search|docs\/api)\/?$/.test(new URL(page).pathname) }),
    headingIds(),
  ],
  markdown: {
    // Astro's default Sätteri pipeline, with the site's code theme and two
    // plugins: synced guides carry wide tables, and each gets its own scroll
    // container; root-relative links get the site base, as getPath() gives it.
    processor: satteri({ hastPlugins: [wrapTables, baseLinks(base)] }),
    shikiConfig: { theme: krabkaTheme },
  },
  vite: {
    plugins: [tailwindcss()],
  },
});
