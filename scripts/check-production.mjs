// HTTP smoke checks after deployment and on the existing daily schedule.
// Browser behavior is covered separately by check-site against the build.
import path from 'node:path';
import { readSitemap, siteURL } from './sitemap.mjs';
import { fileURLToPath } from 'node:url';

export async function checkProduction(site = 'https://krabka.io') {
  const failures = [];
  const pages = ['/', '/get-started/', '/docs/', '/docs/quickstart/', '/docs/operations/', '/docs/browser-tools/', '/api/', '/versions/', '/verification/', '/benchmarks/', '/search/', '/docs/lab/', '/docs/proof-explorer/'];
  const assets = ['/pagefind/pagefind-ui.js', '/pagefind/pagefind-ui.css', '/playground/krabka_playground.js', '/playground/krabka_playground_bg.wasm', '/playground/broker/krabka-broker.wasm', '/why3-web/manifest.json', '/robots.txt'];
  for (const route of [...pages, ...assets]) {
    try {
      const page = pages.includes(route);
      const response = await fetch(new URL(route, site), { method: page ? 'GET' : 'HEAD', signal: AbortSignal.timeout(20000) });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const expected = new URL(route, site);
      const received = new URL(response.url);
      if (received.origin !== expected.origin || received.pathname.replace(/\/$/, '') !== expected.pathname.replace(/\/$/, '')) throw new Error(`Unexpected redirect to ${received.href}`);
      const contentType = response.headers.get('content-type') ?? '';
      const html = contentType.includes('text/html');
      if (page) {
        const body = await response.text();
        if (!html || !/<h1\b/i.test(body) || !/<title>[^<]*krabka/i.test(body) || /<title>[^<]*page not found/i.test(body)) throw new Error('Expected the authored page, received missing or unexpected HTML');
      } else {
        const expectedType = route.endsWith('.js') ? /(?:text|application)\/(?:javascript|ecmascript)/
          : route.endsWith('.css') ? /text\/css/ : route.endsWith('.wasm') ? /application\/wasm/
          : route.endsWith('.json') ? /application\/json/ : route.endsWith('.xml') ? /(?:text|application)\/xml/ : /text\/plain/;
        if (!expectedType.test(contentType) || response.headers.get('content-length') === '0') throw new Error(`Expected a nonempty asset with its correct MIME type; received ${contentType || 'no type'}`);
      }
      console.log(`✓ ${route}`);
    } catch (error) {
      failures.push(`${route}: ${error.message}`);
      console.error(`✗ ${failures.at(-1)}`);
    }
  }
  try {
    async function xml(url, root) {
      const response = await fetch(url, { signal: AbortSignal.timeout(20000), redirect: 'error' });
      if (!response.ok) throw new Error(`${url}: HTTP ${response.status}`);
      if (!/(?:text|application)\/xml/.test(response.headers.get('content-type') ?? '')) throw new Error(`${url}: expected XML MIME type`);
      return readSitemap(await response.text(), root, url);
    }
    const indexURL = new URL('/sitemap-index.xml', site).href;
    const children = await xml(indexURL, 'sitemapindex');
    const pages = new Set();
    for (const child of children) {
      siteURL(child, site, indexURL);
      for (const location of await xml(child, 'urlset')) {
        const page = siteURL(location, site, child).href;
        if (pages.has(page)) throw new Error(`Duplicate sitemap URL: ${page}`);
        pages.add(page);
      }
    }
    if (!pages.has(new URL('/', site).href)) throw new Error('Sitemap does not include the home page');
    console.log(`✓ Published sitemap: ${pages.size} URLs in ${children.length} file(s)`);
  } catch (error) {
    failures.push(`sitemap: ${error.message}`);
    console.error(`✗ ${failures.at(-1)}`);
  }
  return failures;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const failures = await checkProduction(process.argv[2]);
  process.exitCode = failures.length ? 1 : 0;
}
