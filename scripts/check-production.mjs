// HTTP smoke checks after deployment and on the existing daily schedule.
// Browser behavior is covered separately by check-site against the build.
import fs from 'node:fs';
import path from 'node:path';
import { readSitemap, scopedURL, validateXSD, advertisedSitemaps, assertCrawlable, assertSection } from './sitemap.mjs';
import { fileURLToPath } from 'node:url';

export async function checkPublishedSitemap(site, expected, { sections = true } = {}) {
  async function body(url, type) {
    const response = await fetch(url, { signal: AbortSignal.timeout(20000), redirect: 'error' });
    if (!response.ok) throw new Error(`${url}: HTTP ${response.status}`);
    if (!type.test(response.headers.get('content-type') ?? '')) throw new Error(`${url}: unexpected MIME type`);
    if (/\b(noindex|none)\b/i.test(response.headers.get('x-robots-tag') ?? '')) throw new Error(`${url}: crawl-blocking X-Robots-Tag`);
    const data = Buffer.from(await response.arrayBuffer());
    return data;
  }
  const robots = (await body(new URL('/robots.txt', site), /text\/plain/)).toString('utf8');
  const roots = advertisedSitemaps(robots, site);
  const records = [];
  async function xml(url, root) {
    const data = await body(url, /(?:text|application)\/xml/);
    records.push({ file: url, root, base64: data.toString('base64') });
    return readSitemap(data, root, url);
  }
  const pages = new Set();
  const children = new Set();
  for (const root of roots) {
    for (const child of await xml(root, 'sitemapindex')) {
      scopedURL(child, site, root);
      if (children.has(child)) throw new Error(`Duplicate sitemap reference: ${child}`);
      children.add(child);
      const entries = await xml(child, 'urlset');
      if (sections) assertSection(child, entries);
      for (const location of entries) {
        const page = scopedURL(location, site, child).href;
        if (pages.has(page)) throw new Error(`Duplicate sitemap URL: ${page}`);
        pages.add(page);
      }
    }
  }
  validateXSD(records);
  assertCrawlable(robots, [...pages, ...children, ...roots]);
  if (!pages.has(new URL('/', site).href)) throw new Error('Sitemap does not include the home page');
  if (expected) {
    if (new URL(expected.site).origin !== new URL(site).origin) throw new Error('Release inventory site does not match deployment');
    const wanted = new Set(expected.urls);
    const missing = [...wanted].filter((url) => !pages.has(url));
    const extra = [...pages].filter((url) => !wanted.has(url));
    if (missing.length || extra.length) throw new Error(`Published URL inventory differs from release artifact: missing ${missing.join(', ')}; unexpected ${extra.join(', ')}`);
  }
  return { pages: pages.size, sitemaps: children.size, compared: Boolean(expected) };
}

export async function checkProduction(site = 'https://krabka.io', inventoryFile) {
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
    const expected = inventoryFile ? JSON.parse(fs.readFileSync(inventoryFile, 'utf8')) : undefined;
    const result = await checkPublishedSitemap(site, expected);
    console.log(`✓ Published sitemap: ${result.pages} URLs in ${result.sitemaps} file(s); ${result.compared ? 'matches release artifact' : 'no release artifact supplied; freshness unverified'}`);
  } catch (error) {
    failures.push(`sitemap: ${error.message}`);
    console.error(`✗ ${failures.at(-1)}`);
  }
  return failures;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const failures = await checkProduction(process.argv[2], process.argv[3]);
  process.exitCode = failures.length ? 1 : 0;
}
