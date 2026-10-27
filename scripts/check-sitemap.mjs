import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  readSitemap,
  siteURL,
  scopedURL,
  validateXSD,
  advertisedSitemaps,
  assertCrawlable,
  assertSection,
} from './sitemap.mjs';

function files(dir) {
  if (!fs.existsSync(dir)) throw new Error(`Missing build directory: ${dir}`);
  return fs.readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const file = path.join(dir, entry.name);
    return entry.isDirectory()
      ? files(file)
      : entry.isFile() && file.endsWith('.html')
        ? [file]
        : [];
  });
}

function attributes(tag) {
  return Object.fromEntries(
    [...tag.matchAll(/([^\s=<>]+)\s*=\s*(["'])(.*?)\2/gs)].map((m) => [m[1].toLowerCase(), m[3]]),
  );
}

export function assertReachable(graph, site) {
  const result = spawnSync(
    'python3',
    [fileURLToPath(new URL('./check-sitemap-reachability.py', import.meta.url))],
    {
      input: JSON.stringify({ site, pages: [...graph.values()] }),
      encoding: 'utf8',
      timeout: 30_000,
    },
  );
  if (result.error || result.status !== 0)
    throw new Error(result.error?.message || result.stderr.trim());
}

export function checkSitemap(dir, site = 'https://krabka.io', { sections = true } = {}) {
  const expected = new Set();
  const base = path.resolve(dir);
  const html = files(base);
  const graph = new Map();
  if (!html.length) throw new Error(`No built HTML pages in ${base}`);
  for (const file of html) {
    const relative = path.relative(base, file).split(path.sep).join('/');
    // Chart distribution artifacts are not site pages.
    if (relative.startsWith('charts/')) continue;
    const content = fs
      .readFileSync(file, 'utf8')
      .replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, '')
      .replace(/<!--[\s\S]*?-->/g, '');
    const metas = [...content.matchAll(/<meta\b[^>]*>/gi)].map((m) => attributes(m[0]));
    if (
      /^(404|500)(\.html|\/index\.html)$/.test(relative) ||
      metas.some(
        (a) =>
          a['http-equiv']?.toLowerCase() === 'refresh' ||
          (['robots', 'googlebot'].includes(a.name?.toLowerCase()) &&
            /\b(noindex|none)\b/i.test(a.content ?? '')),
      )
    )
      continue;
    const canonicals = [...content.matchAll(/<link\b[^>]*>/gi)]
      .map((m) => attributes(m[0]))
      .filter((a) => a.rel?.toLowerCase().split(/\s+/).includes('canonical'));
    if (canonicals.length !== 1) throw new Error(`${relative}: expected exactly one canonical`);
    const canonical = canonicals[0].href;
    const route = '/' + relative.replace(/index\.html$/, '').replace(/\.html$/, '');
    const url = siteURL(canonical, site, relative);
    if (url.pathname.replace(/\/$/, '') !== route.replace(/\/$/, ''))
      throw new Error(`${relative}: canonical does not match built route: ${canonical}`);
    if (expected.has(url.href)) throw new Error(`${relative}: duplicate canonical ${url.href}`);
    expected.add(url.href);
    graph.set(url.href, { file, base: url.href });
  }
  if (!expected.size) throw new Error('No indexable built pages');
  const read = (name) => fs.readFileSync(path.join(base, name));
  const index = readSitemap(read('sitemap-index.xml'), 'sitemapindex', 'sitemap-index.xml');
  const schemaFiles = [{ file: path.join(base, 'sitemap-index.xml'), root: 'sitemapindex' }];
  const actual = new Set();
  for (const location of index) {
    const url = scopedURL(location, site, new URL('/sitemap-index.xml', site).href);
    const name = decodeURIComponent(url.pathname).replace(/^\//, '');
    const filename = path.resolve(base, name);
    if (!filename.startsWith(base + path.sep) || !name.endsWith('.xml'))
      throw new Error(`Invalid sitemap path: ${location}`);
    schemaFiles.push({ file: filename, root: 'urlset' });
    const entries = readSitemap(fs.readFileSync(filename), 'urlset', name);
    if (sections) assertSection(location, entries);
    for (const entry of entries) {
      const page = scopedURL(entry, site, location).href;
      if (actual.has(page)) throw new Error(`Duplicate sitemap URL: ${page}`);
      actual.add(page);
    }
  }
  validateXSD(schemaFiles);
  const missing = [...expected].filter((url) => !actual.has(url));
  const extra = [...actual].filter((url) => !expected.has(url));
  if (missing.length || extra.length)
    throw new Error(
      [
        ...missing.map((url) => `Missing sitemap URL: ${url}`),
        ...extra.map(
          (url) => `Unexpected sitemap URL (missing, redirected or noindex page): ${url}`,
        ),
      ].join('\n'),
    );
  const robots = read('robots.txt').toString('utf8');
  advertisedSitemaps(robots, site);
  assertCrawlable(robots, [...actual, ...index, new URL('/sitemap-index.xml', site).href]);
  assertReachable(graph, site);
  return { pages: actual.size, sitemaps: index.length };
}

// Written only after all checks pass, and packaged inside the Pages artifact.
export function buildInventory(dir, site = 'https://krabka.io') {
  const children = readSitemap(
    fs.readFileSync(path.join(dir, 'sitemap-index.xml')),
    'sitemapindex',
    'sitemap-index.xml',
  );
  return {
    site,
    urls: children
      .flatMap((child) =>
        readSitemap(
          fs.readFileSync(
            path.join(dir, decodeURIComponent(new URL(child).pathname).replace(/^\//, '')),
          ),
          'urlset',
          child,
        ),
      )
      .sort(),
  };
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const dir = path.resolve(
      process.argv[2] ?? fileURLToPath(new URL('../dist/', import.meta.url)),
    );
    const result = checkSitemap(dir);
    fs.writeFileSync(
      path.join(dir, 'sitemap-inventory.json'),
      JSON.stringify(buildInventory(dir), null, 2) + '\n',
    );
    console.log(`Sitemap check passed: ${result.pages} pages in ${result.sitemaps} sitemap(s).`);
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
