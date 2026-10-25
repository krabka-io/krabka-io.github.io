import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { readSitemap, siteURL } from './sitemap.mjs';

function files(dir) {
  if (!fs.existsSync(dir)) throw new Error(`Missing build directory: ${dir}`);
  return fs.readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const file = path.join(dir, entry.name);
    return entry.isDirectory() ? files(file) : entry.isFile() && file.endsWith('.html') ? [file] : [];
  });
}

function attributes(tag) {
  return Object.fromEntries([...tag.matchAll(/([^\s=<>]+)\s*=\s*(["'])(.*?)\2/gs)].map((m) => [m[1].toLowerCase(), m[3]]));
}

export function checkSitemap(dir, site = 'https://krabka.io') {
  const expected = new Set();
  const base = path.resolve(dir);
  const html = files(base);
  if (!html.length) throw new Error(`No built HTML pages in ${base}`);
  for (const file of html) {
    const relative = path.relative(base, file).split(path.sep).join('/');
    // Chart distribution artifacts are not site pages.
    if (relative.startsWith('charts/')) continue;
    const content = fs.readFileSync(file, 'utf8').replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, '').replace(/<!--[\s\S]*?-->/g, '');
    const metas = [...content.matchAll(/<meta\b[^>]*>/gi)].map((m) => attributes(m[0]));
    if (/^(404|500)(\.html|\/index\.html)$/.test(relative)
      || metas.some((a) => a['http-equiv']?.toLowerCase() === 'refresh'
        || ['robots', 'googlebot'].includes(a.name?.toLowerCase()) && /\b(noindex|none)\b/i.test(a.content ?? ''))) continue;
    const canonical = [...content.matchAll(/<link\b[^>]*>/gi)].map((m) => attributes(m[0])).find((a) => a.rel?.toLowerCase() === 'canonical')?.href;
    const route = '/' + relative.replace(/index\.html$/, '').replace(/\.html$/, '');
    const url = siteURL(canonical, site, relative);
    if (url.pathname.replace(/\/$/, '') !== route.replace(/\/$/, '')) throw new Error(`${relative}: canonical does not match built route: ${canonical}`);
    if (expected.has(url.href)) throw new Error(`${relative}: duplicate canonical ${url.href}`);
    expected.add(url.href);
  }
  if (!expected.size) throw new Error('No indexable built pages');
  const read = (name) => fs.readFileSync(path.join(base, name), 'utf8');
  const index = readSitemap(read('sitemap-index.xml'), 'sitemapindex', 'sitemap-index.xml');
  const actual = new Set();
  for (const location of index) {
    const url = siteURL(location, site, 'sitemap-index.xml');
    const name = decodeURIComponent(url.pathname).replace(/^\//, '');
    const filename = path.resolve(base, name);
    if (!filename.startsWith(base + path.sep) || !name.endsWith('.xml')) throw new Error(`Invalid sitemap path: ${location}`);
    for (const entry of readSitemap(fs.readFileSync(filename, 'utf8'), 'urlset', name)) {
      const page = siteURL(entry, site, name).href;
      if (actual.has(page)) throw new Error(`Duplicate sitemap URL: ${page}`);
      actual.add(page);
    }
  }
  const missing = [...expected].filter((url) => !actual.has(url));
  const extra = [...actual].filter((url) => !expected.has(url));
  if (missing.length || extra.length) throw new Error([
    ...missing.map((url) => `Missing sitemap URL: ${url}`),
    ...extra.map((url) => `Unexpected sitemap URL (missing, redirected or noindex page): ${url}`),
  ].join('\n'));
  const advertised = new URL('/sitemap-index.xml', site).href;
  if (!read('robots.txt').split(/\r?\n/).some((line) => line.trim() === `Sitemap: ${advertised}`)) throw new Error(`robots.txt must advertise ${advertised}`);
  return { pages: actual.size, sitemaps: index.length };
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const result = checkSitemap(path.resolve(process.argv[2] ?? new URL('../dist/', import.meta.url).pathname));
    console.log(`Sitemap check passed: ${result.pages} pages in ${result.sitemaps} sitemap(s).`);
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
