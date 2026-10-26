import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { test } from 'node:test';
import { checkSitemap as checkBuild, buildInventory } from './check-sitemap.mjs';
import { readSitemap, MAX_BYTES, MAX_ENTRIES, siteURL, scopedURL, assertSection, assertCrawlable } from './sitemap.mjs';

const checkSitemap = (dir) => checkBuild(dir, 'https://krabka.io', { sections: false });
const site = 'https://krabka.io';
const ns = 'http://www.sitemaps.org/schemas/sitemap/0.9';
const xml = (root, entry, values) => `<${root} xmlns="${ns}">${values.map((value) => `<${entry}><loc>${value}</loc></${entry}>`).join('')}</${root}>`;
function fixture(t) {
  fs.mkdirSync(process.env.TMPDIR ?? path.resolve('artifacts'), { recursive: true, mode: 0o700 });
  const dir = fs.mkdtempSync(path.join(process.env.TMPDIR ?? path.resolve('artifacts'), 'sitemap-'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const write = (name, body) => {
    fs.mkdirSync(path.dirname(path.join(dir, name)), { recursive: true });
    fs.writeFileSync(path.join(dir, name), body);
  };
  const page = (route) => `<link href="${site}${route}" rel="canonical"><meta content="index, follow" name="robots"><a href="/docs/">Docs</a><a href="/docs/new/">New guide</a>`;
  const urls = (routes) => write('sitemap-0.xml', xml('urlset', 'url', routes.map((route) => site + route)));
  write('index.html', page('/'));
  write('docs/index.html', page('/docs/'));
  write('search/index.html', '<meta content="noindex, follow" name="robots">');
  write('old/index.html', '<meta content="0;url=/docs/" http-equiv="refresh">');
  write('404.html', '<h1>Not found</h1>');
  write('robots.txt', `User-agent: *\nSitemap: ${site}/sitemap-index.xml\n`);
  write('sitemap-index.xml', xml('sitemapindex', 'sitemap', [site + '/sitemap-0.xml']));
  urls(['/', '/docs/']);
  return { dir, write, page, urls };
}

test('sitemap covers indexable pages and follows additions and removals', (t) => {
  const { dir, write, page, urls } = fixture(t);
  assert.deepEqual(checkSitemap(dir), { pages: 2, sitemaps: 1 });
  write('docs/new/index.html', page('/docs/new/'));
  assert.throws(() => checkSitemap(dir), /Missing sitemap URL.*docs\/new/);
  urls(['/', '/docs/', '/docs/new/']);
  assert.equal(checkSitemap(dir).pages, 3);
  fs.rmSync(path.join(dir, 'docs/new'), { recursive: true });
  assert.throws(() => checkSitemap(dir), /Unexpected sitemap URL.*docs\/new/);
});

test('release inventory resolves encoded child filenames like the build checker', (t) => {
  const { dir, write } = fixture(t);
  fs.renameSync(path.join(dir, 'sitemap-0.xml'), path.join(dir, 'sitemap-café.xml'));
  write('sitemap-index.xml', xml('sitemapindex', 'sitemap', [site + '/sitemap-caf%C3%A9.xml']));
  assert.deepEqual(checkSitemap(dir), { pages: 2, sitemaps: 1 });
  assert.deepEqual(buildInventory(dir), { site, urls: [site + '/', site + '/docs/'] });
});

test('sitemap rejects excluded pages, duplicate URLs, external URLs and missing files', (t) => {
  const { dir, write, urls } = fixture(t);
  for (const route of ['/search/', '/old/', '/404.html', '/missing/']) {
    urls(['/', '/docs/', route]);
    assert.throws(() => checkSitemap(dir), /Unexpected sitemap URL/);
  }
  urls(['/', '/docs/', '/docs/']);
  assert.throws(() => checkSitemap(dir), /duplicate locations/);
  write('sitemap-index.xml', xml('sitemapindex', 'sitemap', ['https://other.test/sitemap.xml']));
  assert.throws(() => checkSitemap(dir), /same-origin/);
  write('sitemap-index.xml', xml('sitemapindex', 'sitemap', [site + '/missing.xml']));
  assert.throws(() => checkSitemap(dir), /ENOENT/);
});

test('sitemap XML rejects empty, truncated, HTML and duplicate-location entries', () => {
  for (const source of ['', '<urlset/>', `<urlset xmlns="${ns}"/>`, '<html><loc>https://krabka.io/</loc></html>',
    `<urlset xmlns="${ns}"><url><loc>https://krabka.io/</loc></url>`,
    `<urlset xmlns="${ns}"><url><loc>https://krabka.io/</loc><loc>https://krabka.io/docs/</loc></url></urlset>`]) {
    assert.throws(() => readSitemap(source, 'urlset', 'fixture'));
  }
  const escaped = xml('urlset', 'url', ['https://krabka.io/a&amp;b/']);
  assert.deepEqual(readSitemap(escaped, 'urlset', 'fixture'), ['https://krabka.io/a&b/']);
});

test('multiple child sitemaps must cover each page exactly once', (t) => {
  const { dir, write, urls } = fixture(t);
  write('sitemap-index.xml', xml('sitemapindex', 'sitemap', [site + '/sitemap-0.xml', site + '/sitemap-1.xml']));
  urls(['/']);
  write('sitemap-1.xml', xml('urlset', 'url', [site + '/docs/']));
  assert.deepEqual(checkSitemap(dir), { pages: 2, sitemaps: 2 });
  write('sitemap-1.xml', xml('urlset', 'url', [site + '/']));
  assert.throws(() => checkSitemap(dir), /Duplicate sitemap URL/);
});

test('optional metadata accepts real dates and protocol frequencies without requiring either', () => {
  const page = (metadata) => `<urlset xmlns="${ns}"><url><loc>${site}/</loc>${metadata}</url></urlset>`;
  for (const date of ['2024-02-29', '2026-10-26', '2026-10-26T00:17:41Z', '2026-10-26T00:17:41.123+05:30', '2026-10-26T00:17:41-14:00']) {
    assert.deepEqual(readSitemap(page(`<lastmod>${date}</lastmod>`), 'urlset', 'fixture'), [site + '/']);
  }
  for (const frequency of ['always', 'hourly', 'daily', 'weekly', 'monthly', 'yearly', 'never']) {
    assert.deepEqual(readSitemap(page(`<changefreq>${frequency}</changefreq>`), 'urlset', 'fixture'), [site + '/']);
  }
  assert.deepEqual(readSitemap(page(''), 'urlset', 'fixture'), [site + '/']);
  assert.deepEqual(readSitemap(page('<lastmod>2026-10-26</lastmod><changefreq>weekly</changefreq><priority>0.8</priority>'), 'urlset', 'fixture'), [site + '/']);
});

test('optional metadata rejects invalid calendars, values, namespaces, nesting and ordering', () => {
  const page = (metadata) => `<urlset xmlns="${ns}"><url><loc>${site}/</loc>${metadata}</url></urlset>`;
  for (const date of ['', 'yesterday', '0000-01-01', '2026-02-29', '2024-02-30', '2026-04-31', '2026-13-01', '2026-00-01', '2026-01-00', '2026-10-26T24:00:00Z', '2026-10-26T00:60:00Z', '2026-10-26T00:00:60Z', '2026-10-26T00:00:00', '2026-10-26T00:00:00+14:01', '2026-10-26T00:00:00+05:60']) {
    assert.throws(() => readSitemap(page(`<lastmod>${date}</lastmod>`), 'urlset', 'fixture'), /invalid lastmod/);
  }
  for (const metadata of ['<changefreq/>', '<changefreq>sometimes</changefreq>', '<changefreq>Weekly</changefreq>',
    '<lastmod>2026-10-26</lastmod><lastmod>2026-10-26</lastmod>',
    '<changefreq>weekly</changefreq><lastmod>2026-10-26</lastmod>',
    '<lastmod xmlns="urn:wrong">2026-10-26</lastmod>', '<lastmod><date>2026-10-26</date></lastmod>',
    '<priority>1.1</priority>', '<priority>-0.1</priority>', '<priority>1e0</priority>']) {
    assert.throws(() => readSitemap(page(metadata), 'urlset', 'fixture'), /invalid|nested/);
  }
});

test('index dates are optional and checked separately; page-only hints are invalid in the index', () => {
  const index = (metadata) => `<sitemapindex xmlns="${ns}"><sitemap><loc>${site}/sitemap-docs-0.xml</loc>${metadata}</sitemap></sitemapindex>`;
  assert.deepEqual(readSitemap(index('<lastmod>2026-10-26</lastmod>'), 'sitemapindex', 'fixture'), [site + '/sitemap-docs-0.xml']);
  assert.throws(() => readSitemap(index('<lastmod>2026-02-30</lastmod>'), 'sitemapindex', 'fixture'), /invalid lastmod/);
  assert.throws(() => readSitemap(index('<changefreq>weekly</changefreq>'), 'sitemapindex', 'fixture'), /invalid/);
});

test('build checks tolerate missing metadata but reject malformed supplied metadata', (t) => {
  const { dir, write } = fixture(t);
  assert.equal(checkSitemap(dir).pages, 2);
  write('sitemap-0.xml', xml('urlset', 'url', [site + '/', site + '/docs/']).replace('</loc>', '</loc><lastmod>not-a-date</lastmod>'));
  assert.throws(() => checkSitemap(dir), /invalid lastmod/);
});

test('build gate applies XSD to the index and every referenced child', (t) => {
  const { dir, write, urls } = fixture(t);
  const child = xml('urlset', 'url', [site + '/', site + '/docs/']);
  for (const invalid of [
    child.replace('</url>', '<unknown/></url>'),
    child.replace('<url>', '<url unexpected="yes">'),
    child.replace('</urlset>', '<unexpected/></urlset>'),
    child.replace('</url>', '<ext:unknown xmlns:ext="urn:extension"/></url>'),
  ]) {
    write('sitemap-0.xml', invalid);
    assert.throws(() => checkSitemap(dir), /sitemap-0\.xml: XSD validation failed/);
  }
  urls(['/', '/docs/']);
  write('sitemap-index.xml', xml('sitemapindex', 'sitemap', [site + '/sitemap-0.xml']).replace('</sitemap>', '<unknown/></sitemap>'));
  assert.throws(() => checkSitemap(dir), /sitemap-index\.xml: XSD validation failed/);
  write('sitemap-index.xml', xml('sitemapindex', 'sitemap', [site + '/sitemap-0.xml', site + '/sitemap-1.xml']));
  urls(['/']);
  write('sitemap-1.xml', xml('urlset', 'url', [site + '/docs/']).replace('</url>', '<unknown/></url>'));
  assert.throws(() => checkSitemap(dir), /sitemap-1\.xml: XSD validation failed/);
});

test('official XSD rejects structure errors independently of the shared reader', (t) => {
  const { dir, write } = fixture(t);
  const validate = (body, root = 'urlset') => {
    write('probe.xml', body);
    return spawnSync('python3', [fileURLToPath(new URL('./validate-sitemap-xsd.py', import.meta.url))], {
      input: JSON.stringify([{ file: path.join(dir, 'probe.xml'), root }]), encoding: 'utf8',
    });
  };
  const page = (content) => `<urlset xmlns="${ns}"><url>${content}</url></urlset>`;
  for (const invalid of [page(''), page(`<loc>${site}/</loc><loc>${site}/docs/</loc>`),
    page(`<loc xmlns="urn:wrong">${site}/</loc>`), page(`<loc><nested>${site}/</nested></loc>`),
    page(`<lastmod>2026-10-26</lastmod><loc>${site}/</loc>`),
    page(`<loc>${site}/</loc><priority>1.1</priority>`),
    page(`<loc>${site}/</loc><lastmod>2026-02-30</lastmod>`),
    xml('urlset', 'url', [site + '/']).replace(ns, 'urn:wrong'),
    Buffer.from(`<urlset xmlns="${ns}"><url><loc>${site}/\xff</loc></url></urlset>`, 'latin1'),
    `<?xml version="1.0" encoding="ISO-8859-1"?>${page(`<loc>${site}/</loc>`)}`,
  ]) {
    const result = validate(invalid);
    assert.equal(result.status, 1, result.stderr);
    assert.match(result.stderr, /XSD validation failed/);
  }
  assert.equal(validate(page(`<loc>${site}/caf%C3%A9?a=1&amp;b=2</loc><lastmod>2026-10-26</lastmod><changefreq>weekly</changefreq><priority>0.8</priority>`)).status, 0);
  assert.equal(validate(xml('sitemapindex', 'sitemap', [site + '/sitemap-0.xml']), 'sitemapindex').status, 0);
  assert.equal(validate(xml('urlset', 'url', [site + '/']), 'sitemapindex').status, 1);
});

test('protocol entry limits apply to both URL sets and indexes', () => {
  for (const [root, entry] of [['urlset', 'url'], ['sitemapindex', 'sitemap']]) {
    const values = Array.from({ length: MAX_ENTRIES }, (_, i) => `${site}/${i}`);
    assert.equal(readSitemap(xml(root, entry, values), root, 'boundary').length, MAX_ENTRIES);
    values.push(`${site}/over`);
    assert.throws(() => readSitemap(xml(root, entry, values), root, 'boundary'), /50000 entries/);
  }
});

test('uncompressed byte limits include XML overhead and UTF-8 byte width', () => {
  for (const [root, entry] of [['urlset', 'url'], ['sitemapindex', 'sitemap']]) {
    const body = xml(root, entry, [site + '/']);
    const prefix = '<!--é-->';
    const padding = MAX_BYTES - Buffer.byteLength(prefix + body);
    const exact = prefix + ' '.repeat(padding) + body;
    assert.equal(Buffer.byteLength(exact), MAX_BYTES);
    assert.equal(readSitemap(exact, root, 'boundary').length, 1);
    assert.throws(() => readSitemap(exact + ' ', root, 'boundary'), /uncompressed bytes/);
  }
  assert.throws(() => readSitemap(Buffer.from([0xff]), 'urlset', 'bad UTF-8'));
});

test('URL length, encoding, origin and directory scope are enforced', () => {
  const length2047 = site + '/' + 'a'.repeat(2047 - site.length - 1);
  assert.equal(siteURL(length2047, site, 'boundary').href.length, 2047);
  for (const value of [length2047 + 'a', site + '/a b', site + '/café', site + '/bad%', site + '/bad%ff', site + '/a\\b', site + '/a|b', '/relative']) {
    assert.throws(() => siteURL(value, site, 'fixture'), /length|encoding/);
  }
  for (const value of ['http://krabka.io/', 'https://krabka.io:444/', 'https://other.test/', site + '/?q=x', site + '/#x']) {
    assert.throws(() => siteURL(value, site, 'fixture'), /same-origin/);
  }
  assert.equal(scopedURL(site + '/docs/a', site, site + '/docs/sitemap.xml').pathname, '/docs/a');
  for (const value of [site + '/', site + '/docs-other/a', site + '/docs/../elsewhere']) {
    assert.throws(() => scopedURL(value, site, site + '/docs/sitemap.xml'), /directory scope/);
  }
  assert.equal(siteURL(site + '/caf%C3%A9', site, 'fixture').pathname, '/caf%C3%A9');
});

test('conflicting canonical links and orphan pages fail with actionable diagnostics', (t) => {
  const { dir, write, page, urls } = fixture(t);
  write('index.html', page('/') + `<link rel="canonical" href="${site}/other/">`);
  assert.throws(() => checkSitemap(dir), /exactly one canonical/);
  write('index.html', `<link rel="canonical" href="${site}/"><script>const x = '<a href="/docs/">';</script><!-- <a href="/docs/"> -->`);
  assert.throws(() => checkSitemap(dir), /orphan pages.*docs/);
  write('index.html', page('/'));
  write('docs/new/index.html', page('/docs/new/'));
  urls(['/', '/docs/', '/docs/new/']);
  assert.equal(checkSitemap(dir).pages, 3);
});

test('robots rules protect sitemap discovery and crawler-specific page access', () => {
  const urls = [site + '/docs/a', site + '/sitemap-index.xml'];
  for (const rules of ['User-agent: *\nDisallow: /', 'User-agent: *\nDisallow: /docs/*$', 'User-agent: *\nAllow: /\nUser-agent: Googlebot\nDisallow: /docs/', 'User-agent: *\nDisallow: /*.xml$']) {
    assert.throws(() => assertCrawlable(rules, urls), /robots.txt blocks/);
  }
  assert.doesNotThrow(() => assertCrawlable('User-agent: *\nDisallow: /docs/\nAllow: /docs/a', urls));
  assert.doesNotThrow(() => assertCrawlable('User-agent: *\nDisallow: /docs/a\nAllow: /docs/a', urls));
  assert.doesNotThrow(() => assertCrawlable('User-agent: *\nDisallow: /docs/a.b', [site + '/docs/axb']));
});

test('docs and website sections are mutually exclusive; empty groups are not required', (t) => {
  const { dir, write } = fixture(t);
  write('sitemap-index.xml', xml('sitemapindex', 'sitemap', [site + '/sitemap-docs-0.xml', site + '/sitemap-website-0.xml']));
  write('sitemap-docs-0.xml', xml('urlset', 'url', [site + '/docs/']));
  write('sitemap-website-0.xml', xml('urlset', 'url', [site + '/']));
  assert.equal(checkBuild(dir).sitemaps, 2);
  write('sitemap-docs-0.xml', xml('urlset', 'url', [site + '/']));
  assert.throws(() => checkBuild(dir), /wrong section/);
  assert.throws(() => assertSection(site + '/sitemap-0.xml', [site + '/']), /Unrecognized/);
  assert.doesNotThrow(() => assertSection(site + '/sitemap-website-0.xml', [site + '/api/']));
});

test('generator shards on serialized bytes and entry count without losing URLs', async () => {
  const { splitSitemap } = await import('./sitemap-limits.mjs');
  const values = Array.from({ length: 7 }, (_, i) => `${site}/caf%C3%A9/${i}`);
  const body = xml('urlset', 'url', values);
  const one = Buffer.byteLength(xml('urlset', 'url', values.slice(0, 1)));
  const entryBytes = Buffer.byteLength(`<url><loc>${values[0]}</loc></url>`);
  for (const options of [{ maxBytes: one + entryBytes }, { maxEntries: 2 }]) {
    const chunks = splitSitemap(body, options);
    assert.equal(chunks.length, 4);
    assert.deepEqual(chunks.flatMap((chunk) => readSitemap(chunk, 'urlset', 'shard')), values);
    assert.ok(chunks.every((chunk) => Buffer.byteLength(chunk) <= (options.maxBytes ?? MAX_BYTES)));
  }
  assert.throws(() => splitSitemap(body, { maxBytes: one - 1 }), /single sitemap entry/);
});
