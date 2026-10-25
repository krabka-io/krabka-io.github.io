import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { test } from 'node:test';
import { checkSitemap } from './check-sitemap.mjs';
import { readSitemap } from './sitemap.mjs';

const site = 'https://krabka.io';
const ns = 'http://www.sitemaps.org/schemas/sitemap/0.9';
const xml = (root, entry, values) => `<${root} xmlns="${ns}">${values.map((value) => `<${entry}><loc>${value}</loc></${entry}>`).join('')}</${root}>`;
function fixture(t) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sitemap-'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const write = (name, body) => {
    fs.mkdirSync(path.dirname(path.join(dir, name)), { recursive: true });
    fs.writeFileSync(path.join(dir, name), body);
  };
  const page = (route) => `<link href="${site}${route}" rel="canonical"><meta content="index, follow" name="robots">`;
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
