import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { test } from 'node:test';
import http from 'node:http';
import { runInNewContext } from 'node:vm';
import { encodeShare } from '../public/playground/lab/codec.js';
import { checkProduction } from './check-production.mjs';

const scripts = path.dirname(fileURLToPath(import.meta.url));
test('analytics load on the configured site, never on local checks or previews', () => {
  const layout = fs.readFileSync(path.join(scripts, '../src/layouts/BaseLayout.astro'), 'utf8');
  const loader = [...layout.matchAll(/<script is:inline\b[^>]*>([\s\S]*?)<\/script>/g)]
    .map((match) => match[1]).find((code) => code.includes('https://static.cloudflareinsights.com/beacon.min.js'));
  assert.ok(loader, 'analytics loader exists');
  for (const hostname of ['krabka.io', 'localhost', '127.0.0.1', '[::1]', 'krabka-io.github.io']) {
    const added = [];
    runInNewContext(loader, { beaconToken: 'test-token', siteHostname: 'krabka.io', location: { hostname },
      document: { createElement: () => ({ dataset: {} }), head: { appendChild: (script) => added.push(script) } } });
    assert.equal(added.length, hostname === 'krabka.io' ? 1 : 0, hostname);
    if (added.length) {
      assert.equal(added[0].src, 'https://static.cloudflareinsights.com/beacon.min.js');
      assert.equal(added[0].async, true);
      assert.deepEqual(JSON.parse(added[0].dataset.cfBeacon), { token: 'test-token' });
    }
  }
});

function fixture(t) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'krabka-site-check-'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  return dir;
}
function write(dir, file, content) {
  fs.mkdirSync(path.dirname(path.join(dir, file)), { recursive: true });
  fs.writeFileSync(path.join(dir, file), content);
}
function check(script, dir, status) {
  const result = spawnSync(process.execPath, [path.join(scripts, script), dir], { encoding: 'utf8' });
  assert.equal(result.status, status, result.stdout + result.stderr);
  return result.stdout + result.stderr;
}

test('link and SEO checks reject missing and empty builds', (t) => {
  const dir = fixture(t);
  for (const script of ['check-links.mjs', 'check-seo.mjs']) {
    assert.match(check(script, path.join(dir, 'missing'), 1), /No built HTML pages/);
    check(script, dir, 1);
  }
});

test('links cover the authored API page, encoded fragments and safe URL resolution', (t) => {
  const dir = fixture(t);
  write(dir, 'index.html', '<h1 id="home">Home</h1><a href="/api/?q=1&amp;x=2#caf%C3%A9">API</a><a href="#home">Home</a><a href="//example.com/remote">Remote</a><script>const example = \'<a href="/not-a-link">\';</script>');
  write(dir, 'api/index.html', '<h1 id="café">API</h1><a href="../#home">Home</a><a href="https://krabka.io/krabka-broker/">Separate API deployment</a><a href="https://krabka.io/krabka-o11y/">Observability site</a><a href="https://krabka.io/krabka-o11y/docs/observing_krabka_clusters/">Observability guide</a>');
  check('check-links.mjs', dir, 0);
  write(dir, 'api/index.html', '<a href="/#missing">Broken fragment</a><a href="/missing">Broken file</a><a href="/%2e%2e%2foutside">Escape</a><a href="javascript:alert(1)">Unsafe</a><a href="#bad%escape">Malformed</a><a href="/krabka-o11y-other/">Unknown project</a>');
  const output = check('check-links.mjs', dir, 1);
  assert.match(output, /api[\\/]index\.html/);
  assert.match(output, /Missing fragment/);
  assert.match(output, /escapes the built site/);
  assert.match(output, /Unsupported link protocol/);
  assert.match(output, /Target not found: \/krabka-o11y-other\//);
});

test('demo fragments validate scenarios, kernels and proof-session IDs', async (t) => {
  const dir = fixture(t);
  const scenario = await encodeShare({ version: 1, nodes: [{ id: 1, kind: 'producer' }] });
  write(dir, 'index.html', `<a href="/docs/lab/#s=${scenario}">Lab</a><a href="/docs/verification-playground#kernel=known">Kernel</a><a href="/docs/proof-explorer#session=module%2Fknown">Proof</a>`);
  write(dir, 'docs/lab/index.html', '<h1>Lab</h1>');
  write(dir, 'docs/verification-playground/index.html', '<script id="krabka-kernel-specs" type="application/json">[{"id":"known"}]</script>');
  write(dir, 'docs/proof-explorer/index.html', '<script id="proof-sessions" type="application/json">{"sessions":[{"id":"module/known"}]}</script>');
  check('check-links.mjs', dir, 0);
  write(dir, 'index.html', '<a href="/docs/lab/#s=damaged">Lab</a><a href="/docs/verification-playground#kernel=missing">Kernel</a><a href="/docs/proof-explorer#session=missing">Proof</a>');
  check('check-links.mjs', dir, 1);
});

test('SEO issues in the API directory and missing crawler files fail', (t) => {
  const dir = fixture(t);
  const page = (title) => `<title>${title} | krabka Documentation</title><meta name="description" content="A useful technical guide for configuring and running the broker."><link rel="canonical" href="https://krabka.io/"><meta property="og:title"><meta property="og:description"><meta property="og:url"><meta property="og:type"><meta name="twitter:card"><h1>${title}</h1>`;
  write(dir, 'index.html', page('Home'));
  write(dir, 'api/index.html', page('API'));
  write(dir, 'robots.txt', 'User-agent: *');
  write(dir, 'sitemap-index.xml', '<sitemapindex/>');
  check('check-seo.mjs', dir, 0);
  write(dir, 'api/index.html', '<h1>API</h1>');
  assert.match(check('check-seo.mjs', dir, 1), /api[\\/]index\.html/);
  write(dir, 'api/index.html', page('API'));
  fs.rmSync(path.join(dir, 'robots.txt'));
  check('check-seo.mjs', dir, 1);
});

test('production smoke detects missing pages and HTML returned for essential assets', async (t) => {
  let broken = false;
  let redirected = false;
  const server = http.createServer((req, res) => {
    if (redirected && req.url === '/docs/') { res.writeHead(302, { location: '/' }); res.end(); return; }
    if (broken && req.url === '/api/') { res.writeHead(404); res.end(); return; }
    const page = req.url.endsWith('/');
    const type = req.url.endsWith('.js') ? 'text/javascript' : req.url.endsWith('.css') ? 'text/css'
      : req.url.endsWith('.wasm') ? 'application/wasm' : req.url.endsWith('.json') ? 'application/json'
      : req.url.endsWith('.xml') ? 'application/xml' : 'text/plain';
    res.setHeader('Content-Type', page || (broken && req.url.endsWith('.wasm')) ? 'text/html' : type);
    res.end(page ? '<title>krabka documentation</title><h1>Documentation</h1>' : 'asset');
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => server.close(resolve)));
  const site = `http://127.0.0.1:${server.address().port}`;
  assert.deepEqual(await checkProduction(site), []);
  broken = true;
  const failures = await checkProduction(site);
  assert.equal(failures.length, 3);
  assert.ok(failures.some((failure) => failure.includes('/api/: HTTP 404')));
  broken = false;
  redirected = true;
  assert.match((await checkProduction(site)).join('\n'), /Unexpected redirect/);
});
