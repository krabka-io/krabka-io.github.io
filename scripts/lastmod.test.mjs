import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { test } from 'node:test';
import { contentHash, sourceFileProvenance, importedInputs, resolveLastmod, validateAttestations } from './lastmod-provenance.mjs';
import { authoredRoutes, routeInputs } from './lastmod-routes.mjs';
import { syncGuides } from './sync-docs.mjs';
import { applyLastmod } from './sitemap-lastmod.mjs';
import { enforceSitemapLimits } from './sitemap-limits.mjs';
import { readSitemap, validateXSD } from './sitemap.mjs';

function fixture(t) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'lastmod-test-'));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const write = (file, text) => { fs.mkdirSync(path.dirname(path.join(root, file)), { recursive: true }); fs.writeFileSync(path.join(root, file), text); };
  const git = (...args) => execFileSync('git', ['-C', root, ...args], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'], env: { ...process.env, GIT_AUTHOR_NAME: 'Fixture', GIT_AUTHOR_EMAIL: 'fixture@example.invalid', GIT_COMMITTER_NAME: 'Fixture', GIT_COMMITTER_EMAIL: 'fixture@example.invalid', GIT_AUTHOR_DATE: '2026-10-01T12:00:00Z', GIT_COMMITTER_DATE: '2026-10-01T12:00:00Z' } }).trim();
  git('init', '-q');
  const commit = () => { git('add', '.'); git('commit', '-qm', 'Fixture content'); return git('rev-parse', 'HEAD'); };
  const reviewed = (inputs, date = '2026-10-01') => ({ date, complete: true, owner: 'Fixture reviewer', revision: git('rev-parse', 'HEAD'), evidence: 'Explicit significant-update review for these fixture inputs.', inputs: Object.fromEntries(inputs.map((file) => [file, contentHash(fs.readFileSync(path.join(root, file)))])) });
  return { root, write, git, commit, reviewed };
}

const mapPage = (source, inputs = [source]) => ({ family: 'authored', source, inputs, complete: true });

test('explicit review wins; unknown, changed, dirty, shallow and non-Git inputs omit without a fallback', (t) => {
  const f = fixture(t);
  f.write('page.astro', '<h1>Original claim</h1>');
  f.write('shared.astro', '<p>Shared claim</p>');
  f.commit();
  const attestation = f.reviewed(['page.astro', 'shared.astro']);
  const resolve = (changes = {}) => resolveLastmod({ root: f.root, url: 'https://krabka.io/page/', mapping: mapPage('page.astro', ['page.astro', 'shared.astro']), attestation, ...changes });
  assert.equal(resolve().date, '2026-10-01');
  f.write('unrelated.txt', 'An unrelated commit'); f.commit();
  assert.equal(resolve().date, '2026-10-01');
  assert.equal(resolve({ mapping: null }).reason, 'unmapped-route');
  assert.equal(resolve({ attestation: null }).reason, 'no-reviewed-significant-update');
  assert.equal(resolve({ state: { historyComplete: false } }).date, null);
  assert.equal(resolve({ state: { historyComplete: false, reason: 'non-git-source' } }).reason, 'non-git-source');
  f.write('shared.astro', '<p>Changed substantive claim</p>');
  assert.equal(resolve().reason, 'dirty-or-untracked-input');
  f.commit();
  assert.equal(resolve().reason, 'input-history-changed-since-review');
  assert.equal(resolve({ attestation: f.reviewed(['page.astro', 'shared.astro'], '2026-10-02') }).date, '2026-10-02');
  f.write('page.astro', '<h1>Original claim</h1>\n'); f.commit();
  // Cosmetic reviews keep the previous significant date, never the commit date.
  assert.equal(resolve({ attestation: f.reviewed(['page.astro', 'shared.astro'], '2026-10-02') }).date, '2026-10-02');
  assert.equal(resolve({ attestation: f.reviewed(['page.astro']) }).reason, 'unattested-route-input');
});

test('new imported dependencies cannot silently reuse an old date', (t) => {
  const f = fixture(t);
  f.write('page.astro', "---\nimport Shared from './shared.astro';\n---\n<Shared />");
  f.write('shared.astro', '<p>A meaningful input</p>');
  f.commit();
  assert.deepEqual([...importedInputs(f.root, 'page.astro')].sort(), ['page.astro', 'shared.astro']);
  const resolve = (record) => resolveLastmod({ root: f.root, url: 'https://krabka.io/page/', mapping: mapPage('page.astro'), attestation: record });
  assert.equal(resolve(f.reviewed(['page.astro'])).reason, 'unattested-import-dependency');
  assert.equal(resolve(f.reviewed(['page.astro', 'shared.astro'])).date, '2026-10-01');
});

test('restored bytes cannot hide a significant intervening edit or revert', (t) => {
  const f = fixture(t);
  f.write('page.astro', '<h1>Original claim</h1>'); f.commit();
  const attestation = f.reviewed(['page.astro']);
  f.write('page.astro', '<h1>Revised claim</h1>'); f.commit();
  f.write('page.astro', '<h1>Original claim</h1>'); f.commit();
  const record = resolveLastmod({ root: f.root, url: 'https://krabka.io/page/', mapping: mapPage('page.astro'), attestation });
  assert.equal(record.date, null);
  assert.equal(record.reason, 'input-history-changed-since-review');
});

test('source history pins paths and follows renames; HEAD, copies and local modifications are not page dates', (t) => {
  const f = fixture(t);
  f.write('docs/operations/KIP_MATRIX.md', '# Original matrix\n');
  const original = f.commit();
  f.write('unrelated.txt', 'Not a guide change'); const pinned = f.commit();
  const clean = sourceFileProvenance(f.root, 'broker', 'docs/operations/KIP_MATRIX.md', pinned);
  assert.equal(clean.lastChange.commit, original);
  assert.equal(clean.revision, pinned);
  assert.equal(clean.lastChange.significance, 'unreviewed');
  assert.equal(clean.historyComplete, true);
  f.git('mv', 'docs/operations/KIP_MATRIX.md', 'docs/operations/Renamed.md'); f.commit();
  assert.equal(sourceFileProvenance(f.root, 'broker', 'docs/operations/Renamed.md').lastChange.commit, f.git('rev-parse', 'HEAD'));
  f.write('docs/operations/Renamed.md', '# Local change');
  assert.equal(sourceFileProvenance(f.root, 'broker', 'docs/operations/Renamed.md').reason, 'dirty-or-untracked-source');
  f.write('docs/untracked.md', '# New guide');
  assert.equal(sourceFileProvenance(f.root, 'broker', 'docs/untracked.md').reason, 'dirty-or-untracked-source');
  fs.mkdirSync(path.join(f.root, 'archive'));
  f.write('archive/guide.md', '# No Git metadata');
  assert.equal(sourceFileProvenance(path.join(f.root, 'archive'), 'broker', 'guide.md').reason, 'non-git-source');
  f.git('checkout', '--', 'docs/operations/Renamed.md');
  const shallow = path.join(os.tmpdir(), 'shallow-' + path.basename(f.root));
  t.after(() => fs.rmSync(shallow, { recursive: true, force: true }));
  execFileSync('git', ['clone', '-q', '--depth', '1', 'file://' + f.root, shallow]);
  const incomplete = sourceFileProvenance(shallow, 'broker', 'docs/operations/Renamed.md');
  assert.equal(incomplete.reason, 'incomplete-history');
  assert.equal(incomplete.lastChange, null);
});

test('sync publishes per-file/custom/nested provenance atomically without changing source-link mappings', (t) => {
  const f = fixture(t);
  f.write('docs/operations/KIP_MATRIX.md', '# Matrix\n');
  f.write('README.md', '# Custom guide\n'); f.commit();
  const content = path.join(os.tmpdir(), 'content-' + path.basename(f.root));
  t.after(() => fs.rmSync(content, { recursive: true, force: true }));
  const comp = { name: 'broker', repo: 'krabka-broker', docsSubdir: 'broker', localRepoDir: f.root,
    guides: [{ source: 'docs/operations/KIP_MATRIX.md', destination: 'operations/KIP_MATRIX.md' }, { source: 'README.md', destination: 'reference.md' }] };
  syncGuides([comp], content);
  const file = path.join(content, 'lastmod-provenance.json');
  const before = fs.readFileSync(file, 'utf8');
  const record = JSON.parse(before).components.broker.files['operations/KIP_MATRIX.md'];
  assert.equal(record.sourcePath, 'docs/operations/KIP_MATRIX.md');
  assert.equal(record.renderedHash, contentHash(fs.readFileSync(path.join(content, 'broker/operations/KIP_MATRIX.md'))));
  assert.equal(JSON.parse(fs.readFileSync(path.join(content, 'sources.json'))).broker.files['reference.md'], 'README.md');
  const site = path.join(f.root, 'site');
  fs.mkdirSync(path.join(site, 'src/content/docs'), { recursive: true });
  fs.copyFileSync(file, path.join(site, 'src/content/docs/lastmod-provenance.json'));
  const mapped = routeInputs(site, 'https://krabka.io/docs/broker/operations/kip_matrix/');
  assert.equal(mapped.family, 'synced-guide');
  assert.equal(mapped.provenance[0].sourcePath, 'docs/operations/KIP_MATRIX.md');
  assert.equal(routeInputs(site, 'https://krabka.io/docs/broker/reference/').family, 'synced-guide');
  assert.throws(() => syncGuides([{ ...comp, guides: [...comp.guides, { source: 'missing.md', destination: 'missing.md' }] }], content));
  assert.equal(fs.readFileSync(file, 'utf8'), before);
});

test('malformed explicit dates fail, including records for unmapped routes', () => {
  for (const date of ['2026-02-30', '2026-10-01T12:00:00', 'not-a-date']) assert.throws(() => validateAttestations({ '/unknown': { date } }), /invalid explicit lastmod/);
  assert.equal(routeInputs(process.cwd(), 'https://krabka.io/releases/').gap, 'release-edit-and-membership-history-unavailable');
  assert.equal(routeInputs(process.cwd(), 'https://krabka.io/versions/').gap, 'live-version-snapshot-history-unavailable');
  assert.equal(routeInputs(process.cwd(), 'https://krabka.io/new-route/'), null);
  assert.ok(Object.keys(authoredRoutes).length > 1);
});

const ns = 'http://www.sitemaps.org/schemas/sitemap/0.9';
function sitemaps(dir, urls) {
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, 'sitemap-index.xml'), `<?xml version="1.0" encoding="UTF-8"?><sitemapindex xmlns="${ns}"><sitemap><loc>https://krabka.io/sitemap-website-0.xml</loc><lastmod>2026-10-01</lastmod></sitemap></sitemapindex>`);
  fs.writeFileSync(path.join(dir, 'sitemap-website-0.xml'), `<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="${ns}">${urls.map((url) => `<url><loc>${url}</loc></url>`).join('')}</urlset>`);
}

test('central build hook preserves dates through forced byte/count splits and unchanged rebuilds, never index dates', (t) => {
  const f = fixture(t);
  f.write('page.astro', '<h1>Verified content</h1>'); f.commit();
  const record = f.reviewed(['page.astro']);
  const urls = ['https://krabka.io/one/', 'https://krabka.io/two/', 'https://krabka.io/three/'];
  const options = { root: f.root, attestations: Object.fromEntries(['/one', '/two', '/three'].map((route) => [route, record])), map: () => mapPage('page.astro') };
  const build = (name, inventory = urls) => {
    const dir = path.join(f.root, name); sitemaps(dir, inventory);
    const records = applyLastmod(dir, options);
    assert.equal(records.filter((entry) => entry.date).length, inventory.length);
    enforceSitemapLimits(dir, { maxBytes: 260, maxEntries: 1 });
    const indexFile = path.join(dir, 'sitemap-index.xml');
    const index = fs.readFileSync(indexFile, 'utf8');
    assert.doesNotMatch(index, /lastmod/);
    const children = readSitemap(index, 'sitemapindex', indexFile);
    assert.equal(children.length, inventory.length);
    validateXSD([{ file: indexFile, root: 'sitemapindex' }, ...children.map((child) => ({ file: path.join(dir, path.basename(new URL(child).pathname)), root: 'urlset' }))]);
    for (const child of children) {
      const xml = fs.readFileSync(path.join(dir, path.basename(new URL(child).pathname)), 'utf8');
      assert.match(xml, /<lastmod>2026-10-01<\/lastmod>/);
      assert.ok(Buffer.byteLength(xml) <= 260);
    }
    return Object.fromEntries(fs.readdirSync(dir).sort().map((file) => [file, fs.readFileSync(path.join(dir, file), 'utf8')]));
  };
  assert.deepEqual(build('first'), build('second'));
  build('removed', urls.slice(0, 1));
});
