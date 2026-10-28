import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { test } from 'node:test';
import {
  contentHash,
  sourceFileProvenance,
  importedInputs,
  resolveLastmod,
  ignoredRevisions,
} from './lastmod-provenance.mjs';
import { authoredRoutes, routeInputs } from './lastmod-routes.mjs';
import { syncGuides } from './sync-docs.mjs';
import { applyLastmod } from './sitemap-lastmod.mjs';
import { enforceSitemapLimits } from './sitemap-limits.mjs';
import { readSitemap, validateXSD } from './sitemap.mjs';

function fixture(t) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'lastmod-test-'));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  let date = '2026-10-01T12:00:00Z';
  const write = (file, text) => {
    fs.mkdirSync(path.dirname(path.join(root, file)), { recursive: true });
    fs.writeFileSync(path.join(root, file), text);
  };
  const git = (...args) =>
    execFileSync('git', ['-C', root, ...args], {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'pipe'],
      env: {
        ...process.env,
        GIT_AUTHOR_NAME: 'Fixture',
        GIT_AUTHOR_EMAIL: 'fixture@example.invalid',
        GIT_COMMITTER_NAME: 'Fixture',
        GIT_COMMITTER_EMAIL: 'fixture@example.invalid',
        GIT_AUTHOR_DATE: date,
        GIT_COMMITTER_DATE: date,
      },
    }).trim();
  git('init', '-q');
  const commit = (when = date) => {
    date = when;
    git('add', '.');
    git('commit', '-qm', 'Fixture content');
    return git('rev-parse', 'HEAD');
  };
  return { root, write, git, commit };
}
const mapPage = (source, inputs = [source]) => ({ family: 'authored', source, inputs });
const resolve = (f, mapping, options = {}) =>
  resolveLastmod({ root: f.root, url: 'https://krabka.io/page/', mapping, ...options });

test('maximum content-file date is automatic; unrelated HEAD, dirty, untracked and incomplete history behave correctly', (t) => {
  const f = fixture(t);
  f.write('page.astro', '<h1>Original</h1>');
  f.commit();
  f.write('shared.astro', '<p>Shared claim</p>');
  f.commit('2026-10-02T12:00:00Z');
  const mapping = mapPage('page.astro', ['page.astro', 'shared.astro']);
  assert.equal(resolve(f, mapping).date, '2026-10-02');
  f.write('unrelated.txt', 'Unrelated');
  f.commit('2026-10-03T12:00:00Z');
  assert.equal(resolve(f, mapping).date, '2026-10-02');
  assert.equal(resolve(f, null).reason, 'unmapped-route');
  assert.equal(resolve(f, mapping, { state: { historyComplete: false } }).date, null);
  assert.equal(
    resolve(f, mapping, { state: { historyComplete: false, reason: 'non-git-source' } }).reason,
    'non-git-source',
  );
  f.write('shared.astro', '<p>Changed claim</p>');
  assert.equal(resolve(f, mapping).reason, 'dirty-or-untracked-source');
  f.git('add', 'shared.astro');
  assert.equal(resolve(f, mapping).date, null); // staged changes also omit
  f.commit('2026-10-04T12:00:00Z');
  assert.equal(resolve(f, mapping).date, '2026-10-04');
  f.write('new.json', '{}');
  assert.equal(resolve(f, mapPage('new.json')).date, null);
  f.write('.gitignore', 'ignored.json\n');
  f.write('ignored.json', '{}');
  assert.equal(resolve(f, mapPage('ignored.json')).date, null);
});

test('Git clean filters such as LFS do not mistake hydrated content for a dirty or mismatched input', (t) => {
  const f = fixture(t);
  f.git('config', 'filter.fixture.clean', 'sed s/hydrated/stored/g');
  f.git('config', 'filter.fixture.smudge', 'sed s/stored/hydrated/g');
  f.write('.gitattributes', 'asset.bin filter=fixture\n');
  f.write('asset.bin', 'hydrated');
  f.commit();
  assert.equal(f.git('show', 'HEAD:asset.bin'), 'stored');
  assert.equal(resolve(f, mapPage('asset.bin')).date, '2026-10-01');
});

test('optional ignored cosmetic commits and prefixes select previous file history; edit/revert requires no attestation', (t) => {
  const f = fixture(t);
  f.write('page.astro', '<h1>Original</h1>');
  const original = f.commit();
  f.write('page.astro', '<h1>Original</h1>\n');
  const cosmetic = f.commit('2026-10-02T12:00:00Z');
  assert.equal(resolve(f, mapPage('page.astro')).date, '2026-10-02');
  for (const ignored of [cosmetic, cosmetic.slice(0, 9)]) {
    const record = resolve(f, mapPage('page.astro'), { ignoreRevs: [ignored] });
    assert.equal(record.date, '2026-10-01');
    assert.equal(record.evidence[0].lastChange.commit, original);
  }
  f.write('scripts/lastmod-ignore-revs.txt', `# cosmetic\n${cosmetic.slice(0, 9)} # formatting\n`);
  assert.equal(resolve(f, mapPage('page.astro')).date, '2026-10-01');
  f.write('page.astro', '<h1>Revised</h1>');
  f.commit('2026-10-03T12:00:00Z');
  f.write('page.astro', '<h1>Original</h1>');
  f.commit('2026-10-04T12:00:00Z');
  assert.equal(resolve(f, mapPage('page.astro')).date, '2026-10-04');
  f.write('scripts/lastmod-ignore-revs.txt', 'not-a-sha\n');
  assert.throws(() => ignoredRevisions(f.root), /Invalid ignored/);
});

test('content import traversal stops at chrome/plumbing; shared content and data changes update the page date', (t) => {
  const f = fixture(t);
  f.write(
    'src/pages/features/kraft.astro',
    "import Base from '../../layouts/BaseLayout.astro';\nimport Hero from '../../components/Hero.astro';",
  );
  f.write('src/layouts/BaseLayout.astro', "import versions from '../data/versions.json';");
  f.write('src/data/versions.json', '{}');
  f.write(
    'src/components/Hero.astro',
    "import data from '../data/claims.json';\nimport '../styles/global.css';",
  );
  f.write('src/data/claims.json', '{}');
  f.write('src/styles/global.css', 'body {}');
  f.commit();
  const mapping = () => routeInputs(f.root, 'https://krabka.io/features/kraft/');
  assert.deepEqual(mapping().inputs, [
    'src/components/Hero.astro',
    'src/data/claims.json',
    'src/pages/features/kraft.astro',
  ]);
  f.write(
    'src/layouts/BaseLayout.astro',
    "import versions from '../data/versions.json';\n<!-- chrome -->",
  );
  f.write('src/data/versions.json', '{"unused":"updated"}');
  f.write('src/styles/global.css', 'body { margin: 0; }');
  // Even dirty shell/CSS and its unused data do not suppress content dates.
  assert.equal(resolve(f, mapping()).date, '2026-10-01');
  f.commit('2026-10-02T12:00:00Z');
  assert.equal(resolve(f, mapping()).date, '2026-10-01');
  f.write('src/data/claims.json', '{"new":"claim"}');
  f.commit('2026-10-03T12:00:00Z');
  assert.equal(resolve(f, mapping()).date, '2026-10-03');
  f.write(
    'src/components/Hero.astro',
    "import data from '../data/claims.json';\n<p>New shared content</p>",
  );
  f.commit('2026-10-04T12:00:00Z');
  assert.equal(resolve(f, mapping()).date, '2026-10-04');
  assert.ok(
    ![...importedInputs(f.root, 'src/pages/features/kraft.astro')].some((file) =>
      file.includes('BaseLayout'),
    ),
  );
});

test('source history pins paths, follows renames, filters upstream cosmetic commits and rejects shallow/non-Git/dirty sources', (t) => {
  const f = fixture(t);
  f.write('docs/operations/KIP_MATRIX.md', '# Matrix\n');
  const original = f.commit();
  f.write('unrelated.txt', 'Unrelated');
  const pinned = f.commit('2026-10-02T12:00:00Z');
  const record = sourceFileProvenance(f.root, 'broker', 'docs/operations/KIP_MATRIX.md', pinned);
  assert.equal(record.lastChange.commit, original);
  assert.equal(record.revision, pinned);
  assert.equal(record.lastChange.significance, undefined);
  f.git('mv', 'docs/operations/KIP_MATRIX.md', 'docs/operations/Renamed.md');
  const renamed = f.commit('2026-10-03T12:00:00Z');
  assert.equal(
    sourceFileProvenance(f.root, 'broker', 'docs/operations/Renamed.md').lastChange.commit,
    renamed,
  );
  assert.equal(
    sourceFileProvenance(f.root, 'broker', 'docs/operations/Renamed.md', undefined, {
      ignoreRevs: [renamed],
    }).lastChange.commit,
    original,
  );
  f.write('docs/operations/Renamed.md', '# Local');
  assert.equal(
    sourceFileProvenance(f.root, 'broker', 'docs/operations/Renamed.md').reason,
    'dirty-or-untracked-source',
  );
  f.git('checkout', '--', 'docs/operations/Renamed.md');
  const shallow = path.join(f.root, 'shallow');
  execFileSync('git', ['clone', '-q', '--depth', '1', 'file://' + f.root, shallow]);
  assert.equal(
    sourceFileProvenance(shallow, 'broker', 'docs/operations/Renamed.md').reason,
    'incomplete-history',
  );
  fs.mkdirSync(path.join(f.root, 'archive'));
  f.write('archive/guide.md', '# Archive');
  assert.equal(
    sourceFileProvenance(path.join(f.root, 'archive'), 'broker', 'guide.md').reason,
    'non-git-source',
  );
});

test('sync publishes automatic nested/custom guide dates atomically and preserves source-link mappings', (t) => {
  const f = fixture(t);
  f.write('docs/operations/KIP_MATRIX.md', '# Matrix\n');
  f.write('README.md', '# Custom guide\n');
  f.commit();
  f.write('README.md', '# Custom guide\n\n');
  const cosmetic = f.commit('2026-10-02T12:00:00Z');
  const content = path.join(f.root, 'site/src/content/docs');
  const comp = {
    name: 'broker',
    repo: 'krabka-broker',
    docsSubdir: 'broker',
    localRepoDir: f.root,
    guides: [
      { source: 'docs/operations/KIP_MATRIX.md', destination: 'operations/KIP_MATRIX.md' },
      { source: 'README.md', destination: 'reference.md' },
    ],
  };
  syncGuides([comp], content, { ignoreRevs: [cosmetic] });
  const file = path.join(content, 'lastmod-provenance.json');
  const before = fs.readFileSync(file, 'utf8');
  const record = JSON.parse(before).components.broker.files['operations/KIP_MATRIX.md'];
  assert.equal(record.sourcePath, 'docs/operations/KIP_MATRIX.md');
  assert.equal(
    record.renderedHash,
    contentHash(fs.readFileSync(path.join(content, 'broker/operations/KIP_MATRIX.md'))),
  );
  assert.equal(
    JSON.parse(fs.readFileSync(path.join(content, 'sources.json'))).broker.files['reference.md'],
    'README.md',
  );
  for (const route of ['/docs/broker/operations/kip_matrix/', '/docs/broker/reference/']) {
    const mapped = routeInputs(path.join(f.root, 'site'), 'https://krabka.io' + route);
    assert.equal(mapped.family, 'synced-guide');
    assert.deepEqual(mapped.inputs, []);
    assert.equal(resolve(f, mapped).date, '2026-10-01');
    assert.equal(
      resolve(f, { ...mapped, provenance: [{ ...record, historyComplete: false }] }).date,
      null,
    );
    assert.equal(
      resolve(f, { ...mapped, provenance: [{ ...record, dirtySource: true }] }).date,
      null,
    );
  }
  assert.throws(() =>
    syncGuides(
      [{ ...comp, guides: [...comp.guides, { source: 'missing.md', destination: 'missing.md' }] }],
      content,
    ),
  );
  assert.equal(fs.readFileSync(file, 'utf8'), before);
});

test('CRD kinds use their matching YAML history, index uses all YAML histories, and content components contribute', (t) => {
  const f = fixture(t);
  const base = { repo: 'operator', historyComplete: true, dirtySource: false, reason: null };
  const crds = Object.fromEntries(
    ['Kafka', 'KafkaTopic'].map((kind, i) => [
      `charts/crds/${kind}.yaml`,
      {
        ...base,
        sourcePath: `charts/crds/${kind}.yaml`,
        lastChange: { date: `2026-10-0${i + 2}T12:00:00Z`, commit: String(i).repeat(40) },
      },
    ]),
  );
  f.write(
    'src/content/docs/lastmod-provenance.json',
    JSON.stringify({ components: { operator: { crds, files: {} } } }),
  );
  f.write(
    'src/content/docs/operator/crds.json',
    JSON.stringify({
      resources: ['Kafka', 'KafkaTopic'].map((kind) => ({
        _sourcePath: `charts/crds/${kind}.yaml`,
        spec: { names: { kind } },
      })),
    }),
  );
  f.write('src/pages/docs/operator/crds.astro', '<h1>CRDs</h1>');
  f.write(
    'src/pages/docs/operator/crds/[kind].astro',
    "import Description from '../../../../components/CrdDescription.astro';",
  );
  f.write('src/components/CrdDescription.astro', '<p>Description</p>');
  f.commit();
  const mapping = (route) => routeInputs(f.root, 'https://krabka.io/docs/operator/crds' + route);
  assert.equal(resolve(f, mapping('/kafka/')).date, '2026-10-02');
  assert.equal(resolve(f, mapping('/kafkatopic/')).date, '2026-10-03');
  assert.equal(mapping('/kafka/').provenance.length, 1);
  assert.equal(resolve(f, mapping('/')).date, '2026-10-03');
  f.write('src/components/CrdDescription.astro', '<p>Updated description</p>');
  f.commit('2026-10-04T12:00:00Z');
  assert.equal(resolve(f, mapping('/kafka/')).date, '2026-10-04');
});

test('generated benchmark imports resolve to tracked sources before generation and retain change dates', (t) => {
  const f = fixture(t);
  const source = 'src/pages/benchmarks/explorer.astro';
  const snapshot = 'src/data/benchmark-archive.json';
  const archive = 'benchmarks/archive/fixture.tar.gz';
  f.write(source, "import archive from '../../data/benchmark-archive.json';");
  f.write('benchmarks/archive/README.md', 'Archive sources');
  f.write(archive, 'Original archive');
  f.write('.gitignore', `${snapshot}\n`);
  f.commit();
  const mapping = () => routeInputs(f.root, 'https://krabka.io/benchmarks/explorer/');
  const before = mapping();
  assert.ok(before.inputs.includes(archive));
  assert.ok(!before.inputs.includes(snapshot));
  assert.equal(resolve(f, before).date, '2026-10-01');
  f.write(snapshot, '{"generated":true}');
  assert.deepEqual(mapping(), before);
  f.write(archive, 'Updated archive');
  f.commit('2026-10-02T12:00:00Z');
  assert.equal(resolve(f, mapping()).date, '2026-10-02');
  fs.rmSync(path.join(f.root, snapshot));
  assert.equal(resolve(f, mapping()).date, '2026-10-02');
  f.write(source, "import missing from '../../data/unknown.json';");
  assert.throws(mapping, /unresolved-import:.*unknown\.json/);
});

test('data-driven routes have concrete tracked inputs, not unreviewed gaps or generated snapshots', () => {
  for (const route of [
    '/',
    '/benchmarks',
    '/benchmarks/explorer',
    '/verification',
    '/docs/verification-playground',
    '/docs/proof-explorer',
  ]) {
    const mapping = routeInputs(process.cwd(), 'https://krabka.io' + route);
    assert.equal(mapping.gap, undefined, route);
    assert.ok(mapping.inputs.includes(authoredRoutes[route]));
    assert.ok(
      !mapping.inputs.some(
        (file) =>
          file.includes('*') ||
          file.includes('BaseLayout') ||
          file === 'src/data/proof-sessions.json' ||
          file === 'src/data/benchmark-archive.json',
      ),
    );
  }
  assert.ok(
    routeInputs(process.cwd(), 'https://krabka.io/').inputs.includes(
      'src/pages/docs/quickstart.astro',
    ),
  );
  assert.ok(
    routeInputs(process.cwd(), 'https://krabka.io/').inputs.includes('src/data/versions.json'),
  );
  assert.ok(
    !routeInputs(process.cwd(), 'https://krabka.io/features/kraft/').inputs.includes(
      'src/data/versions.json',
    ),
  );
  assert.ok(
    routeInputs(process.cwd(), 'https://krabka.io/benchmarks/explorer').inputs.some((file) =>
      file.endsWith('.tar.gz'),
    ),
  );
  for (const route of ['/releases', '/versions'])
    assert.equal(
      routeInputs(process.cwd(), 'https://krabka.io' + route).gap,
      'unpinned-external-api-history',
    );
  assert.equal(routeInputs(process.cwd(), 'https://krabka.io/new-route'), null);
});

test('a non-Git source archive builds and reports omission instead of failing wildcard discovery', (t) => {
  const f = fixture(t);
  const archive = path.join(f.root, 'archive');
  f.write('archive/src/pages/benchmarks/explorer.astro', '<h1>Explorer</h1>');
  const dir = path.join(archive, 'dist');
  sitemaps(dir, ['https://krabka.io/benchmarks/explorer/']);
  const records = applyLastmod(dir, { root: archive });
  assert.equal(records[0].date, null);
  assert.equal(records[0].reason, 'non-git-source');
});

test('date comparison uses instants across time zones and survives rewritten/squashed history without pinned baselines', (t) => {
  const f = fixture(t);
  f.write('page.astro', '<p>Page</p>');
  f.commit();
  const upstream = (date) => ({ historyComplete: true, dirtySource: false, lastChange: { date } });
  const mapping = {
    ...mapPage('page.astro'),
    provenance: [upstream('2026-10-03T23:30:00-04:00'), upstream('2026-10-04T02:00:00Z')],
  };
  assert.equal(resolve(f, mapping).date, '2026-10-04');
  f.write('page.astro', '<p>Squashed content</p>');
  f.git('add', '.');
  f.git('commit', '--amend', '-qm', 'Squashed');
  assert.equal(resolve(f, mapPage('page.astro')).date, '2026-10-01');
});

const ns = 'http://www.sitemaps.org/schemas/sitemap/0.9';
function sitemaps(dir, urls) {
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(
    path.join(dir, 'sitemap-index.xml'),
    `<?xml version="1.0" encoding="UTF-8"?><sitemapindex xmlns="${ns}"><sitemap><loc>https://krabka.io/sitemap-website-0.xml</loc><lastmod>2026-10-01</lastmod></sitemap></sitemapindex>`,
  );
  fs.writeFileSync(
    path.join(dir, 'sitemap-website-0.xml'),
    `<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="${ns}">${urls.map((url) => `<url><loc>${url}</loc></url>`).join('')}</urlset>`,
  );
}

test('central hook preserves automatic dates through forced byte/count splits and identical rebuilds, never index dates', (t) => {
  const f = fixture(t);
  f.write('page.astro', '<h1>Content</h1>');
  f.commit();
  const urls = ['https://krabka.io/one/', 'https://krabka.io/two/', 'https://krabka.io/three/'];
  const options = { root: f.root, map: () => mapPage('page.astro') };
  const build = (name, inventory = urls) => {
    const dir = path.join(f.root, name);
    sitemaps(dir, inventory);
    const records = applyLastmod(dir, options);
    assert.equal(records.filter((entry) => entry.date).length, inventory.length);
    enforceSitemapLimits(dir, { maxBytes: 260, maxEntries: 1 });
    const indexFile = path.join(dir, 'sitemap-index.xml');
    const index = fs.readFileSync(indexFile, 'utf8');
    assert.doesNotMatch(index, /lastmod/);
    const children = readSitemap(index, 'sitemapindex', indexFile);
    assert.equal(children.length, inventory.length);
    validateXSD([
      { file: indexFile, root: 'sitemapindex' },
      ...children.map((child) => ({
        file: path.join(dir, path.basename(new URL(child).pathname)),
        root: 'urlset',
      })),
    ]);
    for (const child of children) {
      const xml = fs.readFileSync(path.join(dir, path.basename(new URL(child).pathname)), 'utf8');
      assert.match(xml, /<lastmod>2026-10-01<\/lastmod>/);
      assert.ok(Buffer.byteLength(xml) <= 260);
    }
    return Object.fromEntries(
      fs
        .readdirSync(dir)
        .sort()
        .map((file) => [file, fs.readFileSync(path.join(dir, file), 'utf8')]),
    );
  };
  assert.deepEqual(build('first'), build('second'));
  build('removed', urls.slice(0, 1));
});
