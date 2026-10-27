import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { test } from 'node:test';
import { classifyPaths, changedPaths, selectChecks } from './ci-changes.mjs';
import { pages } from './snippets/extract.mjs';

const full = { site: true, lab: true, proofs: true, snippets: true };
const light = { site: true, lab: false, proofs: false, snippets: false };
const pr = {
  pull_request: { draft: false, base: { sha: 'a'.repeat(40) }, head: { sha: 'b'.repeat(40) } },
};
for (const [file, expected] of [
  ['src/pages/news/example.md', light],
  ['docs/architecture.md', light],
  ['src/styles/global.css', full],
  ['src/layouts/BaseLayout.astro', full],
  ['src/components/CodeBlock.astro', full],
  ['package-lock.json', full],
  ['package.json', full],
  ['src/data/versions.json', full],
  ['.github/workflows/deploy.yml', full],
  ['scripts/ci-changes.mjs', full],
  ['scripts/tool-pins.json', full],
  ['scripts/sync-docs.mjs', full],
  ['astro.config.mjs', full],
  ['MODULE.bazel.lock', full],
  ['unknown-input', full],
  ['public/playground/lab/world.js', { ...light, lab: true }],
  ['src/pages/docs/lab.astro', { ...light, lab: true }],
  ['playground/Cargo.lock', { ...light, lab: true, snippets: true }],
  ['public/proofs/app.js', { ...light, proofs: true }],
  ['why3-web/install.sh', { ...light, proofs: true }],
  ['src/pages/docs/proof-explorer.astro', { ...light, proofs: true }],
  ['scripts/fixtures/barrier.coma', { ...light, proofs: true }],
  ['public/quickstart/docker-compose.yml', { ...light, snippets: true }],
  ['public/charts/index.yaml', { ...light, snippets: true }],
  ['scripts/snippets/extract.mjs', { ...light, snippets: true }],
  ...Object.keys(pages).map((page) => [`src/pages/${page}.astro`, { ...light, snippets: true }]),
])
  test(`classify ${file}`, () => assert.deepEqual(classifyPaths([file]), expected));

test('empty and combined changes retain site checks and combine suites', () => {
  assert.deepEqual(classifyPaths([]), light);
  assert.deepEqual(classifyPaths(['public/playground/app.js', 'public/proofs/app.js']), {
    ...full,
    snippets: false,
  });
});
test('every non-PR event gets full coverage, including branch manual runs', () => {
  for (const event of ['push', 'schedule', 'repository_dispatch', 'workflow_dispatch', 'unknown']) {
    const { reason, ...result } = selectChecks(event, {}, () => {
      throw new Error('must not diff');
    });
    assert.deepEqual(result, full);
  }
});
test('drafts stay light; ready_for_review reselects the expensive suites', () => {
  const draft = structuredClone(pr);
  draft.pull_request.draft = true;
  const { reason, ...selected } = selectChecks('pull_request', draft, () => ['package.json']);
  assert.deepEqual(selected, light);
  const ready = { ...pr, action: 'ready_for_review' };
  assert.deepEqual(
    selectChecks('pull_request', ready, () => ['package.json']),
    { ...full, reason: 'PR changed paths' },
  );
});
test('missing metadata and diff failures select full checks, not a false green', () => {
  for (const event of [{}, pr]) {
    const { reason, ...selected } = selectChecks('pull_request', event, () => {
      throw new Error('bad diff');
    });
    assert.deepEqual(selected, full);
    assert.match(reason, /full fallback/);
  }
});
test('real merge-base diff covers deletions, renames and unusual filenames', (t) => {
  const parent = path.resolve('artifacts/tmp');
  fs.mkdirSync(parent, { recursive: true, mode: 0o700 });
  const dir = fs.mkdtempSync(path.join(parent, 'ci-diff-'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const git = (...args) => execFileSync('git', args, { cwd: dir, encoding: 'utf8' }).trim();
  git('init', '-q');
  git('config', 'user.email', 'ci@example.invalid');
  git('config', 'user.name', 'CI test');
  fs.mkdirSync(path.join(dir, 'playground'));
  fs.writeFileSync(path.join(dir, 'playground/input'), 'test');
  git('add', '.');
  git('commit', '-qm', 'base');
  const base = git('rev-parse', 'HEAD');
  fs.renameSync(path.join(dir, 'playground/input'), path.join(dir, 'renamed\nfile'));
  git('add', '-A');
  git('commit', '-qm', 'rename');
  const files = changedPaths(base, git('rev-parse', 'HEAD'), dir);
  assert.deepEqual(files.sort(), ['playground/input', 'renamed\nfile']);
  assert.deepEqual(classifyPaths(files), full); // unknown destination is conservative
  assert.throws(() => changedPaths('--bad', base, dir), /invalid PR commit/);
  assert.throws(() => changedPaths('0'.repeat(40), base, dir));
});

test('classifier CLI emits full safe outputs for a malformed event', (t) => {
  const parent = path.resolve('artifacts/tmp');
  fs.mkdirSync(parent, { recursive: true, mode: 0o700 });
  const dir = fs.mkdtempSync(path.join(parent, 'ci-event-'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const eventFile = path.join(dir, 'event.json');
  const output = path.join(dir, 'output');
  const summary = path.join(dir, 'summary');
  fs.writeFileSync(eventFile, '{broken');
  const stdout = execFileSync(process.execPath, ['scripts/ci-changes.mjs'], {
    encoding: 'utf8',
    env: {
      ...process.env,
      GITHUB_EVENT_PATH: eventFile,
      GITHUB_EVENT_NAME: 'pull_request',
      GITHUB_OUTPUT: output,
      GITHUB_STEP_SUMMARY: summary,
    },
  });
  assert.match(stdout, /full fallback/);
  assert.equal(
    fs.readFileSync(output, 'utf8'),
    'site=true\nlab=true\nproofs=true\nsnippets=true\n',
  );
  assert.match(fs.readFileSync(summary, 'utf8'), /CI selection/);
});
