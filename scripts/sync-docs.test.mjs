import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { test } from 'node:test';
import { syncGuides } from './sync-docs.mjs';

test('nested guides are staged together; a missing required guide preserves previous content', (t) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'krabka-docs-test-'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const content = path.join(dir, 'content');
  const component = (name) => ({ name, repo: name, docsSubdir: name, localRepoDir: path.join(dir, name), required: ['operations/restore.md'] });
  const components = [component('broker'), component('streams')];
  for (const comp of components) {
    fs.mkdirSync(path.join(comp.localRepoDir, 'docs/operations'), { recursive: true });
    fs.writeFileSync(path.join(comp.localRepoDir, 'docs/operations/restore.md'), '# Restore\n[config](../config.md)');
    fs.writeFileSync(path.join(comp.localRepoDir, 'docs/config.md'), '# Config');
    fs.writeFileSync(path.join(comp.localRepoDir, 'docs/index.md'), '# Excluded home');
    fs.mkdirSync(path.join(comp.localRepoDir, 'docs/style_guides'));
    fs.writeFileSync(path.join(comp.localRepoDir, 'docs/style_guides/writing.md'), '# Contributor guide');
  }
  syncGuides(components, content);
  const previous = fs.readFileSync(path.join(content, 'broker/operations/restore.md'), 'utf8');
  const provenance = fs.readFileSync(path.join(content, 'sources.json'), 'utf8');
  assert.match(previous, /\[config\]\(\/docs\/broker\/config\)/);
  assert.equal(fs.existsSync(path.join(content, 'broker/index.md')), false);
  assert.equal(fs.existsSync(path.join(content, 'broker/style_guides')), false);
  fs.writeFileSync(path.join(components[0].localRepoDir, 'docs/operations/restore.md'), '# New content');
  fs.rmSync(path.join(components[1].localRepoDir, 'docs/operations/restore.md'));
  assert.throws(() => syncGuides(components, content), /required guide operations\/restore.md is missing or empty/);
  assert.equal(fs.readFileSync(path.join(content, 'broker/operations/restore.md'), 'utf8'), previous);
  assert.equal(fs.readFileSync(path.join(content, 'sources.json'), 'utf8'), provenance);
  fs.writeFileSync(path.join(components[1].localRepoDir, 'docs/operations/restore.md'), '   ');
  assert.throws(() => syncGuides(components, content), /missing or empty/);
});
