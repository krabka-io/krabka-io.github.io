import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { test } from 'node:test';
import { syncGuides, COMPONENTS } from './sync-docs.mjs';

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

test('component READMEs and CRD schemas publish together with repository paths', (t) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'krabka-crds-test-'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const content = path.join(dir, 'content');
  const repo = path.join(dir, 'operator');
  fs.mkdirSync(path.join(repo, 'charts/crds'), { recursive: true });
  fs.mkdirSync(path.join(repo, 'demo'), { recursive: true });
  fs.writeFileSync(path.join(repo, 'README.md'), '# Operator\n[demo](demo/README.md#run) [chart](charts/values.yaml)');
  fs.writeFileSync(path.join(repo, 'demo/README.md'), '# Demo\n[home](../README.md)');
  const schema = 'kind: CustomResourceDefinition\nspec:\n  names:\n    kind: Kafka\n  versions:\n    - name: v1alpha1\n      schema:\n        openAPIV3Schema:\n          type: object\n          properties:\n            spec:\n              type: object\n';
  fs.writeFileSync(path.join(repo, 'charts/crds/kafka.yaml'), schema);
  const component = { name: 'operator', repo: 'krabka-operator', docsSubdir: 'operator', localRepoDir: repo, guides: [{ source: 'README.md', destination: 'reference.md' }, { source: 'demo/README.md', destination: 'demo.md' }], crdsDir: 'charts/crds', requiredKinds: ['Kafka'] };
  syncGuides([component], content);
  assert.equal(fs.readFileSync(path.join(content, 'operator/demo.md'), 'utf8'), '# Demo\n[home](/docs/operator/reference)');
  assert.match(fs.readFileSync(path.join(content, 'operator/reference.md'), 'utf8'), /\[demo\]\(\/docs\/operator\/demo#run\)/);
  assert.match(fs.readFileSync(path.join(content, 'operator/reference.md'), 'utf8'), /charts\/values.yaml/);
  const previous = fs.readFileSync(path.join(content, 'operator/crds.json'), 'utf8');
  const { resources } = JSON.parse(previous);
  assert.equal(resources[0]._sourcePath, 'charts/crds/kafka.yaml');
  assert.equal(resources[0].spec.versions[0].schema.openAPIV3Schema.properties.spec.type, 'object');
  fs.writeFileSync(path.join(repo, 'charts/crds/kafka.yaml'), schema.replace('kind: Kafka', 'kind: Other'));
  assert.throws(() => syncGuides([component], content), /required CRD Kafka is missing/);
  assert.equal(fs.readFileSync(path.join(content, 'operator/crds.json'), 'utf8'), previous);
  fs.writeFileSync(path.join(repo, 'charts/crds/kafka.yaml'), 'kind: CustomResourceDefinition\nspec: {}');
  assert.throws(() => syncGuides([component], content), /invalid CRD/);
  assert.equal(fs.readFileSync(path.join(content, 'operator/crds.json'), 'utf8'), previous);
  fs.writeFileSync(path.join(repo, 'charts/crds/kafka.yaml'), schema);
  fs.writeFileSync(path.join(repo, 'README.md'), ' ');
  assert.throws(() => syncGuides([component], content), /required guide README.md is empty/);
  assert.equal(fs.readFileSync(path.join(content, 'operator/crds.json'), 'utf8'), previous);
  assert.deepEqual(COMPONENTS.filter((comp) => comp.guides).map((comp) => comp.name), ['operator', 'rebalancer', 'gateway']);
});
