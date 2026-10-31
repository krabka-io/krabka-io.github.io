import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import { composition, checkLock } from './site-archives.mjs';

const catalog = JSON.parse(
  fs.readFileSync(new URL('../src/data/api-reference.json', import.meta.url)),
);

test('composition covers the site and catalog, mounting Java at its repository root', () => {
  const result = composition(catalog);
  assert.equal(result.components.length, catalog.length + 1);
  assert.equal(result.components[0].site_url, 'https://krabka.io/');
  assert.deepEqual(result.components.find(({ id }) => id === 'cli').render_pages, [
    'index.html',
    'krabka-cli/krabka_cli/index.html',
  ]);
  assert.deepEqual(
    result.components.find(({ id }) => id === 'streams-java'),
    {
      id: 'streams-java',
      site_url: 'https://krabka.io/krabka-streams-java/',
      repo: 'krabka-io/krabka-streams-java',
      render_pages: ['api/index.html'],
    },
  );
});

test('reject ambiguous mounts and paths outside the declared archive', () => {
  assert.throws(() => composition([catalog[0], { ...catalog[0], id: 'other' }]), /mount/);
  assert.throws(() => composition([{ ...catalog[0], apiPath: '/elsewhere/' }]), /mount/);
  assert.throws(
    () => composition([{ ...catalog[0], apiPath: '/krabka-broker/../' }]),
    /entry path/,
  );
  assert.throws(() => composition([{ ...catalog[0], id: 'site' }]), /id/);
});

test('lock requires the entire catalog and exact routes independently of artifact pins', () => {
  const desired = composition(catalog);
  const locked = structuredClone(desired);
  for (const entry of locked.components) entry.run_id = 123;
  locked.components.reverse();
  checkLock(desired, locked);
  for (const mutate of [
    (doc) => doc.components.pop(),
    (doc) => doc.components.push(doc.components[0]),
    (doc) => {
      doc.components[0].site_url = 'https://krabka.io/wrong/';
    },
    (doc) => {
      doc.components[0].render_pages = [];
    },
    (doc) => {
      doc.components[0].repo = 'other/repo';
    },
  ]) {
    const invalid = structuredClone(locked);
    mutate(invalid);
    assert.throws(() => checkLock(desired, invalid), /differ/);
  }
});

test('extra render pages are catalog-driven and archive-relative', () => {
  const entry = { ...catalog[0], extraRenderPages: ['crate/type.html'] };
  assert.deepEqual(composition([entry]).components[1].render_pages, [
    'index.html',
    'crate/type.html',
  ]);
  for (const extraRenderPages of [
    'index.html',
    [null],
    ['../index.html'],
    ['/index.html'],
    ['https://elsewhere/index.html'],
  ]) {
    assert.throws(() => composition([{ ...entry, extraRenderPages }]), /extra render/);
  }
});
