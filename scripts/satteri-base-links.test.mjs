import assert from 'node:assert/strict';
import test from 'node:test';
import { withBase } from '../src/utils/satteri-base-links.mjs';

test('root-relative Markdown links get the site base, as getPath() gives them', () => {
  assert.equal(withBase('/', '/benchmarks#openmessaging'), '/benchmarks#openmessaging');
  assert.equal(
    withBase('/docs-preview/', '/benchmarks#openmessaging'),
    '/docs-preview/benchmarks#openmessaging',
  );
  assert.equal(withBase('/docs-preview', '/news/krabka-1-0-0'), '/docs-preview/news/krabka-1-0-0');
});

test('other links and links that already carry the base stay as they are', () => {
  for (const href of [
    'https://github.com/krabka-io',
    '//cdn.example/x',
    '#upgrading',
    'relative/page',
    undefined,
  ]) {
    assert.equal(withBase('/docs-preview/', href), href);
  }
  assert.equal(withBase('/docs-preview/', '/docs-preview/benchmarks'), '/docs-preview/benchmarks');
  assert.equal(withBase('/docs-preview/', '/docs-preview'), '/docs-preview');
});
