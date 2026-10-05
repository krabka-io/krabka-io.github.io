import test from 'node:test';
import assert from 'node:assert/strict';
import { addHeadingIds } from '../src/utils/heading-ids.mjs';

test('headings without an id get a unique one from their text', () => {
  const html = '<h2 id="x">A</h2><h2>Layering Principles</h2><h3 class="c">A &amp; B <code>x</code></h3><h2>Layering Principles</h2><h4 id="layering-principles-3">z</h4><h2>Layering Principles</h2>';
  const out = addHeadingIds(html);
  assert.match(out, /<h2 id="x">A<\/h2>/);
  assert.match(out, /<h2 id="layering-principles">Layering/);
  assert.match(out, /<h3 id="a-b-x" class="c">/);
  assert.match(out, /<h2 id="layering-principles-2">/);
  // An id already on the page is never reused.
  assert.match(out, /<h2 id="layering-principles-4">/);
});
