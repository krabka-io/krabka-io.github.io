import assert from 'node:assert/strict';
import { highlightWhy } from '../public/proofs/highlight.js';

// `let%span` and friends are one keyword, not `let`, `%` and a plain `span`.
for (const kw of ['let%span', 'val%foo', 'axiom%bar']) {
  assert.ok(highlightWhy(`${kw} x`).includes(`<span class="px-hl-keyword">${kw}</span>`), kw);
}
assert.ok(highlightWhy('let x = 1').includes('<span class="px-hl-keyword">let</span>'));

// The newline that ends a file does not add an empty numbered line.
assert.equal(highlightWhy('a\nb\n').split('\n').length, 2);
assert.equal(highlightWhy('a\nb').split('\n').length, 2);

// Text is escaped, and a comment keeps its colour across lines.
assert.ok(highlightWhy('<b>').includes('&lt;'));
assert.equal(highlightWhy('(* a\nb *)').match(/px-hl-comment/g).length, 2);
