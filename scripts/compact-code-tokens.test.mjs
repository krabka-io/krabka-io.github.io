import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import { createHighlighter } from 'shiki';
import theme from '../src/utils/krabka-shiki-theme.mjs';
import compactCodeTokens from '../src/utils/compact-code-tokens.mjs';
import { websiteSnippets } from './snippets/extract.mjs';

// Compare each character and its inherited style, not span boundaries: merging
// same-style spans must leave the source and highlighting unchanged.
function styledCharacters(node, inherited = '') {
  if (node.type === 'text') return [...node.value].map((char) => [char, inherited]);
  const classes = [node.properties?.className ?? node.properties?.class ?? []].flat().join(' ').split(/\s+/);
  const tone = classes.find((name) => name.startsWith('tok-'));
  const style = node.properties?.style ?? (tone ? `color:var(--code-${tone.slice(4)})` : inherited);
  return (node.children ?? []).flatMap((child) => styledCharacters(child, style));
}

test('shared token classes preserve source and highlighting for every authored snippet', async () => {
  const snippets = websiteSnippets(process.cwd());
  const highlighter = await createHighlighter({
    themes: [theme], langs: [...new Set(snippets.map(({ lang }) => lang))],
  });
  let originalBytes = 0;
  let compactBytes = 0;
  const css = readFileSync('src/styles/custom.css', 'utf8');
  try {
    for (const { id, code, lang } of snippets) {
      const options = { lang, theme: theme.name };
      const original = highlighter.codeToHast(code, options);
      const compact = highlighter.codeToHast(code, { ...options, transformers: [compactCodeTokens()] });
      assert.deepEqual(styledCharacters(compact), styledCharacters(original), id);
      const html = highlighter.codeToHtml(code, { ...options, transformers: [compactCodeTokens()] });
      for (const [, tone] of html.matchAll(/class="tok-([a-z]+)"/g)) {
        assert(css.includes(`.code-block .tok-${tone} { color: var(--code-${tone}); }`), tone);
      }
      originalBytes += Buffer.byteLength(highlighter.codeToHtml(code, options));
      compactBytes += Buffer.byteLength(html);
    }
  } finally {
    highlighter.dispose();
  }
  assert(compactBytes < originalBytes, 'shared rendering must reduce generated markup');
});

test('unknown and compound token styles stay inline', () => {
  const transformer = compactCodeTokens();
  for (const style of ['color:red', 'color:var(--code-future)', 'color:var(--code-keyword);font-style:italic']) {
    const node = { properties: { style } };
    transformer.span.call({ addClassToHast() { assert.fail('unknown style was converted'); } }, node);
    assert.equal(node.properties.style, style);
  }
});
