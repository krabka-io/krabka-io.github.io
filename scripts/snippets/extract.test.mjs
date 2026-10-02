import assert from 'node:assert/strict';
import { test } from 'node:test';
import { astroSnippets, markdownSnippets, websiteSnippets } from './extract.mjs';

test('extract the website literals, including shell escapes and release interpolation', () => {
  const snippets = websiteSnippets(process.cwd());
  assert.equal(snippets.length, 18);
  assert(snippets.find(s => s.id === 'docs/quickstart/composeCode').code.includes("printf 'hello krabka\\n'"));
  assert(snippets.find(s => s.id === 'docs/quickstart/helmCode').code.includes(' \\\n'));
  assert(!snippets.find(s => s.id.endsWith('/javaGradle')).code.includes('${'));
});

test('new or missing displayed snippets fail coverage', () => {
  const source = 'const goInstallCode = `go get example`;\nconst goCode = `package main`;\n<CodeBlock code={goInstallCode} lang="bash" /><CodeBlock code={goCode} lang="go" />';
  assert.equal(astroSnippets(source, 'docs/streams-go').length, 2);
  assert.throws(() => astroSnippets(source + '<CodeBlock code={newExample} lang="go" />', 'docs/streams-go'), /add CI coverage/);
  assert.throws(() => astroSnippets(source.replace('<CodeBlock code={goCode} lang="go" />', ''), 'docs/streams-go'), /coverage changed/);
});

test('Markdown fragments retain their language, source order and context', () => {
  const snippets = markdownSnippets('## Example\n```go\nvalue := `raw`\n```\n```shell\ngo test ./...\n```\n', 'guide');
  assert.equal(snippets.length, 2);
  assert.equal(snippets[0].code, 'value := `raw`\n');
  assert.equal(snippets[1].heading, 'Example');
  assert.throws(() => markdownSnippets('', 'guide'), /no snippets/);
});
