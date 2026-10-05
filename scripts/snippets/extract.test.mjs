import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import { astroSnippets, markdownSnippets, websiteSnippets } from './extract.mjs';

test('extract the website literals, including shell escapes and release interpolation', () => {
  const snippets = websiteSnippets(process.cwd());
  assert.equal(snippets.length, 24);
  assert(snippets.find(s => s.id === 'docs/quickstart/composeVerifyCode').code.includes("printf 'hello krabka\\n'"));
  assert(snippets.find(s => s.id === 'docs/quickstart/helmCode').code.includes(' \\\n'));
  assert(!snippets.find(s => s.id.endsWith('/javaGradle')).code.includes('${'));
});

test('new or missing displayed snippets fail coverage', () => {
  const source = 'const goInstallCode = `go get example`;\nconst goCode = `package main`;\n<CodeBlock code={goInstallCode} lang="bash" /><CodeBlock code={goCode} lang="go" />';
  assert.equal(astroSnippets(source, 'docs/streams-go').length, 2);
  assert.throws(() => astroSnippets(source + '<CodeBlock code={newExample} lang="go" />', 'docs/streams-go'), /add CI coverage/);
  assert.throws(() => astroSnippets(source.replace('<CodeBlock code={goCode} lang="go" />', ''), 'docs/streams-go'), /coverage changed/);
});

test('startup and verification omit cleanup; the download matches the tested producer', () => {
  const snippets = websiteSnippets(process.cwd());
  for (const snippet of snippets.filter(s => s.id.startsWith('docs/quickstart/') && !s.id.includes('Cleanup'))) {
    assert(!/docker compose down|kubectl delete|helm uninstall/.test(snippet.code), `${snippet.id} includes cleanup`);
  }
  const javascript = snippets.find(s => s.id === 'get-started/clientSnippets.javascript').code;
  assert.equal(readFileSync('public/quickstart/javascript/app.cjs', 'utf8').trim(), javascript.trim());
  const manifest = JSON.parse(readFileSync('public/quickstart/javascript/package.json', 'utf8'));
  assert.equal(manifest.dependencies.kafkajs, javascript.match(/npm install kafkajs@(\S+)/)[1]);
});

test('Markdown fragments retain their language, source order and context', () => {
  const snippets = markdownSnippets('## Example\n```go\nvalue := `raw`\n```\n```shell\ngo test ./...\n```\n', 'guide');
  assert.equal(snippets.length, 2);
  assert.equal(snippets[0].code, 'value := `raw`\n');
  assert.equal(snippets[1].heading, 'Example');
  assert.throws(() => markdownSnippets('', 'guide'), /no snippets/);
});
