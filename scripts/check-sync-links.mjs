// Guards the link rewrite in sync-docs: code samples must pass through untouched,
// real relative links must still be rewritten.
import assert from 'node:assert/strict';
import { rewriteDocLinks } from './rewrite-doc-links.mjs';

const opts = { docsSubdir: 'streams-go', repo: 'krabka-streams-go', sourceDocsDir: process.cwd() };

const code = [
  '```go',
  'serde, err := schema.NewAvroSerde[Order](schemaText, cache, schema.RoleValue)',
  '```',
  '',
  'Inline `NewRowCodec[string](stringSerde{}, mem)` too.',
].join('\n');
assert.equal(rewriteDocLinks(code, opts), code, 'code must not be rewritten');

const links = 'See [serdes](serdes.md#avro) and [the tests](../tests/foo_test.go) and [site](https://x.io/a).';
assert.equal(
  rewriteDocLinks(links, opts),
  'See [serdes](/docs/streams-go/serdes#avro) and [the tests](https://github.com/krabka-io/krabka-streams-go/blob/main/tests/foo_test.go) and [site](https://x.io/a).',
);

console.log('check-sync-links: ok');
