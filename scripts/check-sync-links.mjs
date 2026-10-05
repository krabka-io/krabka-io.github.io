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

// A shorter fence inside a longer one stays code, and an unclosed fence runs to the end.
for (const sample of ['````markdown\n```go\nx[1](a.md)\n```\n````', 'text\n```\nunclosed\nf[T](y.md)']) {
  assert.equal(rewriteDocLinks(sample, opts), sample, 'nested or unclosed fence must not be rewritten');
}

const links = 'See [serdes](serdes.md#avro) and [the tests](../tests/foo_test.go) and [site](https://x.io/a).';
assert.equal(
  rewriteDocLinks(links, opts),
  'See [serdes](/docs/streams-go/serdes#avro) and [the tests](https://github.com/krabka-io/krabka-streams-go/blob/main/tests/foo_test.go) and [site](https://x.io/a).',
);

// A rustdoc intra-doc link has nowhere to go on the site: it becomes plain code, outside code only.
const intraDoc = 'Maps to [`crate::BrokerConfig::extra_log_dirs`]. See [`x`](y.md).\n```rust\n/// [`Kept`]\n```';
assert.equal(
  rewriteDocLinks(intraDoc, opts),
  'Maps to `crate::BrokerConfig::extra_log_dirs`. See [`x`](/docs/streams-go/y).\n```rust\n/// [`Kept`]\n```',
);

console.log('check-sync-links: ok');
