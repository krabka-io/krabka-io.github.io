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

const nested = { docsSubdir: 'broker', repo: 'krabka-broker', sourceFile: 'operations/runbooks/restore.md', sourceRef: 'abc123' };
assert.equal(
  rewriteDocLinks('[backup](../backup-restore.md#recover) [config](../../config-reference.md) [code](../../../crates/broker.rs) ![diagram](../figures/flow.svg) [guide](../KIP_MATRIX.md)', nested),
  '[backup](/docs/broker/operations/backup-restore#recover) [config](/docs/broker/config-reference) [code](https://github.com/krabka-io/krabka-broker/blob/abc123/crates/broker.rs) ![diagram](https://raw.githubusercontent.com/krabka-io/krabka-broker/abc123/docs/operations/figures/flow.svg) [guide](/docs/broker/operations/kip_matrix)',
);
assert.equal(rewriteDocLinks('[site](/docs/quickstart) [external](//example.com/a) [up](../../index.md)', nested), '[site](/docs/quickstart) [external](//example.com/a) [up](/docs/broker)');
assert.throws(() => rewriteDocLinks('[escape](../../../../secret)', nested), /leaves the repository/);
assert.equal(rewriteDocLinks('[design](../kfcs/proposal.md)', { ...nested, sourceFile: 'operations/backup.md', publishedFiles: new Set(['operations/backup.md']) }), '[design](https://github.com/krabka-io/krabka-broker/blob/abc123/docs/kfcs/proposal.md)');
assert.equal(rewriteDocLinks('[missing](runbooks/missing.md)', { ...nested, sourceFile: 'operations/backup.md', publishedFiles: new Set(['operations/backup.md']) }), '[missing](/docs/broker/operations/runbooks/missing)');

console.log('check-sync-links: ok');
