// The home page shows short excerpts of the local quickstart client snippets, which
// CI runs against a real broker (scripts/check-snippets.mjs). The excerpts are
// cut from those strings at build time rather than copied, and a pattern that
// no longer matches fails the build, so the home page cannot drift from the
// tested code.
import assert from 'node:assert/strict';

// Each excerpt is one or more segments: from the first line matching `from`
// through the first line after it matching `to` (or just that line).
export const clientExcerpts = [
  {
    id: 'java',
    label: 'Java',
    lang: 'java',
    library: (code) => code.match(/kafka-clients:(\S+)/)?.[1].replace(/^/, 'kafka-clients '),
    segments: [{ from: /^\s*Properties props = /, to: /producer\.send\(/ }],
    address: /bootstrap\.servers/,
  },
  {
    id: 'python',
    label: 'Python',
    lang: 'python',
    library: (code) => code.match(/pip install (confluent-kafka\S*)/)?.[1].replace('==', ' '),
    segments: [
      { from: /^from confluent_kafka import/ },
      { from: /^producer = Producer\(/ },
      { from: /^producer\.produce\(/, to: /^\)/ },
    ],
    drop: [/on_delivery=/],
    address: /bootstrap\.servers/,
  },
  {
    id: 'go',
    label: 'Go',
    lang: 'go',
    library: (code) => code.match(/github\.com\/twmb\/franz-go/) && 'franz-go',
    segments: [{ from: /kgo\.NewClient\(/ }, { from: /^\s*rec := /, to: /ProduceSync\(/ }],
    address: /SeedBrokers\(/,
  },
  {
    id: 'javascript',
    label: 'Node.js',
    lang: 'javascript',
    library: (code) => code.match(/npm install (kafkajs\S*)/)?.[1].replace('@', ' '),
    segments: [
      { from: /^const \{ Kafka \} = require/ },
      { from: /const kafka = new Kafka\(/, to: /await producer\.connect\(/ },
      { from: /await producer\.send\(\{/, to: /^\s*\}\);/ },
    ],
    address: /brokers:/,
  },
  {
    id: 'cli',
    label: 'CLI',
    lang: 'bash',
    library: (code) => code.match(/apache\/kafka/) && 'Apache Kafka tools',
    // One command, so the CLI tab also highlights exactly one line.
    segments: [{ from: /kafka-console-producer\.sh/ }],
    // The tested commands run the tools in a container; the excerpt shows the tool itself.
    transform: (line) => line.replace(/^.*?\/opt\/kafka\/bin\//, ''),
    address: /--bootstrap-server/,
  },
];

function dedent(lines) {
  const indent = Math.min(...lines.filter((l) => l.trim()).map((l) => l.match(/^\s*/)[0].length));
  return lines.map((l) => l.slice(indent));
}

export function cutExcerpt(code, spec) {
  const lines = code.split('\n');
  const parts = spec.segments.map(({ from, to }) => {
    const start = lines.findIndex((l) => from.test(l));
    assert(start >= 0, `${spec.id}: excerpt start ${from} no longer matches the tested snippet`);
    let end = start;
    if (to) {
      end = lines.findIndex((l, i) => i >= start && to.test(l));
      assert(end >= 0, `${spec.id}: excerpt end ${to} no longer matches the tested snippet`);
    }
    let segment = lines
      .slice(start, end + 1)
      .filter((l) => !(spec.drop ?? []).some((d) => d.test(l)));
    if (spec.transform) segment = segment.map(spec.transform);
    return dedent(segment).join('\n');
  });
  const text = parts.join('\n\n');
  const highlight = text.split('\n').flatMap((l, i) => (spec.address.test(l) ? [i + 1] : []));
  assert(highlight.length, `${spec.id}: the broker address line is missing from the excerpt`);
  return { text, highlight };
}

export function buildExcerpts(snippets) {
  return clientExcerpts.map((spec) => {
    const code = snippets[spec.id];
    assert(code, `missing tested client snippet: ${spec.id}`);
    return {
      id: spec.id,
      label: spec.label,
      lang: spec.lang,
      library: spec.library(code) || '',
      ...cutExcerpt(code, spec),
    };
  });
}
