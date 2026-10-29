import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import { buildExcerpts } from '../../src/utils/client-excerpts.mjs';
import { astroSnippets, markdownSnippets, websiteSnippets } from './extract.mjs';

test('extract the website literals, including shell escapes and release interpolation', () => {
  const snippets = websiteSnippets(process.cwd());
  assert.equal(snippets.length, 24);
  assert(
    snippets
      .find((s) => s.id === 'docs/quickstart/composeVerifyCode')
      .code.includes("printf 'hello krabka\\n'"),
  );
  assert(
    snippets
      .find((s) => s.id === 'docs/kubernetes-quickstart/helmInstallCode')
      .code.includes(' \\\n'),
  );
  assert(!snippets.find((s) => s.id.endsWith('/javaGradle')).code.includes('${'));
  const versions = JSON.parse(readFileSync('src/data/versions.json', 'utf8'));
  const standalone = snippets.find((s) => s.id === 'docs/broker/runCommand').code;
  assert.equal(
    standalone.split(`ghcr.io/krabka-io/krabka-broker:v${versions.broker.version}`).length - 1,
    2,
  );
  assert(!standalone.includes(':latest') && !standalone.includes('${'));
  const helm = snippets.find((s) => s.id === 'docs/kubernetes-quickstart/helmInstallCode').code;
  assert(helm.includes(`--version ${versions.operator.version} --skip-crds`));
  assert(!helm.includes('--set image.') && !helm.includes('--set-string image.'));
  assert(snippets.find((s) => s.id === 'docs/quickstart/inspectSnippet'));
  assert(!snippets.some((s) => s.id.startsWith('get-started/')));
  assert.equal(
    snippets.filter((s) => s.id.startsWith('docs/quickstart/clientSnippets.')).length,
    5,
  );
});

test('new or missing displayed snippets fail coverage', () => {
  const source =
    'const goInstallCode = `go get example`;\nconst goCode = `package main`;\n<CodeBlock code={goInstallCode} lang="bash" /><CodeBlock code={goCode} lang="go" />';
  assert.equal(astroSnippets(source, 'docs/streams-go').length, 2);
  assert.throws(
    () => astroSnippets(source + '<CodeBlock code={newExample} lang="go" />', 'docs/streams-go'),
    /add CI coverage/,
  );
  assert.throws(
    () =>
      astroSnippets(source.replace('<CodeBlock code={goCode} lang="go" />', ''), 'docs/streams-go'),
    /coverage changed/,
  );
});

test('startup and verification omit cleanup; the download matches the tested producer', () => {
  const snippets = websiteSnippets(process.cwd());
  for (const snippet of snippets.filter(
    (s) => /^docs\/(quickstart|kubernetes-quickstart)\//.test(s.id) && !s.id.includes('Cleanup'),
  )) {
    assert(
      !/docker compose down|kubectl delete|helm uninstall/.test(snippet.code),
      `${snippet.id} includes cleanup`,
    );
  }
  const javascript = snippets.find(
    (s) => s.id === 'docs/quickstart/clientSnippets.javascript',
  ).code;
  assert.equal(
    readFileSync('public/quickstart/javascript/app.cjs', 'utf8').trim(),
    javascript.trim(),
  );
  const manifest = JSON.parse(readFileSync('public/quickstart/javascript/package.json', 'utf8'));
  assert.equal(manifest.dependencies.kafkajs, javascript.match(/npm install kafkajs@(\S+)/)[1]);
});

test('Markdown fragments retain their language, source order and context', () => {
  const snippets = markdownSnippets(
    '## Example\n```go\nvalue := `raw`\n```\n```shell\ngo test ./...\n```\n',
    'guide',
  );
  assert.equal(snippets.length, 2);
  assert.equal(snippets[0].code, 'value := `raw`\n');
  assert.equal(snippets[1].heading, 'Example');
  assert.throws(() => markdownSnippets('', 'guide'), /no snippets/);
});

test('split Helm startup retains command order and the round trip retains shell semantics', () => {
  const snippets = websiteSnippets(process.cwd());
  const get = (name) => snippets.find((s) => s.id === `docs/kubernetes-quickstart/${name}`).code;
  const install = get('helmInstallCode');
  const cluster = get('helmClusterCode');
  assert(install.startsWith('kubectl apply -f https://krabka.io/quickstart/crds.yaml'));
  assert(
    install.endsWith(
      'kubectl rollout status deployment/krabka-operator-krabka-operator -n krabka-system --timeout=5m',
    ),
  );
  assert(cluster.startsWith('kubectl apply -f https://krabka.io/quickstart/krabka-cluster.yaml'));
  assert(cluster.endsWith('kubectl wait --for=condition=Ready kafkatopic/quickstart --timeout=5m'));
  const verify = get('helmVerifyCode');
  // Capture the real bash -lc argument after heredoc expansion without contacting a cluster.
  const args = execFileSync('bash', ['-c', 'kubectl() { printf \'%s\\0\' "$@"; };\n' + verify], {
    encoding: 'utf8',
  })
    .split('\0')
    .slice(0, -1);
  assert.deepEqual(args.slice(0, 2), ['run', 'kafka-tools']);
  assert.deepEqual(args.slice(-3, -1), ['/bin/bash', '-lc']);
  const script = args.at(-1);
  assert(script.includes('--topic quickstart &&'));
  assert(script.includes('--max-messages 1 --timeout-ms 30000'));
  assert(!/set -|pipefail/.test(script));
  assert(script.includes("printf 'hello krabka\\n'"));
});

test('home excerpts use the five application sources and the tested Step 3 CLI round trip', () => {
  const snippets = websiteSnippets(process.cwd());
  const clients = Object.fromEntries(
    snippets
      .filter((s) => s.id.includes('/clientSnippets.'))
      .map((s) => [s.id.split('.').at(-1), s.code]),
  );
  assert(!('cli' in clients));
  const cli = snippets.find((s) => s.id.endsWith('/composeVerifyCode')).code;
  const excerpts = buildExcerpts({ ...clients, cli });
  assert.deepEqual(
    excerpts.map((e) => e.id),
    ['java', 'python', 'go', 'javascript', 'cli'],
  );
  assert(excerpts.find((e) => e.id === 'java').text.includes(')).get();'));
  assert(
    excerpts.find((e) => e.id === 'go').text.includes('Value: []byte("Hello Krabka from Go!")'),
  );
  assert(excerpts.find((e) => e.id === 'cli').text.includes('--bootstrap-server localhost:9092'));
});
