import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { websiteSnippets, markdownSnippets } from './snippets/extract.mjs';

const root = fileURLToPath(new URL('../', import.meta.url)).replace(/\/$/, '');
const work = `${root}/.snippets`;
const mode = process.argv[2] ?? 'check';
const project = `website-snippets-${process.pid}`;
assert(['check', 'compose', 'helm', 'rust', 'go', 'java', 'python', 'javascript'].includes(mode), `Unknown suite: ${mode}`);
mkdirSync(work, { recursive: true });
const snippets = websiteSnippets(root);
function run(command, args, cwd = work, options = {}) {
  console.log(`> ${command} ${args.join(' ')}`);
  return execFileSync(command, args, { cwd, stdio: options.input === undefined ? 'inherit' : ['pipe', 'inherit', 'inherit'], timeout: 15 * 60 * 1000, ...options });
}
function write(name, content) {
  const target = `${work}/${name}`;
  mkdirSync(dirname(target), { recursive: true });
  writeFileSync(target, content);
  return target;
}
function get(id) {
  const found = snippets.find(s => s.id === id);
  assert(found, `Missing snippet ${id}`);
  console.log(`Testing ${id}`);
  return found.code;
}
function shell(code, name, options = {}) {
  return run('bash', ['-euo', 'pipefail', write(`${name}.sh`, code)], work, options);
}
function localAssets(code) {
  return code.replaceAll('https://krabka.io/quickstart/', `${mode === 'helm' ? '' : 'file://'}${root}/public/quickstart/`);
}
async function guide(language) {
  const page = `docs/streams-${language}/getting-started`;
  // This is the same main/docs/getting-started.md that sync-docs publishes.
  const synced = `${root}/src/content/${page}.md`;
  let source;
  if (existsSync(synced)) source = readFileSync(synced, 'utf8');
  else {
    const response = await fetch(`https://raw.githubusercontent.com/krabka-io/krabka-streams-${language}/main/docs/getting-started.md`, { signal: AbortSignal.timeout(30000) });
    assert(response.ok, `${page}: HTTP ${response.status}`);
    source = await response.text();
  }
  write(`${language}/getting-started.md`, source);
  const blocks = markdownSnippets(source, page);
  snippets.push(...blocks);
  write('inventory.json', JSON.stringify(snippets, null, 2));
  return blocks;
}

// Use the tools image documented by the quickstart, including its immutable digest.
const toolsImage = get('docs/quickstart/composeCode').match(/KAFKA_TOOLS=(\S+)/)[1];
const compose = ['compose', '--project-name', project, '-f', `${work}/docker-compose.yml`];
const kafka = (tool, args, options = {}) => run('docker', ['run', '--rm', '--network', 'host', toolsImage, `/opt/kafka/bin/kafka-${tool}.sh`, '--bootstrap-server', 'localhost:9092', ...args], work, { timeout: 60000, ...options });
const readiness = `for attempt in {1..60}; do
  if docker run --rm --network host -v '${work}/admin.properties:/tmp/admin.properties:ro' '${toolsImage}' /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --command-config /tmp/admin.properties --list; then exit 0; fi
  sleep 2
done
exit 1`;
write('admin.properties', 'default.api.timeout.ms=5000\nrequest.timeout.ms=5000\n');
function waitForBroker() {
  shell(readiness, 'wait-for-broker', { timeout: 10 * 60 * 1000 });
}
function withBroker(test) {
  write('docker-compose.yml', readFileSync(`${root}/public/quickstart/docker-compose.yml`));
  try {
    run('docker', [...compose, 'up', '-d']);
    waitForBroker();
    test();
  } finally {
    run('docker', [...compose, 'logs', '--no-color']);
    run('docker', [...compose, 'down', '--volumes', '--remove-orphans']);
  }
}
function topic(name) {
  kafka('topics', ['--create', '--topic', name, '--partitions', '1', '--replication-factor', '1']);
}
function expectRecord(topicName, expected, count = 1) {
  const output = kafka('console-consumer', ['--topic', topicName, '--from-beginning', '--max-messages', String(count), '--timeout-ms', '30000'], { stdio: ['ignore', 'pipe', 'inherit'] }).toString();
  assert(output.split('\n').includes(expected), `Expected ${JSON.stringify(expected)} in ${JSON.stringify(output)}`);
}

if (mode === 'go') {
  const blocks = await guide('go');
  const install = get('docs/streams-go/goInstallCode');
  const goDir = `${work}/go`;
  write('go/go.mod', 'module website-snippets\n\ngo 1.26.0\n');
  shell(`cd '${goDir}'\n${install}\ngo get github.com/twmb/franz-go@v1.21.6`, 'go-dependencies');
  const examples = [
    ['producer', get('get-started/clientSnippets.go')],
    ['columnar', get('docs/streams-go/goCode')],
  ];
  const expected = ['Installation', 'Installation', 'A first serde', 'A first columnar topology', 'Building this repository', 'Building this repository'];
  assert.deepEqual(blocks.map(s => s.heading), expected, 'Go guide changed: update snippet coverage');
  for (const block of blocks) {
    console.log(`Testing ${block.id} (${block.heading})`);
    if (block.lang === 'shell') {
      run('bash', ['-n'], work, { input: block.code });
      if (block.heading === 'Installation') shell(`cd '${goDir}'\n${block.code}`, 'guide-go-install');
    } else if (block.heading === 'Installation') {
      // Import-only documentation fragment: blank imports check every package exists.
      examples.push(['imports', `package main\n${block.code.replace(/^(\s*)(?:krabkastreams\s+)?"/gm, '$1_ "')}\nfunc main() {}\n`]);
    } else if (block.heading === 'A first serde') {
      examples.push(['serde', `package main
import ("context"; "log"; "github.com/krabka-io/krabka-streams-go/schema"; "github.com/krabka-io/krabka-streams-go/krabkatest")
func main() {
stub, err := krabkatest.NewSchemaRegistryStub()
if err != nil { panic(err) }; defer stub.Close()
${block.code.replace('"http://localhost:8081"', 'stub.URL()')}
if err != nil || back.ID != "o-1" { panic("serde roundtrip failed") }
}
`]);
    } else if (block.heading === 'A first columnar topology') {
      examples.push(['guide-columnar', `package main
import ("log"; "github.com/apache/arrow-go/v18/arrow/memory"; "github.com/krabka-io/krabka-streams-go/columnar")
type stringSerde struct{}
func (stringSerde) Serialize(_ string, value string) ([]byte, error) { return []byte(value), nil }
func (stringSerde) Deserialize(_ string, value []byte) (string, error) { return string(value), nil }
func main() {
records := []columnar.ConsumedRecord{columnar.NewConsumedRecord([]byte("key"), []byte("hello"), 0, 0, 0)}
${block.code}
if err != nil || len(produced) != 1 || string(produced[0].Record.Value) != "hello" { panic("columnar roundtrip failed") }
}
`]);
    } else throw new Error(`Uncovered snippet ${block.id}`);
  }
  for (const [name, code] of examples) {
    write(`go/${name}/main.go`, code);
  }
  run('go', ['mod', 'tidy'], goDir);
  run('go', ['build', './...'], goDir);
  for (const name of ['columnar', 'guide-columnar', 'imports', 'serde']) run('go', ['run', `./${name}`], goDir);
  withBroker(() => {
    topic('quickstart-events');
    run('go', ['run', './producer'], goDir, { timeout: 60000 });
    expectRecord('quickstart-events', 'Hello Krabka from Go!');
  });
}

if (mode === 'rust') {
  const manifest = readFileSync(`${root}/playground/Cargo.toml`, 'utf8');
  const dependency = name => {
    const line = manifest.match(new RegExp(`^${name} = .*`, 'm'));
    assert(line, `Missing pinned ${name} dependency`);
    return line[0];
  };
  const names = ['krabka-client-producer', 'krabka-client-consumer', 'krabka-client-streams', 'krabka-units'];
  write('rust/Cargo.toml', `[package]\nname = "website-snippets"\nversion = "0.0.0"\nedition = "2024"\n[workspace]\n[dependencies]\nbytes = "1"\ntokio = { version = "1", features = ["macros", "rt-multi-thread"] }\n${names.map(dependency).join('\n')}\n${manifest.slice(manifest.indexOf('[patch.crates-io]'))}`);
  for (const [name, id] of Object.entries({ producer: 'get-started/clientSnippets.rust', compressed: 'docs/streams-rs/producerCode', consumer: 'docs/streams-rs/consumerCode', share: 'docs/streams-rs/shareConsumerCode', streams: 'docs/streams-rs/streamsCode' })) {
    write(`rust/src/bin/${name}.rs`, get(id));
  }
  write('rust/Cargo.lock', readFileSync(`${root}/playground/Cargo.lock`));
  const rustDir = `${work}/rust`;
  run('cargo', ['build', '--bins'], rustDir);
  run('cargo', ['run', '--bin', 'streams'], rustDir);
  withBroker(() => {
    topic('quickstart-events'); topic('events');
    run('cargo', ['run', '--bin', 'producer'], rustDir, { timeout: 60000 });
    expectRecord('quickstart-events', 'Hello Krabka from Rust!');
    run('cargo', ['run', '--bin', 'compressed'], rustDir, { timeout: 60000 });
    expectRecord('events', 'hello krabka');
  });
}

if (mode === 'java') {
  const blocks = await guide('java');
  assert.deepEqual(blocks.map(s => s.lang), ['kotlin', 'xml', 'java', 'java', 'java', 'text', 'shell'], 'Java guide changed: update snippet coverage');
  // Use Gradle to resolve the actual BOM snippets, and Maven for the XML fragment.
  // The runner needs Gradle and Maven on PATH; CI installs both through its JDK/Gradle setup.
  for (const [name, dependencies] of [['site', get('docs/streams-java/javaGradle')], ['guide', blocks[0].code]]) {
    const javaDir = `${work}/java/${name}`;
    write(`java/${name}/settings.gradle.kts`, 'rootProject.name = "website-snippets"\n');
    write(`java/${name}/build.gradle.kts`, `plugins { java }\nrepositories { mavenCentral() }\n${dependencies}\n`);
    write(`java/${name}/src/main/java/App.java`, get('get-started/clientSnippets.java'));
    if (name === 'guide') {
      for (const [index, block] of blocks.entries()) {
        if (block.lang !== 'java') continue;
        const imports = [...block.code.matchAll(/^import .*;$/gm)].map(m => m[0]).join('\n');
        write(`java/${name}/original-${index}.java.txt`, block.code);
        let body = block.code.replace(/^import .*;\s*$/gm, '');
        if (index === 2) body = body.replace('streams.start();', 'streams.close();') + `\ntry (var driver = new org.apache.kafka.streams.TopologyTestDriver(builder.build(), new java.util.Properties() {{ putAll(settings); }})) {\n  var input = driver.createInputTopic("orders", Serdes.String().serializer(), Serdes.String().serializer());\n  var output = driver.createOutputTopic("order-counts", Serdes.String().deserializer(), Serdes.Long().deserializer());\n  input.pipeInput("key", "drop"); input.pipeInput("key", "keep one"); input.pipeInput("key", "keep two");\n  if (!output.readValuesToList().equals(java.util.List.of(1L, 2L))) throw new AssertionError("streams output");\n}\n`;
        if (index === 3) body = 'try (var registry = new io.krabka.streams.test.SchemaRegistryStub()) {\n' + body.replace('URI.create("http://localhost:8081")', 'registry.uri()') + '\n}';
        write(`java/${name}/src/test/java/Example${index}.java`, `${imports}\npublic class Example${index} { public static void main(String[] args) throws Exception {\n${body}\n} }\n`);
      }
    }
    run('gradle', ['--no-daemon', 'classes', 'testClasses'], javaDir);
    // Export the runtime classpath without inventing dependency resolution in the harness.
    write(`java/${name}/classpath.gradle`, `allprojects { tasks.register('snippetClasspath') { doLast { println(sourceSets.${name === 'guide' ? 'test' : 'main'}.runtimeClasspath.asPath) } } }\n`);
    const cp = run('gradle', ['--no-daemon', '-q', '-I', 'classpath.gradle', 'snippetClasspath'], javaDir, { stdio: ['ignore', 'pipe', 'inherit'] }).toString().trim();
    if (name === 'guide') for (const example of ['Example2', 'Example3', 'Example4']) run('java', [blocks[5].code.trim(), '-cp', cp, example]);
    if (name === 'site') withBroker(() => {
      topic('quickstart-events');
      run('java', ['-cp', cp, 'App'], javaDir, { timeout: 60000 });
      expectRecord('quickstart-events', 'Hello Krabka from Java!');
    });
  }
  write('java/maven/pom.xml', `<project xmlns="http://maven.apache.org/POM/4.0.0"><modelVersion>4.0.0</modelVersion><groupId>website</groupId><artifactId>snippets</artifactId><version>0</version><dependencies>${blocks[1].code}</dependencies></project>`);
  run('mvn', ['-B', 'dependency:resolve'], `${work}/java/maven`);
  run('bash', ['-n'], work, { input: blocks.at(-1).code });
}

if (mode === 'python' || mode === 'javascript') {
  const code = get(`get-started/clientSnippets.${mode}`);
  const dir = `${work}/${mode}`;
  let command, args;
  if (mode === 'python') {
    const file = write('python/app.py', code);
    run('python3', ['-m', 'venv', `${dir}/venv`]);
    const dependency = code.match(/pip install (\S+)/)[1];
    run(`${dir}/venv/bin/python`, ['-m', 'pip', 'install', dependency]);
    command = `${dir}/venv/bin/python`; args = [file];
  } else {
    const file = write('javascript/app.cjs', code);
    write('javascript/package.json', '{"private":true}\n');
    run('npm', ['install', '--no-audit', '--no-fund', code.match(/npm install (\S+)/)[1]], dir);
    command = 'node'; args = [file];
  }
  withBroker(() => {
    topic('quickstart-events');
    run(command, args, dir, { timeout: 60000 });
    expectRecord('quickstart-events', `Hello Krabka from ${mode === 'python' ? 'Python' : 'KafkaJS'}!`);
  });
}

if (mode === 'compose') {
  // Docker Compose is forced onto an isolated project. It never touches a developer's stack.
  write('docker-compose.yml', readFileSync(`${root}/public/quickstart/docker-compose.yml`));
  const env = { ...process.env, COMPOSE_PROJECT_NAME: project };
  try {
    run('docker', [...compose, 'config', '--quiet']);
    // The documentation starts the stack before using Kafka. Poll at that boundary.
    const quickstart = localAssets(get('docs/quickstart/composeCode'))
      .replace('docker compose up -d', 'docker compose up -d\nbash wait-for-broker.sh');
    write('wait-for-broker.sh', readiness);
    const output = shell(quickstart, 'compose-quickstart', { env, stdio: ['ignore', 'pipe', 'inherit'] }).toString();
    assert(output.split('\n').includes('hello krabka'), 'Compose quickstart did not roundtrip its record');
    shell(localAssets(get('get-started/deploySnippets.docker')), 'get-started-docker', { env });
    waitForBroker();
    // Supply stdin and a bound for the two interactive CLI examples.
    for (const tool of ['topics', 'console-producer', 'console-consumer', 'metadata-quorum', 'consumer-groups', 'broker-api-versions']) {
      const path = write(`bin/kafka-${tool}.sh`, `#!/usr/bin/env bash\nexec docker run --rm -i --network host '${toolsImage}' /opt/kafka/bin/kafka-${tool}.sh \"$@\"\n`);
      run('chmod', ['+x', path]);
    }
    process.env.PATH = `${work}/bin:${process.env.PATH}`;
    const cli = get('get-started/clientSnippets.cli').split('\n');
    shell(cli[0], 'cli-create');
    shell(cli[1], 'cli-produce', { input: 'website CLI roundtrip\n' });
    const consumed = shell(`${cli[2]} --max-messages 1 --timeout-ms 30000`, 'cli-consume', { stdio: ['ignore', 'pipe', 'inherit'], timeout: 60000 }).toString();
    assert(consumed.split('\n').includes('website CLI roundtrip'), 'CLI snippet did not roundtrip its record');
    shell(get('get-started/inspectSnippet'), 'inspect', { timeout: 120000 });
  } finally {
    run('docker', [...compose, 'logs', '--no-color']);
    run('docker', [...compose, 'down', '--volumes', '--remove-orphans']);
  }
}

if (mode === 'helm') {
  const env = { ...process.env, KUBECONFIG: `${work}/kubeconfig` };
  try {
    run('kind', ['create', 'cluster', '--name', project, '--kubeconfig', env.KUBECONFIG, '--wait', '120s'], work, { env });
    const result = shell(localAssets(get('docs/quickstart/helmCode')), 'helm-quickstart', { env, stdio: ['ignore', 'pipe', 'inherit'], timeout: 25 * 60 * 1000 }).toString();
    assert(result.split('\n').includes('hello krabka'), 'Helm quickstart did not roundtrip its record');
    shell(localAssets(get('get-started/deploySnippets.helm')), 'get-started-helm', { env, timeout: 20 * 60 * 1000 });
    run('kubectl', ['wait', '--for=condition=Ready', 'kafkatopic/quickstart', '--timeout=5m'], work, { env });
  } finally {
    run('kind', ['export', 'logs', `${work}/kind-logs`, '--name', project], work, { env });
    run('kind', ['delete', 'cluster', '--name', project], work, { env });
  }
}

if (mode === 'check') {
  await guide('java'); await guide('go');
}
for (const snippet of snippets) {
  console.log(`${snippet.id} (${snippet.lang})`);
  if (['bash', 'shell'].includes(snippet.lang)) run('bash', ['-n'], work, { input: snippet.code });
  assert(['bash', 'shell', 'rust', 'go', 'java', 'kotlin', 'xml', 'text', 'python', 'javascript'].includes(snippet.lang), `Uncovered language: ${snippet.id}`);
}
write('inventory.json', JSON.stringify(snippets, null, 2));
console.log(`PASS: ${mode} (${snippets.length} snippets inventoried)`);
