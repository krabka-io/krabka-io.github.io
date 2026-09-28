// End-to-end check of the real krabka-broker in the Cluster Lab (`/docs/lab`),
// in headless Chromium.
//
// `check-lab-external` runs the WASI test guest in a real broker's place; this
// check runs the broker itself: `playground/broker-wasi`, the real
// `krabka-broker` built for wasm32-wasip1 by `npm run build:broker`. It
// builds the module (a no-op for cargo when it is fresh), serves the built
// site from `dist/` with the module at `/playground/broker/krabka-broker.wasm`
// and without COOP/COEP headers (the lab's service worker provides isolation,
// as on GitHub Pages), and checks that:
//
// - one real broker boots: its process reaches `running` with the contract's
//   environment, formats its volume and serves on its virtual address;
// - three real broker voters form a KRaft quorum and serve, and the lab's
//   admin node creates a topic with three replicas on them;
// - the lab's own producer and consumer nodes, the simulated Kafka clients,
//   produce to that topic and consume it back, through a classic consumer
//   group and a KIP-848 one;
// - the inspector shows the real brokers' metadata, as the clients got it
//   from them (brokers, controller, cluster id, partition leaders and ISR);
// - killing the leader of a partition moves that leadership while the
//   consumer keeps consuming, and a restart brings the broker back on its
//   volume (it boots in Rejoin mode) and into every ISR;
// - a page reload restores the real brokers from their IndexedDB volumes, and
//   the records are still there: a new group reads every one of them from
//   offset 0, and both groups resume from their committed offsets;
// - no page errors.
//
// The killed broker coordinates neither group: the first active controller
// creates `__consumer_offsets` as it boots, with as many replicas as brokers
// had registered by then (Kafka waits for `offsets.topic.replication.factor`
// brokers), so a cold start of three voters puts every partition of it on one
// broker, and its groups have no coordinator to fail over to. The classic member leaves its group before the reload, as an
// application does when it shuts down: a classic member that stays is
// refused when it comes back (INCONSISTENT_GROUP_PROTOCOL), because the
// broker replays the group's members without their protocols. The fault part
// runs the lab at 5x, which the processes keep up with. Timings are printed
// with the results.
//
// Usage:  npm run build && npm run check-real-broker [-- --headed] [--no-build] [--kafkactl]
// Needs what `npm run build:broker` needs (cargo, the wasm32-wasip1 target,
// clang and a WASI sysroot), Playwright (`playwright` or `playwright-core`,
// local or global) and a Chromium, found as `check-lab` finds it.
// `--no-build` takes the module staged in public/playground/broker/ as it is.
// Exits 2 when a tool or the module is missing, 1 when a check fails.

import crypto from 'crypto';
import fs from 'fs';
import http from 'http';
import os from 'os';
import path from 'path';
import { createRequire } from 'module';
import { execSync, spawn, spawnSync } from 'child_process';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const DIST_DIR = path.join(ROOT, 'dist');
const BUILD_SCRIPT = path.join(ROOT, 'playground', 'broker-wasi', 'build.sh');
const STAGED_MODULE = path.join(ROOT, 'public', 'playground', 'broker', 'krabka-broker.wasm');
const MODULE_URL = '/playground/broker/krabka-broker.wasm';
const args = new Set(process.argv.slice(2));
const HEADLESS = !args.has('--headed');
const BUILD = !args.has('--no-build');
// Wall-clock limits: a first boot downloads and compiles the module, a
// cluster change waits for Kafka's timeouts in lab time.
const BOOT_TIMEOUT = 180_000;
const STEP_TIMEOUT = 120_000;
// How fast the lab runs while the cluster rides out a fault.
const FAULT_SPEED = 5;
const TOPIC = 'orders';
const PARTITIONS = 3;
const KAFKACTL = path.join(ROOT, 'kafkactl-lab', process.platform === 'win32' ? 'kafkactl.exe' : 'kafkactl');
process.env.PLAYWRIGHT_BROWSERS_PATH ??= '/opt/pw-browsers';

// ---- the scenarios ------------------------------------------------------------------------------

const realBroker = (id, x) => ({ id, kind: 'krabka-broker', name: `real-${id}`, x, y: 90, config: {} });

const ONE_BROKER = {
  version: 1,
  seed: 11,
  name: 'One real broker',
  links: { default_latency_ms: 5 },
  nodes: [realBroker(1, 200)],
  topics: [],
};

// Three voters, a producer and a classic consumer group member. The topic
// comes from the lab's admin node once the brokers serve (`createTopic`): the
// world builds a scenario's own `topics` with simulated brokers only, and the
// lab's clients never ask a broker to create a topic.
const THREE_BROKERS = {
  version: 1,
  seed: 12,
  name: 'Three real brokers',
  links: { default_latency_ms: 5 },
  nodes: [
    realBroker(1, 120),
    realBroker(2, 360),
    realBroker(3, 600),
    {
      id: 4,
      kind: 'producer',
      name: 'orders-producer',
      x: 120,
      y: 330,
      config: { bootstrap: [1, 2, 3], topic: TOPIC, rate_per_sec: 10, acks: -1, key: { pattern: 'customer-{seq % 10}' } },
    },
    {
      id: 5,
      kind: 'consumer',
      name: 'billing',
      x: 600,
      y: 330,
      config: { bootstrap: [1, 2, 3], group: 'billing', topics: [TOPIC], auto_offset_reset: 'earliest' },
    },
  ],
  topics: [],
};

// ---- the module ---------------------------------------------------------------------------------

function buildModule() {
  const result = spawnSync('bash', [BUILD_SCRIPT], { cwd: ROOT, stdio: 'inherit' });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`${path.relative(ROOT, BUILD_SCRIPT)} exited with ${result.status}`);
}

// ---- Playwright and Chromium, as check-lab finds them ------------------------------------------

async function loadPlaywright() {
  const require = createRequire(import.meta.url);
  const candidates = ['playwright', 'playwright-core'];
  try {
    const globalRoot = execSync('npm root -g', { encoding: 'utf8' }).trim();
    candidates.push(path.join(globalRoot, 'playwright'), path.join(globalRoot, 'playwright-core'));
  } catch {
    // No npm on the path; the local package is the only candidate.
  }
  for (const c of candidates) {
    try {
      return require(c);
    } catch {
      // try the next
    }
  }
  return null;
}

function installedChromium() {
  const base = process.env.PLAYWRIGHT_BROWSERS_PATH;
  if (!base || !fs.existsSync(base)) return undefined;
  const builds = [['chromium', ['chrome-linux/chrome', 'chrome-linux64/chrome', 'chrome-mac/Chromium.app/Contents/MacOS/Chromium', 'chrome-win/chrome.exe']]];
  if (HEADLESS) builds.unshift(['chromium_headless_shell', ['chrome-headless-shell-linux64/chrome-headless-shell', 'chrome-linux/headless_shell']]);
  const entries = fs.readdirSync(base);
  for (const [name, executables] of builds) {
    const pattern = new RegExp(`^${name}-(\\d+)$`);
    const dirs = entries.filter((d) => pattern.test(d)).sort((a, b) => Number(b.match(pattern)[1]) - Number(a.match(pattern)[1]));
    for (const dir of dirs) {
      for (const executable of executables) {
        const candidate = path.join(base, dir, executable);
        if (fs.existsSync(candidate)) return candidate;
      }
    }
  }
  return undefined;
}

async function launchChromium(pw) {
  try {
    return await pw.chromium.launch({ headless: HEADLESS });
  } catch (err) {
    const executablePath = installedChromium();
    if (!executablePath) throw err;
    return pw.chromium.launch({ headless: HEADLESS, executablePath });
  }
}

// ---- the site: dist/ and the module, without isolation headers ----------------------------------

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.wasm': 'application/wasm',
  '.json': 'application/json',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.ico': 'image/x-icon',
  '.woff2': 'font/woff2',
  '.txt': 'text/plain',
  '.xml': 'application/xml',
};

// The module is served from where `build:broker` staged it, so the check runs
// the build it just made whatever the last site build copied into dist/.
function serve(dir, moduleFile) {
  const server = http.createServer((req, res) => {
    const p = decodeURIComponent(new URL(req.url, 'http://x').pathname);
    let file = p === MODULE_URL ? moduleFile : path.join(dir, p);
    if (file !== moduleFile && !file.startsWith(dir)) {
      res.writeHead(403).end();
      return;
    }
    if (fs.existsSync(file) && fs.statSync(file).isDirectory()) file = path.join(file, 'index.html');
    else if (!fs.existsSync(file) && fs.existsSync(`${file}.html`)) file = `${file}.html`;
    if (!fs.existsSync(file)) {
      res.writeHead(404).end('not found');
      return;
    }
    const headers = { 'content-type': MIME[path.extname(file)] || 'application/octet-stream', 'cache-control': 'no-store' };
    if (req.method === 'HEAD') {
      res.writeHead(200, { ...headers, 'content-length': fs.statSync(file).size }).end();
      return;
    }
    res.writeHead(200, headers);
    fs.createReadStream(file).pipe(res);
  });
  return new Promise((resolve) => server.listen(0, '127.0.0.1', () => resolve({ server, port: server.address().port })));
}

// ---- checks -------------------------------------------------------------------------------------

let passed = 0;
const failures = [];
const started = Date.now();
function check(name, ok, detail) {
  if (ok) {
    passed += 1;
    console.log(`  ok   ${name}${detail ? `: ${detail}` : ''}`);
  } else {
    failures.push(`${name}${detail ? ` (${detail})` : ''}`);
    console.error(`  FAIL ${name}${detail ? ` (${detail})` : ''}`);
  }
}

function elapsed(since = started) {
  return `${((Date.now() - since) / 1000).toFixed(1)} s`;
}

// Polls `fn` in the page until it returns something truthy. A navigation in
// between (the isolation reload, the page reload) is not an error: the poll
// goes on.
async function waitFor(page, fn, label, timeout = STEP_TIMEOUT, arg) {
  const start = Date.now();
  while (Date.now() - start < timeout) {
    try {
      const value = await page.evaluate(fn, arg);
      if (value) return value;
    } catch {
      // The page is navigating; ask again.
    }
    await page.waitForTimeout(250);
  }
  throw new Error(`timed out after ${timeout / 1000} s waiting for ${label}`);
}

// Page errors and console errors from the site's own origin.
function watchErrors(page, name, base) {
  const errors = [];
  page.on('pageerror', (e) => errors.push(`${name}: ${e.message}`));
  page.on('console', (m) => {
    if (m.type() !== 'error') return;
    const url = (m.location() && m.location().url) || '';
    if (url && !url.startsWith(base)) return;
    errors.push(`${name}: console.error ${m.text()}${url ? ` @ ${url}` : ''}`);
  });
  return errors;
}

// JSON with every object's keys sorted, for comparing whole values.
function canonical(value) {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  if (value && typeof value === 'object') return `{${Object.keys(value).sort().map((k) => `${JSON.stringify(k)}:${canonical(value[k])}`).join(',')}}`;
  return JSON.stringify(value);
}

// The Kafka cluster id the lab derives from a scenario id (`clusterIdFor` in
// `external.js`, the contract's rule), computed independently here.
function expectedClusterId(scenarioId) {
  for (let salt = 0; ; salt++) {
    const bytes = crypto.createHash('sha256').update(`krabka-lab/cluster-id/${scenarioId}${salt ? `/${salt}` : ''}`).digest().subarray(0, 16);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    const id = bytes.toString('base64url');
    if (!id.startsWith('-')) return id;
  }
}

// A node's snapshot entry once `pred(entry)` holds, as JSON text.
const nodeWhere = (id, pred) => `(() => { const s = window.krabkaLab.world.snapshot(); const n = s && s.nodes.find((x) => x.id === ${id}); return n && (${pred})(n) ? JSON.stringify(n) : null; })()`;
// A real broker whose process runs.
const isRunning = `(n) => n.state && n.state.process && n.state.process.state === 'running'`;
// A node's snapshot entry once `pred(entry)` holds. A timeout says what the
// node's state was at the end.
async function node(page, id, pred = '() => true', label = `node ${id}`, timeout = STEP_TIMEOUT) {
  try {
    return JSON.parse(await waitFor(page, nodeWhere(id, pred), label, timeout));
  } catch (err) {
    const last = await page.evaluate(nodeWhere(id, '() => true')).catch(() => null);
    const state = last ? JSON.parse(last).state : null;
    const brief = state ? JSON.stringify({ ...state, client: undefined, stdout: undefined, runtime: undefined, stderr: state.stderr?.slice(-5) }) : 'no state';
    throw new Error(`${err.message}; node ${id} was ${brief.slice(0, 1500)}`);
  }
}

// The first line of a real broker's stderr (the runtime keeps its last 200)
// that contains `text`, once there is one.
async function stderrLine(page, id, text, label, timeout = STEP_TIMEOUT) {
  return waitFor(
    page,
    `(() => { const p = window.krabkaLab.external.process(${id}); return p ? p.tail('stderr').find((l) => l.includes(${JSON.stringify(text)})) || null : null; })()`,
    label,
    timeout,
  );
}

async function openLab(page, base) {
  await page.goto(`${base}/docs/lab/`, { waitUntil: 'load' });
  await page.waitForSelector('#krabka-lab[data-ready="true"]', { timeout: STEP_TIMEOUT });
}

const ready = `self.crossOriginIsolated && document.querySelector('#krabka-lab[data-ready="true"]') !== null && Boolean(window.krabkaLab && window.krabkaLab.world.id)`;

// The producer's view of the cluster: its client's metadata, as the real
// brokers served it.
async function metadata(page, pred = '() => true', label = 'the producer to have metadata') {
  const n = await node(page, 4, `(n) => { const m = n.state && n.state.client && n.state.client.metadata; return Boolean(m) && (${pred})(m); }`, label);
  return n.state.client.metadata;
}

const partitionsOf = (m) => ((m.topics && m.topics[TOPIC] && m.topics[TOPIC].partitions) || []).slice().sort((a, b) => a.partition - b.partition);

// The consumer's partitions: `[{ partition, position, committed, hwm, lag }]`.
function assignmentOf(state, topic = TOPIC) {
  return (state.assignment || []).filter((a) => a.topic === topic).sort((a, b) => a.partition - b.partition);
}

// ---- the flows ----------------------------------------------------------------------------------

// One real broker: it boots, formats its volume, and serves.
async function oneBroker(context, base, errors) {
  console.log('One real broker');
  const page = await context.newPage();
  errors.push(...watchErrors(page, 'one broker', base));
  await openLab(page, base);
  const since = Date.now();
  await page.evaluate((doc) => window.krabkaLab.openScenario(doc), ONE_BROKER);
  await waitFor(page, ready, 'the isolated reload', STEP_TIMEOUT);
  const scenarioId = await page.evaluate(() => window.krabkaLab.world.id);
  const running = await node(page, 1, isRunning, 'the process to run', BOOT_TIMEOUT);
  check(`the process reaches running (${elapsed(since)} after the scenario opened, the module's download and compile included)`, running.alive === true, JSON.stringify(running.state.process));
  check(
    'it runs with the contract environment',
    canonical(running.state.env) === canonical({ KRABKA_NODE_ID: '1', KRABKA_HOST: '10.0.0.1', KRABKA_VOTERS: '1@10.0.0.1:9093', KRABKA_CLUSTER_ID: expectedClusterId(scenarioId), KRABKA_CONFIG: '{}' }),
    JSON.stringify(running.state.env),
  );
  const boot = await stderrLine(page, 1, 'starting the broker on /data/log', 'the broker to start');
  check('it formats its volume and boots a fresh cluster', /bootstrap_mode=Bootstrap/.test(boot) && /voter=true/.test(boot), boot);
  const serving = await stderrLine(page, 1, 'krabka-broker serving on', 'the broker to serve', BOOT_TIMEOUT);
  check(`it advertises its local bridge address (${elapsed(since)})`, serving.endsWith('krabka-broker serving on 127.0.0.1:9092'), serving);
  await page.close();
}

// Asks the lab's admin node to create the topic, as `kafka-topics --create`
// would; resolves when the brokers answered.
async function createTopic(page) {
  const adminId = await page.evaluate(
    (topic) =>
      window.krabkaLab.world.addNode({
        id: 0,
        kind: 'admin',
        name: 'topic-admin',
        x: 0,
        y: 0,
        config: { bootstrap: [1, 2, 3], topics: [{ name: topic, partitions: 3, replication_factor: 3 }] },
      }),
    TOPIC,
  );
  const admin = await node(page, adminId, `(n) => n.state && Array.isArray(n.state.topics) && n.state.topics.some((t) => t.status === 'created' || t.status === 'exists' || t.status === 'failed')`, 'the admin node to create the topic');
  return admin.state.topics[0];
}

async function checkKafkactl(page, base) {
  if (!fs.existsSync(KAFKACTL)) throw new Error(`build ${KAFKACTL} before the kafkactl check`);
  await page.evaluate(() => window.krabkaLab.control(4, { cmd: 'pause' }));
  const config = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'krabka-kafkactl-')), 'config.yml');
  const command = (args, input = '', timeout = 15_000, until = '') => new Promise((resolve) => {
    const child = spawn(KAFKACTL, ['--config', config, ...args]);
    let output = '';
    child.stdout.on('data', (data) => { output += data; if (until && output.includes(until)) child.kill(); });
    child.stderr.on('data', (data) => { output += data; });
    if (input) child.stdin.write(input);
    child.stdin.end();
    const timer = setTimeout(() => child.kill(), timeout);
    child.on('close', (code) => { clearTimeout(timer); resolve({ code, output }); });
  });
  const bridge = spawn(KAFKACTL, ['lab', 'bridge', '--origin', base]);
  try {
    const token = await new Promise((resolve, reject) => {
      let output = '';
      const timer = setTimeout(() => reject(new Error(`bridge token timed out: ${output}`)), 10_000);
      bridge.stdout.on('data', (data) => {
        output += data;
        const found = output.match(/\b[0-9a-f]{48}\b/);
        if (found) { clearTimeout(timer); resolve(found[0]); }
      });
      bridge.on('exit', (code) => { clearTimeout(timer); reject(new Error(`bridge exited ${code}: ${output}`)); });
    });
    await page.locator('#krabka-lab .lab-bridge summary').click();
    await page.locator('#krabka-lab .lab-bridge input[type="password"]').fill(token);
    await page.locator('#krabka-lab .lab-bridge button', { hasText: 'Connect' }).click();
    await waitFor(page, `window.krabkaLab.bridge.state === 'connected'`, 'the browser to pair with kafkactl');
    const clientId = await page.evaluate(() => window.krabkaLab.world.scenario().nodes.find((n) => n.kind === 'local-client').id);
    check('the distributed kafkactl binary pairs with the browser', await page.evaluate(() => window.krabkaLab.bridge.state) === 'connected');
    const setup = await command(['config', 'add', 'krabka-lab', '--broker', '127.0.0.1:9092']);
    check('kafkactl adds the krabka-lab context', setup.code === 0, setup.output);
    const brokers = await command(['--context', 'krabka-lab', 'get', 'brokers']);
    check('kafkactl queries all three real brokers', brokers.code === 0 && [1, 2, 3].every((id) => brokers.output.includes(`127.0.0.1:${9091 + id}`)), brokers.output);
    const topics = await command(['--context', 'krabka-lab', 'get', 'topics']);
    check('kafkactl queries the orders topic', topics.code === 0 && topics.output.includes('orders'), topics.output);
    const configText = fs.readFileSync(config, 'utf8');
    if (!configText.includes('127.0.0.1:9092')) throw new Error('the kafkactl context has no expected bootstrap broker');
    fs.writeFileSync(config, configText.replace('127.0.0.1:9092', '127.0.0.1:9093'));
    const alternateTopics = await command(['--context', 'krabka-lab', 'get', 'topics']);
    check('kafkactl uses the context broker for topics', alternateTopics.code === 0 && alternateTopics.output.includes('orders'), alternateTopics.output);
    fs.writeFileSync(config, configText);
    const produced = await command(['--context', 'krabka-lab', 'produce', 'orders'], 'bridge-check-record\n');
    check('kafkactl produces to orders', produced.code === 0, produced.output);
    const consumed = await command(['--context', 'krabka-lab', 'consume', 'orders', '--offset=oldest', '--output=raw'], '', 15_000, 'bridge-check-record');
    check('kafkactl consumes its record', consumed.output.includes('bridge-check-record'), consumed.output.slice(-300));
    for (const partition of [0, 1, 2]) {
      const read = await command(['--context', 'krabka-lab', 'consume', 'orders', `--partition=${partition}`, '--offset=oldest', '--output=raw'], '', 8_000, '"id":');
      check(`kafkactl fetches partition ${partition}`, read.output.includes('"id":'), read.output.slice(-300));
    }
    await page.evaluate((id) => window.krabkaLab.fault({ kind: 'partition', a: id, b: 1 }), clientId);
    const cut = await command(['--context', 'krabka-lab', 'get', 'brokers'], '', 3_000);
    check('the lab link cut blocks a new kafkactl request', cut.code !== 0, cut.output.slice(-500));
    await page.evaluate((id) => window.krabkaLab.fault({ kind: 'heal', a: id, b: 1 }), clientId);
    const healed = await command(['--context', 'krabka-lab', 'get', 'brokers']);
    check('kafkactl works again after the link heals', healed.code === 0 && healed.output.includes('127.0.0.1:9094'), healed.output);
    await page.evaluate((id) => window.krabkaLab.world.removeNode(id), clientId);
    await waitFor(page, `window.krabkaLab.bridge.state === 'error'`, 'the bridge to deconfigure after client removal', 10_000);
    const removed = await command(['--context', 'krabka-lab', 'get', 'brokers'], '', 3_000);
    check('removing the local client closes the bridge listeners', removed.code !== 0, removed.output.slice(-500));
  } finally {
    bridge.kill();
    await page.evaluate(() => window.krabkaLab.control(4, { cmd: 'resume' }));
    fs.rmSync(path.dirname(config), { recursive: true, force: true });
  }
}

async function threeBrokers(context, base, errors) {
  console.log('Three real brokers, the lab clients, a fault and a reload');
  const page = await context.newPage();
  errors.push(...watchErrors(page, 'three brokers', base));
  await openLab(page, base);
  const since = Date.now();
  await page.evaluate((doc) => window.krabkaLab.openScenario(doc), THREE_BROKERS);
  await waitFor(page, ready, 'the scenario', STEP_TIMEOUT);
  const scenarioId = await page.evaluate(() => window.krabkaLab.world.id);
  const clusterId = expectedClusterId(scenarioId);

  // The quorum.
  for (const id of [1, 2, 3]) await node(page, id, isRunning, `broker ${id} to run`, BOOT_TIMEOUT);
  const envs = await page.evaluate(() => [1, 2, 3].map((id) => window.krabkaLab.world.snapshot().nodes.find((n) => n.id === id).state.env.KRABKA_VOTERS));
  check('three voters, each with the same static quorum', envs.every((v) => v === '1@10.0.0.1:9093,2@10.0.0.2:9093,3@10.0.0.3:9093'), JSON.stringify(envs));
  const servingLines = [];
  for (const id of [1, 2, 3]) servingLines.push(await stderrLine(page, id, 'krabka-broker serving on', `broker ${id} to serve`, BOOT_TIMEOUT));
  check(`the three brokers form a quorum and serve (${elapsed(since)})`, servingLines.every((l, i) => l.endsWith(`serving on 127.0.0.1:${9092 + i}`)), servingLines.join(' | '));
  const topic = await createTopic(page);
  check(`the lab's admin node creates "${TOPIC}" with three replicas on the real brokers (${elapsed(since)})`, topic.status === 'created', JSON.stringify(topic));
  if (args.has('--kafkactl') || args.has('--kafkactl-only')) {
    await node(page, 4, '(n) => n.state && n.state.acked >= 30', 'the producer to ack 30 records before the kafkactl check');
    await metadata(page, `(m) => m.topics?.${TOPIC}?.partitions?.length === 3 && m.topics.${TOPIC}.partitions.every((p) => p.isr.length === 3)`, 'all three topic replicas to be in sync');
    await checkKafkactl(page, base);
  }
  if (args.has('--kafkactl-only')) { await page.close(); return; }

  // The lab's clients produce and consume through the real brokers.
  const produced = await node(page, 4, '(n) => n.state && n.state.acked >= 30', 'the producer to have 30 records acknowledged');
  check(`the producer's records are acknowledged with acks=all (${elapsed(since)})`, produced.state.failed === 0, `acked ${produced.state.acked}, failed ${produced.state.failed}, producer id ${produced.state.producer_id}`);
  const consumed = await node(page, 5, `(n) => n.state && n.state.state === 'stable' && n.state.processed >= 30`, 'the consumer to process 30 records');
  const parts = assignmentOf(consumed.state);
  check(
    `the consumer joins its group and consumes them back (${elapsed(since)})`,
    parts.length === PARTITIONS && consumed.state.processed >= 30,
    `group ${consumed.state.group}, coordinator ${consumed.state.coordinator}, processed ${consumed.state.processed}, partitions ${JSON.stringify(parts)}`,
  );

  // A KIP-848 group (ConsumerGroupHeartbeat) joins now that the topic exists.
  const auditId = await page.evaluate(
    (topic) =>
      window.krabkaLab.world.addNode({
        id: 0,
        kind: 'consumer',
        name: 'audit',
        x: 840,
        y: 330,
        config: { bootstrap: [1, 2, 3], group: 'audit', topics: [topic], auto_offset_reset: 'earliest', protocol: 'consumer' },
      }),
    TOPIC,
  );
  const audit = await node(page, auditId, `(n) => n.state && n.state.state === 'stable' && (n.state.assignment || []).length === ${PARTITIONS} && n.state.processed >= 30`, 'the KIP-848 member to consume');
  check(
    `a KIP-848 member gets every partition from the broker's assignor and consumes them too (${elapsed(since)})`,
    audit.state.protocol === 'consumer' && audit.state.epoch >= 1,
    `epoch ${audit.state.epoch}, processed ${audit.state.processed}, partitions ${assignmentOf(audit.state).map((a) => a.partition).join(',')}`,
  );

  // The real brokers' metadata, as the lab's clients got it, in the inspector.
  const meta = await metadata(page, `(m) => (m.brokers || []).length === 3 && ${JSON.stringify(TOPIC)} in (m.topics || {}) && m.topics[${JSON.stringify(TOPIC)}].partitions.every((p) => p.isr.length === 3)`, 'metadata with three brokers and a full ISR');
  const brokers = meta.brokers.map((b) => ({ id: b.id, host: b.host, port: b.port, node: b.node })).sort((a, b) => a.id - b.id);
  check(
    'the metadata names the three real brokers at their bridge addresses',
    canonical(brokers) === canonical([1, 2, 3].map((id) => ({ id, host: '127.0.0.1', port: 9091 + id, node: id }))),
    JSON.stringify(brokers),
  );
  check('and the scenario\'s cluster id and a controller among them', meta.cluster_id === clusterId && [1, 2, 3].includes(meta.controller), `cluster ${meta.cluster_id}, controller ${meta.controller}`);
  const leaders = partitionsOf(meta).map((p) => ({ partition: p.partition, leader: p.leader, replicas: [...p.replicas].sort(), isr: [...p.isr].sort() }));
  check(
    `"${TOPIC}" has three partitions, each led by one of them with all three in sync`,
    leaders.length === PARTITIONS && leaders.every((p) => [1, 2, 3].includes(p.leader) && canonical(p.replicas) === '[1,2,3]' && canonical(p.isr) === '[1,2,3]'),
    JSON.stringify(leaders),
  );
  await page.locator('#krabka-lab .lab-node[data-node-id="4"]').click();
  await page.locator('#krabka-lab .lab-inspector #lab-tab-raw').click();
  const raw = await waitFor(page, `(() => { const t = document.querySelector('#krabka-lab .lab-inspector .lab-tabpanel[data-tab="raw"] pre')?.textContent; return t && t.includes('"metadata"') ? t : null; })()`, 'the inspector to show the raw snapshot');
  const shown = JSON.parse(raw).state.client.metadata;
  check(
    "the inspector shows the real brokers' metadata",
    shown.cluster_id === clusterId && canonical(shown.brokers.map((b) => b.port).sort()) === canonical([9092, 9093, 9094]) && partitionsOf(shown).length === PARTITIONS,
    `${shown.brokers.map((b) => `${b.id}@${b.host}:${b.port}`).join(', ')}; ${partitionsOf(shown).map((p) => `p${p.partition} leader ${p.leader} isr [${p.isr}]`).join(', ')}`,
  );
  await page.locator('#krabka-lab .lab-inspector #lab-tab-state').click();
  await page.locator('#krabka-lab .lab-node[data-node-id="1"]').click();
  const stateShown = await waitFor(page, `document.querySelector('#krabka-lab .lab-inspector dd[data-field="process_state"]')?.textContent || null`, 'the real broker in the inspector');
  const stderrShown = await waitFor(page, `document.querySelector('#krabka-lab .lab-inspector pre[data-field="stderr"]')?.textContent || null`, 'the stderr tail');
  check("a real broker's own view shows its process and its log", stateShown === 'running' && stderrShown.includes('krabka_broker'), `${stateShown}; ${stderrShown.split('\n').length} lines`);

  // Where the group's offsets live: every partition of `__consumer_offsets`
  // whose replicas the clients' metadata shows.
  const offsets = await page.evaluate(() => {
    const topics = [4, 5].map((id) => window.krabkaLab.world.snapshot().nodes.find((n) => n.id === id).state.client.metadata.topics);
    const found = topics.find((t) => t && t.__consumer_offsets);
    if (!found) return null;
    const parts = found.__consumer_offsets.partitions;
    return { partitions: parts.length, replicas: [...new Set(parts.map((p) => p.replicas.length))], brokers: [...new Set(parts.flatMap((p) => p.replicas))].sort() };
  });
  console.log(`  note __consumer_offsets: ${offsets ? `${offsets.partitions} partitions with ${offsets.replicas.join('/')} replicas, on brokers ${offsets.brokers.join(', ')}` : 'not in the clients\' metadata'}`);

  // Kill the leader of a partition, one that coordinates neither group (a
  // coordinator failover needs `__consumer_offsets` replicas elsewhere): its
  // leadership moves, the consumer keeps consuming, and a restart brings the
  // broker back on its volume.
  const coordinators = [(await node(page, 5)).state.coordinator, (await node(page, auditId)).state.coordinator];
  const victim = leaders.find((p) => !coordinators.includes(p.leader)) ?? leaders[0];
  const leader = victim.leader;
  const before = (await node(page, 5)).state.processed;
  const killedAt = Date.now();
  await page.evaluate(
    ({ id, speed }) => {
      window.krabkaLab.fault({ kind: 'kill', node: id });
      window.krabkaLab.world.setSpeed(speed);
    },
    { id: leader, speed: FAULT_SPEED },
  );
  const killed = await node(page, leader, `(n) => n.alive === false && n.state.process.state === 'killed'`, 'the leader to be killed');
  check(`kill stops broker ${leader}, the leader of ${TOPIC}-${victim.partition}`, killed.alive === false);
  const movedTo = await metadata(
    page,
    `(m) => { const p = (m.topics[${JSON.stringify(TOPIC)}] || { partitions: [] }).partitions.find((x) => x.partition === ${victim.partition}); return p && p.leader >= 1 && p.leader !== ${leader}; }`,
    `the leadership of ${TOPIC}-${victim.partition} to move`,
  );
  const moved = partitionsOf(movedTo)[victim.partition];
  check(`the leadership of ${TOPIC}-${victim.partition} moves to broker ${moved.leader} (${elapsed(killedAt)})`, moved.leader !== leader && !moved.isr.includes(leader), JSON.stringify(moved));
  const going = await node(page, 5, `(n) => n.state.processed >= ${before + 30} && n.state.state === 'stable'`, 'the consumer to go on consuming');
  check(`the consumer keeps consuming (${elapsed(killedAt)})`, going.state.processed >= before + 30, `${before} -> ${going.state.processed}, coordinator ${going.state.coordinator}`);
  const restartedAt = Date.now();
  await page.evaluate((id) => window.krabkaLab.fault({ kind: 'restart', node: id }), leader);
  const back = await node(page, leader, `(n) => n.alive && n.state.process.state === 'running' && n.state.process.incarnation === 2`, 'the restarted broker', BOOT_TIMEOUT);
  const rejoin = await stderrLine(page, leader, 'starting the broker on /data/log', 'the restarted broker to start');
  check(`restart brings broker ${leader} back on its volume (${elapsed(restartedAt)})`, back.state.process.volume === `${scenarioId}/${leader}` && /bootstrap_mode=Rejoin/.test(rejoin), rejoin);
  // A client that starts again fetches fresh metadata, as a restarted Kafka
  // client does; the lab's clients otherwise refresh it only on an error or
  // after metadata.max.age.ms (5 min).
  let inSync = null;
  for (const start = Date.now(); !inSync && Date.now() - start < STEP_TIMEOUT; ) {
    await page.evaluate(() => window.krabkaLab.fault({ kind: 'restart', node: 4 }));
    await page.waitForTimeout(3000);
    const m = await page.evaluate(() => window.krabkaLab.world.snapshot().nodes.find((n) => n.id === 4).state.client.metadata);
    const parts = m ? partitionsOf(m) : [];
    if (parts.length === PARTITIONS && parts.every((p) => p.isr.includes(leader))) inSync = parts;
  }
  check(
    `and it catches up into the ISR of every partition (${elapsed(restartedAt)})`,
    Boolean(inSync),
    JSON.stringify((inSync || []).map((p) => ({ partition: p.partition, leader: p.leader, isr: p.isr }))),
  );
  await page.evaluate(() => window.krabkaLab.world.setSpeed(1));

  // Stop producing and let both groups commit every record. The classic
  // member then closes, as an application does when it shuts down: it
  // commits and leaves, and its group is empty. The KIP-848 member stays in
  // its group, as a process that is killed does. Then the page reloads.
  await page.evaluate(() => {
    const spec = window.krabkaLab.world.spec(4);
    window.krabkaLab.world.updateNode(4, { ...spec, config: { ...spec.config, rate_per_sec: 0 } });
  });
  const allCommitted = (id) =>
    node(
      page,
      id,
      `(n) => { const a = (n.state.assignment || []).filter((x) => x.topic === ${JSON.stringify(TOPIC)}); return a.length === ${PARTITIONS} && n.state.lag === 0 && a.every((x) => x.hwm > 0 && x.committed === x.hwm); }`,
      `group ${id === 5 ? 'billing' : 'audit'} to commit every record`,
    );
  const settled = await allCommitted(5);
  const hwms = assignmentOf(settled.state).map((a) => a.hwm);
  const total = hwms.reduce((a, b) => a + b, 0);
  const auditSettled = await allCommitted(auditId);
  check(
    `both groups committed every record: ${total} across the partitions`,
    total > 0 && canonical(assignmentOf(auditSettled.state).map((a) => a.committed)) === canonical(hwms),
    `billing ${JSON.stringify(assignmentOf(settled.state).map((a) => a.committed))}, audit ${JSON.stringify(assignmentOf(auditSettled.state).map((a) => a.committed))}`,
  );
  const closed = await page.evaluate(() => window.krabkaLab.world.control(5, { cmd: 'close' }));
  await node(page, 5, '(n) => n.state && n.state.closed === true', 'the classic member to close');
  check('the classic member closes: it commits and leaves its group', closed.ok === true, JSON.stringify(closed));
  await page.waitForTimeout(1500); // the volumes' write-behind
  const reloadedAt = Date.now();
  await page.reload({ waitUntil: 'load' });
  await waitFor(page, ready, 'the reloaded lab', STEP_TIMEOUT);
  const sameScenario = await page.evaluate(() => window.krabkaLab.world.id);
  check('the reload reopens the scenario', sameScenario === scenarioId, sameScenario);
  const rejoins = [];
  for (const id of [1, 2, 3]) {
    await node(page, id, isRunning, `broker ${id} to run after the reload`, BOOT_TIMEOUT);
    rejoins.push(await stderrLine(page, id, 'starting the broker on /data/log', `broker ${id} to start after the reload`));
  }
  check(`the real brokers restart from their IndexedDB volumes (${elapsed(reloadedAt)})`, rejoins.every((l) => /bootstrap_mode=Rejoin/.test(l)), rejoins.join(' | '));
  const replayId = await page.evaluate(
    (topic) =>
      window.krabkaLab.world.addNode({
        id: 0,
        kind: 'consumer',
        name: 'replay',
        x: 360,
        y: 480,
        config: { bootstrap: [1, 2, 3], group: 'replay', topics: [topic], auto_offset_reset: 'earliest' },
      }),
    TOPIC,
  );
  const replay = await node(page, replayId, `(n) => n.state && n.state.processed >= ${total} && n.state.state === 'stable'`, 'a new group to read every record');
  const replayed = assignmentOf(replay.state);
  check(
    `the records are still there: a new group reads all ${total} from offset 0 (${elapsed(reloadedAt)})`,
    replay.state.processed === total && canonical(replayed.map((a) => a.position)) === canonical(hwms),
    `processed ${replay.state.processed}, positions ${JSON.stringify(replayed.map((a) => a.position))}, before the reload ${JSON.stringify(hwms)}`,
  );
  const resumed = await node(page, 5, `(n) => n.state && n.state.state === 'stable' && (n.state.assignment || []).filter((x) => x.topic === ${JSON.stringify(TOPIC)} && x.committed != null).length === ${PARTITIONS}`, 'the classic group to rejoin');
  check(
    'the classic group resumes from its committed offsets',
    canonical(assignmentOf(resumed.state).map((a) => a.committed)) === canonical(hwms) && resumed.state.processed === 0,
    JSON.stringify(assignmentOf(resumed.state)),
  );
  // The KIP-848 member the reload stopped is still in its group: the node
  // draws the same member id from the scenario's seed and rejoins with epoch
  // 0, so the coordinator hands it back its own partitions.
  const auditBack = await node(page, auditId, `(n) => n.state && n.state.state === 'stable' && (n.state.assignment || []).filter((x) => x.topic === ${JSON.stringify(TOPIC)} && x.committed != null).length === ${PARTITIONS}`, 'the KIP-848 group to take its partitions back');
  check(
    `the KIP-848 member rejoins its group and resumes from its committed offsets (${elapsed(reloadedAt)})`,
    canonical(assignmentOf(auditBack.state).map((a) => a.committed)) === canonical(hwms) && auditBack.state.processed === 0,
    JSON.stringify(assignmentOf(auditBack.state)),
  );
  await page.close();
}

async function main() {
  if (!fs.existsSync(path.join(DIST_DIR, 'docs', 'lab', 'index.html'))) {
    console.error('dist/docs/lab/index.html is missing: run `npm run build` first.');
    process.exit(1);
  }
  if (BUILD) {
    try {
      buildModule();
    } catch (err) {
      console.error(`The real broker did not build: ${err.message}`);
      process.exit(2);
    }
  }
  if (!fs.existsSync(STAGED_MODULE)) {
    console.error(`${path.relative(ROOT, STAGED_MODULE)} is missing: run \`npm run build:broker\`.`);
    process.exit(2);
  }
  const size = fs.statSync(STAGED_MODULE).size;
  console.log(`  module: ${path.relative(ROOT, STAGED_MODULE)} (${size.toLocaleString('en')} bytes)`);
  const pw = await loadPlaywright();
  if (!pw) {
    console.error('Playwright is not installed (neither in node_modules nor globally).');
    process.exit(2);
  }
  let browser;
  try {
    browser = await launchChromium(pw);
  } catch (err) {
    console.error(`Playwright could not launch Chromium: ${err.message.split('\n')[0]}`);
    process.exit(2);
  }
  const { server, port } = await serve(DIST_DIR, STAGED_MODULE);
  const base = `http://127.0.0.1:${port}`;
  const errors = [];
  const browserStart = Date.now();
  try {
    const context = await browser.newContext({ viewport: { width: 1400, height: 1000 } });
    if (!args.has('--kafkactl-only')) await oneBroker(context, base, errors);
    await threeBrokers(context, base, errors);
    await context.close();
  } catch (err) {
    failures.push(`exception: ${err.message}`);
    console.error(`  FAIL exception: ${err.stack || err.message}`);
  } finally {
    await browser.close();
    server.close();
  }
  check('no page errors or console errors', errors.length === 0, errors.slice(0, 3).join(' | '));
  console.log(`\n${passed} checks passed${failures.length ? `, ${failures.length} failed` : ''} in ${elapsed(browserStart)} of browser time (${elapsed()} in all)`);
  if (failures.length) {
    for (const f of failures) console.error(`  • ${f}`);
    process.exit(1);
  }
  console.log('✅ PASS: the real krabka-broker runs as lab nodes.');
}

main();
