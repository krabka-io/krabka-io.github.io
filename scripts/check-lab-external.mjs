// End-to-end check of real brokers in the Cluster Lab (`/docs/lab`) in
// headless Chromium.
//
// Until the real `krabka-broker` build is on the site, the WASI test guest
// (`playground/wasi-guest`) stands in for it: with `KRABKA_NODE_ID` set it
// follows the same process contract (`playground/docs/lab-real-broker.md`),
// echoes on every listener, dials on command and relays, and counts its boots
// in its volume. The check builds the guest, serves the built site from `dist/`
// with the guest beside it and without COOP/COEP headers (the lab's service
// worker provides isolation, as on GitHub Pages), points the lab at the guest
// through `window.krabkaLab.external.useModule`, and checks that (after the
// contract's pure parts, in Node: addresses, configuration, environment,
// cluster id, Kafka framing):
//
// - on a page without the broker build, a real broker node says so and the
//   page does not reload;
// - adding a real broker reloads the page once, cross-origin isolated, and the
//   scenario survives the reload; so does opening a shared scenario with one;
// - while the module downloads, the node shows how much of it has arrived;
// - the process gets the contract's descriptors and environment;
// - a pinger's echoes through the process keep the link's latency, with the
//   arithmetic of `check-lab`'s WebRTC check;
// - the process dials its own controller listener through the world, at
//   boot and on command, as a combined-mode broker does;
// - a dial from the process to an echo node goes through the world (its
//   latency and counters show it) and the answer comes back, byte for byte
//   and one lab frame per Kafka frame even when the process's send buffer
//   fills; dials to no node and to a node that is down fail; a dial across a
//   cut link waits for the link, and times out after 30 s of lab time;
// - a configuration change restarts it with the new FileConfig;
// - kill refuses connections, restart brings the process back on the same
//   volume, wipe forgets it;
// - pausing the lab stops the process's clock;
// - an exit or a trap kills the node, with the reason in the timeline and
//   the inspector, which also shows the process's stderr;
// - the Storage panel lists and forgets the volume;
// - a session keeps the node on the hub;
// - no page errors.
//
// Usage:  npm run build && npm run check-lab-external [-- --headed]
// Needs cargo with the wasm32-wasip1 target (the guest builds in
// $CARGO_TARGET_DIR, or the crate's target/), Playwright (`playwright` or
// `playwright-core`, local or global) and a Chromium, found as `check-lab`
// finds it. Uses `wasm-opt` when node_modules or the PATH has one. Exits 2
// when a tool is missing, 1 when a check fails.

import crypto from 'crypto';
import fs from 'fs';
import http from 'http';
import path from 'path';
import { createRequire } from 'module';
import { execFileSync, execSync, spawnSync } from 'child_process';
import { fileURLToPath, pathToFileURL } from 'url';
import { PRESETS as SIMULATED_PRESETS } from './lab-simulated-presets.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const DIST_DIR = path.join(ROOT, 'dist');
const CRATE = path.join(ROOT, 'playground', 'wasi-guest');
const args = new Set(process.argv.slice(2));
const HEADLESS = !args.has('--headed');
const STEP_TIMEOUT = 30_000;

// A fresh browser context that has already seen the lab tour, which would
// otherwise open over the canvas and take the checks' clicks.
async function newLabContext(browser, viewport) {
  const context = await browser.newContext({ viewport });
  await context.addInitScript(() => {
    try {
      localStorage.setItem('krabka-lab.tour', 'done');
    } catch {
      // No storage: the tour opens, and the check reports what it finds.
    }
  });
  return context;
}
const GUEST_URL = '/lab-test/krabka-wasi-guest.wasm';
// The guest again, sent 16 KiB at a time, a piece every 60 ms.
const SLOW_GUEST_URL = '/lab-test/slow/krabka-wasi-guest.wasm';
const SLOW_CHUNK = 16 * 1024;
const SLOW_CHUNK_MS = 60;
const MISSING_BUILD = 'the real broker build is not on this site yet';
const BROKER_BUILD = '/playground/broker/krabka-broker.wasm';
// A node configuration, in an order other than the page's, and the
// KRABKA_CONFIG it becomes.
const RECONFIGURED = { replica_lag_time_max_ms: 10000, min_insync_replicas: 1, rack: 'a', num_partitions: 3 };
const RECONFIGURED_FILE_CONFIG = '{"rack":"a","runtime":{"num_partitions":3,"default_min_insync_replicas":1},"replica_lag_time_max":"10000ms"}';
process.env.PLAYWRIGHT_BROWSERS_PATH ??= '/opt/pw-browsers';

// ---- the guest ------------------------------------------------------------------------------

function run(cmd, cmdArgs, options = {}) {
  const result = spawnSync(cmd, cmdArgs, { stdio: 'inherit', ...options });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`${cmd} ${cmdArgs.join(' ')} exited with ${result.status}`);
}

function wasmOpt() {
  if (process.argv.includes('--no-opt')) return null;
  const local = path.join(ROOT, 'node_modules', '.bin', 'wasm-opt');
  if (fs.existsSync(local)) return local;
  try {
    execFileSync('wasm-opt', ['--version'], { stdio: 'ignore' });
    return 'wasm-opt';
  } catch {
    return null;
  }
}

// Builds the guest the way `check-wasi` does: cargo from inside the crate (its
// `.cargo/config.toml` sets the target and `--cfg tokio_unstable`), then
// `wasm-opt -Oz` when there is one.
function buildGuest() {
  const targetDir = process.env.CARGO_TARGET_DIR ? path.resolve(process.env.CARGO_TARGET_DIR) : path.join(CRATE, 'target');
  run('cargo', ['build', '--release', '--target', 'wasm32-wasip1'], { cwd: CRATE, env: { ...process.env, CARGO_TARGET_DIR: targetDir } });
  const raw = path.join(targetDir, 'wasm32-wasip1', 'release', 'krabka-wasi-guest.wasm');
  const opt = wasmOpt();
  if (!opt) return raw;
  const optimised = path.join(targetDir, 'wasm32-wasip1', 'release', 'krabka-wasi-guest.lab.wasm');
  run(opt, ['-Oz', '--enable-bulk-memory', '--enable-sign-ext', '--enable-mutable-globals', '--enable-nontrapping-float-to-int', '--enable-reference-types', '--enable-multivalue', raw, '-o', optimised]);
  return optimised;
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

// ---- the site: dist/ and the guest, without isolation headers ---------------------------------------

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

// Sends the guest in small pieces, with its length, so that the page has a
// download to show the progress of.
function serveSlowly(res, file, head) {
  const bytes = fs.readFileSync(file);
  res.writeHead(200, { 'content-type': 'application/wasm', 'content-length': bytes.length, 'cache-control': 'no-store' });
  if (head) {
    res.end();
    return;
  }
  let at = 0;
  const next = () => {
    if (res.destroyed) return;
    const chunk = bytes.subarray(at, at + SLOW_CHUNK);
    at += chunk.length;
    if (at >= bytes.length) {
      res.end(chunk);
      return;
    }
    res.write(chunk);
    setTimeout(next, SLOW_CHUNK_MS);
  };
  next();
}

function serve(dir, guest) {
  const server = http.createServer((req, res) => {
    const p = decodeURIComponent(new URL(req.url, 'http://x').pathname);
    if (p === SLOW_GUEST_URL) {
      serveSlowly(res, guest, req.method === 'HEAD');
      return;
    }
    // The site this check serves has no broker build, whatever `dist/` holds:
    // after `npm run build:broker` it holds the real module, and the
    // missing-build flow needs it absent. Every other flow runs the guest
    // through `useModule`.
    if (p === BROKER_BUILD) {
      res.writeHead(404).end('not found');
      return;
    }
    let file = p === GUEST_URL ? guest : path.join(dir, p);
    if (file !== guest && !file.startsWith(dir)) {
      res.writeHead(403).end();
      return;
    }
    if (fs.existsSync(file) && fs.statSync(file).isDirectory()) file = path.join(file, 'index.html');
    else if (!fs.existsSync(file) && fs.existsSync(`${file}.html`)) file = `${file}.html`;
    if (!fs.existsSync(file)) {
      res.writeHead(404).end('not found');
      return;
    }
    res.writeHead(200, { 'content-type': MIME[path.extname(file)] || 'application/octet-stream', 'cache-control': 'no-store' });
    fs.createReadStream(file).pipe(res);
  });
  return new Promise((resolve) => server.listen(0, '127.0.0.1', () => resolve({ server, port: server.address().port })));
}

// ---- checks ---------------------------------------------------------------------------------------

let passed = 0;
const failures = [];
function check(name, ok, detail) {
  if (ok) {
    passed += 1;
    console.log(`  ok   ${name}${detail ? `: ${detail}` : ''}`);
  } else {
    failures.push(`${name}${detail ? ` (${detail})` : ''}`);
    console.error(`  FAIL ${name}${detail ? ` (${detail})` : ''}`);
  }
}

// Polls `fn` in the page until it returns something truthy. A navigation in
// between (the isolation reload) is not an error: the poll goes on.
async function waitFor(page, fn, label, timeout = STEP_TIMEOUT, arg) {
  const start = Date.now();
  while (Date.now() - start < timeout) {
    try {
      const value = await page.evaluate(fn, arg);
      if (value) return value;
    } catch {
      // The page is navigating; ask again.
    }
    await page.waitForTimeout(100);
  }
  throw new Error(`timed out waiting for ${label}`);
}

// Page errors and console errors from the site's own origin. Chromium logs
// every 404 as a console error, and the missing broker build is one on
// purpose: that one is left out, like a font from another origin.
function watchErrors(page, name, base) {
  const errors = [];
  page.on('pageerror', (e) => errors.push(`${name}: ${e.message}`));
  page.on('console', (m) => {
    if (m.type() !== 'error') return;
    const url = (m.location() && m.location().url) || '';
    if (url && !url.startsWith(base)) return;
    if (url === `${base}${BROKER_BUILD}` && /404/.test(m.text())) return;
    errors.push(`${name}: console.error ${m.text()}${url ? ` @ ${url}` : ''}`);
  });
  return errors;
}

// JSON with every object's keys sorted: snapshots pass through the crate's
// `serde_json::Value`, whose maps sort their keys.
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

// Helpers every page gets before its scripts run (serialised by Playwright).
// `T.direct` opens a connection straight to a real broker's process through
// the WASI runtime, beside the world: that is how the check talks to the test
// guest's command port.
function installHelpers() {
  const enc = new TextEncoder();
  const dec = new TextDecoder();
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

  class Reader {
    constructor(conn) {
      this.chunks = [];
      this.size = 0;
      this.closed = false;
      this.waiters = [];
      conn.on('data', (bytes) => {
        this.chunks.push(bytes);
        this.size += bytes.length;
        this.wake();
      });
      const end = () => {
        this.closed = true;
        this.wake();
      };
      conn.on('end', end);
      conn.on('close', end);
    }

    wake() {
      for (const w of this.waiters.splice(0)) w();
    }

    flat() {
      const all = new Uint8Array(this.size);
      let at = 0;
      for (const c of this.chunks) {
        all.set(c, at);
        at += c.length;
      }
      this.chunks = [all];
      return all;
    }

    async until(ready, timeoutMs, what) {
      const deadline = performance.now() + timeoutMs;
      while (!ready()) {
        const left = deadline - performance.now();
        if (left <= 0) throw new Error(`timed out waiting for ${what}`);
        if (this.closed && !ready()) throw new Error(`the connection ended while waiting for ${what}`);
        await new Promise((resolve) => {
          const timer = setTimeout(resolve, left);
          this.waiters.push(() => {
            clearTimeout(timer);
            resolve();
          });
        });
      }
    }

    async bytes(n, timeoutMs = 20000) {
      await this.until(() => this.size >= n, timeoutMs, `${n} bytes`);
      return this.flat().slice(0, n);
    }

    async end(timeoutMs = 20000) {
      await this.until(() => this.closed, timeoutMs, 'the end of the stream');
      return dec.decode(this.flat());
    }
  }

  function frame(text) {
    const body = enc.encode(text);
    const out = new Uint8Array(4 + body.length);
    new DataView(out.buffer).setInt32(0, body.length);
    out.set(body, 4);
    return out;
  }

  function direct(id, bytes) {
    const proc = window.krabkaLab.external.process(id);
    const conn = proc.connect(9092);
    const reader = new Reader(conn);
    conn.send(bytes);
    return { conn, reader };
  }

  // Sends one command line and returns everything the process answers before it closes.
  async function command(id, line) {
    const { conn, reader } = direct(id, enc.encode(`${line}\n`));
    const text = await reader.end();
    conn.close();
    return text;
  }

  window.T = { Reader, frame, direct, command, sleep, enc, dec };
}

// A node's snapshot entry, as JSON text, once `pred(entry)` holds.
const nodeWhere = (id, pred) => `(() => { const s = window.krabkaLab.world.snapshot(); const n = s && s.nodes.find((x) => x.id === ${id}); return n && (${pred})(n) ? JSON.stringify(n) : null; })()`;
const processIs = (id, state, extra = 'true') => nodeWhere(id, `(n) => n.state && n.state.process && n.state.process.state === ${JSON.stringify(state)} && (${extra})`);

// The process's report of its boot-time dial to its own controller listener.
async function selfDial(page, id) {
  const raw = await waitFor(page, nodeWhere(id, `(n) => n.state && Array.isArray(n.state.stdout) && n.state.stdout.some((l) => l.startsWith('self-dial '))`), `node ${id} to dial itself`);
  return JSON.parse(raw).state.stdout.find((l) => l.startsWith('self-dial '));
}

async function readyLine(page, id, boots) {
  const raw = await waitFor(page, nodeWhere(id, `(n) => n.state && Array.isArray(n.state.stdout) && n.state.stdout.some((l) => l.startsWith('ready ') && l.endsWith(' boots=${boots}'))`), `node ${id} to say boots=${boots}`);
  return JSON.parse(raw).state.stdout.find((l) => l.startsWith('ready '));
}

// The pinger's round trip over a window of fresh echoes, from two samples of
// `mean_rtt_ms` × `echoes`: the arithmetic of check-lab's WebRTC check.
async function windowedRtt(page, pinger, window = 10) {
  const at = async (min) => JSON.parse(await waitFor(page, nodeWhere(pinger, `(n) => n.state.echoes >= ${min}`), `${min} echoes`, 40_000)).state;
  const first = await at(window);
  const second = await at(first.echoes + window);
  return Math.round((second.mean_rtt_ms * second.echoes - first.mean_rtt_ms * first.echoes) / (second.echoes - first.echoes));
}

async function openLab(page, base) {
  await page.goto(`${base}/docs/lab/`, { waitUntil: 'load' });
  await page.waitForSelector('#krabka-lab[data-ready="true"]', { timeout: STEP_TIMEOUT });
  await page.evaluate((scenario) => window.krabkaLab.openScenario(scenario), SIMULATED_PRESETS.find((p) => p.id === 'network-probe').scenario);
  await page.locator('#krabka-lab .lab-dtab[data-tab="build"]').click();
}

// ---- the flows --------------------------------------------------------------------------------------

// A page without the broker build: the node says so, nothing reloads.
async function missingBuild(context, base, errors) {
  console.log('Real broker: no build on the site');
  const page = await context.newPage();
  errors.push(...watchErrors(page, 'no-build page', base));
  let navigations = 0;
  page.on('load', () => {
    navigations += 1;
  });
  await openLab(page, base);
  const before = navigations;
  await page.locator('#krabka-lab .lab-kind-btn[data-kind="krabka-broker"]').click();
  await page.waitForSelector('#krabka-lab dialog[open]');
  const note = await page.locator('#krabka-lab dialog [data-field="isolation-reload"]').count();
  const submit = await page.locator('#krabka-lab dialog button[type="submit"]').textContent();
  check('without the build the add dialog promises no reload', note === 0 && submit === 'Add', `${note} notes, "${submit}"`);
  await page.locator('#krabka-lab dialog button[type="submit"]').click();
  const node = JSON.parse(await waitFor(page, processIs(4, 'unavailable'), 'the missing-build state'));
  check('a real broker without the build shows the clear state', node.state.process.reason === MISSING_BUILD && node.alive === true, JSON.stringify(node.state.process));
  await page.locator('#krabka-lab .lab-node[data-node-id="4"]').click();
  // The inspector showed the node while it loaded; the change to the missing
  // build comes within its quarter-second throttle and shows once that passes.
  const reasonText = `document.querySelector('#krabka-lab .lab-inspector dd[data-field="process_reason"]')?.textContent ?? null`;
  const why = await waitFor(page, `(${reasonText}) === ${JSON.stringify(MISSING_BUILD)} ? ${JSON.stringify(MISSING_BUILD)} : null`, 'the inspector reason', 3000).catch(() => page.evaluate(reasonText));
  const status = await page.locator('#krabka-lab .lab-node[data-node-id="4"]').getAttribute('data-status');
  check('the inspector and the card say the build is missing', why === MISSING_BUILD && status === 'no build on this site', `${why} / ${status}`);
  await page.waitForTimeout(500);
  const isolated = await page.evaluate(() => self.crossOriginIsolated);
  check('and the page did not reload for isolation', navigations === before && isolated === false, `${navigations - before} navigations, isolated ${isolated}`);
  await page.close();
}

async function realBroker(context, base, errors) {
  console.log('Real broker: the test guest as krabka-broker');
  const page = await context.newPage();
  errors.push(...watchErrors(page, 'lab', base));
  let navigations = 0;
  page.on('load', () => {
    navigations += 1;
  });
  await openLab(page, base);
  await page.evaluate((url) => window.krabkaLab.external.useModule(url), SLOW_GUEST_URL);
  const scenarioId = await waitFor(page, 'window.krabkaLab.world.id || null', 'the scenario identity');
  check('the lab starts without isolation', (await page.evaluate(() => self.crossOriginIsolated)) === false);

  // Add it through the palette: the dialog announces the reload.
  const before = navigations;
  await page.locator('#krabka-lab .lab-kind-btn[data-kind="krabka-broker"]').click();
  await page.waitForSelector('#krabka-lab dialog [data-field="isolation-reload"]');
  const submit = await page.locator('#krabka-lab dialog button[type="submit"]').textContent();
  check('the add dialog says the page reloads once', submit === 'Add and reload', submit);
  await page.locator('#krabka-lab dialog button[type="submit"]').click();
  await waitFor(page, `self.crossOriginIsolated && document.querySelector('#krabka-lab[data-ready="true"]') !== null`, 'the isolated reload');
  // The module comes in slowly: the node shows how much of it has arrived.
  const loading = [];
  for (const start = Date.now(); Date.now() - start < STEP_TIMEOUT; ) {
    const p = await page.evaluate(() => window.krabkaLab.world.snapshot()?.nodes.find((n) => n.id === 4)?.state?.process ?? null).catch(() => null);
    if (p?.state === 'loading' && loading.at(-1) !== p.reason) loading.push(p.reason);
    if (p && p.state !== 'loading') break;
    await page.waitForTimeout(40);
  }
  const progress = loading.filter((r) => /^downloading the broker: \d+\.\d MB of 0\.3 MB$/.test(r));
  check('while the module downloads the node shows how much has arrived', progress.length > 0, loading.join(' → '));
  const told =await waitFor(page, `[...document.querySelectorAll('#krabka-lab .lab-toast-text')].map((t) => t.textContent).find((t) => t.startsWith('The page reloaded once')) || null`, 'the reload notice', 5000).catch(() => null);
  check('after the reload the page says why it reloaded', Boolean(told), told);
  await page.waitForTimeout(1000);
  const after = await page.evaluate(() => ({ id: window.krabkaLab.world.id, nodes: window.krabkaLab.world.scenario().nodes.map((n) => `${n.id}:${n.kind}:${n.name}`) }));
  check('adding the node reloads the page once, cross-origin isolated', navigations - before === 1, `${navigations - before} navigations`);
  check(
    'the scenario survives the reload',
    after.id === scenarioId && JSON.stringify(after.nodes) === JSON.stringify(['1:echo:echo-a', '2:echo:echo-b', '3:pinger:pinger', '4:krabka-broker:krabka-broker-4']),
    `${after.id} ${after.nodes.join(', ')}`,
  );

  // The process and its contract.
  const running = JSON.parse(await waitFor(page, processIs(4, 'running'), 'the process to run'));
  const clusterId = expectedClusterId(scenarioId);
  check(
    'the process runs with the contract environment',
    canonical(running.state.env) === canonical({ KRABKA_NODE_ID: '4', KRABKA_HOST: '10.0.0.4', KRABKA_VOTERS: '4@10.0.0.4:9093', KRABKA_CLUSTER_ID: clusterId, KRABKA_CONFIG: '{}' }),
    JSON.stringify(running.state.env),
  );
  const env = await page.evaluate(() => window.T.command(4, 'ENV'));
  const expectedEnv = [
    `KRABKA_CLUSTER_ID=${clusterId}`,
    'KRABKA_CONFIG={}',
    'KRABKA_DIAL_FD=6',
    'KRABKA_HOST=10.0.0.4',
    'KRABKA_LISTEN_FDS=4,5',
    'KRABKA_LISTEN_PORTS=9092,9093',
    'KRABKA_NODE_ID=4',
    'KRABKA_VOTERS=4@10.0.0.4:9093',
    'END',
    '',
  ].join('\n');
  check('the process sees the volume, two listeners and the dialer, and the whole environment', env === expectedEnv, JSON.stringify(env));
  const line = await readyLine(page, 4, 1);
  check('the guest accepted the contract and recorded its first boot', line === `ready node=4 host=10.0.0.4 cluster=${clusterId} voters=4@10.0.0.4:9093 listeners=9092,9093 config={} boots=1`, line);
  // A combined-mode broker reaches its own controller over the network: the
  // guest dials 10.0.0.4:9093 at boot, through the world and back into itself.
  const booted = await selfDial(page, 4);
  const loops = await page.evaluate(() => (window.krabkaLab.world.snapshot().delivered.find((d) => d[0] === 4 && d[1] === 4) || [4, 4, 0])[2]);
  check('at boot the process dials its own controller listener through the world', booted === 'self-dial ok: 10.0.0.4:9093 echoed 23 bytes' && loops >= 3, `${booted}, ${loops} frames from node 4 to node 4`);
  const card = await page.locator('#krabka-lab .lab-node[data-node-id="4"]').evaluate((g) => ({ real: g.classList.contains('lab-real'), badge: [...g.querySelectorAll('.lab-badge-text')].map((t) => t.textContent) }));
  check('the canvas marks the node as real code', card.real && card.badge.includes('real'), JSON.stringify(card));

  // A pinger across a 200 ms link: first to the simulated echo, then to the process.
  // A fresh snapshot right after each change, so no sample of the pinger's
  // earlier self (its echoes over the 5 ms default link) enters the window.
  await page.evaluate(() => {
    window.krabkaLab.fault({ kind: 'latency', a: 3, b: 1, ms: 200 });
    window.krabkaLab.fault({ kind: 'latency', a: 3, b: 4, ms: 200 });
    window.krabkaLab.fault({ kind: 'wipe', node: 3 });
    window.krabkaLab.world.flush(performance.now(), true);
  });
  const sameTab = await windowedRtt(page, 3);
  check(`a pinger to the simulated echo over a 200 ms link: ${sameTab} ms`, Math.abs(sameTab - 400) <= 5, `${sameTab} ms`);
  await page.evaluate(() => {
    const spec = window.krabkaLab.world.spec(3);
    window.krabkaLab.world.updateNode(3, { ...spec, config: { ...spec.config, target: 4 } });
    window.krabkaLab.world.flush(performance.now(), true);
  });
  const real = await windowedRtt(page, 3);
  check(`a pinger to the real broker gets its echoes with the link's latency intact (${real} ms vs ${sameTab} ms)`, real >= sameTab - 5 && real <= sameTab + 150, `${real} ms`);

  // A dial from the process to echo-b, through the world, over a 300 ms link.
  await page.evaluate(() => window.krabkaLab.fault({ kind: 'latency', a: 4, b: 2, ms: 300 }));
  const dialed = await page.evaluate(async () => {
    const { T } = window;
    const world = window.krabkaLab.world;
    const counter = () => world.snapshot().nodes.find((n) => n.id === 2).state.frames;
    const delivered = () => (world.snapshot().delivered.find((d) => d[0] === 4 && d[1] === 2) || [4, 2, 0])[2];
    const payload = T.frame('hello through the world');
    const request = new Uint8Array([...T.enc.encode('DIAL 10.0.0.2:9092\n'), ...payload]);
    const framesBefore = counter();
    const deliveredBefore = delivered();
    const sentAt = world.now();
    const { conn, reader } = T.direct(4, request);
    const answer = await reader.bytes(payload.length);
    const answeredAt = world.now();
    conn.close();
    await T.sleep(800);
    return {
      same: answer.length === payload.length && answer.every((b, i) => b === payload[i]),
      labMs: answeredAt - sentAt,
      frames: counter() - framesBefore,
      delivered: delivered() - deliveredBefore,
    };
  });
  check('a dial from the process to an echo node comes back with its answer', dialed.same, JSON.stringify(dialed));
  check('the dial went through the world: two 300 ms hops, and echo-b counted it', dialed.labMs >= 600 && dialed.frames >= 3 && dialed.delivered >= 3, JSON.stringify(dialed));
  const loopback = await page.evaluate(async () => {
    const { T } = window;
    const world = window.krabkaLab.world;
    const loops = () => (world.snapshot().delivered.find((d) => d[0] === 4 && d[1] === 4) || [4, 4, 0])[2];
    const payload = T.frame('to myself, through the world');
    const before = loops();
    const { conn, reader } = T.direct(4, new Uint8Array([...T.enc.encode('DIAL 10.0.0.4:9093\n'), ...payload]));
    const answer = await reader.bytes(payload.length);
    conn.close();
    await T.sleep(500);
    return { same: answer.length === payload.length && answer.every((b, i) => b === payload[i]), frames: loops() - before };
  });
  check('a dial from the process to its own controller listener goes through the world and back', loopback.same && loopback.frames >= 4, JSON.stringify(loopback));
  const nowhere = await page.evaluate(() => window.T.direct(4, window.T.enc.encode('DIAL 10.0.9.9:9092\nhello')).reader.end());
  check('a dial to an address that is no node fails', /^DIAL-ERR HostUnreachable/.test(nowhere), nowhere.trim());
  await page.evaluate(() => window.krabkaLab.fault({ kind: 'kill', node: 1 }));
  const refused = await page.evaluate(() => window.T.direct(4, window.T.enc.encode('DIAL 10.0.0.1:9092\nhello')).reader.end());
  check('a dial to a node that is down is refused', /^DIAL-ERR ConnectionRefused/.test(refused), refused.trim());
  // Across a cut link a dial waits, like a SYN into a black hole: it connects
  // when the link heals, and fails with ETIMEDOUT after 30 s of lab time.
  await page.evaluate(() => window.krabkaLab.fault({ kind: 'partition', a: 4, b: 2 }));
  const healed = await page.evaluate(async () => {
    const { T } = window;
    const payload = T.frame('after the heal');
    const { conn, reader } = T.direct(4, new Uint8Array([...T.enc.encode('DIAL 10.0.0.2:9092\n'), ...payload]));
    const waiting = async () => window.krabkaLab.external.state(4).connections.waiting_dials;
    for (let i = 0; i < 100 && (await waiting()) === 0; i++) await T.sleep(20);
    const waited = await waiting();
    await T.sleep(500);
    const stillWaiting = await waiting();
    window.krabkaLab.fault({ kind: 'heal', a: 4, b: 2 });
    const answer = await reader.bytes(payload.length);
    conn.close();
    return { waited, stillWaiting, same: answer.every((b, i) => b === payload[i]) };
  });
  check('a dial across a cut link waits, and connects when the link heals', healed.waited === 1 && healed.stillWaiting === 1 && healed.same, JSON.stringify(healed));
  await page.evaluate(() => {
    window.krabkaLab.fault({ kind: 'partition', a: 4, b: 2 });
    window.krabkaLab.world.setSpeed(20);
  });
  const blackHole = await page.evaluate(async () => {
    const { T } = window;
    const world = window.krabkaLab.world;
    const sentAt = world.now();
    const text = await T.direct(4, T.enc.encode('DIAL 10.0.0.2:9092\nhello')).reader.end(30000);
    return { text: text.trim(), labMs: world.now() - sentAt };
  });
  await page.evaluate(() => {
    window.krabkaLab.world.setSpeed(1);
    window.krabkaLab.fault({ kind: 'heal', a: 4, b: 2 });
  });
  check('a dial into a cut link fails with ETIMEDOUT after 30 s of lab time', /^DIAL-ERR TimedOut/.test(blackHole.text) && blackHole.labMs >= 30000, JSON.stringify(blackHole));

  // Six 300 KiB Kafka frames each way: the process's stream is cut into one
  // frame per message for echo-b, and the echoes wait in the page while the
  // process's send buffer is full; every byte comes back, in order.
  const bulk = await page.evaluate(async () => {
    const { T } = window;
    const head = T.enc.encode('DIAL 10.0.0.2:9092\n');
    const frameBytes = 4 + 300 * 1024;
    const request = new Uint8Array(head.length + 6 * frameBytes);
    request.set(head);
    for (let i = 0; i < 6; i++) {
      const at = head.length + i * frameBytes;
      new DataView(request.buffer, at, 4).setInt32(0, frameBytes - 4);
      request.fill(i + 1, at + 4, at + frameBytes);
    }
    const framesBefore = window.krabkaLab.world.snapshot().nodes.find((n) => n.id === 2).state.frames;
    const { conn, reader } = T.direct(4, request);
    const back = await reader.bytes(6 * frameBytes, 60000);
    conn.close();
    await T.sleep(800);
    const framesAfter = window.krabkaLab.world.snapshot().nodes.find((n) => n.id === 2).state.frames;
    return { same: back.every((b, i) => b === request[head.length + i]), bytes: back.length, frames: framesAfter - framesBefore };
  });
  check('six 300 KiB frames relayed through the world come back byte for byte, one lab frame each', bulk.same && bulk.frames === 8, JSON.stringify(bulk));

  // Kill: the world refuses the pinger's connections; restart: the same volume.
  await page.evaluate(() => window.krabkaLab.fault({ kind: 'kill', node: 4 }));
  const killed = JSON.parse(await waitFor(page, processIs(4, 'killed'), 'the process to be killed'));
  const down = JSON.parse(await waitFor(page, nodeWhere(3, '(n) => true'), 'the pinger')).state;
  await page.waitForTimeout(1500);
  const later = JSON.parse(await waitFor(page, nodeWhere(3, '(n) => true'), 'the pinger')).state;
  check('kill stops the process and the node goes down', killed.alive === false, JSON.stringify(killed.state.process));
  check('a killed node refuses connections', later.echoes === down.echoes && later.closes > down.closes, `echoes ${down.echoes} -> ${later.echoes}, closes ${down.closes} -> ${later.closes}`);
  await page.evaluate(() => window.krabkaLab.fault({ kind: 'restart', node: 4 }));
  const restarted = await readyLine(page, 4, 2);
  const incarnation = JSON.parse(await waitFor(page, processIs(4, 'running'), 'the restarted process')).state.process.incarnation;
  check('restart brings the process back on the same volume (its first boot is still recorded)', restarted.endsWith('boots=2') && incarnation === 2, `${restarted}, incarnation ${incarnation}`);
  const againSelf = await selfDial(page, 4);
  check('the restarted process reaches its own controller listener again', againSelf === 'self-dial ok: 10.0.0.4:9093 echoed 23 bytes', againSelf);
  const echoesBefore = later.echoes;
  await waitFor(page, nodeWhere(3, `(n) => n.state.echoes > ${echoesBefore}`), 'echoes after the restart');
  check('the pinger gets echoes again', true);

  // Wipe: a fresh volume.
  await page.evaluate(() => window.krabkaLab.fault({ kind: 'wipe', node: 4 }));
  const wiped = await readyLine(page, 4, 1);
  const wipedInc = JSON.parse(await waitFor(page, processIs(4, 'running', 'n.state.process.incarnation === 3'), 'the wiped process')).state.process.incarnation;
  check('wipe forgets the volume: the process starts from nothing', wiped.endsWith('boots=1') && wipedInc === 3, `${wiped}, incarnation ${wipedInc}`);

  // A configuration change starts the node from nothing with the new FileConfig.
  await page.evaluate((config) => {
    const spec = window.krabkaLab.world.spec(4);
    window.krabkaLab.world.updateNode(4, { ...spec, config });
  }, RECONFIGURED);
  const reconfigured = await waitFor(page, nodeWhere(4, `(n) => n.state && Array.isArray(n.state.stdout) && n.state.stdout.some((l) => l.startsWith('ready ') && l.includes('rack'))`), 'the reconfigured process');
  const readyAgain = JSON.parse(reconfigured).state.stdout.find((l) => l.startsWith('ready '));
  check(
    'a configuration change restarts the process from nothing with its FileConfig in KRABKA_CONFIG',
    readyAgain.endsWith(`config=${RECONFIGURED_FILE_CONFIG} boots=1`),
    readyAgain,
  );

  // Pause stops the process's clock.
  const ticks = async () => Number((await page.evaluate(() => window.T.command(4, 'TICKS'))).split(' ')[1]);
  await page.evaluate(() => window.krabkaLab.world.setPaused(true));
  await page.waitForTimeout(400);
  const t1 = await ticks();
  const worldAt = await page.evaluate(() => window.krabkaLab.world.now());
  await page.waitForTimeout(1500);
  const t2 = await ticks();
  const clockMs = JSON.parse(await waitFor(page, nodeWhere(4, `(n) => n.state.runtime && n.state.runtime.clock_ms === ${worldAt}`), 'the process clock to show the paused world time', 10_000)).state.runtime.clock_ms;
  check("pausing the lab stops the process's clock", t1 === t2 && clockMs === worldAt, `${t1} -> ${t2} ticks, process clock ${clockMs} ms, world ${worldAt} ms`);
  await page.evaluate(() => window.krabkaLab.world.setPaused(false));
  await page.waitForTimeout(800);
  const t3 = await ticks();
  check('and running it again moves the clock on', t3 > t2, `${t2} -> ${t3}`);

  // An exit kills the node; the timeline and the inspector say why.
  await page.evaluate(() => window.T.direct(4, window.T.enc.encode('EXIT 3\n')));
  const exited = JSON.parse(await waitFor(page, processIs(4, 'exited', 'n.alive === false'), 'the exit'));
  check('a process that exits takes its node down', exited.alive === false && exited.state.process.exit.code === 3, JSON.stringify(exited.state.process.exit));
  const row = await waitFor(page, `(() => { const r = [...document.querySelectorAll('#krabka-lab .lab-ev[data-node="4"]')].find((li) => li.querySelector('.lab-ev-kind').textContent === 'process_exit'); return r ? r.querySelector('.lab-ev-detail').textContent : null; })()`, 'the timeline row');
  check('the timeline shows the exit and its code', /code 3/.test(row), row);
  await page.locator('#krabka-lab .lab-node[data-node-id="4"]').click();
  const shown = await waitFor(page, `(() => { const s = document.querySelector('#krabka-lab .lab-inspector dd[data-field="process_state"]'); const e = document.querySelector('#krabka-lab .lab-inspector dd[data-field="process_exit"]'); return s && e ? s.textContent + ' / ' + e.textContent : null; })()`, 'the inspector');
  check('the inspector shows the exit', shown === 'exited / exited with code 3', shown);
  const stderr = await waitFor(page, `document.querySelector('#krabka-lab .lab-inspector pre[data-field="stderr"]')?.textContent || null`, 'the stderr tail');
  check("the inspector shows the process's stderr", stderr.includes('[lab-guest] exiting with code 3 on request'), stderr.slice(-120));
  await page.evaluate(() => window.krabkaLab.fault({ kind: 'restart', node: 4 }));
  await waitFor(page, processIs(4, 'running', 'n.alive === true'), 'the process after the exit');
  await page.evaluate(() => window.T.direct(4, window.T.enc.encode('PANIC boom from the check\n')));
  const trapped = JSON.parse(await waitFor(page, processIs(4, 'trapped', 'n.alive === false'), 'the trap'));
  const trapRow = await waitFor(page, `(() => { const r = [...document.querySelectorAll('#krabka-lab .lab-ev[data-node="4"]')].find((li) => li.querySelector('.lab-ev-kind').textContent === 'process_trap'); return r ? r.querySelector('.lab-ev-detail').textContent : null; })()`, 'the trap in the timeline');
  const trapStderr = trapped.state.stderr.join('\n');
  check('a trap takes the node down, with the reason in the timeline and the stderr tail', /^trapped: /.test(trapped.state.process.reason) && /trapped/.test(trapRow) && trapStderr.includes('boom from the check'), `${trapped.state.process.reason} | ${trapRow}`);

  // The Storage panel lists the volume; it can be forgotten once nothing runs on it.
  await page.evaluate(() => {
    document.querySelector('#krabka-lab .lab-storage').open = true;
  });
  await page.locator('#krabka-lab .lab-dtab[data-tab="storage"]').click();
  const volume = `${scenarioId}/4`;
  await waitFor(page, `(() => { const td = document.querySelector('#krabka-lab tr[data-storage-volume="${volume}"] td[data-field="volume-bytes"]'); return td && td.textContent !== '0 B'; })()`, 'the volume in the Storage panel');
  check('the Storage panel lists the real broker volume', true);
  await page.locator(`#krabka-lab tr[data-storage-volume="${volume}"] button`, { hasText: 'Forget' }).click();
  await waitFor(page, `document.querySelector('#krabka-lab tr[data-storage-volume="${volume}"]') === null`, 'the volume to go');
  const left = await page.evaluate(async (v) => (await import('/playground/wasi/host.js')).listVolumes().then((l) => l.map((x) => x.id).includes(v)), volume);
  check('Forget deletes the volume', left === false);

  // In a session a real broker stays on the hub.
  await page.evaluate(() => window.krabkaLab.fault({ kind: 'restart', node: 4 }));
  await waitFor(page, processIs(4, 'running'), 'the process back');
  const pinned = await page.evaluate(() => {
    const s = window.krabkaLab.session;
    s.becomeHub();
    const moved = s.setHost(4, 'some-other-tab');
    const host = s.hostOf(4);
    window.krabkaLab.pushPanels();
    return { moved, host, me: s.me };
  });
  await page.locator('#krabka-lab .lab-node[data-node-id="4"]').click();
  const note = await waitFor(page, `document.querySelector('#krabka-lab .lab-inspector [data-field="pinned-note"]')?.textContent || null`, 'the pinned note');
  const pickers = await page.locator('#krabka-lab .lab-inspector select[data-host-select="4"]').count();
  check('a session keeps a real broker on the hub', pinned.moved === false && pinned.host === pinned.me && pickers === 0, JSON.stringify(pinned));
  check('and the inspector says why', /cannot move/.test(note), note);
  await page.evaluate(() => window.krabkaLab.session.leave());
  const link = await page.evaluate(() => import('/playground/lab/scenarios.js').then((m) => m.shareLink(window.krabkaLab.world.scenario())));
  await page.close();
  return link;
}

// The contract's pure parts, in Node, on the copy of `external.js` that ships.
async function checkContract() {
  console.log('Real broker: the process contract');
  const m = await import(pathToFileURL(path.join(DIST_DIR, 'playground', 'lab', 'external.js')).href);
  const addresses = [[1, '10.0.0.1'], [254, '10.0.0.254'], [258, '10.0.1.2'], [65535, '10.0.255.255']];
  const wrongIps = ['10.0.0.0', '10.1.0.1', '192.168.0.1', '10.0.0.01', '10.0.0.256', '10.0.0'];
  const badAddress = [
    ...addresses.filter(([id, ip]) => m.nodeIp(id) !== ip || m.nodeForIp(ip) !== id).map(([id, ip]) => `${id}/${ip}`),
    ...wrongIps.filter((ip) => m.nodeForIp(ip) !== null),
  ];
  check('virtual addresses name nodes both ways, as net::node_ip and node_for_ip do', badAddress.length === 0, badAddress.join(', '));

  const configs = [
    [{}, { voter: true, fileConfig: {} }],
    [{ voter: false }, { voter: false, fileConfig: {} }],
    [
      { voter: true, rack: 'a', num_partitions: 3, default_replication_factor: 2, min_insync_replicas: 1, replica_lag_time_max_ms: 10000 },
      { voter: true, fileConfig: { rack: 'a', runtime: { num_partitions: 3, default_replication_factor: 2, default_min_insync_replicas: 1 }, replica_lag_time_max: '10000ms' } },
    ],
  ];
  const badConfig = configs.filter(([config, want]) => canonical(m.parseConfig(config)) !== canonical(want)).map(([config]) => JSON.stringify(config));
  check('a node configuration maps onto the broker\'s FileConfig JSON', badConfig.length === 0, badConfig.join(', '));
  const ordered = JSON.stringify(m.parseConfig(RECONFIGURED).fileConfig);
  check('the FileConfig keys come in one order, whatever order the node keeps them in', ordered === RECONFIGURED_FILE_CONFIG, ordered);
  const refused = [
    { properties: {} },
    { voter: 'yes' },
    { num_partitions: 0 },
    { num_partitions: 1.5 },
    { num_partitions: 2 ** 31 },
    { rack: '' },
    { default_replication_factor: 40000 },
    { replica_lag_time_max_ms: '10000' },
  ];
  const accepted = refused.filter((config) => {
    try {
      m.parseConfig(config);
      return true;
    } catch {
      return false;
    }
  });
  check('an unknown key or a value of the wrong type is refused', accepted.length === 0, accepted.map((c) => JSON.stringify(c)).join(', '));

  const scenario = {
    nodes: [
      { id: 3, kind: 'krabka-broker', config: {} },
      { id: 1, kind: 'krabka-broker', config: { voter: false } },
      { id: 2, kind: 'krabka-broker', config: { voter: true, rack: 'b' } },
      { id: 5, kind: 'broker', config: { broker_id: 5 } },
      { id: 6, kind: 'krabka-broker', config: { unknown: 1 } },
    ],
  };
  const voters = m.votersOf(scenario);
  check('the voters are the real brokers with voter set, in ascending id', JSON.stringify(voters) === '[2,3]', JSON.stringify(voters));
  const env = m.processEnv({ nodeId: 258, voters, clusterId: 'AAAAAAAAAAAAAAAAAAAAAA', fileConfig: m.parseConfig({ rack: 'b' }).fileConfig });
  check(
    'the environment of a process',
    canonical(env) === canonical({ KRABKA_NODE_ID: '258', KRABKA_HOST: '10.0.1.2', KRABKA_VOTERS: '2@10.0.0.2:9093,3@10.0.0.3:9093', KRABKA_CLUSTER_ID: 'AAAAAAAAAAAAAAAAAAAAAA', KRABKA_CONFIG: '{"rack":"b"}' }),
    JSON.stringify(env),
  );
  const ids = ['b6cd1ed5-86cc-410a-ace6-5699cf81895e', 'abc', ''];
  const clusters = await Promise.all(ids.map((id) => m.clusterIdFor(id)));
  const badCluster = ids.filter((id, i) => clusters[i] !== expectedClusterId(id) || !/^[A-Za-z0-9_][A-Za-z0-9_-]{21}$/.test(clusters[i]));
  check('the cluster id is the documented hash of the scenario id, 22 characters of Kafka base64', badCluster.length === 0, clusters.join(', '));

  const enc = new TextEncoder();
  const frame = (text) => {
    const body = enc.encode(text);
    const out = new Uint8Array(4 + body.length);
    new DataView(out.buffer).setInt32(0, body.length);
    out.set(body, 4);
    return out;
  };
  const join = (...parts) => Uint8Array.from(parts.flatMap((p) => [...p]));
  const hex = (chunks) => chunks.map((c) => Buffer.from(c).toString('hex')).join('|');
  const a = frame('first frame');
  const b = frame('second');
  const cuts = [
    ['one frame in one chunk', [a], [a]],
    ['one frame in three chunks', [a.subarray(0, 2), a.subarray(2, 7), a.subarray(7)], [a]],
    ['two frames in one chunk', [join(a, b)], [a, b]],
    ['a frame and a half, then the rest', [join(a, b.subarray(0, 5)), b.subarray(5)], [a, b]],
    ['a stream that is not Kafka passes through', [enc.encode('ping 1'), enc.encode('ping 2')], [enc.encode('ping 1'), enc.encode('ping 2')]],
  ];
  const badCut = cuts.filter(([, chunks, want]) => {
    const framer = new m.KafkaFramer();
    return hex(chunks.flatMap((c) => framer.push(c))) !== hex(want);
  });
  check('a byte stream is cut into Kafka frames', badCut.length === 0, badCut.map(([name]) => name).join(', '));
  const whole = [[a, true], [join(a, Uint8Array.of(0)), false], [enc.encode('ping 1'), false], [Uint8Array.of(0, 0, 0), false]];
  const badWhole = whole.filter(([bytes, want]) => m.isKafkaFrame(bytes) !== want);
  check('exactly one Kafka frame is told apart', badWhole.length === 0, `${badWhole.length} cases`);
}

// Opening a shared scenario with a real broker reloads the page once and keeps it.
async function openShared(browser, base, link, errors) {
  console.log('Real broker: opening a shared scenario that has one');
  const context = await newLabContext(browser, { width: 1400, height: 1000 });
  await context.addInitScript(installHelpers);
  await context.addInitScript((url) => sessionStorage.setItem('krabka-lab.broker-module', url), GUEST_URL);
  const page = await context.newPage();
  errors.push(...watchErrors(page, 'shared page', base));
  let navigations = 0;
  page.on('load', () => {
    navigations += 1;
  });
  await page.goto(link, { waitUntil: 'load' });
  await waitFor(page, `self.crossOriginIsolated && document.querySelector('#krabka-lab[data-ready="true"]') !== null`, 'the isolated reload');
  const running = JSON.parse(await waitFor(page, processIs(4, 'running'), 'the process to run'));
  const opened = await page.evaluate(() => ({
    hash: location.hash,
    id: window.krabkaLab.world.id,
    nodes: window.krabkaLab.world.scenario().nodes.map((n) => `${n.id}:${n.kind}`),
  }));
  check(
    'opening a shared scenario with a real broker keeps the scenario',
    navigations >= 1 && navigations <= 2 && opened.hash === '' && Boolean(opened.id) && JSON.stringify(opened.nodes) === JSON.stringify(['1:echo', '2:echo', '3:pinger', '4:krabka-broker']),
    `${navigations} navigations, ${JSON.stringify(opened)}`,
  );
  check('and its real broker runs there', running.state.env.KRABKA_CLUSTER_ID === expectedClusterId(opened.id), running.state.env.KRABKA_CLUSTER_ID);
  await context.close();
}

async function main() {
  if (!fs.existsSync(path.join(DIST_DIR, 'docs', 'lab', 'index.html'))) {
    console.error('dist/docs/lab/index.html is missing: run `npm run build` first.');
    process.exit(1);
  }
  await checkContract();
  let guest;
  try {
    guest = buildGuest();
  } catch (err) {
    console.error(`The WASI test guest did not build: ${err.message}`);
    process.exit(2);
  }
  console.log(`  guest: ${path.relative(ROOT, guest).startsWith('..') ? guest : path.relative(ROOT, guest)} (${fs.statSync(guest).size.toLocaleString('en')} bytes)`);
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
  const { server, port } = await serve(DIST_DIR, guest);
  const base = `http://127.0.0.1:${port}`;
  const errors = [];
  try {
    const plain = await newLabContext(browser, { width: 1400, height: 1000 });
    await plain.addInitScript(installHelpers);
    await missingBuild(plain, base, errors);
    await plain.close();
    const context = await newLabContext(browser, { width: 1400, height: 1000 });
    await context.addInitScript(installHelpers);
    const link = await realBroker(context, base, errors);
    await context.close();
    await openShared(browser, base, link, errors);
  } catch (err) {
    failures.push(`exception: ${err.message}`);
    console.error(`  FAIL exception: ${err.stack || err.message}`);
  } finally {
    await browser.close();
    server.close();
  }
  check('no page errors or console errors', errors.length === 0, errors.slice(0, 3).join(' | '));
  console.log(`\n${passed} checks passed${failures.length ? `, ${failures.length} failed` : ''}`);
  if (failures.length) {
    for (const f of failures) console.error(`  • ${f}`);
    process.exit(1);
  }
  console.log('✅ PASS: real brokers run as lab nodes.');
}

main();
