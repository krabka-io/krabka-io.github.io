// End-to-end check of the Cluster Lab page (`/docs/lab`) in headless Chromium.
//
// Serves the built site from `dist/`, opens the lab, and drives it the way a
// reader would: the default preset runs, frames cross the canvas, the
// inspector shows the pinger's round trips, a kill and a restart take effect,
// the echo node's frame counter survives a page reload through IndexedDB and
// comes back when the saved scenario is reopened from the "Saved" list, the
// durable state stored after persistence is turned back on equals the live
// state, a share link reproduces the
// scenario even without `DecompressionStream`, and two pages in one browser
// host a cluster together over WebRTC with the link latency intact and the
// scenario edited only by the host. Before the browser starts, the
// raw-DEFLATE fallback round-trips `CompressionStream` output in Node.
//
// The cluster presets run at 20× in a browser context of their own. Three
// brokers form one KRaft quorum, create the topic and serve two consumers
// that share its partitions; the inspector shows the quorum and the
// partitions; the command bars pause, resume, send, set rates and
// processing times, commit, seek, close and query; killing the leader of a partition
// moves the leadership in the inspector while the group keeps consuming,
// and the restarted broker rejoins the ISR; a page reload brings the brokers
// back from IndexedDB and the group resumes from its committed offsets; a
// broker added to the running scenario observes the quorum and says why. The
// registry preset's producer registers its schema, the registry answers
// `GET /subjects` and the consumer decodes every value; schemas registered
// through REST with persistence on and off come back after a reload from the
// brokers' `_schemas` log, since the registry keeps nothing of its own; a
// second registry joins the group as a secondary and serves a write by
// forwarding it to the primary; the streams preset
// counts words into its store, with the changelog topic the group created,
// and answers a store query; in the five-broker preset the majority serves
// while two brokers are cut off, and they join once the links heal.
//
// Usage:  npm run build && npm run check-lab [-- --no-webrtc] [--no-cluster] [--headed]
// Needs `playwright` or `playwright-core`, project-local or global (found
// through `npm root -g`), and a Chromium: the one that Playwright finds by
// itself (its own download, `npx playwright install chromium`, or the build
// it expects under PLAYWRIGHT_BROWSERS_PATH), or else the newest `chromium-N`
// under PLAYWRIGHT_BROWSERS_PATH. The fallback covers a project-local
// `playwright-core` newer than the installed browsers, which is what
// `npm run check-wasi` leaves behind. Exits 2 when either is missing, 1 when
// a check fails.

import fs from 'fs';
import http from 'http';
import path from 'path';
import crypto from 'crypto';
import { createRequire } from 'module';
import { execSync } from 'child_process';
import { fileURLToPath, pathToFileURL } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DIST_DIR = path.resolve(__dirname, '..', 'dist');
const args = new Set(process.argv.slice(2));
const WEBRTC = !args.has('--no-webrtc');
const CLUSTER = !args.has('--no-cluster');
const HEADLESS = !args.has('--headed');
const STEP_TIMEOUT = 30_000;

// ---- Playwright, project-local or global ----------------------------------------------------

async function loadPlaywright() {
  const require = createRequire(import.meta.url);
  // `playwright-core` is what `npm i --no-save playwright-core` brings; it has
  // the same `chromium` API without the browser download hooks.
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

// The newest Chromium under PLAYWRIGHT_BROWSERS_PATH, for a Playwright whose
// own browser build is not installed there; undefined when there is none. A
// headless run prefers the headless shell, as Playwright itself does.
function installedChromium() {
  const base = process.env.PLAYWRIGHT_BROWSERS_PATH;
  if (!base || !fs.existsSync(base)) return undefined;
  const builds = [
    ['chromium', ['chrome-linux/chrome', 'chrome-linux64/chrome', 'chrome-mac/Chromium.app/Contents/MacOS/Chromium', 'chrome-win/chrome.exe']],
  ];
  if (HEADLESS) {
    builds.unshift(['chromium_headless_shell', ['chrome-headless-shell-linux64/chrome-headless-shell', 'chrome-linux/headless_shell']]);
  }
  const entries = fs.readdirSync(base);
  for (const [name, executables] of builds) {
    const pattern = new RegExp(`^${name}-(\\d+)$`);
    const dirs = entries
      .filter((d) => pattern.test(d))
      .sort((a, b) => Number(b.match(pattern)[1]) - Number(a.match(pattern)[1]));
    for (const dir of dirs) {
      for (const executable of executables) {
        const candidate = path.join(base, dir, executable);
        if (fs.existsSync(candidate)) return candidate;
      }
    }
  }
  return undefined;
}

// Launch Chromium, falling back to `installedChromium()` when Playwright's own
// build is missing.
async function launchChromium(pw) {
  try {
    return await pw.chromium.launch({ headless: HEADLESS });
  } catch (err) {
    const executablePath = installedChromium();
    if (!executablePath) throw err;
    return pw.chromium.launch({ headless: HEADLESS, executablePath });
  }
}

// ---- a static server over dist/ -----------------------------------------------------------

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

function serve(dir) {
  const server = http.createServer((req, res) => {
    let p = decodeURIComponent(new URL(req.url, 'http://x').pathname);
    let file = path.join(dir, p);
    if (!file.startsWith(dir)) {
      res.writeHead(403).end();
      return;
    }
    if (fs.existsSync(file) && fs.statSync(file).isDirectory()) file = path.join(file, 'index.html');
    else if (!fs.existsSync(file) && fs.existsSync(`${file}.html`)) file = `${file}.html`;
    if (!fs.existsSync(file)) {
      res.writeHead(404).end('not found');
      return;
    }
    res.writeHead(200, { 'content-type': MIME[path.extname(file)] || 'application/octet-stream' });
    fs.createReadStream(file).pipe(res);
  });
  return new Promise((resolve) => server.listen(0, '127.0.0.1', () => resolve({ server, port: server.address().port })));
}

// ---- the checks ------------------------------------------------------------------------------

let passed = 0;
const failures = [];
function check(name, ok, detail) {
  if (ok) {
    passed += 1;
    console.log(`  ok   ${name}`);
  } else {
    failures.push(`${name}${detail ? ` (${detail})` : ''}`);
    console.error(`  FAIL ${name}${detail ? ` (${detail})` : ''}`);
  }
}

async function waitFor(page, fn, label, timeout = STEP_TIMEOUT) {
  const start = Date.now();
  let last;
  while (Date.now() - start < timeout) {
    last = await page.evaluate(fn);
    if (last) return last;
    await page.waitForTimeout(100);
  }
  throw new Error(`timed out waiting for ${label}`);
}

// Page errors and console errors from the page's own origin. A font or a
// script from another origin that fails to load (no network in CI, a proxy)
// is not a lab failure and is left out.
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

async function openLab(page, base, hash = '') {
  await page.goto(`${base}/docs/lab/${hash}`, { waitUntil: 'load' });
  await page.waitForSelector('#krabka-lab[data-ready="true"]', { timeout: STEP_TIMEOUT });
}

const nodeState = (id) => `(() => { const s = window.krabkaLab.world.snapshot(); const n = s && s.nodes.find((x) => x.id === ${id}); return n ? JSON.stringify({ alive: n.alive, hosted: n.hosted, isolated: n.isolated, state: n.state }) : null; })()`;

// The node's snapshot entry, waiting for the first snapshot after a load.
async function readNode(page, id) {
  const raw = await waitFor(page, nodeState(id), `a snapshot of node ${id}`);
  return JSON.parse(raw);
}

// ---- the raw-DEFLATE fallback, in Node ----------------------------------------------------

// Round-trip `CompressionStream("deflate-raw")` output through the decoder the
// page falls back on, the copy in dist/ that ships: several sizes, past 64 KiB
// (random data is stored uncompressed, in blocks of at most 65535 bytes), and
// random, repetitive and scenario-like data.
async function checkInflate() {
  console.log('Share codes: the raw-DEFLATE fallback');
  if (typeof CompressionStream !== 'function') {
    check('Node has CompressionStream', false, 'needs Node 21.2 or later');
    return;
  }
  const { inflateRaw } = await import(pathToFileURL(path.join(DIST_DIR, 'playground', 'lab', 'inflate.js')).href);
  const deflate = async (bytes) =>
    new Uint8Array(await new Response(new Blob([bytes]).stream().pipeThrough(new CompressionStream('deflate-raw'))).arrayBuffer());
  const make = {
    random: (n) => new Uint8Array(crypto.randomBytes(n)),
    repetitive: (n) => Uint8Array.from({ length: n }, (_, i) => 'krabka-lab '.charCodeAt(i % 11) ^ (i >> 12 & 1)),
    scenario: (n) => {
      const text = JSON.stringify({ nodes: Array.from({ length: Math.ceil(n / 40) + 1 }, (_, i) => ({ id: i, kind: i % 3 ? 'broker' : 'consumer', x: (i * 37) % 900 })) });
      return new TextEncoder().encode(text).slice(0, n);
    },
  };
  const sizes = [0, 1, 1000, 65535, 65536, 65537, 70000, 300000];
  const bad = [];
  let cases = 0;
  for (const size of sizes) {
    for (const [kind, gen] of Object.entries(make)) {
      cases += 1;
      const input = gen(size);
      let out;
      try {
        out = inflateRaw(await deflate(input));
      } catch (err) {
        out = err;
      }
      if (!(out instanceof Uint8Array) || Buffer.compare(Buffer.from(out), Buffer.from(input)) !== 0) {
        bad.push(`${kind}/${size}: ${out instanceof Error ? out.message : `${out.length} bytes`}`);
      }
    }
  }
  check(`inflateRaw round-trips CompressionStream('deflate-raw') output (${cases} cases, up to ${sizes[sizes.length - 1]} bytes)`, bad.length === 0, bad.slice(0, 3).join('; '));
  const packed = await deflate(make.repetitive(20000));
  let threw = false;
  try {
    inflateRaw(packed.subarray(0, packed.length >> 1));
  } catch {
    threw = true;
  }
  check('inflateRaw rejects a truncated stream', threw);
}

// Turn "Persist to this browser" on or off through the Storage panel, the way
// a reader does, so the app's own handler runs.
async function setPersistUI(page, on) {
  await page.evaluate(() => {
    document.querySelector('#krabka-lab .lab-storage').open = true;
  });
  const box = page.locator('#krabka-lab .lab-storage input[type="checkbox"]');
  if ((await box.isChecked()) !== on) await box.click();
  await waitFor(page, `window.krabkaLab.storage.persist === ${on}`, `persistence ${on ? 'on' : 'off'}`);
}

const avro = (name, fields) => JSON.stringify({ type: 'record', name, namespace: 'io.krabka.lab', fields });
// The registry preset's Order schema with one more field, which has a
// default: a BACKWARD-compatible second version of orders-value.
const ORDER_V2 = avro('Order', [{ name: 'id', type: 'long' }, { name: 'customer', type: 'string' }, { name: 'total', type: 'double' }, { name: 'note', type: 'string', default: '' }]);
const PAYMENT_V1 = avro('Payment', [{ name: 'id', type: 'long' }, { name: 'amount', type: 'double' }]);
const INVENTORY_V1 = avro('Inventory', [{ name: 'sku', type: 'string' }, { name: 'count', type: 'int' }]);
const SHIPMENT_V1 = avro('Shipment', [{ name: 'order', type: 'long' }, { name: 'carrier', type: 'string' }]);

// What the store holds for the echo counter of node 1.
const storedFrames = (page, scenarioId) =>
  page.evaluate(async (id) => {
    const images = await window.krabkaLab.storage.loadImages(id);
    const b64 = images['1']?.kv?.counters?.frames;
    return b64 == null ? null : Number(atob(b64));
  }, scenarioId);

// ---- the KRaft cluster presets ---------------------------------------------------------------

// The presets as the site ships them, for their names and node ids.
async function loadPresets() {
  return (await import(pathToFileURL(path.join(DIST_DIR, 'playground', 'lab', 'presets.js')).href)).PRESETS;
}

// Load a preset the way a reader does, from its button, and run it at 20×.
async function openPreset(page, preset) {
  await page.locator(`#krabka-lab .lab-preset-btn[data-preset="${preset.id}"]`).click();
  await waitFor(page, `window.krabkaLab.world.scenario().name === ${JSON.stringify(preset.name)}`, `the ${preset.id} preset`);
  await fastest(page);
}

async function fastest(page) {
  await page.locator('#krabka-lab select[aria-label="Simulation speed"]').selectOption('20');
}

// Wait until `fn`, the source of a function of the snapshot's nodes by id,
// returns something truthy, and return it. It runs in the page, so it can
// only use what it is given.
function until(page, label, fn, timeout = 60_000) {
  const expr = `(() => { const s = window.krabkaLab.world.snapshot(); if (!s) return null; const n = {}; for (const x of s.nodes) n[x.id] = x; try { const r = (${fn})(n); return r ? JSON.stringify(r) : null; } catch { return null; } })()`;
  return waitFor(page, expr, label, timeout).then((r) => JSON.parse(r));
}

const nodeStateOf = (page, id) => page.evaluate((id) => window.krabkaLab.world.snapshot()?.nodes.find((x) => x.id === id)?.state ?? null, id);

// Fit every card into the canvas, as a reader does after adding a node.
async function fit(page) {
  await page.locator('#krabka-lab .lab-canvas-tools button', { hasText: 'Fit' }).click();
}

// Select a node on the canvas and wait for the inspector to show it.
async function inspect(page, id, name) {
  await page.locator(`#krabka-lab .lab-node[data-node-id="${id}"]`).click();
  await waitFor(page, `document.querySelector('#krabka-lab .lab-inspector .lab-insp-name')?.textContent === ${JSON.stringify(name)}`, `the inspector on ${name}`);
}

// Run a command from the inspector's command bar, after filling its inputs,
// and return what the bar says.
async function command(page, cmd, params = {}) {
  const row = page.locator(`#krabka-lab .lab-insp-commands .lab-cmd[data-command="${cmd}"]`);
  for (const [key, value] of Object.entries(params)) {
    const input = row.locator(`[data-param="${key}"]`);
    if ((await input.evaluate((e) => e.tagName)) === 'SELECT') await input.selectOption(String(value));
    else await input.fill(String(value));
  }
  await page.evaluate(() => {
    const r = document.querySelector('#krabka-lab .lab-insp-commands [data-field="command-result"]');
    if (r) r.dataset.command = '';
  });
  await row.locator(`button[data-command="${cmd}"]`).click();
  const text = await waitFor(page, `(() => { const r = document.querySelector('#krabka-lab .lab-insp-commands [data-field="command-result"]'); return r && r.dataset.command === ${JSON.stringify(cmd)} ? JSON.stringify({ ok: r.dataset.ok === 'true', text: r.textContent }) : null; })()`, `the answer to ${cmd}`);
  return JSON.parse(text);
}

// The text of one cell of an inspector table.
const cell = (page, row, col) =>
  page.evaluate(([row, col]) => document.querySelector(`#krabka-lab .lab-inspector tr[data-row="${row}"] td[data-col="${col}"]`)?.textContent ?? null, [row, col]);

// The text of one key/value row of the inspector.
const field = (page, name) => page.evaluate((name) => document.querySelector(`#krabka-lab .lab-inspector dd[data-field="${name}"]`)?.textContent ?? null, name);

async function checkClusters(browser, base, errors) {
  const presets = await loadPresets();
  const byId = (id) => presets.find((p) => p.id === id);
  // A context of its own: its IndexedDB and last scenario are the cluster's.
  const context = await browser.newContext({ viewport: { width: 1400, height: 1000 } });
  const page = await context.newPage();
  const pageErrors = watchErrors(page, 'cluster page', base);
  await openLab(page, base);
  // Each flow on its own: one that fails does not hide the others.
  for (const [flow, preset] of [
    [checkThreeBrokers, 'three-brokers'],
    [checkRegistryPreset, 'schema-registry'],
    [checkWordCount, 'streams-word-count'],
    [checkPartitionPreset, 'five-brokers-partition'],
  ]) {
    try {
      await flow(page, byId(preset));
    } catch (err) {
      failures.push(`${preset}: ${err.message}`);
      console.error(`  FAIL ${preset}: ${err.stack || err.message}`);
    }
  }
  errors.push(...pageErrors);
  await context.close();
}

async function checkThreeBrokers(page, preset) {
  console.log(`Cluster Lab: ${preset.name}`);
  await openPreset(page, preset);
  // Brokers 1 to 3, the producer 4, the consumers 5 and 6.
  const quorum = await until(page, 'the quorum and the topic', `(n) => {
    const b = [1, 2, 3].map((i) => n[i] && n[i].state);
    if (!b.every((s) => s && s.state === 'RUNNING')) return null;
    const orders = b[0].topics.find((t) => t.name === 'orders');
    const leaders = b.filter((s) => s.quorum.role === 'Leader');
    if (!orders || leaders.length !== 1) return null;
    return { voters: b.map((s) => s.quorum.voters.join(',')), votes: b.map((s) => s.quorum.voter), controllers: b.map((s) => s.controller_id), leader: leaders[0].broker_id, replicas: orders.partitions.map((p) => p.replicas.length) };
  }`);
  check('three brokers run one KRaft quorum, voters 1, 2 and 3, with one active controller', quorum.voters.every((v) => v === '1,2,3') && quorum.votes.every(Boolean) && quorum.controllers.every((c) => c === quorum.leader), JSON.stringify(quorum));
  check('the controller creates orders with three partitions of three replicas', JSON.stringify(quorum.replicas) === '[3,3,3]', JSON.stringify(quorum));

  const group = await until(page, 'both consumers to consume', `(n) => {
    const c = [5, 6].map((i) => n[i].state);
    if (!c.every((s) => s && s.state === 'stable' && s.processed > 0 && s.assignment.length)) return null;
    return c.map((s) => ({ parts: s.assignment.map((a) => a.topic + '-' + a.partition), processed: s.processed }));
  }`, 90_000);
  const shared = [...group[0].parts, ...group[1].parts].sort();
  check('the two consumers of the group share the three partitions', JSON.stringify(shared) === '["orders-0","orders-1","orders-2"]', JSON.stringify(group));
  check('and both of them consume', group.every((c) => c.processed > 0), JSON.stringify(group));

  // The broker inspector: the quorum, and the partitions it hosts.
  await inspect(page, 2, 'broker-2');
  await waitFor(page, `document.querySelector('#krabka-lab .lab-inspector dd[data-field="quorum_voters"]')?.textContent === '1, 2, 3'`, 'the quorum in the inspector');
  const insp = { votes: await field(page, 'quorum_votes'), state: await field(page, 'state'), leader: await cell(page, 'orders-0', 'leader'), isr: await cell(page, 'orders-0', 'isr') };
  check('the broker inspector shows the quorum and the partitions', insp.votes === 'yes: a voter' && insp.state === 'RUNNING' && /^[123]$/.test(insp.leader) && insp.isr.split(' ').length === 3, JSON.stringify(insp));

  // The producer's command bar.
  await inspect(page, 4, 'orders-producer');
  const paused = await command(page, 'pause');
  const atPause = await until(page, 'the producer to pause', `(n) => n[4].state.paused === true && { generated: n[4].state.generated, now: window.krabkaLab.world.now() }`);
  await waitFor(page, `window.krabkaLab.world.now() > ${atPause.now} + 3000`, 'three seconds to pass');
  const stillPaused = (await nodeStateOf(page, 4)).generated;
  check('Pause stops the producer', paused.ok && stillPaused === atPause.generated, `${JSON.stringify(paused)}; ${atPause.generated} -> ${stillPaused}`);
  const sent = await command(page, 'send', { count: 7 });
  const afterSend = await until(page, 'Send to generate seven records', `(n) => n[4].state.generated === ${atPause.generated + 7} && n[4].state`);
  check('Send generates records at once, paused or not', sent.ok && /"generated":7/.test(sent.text) && afterSend.paused === true, sent.text);
  await command(page, 'resume');
  const rate = await command(page, 'rate', { rate_per_sec: 10 });
  const resumed = await until(page, 'the producer to resume at ten a second', `(n) => n[4].state.paused === false && n[4].state.rate === 10 && n[4].state`);
  check('Resume and Set rate take effect', rate.ok && resumed.rate === 10, rate.text);
  await command(page, 'rate', { rate_per_sec: 5 });

  // The consumer's command bar.
  await inspect(page, 5, 'billing-1');
  // Paused, the consumer takes no records; like Kafka's fetcher, its client
  // does not fetch a partition while records wait in its buffer.
  await command(page, 'pause');
  const held = await until(page, 'the consumer to pause', `(n) => n[5].state.paused === true && { processed: n[5].state.processed, now: window.krabkaLab.world.now() }`);
  await waitFor(page, `window.krabkaLab.world.now() > ${held.now} + 3000`, 'three seconds to pass');
  const stillHeld = (await nodeStateOf(page, 5)).processed;
  check('Pause stops the consumer taking records', stillHeld === held.processed, `${held.processed} -> ${stillHeld}`);
  await command(page, 'resume');
  const caught = await until(page, 'the resumed consumer to catch up', `(n) => n[5].state.paused === false && n[5].state.lag === 0 && n[5].state.processed > ${stillHeld} && n[5].state`);
  check('Resume lets it catch up', caught.lag === 0, `processed ${stillHeld} -> ${caught.processed}`);
  const slow = await command(page, 'process_ms', { ms: 5 });
  await until(page, 'the new processing time', `(n) => n[5].state.process_ms === 5`);
  check('Set processing changes the time per record', slow.ok, slow.text);
  await command(page, 'process_ms', { ms: 2 });
  const commits = (await nodeStateOf(page, 5)).commits;
  const commit = await command(page, 'commit');
  await until(page, 'the commit', `(n) => n[5].state.commits > ${commits}`);
  check('Commit now commits', commit.ok, commit.text);
  // Seek, with the producer held so what the consumer reads next is the
  // records it read before.
  await page.evaluate(() => window.krabkaLab.control(4, { cmd: 'pause' }));
  await until(page, 'billing-1 to catch up', `(n) => n[4].state.paused && n[5].state.lag === 0 && n[5].state.processing_backlog === 0`);
  const [row] = (await nodeStateOf(page, 5)).assignment.filter((a) => a.position >= 3);
  const back = row.position - 3;
  const seek = await command(page, 'seek', { topic: row.topic, partition: row.partition, offset: back });
  const reread = await until(page, 'the consumer to read again from the offset', `(n) => {
    const r = n[5].state.last_records.filter((x) => x.topic === '${row.topic}' && x.partition === ${row.partition}).map((x) => x.offset);
    return r.includes(${back}) && r.includes(${row.position - 1}) && r.slice(-3);
  }`);
  check(`Seek reads ${row.topic}-${row.partition} again from offset ${back}`, seek.ok && JSON.stringify(reread) === JSON.stringify([back, back + 1, back + 2]), `${seek.text} ${JSON.stringify(reread)}`);
  await page.evaluate(() => window.krabkaLab.control(4, { cmd: 'resume' }));

  // Kill the leader of orders-0: another broker's inspector shows the
  // leadership move, and the group keeps consuming.
  await inspect(page, 1, 'broker-1');
  await waitFor(page, `/^[123]$/.test(document.querySelector('#krabka-lab .lab-inspector tr[data-row="orders-0"] td[data-col="leader"]')?.textContent || '')`, 'the leader of orders-0');
  const leader = Number(await cell(page, 'orders-0', 'leader'));
  const before = await until(page, 'the log end of orders-0', `(n) => { const p = n[${leader}].state.topics.find((t) => t.name === 'orders').partitions.find((x) => x.index === 0); return { hwm: p.hwm, processed: n[5].state.processed + n[6].state.processed }; }`);
  await inspect(page, leader, `broker-${leader}`);
  await page.locator('#krabka-lab .lab-faults button', { hasText: 'Kill' }).click();
  await waitFor(page, `(() => { const r = ${nodeState(leader)}; return r && JSON.parse(r).alive === false; })()`, `broker ${leader} to be down`);
  const other = [1, 2, 3].find((b) => b !== leader);
  await inspect(page, other, `broker-${other}`);
  const moved = await waitFor(page, `(() => { const v = document.querySelector('#krabka-lab .lab-inspector tr[data-row="orders-0"] td[data-col="leader"]')?.textContent; return v && v !== '${leader}' && v !== 'none' ? v : null; })()`, 'the leadership of orders-0 to move', 60_000);
  check(`killing broker ${leader}, the leader of orders-0, moves the leadership to broker ${moved} in the inspector`, Number(moved) !== leader);
  const kept = await until(page, 'the group to consume past the kill', `(n) => {
    const c = [5, 6].map((i) => n[i].state);
    const past = c.some((s) => s.last_records.some((r) => r.partition === 0 && r.offset >= ${before.hwm}));
    return c[0].processed + c[1].processed >= ${before.processed} + 15 && past && { processed: c[0].processed + c[1].processed };
  }`, 90_000);
  check('the consumers keep consuming, orders-0 from its new leader', kept.processed >= before.processed + 15, JSON.stringify({ before, kept }));
  await inspect(page, leader, `broker-${leader}`);
  await page.locator('#krabka-lab .lab-faults button', { hasText: 'Restart' }).click();
  const rejoined = await until(page, `broker ${leader} to rejoin the ISR`, `(n) => {
    const s = n[${leader}].state;
    if (!n[${leader}].alive || s.state !== 'RUNNING') return null;
    const p = n[${moved}].state.topics.find((t) => t.name === 'orders').partitions.find((x) => x.index === 0);
    return p.isr.includes(${leader}) && { isr: p.isr };
  }`, 90_000);
  check(`restarted, broker ${leader} catches up and rejoins the ISR of orders-0`, rejoined.isr.length === 3, JSON.stringify(rejoined));

  await checkReload(page);

  // A broker added to the running scenario observes the quorum, and says why.
  await page.locator('#krabka-lab .lab-kind-btn[data-kind="broker"]').click();
  await page.waitForSelector('#krabka-lab dialog[open]');
  await page.locator('#krabka-lab dialog button[type="submit"]').click();
  const added = await waitFor(page, `(() => { const n = window.krabkaLab.world.scenario().nodes.find((x) => x.kind === 'broker' && x.id > 3); return n ? n.id : null; })()`, 'the added broker');
  await fit(page);
  await inspect(page, added, `broker-${added}`);
  await waitFor(page, `document.querySelector('#krabka-lab .lab-inspector dd[data-field="quorum_votes"]')?.textContent === 'no: an observer'`, 'the added broker to observe', 60_000);
  const why = await page.evaluate(() => document.querySelector('#krabka-lab .lab-inspector [data-field="observer-note"]')?.textContent || '');
  const observed = await page.evaluate((id) => window.krabkaLab.timeline.events.some((e) => e.node === id && e.kind === 'quorum_observer'), added);
  check('a broker added to the running scenario observes the static quorum, and the inspector says why', /joined a running scenario/.test(why) && /1, 2, 3/.test(why) && observed, why);

  // Close: billing-2 commits and leaves, and billing-1 takes its partitions.
  await inspect(page, 6, 'billing-2');
  const closed = await command(page, 'close');
  const alone = await until(page, 'billing-1 to take every partition', `(n) => n[6].state.closed === true && n[6].state.assignment.length === 0 && n[5].state.assignment.length === 3 && n[5].state.assignment.map((a) => a.partition)`);
  check('Close commits and leaves the group, and the other member takes its partitions over', closed.ok && alone.length === 3, closed.text);
}

// The cluster survives a page reload through IndexedDB, and the consumers
// resume from their committed offsets. The producer is set to rate 0 in its
// config first, so after the reload nothing new arrives until it is set back:
// a consumer that started over would read the old records again.
async function checkReload(page) {
  await inspect(page, 4, 'orders-producer');
  await page.locator('#krabka-lab .lab-tab#lab-tab-config').click();
  await page.getByLabel('Records per second').fill('0');
  await page.locator('#krabka-lab .lab-tabpanel[data-tab="config"] button', { hasText: 'Apply' }).click();
  await waitFor(page, `window.krabkaLab.world.spec(4).config.rate_per_sec === 0`, 'the producer to stop');
  for (const [id, name] of [[5, 'billing-1'], [6, 'billing-2']]) {
    await until(page, `${name} to catch up`, `(n) => n[${id}].state.lag === 0 && n[${id}].state.processing_backlog === 0`, 60_000);
    await inspect(page, id, name);
    await command(page, 'commit');
  }
  const committed = await until(page, 'the commits to cover every record', `(n) => {
    const rows = [5, 6].flatMap((i) => n[i].state.assignment);
    if (rows.length !== 3 || !rows.every((a) => a.committed != null && a.committed === a.hwm && a.hwm > 0)) return null;
    return Object.fromEntries(rows.map((a) => [a.topic + '-' + a.partition, a.committed]));
  }`, 60_000);
  const scenarioId = await page.evaluate(() => window.krabkaLab.world.id);
  await page.evaluate(() => window.krabkaLab.saveNow());
  await page.evaluate(() => window.krabkaLab.storage.flush());
  await page.reload({ waitUntil: 'load' });
  await page.waitForSelector('#krabka-lab[data-ready="true"]', { timeout: STEP_TIMEOUT });
  const reopened = await page.evaluate(() => ({ id: window.krabkaLab.world.id, nodes: window.krabkaLab.world.scenario().nodes.length }));
  check('a reload reopens the cluster', reopened.id === scenarioId && reopened.nodes >= 6, JSON.stringify(reopened));
  await fastest(page);
  const logs = await until(page, 'the brokers to serve again', `(n) => {
    if (![1, 2, 3].every((i) => n[i].state.state === 'RUNNING')) return null;
    const orders = n[1].state.topics.find((t) => t.name === 'orders');
    return orders && Object.fromEntries(orders.partitions.map((p) => ['orders-' + p.index, n[p.leader] ? n[p.leader].state.topics.find((t) => t.name === 'orders').partitions.find((x) => x.index === p.index).hwm : null]));
  }`, 90_000);
  check('the brokers restore their logs from IndexedDB and serve again', Object.entries(committed).every(([p, c]) => logs[p] >= c), JSON.stringify({ committed, logs }));
  const rejoined = await until(page, 'the consumers to rejoin with their commits', `(n) => {
    const rows = [5, 6].flatMap((i) => n[i].state.state === 'stable' ? n[i].state.assignment : []);
    if (rows.length !== 3 || !rows.every((a) => a.committed != null)) return null;
    return { rows: Object.fromEntries(rows.map((a) => [a.topic + '-' + a.partition, a.committed])), now: window.krabkaLab.world.now() };
  }`, 120_000);
  await waitFor(page, `window.krabkaLab.world.now() > ${rejoined.now} + 5000`, 'five seconds after the rejoin');
  const idle = [(await nodeStateOf(page, 5)).processed, (await nodeStateOf(page, 6)).processed];
  const sorted = (o) => JSON.stringify(Object.entries(o).sort());
  check('after the reload the group fetches its committed offsets and reads nothing again', sorted(rejoined.rows) === sorted(committed) && idle[0] === 0 && idle[1] === 0, JSON.stringify({ committed, rejoined: rejoined.rows, processed: idle }));
  // New records: the producer back at its rate (Apply restarts it).
  await inspect(page, 4, 'orders-producer');
  await page.locator('#krabka-lab .lab-tab#lab-tab-config').click();
  await page.getByLabel('Records per second').fill('5');
  await page.locator('#krabka-lab .lab-tabpanel[data-tab="config"] button', { hasText: 'Apply' }).click();
  const fresh = await until(page, 'new records to arrive', `(n) => {
    const c = [5, 6].map((i) => n[i].state);
    return c[0].processed + c[1].processed >= 5 && c.flatMap((s) => s.last_records.map((r) => ({ p: r.topic + '-' + r.partition, offset: r.offset })));
  }`, 60_000);
  check('and consumes the new records from where it left off', fresh.every((r) => r.offset >= committed[r.p]), JSON.stringify(fresh));
}

async function checkRegistryPreset(page, preset) {
  console.log(`Cluster Lab: ${preset.name}`);
  await openPreset(page, preset);
  // Brokers 1 to 3, the registry 4, the producer 5, the consumer 6.
  const registered = await until(page, 'the producer to register its schema', `(n) => n[5].state.serialization && n[5].state.serialization.state === 'ready' && n[5].state.serialization`, 90_000);
  const subjects = await page.evaluate(() => window.krabkaLab.world.control(4, { cmd: 'http', method: 'GET', path: '/subjects' }));
  check('the registry answers GET /subjects with the subject the producer registered', subjects.ok && subjects.answer.status === 200 && JSON.stringify(subjects.answer.body) === '["orders-value"]', JSON.stringify(subjects));
  const decoded = await until(page, 'the consumer to decode', `(n) => {
    const r = n[6].state.last_records;
    return n[5].state.acked > 0 && r.length && r.every((x) => x.schema_id === ${registered.schema_id} && x.value_preview && typeof x.value_preview.customer === 'string') && r;
  }`, 90_000);
  check('the consumer decodes every value with the schema it fetched by id', decoded.length > 0, JSON.stringify(decoded[0]));
  await inspect(page, 6, 'billing');
  const shown = await waitFor(page, `document.querySelector('#krabka-lab .lab-inspector td[data-col="schema_id"]')?.textContent || null`, 'the schema column');
  check('the consumer inspector shows the schema id of each record', shown === `id ${registered.schema_id}`, shown);

  // The registry keeps nothing of its own: a schema's record lives in
  // `_schemas` on the brokers, which the page persists. With persistence on,
  // then off, then on again, a reload brings every schema back.
  const scenarioId = await page.evaluate(() => window.krabkaLab.world.id);
  const payments = await registryWrite(page, 4, 'POST', '/subjects/payments-value/versions', { schema: PAYMENT_V1 });
  check('a schema registered through REST is answered once its record is back from the brokers', payments.ok && payments.answer.status === 200 && Number.isInteger(payments.answer.body.id), JSON.stringify(payments));
  await setPersistUI(page, false);
  const storedAtOff = await schemasStored(page, scenarioId);
  const inventory = await registryWrite(page, 4, 'POST', '/subjects/inventory-value/versions', { schema: INVENTORY_V1 });
  const orders2 = await registryWrite(page, 4, 'POST', '/subjects/orders-value/versions', { schema: ORDER_V2 });
  const storedWhileOff = await schemasStored(page, scenarioId);
  check(
    'with persistence off the registry still writes, and the brokers\' new _schemas records are not stored',
    inventory.ok && inventory.answer.status === 200 && orders2.ok && orders2.answer.status === 200 && storedAtOff > 0 && storedWhileOff === storedAtOff,
    JSON.stringify({ inventory, orders2, storedAtOff, storedWhileOff }),
  );
  await setPersistUI(page, true);
  const version2 = await page.evaluate(() => window.krabkaLab.world.control(4, { cmd: 'http', method: 'GET', path: '/subjects/orders-value/versions/latest' }));
  const live = await until(page, 'the registry to show every subject', `(n) => n[4].state.subjects.length === 3 && n[4].state.subjects`);
  await page.evaluate(() => window.krabkaLab.saveNow());
  await page.evaluate(() => window.krabkaLab.storage.flush());
  check('turning persistence back on stores the _schemas records written while it was off', (await schemasStored(page, scenarioId)) > storedAtOff);
  await page.reload({ waitUntil: 'load' });
  await page.waitForSelector('#krabka-lab[data-ready="true"]', { timeout: STEP_TIMEOUT });
  await fastest(page);
  const replayed = await until(page, 'the registry to replay _schemas', `(n) => n[4].state.state === 'ready' && n[4].state.subjects`, 120_000);
  check('after a reload the registry replays every schema from the brokers\' _schemas log', JSON.stringify(replayed) === JSON.stringify(live), `${JSON.stringify(replayed)} vs ${JSON.stringify(live)}`);
  const served = await page.evaluate(() => window.krabkaLab.world.control(4, { cmd: 'http', method: 'GET', path: '/subjects/inventory-value/versions/1' }));
  const latest = await page.evaluate(() => window.krabkaLab.world.control(4, { cmd: 'http', method: 'GET', path: '/subjects/orders-value/versions/latest' }));
  check(
    'and serves the schemas registered while persistence was off, with the same ids and versions',
    served.ok && served.answer.status === 200 && served.answer.body.id === inventory.answer.body.id && latest.ok && JSON.stringify(latest.answer.body) === JSON.stringify(version2.answer.body) && latest.answer.body.version === 2,
    JSON.stringify({ served, latest }),
  );

  // A second registry joins the group. The eligible instance with the
  // smallest URL, node 4, stays the primary; the new one is a secondary that
  // forwards the writes it takes to the primary.
  await page.locator('#krabka-lab .lab-kind-btn[data-kind="schema-registry"]').click();
  await page.waitForSelector('#krabka-lab dialog[open]');
  await page.locator('#krabka-lab dialog button[type="submit"]').click();
  const second = await waitFor(page, `(() => { const n = window.krabkaLab.world.scenario().nodes.find((x) => x.kind === 'schema-registry' && x.id !== 4); return n ? n.id : null; })()`, 'the second registry');
  const roles = await until(page, 'the two registries to elect a primary', `(n) => {
    const e = [4, ${second}].map((i) => n[i].state.state === 'ready' && n[i].state.election);
    return e[0] && e[1] && e[0].leader && e[0].leader === e[1].leader && { leader: e[0].leader, primary: e.map((x) => x.is_leader) };
  }`, 120_000);
  const cards = await waitFor(page, `(() => { const s = [4, ${second}].map((i) => document.querySelector('#krabka-lab .lab-node[data-node-id="' + i + '"]')?.dataset.status || ''); return /primary/.test(s[0]) && /secondary/.test(s[1]) ? JSON.stringify(s) : null; })()`, 'the cards to say primary and secondary');
  check(
    'two registries of one group elect one primary, the smaller URL, and the cards say so',
    roles.leader === 'http://node-4:8081' && JSON.stringify(roles.primary) === '[true,false]',
    `${JSON.stringify(roles)} ${cards}`,
  );
  const via = await registryWrite(page, second, 'POST', '/subjects/shipments-value/versions', { schema: SHIPMENT_V1 });
  const onPrimary = await page.evaluate(() => window.krabkaLab.world.control(4, { cmd: 'http', method: 'GET', path: '/subjects/shipments-value/versions/1' }));
  const forwarded = await until(page, 'the secondary to count the forward', `(n) => n[${second}].state.forwarder && n[${second}].state.forwarder.forwarded > 0 && n[${second}].state.forwarder`);
  check(
    'a write through the secondary is forwarded to the primary and served',
    via.ok && via.answer.status === 200 && onPrimary.ok && onPrimary.answer.status === 200 && onPrimary.answer.body.id === via.answer.body.id && forwarded.forwarded > 0,
    JSON.stringify({ via, onPrimary: onPrimary.answer, forwarded }),
  );
  await fit(page);
  await inspect(page, second, `schema-registry-${second}`);
  const role = await waitFor(page, `document.querySelector('#krabka-lab .lab-inspector dd[data-field="election_role"]')?.textContent || null`, 'the election in the inspector');
  const labelled = await page.evaluate(() => [...document.querySelectorAll('#krabka-lab .lab-ev-kind[data-kind="election"]')].map((e) => e.textContent));
  check('the inspector names the secondary, and the timeline labels the election', /^secondary/.test(role) && labelled.length > 0 && labelled.every((t) => t === 'registry election'), `${role}; ${labelled.length} election rows`);
  const listed = await command(page, 'http', { path: '/subjects' });
  check('the registry command bar reads a REST resource', listed.ok && /"status":200/.test(listed.text) && /shipments-value/.test(listed.text), listed.text);
}

// A REST write through the registry's `http` command: it joins the write
// queue and is answered by the `registry` event that carries its number.
async function registryWrite(page, id, method, path, body) {
  const r = await page.evaluate(([id, method, path, body]) => window.krabkaLab.world.control(id, { cmd: 'http', method, path, body }), [id, method, path, body]);
  if (!r.ok || r.answer.queued == null) return r;
  const detail = JSON.parse(
    await waitFor(
      page,
      `(() => { const e = window.krabkaLab.timeline.events.find((x) => x.node === ${id} && x.kind === 'registry' && x.detail && x.detail.request === ${r.answer.queued}); return e ? JSON.stringify(e.detail) : null; })()`,
      `the answer to request ${r.answer.queued}`,
      60_000,
    ),
  );
  return { ok: true, answer: { status: detail.status, body: detail.result ?? { message: detail.message } } };
}

// How many `_schemas` records the page stored, over every broker.
const schemasStored = (page, scenarioId) =>
  page.evaluate(async (id) => {
    const images = await window.krabkaLab.storage.loadImages(id);
    return Object.values(images).reduce((sum, image) => sum + (image.logs?.['log/_schemas/0']?.length ?? 0), 0);
  }, scenarioId);

async function checkPartitionPreset(page, preset) {
  console.log(`Cluster Lab: ${preset.name}`);
  await openPreset(page, preset);
  // Brokers 1 to 5, with 4 and 5 cut off from the rest; the producer 6, the consumer 7.
  const split = await until(page, 'the majority to serve', `(n) => {
    const b = [1, 2, 3, 4, 5].map((i) => n[i].state);
    if (!b.slice(0, 3).every((s) => s.state === 'RUNNING') || !(n[7].state.processed > 0)) return null;
    return { states: b.map((s) => s.state), voters: b[0].quorum.voters, hwm: b[0].quorum.hwm, processed: n[7].state.processed };
  }`, 90_000);
  check(
    'with brokers 4 and 5 cut off, the majority of five voters elects a controller and serves the group',
    split.voters.length === 5 && split.states.slice(3).every((s) => s !== 'RUNNING') && split.processed > 0,
    JSON.stringify(split),
  );
  for (const a of [4, 5]) for (const b of [1, 2, 3]) await page.evaluate(([a, b]) => window.krabkaLab.fault({ kind: 'heal', a, b }), [a, b]);
  const healed = await until(page, 'the minority to catch up and join', `(n) => [4, 5].every((i) => n[i].state.state === 'RUNNING' && n[i].state.lifecycle.fenced === false) && [1, 2, 3, 4, 5].map((i) => n[i].state.quorum.hwm)`, 90_000);
  check('healed, brokers 4 and 5 catch up with the metadata log, register and are unfenced', healed.slice(3).every((h) => h >= split.hwm), JSON.stringify({ before: split.hwm, healed }));
}

async function checkWordCount(page, preset) {
  console.log(`Cluster Lab: ${preset.name}`);
  await openPreset(page, preset);
  // Brokers 1 to 3, the producer 4, the streams app 5, the consumer 6.
  const counting = await until(page, 'the streams app to count', `(n) => {
    const s = n[5].state;
    if (s.state !== 'running' || s.tasks.length !== 3 || !s.tasks.every((t) => t.phase === 'running')) return null;
    const entries = s.stores.flatMap((st) => st.entries);
    return entries.length && n[6].state.processed > 0 && { entries, tasks: s.tasks.map((t) => t.id), sink: n[6].state.processed };
  }`, 90_000);
  check('the streams app runs three tasks and counts the words in its store', counting.tasks.length === 3 && counting.entries.every(([, count]) => count > 0), JSON.stringify(counting.entries.slice(0, 4)));
  check('a consumer reads the counts from word-counts', counting.sink > 0, `${counting.sink} records`);
  const changelog = await until(page, 'the changelog topic', `(n) => { const t = n[1].state.topics.find((x) => x.name === 'word-count-counts-changelog'); return t && { partitions: t.partitions.length }; }`);
  const pill = await page.locator('#krabka-lab .lab-topic[data-topic="word-count-counts-changelog"]').count();
  check('the group creates the store\'s changelog topic, and the canvas draws it', changelog.partitions === 3 && pill === 1, `${JSON.stringify(changelog)}, ${pill} pill`);

  await inspect(page, 5, 'word-count');
  const [word] = counting.entries[0];
  const query = await command(page, 'query', { store: 'counts', key: word });
  const answer = JSON.parse(query.text.slice(query.text.indexOf('{')));
  check(`the query box reads ${word} from the counts store`, query.ok && answer.key === word && answer.value >= counting.entries[0][1], query.text);
  await command(page, 'pause');
  const held = await until(page, 'the streams app to pause', `(n) => n[5].state.paused === true && { in: n[5].state.records_in, now: window.krabkaLab.world.now() }`);
  await waitFor(page, `window.krabkaLab.world.now() > ${held.now} + 3000`, 'three seconds to pass');
  const stillIn = (await nodeStateOf(page, 5)).records_in;
  await command(page, 'resume');
  const moving = await until(page, 'the streams app to resume', `(n) => n[5].state.paused === false && n[5].state.records_in > ${stillIn} && n[5].state.records_in`);
  check('Pause holds the streams app and Resume starts it again', stillIn === held.in && moving > stillIn, `${held.in} -> ${stillIn} -> ${moving}`);
}

async function main() {
  if (!fs.existsSync(path.join(DIST_DIR, 'docs', 'lab', 'index.html'))) {
    console.error('dist/docs/lab/index.html is missing: run `npm run build` first.');
    process.exit(1);
  }
  await checkInflate();

  const pw = await loadPlaywright();
  if (!pw) {
    console.error('Playwright is not installed (neither in node_modules nor globally); cannot run the lab check.');
    process.exit(2);
  }
  let browser;
  try {
    browser = await launchChromium(pw);
  } catch (err) {
    console.error(`Playwright could not launch Chromium: ${err.message.split('\n')[0]}`);
    console.error('Install it with `npx playwright install chromium`, or point PLAYWRIGHT_BROWSERS_PATH at an installed one.');
    process.exit(2);
  }
  const { server, port } = await serve(DIST_DIR);
  const base = `http://127.0.0.1:${port}`;
  const context = await browser.newContext({ viewport: { width: 1400, height: 1000 } });
  const errors = [];
  try {
    console.log('Cluster Lab: solo flow');
    const page = await context.newPage();
    const pageErrors = watchErrors(page, 'page', base);
    await openLab(page, base);
    check('page boots and the module initialises', true);

    const nodeCount = await page.locator('#krabka-lab .lab-node').count();
    check('the network probe preset is on the canvas', nodeCount === 3, `${nodeCount} nodes`);
    const names = await page.evaluate(() => window.krabkaLab.world.scenario().nodes.map((n) => n.name).sort());
    check('preset nodes are echo-a, echo-b and pinger', JSON.stringify(names) === JSON.stringify(['echo-a', 'echo-b', 'pinger']), names.join(','));

    const t0 = await page.evaluate(() => window.krabkaLab.world.now());
    await waitFor(page, `window.krabkaLab.world.now() > ${t0} + 300`, 'the clock to advance');
    check('the simulated clock advances', true);

    const pinger = JSON.parse(await waitFor(page, `(() => { const r = ${nodeState(3)}; return r && JSON.parse(r).state.echoes > 3 ? r : null; })()`, 'the pinger to receive echoes'));
    check('the pinger receives echoes', pinger.state.echoes > 3, JSON.stringify(pinger.state));
    check('the pinger reports a round trip', typeof pinger.state.mean_rtt_ms === 'number' && pinger.state.mean_rtt_ms >= 20, `rtt ${pinger.state.mean_rtt_ms}`);

    const dots = await page.locator('#krabka-lab .lab-frame').count();
    check('frames are drawn in flight', dots > 0, `${dots} dots`);

    // Inspector: click the pinger card.
    await page.locator('#krabka-lab .lab-node[data-node-id="3"]').click();
    await page.waitForSelector('#krabka-lab .lab-inspector .lab-insp-name');
    const inspName = await page.locator('#krabka-lab .lab-inspector .lab-insp-name').textContent();
    check('the inspector shows the selected node', inspName === 'pinger', inspName);
    await waitFor(page, `!!document.querySelector('#krabka-lab .lab-inspector dd[data-field="echoes"]')`, 'the state view');
    const echoesText = await page.locator('#krabka-lab .lab-inspector dd[data-field="echoes"]').textContent();
    check('the state view lists the echo counter', /^\d/.test(echoesText), echoesText);
    await page.locator('#krabka-lab .lab-tab#lab-tab-raw').click();
    const raw = await page.locator('#krabka-lab .lab-raw').textContent();
    check('the raw tab shows the snapshot JSON', raw.includes('"kind": "pinger"'));
    await page.locator('#krabka-lab .lab-tab#lab-tab-state').click();

    // Status line on the card.
    const status = await page.locator('#krabka-lab .lab-node[data-node-id="3"]').getAttribute('data-status');
    check('the card carries a status line', /echoed/.test(status || ''), status);

    // Faults: kill echo-a (node 1), then restart it.
    await page.locator('#krabka-lab .lab-node[data-node-id="1"]').click();
    await page.locator('#krabka-lab .lab-faults button', { hasText: 'Kill' }).click();
    await waitFor(page, `(() => { const r = ${nodeState(1)}; return r && JSON.parse(r).alive === false; })()`, 'echo-a to be down');
    check('Kill halts the node', true);
    const eventKinds = await page.evaluate(() => [...document.querySelectorAll('#krabka-lab .lab-ev-kind')].map((e) => e.textContent));
    check('the timeline logs the fault', eventKinds.includes('fault'), eventKinds.slice(-5).join(','));
    await page.locator('#krabka-lab .lab-faults button', { hasText: 'Restart' }).click();
    await waitFor(page, `(() => { const r = ${nodeState(1)}; return r && JSON.parse(r).alive === true; })()`, 'echo-a to be up');
    check('Restart brings it back', true);

    // Link faults: pick echo-a and the pinger, cut, heal.
    await page.locator('#krabka-lab .lab-node[data-node-id="3"]').click({ modifiers: ['Shift'] });
    await page.locator('#krabka-lab .lab-faults button', { hasText: 'Partition' }).click();
    await waitFor(page, `(() => { const s = window.krabkaLab.world.snapshot(); return s.links.some((l) => l.cut); })()`, 'the link to be cut');
    check('Partition cuts the link', true);
    const overlay = await page.locator('#krabka-lab .lab-link-cut').count();
    check('the cut link is drawn', overlay === 1, `${overlay} overlays`);
    await page.locator('#krabka-lab .lab-faults button', { hasText: 'Heal' }).click();
    await waitFor(page, `(() => { const s = window.krabkaLab.world.snapshot(); return !s.links.some((l) => l.cut); })()`, 'the link to heal');
    check('Heal restores it', true);

    // Persistence: the echo node's frame counter is durable.
    const scenarioId = await waitFor(page, `window.krabkaLab.world.id || null`, 'the scenario to get an id');
    check('the scenario got an identity on autosave', /^[0-9a-f-]{8,}/.test(scenarioId), scenarioId);
    const before = JSON.parse(await waitFor(page, `(() => { const r = ${nodeState(1)}; return r && JSON.parse(r).state.frames >= 20 ? r : null; })()`, 'echo-a to count 20 frames'));
    const framesBefore = before.state.frames;
    await page.evaluate(() => window.krabkaLab.storage.flush());
    await page.evaluate(() => window.krabkaLab.world.setPaused(true));
    const framesAtPause = (await readNode(page, 1)).state.frames;
    await page.evaluate(() => window.krabkaLab.storage.flush());

    // The storage panel reports it.
    await page.locator('#krabka-lab .lab-storage summary').click();
    await waitFor(page, `(() => { const td = document.querySelector('#krabka-lab tr[data-storage-node="1"] td[data-field="bytes"]'); return td && td.textContent !== '0 B'; })()`, 'the storage panel to list echo-a');
    check('the storage panel shows bytes kept for echo-a', true);

    await page.reload({ waitUntil: 'load' });
    await page.waitForSelector('#krabka-lab[data-ready="true"]', { timeout: STEP_TIMEOUT });
    const idAfterReload = await page.evaluate(() => window.krabkaLab.world.id);
    check('a reload reopens the last scenario', idAfterReload === scenarioId, `${idAfterReload} vs ${scenarioId}`);
    const restored = await readNode(page, 1);
    check('the echo counter continues from the stored value after reload', restored.state.frames >= framesAtPause, `${restored.state.frames} < ${framesAtPause}`);

    // Move away, then reopen the saved scenario from the Saved list.
    await page.locator('#krabka-lab button', { hasText: 'New (empty)' }).click();
    await waitFor(page, `window.krabkaLab.world.scenario().nodes.length === 0`, 'an empty scenario');
    await page.locator('#krabka-lab .lab-pal-section summary', { hasText: 'Saved' }).click();
    await page.waitForSelector(`#krabka-lab [data-saved-open="${scenarioId}"]`, { timeout: STEP_TIMEOUT });
    await page.locator(`#krabka-lab [data-saved-open="${scenarioId}"]`).click();
    await waitFor(page, `window.krabkaLab.world.id === ${JSON.stringify(scenarioId)} && window.krabkaLab.world.scenario().nodes.length === 3`, 'the saved scenario to reopen');
    const reopened = await readNode(page, 1);
    check('reopening from the Saved list restores the echo counter', reopened.state.frames >= framesAtPause, `${reopened.state.frames} < ${framesAtPause}`);
    await waitFor(page, `(() => { const r = ${nodeState(1)}; return r && JSON.parse(r).state.frames > ${reopened.state.frames}; })()`, 'the counter to keep counting');
    check('and the counter keeps counting from there', true);

    // "Persist to this browser" off: the ops fold into the in-memory mirror and
    // are not stored. The echo counts on; the store keeps the counter it had.
    await setPersistUI(page, false);
    const storedAtOff = await storedFrames(page, scenarioId);
    const liveAtOff = (await readNode(page, 1)).state.frames;
    await waitFor(page, `(() => { const r = ${nodeState(1)}; return r && JSON.parse(r).state.frames > ${liveAtOff} + 3; })()`, 'echo-a to count on with persistence off');
    const storedWhileOff = await storedFrames(page, scenarioId);
    check('with persistence off, new durable ops are not stored', storedAtOff != null && storedWhileOff === storedAtOff, `${storedAtOff} -> ${storedWhileOff}`);

    // Persistence back on: the store is replaced with the live state before any
    // later op, so the reload restores exactly what was live. Cutting the
    // pinger off echo-a first holds the counter still across the reload (the
    // cut is part of the saved scenario), so the comparison is exact.
    await page.evaluate(() => window.krabkaLab.fault({ kind: 'partition', a: 3, b: 1 }));
    await setPersistUI(page, true);
    await page.evaluate(() => window.krabkaLab.world.setPaused(true));
    await page.evaluate(() => window.krabkaLab.storage.flush());
    await page.evaluate(() => window.krabkaLab.saveNow());
    const liveEcho = (await readNode(page, 1)).state.frames;
    await page.reload({ waitUntil: 'load' });
    await page.waitForSelector('#krabka-lab[data-ready="true"]', { timeout: STEP_TIMEOUT });
    const restoredEcho = (await readNode(page, 1)).state.frames;
    check('after persistence is back on, the restored echo counter equals the live one', restoredEcho === liveEcho, `${restoredEcho} vs ${liveEcho} live`);

    // Forget while the node runs: its records go, and it is not written again
    // (its next op alone would be a partial image), so a reload starts it
    // from nothing.
    await page.evaluate(() => window.krabkaLab.fault({ kind: 'heal', a: 3, b: 1 }));
    const beforeForget = (await readNode(page, 1)).state.frames;
    await waitFor(page, `(() => { const r = ${nodeState(1)}; return r && JSON.parse(r).state.frames > ${beforeForget} + 2; })()`, 'echo-a to count again after the heal');
    await page.evaluate(() => window.krabkaLab.forgetNode(1));
    const atForget = (await readNode(page, 1)).state.frames;
    await waitFor(page, `(() => { const r = ${nodeState(1)}; return r && JSON.parse(r).state.frames > ${atForget} + 3; })()`, 'echo-a to count on after Forget');
    await page.evaluate(() => window.krabkaLab.storage.flush());
    const leftAfterForget = await page.evaluate((id) => window.krabkaLab.storage.usage(id).then((u) => u.nodes[1] || null), scenarioId);
    check("Forget drops the node's stored data, and the running node is not stored again", leftAfterForget === null, JSON.stringify(leftAfterForget));
    const liveAtReload = (await readNode(page, 1)).state.frames;
    await page.evaluate(() => window.krabkaLab.saveNow());
    await page.reload({ waitUntil: 'load' });
    await page.waitForSelector('#krabka-lab[data-ready="true"]', { timeout: STEP_TIMEOUT });
    const forgotten = await readNode(page, 1);
    check('after Forget, a reload starts the counter from nothing', forgotten.state.frames < 10 && forgotten.state.frames < liveAtReload, `${forgotten.state.frames}, was ${liveAtReload}`);

    // Share link.
    const link = await page.evaluate(() => import('/playground/lab/scenarios.js').then((m) => m.shareLink(window.krabkaLab.world.scenario())));
    check('a share link carries the scenario in its hash', /#s=[dp][A-Za-z0-9_-]+$/.test(link), link.slice(0, 80));
    const page2 = await context.newPage();
    const page2Errors = watchErrors(page2, 'shared page', base);
    await page2.goto(link, { waitUntil: 'load' });
    await page2.waitForSelector('#krabka-lab[data-ready="true"]', { timeout: STEP_TIMEOUT });
    const sharedNodes = await page2.evaluate(() => window.krabkaLab.world.scenario().nodes.map((n) => n.name).sort());
    const sharedId = await page2.evaluate(() => window.krabkaLab.world.id);
    const expectedNodes = JSON.stringify(['echo-a', 'echo-b', 'pinger']);
    check('opening the link reproduces the nodes', JSON.stringify(sharedNodes) === expectedNodes, sharedNodes.join(','));
    check('a shared scenario gets its own identity', sharedId !== scenarioId, sharedId);
    await page2.close();

    // A browser without DecompressionStream opens the same compressed link
    // through the JavaScript raw-DEFLATE decoder.
    check('the share link is compressed', /#s=d/.test(link), link.slice(0, 60));
    const page3 = await context.newPage();
    await page3.addInitScript(() => {
      delete globalThis.DecompressionStream;
    });
    const page3Errors = watchErrors(page3, 'page without DecompressionStream', base);
    await page3.goto(link, { waitUntil: 'load' });
    await page3.waitForSelector('#krabka-lab[data-ready="true"]', { timeout: STEP_TIMEOUT });
    const noStream = await page3.evaluate(() => typeof DecompressionStream);
    // The page decodes the hash itself, and opened it: a scenario it fell back
    // to would carry an identity it already had, not a fresh one.
    const decodedNodes = await page3.evaluate(
      (hash) =>
        import('/playground/lab/scenarios.js')
          .then((m) => m.scenarioFromHash(hash))
          .then((doc) => doc.nodes.map((n) => n.name).sort(), (err) => `error: ${err.message}`),
      new URL(link).hash,
    );
    const inflatedNodes = await page3.evaluate(() => window.krabkaLab.world.scenario().nodes.map((n) => n.name).sort());
    const inflatedId = await page3.evaluate(() => window.krabkaLab.world.id);
    check(
      'without DecompressionStream the link opens through the JavaScript decoder',
      noStream === 'undefined' && JSON.stringify(decodedNodes) === expectedNodes && JSON.stringify(inflatedNodes) === expectedNodes && inflatedId !== sharedId && inflatedId !== scenarioId,
      `${noStream}: decoded ${Array.isArray(decodedNodes) ? decodedNodes.join(',') : decodedNodes}, opened ${inflatedNodes.join(',')} as ${inflatedId}`,
    );
    await page3.close();

    // Add a node through the palette dialog.
    await page.locator('#krabka-lab .lab-kind-btn[data-kind="pinger"]').click();
    await page.waitForSelector('#krabka-lab dialog[open]');
    await page.locator('#krabka-lab dialog select').first().selectOption('2');
    await page.locator('#krabka-lab dialog button[type="submit"]').click();
    await waitFor(page, `window.krabkaLab.world.scenario().nodes.length === 4`, 'the new node');
    const added = await page.evaluate(() => window.krabkaLab.world.scenario().nodes.find((n) => n.id === 4));
    check('the palette adds a configured node', added && added.kind === 'pinger' && added.config.target === 2, JSON.stringify(added));
    await page.evaluate(() => window.krabkaLab.world.setPaused(false));
    await waitFor(page, `(() => { const r = ${nodeState(4)}; return r && JSON.parse(r).state.echoes > 0; })()`, 'the new pinger to ping');
    check('and it runs', true);

    errors.push(...pageErrors, ...page2Errors, ...page3Errors);

    if (CLUSTER) await checkClusters(browser, base, errors);

    if (WEBRTC) {
      console.log('Cluster Lab: two tabs over WebRTC');
      await page.evaluate(() => window.krabkaLab.world.setPaused(true));
      const hub = await context.newPage();
      const hubErrors = watchErrors(hub, 'hub', base);
      await openLab(hub, base);
      await hub.evaluate(() => window.krabkaLab.loadPreset('network-probe'));
      await waitFor(hub, `window.krabkaLab.world.scenario().nodes.length === 3`, 'the hub preset');

      // Same-tab baseline: 200 ms each way on the pinger–echo-a link, the
      // pinger restarted from nothing so its mean covers only this latency.
      await hub.evaluate(() => {
        window.krabkaLab.fault({ kind: 'latency', a: 3, b: 1, ms: 200 });
        window.krabkaLab.fault({ kind: 'wipe', node: 3 });
      });
      const sameTab = JSON.parse(await waitFor(hub, `(() => { const r = ${nodeState(3)}; return r && JSON.parse(r).state.echoes >= 6 ? r : null; })()`, 'same-tab echoes over the slow link')).state.mean_rtt_ms;
      check(`in one tab, the round trip over a 200 ms link is 400 ms (${sameTab} ms)`, Math.abs(sameTab - 400) <= 5, `${sameTab} ms`);

      const invite = await hub.evaluate(() => window.krabkaLab.session.createInvite(window.location.href));
      check('the hub makes an invite link', /\?join=/.test(invite), invite.slice(0, 60));
      const spoke = await context.newPage();
      const spokeErrors = watchErrors(spoke, 'spoke', base);
      await spoke.goto(invite, { waitUntil: 'load' });
      await spoke.waitForSelector('#krabka-lab[data-ready="true"]', { timeout: STEP_TIMEOUT });
      await spoke.waitForSelector('#krabka-lab textarea[data-field="answer-code"]', { timeout: STEP_TIMEOUT });
      const answer = await spoke.locator('#krabka-lab textarea[data-field="answer-code"]').inputValue();
      check('the spoke produces an answer code', answer.length > 20);
      await hub.evaluate((code) => window.krabkaLab.session.acceptAnswer(code), answer);
      await waitFor(hub, `window.krabkaLab.session.peerList().filter((p) => !p.self && p.state === 'connected').length === 1`, 'the peer to connect');
      check('the data channel connects', true);
      await waitFor(spoke, `window.krabkaLab.world.scenario().nodes.length === 3 && window.krabkaLab.session.role === 'spoke'`, 'the spoke to receive the scenario');
      check('the spoke receives the scenario', true);

      // Only the host edits the scenario: the spoke's inspector offers no Edit,
      // shows the configuration read-only, and its update path refuses.
      await spoke.locator('#krabka-lab .lab-node[data-node-id="2"]').click();
      await spoke.waitForSelector('#krabka-lab .lab-inspector .lab-insp-name');
      const spokeEdit = await spoke.locator('#krabka-lab .lab-insp-actions button', { hasText: 'Edit' }).count();
      check('a spoke offers no Edit in the inspector', spokeEdit === 0, `${spokeEdit} Edit buttons`);
      await spoke.locator('#krabka-lab .lab-tab#lab-tab-config').click();
      const readOnly = await spoke.locator('#krabka-lab [data-field="config-readonly"]').count();
      const formInputs = await spoke.locator('#krabka-lab .lab-tabpanel[data-tab="config"] input, #krabka-lab .lab-tabpanel[data-tab="config"] select').count();
      check('a spoke shows the configuration read-only', readOnly === 1 && formInputs === 0, `${readOnly} notes, ${formInputs} inputs`);
      const refused = await spoke.evaluate(() => {
        const spec = window.krabkaLab.world.spec(2);
        return window.krabkaLab.inspector.hooks.onUpdateNode(2, { ...spec, name: 'renamed-by-spoke' });
      });
      const namesAfter = await spoke.evaluate(() => window.krabkaLab.world.spec(2).name);
      check('a spoke refuses a configuration update', refused === false && namesAfter === 'echo-b', `${refused}, ${namesAfter}`);
      const spokeId = await spoke.evaluate(() => window.krabkaLab.session.me);
      await hub.evaluate((peer) => window.krabkaLab.session.setHost(3, peer), spokeId);
      await waitFor(spoke, `(() => { const r = ${nodeState(3)}; return r && JSON.parse(r).hosted === true; })()`, 'the spoke to host the pinger');
      check('the hub hands the pinger to the spoke', true);
      // The hub's snapshot refreshes every 50 ms, so wait for it, not sample it.
      const hubView = JSON.parse(await waitFor(hub, `(() => { const r = ${nodeState(3)}; return r && JSON.parse(r).hosted === false ? r : null; })()`, 'the hub to see the pinger as remote', 10_000).catch(() => 'null'));
      check('the hub sees it as remote', hubView != null && hubView.hosted === false);
      // The pinger restarts in the spoke on the same connection id, and echoes
      // of pings its old self sent from the hub may still arrive and count as
      // echoes with no round trip. So the round trip is the mean over a window
      // of ten fresh echoes, from two samples of `mean_rtt_ms` × `echoes`.
      const pingerAt = async (p, min) =>
        JSON.parse(await waitFor(p, `(() => { const r = ${nodeState(3)}; return r && JSON.parse(r).state.echoes >= ${min} ? r : null; })()`, `${min} echoes across the channel`, 40_000)).state;
      const first = await pingerAt(spoke, 10);
      const second = await pingerAt(spoke, first.echoes + 10);
      const crossTab = Math.round((second.mean_rtt_ms * second.echoes - first.mean_rtt_ms * first.echoes) / (second.echoes - first.echoes));
      check('pings cross the WebRTC channel and come back', true);
      // Each side holds its frames until its own clock reaches deliver_at, so
      // the link's 200 ms each way survive the hop; the rest is frame timing.
      check(`across tabs, the round trip matches the same-tab one (${crossTab} ms vs ${sameTab} ms)`, crossTab >= sameTab - 5 && crossTab <= sameTab + 150, `${crossTab} ms across tabs, ${sameTab} ms in one`);
      await waitFor(hub, `(() => { const r = ${nodeState(3)}; return r && JSON.parse(r).state && JSON.parse(r).state.echoes > 0; })()`, 'the remote snapshot on the hub', 20_000);
      check('the hub shows the remote node\'s snapshot', true);
      await hub.evaluate(() => window.krabkaLab.fault({ kind: 'kill', node: 1 }));
      await waitFor(spoke, `(() => { const r = ${nodeState(1)}; return r && JSON.parse(r).alive === false; })()`, 'the fault to mirror');
      check('faults are mirrored to the peer', true);
      errors.push(...hubErrors, ...spokeErrors);
      await spoke.close();
      await hub.close();
    }
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
  console.log('✅ PASS: the Cluster Lab works end to end.');
}

main();
