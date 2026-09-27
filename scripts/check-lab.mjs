// End-to-end check of the Cluster Lab page (`/docs/lab`) in headless Chromium.
//
// Serves the built site from `dist/`, opens the lab, and drives it the way a
// reader would: the default preset runs, frames cross the canvas, the
// inspector shows the pinger's round trips, a kill and a restart take effect,
// the echo node's frame counter survives a page reload through IndexedDB and
// comes back when the saved scenario is reopened from the "Saved" list, a
// share link reproduces the scenario, and two pages in one browser host a
// cluster together over WebRTC.
//
// Usage:  npm run build && npm run check-lab [-- --no-webrtc] [--headed]
// Needs `playwright` or `playwright-core`, project-local or global (found
// through `npm root -g`), and a Chromium that Playwright finds by itself: its
// own download (`npx playwright install chromium`) or the directory named by
// PLAYWRIGHT_BROWSERS_PATH. Exits 2 when either is missing, 1 when a check
// fails.

import fs from 'fs';
import http from 'http';
import path from 'path';
import { createRequire } from 'module';
import { execSync } from 'child_process';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DIST_DIR = path.resolve(__dirname, '..', 'dist');
const args = new Set(process.argv.slice(2));
const WEBRTC = !args.has('--no-webrtc');
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

async function main() {
  if (!fs.existsSync(path.join(DIST_DIR, 'docs', 'lab', 'index.html'))) {
    console.error('dist/docs/lab/index.html is missing: run `npm run build` first.');
    process.exit(1);
  }
  const pw = await loadPlaywright();
  if (!pw) {
    console.error('Playwright is not installed (neither in node_modules nor globally); cannot run the lab check.');
    process.exit(2);
  }
  let browser;
  try {
    browser = await pw.chromium.launch({ headless: HEADLESS });
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

    // "Persist to this browser" off: ops are drained and dropped, so the
    // stored counter stays where it was while the node counts on. Turning it
    // off first and flushing after keeps the ops queued while it was on out
    // of the comparison.
    const storedFrames = () =>
      page.evaluate(async (id) => {
        await window.krabkaLab.storage.flush();
        const images = await window.krabkaLab.storage.loadImages(id);
        const b64 = images['1']?.kv?.counters?.frames;
        return b64 == null ? null : Number(atob(b64));
      }, scenarioId);
    await page.evaluate(() => window.krabkaLab.storage.setPersist(false));
    const storedAtOff = await storedFrames();
    const liveAtOff = (await readNode(page, 1)).state.frames;
    await waitFor(page, `(() => { const r = ${nodeState(1)}; return r && JSON.parse(r).state.frames > ${liveAtOff} + 3; })()`, 'echo-a to count on with persistence off');
    const storedWhileOff = await storedFrames();
    check('with persistence off, new durable ops are dropped', storedAtOff != null && storedWhileOff === storedAtOff, `${storedAtOff} -> ${storedWhileOff}`);
    await page.evaluate(() => window.krabkaLab.storage.setPersist(true));

    // Forget: pause first, so the node cannot write its key again, drop
    // echo-a's stored data, and reload with persistence on. The stored image
    // is read, finds nothing for echo-a, and its counter starts from nothing.
    await page.evaluate(() => window.krabkaLab.world.setPaused(true));
    await page.evaluate(() => window.krabkaLab.forgetNode(1)); // flushes the queue before deleting
    const leftAfterForget = await page.evaluate((id) => window.krabkaLab.storage.usage(id).then((u) => u.nodes[1] || null), scenarioId);
    check("Forget drops the node's stored data", leftAfterForget === null, JSON.stringify(leftAfterForget));
    await page.reload({ waitUntil: 'load' });
    await page.waitForSelector('#krabka-lab[data-ready="true"]', { timeout: STEP_TIMEOUT });
    const forgotten = await readNode(page, 1);
    check('after Forget, a reload starts the counter from nothing', forgotten.state.frames < framesAtPause, `${forgotten.state.frames}, was ${framesAtPause}`);

    // Share link.
    const link = await page.evaluate(() => import('/playground/lab/scenarios.js').then((m) => m.shareLink(window.krabkaLab.world.scenario())));
    check('a share link carries the scenario in its hash', /#s=[dp][A-Za-z0-9_-]+$/.test(link), link.slice(0, 80));
    const page2 = await context.newPage();
    const page2Errors = watchErrors(page2, 'shared page', base);
    await page2.goto(link, { waitUntil: 'load' });
    await page2.waitForSelector('#krabka-lab[data-ready="true"]', { timeout: STEP_TIMEOUT });
    const sharedNodes = await page2.evaluate(() => window.krabkaLab.world.scenario().nodes.map((n) => n.name).sort());
    const sharedId = await page2.evaluate(() => window.krabkaLab.world.id);
    check('opening the link reproduces the nodes', JSON.stringify(sharedNodes) === JSON.stringify(['echo-a', 'echo-b', 'pinger']), sharedNodes.join(','));
    check('a shared scenario gets its own identity', sharedId !== scenarioId, sharedId);
    await page2.close();

    // Add a node through the palette dialog.
    await page.locator('#krabka-lab .lab-kind-btn[data-kind="pinger"]').click();
    await page.waitForSelector('#krabka-lab dialog[open]');
    await page.locator('#krabka-lab dialog select').first().selectOption('2');
    await page.locator('#krabka-lab dialog button[type="submit"]').click();
    await waitFor(page, `window.krabkaLab.world.scenario().nodes.length === 4`, 'the new node');
    const added = await page.evaluate(() => window.krabkaLab.world.scenario().nodes[3]);
    check('the palette adds a configured node', added.kind === 'pinger' && added.config.target === 2, JSON.stringify(added));
    await page.evaluate(() => window.krabkaLab.world.setPaused(false));
    await waitFor(page, `(() => { const r = ${nodeState(4)}; return r && JSON.parse(r).state.echoes > 0; })()`, 'the new pinger to ping');
    check('and it runs', true);

    errors.push(...pageErrors, ...page2Errors);

    if (WEBRTC) {
      console.log('Cluster Lab: two tabs over WebRTC');
      await page.evaluate(() => window.krabkaLab.world.setPaused(true));
      const hub = await context.newPage();
      const hubErrors = watchErrors(hub, 'hub', base);
      await openLab(hub, base);
      await hub.evaluate(() => window.krabkaLab.loadPreset('network-probe'));
      await waitFor(hub, `window.krabkaLab.world.scenario().nodes.length === 3`, 'the hub preset');
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
      const spokeId = await spoke.evaluate(() => window.krabkaLab.session.me);
      await hub.evaluate((peer) => window.krabkaLab.session.setHost(3, peer), spokeId);
      await waitFor(spoke, `(() => { const r = ${nodeState(3)}; return r && JSON.parse(r).hosted === true; })()`, 'the spoke to host the pinger');
      check('the hub hands the pinger to the spoke', true);
      const hubView = await readNode(hub, 3);
      check('the hub sees it as remote', hubView.hosted === false);
      const e0 = (await readNode(spoke, 3)).state.echoes || 0;
      await waitFor(spoke, `(() => { const r = ${nodeState(3)}; return r && JSON.parse(r).state.echoes > ${e0} + 3; })()`, 'echoes across the channel', 40_000);
      check('pings cross the WebRTC channel and come back', true);
      await waitFor(hub, `(() => { const r = ${nodeState(3)}; return r && JSON.parse(r).state && JSON.parse(r).state.echoes > ${e0}; })()`, 'the remote snapshot on the hub', 20_000);
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
