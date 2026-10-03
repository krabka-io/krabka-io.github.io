// End-to-end check of the Cluster Lab page (`/docs/lab`) in headless Chromium.
//
// It needs no broker module: the scenario is the echo-and-pinger Network probe
// (`NETWORK_PROBE` in lab-check-lib.mjs). The cluster flows on real brokers
// are in `check-lab-clusters`.
//
// Serves the built site from `dist/`, opens the lab, and drives it the way a
// reader would: the probe runs, frames cross the canvas, the inspector shows
// the pinger's round trips, a kill and a restart take effect, the echo node's
// frame counter survives a page reload through IndexedDB and comes back when
// the saved scenario is reopened from the "Saved" list, the durable state
// stored after persistence is turned back on equals the live state, a share
// link reproduces the scenario even without `DecompressionStream`, and two
// pages in one browser host a cluster together over WebRTC with the link
// latency intact and the scenario edited only by the host. Before the browser
// starts, the raw-DEFLATE fallback round-trips `CompressionStream` output in
// Node.
//
// Usage:  npm run build && npm run check-lab [-- --no-webrtc] [--headed]
// Needs `playwright` or `playwright-core` and a Chromium, found as
// lab-check-lib.mjs finds them. Exits 2 when either is missing, 1 when a check
// fails.

import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { pathToFileURL } from 'url';
import { recordBatchFields } from '../public/playground/lab/storage-panel.js';
import { DIST_DIR, NETWORK_PROBE, STEP_TIMEOUT, args, checker, launchOrExit, newLabContext, nodeState, openLab, openScenario, serve, waitFor, watchErrors } from './lab-check-lib.mjs';

const WEBRTC = !args.has('--no-webrtc');
const t = checker();
const { check, failures } = t;


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
      const text = JSON.stringify({ nodes: Array.from({ length: Math.ceil(n / 40) + 1 }, (_, i) => ({ id: i, kind: i % 3 ? 'krabka-broker' : 'consumer', x: (i * 37) % 900 })) });
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
  await page.locator('#krabka-lab .lab-dtab[data-tab="storage"]').click();
  const box = page.locator('#krabka-lab .lab-storage input[type="checkbox"]');
  if ((await box.isChecked()) !== on) await box.click();
  await waitFor(page, `window.krabkaLab.storage.persist === ${on}`, `persistence ${on ? 'on' : 'off'}`);
}

// What the store holds for the echo counter of node 1.
const storedFrames = (page, scenarioId) =>
  page.evaluate(async (id) => {
    const images = await window.krabkaLab.storage.loadImages(id);
    const b64 = images['1']?.kv?.counters?.frames;
    return b64 == null ? null : Number(atob(b64));
  }, scenarioId);

async function main() {
  if (!fs.existsSync(path.join(DIST_DIR, 'docs', 'lab', 'index.html'))) {
    console.error('dist/docs/lab/index.html is missing: run `npm run build` first.');
    process.exit(1);
  }
  await checkInflate();
  const batches = new Uint8Array(122);
  const batchView = new DataView(batches.buffer);
  for (const start of [0, 61]) {
    batchView.setInt32(start + 8, 49);
    batches[start + 16] = 2;
  }
  batchView.setBigInt64(61, 42n);
  batchView.setInt32(61 + 57, 3);
  const fields = recordBatchFields(batches, batches.length);
  check('RecordBatch byte labels follow both batch boundaries', fields.some((f) => f.start === 61 && f.label === 'Base offset: 42') && fields.some((f) => f.start === 118 && f.label === 'Record count: 3'));

  const browser = await launchOrExit();
  const { server, port } = await serve(DIST_DIR);
  const base = `http://127.0.0.1:${port}`;
  const context = await newLabContext(browser, { width: 1400, height: 1000 });
  const errors = [];
  try {
    console.log('Cluster Lab: solo flow');
    const page = await context.newPage();
    const pageErrors = watchErrors(page, 'page', base);
    await openLab(page, base);
    check('page boots and the module initialises', true);
    await openScenario(page, NETWORK_PROBE.scenario);

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

    for (const width of [1400, 390]) {
      await page.setViewportSize({ width, height: 844 });
      for (const selector of ['.lab-canvas-wrap', '.lab-inspector', '.lab-dock']) {
        const panel = page.locator(`#krabka-lab ${selector}`).first();
        await panel.locator('.lab-expand').click();
        const bounds = await panel.boundingBox();
        check(`${selector} expands at ${width}px`, bounds.width >= width - 1 && bounds.height >= 843, JSON.stringify(bounds));
        await page.keyboard.press('Escape');
        check(`${selector} closes with Escape`, !(await panel.evaluate((el) => el.classList.contains('lab-expanded'))));
      }
    }
    await page.setViewportSize({ width: 1400, height: 1000 });
    await page.locator('#krabka-lab .lab-node[data-node-id="1"]').click({ modifiers: ['Shift'] });
    await page.locator('#krabka-lab .lab-dtab[data-tab="network"]').click();
    const dock = page.locator('#krabka-lab .lab-dock');
    const network = page.locator('#krabka-lab .lab-wire');
    for (const width of [1400, 390]) {
      await page.setViewportSize({ width, height: 844 });
      await dock.locator('.lab-expand').click();
      const bounds = await dock.boundingBox();
      check(`network bytes expands at ${width}px`, bounds.width >= width - 1 && bounds.height >= 843 && await network.evaluate((el) => el.open));
      await dock.locator('.lab-expand').click();
      check('the close button restores network bytes', !(await dock.evaluate((el) => el.classList.contains('lab-expanded'))));
    }
    await page.setViewportSize({ width: 1400, height: 1000 });
    await page.locator('#krabka-lab .lab-node[data-node-id="3"]').click();
    if (args.has('--expand-only')) {
      console.log(`\n${t.passed} checks passed${failures.length ? `, ${failures.length} failed` : ''}`);
      if (failures.length) process.exitCode = 1;
      return;
    }

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
    await page.locator('#krabka-lab .lab-dtab[data-tab="storage"]').click();
    await waitFor(page, `(() => { const td = document.querySelector('#krabka-lab tr[data-storage-node="1"] td[data-field="bytes"]'); return td && td.textContent !== '0 B'; })()`, 'the storage panel to list echo-a');
    check('the storage panel shows bytes kept for echo-a', true);

    await page.reload({ waitUntil: 'load' });
    await page.waitForSelector('#krabka-lab[data-ready="true"]', { timeout: STEP_TIMEOUT });
    const idAfterReload = await page.evaluate(() => window.krabkaLab.world.id);
    check('a reload reopens the last scenario', idAfterReload === scenarioId, `${idAfterReload} vs ${scenarioId}`);
    const restored = await readNode(page, 1);
    check('the echo counter continues from the stored value after reload', restored.state.frames >= framesAtPause, `${restored.state.frames} < ${framesAtPause}`);

    // Move away, then reopen the saved scenario from the Saved list.
    await page.locator('#krabka-lab .lab-dtab[data-tab="scenarios"]').click();
    await page.locator('#krabka-lab button', { hasText: 'New (empty)' }).click();
    await waitFor(page, `window.krabkaLab.world.scenario().nodes.length === 0`, 'an empty scenario');
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
    await page.locator('#krabka-lab .lab-dtab[data-tab="build"]').click();
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

    if (WEBRTC) {
      console.log('Cluster Lab: two tabs over WebRTC');
      await page.evaluate(() => window.krabkaLab.world.setPaused(true));
      const hub = await context.newPage();
      const hubErrors = watchErrors(hub, 'hub', base);
      await openLab(hub, base);
      await openScenario(hub, NETWORK_PROBE.scenario);
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

  console.log(`\n${t.passed} checks passed${failures.length ? `, ${failures.length} failed` : ''}`);
  if (failures.length) {
    for (const f of failures) console.error(`  • ${f}`);
    process.exit(1);
  }
  console.log('✅ PASS: the Cluster Lab works end to end.');
}

main();
