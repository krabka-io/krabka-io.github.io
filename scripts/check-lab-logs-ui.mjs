// End-to-end check of the Logs tab of the Cluster Lab (`/docs/lab`) in
// headless Chromium.
//
// The real `krabka-broker` module is 13.6 MB, so the WASI test guest
// (`playground/wasi-guest`) stands in for it, as in `check-lab-external`: it
// follows the broker's process contract, writes one JSON log line per event
// the way the broker does, at every level, and honours `KRABKA_LOG`. Two of
// them run in a scenario, and the check drives the tab as a reader does:
//
// - the Logs tab is in the dock and lists the lines the nodes wrote, at the
//   default level (no TRACE or DEBUG);
// - the minimum level hides the lines below it, and leaves the process rows
//   (the lab's own "started", "killed" and "restarted" rows) in place;
// - the search finds words in a line, and `field:value` terms match a line's
//   own fields, a node, a level and a number with `>`;
// - a row opens into the record as a JSON tree and closes again;
// - Download NDJSON hands out exactly the lines in view, as the node wrote
//   them;
// - the Log levels dialog takes a directive for one node, restarts it on its
//   own disk (the guest counts its boots in the volume), and the node then
//   writes at the new level while the other keeps the old one;
// - the process rows say what happened to the node.
//
// Usage:  npm run build && npm run check-lab-logs-ui [-- --headed]
// Needs cargo with the wasm32-wasip1 target, Playwright and a Chromium, as
// `check-lab-external` does. Exits 2 when a tool is missing, 1 when a check
// fails.

import fs from 'fs';
import path from 'path';
import { DIST_DIR, ROOT, buildGuest, checker, launchOrExit, newLabContext, openLab, openScenario, serve, until, waitFor, watchErrors } from './lab-check-lib.mjs';

const GUEST_URL = '/lab-test/krabka-wasi-guest.wasm';
const t = checker();
const { check } = t;

const SCENARIO = {
  version: 1,
  seed: 3,
  name: 'Logs tab',
  links: { default_latency_ms: 5 },
  nodes: [
    { id: 1, kind: 'krabka-broker', name: 'krabka-broker-1', x: 200, y: 160, config: {} },
    { id: 2, kind: 'krabka-broker', name: 'krabka-broker-2', x: 480, y: 160, config: {} },
  ],
  topics: [],
};
const DIRECTIVE = 'trace,lab_guest::heartbeat=warn';

// Everything the page holds about the tab: the store's lines, the lines in view,
// the rows in the DOM.
const logState = (page) =>
  page.evaluate(() => {
    const app = window.krabkaLab;
    const pick = (e) => ({ seq: e.seq, raw: e.raw, level: e.level, marker: e.marker, node: e.node, target: e.target, message: e.message, record: e.record });
    return {
      all: app.logs.entries().map(pick),
      view: app.logsPanel.view.map(pick),
      rows: [...document.querySelectorAll('#krabka-lab .lab-logs .lab-lgrow')].map((r) => Number(r.dataset.seq)),
      status: document.querySelector('#krabka-lab .lab-logs-count')?.textContent ?? '',
      levels: { ...app.logs.levels },
    };
  });

const lines = (entries) => entries.filter((e) => !e.marker);
const countOf = (entries, pred) => entries.filter(pred).length;

// Wait for the store to stop growing: nothing is in flight from a node.
async function quiet(page) {
  let last = -1;
  for (let i = 0; i < 100; i++) {
    const seq = await page.evaluate(() => window.krabkaLab.logs.seq);
    if (seq === last) return;
    last = seq;
    await page.waitForTimeout(400);
  }
  throw new Error('the log did not settle');
}

// Put `text` in the search box and wait for the panel to have filtered by it.
async function search(page, text) {
  const box = page.locator('#krabka-lab .lab-logs [data-field="log-search"]');
  await box.fill(text);
  await waitFor(page, `window.krabkaLab.logsPanel.filter.text === ${JSON.stringify(text)}`, `the search "${text}"`);
  return logState(page);
}

async function minLevel(page, level) {
  await page.locator(`#krabka-lab .lab-logs .lab-seg-opt.lab-lv-${level.toLowerCase()}`).click();
  await waitFor(page, `window.krabkaLab.logsPanel.filter.minLevel === ${JSON.stringify(level)}`, `the ${level} filter`);
  return logState(page);
}

async function download(page) {
  const [file] = await Promise.all([page.waitForEvent('download'), page.locator('#krabka-lab .lab-logs [data-field="log-download"]').click()]);
  return { name: file.suggestedFilename(), text: fs.readFileSync(await file.path(), 'utf8') };
}

async function logsUi(page, base) {
  console.log('Logs tab: two test guests as krabka-broker');
  await openLab(page, base);
  await openScenario(page, SCENARIO, 60_000);
  await until(page, 'both processes to run', `(n) => [1, 2].every((i) => n[i].state?.process?.state === 'running')`, 60_000);

  // The tab is in the dock, and shows what the nodes wrote.
  const tab = page.locator('#krabka-lab .lab-dtab[data-tab="logs"]');
  check('the dock has a Logs tab', (await tab.count()) === 1 && /Logs/.test(await tab.textContent()));
  await tab.click();
  // Every boot writes five lines, one per level; three heartbeats a node make the numeric filter meaningful.
  await waitFor(
    page,
    `(() => { const all = window.krabkaLab.logs.entries(); return [1, 2].every((n) => all.filter((e) => e.node === n && e.target === 'lab_guest::boot').length >= 3 && all.filter((e) => e.node === n && e.target === 'lab_guest::heartbeat').length >= 3); })()`,
    'the guests to write boot lines and three heartbeats each',
    60_000,
  );
  // A still page: the lab paused, the dock as tall as the window.
  await page.evaluate(() => window.krabkaLab.world.setPaused(true));
  await quiet(page);
  await page.locator('#krabka-lab .lab-dock .lab-expand').click();
  await page.waitForSelector('#krabka-lab .lab-logs .lab-lgrow');
  let s = await logState(page);
  const boots = lines(s.all).filter((e) => e.target === 'lab_guest::boot');
  check(
    'the Logs tab lists the lines the nodes wrote, and the guests log at the default level',
    s.view.length === s.all.length && s.rows.length > 0 && s.levels.INFO > 0 && s.levels.WARN > 0 && s.levels.ERROR > 0 && s.levels.TRACE === 0 && s.levels.DEBUG === 0 && boots.length === 6 && new RegExp(`^${s.view.length} lines$`).test(s.status),
    JSON.stringify({ status: s.status, levels: s.levels, boots: boots.length, rows: s.rows.length }),
  );
  const perLevel = await page.evaluate(() => Object.fromEntries([...document.querySelectorAll('#krabka-lab .lab-logs .lab-seg-opt')].map((l) => [l.querySelector('.lab-seg-name').textContent, l.querySelector('.lab-seg-n').textContent])));
  check(
    'each level in the filter shows how many lines it holds',
    Object.entries(perLevel).every(([level, n]) => Number(n) === countOf(lines(s.all), (e) => e.level === level)),
    JSON.stringify(perLevel),
  );

  // The level filter hides the lines below it, and keeps the process rows.
  s = await minLevel(page, 'WARN');
  const hidden = lines(s.all).length - lines(s.view).length;
  check(
    'the level filter hides the lines below it, and keeps the process rows',
    lines(s.view).length > 0 && lines(s.view).every((e) => e.level === 'WARN' || e.level === 'ERROR') && hidden > 0 && s.view.some((e) => e.marker) && /^\d+ of \d+ lines$/.test(s.status),
    JSON.stringify({ status: s.status, hidden }),
  );
  s = await minLevel(page, 'TRACE');
  check('and TRACE shows them again', s.view.length === s.all.length, s.status);

  // Search: words, then field:value terms.
  s = await search(page, 'logs at warn');
  check(
    'the search finds a word in the lines (either case)',
    s.view.length === 2 && s.view.every((e) => /^boot 1 logs at WARN$/.test(e.message)),
    JSON.stringify(s.view.map((e) => e.message)),
  );
  const fieldCases = [
    ['node_id:1', (e) => e.record?.node_id === 1, (e) => lines([e]).length === 1],
    ['level:error', (e) => e.level === 'ERROR' && !e.marker, (e) => e.level === 'ERROR' && !e.marker],
    ['node:2', (e) => e.node === 2, (e) => e.node === 2],
    ['ticks:>=30', (e) => !e.marker && e.record?.ticks >= 30, (e) => !e.marker && e.record?.ticks >= 30],
  ];
  const fieldBad = [];
  for (const [query, want, every] of fieldCases) {
    const r = await search(page, query);
    const expected = r.all.filter(want).length;
    if (!(r.view.length > 0 && r.view.length === expected && r.view.every(want) && r.view.every(every))) fieldBad.push(`${query}: ${r.view.length} shown, ${expected} expected`);
  }
  check('field:value terms match a line\'s fields, its node, its level and a number', fieldBad.length === 0, fieldBad.join('; '));
  s = await search(page, 'node_id:1 level:warn');
  check('terms together narrow the lines to those that match all of them', s.view.length === 1 && s.view[0].record.node_id === 1 && s.view[0].level === 'WARN', JSON.stringify(s.view.map((e) => e.message)));

  // A row opens into its record.
  s = await search(page, 'boot 1 logs at WARN');
  const head = page.locator('#krabka-lab .lab-lgrow .lab-log-head', { hasText: 'boot 1 logs at WARN' }).first();
  await head.click();
  await page.waitForSelector('#krabka-lab .lab-lgrow .lab-log-detail .lab-json');
  const open = await page.evaluate(() => {
    const row = document.querySelector('#krabka-lab .lab-lgrow.lab-log-open');
    return {
      expanded: row?.querySelector('.lab-log-head')?.getAttribute('aria-expanded'),
      keys: [...(row?.querySelectorAll('.lab-json-key') ?? [])].map((k) => k.textContent.replace(/: $/, '')),
      values: [...(row?.querySelectorAll('.lab-json-row') ?? [])].map((r) => r.textContent),
    };
  });
  check(
    'a row expands into its record as a JSON tree',
    open.expanded === 'true' && ['ts', 'level', 'target', 'message', 'node_id'].every((k) => open.keys.includes(k)) && open.values.some((v) => v.includes('lab_guest::boot')) && open.values.some((v) => v.includes('boot 1 logs at WARN')),
    JSON.stringify(open),
  );
  await head.click();
  check('and closes again', (await page.locator('#krabka-lab .lab-lgrow .lab-log-detail').count()) === 0);

  // Download hands out the lines in view, byte for byte.
  s = await minLevel(page, 'WARN');
  s = await search(page, 'node:1');
  const filtered = await download(page);
  const want = s.view.length ? `${s.view.map((e) => e.raw).join('\n')}\n` : '';
  const parsed = filtered.text.trim().split('\n').map((l) => JSON.parse(l));
  check(
    'Download NDJSON holds exactly the lines in view, as the node wrote them',
    filtered.name === 'krabka-lab-logs.ndjson' && filtered.text === want && s.view.length > 0 && parsed.length === s.view.length && parsed.every((r) => r.level === 'WARN' || r.level === 'ERROR' || r.target === 'lab::process') && !filtered.text.includes('logs at INFO'),
    `${filtered.name}, ${s.view.length} lines in view, ${parsed.length} downloaded`,
  );
  s = await search(page, '');
  s = await minLevel(page, 'TRACE');
  const everything = await download(page);
  check('with no filter it holds every line, process rows included', everything.text === `${s.all.map((e) => e.raw).join('\n')}\n` && s.all.some((e) => e.marker), `${everything.text.split('\n').length - 1} lines, ${s.all.length} held`);

  // The process rows tell what happened to a node.
  const markers = s.all.filter((e) => e.marker);
  const rowsOf = await page.evaluate(() => [...document.querySelectorAll('#krabka-lab .lab-lgrow.lab-log-marker')].map((r) => r.querySelector('.lab-log-level').textContent));
  check(
    'each node starts with a process row, shown as PROC',
    [1, 2].every((n) => markers.some((e) => e.node === n && e.marker === 'started' && /default log level/.test(e.message))) && rowsOf.length > 0 && rowsOf.every((l) => l === 'PROC'),
    JSON.stringify({ rows: rowsOf, markers: markers.map((e) => e.message) }),
  );

  // The Log levels dialog: a directive for node 1, applied by a restart on its disk.
  await page.evaluate(() => window.krabkaLab.world.setPaused(false));
  await page.locator('#krabka-lab .lab-logs [data-field="log-levels"]').click();
  const dialog = page.locator('#krabka-lab dialog[aria-label="Log levels"]');
  await dialog.waitFor();
  const running = await dialog.locator('[data-field="loglevel-running"]').textContent();
  check('the Log levels dialog says what each broker runs with', /krabka-broker-1 started with the default/.test(running) && /krabka-broker-2 started with the default/.test(running), running);
  await dialog.locator('[data-field="loglevel-scope"]').selectOption('1');
  await dialog.locator('[data-field="loglevel-directive"]').fill(DIRECTIVE);
  const verdict = await dialog.locator('.lab-loglevels-check').textContent();
  const submit = dialog.locator('button[type="submit"]');
  const label = await submit.textContent();
  check('it checks the directive and says that one broker restarts', /^Valid/.test(verdict) && label === 'Apply and restart 1 broker', `${verdict} / ${label}`);
  await submit.click();
  const confirm = page.locator('#krabka-lab dialog[aria-label="Restart 1 broker on its disk?"]');
  await confirm.waitFor();
  const warning = await confirm.textContent();
  check('a restart is confirmed first, and the dialog says the disk keeps its data', /krabka-broker-1/.test(warning) && /keeps its data/.test(warning), warning.slice(0, 160));
  await confirm.locator('button[type="submit"]').click();
  await waitFor(page, `document.querySelectorAll('#krabka-lab dialog[open]').length === 0`, 'the dialogs to close');

  const restarted = await until(page, 'node 1 to restart', `(n) => n[1].state.process.incarnation === 2 && n[1].state.process.state === 'running' && { incarnation: n[1].state.process.incarnation, other: n[2].state.process.incarnation, env: n[1].state.env }`, 60_000);
  check(
    'applying restarts that node, and only that node, with the directive as KRABKA_LOG',
    restarted.env.KRABKA_LOG === DIRECTIVE && restarted.other === 1,
    JSON.stringify(restarted),
  );
  // Lines from the restarted process only: those after its "restarted" row.
  const from = await page.evaluate(() => window.krabkaLab.logs.entries().find((e) => e.marker === 'restarted' && e.node === 1)?.seq ?? 0);
  const levelsAfter = await waitFor(
    page,
    `(() => { const all = window.krabkaLab.logs.entries().filter((e) => e.seq > ${from} && e.node === 1 && e.target === 'lab_guest::boot'); return all.length >= 5 ? JSON.stringify(all.map((e) => e.message)) : null; })()`,
    'the restarted guest to write its boot lines',
    60_000,
  ).then(JSON.parse);
  // "boot 2": the same volume, which has now seen two boots.
  const expected = ['TRACE', 'DEBUG', 'INFO', 'WARN', 'ERROR'].map((l) => `boot 2 logs at ${l}`);
  check('the guest then logs at the new level, on the disk it had (its second boot)', JSON.stringify(levelsAfter) === JSON.stringify(expected), JSON.stringify(levelsAfter));
  // Node 2 went on at the default; node 1's heartbeats are below its new level for that target.
  const beat = (n) => `window.krabkaLab.logs.entries().filter((e) => e.seq > ${from} && e.node === ${n} && e.target === 'lab_guest::heartbeat').length`;
  await waitFor(page, `${beat(2)} >= 3`, 'node 2 to keep beating', 30_000);
  const silent = await page.evaluate(`${beat(1)}`);
  s = await logState(page);
  check('and the other node keeps its level, while the directive\'s own target entry quiets the heartbeat', silent === 0 && countOf(s.all, (e) => e.seq > from && e.node === 2 && e.level === 'TRACE') === 0, `${silent} heartbeats from node 1`);
  const saved = await page.evaluate(() => window.krabkaLab.logLevels.directive(window.krabkaLab.world.id, 1));
  check('the setting is kept for that node', saved === DIRECTIVE, saved);

  // Process rows for the restart.
  s = await search(page, 'kind:restarted');
  const restartRow = await page.evaluate(() => [...document.querySelectorAll('#krabka-lab .lab-lgrow.lab-log-marker')].map((r) => r.textContent));
  s = await search(page, 'kind:killed');
  check(
    'the restart shows as process rows: killed, then restarted with the new directive',
    s.view.length === 1 && s.view[0].node === 1 && s.view[0].level === 'WARN' && restartRow.length === 1 && restartRow[0].includes('PROC') && restartRow[0].includes(`KRABKA_LOG=${DIRECTIVE}`),
    JSON.stringify({ killed: s.view.map((e) => e.message), restarted: restartRow }),
  );
  await search(page, '');
}

async function main() {
  if (!fs.existsSync(path.join(DIST_DIR, 'docs', 'lab', 'index.html'))) {
    console.error('dist/docs/lab/index.html is missing: run `npm run build` first.');
    process.exit(1);
  }
  let guest;
  try {
    guest = buildGuest();
  } catch (err) {
    console.error(`The WASI test guest did not build: ${err.message}`);
    process.exit(2);
  }
  console.log(`  guest: ${path.relative(ROOT, guest).startsWith('..') ? guest : path.relative(ROOT, guest)} (${fs.statSync(guest).size.toLocaleString('en')} bytes)`);
  const browser = await launchOrExit();
  const { server, port } = await serve(DIST_DIR, (req, res, p) => {
    if (p !== GUEST_URL) return false;
    res.writeHead(200, { 'content-type': 'application/wasm', 'cache-control': 'no-store' });
    fs.createReadStream(guest).pipe(res);
    return true;
  });
  const base = `http://127.0.0.1:${port}`;
  const started = Date.now();
  const errors = [];
  try {
    const context = await newLabContext(browser, { width: 1400, height: 1000 });
    await context.addInitScript((url) => sessionStorage.setItem('krabka-lab.broker-module', url), GUEST_URL);
    const page = await context.newPage();
    errors.push(...watchErrors(page, 'lab', base));
    await t.flow('logs tab', () => logsUi(page, base));
    await context.close();
  } finally {
    await browser.close();
    server.close();
  }
  check('no page errors or console errors', errors.length === 0, errors.slice(0, 3).join(' | '));
  t.finish('✅ PASS: the Logs tab works end to end.', started);
}

main();
