// The harness the Cluster Lab's browser checks share: Playwright and Chromium
// discovery, a static server over `dist/`, a checker, and the helpers that
// drive the lab page the way a reader does. `check-lab`, `check-lab-clusters`,
// `check-lab-external` and `check-lab-logs-ui` import it; `check-real-broker`
// keeps a harness of its own.
//
// Needs `playwright` or `playwright-core`, project-local or global (found
// through `npm root -g`), and a Chromium: the one that Playwright finds by
// itself, or else the newest `chromium-N` (or headless shell) under
// PLAYWRIGHT_BROWSERS_PATH. A script exits 2 when either is missing, 1 when a
// check fails.

import fs from 'fs';
import http from 'http';
import path from 'path';
import { createRequire } from 'module';
import { execFileSync, execSync, spawnSync } from 'child_process';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, '..');
export const DIST_DIR = path.join(ROOT, 'dist');
export const args = new Set(process.argv.slice(2));
export const HEADLESS = !args.has('--headed');
export const STEP_TIMEOUT = 30_000;

// Two echo nodes and a pinger: the lab's diagnostic scenario, with no broker
// in it. It is not a palette preset; the checks that need a running scenario
// without a broker module open it.
export const NETWORK_PROBE = {
  id: 'network-probe',
  name: 'Network probe',
  description: 'Two echo nodes and a pinger. The pinger opens a connection and pings every 100 ms; watch the round trip on the canvas and the RTT in the inspector.',
  scenario: {
    version: 1,
    seed: 7,
    name: 'Network probe',
    links: { default_latency_ms: 10 },
    nodes: [
      { id: 1, kind: 'echo', name: 'echo-a', x: 140, y: 120, config: {} },
      { id: 2, kind: 'echo', name: 'echo-b', x: 140, y: 300, config: {} },
      { id: 3, kind: 'pinger', name: 'pinger', x: 460, y: 210, config: { target: 1, period_ms: 100 } },
    ],
    topics: [],
  },
};

// ---- Playwright, project-local or global ----------------------------------------------------

export async function loadPlaywright() {
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
export function installedChromium() {
  const base = process.env.PLAYWRIGHT_BROWSERS_PATH;
  if (!base || !fs.existsSync(base)) return undefined;
  const builds = [
    [
      'chromium',
      ['chrome-linux/chrome', 'chrome-linux64/chrome', 'chrome-mac/Chromium.app/Contents/MacOS/Chromium', 'chrome-win64/chrome.exe', 'chrome-win/chrome.exe'],
    ],
  ];
  if (HEADLESS) {
    builds.unshift([
      'chromium_headless_shell',
      ['chrome-headless-shell-linux64/chrome-headless-shell', 'chrome-linux/headless_shell', 'chrome-headless-shell-win64/chrome-headless-shell.exe'],
    ]);
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
// build is missing (and to Edge on Windows when there is none at all).
export async function launchChromium(pw) {
  try {
    return await pw.chromium.launch({ headless: HEADLESS });
  } catch (err) {
    const executablePath = installedChromium();
    if (!executablePath && process.platform === 'win32') return pw.chromium.launch({ headless: HEADLESS, channel: 'msedge' });
    if (!executablePath) throw err;
    return pw.chromium.launch({ headless: HEADLESS, executablePath });
  }
}

// Playwright and Chromium, or exit 2 with what is missing.
export async function launchOrExit() {
  const pw = await loadPlaywright();
  if (!pw) {
    console.error('Playwright is not installed (neither in node_modules nor globally).');
    process.exit(2);
  }
  try {
    return await launchChromium(pw);
  } catch (err) {
    console.error(`Playwright could not launch Chromium: ${err.message.split('\n')[0]}`);
    console.error('Install it with `npx playwright install chromium`, or point PLAYWRIGHT_BROWSERS_PATH at an installed one.');
    process.exit(2);
  }
}

// A fresh browser context that has already seen the lab tour, which would
// otherwise open over the canvas and take the checks' clicks.
export async function newLabContext(browser, viewport = { width: 1400, height: 1000 }) {
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

// ---- a static server over dist/ -----------------------------------------------------------

export const MIME = {
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

// Serves `dir` with no isolation headers (the lab's service worker provides
// them, as on GitHub Pages). `handler(req, res, pathname)` may answer a
// request first, by returning true.
export function serve(dir, handler) {
  const server = http.createServer((req, res) => {
    const p = decodeURIComponent(new URL(req.url, 'http://x').pathname);
    if (handler?.(req, res, p)) return;
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
    res.writeHead(200, { 'content-type': MIME[path.extname(file)] || 'application/octet-stream', 'cache-control': 'no-store' });
    fs.createReadStream(file).pipe(res);
  });
  return new Promise((resolve) => server.listen(0, '127.0.0.1', () => resolve({ server, port: server.address().port })));
}

// ---- the WASI test guest ---------------------------------------------------------------------------------

function run(cmd, cmdArgs, options = {}) {
  const result = spawnSync(cmd, cmdArgs, { stdio: 'inherit', ...options });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`${cmd} ${cmdArgs.join(' ')} exited with ${result.status}`);
}

// `[command, ...leading args]`: binaryen's npm wasm-opt is a Node script, run
// through Node itself because Windows cannot spawn the `.bin` shim directly.
function wasmOpt() {
  if (process.argv.includes('--no-opt')) return null;
  const local = path.join(ROOT, 'node_modules', 'binaryen', 'bin', 'wasm-opt');
  if (fs.existsSync(local)) return [process.execPath, local];
  try {
    execFileSync('wasm-opt', ['--version'], { stdio: 'ignore' });
    return ['wasm-opt'];
  } catch {
    return null;
  }
}

// Builds `playground/wasi-guest` the way `check-wasi` does: cargo from inside
// the crate (its `.cargo/config.toml` sets the target and `--cfg
// tokio_unstable`), then `wasm-opt -Oz` when there is one. Returns the path of
// the module.
export function buildGuest() {
  const crate = path.join(ROOT, 'playground', 'wasi-guest');
  const targetDir = process.env.CARGO_TARGET_DIR ? path.resolve(process.env.CARGO_TARGET_DIR) : path.join(crate, 'target');
  run('cargo', ['build', '--release', '--target', 'wasm32-wasip1'], { cwd: crate, env: { ...process.env, CARGO_TARGET_DIR: targetDir } });
  const raw = path.join(targetDir, 'wasm32-wasip1', 'release', 'krabka-wasi-guest.wasm');
  const opt = wasmOpt();
  if (!opt) return raw;
  const optimised = path.join(targetDir, 'wasm32-wasip1', 'release', 'krabka-wasi-guest.lab.wasm');
  run(opt[0], [...opt.slice(1), '-Oz', '--enable-bulk-memory', '--enable-sign-ext', '--enable-mutable-globals', '--enable-nontrapping-float-to-int', '--enable-reference-types', '--enable-multivalue', raw, '-o', optimised]);
  return optimised;
}

// ---- checks ---------------------------------------------------------------------------------------------

// A counter of passed checks and a list of failures. `detail` adds the
// detail of a passing check to its line.
export function checker({ detail = false } = {}) {
  const state = {
    passed: 0,
    failures: [],
    check(name, ok, why) {
      if (ok) {
        state.passed += 1;
        console.log(`  ok   ${name}${detail && why ? `: ${why}` : ''}`);
      } else {
        state.failures.push(`${name}${why ? ` (${why})` : ''}`);
        console.error(`  FAIL ${name}${why ? ` (${why})` : ''}`);
      }
    },
    // Runs one flow; an exception fails it without hiding the others.
    async flow(name, fn) {
      try {
        await fn();
      } catch (err) {
        state.failures.push(`${name}: ${err.message}`);
        console.error(`  FAIL ${name}: ${err.stack || err.message}`);
      }
    },
    // The summary, and the exit code when a check failed.
    finish(okLine, since) {
      const secs = since == null ? '' : ` in ${((Date.now() - since) / 1000).toFixed(1)} s`;
      console.log(`\n${state.passed} checks passed${state.failures.length ? `, ${state.failures.length} failed` : ''}${secs}`);
      if (state.failures.length) {
        for (const f of state.failures) console.error(`  • ${f}`);
        process.exit(1);
      }
      console.log(okLine);
    },
  };
  return state;
}

// Polls `fn` in the page until it returns something truthy. A navigation in
// between (the isolation reload) is not an error: the poll goes on.
export async function waitFor(page, fn, label, timeout = STEP_TIMEOUT, arg) {
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

// Page errors and console errors from the page's own origin. A font or a
// script from another origin that fails to load (no network in CI, a proxy)
// is not a lab failure and is left out; `ignore(message, url)` leaves out
// more.
export function watchErrors(page, name, base, { ignore } = {}) {
  const errors = [];
  page.on('pageerror', (e) => errors.push(`${name}: ${e.message}`));
  page.on('console', (m) => {
    if (m.type() !== 'error') return;
    const url = (m.location() && m.location().url) || '';
    if (url && !url.startsWith(base)) return;
    if (ignore?.(m, url)) return;
    errors.push(`${name}: console.error ${m.text()}${url ? ` @ ${url}` : ''}`);
  });
  return errors;
}

export async function openLab(page, base, hash = '') {
  await page.goto(`${base}/docs/lab/${hash}`, { waitUntil: 'load' });
  await page.waitForSelector('#krabka-lab[data-ready="true"]', { timeout: STEP_TIMEOUT });
}

// Opens `scenario` the way a preset button does. A scenario with a real broker
// reloads the page once, cross-origin isolated, and reopens it: the wait is
// for the isolated, ready page that holds the scenario.
export async function openScenario(page, scenario, timeout = STEP_TIMEOUT) {
  // The reload may cut the evaluation short; the wait below covers it.
  await page.evaluate((doc) => window.krabkaLab.openScenario(doc), scenario).catch(() => {});
  const want = JSON.stringify(scenario.name);
  const isolated = scenario.nodes.some((n) => n.kind === 'krabka-broker') ? 'self.crossOriginIsolated && ' : '';
  await waitFor(
    page,
    `${isolated}document.querySelector('#krabka-lab[data-ready="true"]') !== null && Boolean(window.krabkaLab.world.id) && window.krabkaLab.world.scenario().name === ${want}`,
    `the ${scenario.name} scenario`,
    timeout,
  );
}

export async function setSpeed(page, speed) {
  await page.locator('#krabka-lab select[aria-label="Simulation speed"]').selectOption(String(speed));
}

// The node's snapshot entry as JSON text, or null.
export const nodeState = (id) => `(() => { const s = window.krabkaLab.world.snapshot(); const n = s && s.nodes.find((x) => x.id === ${id}); return n ? JSON.stringify({ alive: n.alive, hosted: n.hosted, isolated: n.isolated, state: n.state }) : null; })()`;

export const nodeStateOf = (page, id) => page.evaluate((id) => window.krabkaLab.world.snapshot()?.nodes.find((x) => x.id === id)?.state ?? null, id);

// Wait until `fn`, the source of a function of the snapshot's nodes by id,
// returns something truthy, and return it. It runs in the page, so it can
// only use what it is given.
export function until(page, label, fn, timeout = 60_000) {
  const expr = `(() => { const s = window.krabkaLab.world.snapshot(); if (!s) return null; const n = {}; for (const x of s.nodes) n[x.id] = x; try { const r = (${fn})(n); return r ? JSON.stringify(r) : null; } catch { return null; } })()`;
  return waitFor(page, expr, label, timeout).then((r) => JSON.parse(r));
}

// Fit every card into the canvas, as a reader does after adding a node.
export async function fit(page) {
  await page.locator('#krabka-lab .lab-canvas-tools button', { hasText: 'Fit' }).click();
}

// Select a node on the canvas and wait for the inspector to show it.
export async function inspect(page, id, name) {
  await page.locator(`#krabka-lab .lab-node[data-node-id="${id}"]`).click();
  await waitFor(page, `document.querySelector('#krabka-lab .lab-inspector .lab-insp-name')?.textContent === ${JSON.stringify(name)}`, `the inspector on ${name}`);
}

// Run a command from the inspector's command bar, after filling its inputs,
// and return what the bar says.
export async function command(page, cmd, params = {}) {
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
export const cell = (page, row, col) =>
  page.evaluate(([row, col]) => document.querySelector(`#krabka-lab .lab-inspector tr[data-row="${row}"] td[data-col="${col}"]`)?.textContent ?? null, [row, col]);

// The text of one key/value row of the inspector.
export const field = (page, name) => page.evaluate((name) => document.querySelector(`#krabka-lab .lab-inspector dd[data-field="${name}"]`)?.textContent ?? null, name);
