// End-to-end check of the Cluster Lab's scripted experiments on real brokers,
// in headless Chromium.
//
// Serves the built site from `dist/` (with the real `krabka-broker` module,
// `npm run build:broker`) and runs every preset that carries an `experiment`
// the way a reader does: open the preset, press Run experiment (which
// restarts it fresh), and wait for the run to end. Each preset runs in a
// browser context of its own, at SPEED (the real brokers take real CPU, so
// much faster starves them and they lag the lab's clock). Every step must
// apply and every check must pass. Before the first run it checks that the
// experiment survives a share link and an export/import round trip.
//
// Usage:  npm run build && npm run build:broker && npm run check-lab-experiments [-- --headed] [-- --only=min-isr]
//         [-- --base=http://localhost:4403]   (a running `astro dev` instead of dist/)
// Needs Playwright and a Chromium (see lab-check-lib.mjs). Exits 2 when one
// is missing, or when dist/ has no broker build, 1 when a check fails.

import fs from 'fs';
import path from 'path';
import { pathToFileURL } from 'url';
import { DIST_DIR, ROOT, checker, launchOrExit, newLabContext, openLab, openScenario, serve, setSpeed, waitFor, watchErrors } from './lab-check-lib.mjs';

const SPEED = 5;
const t = checker({ detail: true });
const { check } = t;
const option = (name) => process.argv.find((a) => a.startsWith(`--${name}=`))?.slice(name.length + 3);

// The experiment travels with the scenario: through a share link, and
// through an export and an import of the scenario JSON.
async function roundTrips(page, preset) {
  const r = await page.evaluate(async () => {
    const { shareLink, scenarioFromHash, validateScenario } = await import('/playground/lab/scenarios.js');
    const doc = window.krabkaLab.world.scenario();
    const shared = await scenarioFromHash(new URL(await shareLink(doc)).hash);
    const imported = validateScenario(JSON.parse(JSON.stringify(doc, null, 2)));
    return { want: JSON.stringify(doc.experiment), shared: JSON.stringify(shared.experiment), imported: JSON.stringify(imported.experiment) };
  });
  check(`${preset.id}: the experiment survives Copy link and Export/Import`, r.want && r.shared === r.want && r.imported === r.want, r.want?.slice(0, 80));
}

async function runPreset(page, preset) {
  const exp = preset.scenario.experiment;
  // Run experiment, from the Scenarios tab.
  await page.locator('#krabka-lab .lab-dtab[data-tab="scenarios"]').click();
  const editor = await page.locator('#krabka-lab .lab-exp-editor').inputValue();
  check(`${preset.id}: the Scenarios tab shows the experiment`, JSON.stringify(JSON.parse(editor)) === JSON.stringify(exp));
  await page.locator('#krabka-lab .lab-exp button', { hasText: 'Run experiment' }).click();
  await waitFor(page, `window.krabkaLab.experiment?.state === 'running'`, 'the run to start', 120_000);
  await setSpeed(page, SPEED);
  // Wall time: the lab time at SPEED, with room for the brokers to lag.
  const budget = (exp.end / SPEED) * 4 + 120_000;
  let last = '';
  const started = Date.now();
  for (;;) {
    const r = await page.evaluate(() => window.krabkaLab.experiment.toJSON());
    const line = `${r.state} ${Math.round(r.now / 1000)}s: ${r.checks.map((c) => c.status[0]).join('')}`;
    if (line !== last && process.argv.includes('--verbose')) console.log(`    ${line}`);
    last = line;
    if (!['running', 'starting'].includes(r.state)) break;
    if (Date.now() - started > budget) throw new Error(`the experiment did not end within ${Math.round(budget / 1000)} s (${line})`);
    await page.waitForTimeout(1000);
  }
  const r = await page.evaluate(() => window.krabkaLab.experiment.toJSON());
  for (const s of r.steps) check(`${preset.id}: step at ${s.at} ms, ${s.label}`, s.status === 'done', `${s.status}${s.t != null ? ` at ${s.t} ms` : ''}${s.error ? `: ${s.error}` : ''}`);
  for (const c of r.checks) check(`${preset.id}: ${c.at != null ? `at ${c.at}` : `by ${c.by ?? r.end}`} ms, ${c.label}`, c.status === 'pass', `${c.status}${c.t != null ? ` at ${c.t} ms` : ''}: ${c.observed}`);
  check(`${preset.id}: the run ends ${r.state === 'passed' ? 'passed' : r.state}`, r.state === 'passed', `${r.passed} of ${r.checks.length} checks passed`);
  const shown = await page.locator('#krabka-lab .lab-exp-results .lab-exp-row').count();
  check(`${preset.id}: the results panel lists every step and check`, shown === r.steps.length + r.checks.length, `${shown} rows`);
}

async function main() {
  const external = option('base');
  if (!external) {
    if (!fs.existsSync(path.join(DIST_DIR, 'docs', 'lab', 'index.html'))) {
      console.error('dist/docs/lab/index.html is missing: run `npm run build` first.');
      process.exit(1);
    }
    if (!fs.existsSync(path.join(DIST_DIR, 'playground', 'broker', 'krabka-broker.wasm'))) {
      console.error('dist/playground/broker/krabka-broker.wasm is missing: run `npm run build:broker` and `npm run build` first.');
      process.exit(2);
    }
  }
  const { PRESETS } = await import(pathToFileURL(path.join(ROOT, 'public', 'playground', 'lab', 'presets.js')).href);
  const only = option('only');
  const presets = PRESETS.filter((p) => p.scenario.experiment && (!only || only.split(',').includes(p.id)));
  check('at least one preset carries an experiment', presets.length > 0, presets.map((p) => p.id).join(', '));
  const browser = await launchOrExit();
  const served = external ? null : await serve(DIST_DIR);
  const base = external ?? `http://127.0.0.1:${served.port}`;
  const errors = [];
  const started = Date.now();
  try {
    for (const [i, preset] of presets.entries()) {
      console.log(`Experiment: ${preset.name} · ${preset.scenario.experiment.name}`);
      const t0 = Date.now();
      // A context of its own: its IndexedDB and broker disks are the run's.
      const context = await newLabContext(browser, { width: 1400, height: 1000 });
      const page = await context.newPage();
      errors.push(...watchErrors(page, preset.id, base));
      await t.flow(preset.id, async () => {
        await openLab(page, base);
        await openScenario(page, preset.scenario, 120_000);
        if (i === 0) await roundTrips(page, preset);
        await runPreset(page, preset);
      });
      console.log(`  (${((Date.now() - t0) / 1000).toFixed(1)} s)`);
      await context.close();
    }
  } finally {
    await browser.close();
    served?.server.close();
  }
  check('no page errors or console errors', errors.length === 0, errors.slice(0, 3).join(' | '));
  t.finish('✅ PASS: every preset experiment passes on real brokers.', started);
}

main();
