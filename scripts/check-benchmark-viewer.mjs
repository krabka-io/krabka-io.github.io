// Browser checks against the staged, measured capture (no synthetic chart data).
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { chromium } from 'playwright';
import { serve, DIST_DIR, installedChromium } from './lab-check-lib.mjs';

const archive = JSON.parse(fs.readFileSync('src/data/benchmark-archive.json', 'utf8'));
const latest = archive.runs[0];
const run = archive.runs.find(run => run.trials.every(trial => trial.resource_samples > 0));
const output = path.resolve('artifacts/benchmark-viewer');
fs.mkdirSync(output, { recursive: true });
let browser;
try { browser = await chromium.launch({ headless: true }); }
catch (error) {
  const executablePath = installedChromium() ?? (fs.existsSync('/usr/bin/chromium') ? '/usr/bin/chromium' : undefined);
  if (!executablePath) throw error;
  browser = await chromium.launch({ headless: true, executablePath });
}
const { server, port } = await serve(DIST_DIR);
const base = `http://127.0.0.1:${port}`;
const errors = [];
let failedRequest;
try {
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 }, colorScheme: 'dark' });
  const page = await context.newPage();
  await page.route('**/result.json.gz', route => route.fulfill({
    body: fs.readFileSync(path.join(DIST_DIR, new URL(route.request().url()).pathname)),
    headers: { 'content-type': 'application/json', 'content-encoding': 'gzip' },
  }));
  page.on('pageerror', error => errors.push(error.message));
  page.on('response', response => { if (response.status() >= 400 && response.url().startsWith(base)) errors.push(`${response.status()} ${response.url()}`); });
  const loaded = () => page.waitForFunction(() => document.getElementById('explorer-status').textContent.startsWith('Loaded'), { timeout: 30000 });
  await page.route('**/archive-data/index.json', route => route.abort());
  await page.goto(base + '/benchmarks/explorer/');
  await page.waitForFunction(() => document.getElementById('explorer-status').hasAttribute('data-error'));
  assert.match(await page.locator('#explorer-status').textContent(), /Reload to retry/);
  await page.unroute('**/archive-data/index.json');
  await page.locator('#retry-load').click(); await loaded();
  await page.goto(base + '/benchmarks/explorer/'); await loaded();
  await page.unroute('**/result.json.gz');
  assert.equal(await page.locator('#run-select').inputValue(), latest.id);
  // The newest run offers resource timelines only when its archive supplies them.
  const latestHasResources = latest.trials.every(trial => trial.resource_samples > 0);
  assert.match(await page.locator('#explorer-status').textContent(), latestHasResources ? /Resource samples available/ : /not published/);
  assert.equal(await page.locator('#source-select option[value="resources"]').evaluate(option => option.disabled), !latestHasResources);
  assert.equal(await page.locator('[data-matrix-row]:visible').count(), 14);
  await page.selectOption('[data-matrix-peer]', 'redpanda');
  assert.match(await page.locator('[data-matrix-summary]').textContent(), /1 not measured/);
  await page.selectOption('[data-matrix-metric]', 'end_to_end_p99_ms');
  assert.match(await page.locator('[data-matrix-note]').textContent(), /aggregate p99/);
  await page.selectOption('[data-matrix-filter]', 'all');
  assert.equal(await page.locator('[data-matrix-row]:visible').count(), latest.provenance.cases.length * latest.provenance.replication_factors.length);
  for (const workload of latest.provenance.cases) {
    const hint = page.locator(`[data-matrix-row][data-workload="${workload.id}"][data-rf="1"] [data-benchmark-hint]`);
    await hint.hover();
    await page.locator('#benchmark-workload-tooltip').waitFor({ state: 'visible' });
    assert.match(await page.locator('#benchmark-workload-tooltip').textContent(), /Why this test is useful/);
    await page.keyboard.press('Escape');
    assert.equal(await page.locator('#benchmark-workload-tooltip').isVisible(), false);
  }
  const firstHint = page.locator('[data-matrix-row] [data-benchmark-hint]').first();
  await firstHint.focus();
  assert.equal(await page.locator('#benchmark-workload-tooltip').isVisible(), true);
  await page.keyboard.press('Escape');
  await page.setViewportSize({ width: 375, height: 1000 });
  await firstHint.click();
  assert.equal(await page.locator('#benchmark-workload-tooltip').isVisible(), true);
  const hintBounds = await page.locator('#benchmark-workload-tooltip').boundingBox();
  assert.ok(hintBounds.x >= 0 && hintBounds.x + hintBounds.width <= 375);
  await page.screenshot({ path: path.join(output, 'hint-375.png') });
  await page.keyboard.press('Escape');
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.selectOption('[data-matrix-peer]', 'previous');
  assert.match(await page.locator('[data-matrix-note]').textContent(), /preceding published release/);
  assert.equal(await page.locator('[data-matrix-row]').first().locator('[data-matrix-cell] strong').first().textContent(), 'First run');
  await page.selectOption('[data-matrix-peer]', 'kafka');
  await page.selectOption('[data-matrix-metric]', 'publish_rate');
  await page.selectOption('[data-matrix-filter]', 'max');
  // Every newer published capture loads without inventing resource telemetry.
  for (const capture of archive.runs.filter(r => r.id !== run.id)) {
    await page.selectOption('#run-select', capture.id); await loaded();
    for (const rf of capture.provenance.replication_factors) for (const workload of capture.provenance.cases) {
      await page.selectOption('#rf-select', String(rf)); await page.selectOption('#workload-select', workload.id); await loaded();
      const trials = capture.trials.filter(t => t.rf === rf && t.case.id === workload.id);
      assert.equal(await page.locator('#summary-cards article').count(), trials.length);
      assert.deepEqual(await page.locator('#summary-cards strong').allTextContents(), trials.map(t => t.publish_rate.toLocaleString('en-US', { maximumFractionDigits: 0 })));
    }
  }
  await page.selectOption('#run-select', run.id); await loaded();
  assert.equal(await page.locator('#catalog-table tbody tr').count(), run.trials.length);
  assert.equal(await page.locator('#summary-cards article').count(), 3);
  assert.equal(await page.locator('#temporal-chart .chart-line').count(), 3);
  assert.ok((await page.locator('#temporal-chart .chart-line').first().getAttribute('d')).startsWith('M'));
  await page.screenshot({ path: path.join(output, 'desktop-dark.png'), fullPage: true });

  // Every workload/RF selection resolves real files and agrees with its index.
  for (const rf of run.provenance.replication_factors) for (const workload of run.provenance.cases) {
    await page.selectOption('#rf-select', String(rf));
    await page.selectOption('#workload-select', workload.id);
    await loaded();
    const trials = run.trials.filter(t => t.rf === rf && t.case.id === workload.id);
    assert.equal(await page.locator('#selection-title').textContent(), workload.id);
    const status = await page.locator('#explorer-status').textContent();
    assert.ok(status.includes(trials.reduce((n, t) => n + t.interval_samples, 0).toLocaleString()), status);
    const cards = await page.locator('#summary-cards strong').allTextContents();
    assert.deepEqual(cards, trials.map(t => t.publish_rate.toLocaleString('en-US', { maximumFractionDigits: 0 })));
  }
  console.log(`Verified all ${run.provenance.cases.length * run.provenance.replication_factors.length} workload/RF selections (${run.trials.length} trials).`);

  await page.selectOption('#workload-select', 'simple-workload'); await loaded();
  await page.selectOption('#source-select', 'resources');
  await page.selectOption('#metric-select', 'cpu_cores');
  await page.selectOption('#scope-select', '0');
  assert.match(await page.locator('#timeline-note').textContent(), /actual sample gap/);
  await page.selectOption('#scale-select', 'log');
  await page.locator('#window-start').fill('20'); await page.locator('#window-start').dispatchEvent('input');
  await page.locator('#window-end').fill('40'); await page.locator('#window-end').dispatchEvent('input');
  const windowSamples = await page.locator('#sample-badge').textContent();
  assert.ok(parseInt(windowSamples.replaceAll(',', '')) > 0);
  await page.locator('#time-cursor').fill('500'); await page.locator('#time-cursor').dispatchEvent('input');
  assert.match(await page.locator('#cursor-values').textContent(), /cores @/);
  await page.locator('#sample-details summary').click();
  assert.equal(await page.locator('#sample-table tbody tr').count(), 50);
  await page.locator('#sample-next').click(); assert.match(await page.locator('#sample-page').textContent(), /Page 2/);
  const csvEvent = page.waitForEvent('download'); await page.locator('#export-samples').click();
  const csvDownload = await csvEvent; const csvText = fs.readFileSync(await csvDownload.path(), 'utf8');
  assert.match(csvText, /capture_time_utc/); assert.match(csvText, /2026-/);
  const sharedURL = page.url(); await page.reload(); await loaded();
  assert.equal(await page.locator('#source-select').inputValue(), 'resources');
  assert.equal(await page.locator('#metric-select').inputValue(), 'cpu_cores');
  assert.equal(await page.locator('#scope-select').inputValue(), '0');
  assert.equal(await page.locator('#window-start').inputValue(), '20');
  assert.equal(page.url(), sharedURL);
  await page.locator('input[name="vendor"][value="kafka"]').uncheck();
  assert.equal(await page.locator('#temporal-chart .chart-line').count(), 2);
  await page.locator('#distribution-details summary').click();
  await page.waitForFunction(() => document.querySelectorAll('#distribution-chart .chart-line').length === 2);
  assert.equal(await page.locator('#distribution-chart .chart-line').count(), 2);
  await page.selectOption('#latency-family', 'PublishDelayLatency');
  assert.match(await page.locator('#latency-table thead').textContent(), /µs/);
  await page.selectOption('#raw-file', 'workload.stderr'); await page.locator('#open-file').click();
  await page.waitForFunction(() => document.getElementById('raw-status').textContent.includes('matching lines'));
  assert.match(await page.locator('#raw-content').textContent(), /empty/);
  await page.selectOption('#raw-file', 'result.json'); await page.locator('#open-file').click();
  await page.waitForFunction(() => document.getElementById('raw-status').textContent.includes('original JSON fields'));
  await page.locator('#raw-search').fill('aggregatedPublishLatency99pct');
  assert.match(await page.locator('#raw-content').textContent(), /aggregatedPublishLatency99pct/);
  await page.locator('#raw-search').fill('');
  await page.selectOption('#raw-file', 'workload.stdout'); await page.locator('#open-file').click();
  await page.waitForFunction(() => document.getElementById('raw-status').textContent.includes('matching lines'));
  await page.locator('#raw-search').fill('Starting benchmark traffic');
  assert.match(await page.locator('#raw-content').textContent(), /Starting benchmark traffic/);
  await page.selectOption('#raw-file', 'provenance.json'); await page.locator('#raw-search').fill(''); await page.locator('#open-file').click();
  await page.waitForFunction(() => document.getElementById('raw-status').textContent.includes('original JSON fields'));
  const downloadEvent = page.waitForEvent('download'); await page.locator('#download-file').click();
  const download = await downloadEvent;
  assert.equal(JSON.parse(fs.readFileSync(await download.path(), 'utf8')).run_id, run.id);
  await page.selectOption('#catalog-sort', 'rate');
  const topRate = Math.max(...run.trials.map(t => t.publish_rate)).toLocaleString('en-US', { maximumFractionDigits: 0 });
  assert.equal(await page.locator('#catalog-table tbody tr').first().locator('td').nth(3).textContent(), topRate);
  await page.selectOption('#catalog-sort', 'capture');
  await page.locator('#catalog-search').fill('rf1-krabka');
  assert.equal(await page.locator('#catalog-table tbody tr').count(), run.provenance.cases.length);
  await page.locator('#catalog-table tbody button').first().click(); await loaded();
  assert.equal(await page.locator('#rf-select').inputValue(), '1');
  assert.match(await page.locator('#detail-trial').inputValue(), /rf1-krabka/);
  await page.locator('#catalog-search').fill('');
  await page.locator('#workload-search').fill('no-workloads-match-this');
  assert.match(await page.locator('#explorer-status').textContent(), /No trials/);
  await page.locator('#workload-search').fill('simple'); await loaded();
  await page.locator('#workload-search').fill(''); await loaded();

  // Failed lazy loads are visible and recoverable.
  await page.route('**/resources.jsonl.gz', route => { failedRequest = true; return route.abort(); });
  await page.selectOption('#workload-select', 'max-rate-1-topic-100-partitions-1kb');
  await page.waitForFunction(() => document.getElementById('explorer-status').hasAttribute('data-error'));
  assert.ok(failedRequest);
  await page.unroute('**/resources.jsonl.gz'); await page.locator('#retry-load').click(); await loaded();

  // Capture the distribution and raw drilldown with the current trial loaded.
  await page.locator('#raw-search').fill('');
  await page.selectOption('#raw-file', 'result.json'); await page.locator('#open-file').click();
  await page.waitForFunction(() => document.getElementById('raw-status').textContent.includes('original JSON fields'));
  for (const width of [375, 1440]) for (const theme of ['light', 'dark']) {
    await page.setViewportSize({ width, height: 1000 });
    await page.evaluate(theme => { localStorage.setItem('theme', theme); document.documentElement.dataset.theme = theme; }, theme);
    await page.evaluate(() => scrollTo(0, 0));
    await page.waitForTimeout(500);
    const structure = await page.evaluate(() => ({ overflow: document.documentElement.scrollWidth - innerWidth, h1s: document.querySelectorAll('h1').length, unnamed: [...document.querySelectorAll('select, input, button')].filter(el => el.getClientRects().length && !el.textContent.trim() && !el.labels?.length && !el.getAttribute('aria-label')).length }));
    assert.ok(structure.overflow <= 1, `${width} ${theme}: ${structure.overflow}px overflow`);
    assert.equal(structure.h1s, 1); assert.equal(structure.unnamed, 0);
    await page.screenshot({ path: path.join(output, `${width}-${theme}.png`), fullPage: true });
    await page.locator('.chart-panel').screenshot({ path: path.join(output, `temporal-${width}-${theme}.png`) });
    await page.locator('section[aria-labelledby="distribution-heading"]').screenshot({ path: path.join(output, `distribution-${width}-${theme}.png`) });
    await page.locator('section[aria-labelledby="raw-heading"]').screenshot({ path: path.join(output, `drilldown-${width}-${theme}.png`) });
  }
  for (const route of ['/', '/benchmarks/', '/benchmarks/explorer/']) {
    await page.goto(base + route);
    if (route.includes('explorer')) await loaded();
    for (const width of [375, 1440]) for (const theme of ['light', 'dark']) {
      await page.setViewportSize({ width, height: 1000 });
      await page.evaluate(theme => { document.documentElement.dataset.theme = theme; }, theme);
      await page.evaluate(() => scrollTo(0, 0));
      await page.waitForTimeout(500);
      assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1), `${route} ${width} ${theme}: page overflow`);
      const surface = route === '/' ? 'home' : route.includes('explorer') ? 'latest' : 'report';
      await page.screenshot({ path: path.join(output, `${surface}-${width}-${theme}.png`), fullPage: true });
      if (route.includes('explorer')) await page.locator('[data-benchmark-matrix]').screenshot({ path: path.join(output, `matrix-${width}-${theme}.png`) });
    }
  }
  assert.deepEqual(errors, []);
  fs.writeFileSync(path.join(output, 'visual-qc.json'), JSON.stringify({
    route: '/benchmarks/explorer/', browser: 'Headless Chromium', widths: [375, 1440], themes: ['light', 'dark'],
    matrix: { combinations: run.provenance.cases.length * run.provenance.replication_factors.length, trials: run.trials.length },
    steps: [
      { step: 1, surface: 'Selection and temporal comparison', health: 'Passed', evidence: ['desktop-dark.png', 'temporal-375-light.png', 'temporal-1440-dark.png'], checks: ['every workload/RF combination', 'metric and scope switching', 'zoom and cursor', 'exact samples and CSV', 'shareable URL restoration'] },
      { step: 2, surface: 'Latency distribution', health: 'Passed', evidence: ['distribution-1440-light.png', 'distribution-375-dark.png'], checks: ['aggregate units', 'full percentile-to-latency mapping', 'contained plot', 'independent legend'] },
      { step: 3, surface: 'Trial drilldown and catalog', health: 'Passed', evidence: ['drilldown-1440-dark.png', 'drilldown-375-light.png'], checks: ['searchable JSON and logs', 'empty stderr', 'original-file download', 'catalog filtering and sorting', 'failed-fetch retry', 'named controls', 'no page overflow'] },
    ],
    limits: ['Screenshots and browser checks do not establish complete accessibility compliance.', 'The broader site budget check requires the separately built broker WASM binary, absent in this local checkout.'],
  }, null, 2) + '\n');
  console.log('Verified temporal controls, exact samples, CSV, distributions, raw drilldown, URL state, retry, and mobile/light/dark layouts.');
} finally { await browser.close(); server.close(); }
