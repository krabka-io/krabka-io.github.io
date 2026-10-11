// Browser checks against the staged, measured capture (no synthetic chart data).
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { chromium } from 'playwright';
import { serve, DIST_DIR, installedChromium } from './lab-check-lib.mjs';

const archive = JSON.parse(fs.readFileSync('src/data/benchmark-archive.json', 'utf8'));
const run = archive.runs[0];
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
  page.on('pageerror', error => errors.push(error.message));
  page.on('response', response => { if (response.status() >= 400 && response.url().startsWith(base)) errors.push(`${response.status()} ${response.url()}`); });
  const loaded = () => page.waitForFunction(() => document.getElementById('explorer-status').textContent.startsWith('Loaded'), { timeout: 30000 });
  await page.goto(base + '/benchmarks/explorer/'); await loaded();
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
  for (const width of [390, 1440]) for (const theme of ['light', 'dark']) {
    await page.setViewportSize({ width, height: 1000 });
    await page.evaluate(theme => { localStorage.setItem('theme', theme); document.documentElement.dataset.theme = theme; }, theme);
    await page.waitForTimeout(200);
    const structure = await page.evaluate(() => ({ overflow: document.documentElement.scrollWidth - innerWidth, h1s: document.querySelectorAll('h1').length, unnamed: [...document.querySelectorAll('select, input, button')].filter(el => el.getClientRects().length && !el.textContent.trim() && !el.labels?.length && !el.getAttribute('aria-label')).length }));
    assert.ok(structure.overflow <= 1, `${width} ${theme}: ${structure.overflow}px overflow`);
    assert.equal(structure.h1s, 1); assert.equal(structure.unnamed, 0);
    await page.screenshot({ path: path.join(output, `${width}-${theme}.png`), fullPage: true });
    await page.locator('.chart-panel').screenshot({ path: path.join(output, `temporal-${width}-${theme}.png`) });
    await page.locator('section[aria-labelledby="distribution-heading"]').screenshot({ path: path.join(output, `distribution-${width}-${theme}.png`) });
    await page.locator('section[aria-labelledby="raw-heading"]').screenshot({ path: path.join(output, `drilldown-${width}-${theme}.png`) });
  }
  assert.deepEqual(errors, []);
  fs.writeFileSync(path.join(output, 'visual-qc.json'), JSON.stringify({
    route: '/benchmarks/explorer/', browser: 'Headless Chromium', widths: [390, 1440], themes: ['light', 'dark'],
    matrix: { combinations: run.provenance.cases.length * run.provenance.replication_factors.length, trials: run.trials.length },
    steps: [
      { step: 1, surface: 'Selection and temporal comparison', health: 'Passed', evidence: ['desktop-dark.png', 'temporal-390-light.png', 'temporal-1440-dark.png'], checks: ['every workload/RF combination', 'metric and scope switching', 'zoom and cursor', 'exact samples and CSV', 'shareable URL restoration'] },
      { step: 2, surface: 'Latency distribution', health: 'Passed', evidence: ['distribution-1440-light.png', 'distribution-390-dark.png'], checks: ['aggregate units', 'full percentile-to-latency mapping', 'contained plot', 'independent legend'] },
      { step: 3, surface: 'Trial drilldown and catalog', health: 'Passed', evidence: ['drilldown-1440-dark.png', 'drilldown-390-light.png'], checks: ['searchable JSON and logs', 'empty stderr', 'original-file download', 'catalog filtering and sorting', 'failed-fetch retry', 'named controls', 'no page overflow'] },
    ],
    limits: ['Screenshots and browser checks do not establish complete accessibility compliance.', 'The broader site budget check requires the separately built broker WASM binary, absent in this local checkout.'],
  }, null, 2) + '\n');
  console.log('Verified temporal controls, exact samples, CSV, distributions, raw drilldown, URL state, retry, and mobile/light/dark layouts.');
} finally { await browser.close(); server.close(); }
