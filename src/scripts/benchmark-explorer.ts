import { vendors, names, colors, resourceMetrics, metricInfo, intervalMetrics, parseResources, temporalSeries, envelope, nearest, quantiles, csv } from '../utils/benchmark-explorer.mjs';

type Trial = { id: string; vendor: string; rf: number; repetition: number; case: { id: string; config: Record<string, number> }; files: any[]; [key: string]: any };
type Capture = { result: Record<string, any>; resources: any[] };
const $ = <T extends HTMLElement = HTMLElement>(id: string) => document.getElementById(id) as T;
const select = (id: string) => $<HTMLSelectElement>(id);
const input = (id: string) => $<HTMLInputElement>(id);
const index = JSON.parse($('archive-index').textContent!);
let run = index.runs[0];
let selected: Trial[] = [];
let captures = new Map<string, Capture>();
const fileCache = new Map<string, string>();
let loadVersion = 0, rawVersion = 0, ready = false;
let series: { trial: Trial; points: { x: number; y: number | null; index: number }[] }[] = [];
let domain = { start: 0, end: 1, max: 1 };
let sampleRows: any[][] = [], samplePage = 0;
let rawLines: { line: number; text: string }[] = [], rawPage = 0;
let currentRaw: { text: string; name: string; object?: any } | null = null;
const pageSize = 50;
const fmt = (n: number | null | undefined, digits = 2) => n == null || !Number.isFinite(n) ? '—' : n.toLocaleString('en-US', { maximumFractionDigits: digits });
const small = (n: number) => n >= 1e6 ? fmt(n / 1e6, 1) + 'M' : n >= 1e3 ? fmt(n / 1e3, 1) + 'k' : fmt(n, n < 1 ? 3 : 1);
const duration = (seconds: number) => `${Math.floor(seconds / 60)}:${Math.floor(seconds % 60).toString().padStart(2, '0')}`;
function node(tag: string, text = '', className = '') { const el = document.createElement(tag); el.textContent = text; el.className = className; return el; }
function options(id: string, values: [string | number, string][], desired?: string) {
  const el = select(id); const old = desired ?? el.value;
  el.replaceChildren(...values.map(([value, title]) => { const option = new Option(title, String(value)); return option; }));
  if (values.some(([value]) => String(value) === old)) el.value = old;
}
function visibleVendors() { return [...document.querySelectorAll<HTMLInputElement>('input[name="vendor"]:checked')].map(el => el.value); }
function selectionTrials() { return run.trials.filter((trial: Trial) => trial.case.id === select('workload-select').value && String(trial.rf) === select('rf-select').value && String(trial.repetition) === select('round-select').value); }
function status(message: string, error = false) { $('explorer-status').textContent = message; $('explorer-status').toggleAttribute('data-error', error); $('retry-load').hidden = !error; }
function fileURL(trial: Trial | null, name: string) {
  const file = (trial ? trial.files : run.files).find((file: any) => file.name === name);
  if (!file) throw new Error(`No captured ${name} file.`);
  return `/benchmarks/archive-data/${encodeURIComponent(run.id)}/${file.file.split('/').map(encodeURIComponent).join('/')}`;
}
async function readFile(trial: Trial | null, name: string) {
  const url = fileURL(trial, name);
  if (fileCache.has(url)) return fileCache.get(url)!;
  const metadata = (trial ? trial.files : run.files).find((f: any) => f.name === name);
  const response = await fetch(url);
  if (!response.ok) throw new Error(`Could not load ${name} (HTTP ${response.status}).`);
  if (typeof DecompressionStream === 'undefined') throw new Error('This browser needs support for gzip DecompressionStream to inspect the archive. Use the source archive link below.');
  const compressed = await response.arrayBuffer();
  const bytes = await new Response(new Blob([compressed]).stream().pipeThrough(new DecompressionStream('gzip'))).arrayBuffer();
  if (bytes.byteLength !== metadata.bytes) throw new Error(`Incomplete ${name} file.`);
  if (crypto.subtle) {
    const hash = [...new Uint8Array(await crypto.subtle.digest('SHA-256', bytes))].map(b => b.toString(16).padStart(2, '0')).join('');
    if (hash !== metadata.sha256) throw new Error(`Integrity check failed for ${name}.`);
  }
  const text = new TextDecoder().decode(bytes);
  // Bound memory while moving through the catalog; raw data stays available on demand.
  if (fileCache.size >= 12) fileCache.delete(fileCache.keys().next().value!);
  fileCache.set(url, text); return text;
}
function updateWorkloads(desired?: string) {
  const search = input('workload-search').value.trim().toLowerCase();
  const workloads = run.provenance.cases.filter((workload: any) => workload.id.toLowerCase().includes(search));
  options('workload-select', workloads.map((workload: any) => [workload.id, workload.id]), desired);
}
function updateRun(desiredWorkload?: string, desiredRf?: string, desiredRound?: string) {
  run = index.runs.find((r: any) => r.id === select('run-select').value);
  captures.clear(); ready = false;
  updateWorkloads(desiredWorkload);
  options('rf-select', run.provenance.replication_factors.map((rf: number) => [rf, `RF${rf}`]), desiredRf);
  options('round-select', Array.from({ length: run.provenance.repetitions }, (_, i) => [i + 1, `Round ${i + 1}`] as [number, string]), desiredRound);
  renderCatalog(); renderProvenance(); void loadSelection();
}
function updateURL() {
  const url = new URL(location.href);
  for (const [key, value] of Object.entries({ run: run.id, workload: select('workload-select').value, rf: select('rf-select').value, round: select('round-select').value, vendors: visibleVendors().join(','), source: select('source-select').value, metric: select('metric-select').value, scope: select('scope-select').value, scale: select('scale-select').value, start: input('window-start').value, end: input('window-end').value })) url.searchParams.set(key, value);
  history.replaceState(null, '', url);
}
function resetRaw() {
  rawVersion++; currentRaw = null; rawLines = []; rawPage = 0;
  $('raw-content').replaceChildren(node('p', 'Choose a captured file and select Inspect file to browse its fields or lines.'));
  $('raw-status').textContent = ''; $('raw-pagination').hidden = true;
}
async function loadSelection() {
  const version = ++loadVersion;
  ready = false; captures.clear(); selected = selectionTrials();
  series = []; sampleRows = []; samplePage = 0;
  $<HTMLButtonElement>('export-samples').disabled = true;
  $('chart-legend').replaceChildren(); renderSampleTable();
  resetRaw(); $('summary-cards').replaceChildren();
  $('temporal-chart').querySelector('g')?.remove(); $('latency-table').replaceChildren(); $('distribution-chart').replaceChildren();
  $('cursor-values').replaceChildren(); $('sample-table').replaceChildren();
  $('selection-title').textContent = select('workload-select').value || 'No matching workload';
  $('selection-eyebrow').textContent = `${run.id} / RF${select('rf-select').value} / Round ${select('round-select').value}`;
  options('detail-trial', selected.map(trial => [trial.id, `${names[trial.vendor]} · RF${trial.rf} · round ${trial.repetition}`]));
  renderConfig(); renderSummary(); renderTrial();
  if (!selected.length) { status('No trials match this workload filter.'); return; }
  status(`Loading ${selected.length} trials. Each file is decompressed and verified in your browser…`);
  document.querySelector('.explorer-main')?.setAttribute('aria-busy', 'true');
  try {
    const loaded = await Promise.all(selected.map(async trial => {
      const [result, resources] = await Promise.all([readFile(trial, 'result.json'), readFile(trial, 'resources.jsonl')]);
      return [trial.id, { result: JSON.parse(result), resources: parseResources(resources) }] as [string, Capture];
    }));
    if (version !== loadVersion) return;
    captures = new Map(loaded); ready = true;
    options('scope-select', [['cluster', 'Cluster total'], ...Array.from({ length: selected[0].rf }, (_, i) => [String(i), `Broker ${i}`] as [string, string])]);
    configureMetrics(); renderAll();
    status(`Loaded ${selected.length} trials · ${selected.reduce((n, t) => n + t.interval_samples, 0).toLocaleString()} OMB intervals · ${selected.reduce((n, t) => n + t.resource_samples, 0).toLocaleString()} resource samples. Original files verified.`);
  } catch (error) { if (version === loadVersion) status((error as Error).message, true); }
  finally { if (version === loadVersion) document.querySelector('.explorer-main')?.setAttribute('aria-busy', 'false'); }
}
function renderConfig() {
  const el = $('workload-config'); el.replaceChildren();
  if (!selected.length) return;
  const config = selected[0].case.config;
  const list = node('dl');
  for (const [label, value] of [['Topics', config.topics], ['Partitions / topic', config.partitionsPerTopic], ['Message size', fmt(config.messageSize, 0) + ' B'], ['Offered rate', config.producerRate > 0 ? fmt(config.producerRate, 0) + ' msg/s' : 'Maximum (probed)'], ['Measurement', config.testDurationMinutes + ' min'], ['Backlog', config.consumerBacklogSizeGB + ' GB']]) {
    const row = node('div'); row.append(node('dt', String(label)), node('dd', String(value))); list.append(row);
  }
  el.append(list);
  if (config.consumerBacklogSizeGB > 0) el.append(node('p', 'End-to-end latency includes the wait while the backlog fills before consumers drain it.', 'context-note'));
}
function renderSummary() {
  $('summary-cards').style.setProperty('--summary-columns', String(Math.max(1, selected.filter(t => visibleVendors().includes(t.vendor)).length)));
  $('summary-cards').replaceChildren(...selected.filter(t => visibleVendors().includes(t.vendor)).map(trial => {
    const card = node('article', '', 'summary-card'); card.style.setProperty('--dot', colors[trial.vendor]);
    card.append(node('h3', names[trial.vendor]));
    const rate = node('div'); rate.append(node('strong', fmt(trial.publish_rate, 0)), node('p', 'mean published messages / s')); card.append(rate);
    const dl = node('dl');
    for (const [label, value] of [['Consume / s', fmt(trial.consume_rate, 0)], ['Publish p99', fmt(trial.publish_latency_ms.p99) + ' ms'], ['E2E p99', fmt(trial.end_to_end_latency_ms.p99) + ' ms'], ['Peak RSS', fmt(trial.rss_peak_bytes / 1024 ** 2, 1) + ' MiB'], ['CPU · full capture', fmt(trial.cpu_seconds, 1) + ' s']]) dl.append(node('dt', label), node('dd', value));
    card.append(dl); return card;
  }));
}
function configureMetrics(desired?: string) {
  const source = select('source-select').value;
  const first = captures.values().next().value;
  const metrics = source === 'omb' && first ? intervalMetrics(first.result) : resourceMetrics.map(([key]) => metricInfo(key));
  options('metric-select', metrics.map((m: any) => [m.key, `${m.title} · ${m.unit}`]), desired);
  $('scope-control').hidden = source !== 'resources';
}
function renderAll() { renderSummary(); if (!ready) return; renderTemporal(); renderLatency(); updateURL(); }
function svgNode(tag: string, attrs: Record<string, string | number> = {}, text = '') { const el = document.createElementNS('http://www.w3.org/2000/svg', tag); for (const [key, value] of Object.entries(attrs)) el.setAttribute(key, String(value)); if (text) el.textContent = text; return el; }
function drawChart(id: string, lines: any[], config: { start: number; end: number; max: number; unit: string; log?: boolean; distribution?: boolean; xUnit?: string }) {
  const svg = $(id); svg.querySelector('g')?.remove();
  const compact = svg.clientWidth < 500;
  const width = compact ? 500 : 1000;
  const height = config.distribution ? 300 : 330, left = compact ? 60 : 75, right = width - 25, top = 20, bottom = height - 50;
  svg.setAttribute('viewBox', `0 0 ${width} ${height}`);
  svg.dataset.plotLeft = String(left); svg.dataset.plotWidth = String(right - left);
  const group = svgNode('g');
  const transform = (n: number) => config.log ? Math.log1p(Math.max(0, n)) : n;
  const maximum = transform(config.max) || 1;
  const xTransform = (n: number) => config.distribution ? Math.log1p(Math.max(0, n)) : n;
  const xMin = xTransform(config.start), xMax = xTransform(config.end);
  const x = (n: number) => left + (xTransform(n) - xMin) / (xMax - xMin || 1) * (right - left);
  const y = (n: number) => bottom - transform(n) / maximum * (bottom - top);
  for (let tick = 0; tick <= 4; tick++) {
    const fraction = tick / 4, py = bottom - fraction * (bottom - top);
    const value = config.log ? Math.expm1(maximum * fraction) : config.max * fraction;
    group.append(svgNode('line', { x1: left, x2: right, y1: py, y2: py, class: 'chart-grid' }), svgNode('text', { x: left - 10, y: py + 4, 'text-anchor': 'end' }, small(value)));
    const xv = config.distribution ? Math.expm1(xMin + fraction * (xMax - xMin)) : config.start + fraction * (config.end - config.start);
    group.append(svgNode('text', { x: left + fraction * (right - left), y: bottom + 24, 'text-anchor': 'middle' }, config.distribution ? small(xv) : duration(xv)));
  }
  group.append(svgNode('text', { x: left, y: 12 }, config.unit), svgNode('text', { x: right, y: height - 3, 'text-anchor': 'end' }, config.distribution ? `Latency · ${config.xUnit}` : 'Elapsed time · min:sec'));
  const clipId = `${id}-clip`;
  const clip = svgNode('clipPath', { id: clipId }); clip.append(svgNode('rect', { x: left, y: top, width: right - left, height: bottom - top })); group.append(clip);
  const plot = svgNode('g', { 'clip-path': `url(#${clipId})` }); group.append(plot);
  for (const line of lines) {
    const visible = line.points.filter((p: any) => p.x >= config.start && p.x <= config.end);
    const points = envelope(visible);
    let path = '', pen = false;
    for (const p of points) {
      if (!Number.isFinite(p.y)) { pen = false; continue; }
      path += `${pen ? 'L' : 'M'}${x(p.x).toFixed(2)},${y(p.y).toFixed(2)}`; pen = true;
    }
    plot.append(svgNode('path', { d: path, stroke: colors[line.trial.vendor], class: 'chart-line', 'data-vendor': line.trial.vendor }));
    if (visible.length === 1 && Number.isFinite(visible[0].y)) plot.append(svgNode('circle', { cx: x(visible[0].x), cy: y(visible[0].y), r: 3, fill: colors[line.trial.vendor] }));
  }
  if (!config.distribution) group.append(svgNode('line', { id: 'chart-cursor-line', x1: left, x2: left, y1: top, y2: bottom, class: 'chart-cursor' }));
  svg.append(group);
}
function renderTemporal() {
  const source = select('source-select').value, metric = select('metric-select').value, scope = select('scope-select').value;
  const info = metricInfo(metric);
  series = selected.filter(t => visibleVendors().includes(t.vendor)).map(trial => ({ trial, points: temporalSeries(trial, captures.get(trial.id), source, metric, scope) }));
  const end = Math.max(1, ...series.map(s => s.points.at(-1)?.x ?? 0));
  domain.start = end * Number(input('window-start').value) / 100; domain.end = end * Number(input('window-end').value) / 100;
  let max = 0;
  for (const s of series) for (const point of s.points) if (point.x >= domain.start && point.x <= domain.end && Number.isFinite(point.y)) max = Math.max(max, point.y!);
  domain.max = max || 1;
  const note = source === 'omb'
    ? 'OMB measurement only. Interval 1 is plotted at 10 s; times indicate nominal interval ends, not recorded wall-clock timestamps. Publish delay is in microseconds; other latency is in milliseconds.'
    : 'Full resource capture: startup, warm-up, rate probing, measurement, and shutdown. Times use the recorded elapsed_ms. CPU is counter change / actual sample gap; the first baseline has no CPU-rate value. Memory is in MiB.';
  $('timeline-note').textContent = note;
  $('chart-title').textContent = `${info.title} over time, ${select('workload-select').value}`;
  $('chart-description').textContent = note + ' Use the time cursor or exact sample table to inspect values.';
  $('chart-legend').replaceChildren(...series.map(s => { const label = node('span'); const dot = node('i', '', `vendor-dot ${s.trial.vendor}`); label.append(dot, document.createTextNode(names[s.trial.vendor])); return label; }));
  const count = series.reduce((n, s) => n + s.points.filter(p => p.x >= domain.start && p.x <= domain.end).length, 0);
  $('sample-badge').textContent = `${count.toLocaleString()} samples in window`;
  $('chart-caption').textContent = !series.length ? 'Select at least one broker to display a comparison.' : `${info.title} · ${info.unit}. Drawing keeps per-bucket minima and maxima; exact samples remain in the table and export.`;
  drawChart('temporal-chart', series, { ...domain, unit: info.unit, log: select('scale-select').value === 'log' });
  $('window-start-label').textContent = `${duration(domain.start)} (${input('window-start').value}%)`;
  $('window-end-label').textContent = `${duration(domain.end)} (${input('window-end').value}%)`;
  sampleRows = series.flatMap(s => s.points.filter(p => p.x >= domain.start && p.x <= domain.end).map(p => [names[s.trial.vendor], p.index + 1, p.x, p.y, info.unit, source === 'resources' ? new Date(Date.parse(s.trial.started_at) + p.x * 1000).toISOString() : '']));
  samplePage = 0; renderSampleTable(); renderCursor();
}
function renderCursor() {
  const x = domain.start + (domain.end - domain.start) * Number(input('time-cursor').value) / 1000;
  $('cursor-time').textContent = `${fmt(x, 3)} s · ${duration(x)}`;
  const svg = $('temporal-chart');
  const px = Number(svg.dataset.plotLeft ?? 75) + (x - domain.start) / (domain.end - domain.start || 1) * Number(svg.dataset.plotWidth ?? 900);
  $('chart-cursor-line')?.setAttribute('x1', String(px)); $('chart-cursor-line')?.setAttribute('x2', String(px));
  const info = metricInfo(select('metric-select').value);
  $('cursor-values').replaceChildren(...series.map(s => { const p = nearest(s.points, x); const text = node('span'); text.append(node('i', '', `vendor-dot ${s.trial.vendor}`), document.createTextNode(` ${names[s.trial.vendor]}: ${fmt(p?.y, 4)} ${info.unit} @ ${fmt(p?.x, 3)} s`)); return text; }));
}
function table(id: string, headers: string[], rows: any[][], caption?: string) {
  const el = $(id); el.replaceChildren();
  if (caption) { const c = node('caption', caption); c.className = 'sr-only'; el.append(c); }
  const head = node('thead'), tr = node('tr'); headers.forEach(h => { const th = node('th', h); th.setAttribute('scope', 'col'); tr.append(th); }); head.append(tr);
  const body = node('tbody'); rows.forEach(row => { const tr = node('tr'); row.forEach((value, i) => { const cell = node(i === 0 ? 'th' : 'td'); if (i === 0) cell.setAttribute('scope', 'row'); if (value instanceof HTMLElement) cell.append(value); else cell.textContent = String(value ?? '—'); tr.append(cell); }); body.append(tr); }); el.append(head, body);
}
function renderSampleTable() {
  $<HTMLButtonElement>('export-samples').disabled = !ready || !sampleRows.length;
  const pages = Math.max(1, Math.ceil(sampleRows.length / pageSize)); samplePage = Math.min(samplePage, pages - 1);
  table('sample-table', ['Broker', 'Sample', 'Elapsed s', 'Value', 'Unit', 'Capture time (UTC)'], sampleRows.slice(samplePage * pageSize, (samplePage + 1) * pageSize).map(row => [row[0], row[1], String(row[2]), row[3] === null ? '—' : String(row[3]), row[4], row[5] || 'Not recorded']), 'Exact captured samples for the selected temporal metric');
  $('sample-count').textContent = `${sampleRows.length.toLocaleString()} original samples in the visible window`;
  $('sample-page').textContent = `Page ${samplePage + 1} / ${pages}`;
  $<HTMLButtonElement>('sample-prev').disabled = samplePage === 0;
  $<HTMLButtonElement>('sample-next').disabled = samplePage >= pages - 1;
}
function renderLatency() {
  if (!ready) return;
  const family = select('latency-family').value, prefix = 'aggregated' + family, unit = family === 'PublishDelayLatency' ? 'µs' : 'ms';
  const metrics = [['Avg', 'Mean'], ['50pct', 'p50'], ['75pct', 'p75'], ['95pct', 'p95'], ['99pct', 'p99'], ['999pct', 'p99.9'], ['9999pct', 'p99.99'], ['Max', 'Max']];
  const trials = selected.filter(t => visibleVendors().includes(t.vendor));
  table('latency-table', ['Broker', ...metrics.map(([, name]) => `${name} (${unit})`)], trials.map(trial => [names[trial.vendor], ...metrics.map(([key]) => fmt(captures.get(trial.id)?.result[prefix + key], 4))]), 'Aggregate latency quantiles');
  if (!$<HTMLDetailsElement>('distribution-details').open) return;
  $('distribution-legend').replaceChildren(...trials.map(trial => { const label = node('span'); label.append(node('i', '', `vendor-dot ${trial.vendor}`), document.createTextNode(names[trial.vendor])); return label; }));
  const lines = trials.map(trial => ({ trial, points: quantiles(captures.get(trial.id)?.result, family) }));
  let max = 0; for (const line of lines) for (const p of line.points) max = Math.max(max, p.x);
  drawChart('distribution-chart', lines, { start: 0, end: max || 1, max: 100, unit: 'Percentile · %', distribution: true, xUnit: unit });
}
function renderTrial() {
  resetRaw();
  const trial = selected.find(t => t.id === select('detail-trial').value);
  const el = $('trial-details'); el.replaceChildren(); if (!trial) return;
  el.append(node('p', `${trial.id}`));
  el.append(node('p', `Capture began ${trial.started_at} · ${fmt(trial.resource_duration_seconds, 2)} s · ${fmt(trial.resource_samples, 0)} resource samples · ${trial.interval_samples} OMB intervals.`));
  el.append(node('p', `Peak cluster RSS ${fmt(trial.rss_peak_bytes / 1024 ** 2)} MiB · peak working set ${fmt(trial.working_set_peak_bytes / 1024 ** 2)} MiB · maximum backlog ${fmt(trial.backlog_max, 0)} messages · retriable commit failures ${trial.retriable_commit_failures}.`));
  el.append(node('p', `Image: ${run.provenance.images[trial.vendor].reference}`));
}
function renderCatalog() {
  const search = input('catalog-search').value.toLowerCase().trim();
  const trials = run.trials.filter((trial: Trial) => `${trial.id} ${names[trial.vendor]} RF${trial.rf}`.toLowerCase().includes(search));
  const sort = select('catalog-sort').value;
  const keys: Record<string, string> = { rate: 'publish_rate', cpu: 'cpu_seconds', memory: 'rss_peak_bytes' };
  if (sort !== 'capture') trials.sort((a: Trial, b: Trial) => sort === 'latency' ? a.end_to_end_latency_ms.p99 - b.end_to_end_latency_ms.p99 : (a[keys[sort]] - b[keys[sort]]) * (sort === 'rate' ? -1 : 1));
  $('catalog-count').textContent = `${trials.length} / ${run.trials.length} trials. Resource totals and peaks cover the full capture.`;
  table('catalog-table', ['Workload / open trial', 'Broker', 'RF', 'Round', 'Publish / s', 'Consume / s', 'Pub p99 ms', 'E2E p99 ms', 'CPU s', 'RSS MiB', 'Working set MiB', 'OMB intervals', 'Resource samples'], trials.map((trial: Trial) => {
    const button = node('button', trial.case.id); button.setAttribute('type', 'button');
    button.addEventListener('click', async () => {
      input('workload-search').value = ''; updateWorkloads(trial.case.id);
      select('rf-select').value = String(trial.rf); select('round-select').value = String(trial.repetition);
      await loadSelection(); select('detail-trial').value = trial.id; renderTrial();
      $('selection-title').scrollIntoView({ behavior: 'smooth', block: 'center' });
    });
    return [button, names[trial.vendor], trial.rf, trial.repetition, fmt(trial.publish_rate, 0), fmt(trial.consume_rate, 0), fmt(trial.publish_latency_ms.p99), fmt(trial.end_to_end_latency_ms.p99), fmt(trial.cpu_seconds), fmt(trial.rss_peak_bytes / 1024 ** 2), fmt(trial.working_set_peak_bytes / 1024 ** 2), trial.interval_samples, trial.resource_samples];
  }), 'All captured trials and aggregate measurements');
}
function renderProvenance() {
  const el = $('archive-provenance'); el.replaceChildren();
  for (const r of index.runs) {
    const p = r.provenance;
    el.append(node('h3', r.id), node('p', `${p.status} · ${p.started_at} → ${p.completed_at} · ${p.host.cpu_model} · ${p.host.logical_cpus} logical CPUs`), node('p', `${r.filename} · SHA-256: ${r.checksum}`));
    for (const vendor of vendors) el.append(node('p', `${names[vendor]}: ${p.images[vendor].reference}`));
  }
  el.append(node('p', 'Inspect provenance.json in Trial drilldown for all image labels, shard sources, CPU sets, OMB commits, driver settings, patch hashes, and jar hashes.'));
}
function jsonTree(value: any, key = 'root', search = ''): HTMLElement {
  if (value === null || typeof value !== 'object') {
    const line = node('div', '', 'json-value'); line.append(node('span', key + ': ', 'json-key'), document.createTextNode(JSON.stringify(value))); return line;
  }
  const entries = Object.entries(value);
  const details = node('details') as HTMLDetailsElement;
  const summary = node('summary', `${key} · ${Array.isArray(value) ? entries.length + ' items' : entries.length + ' fields'}`);
  details.append(summary);
  let populated = false;
  const populate = () => {
    if (populated) return; populated = true;
    const matching = search ? entries.filter(([k, v]) => (k + ' ' + JSON.stringify(v)).toLowerCase().includes(search)) : entries;
    let offset = 0;
    const more = node('button', '', 'json-page') as HTMLButtonElement; more.type = 'button';
    const add = () => { for (const [k, v] of matching.slice(offset, offset + 50)) details.insertBefore(jsonTree(v, k, search), more); offset += 50; more.textContent = `Show next ${Math.min(50, matching.length - offset)} of ${matching.length} entries`; more.hidden = offset >= matching.length; };
    more.addEventListener('click', add); details.append(more); add();
  };
  details.addEventListener('toggle', () => { if (details.open) populate(); });
  if (key === 'root' || search) { populate(); details.open = true; }
  return details;
}
async function inspectRaw() {
  const version = ++rawVersion, trial = selected.find(t => t.id === select('detail-trial').value) ?? null, name = select('raw-file').value;
  const global = ['summary.md', 'provenance.json'].includes(name);
  currentRaw = null; $('raw-content').replaceChildren(); $('raw-pagination').hidden = true;
  $('raw-status').textContent = `Loading ${name}…`;
  try {
    const text = await readFile(global ? null : trial, name);
    if (version !== rawVersion) return;
    currentRaw = { text, name, object: name.endsWith('.json') ? JSON.parse(text) : undefined };
    renderRaw();
  } catch (error) { if (version === rawVersion) $('raw-status').textContent = (error as Error).message; }
}
function renderRaw() {
  if (!currentRaw) return;
  const search = input('raw-search').value.trim().toLowerCase();
  const el = $('raw-content'); el.replaceChildren();
  if (currentRaw.object !== undefined) {
    el.append(jsonTree(currentRaw.object, 'root', search)); $('raw-pagination').hidden = true;
    $('raw-status').textContent = `${currentRaw.name} · original JSON fields; expand arrays and objects to drill down.`;
  } else {
    rawLines = currentRaw.text.split(/\r?\n/).map((text, i) => ({ text, line: i + 1 })).filter(row => (!search || row.text.toLowerCase().includes(search)) && (row.text !== '' || currentRaw!.text !== ''));
    rawPage = 0; renderRawLines();
  }
}
function renderRawLines() {
  if (!currentRaw) return;
  const pages = Math.max(1, Math.ceil(rawLines.length / pageSize)); rawPage = Math.min(rawPage, pages - 1);
  $('raw-content').replaceChildren(node('pre', rawLines.length ? rawLines.slice(rawPage * pageSize, (rawPage + 1) * pageSize).map(row => `${row.line.toString().padStart(6)}  ${row.text}`).join('\n') : currentRaw.text === '' ? 'This captured file is empty.' : 'No matching lines.'));
  $('raw-status').textContent = `${currentRaw.name} · ${rawLines.length.toLocaleString()} matching lines · original line numbers`;
  $('raw-pagination').hidden = rawLines.length <= pageSize;
  $('raw-page').textContent = `Page ${rawPage + 1} / ${pages}`;
  $<HTMLButtonElement>('raw-prev').disabled = rawPage === 0; $<HTMLButtonElement>('raw-next').disabled = rawPage >= pages - 1;
}
function download(text: string, name: string, type = 'text/plain') { const url = URL.createObjectURL(new Blob([text], { type })); const anchor = node('a') as HTMLAnchorElement; anchor.href = url; anchor.download = name; anchor.click(); setTimeout(() => URL.revokeObjectURL(url), 1000); }

select('run-select').addEventListener('change', () => updateRun());
input('workload-search').addEventListener('input', () => { updateWorkloads(); void loadSelection(); });
for (const id of ['workload-select', 'rf-select', 'round-select']) select(id).addEventListener('change', () => { input('window-start').value = '0'; input('window-end').value = '100'; void loadSelection(); });
for (const checkbox of document.querySelectorAll<HTMLInputElement>('input[name="vendor"]')) checkbox.addEventListener('change', renderAll);
select('source-select').addEventListener('change', () => { configureMetrics(); input('window-start').value = '0'; input('window-end').value = '100'; renderAll(); });
for (const id of ['metric-select', 'scope-select', 'scale-select']) select(id).addEventListener('change', renderAll);
for (const id of ['window-start', 'window-end']) input(id).addEventListener('input', () => {
  if (Number(input('window-start').value) >= Number(input('window-end').value)) input(id === 'window-start' ? 'window-end' : 'window-start').value = String(Number(input(id).value) + (id === 'window-start' ? 1 : -1));
  renderAll();
});
$('reset-window').addEventListener('click', () => { input('window-start').value = '0'; input('window-end').value = '100'; renderAll(); });
input('time-cursor').addEventListener('input', renderCursor);
$('temporal-chart').addEventListener('pointermove', event => {
  const rect = $('temporal-chart').getBoundingClientRect(); const x = (event as PointerEvent).clientX - rect.left;
  const svg = $<HTMLElement>('temporal-chart');
  const width = Number(svg.getAttribute('viewBox')!.split(' ')[2]);
  input('time-cursor').value = String(Math.round(Math.max(0, Math.min(1, (x / rect.width * width - Number(svg.dataset.plotLeft)) / Number(svg.dataset.plotWidth))) * 1000)); renderCursor();
});
$('sample-prev').addEventListener('click', () => { samplePage--; renderSampleTable(); });
$('sample-next').addEventListener('click', () => { samplePage++; renderSampleTable(); });
$('export-samples').addEventListener('click', () => download(csv([['broker', 'sample', 'elapsed_seconds', 'value', 'unit', 'capture_time_utc'], ...sampleRows]), `${run.id}-${select('workload-select').value}-${select('metric-select').value}.csv`, 'text/csv'));
select('latency-family').addEventListener('change', renderLatency);
$('distribution-details').addEventListener('toggle', renderLatency);
select('detail-trial').addEventListener('change', renderTrial);
select('raw-file').addEventListener('change', resetRaw);
$('open-file').addEventListener('click', () => void inspectRaw());
input('raw-search').addEventListener('input', renderRaw);
$('raw-prev').addEventListener('click', () => { rawPage--; renderRawLines(); });
$('raw-next').addEventListener('click', () => { rawPage++; renderRawLines(); });
$('download-file').addEventListener('click', async () => {
  const trial = selected.find(t => t.id === select('detail-trial').value) ?? null, name = select('raw-file').value;
  try { const text = await readFile(['summary.md', 'provenance.json'].includes(name) ? null : trial, name); download(text, name, name.endsWith('.json') ? 'application/json' : 'text/plain'); }
  catch (error) { $('raw-status').textContent = (error as Error).message; }
});
input('catalog-search').addEventListener('input', renderCatalog);
select('catalog-sort').addEventListener('change', renderCatalog);
$('retry-load').addEventListener('click', () => void loadSelection());
$('share-selection').addEventListener('click', async () => { updateURL(); try { await navigator.clipboard.writeText(location.href); status('View link copied.'); } catch { status('The address bar contains this view’s shareable link.'); } });

let resized: ReturnType<typeof setTimeout>;
new ResizeObserver(() => { clearTimeout(resized); resized = setTimeout(() => { if (ready) { renderTemporal(); renderLatency(); } }, 100); }).observe($('temporal-chart'));

const params = new URLSearchParams(location.search);
if (index.runs.some((r: any) => r.id === params.get('run'))) select('run-select').value = params.get('run')!;
for (const id of ['source', 'scale']) if ([...select(id + '-select').options].some(o => o.value === params.get(id))) select(id + '-select').value = params.get(id)!;
if (params.has('vendors')) for (const checkbox of document.querySelectorAll<HTMLInputElement>('input[name="vendor"]')) checkbox.checked = params.get('vendors')!.split(',').includes(checkbox.value);
const start = Number(params.get('start') ?? 0), end = Number(params.get('end') ?? 100);
if (Number.isFinite(start) && Number.isFinite(end) && start >= 0 && end <= 100 && start < end) { input('window-start').value = String(start); input('window-end').value = String(end); }
run = index.runs.find((r: any) => r.id === select('run-select').value);
updateWorkloads(params.get('workload') ?? 'max-rate-1-topic-16-partitions-1kb');
options('rf-select', run.provenance.replication_factors.map((rf: number) => [rf, `RF${rf}`]), params.get('rf') ?? undefined);
options('round-select', Array.from({ length: run.provenance.repetitions }, (_, i) => [i + 1, `Round ${i + 1}`] as [number, string]), params.get('round') ?? undefined);
renderCatalog(); renderProvenance();
await loadSelection();
if (ready) { configureMetrics(params.get('metric') ?? undefined); if ([...select('scope-select').options].some(o => o.value === params.get('scope'))) select('scope-select').value = params.get('scope')!; renderAll(); }
