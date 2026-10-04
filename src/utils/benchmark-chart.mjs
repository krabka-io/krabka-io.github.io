export const COLORS = ['#ff8063', '#38bdf8', '#c4a0ff'];
export const DASHES = ['', '8 5', '2 5'];
const metric = (key, label, unit, source = 'workload', scale = 1, digits = 0) => ({ key, label, unit, source, scale, digits });
export const GRAPH_GROUPS = [
  { id: 'delivery', title: 'Delivery rate', note: 'Rates in each recorded interval. The dotted reference is the scheduled offered rate.', metrics: [
    metric('ack_records_per_second', 'Acknowledged', 'records/s'),
    metric('consume_records_per_second', 'Consumed', 'records/s'),
  ] },
  { id: 'latency', title: 'Interval p99 latency', note: 'Each point is an interval quantile. Intervals without latency samples leave a gap.', metrics: [
    metric('latency_ms_p99', 'End to end', 'ms', 'workload', 1, 2),
    metric('ack_latency_ms_p99', 'Acknowledgment', 'ms', 'workload', 1, 2),
  ] },
  { id: 'lag', title: 'Backlog', note: 'Offered backlog also includes scheduled records held back at the producer.', metrics: [
    metric('offered_backlog_records', 'Offered but unconsumed', 'records'),
    metric('consumer_lag_records', 'Acknowledged but unconsumed', 'records'),
  ] },
  { id: 'cpu', title: 'Broker CPU', note: 'Average cores used during the preceding sample interval, summed across brokers.', metrics: [
    metric('cpu_cores', 'CPU cores used', 'cores', 'resource', 1, 2),
  ] },
  { id: 'memory', title: 'Broker memory', note: 'Simultaneous cluster totals. Working set includes active page cache; RSS excludes most disk cache.', metrics: [
    metric('working_set_bytes', 'Working set', 'MiB', 'resource', 1048576, 1),
    metric('rss_bytes', 'RSS', 'MiB', 'resource', 1048576, 1),
    metric('anon_bytes', 'Anonymous memory', 'MiB', 'resource', 1048576, 1),
  ] },
  { id: 'client', title: 'Client resources', note: 'The producer and consumer run together outside broker cgroups.', metrics: [
    metric('client_cpu_cores', 'CPU cores used', 'cores', 'workload', 1, 2),
    metric('client_heap_used_bytes', 'Heap used', 'MiB', 'workload', 1048576, 1),
  ] },
];
export const OVERVIEW_GROUPS = {
  load: [
    { id: 'latency', title: 'Latency versus offered load', metrics: [metric('latency_ms_p99', 'End-to-end p99', 'ms', 'summary', 1, 2), metric('ack_latency_ms_p99', 'Ack p99', 'ms', 'summary', 1, 2)] },
    { id: 'capacity', title: 'Delivery and backlog versus offered load', metrics: [metric('offered_backlog_at_measurement_end', 'End offered backlog', 'records', 'summary'), metric('records_per_second', 'Acknowledged', 'records/s', 'summary')] },
  ],
  memory: [
    { id: 'capacity', title: 'Throughput versus memory budget', metrics: [metric('records_per_second', 'Acknowledged', 'records/s', 'summary')] },
    { id: 'memory', title: 'Memory use versus budget', metrics: [metric('working_set_peak_bytes', 'Peak working set', 'MiB', 'summary', 1048576, 1)] },
  ],
};

export const number = (value, digits = 0) => Number.isFinite(value) ? value.toLocaleString('en-US', {
  minimumFractionDigits: digits, maximumFractionDigits: digits,
}) : 'No samples';
export const tickLabel = value => Math.abs(value) >= 1e6 ? number(value / 1e6, 1) + 'M'
  : Math.abs(value) >= 1e3 ? number(value / 1e3, Math.abs(value) >= 1e4 ? 0 : 1) + 'k'
  : number(value, Math.abs(value) > 0 && Math.abs(value) < 1 ? Math.min(6, Math.ceil(-Math.log10(Math.abs(value))))
    : Math.abs(value) >= 1 && Math.abs(value) < 10 ? 1 : 0);

export const mean = values => values.length && values.every(Number.isFinite)
  ? values.reduce((sum, value) => sum + value, 0) / values.length : null;

export function averageData(captures) {
  if (captures.length !== 3) throw new Error('an average requires three repetitions');
  const averageRecord = records => Object.fromEntries(Object.keys(records[0]).map(key => [key, mean(records.map(r => r[key]))]));
  return { case: captures[0].case, repetition: 'average', trials: captures[0].trials.map(trial => {
    const repetitions = captures.map(capture => capture.trials.find(t => t.id === trial.id));
    return { id: trial.id, name: trial.name, version: trial.version, repetitions,
      metrics: averageRecord(repetitions.map(t => t.metrics)),
      curve: trial.curve ? averageRecord(repetitions.map(t => t.curve)) : undefined,
      events: trial.events.map(event => ({ action: event.action,
        elapsed_ms: mean(repetitions.map(t => t.events.find(e => e.action === event.action).elapsed_ms)) })),
    };
  }) };
}

export function averagePoints(repetitions) {
  // Equal weight per repetition, even when one capture has more samples in a second.
  const buckets = repetitions.map(points => {
    const result = new Map();
    for (const point of points) {
      const second = Math.floor(point.x);
      if (!result.has(second)) result.set(second, []);
      if (Number.isFinite(point.y)) result.get(second).push(point.y);
    }
    return result;
  });
  // Use complete seconds within every capture; never extrapolate a shorter run.
  const start = Math.ceil(Math.max(...repetitions.map(points => points[0].x)));
  const end = Math.floor(Math.min(...repetitions.map(points => points.at(-1).x)));
  return Array.from({ length: Math.max(0, end - start) }, (_, i) => {
    const second = start + i;
    const values = buckets.map(bins => mean(bins.get(second) ?? []));
    return { x: second + 0.5, y: mean(values), contributors: values.filter(Number.isFinite).length };
  });
}

export function timelineSeries(data, metric) {
  if (data.repetition === 'average') {
    const repetitions = [0, 1, 2].map(index => timelineSeries({
      case: data.case, trials: data.trials.map(t => t.repetitions[index]),
    }, metric));
    return repetitions[0].map((line, index) => ({ ...line,
      points: averagePoints(repetitions.map(lines => lines[index].points)) }));
  }
  return data.trials.map((trial, index) => {
    const series = metric.source === 'resource' ? trial.time_series : trial.workload_time_series;
    // Independent monotonic clocks share UTC starts. Align to measurement start, not first sample.
    const offset = data.case.suite === 'curves'
      ? (Date.parse(series.started_at) - Date.parse(trial.workload_time_series.started_at)) / 1000 - data.case.warmup_seconds : 0;
    return { id: trial.id, name: trial.name, color: COLORS[index], dash: DASHES[index],
      points: series.samples.map(sample => ({ x: sample.elapsed_ms / 1000 + offset,
        y: Number.isFinite(metric.source === 'resource' ? sample.cluster[metric.key] : sample[metric.key])
          ? (metric.source === 'resource' ? sample.cluster[metric.key] : sample[metric.key]) / metric.scale : null,
        phase: sample.phase })),
    };
  });
}

export function timelineExtent(data, window = 'full') {
  if (window === 'measurement' && data.case.suite === 'curves') return [0, data.case.seconds];
  const limits = data.trials.flatMap(trial => trial.repetitions ?? [trial]).flatMap(trial => {
    const warmup = data.case.warmup_seconds ?? 0;
    const offset = trial.workload_time_series
      ? (Date.parse(trial.time_series.started_at) - Date.parse(trial.workload_time_series.started_at)) / 1000 - warmup : 0;
    return [trial.time_series.samples[0].elapsed_ms / 1000 + offset,
      trial.time_series.samples.at(-1).elapsed_ms / 1000 + offset,
      ...(trial.workload_time_series ? [trial.workload_time_series.samples.at(-1).elapsed_ms / 1000 - warmup] : [])];
  });
  if (window === 'fault' && data.case.kind === 'recovery') {
    const events = faultEvents(data);
    return [Math.min(...events.map(e => e.x)) - 5, Math.max(...events.map(e => e.x)) + 20];
  }
  return [Math.min(0, ...limits), Math.max(1, data.case.seconds ?? 0, ...limits)];
}

export function faultEvents(data) {
  return data.trials.flatMap((trial, index) => trial.events.map(event => ({ ...event,
    x: event.elapsed_ms / 1000 - data.case.warmup_seconds, color: COLORS[index], name: trial.name,
    average: data.repetition === 'average',
  })));
}

export function overviewSeries(cases, category, metric) {
  return cases[0].summaries.map((vendor, index) => ({ id: vendor.id, name: vendor.name, color: COLORS[index], dash: DASHES[index],
    points: cases.filter(c => c.category === category).map(c => {
      const value = c.summaries[index].metrics[metric.key];
      return { x: category === 'load' ? c.rate : c.memory_gib, key: c.key,
        y: value.median === null ? null : value.median / metric.scale,
        low: value.min === null ? null : value.min / metric.scale,
        high: value.max === null ? null : value.max / metric.scale };
    }),
  }));
}

export function plot(series, extent, logarithmic = false, reference = null) {
  const [minX, maxX] = extent;
  // Include one neighbor on each side for paths that cross a zoom boundary.
  const visible = series.map(line => ({ ...line, points: line.points.filter((p, i, all) =>
    p.x >= minX && p.x <= maxX || p.x < minX && all[i + 1]?.x >= minX || p.x > maxX && all[i - 1]?.x <= maxX) }));
  const values = visible.flatMap(line => line.points.flatMap(p => [p.y, p.low, p.high])).filter(Number.isFinite);
  if (Number.isFinite(reference)) values.push(reference);
  const positive = values.filter(v => v > 0);
  const log = logarithmic && positive.length > 0;
  const highest = Math.max(1, ...values) * 1.08;
  const magnitude = 10 ** Math.floor(Math.log10(highest));
  const maxY = log ? 10 ** Math.ceil(Math.log10(highest))
    : [1, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10].find(step => step * magnitude >= highest) * magnitude;
  const minY = log ? 10 ** Math.floor(Math.log10(Math.min(...positive))) : 0;
  const x = value => 70 + (value - minX) / (maxX - minX) * 540;
  const y = value => 254 - (log
    ? (Math.log10(value) - Math.log10(minY)) / (Math.log10(maxY) - Math.log10(minY)) : value / maxY) * 226;
  const valid = p => Number.isFinite(p.y) && (!log || p.y > 0);
  const ticks = log
    ? Array.from({ length: Math.round(Math.log10(maxY / minY)) + 1 }, (_, i) => minY * 10 ** i)
    : Array.from({ length: 5 }, (_, i) => maxY * i / 4);
  return { minX, maxX, log, x, y,
    xTicks: Array.from({ length: 5 }, (_, i) => minX + (maxX - minX) * i / 4),
    yTicks: ticks.map(value => ({ value, y: y(value) })),
    lines: visible.map(line => {
      let connected = false;
      return { ...line, coordinates: line.points.filter(valid).map(p => ({ ...p, px: x(p.x), py: y(p.y) })),
        path: line.points.map(p => {
          if (!valid(p)) { connected = false; return ''; }
          const command = connected ? 'L' : 'M'; connected = true;
          return command + x(p.x).toFixed(2) + ',' + y(p.y).toFixed(2);
        }).join(' ') };
    }),
  };
}

export function nearestPoint(points, x) {
  if (!points.length || x < points[0].x || x > points.at(-1).x) return null;
  return points.reduce((nearest, point) => !nearest || Math.abs(point.x - x) < Math.abs(nearest.x - x) ? point : nearest, null);
}

const escape = text => String(text).replace(/[&<>"']/g, char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[char]);
export function svgMarkup(graph, { id, label, axis, events = [], duration, overview = false, reference = null }) {
  const node = (tag, attributes, text = '') => '<' + tag + ' ' + Object.entries(attributes)
    .map(([k, v]) => k + '="' + escape(v) + '"').join(' ') + '>' + text + '</' + tag + '>';
  const text = (x, y, content, anchor = 'middle') => node('text', { x, y, 'text-anchor': anchor, fill: '#a6afc1', 'font-size': 12, 'pointer-events': 'none' }, escape(content));
  const path = (d, attrs) => node('path', { d, ...attrs });
  let content = node('title', {}, escape(label)) + '<defs><clipPath id="' + escape(id) + '">' + node('rect', {
    x: overview ? 63 : 70, y: overview ? 21 : 28, width: overview ? 554 : 540, height: overview ? 240 : 226,
  }) + '</clipPath></defs>';
  content += '<g clip-path="url(#' + escape(id) + ')">';
  if (duration !== undefined) {
    if (graph.minX < 0) content += node('rect', { x: 70, y: 28, width: graph.x(0) - 70, height: 226, fill: '#ffffff06' });
    if (graph.maxX > duration) content += node('rect', { x: graph.x(duration), y: 28, width: graph.x(graph.maxX) - graph.x(duration), height: 226, fill: '#ffffff06' });
  }
  for (const event of events.filter(e => e.action === 'pause')) {
    const resume = events.find(e => e.action === 'unpause' && e.name === event.name);
    if (resume) content += node('rect', { x: graph.x(event.x), y: 28, width: graph.x(resume.x) - graph.x(event.x), height: 226, fill: event.color, opacity: 0.04 });
  }
  for (const tick of graph.yTicks) content += path('M70,' + tick.y + 'H610', { stroke: '#ffffff14' });
  if (Number.isFinite(reference) && (!graph.log || reference > 0)) content += path('M70,' + graph.y(reference) + 'H610', { stroke: '#e2e8f0', 'stroke-dasharray': '2 6', 'stroke-opacity': 0.7 });
  for (const event of events) content += node('g', {}, node('title', {}, escape(event.name + ' ' + event.action + ' at ' + number(event.x, 3) + ' s' + (event.average ? ' (mean of 3)' : '')))
    + path('M' + graph.x(event.x) + ',28V254', { stroke: event.color, 'stroke-dasharray': event.action === 'pause' ? '4 5' : '', 'stroke-opacity': 0.5 }));
  if (duration !== undefined) for (const boundary of [0, duration]) content += path('M' + graph.x(boundary) + ',28V254', { stroke: '#ffffff40' });
  for (const line of graph.lines) {
    content += path(line.path, { fill: 'none', stroke: line.color, 'stroke-width': 2.5, 'stroke-dasharray': line.dash, 'data-line': line.id });
    if (overview) for (const point of line.coordinates) {
      if (Number.isFinite(point.low) && Number.isFinite(point.high) && (!graph.log || point.low > 0)) content += path('M' + point.px + ',' + graph.y(point.low) + 'V' + graph.y(point.high), { stroke: line.color, 'stroke-width': 2, opacity: 0.6 });
      const description = line.name + ', ' + number(point.x) + ' ' + axis + ': ' + number(point.y, 2) + '; range ' + number(point.low, 2) + '–' + number(point.high, 2);
      content += node('circle', { cx: point.px, cy: point.py, r: 5, fill: line.color, stroke: '#0f172a', 'stroke-width': 2,
        'data-case': point.key, role: 'button', tabindex: 0, 'aria-label': description + '. View this case over time.' }, node('title', {}, escape(description)));
    }
  }
  content += '<path data-crosshair d="" stroke="#ffffff80" stroke-dasharray="3 5" pointer-events="none"/></g>';
  for (const tick of graph.yTicks) content += text(58, tick.y + 4, tickLabel(tick.value), 'end');
  for (const value of graph.xTicks) content += text(graph.x(value), 292, tickLabel(value));
  content += text(340, 330, axis);
  if (events.length) for (const action of ['pause', 'unpause']) {
    const selected = events.filter(e => e.action === action);
    const x = selected.reduce((sum, e) => sum + e.x, 0) / selected.length;
    if (x >= graph.minX && x <= graph.maxX) content += text(graph.x(x), 17, action === 'pause' ? 'pause' : 'resume');
  }
  return content;
}
