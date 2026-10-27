export const vendors = ['krabka', 'kafka', 'redpanda'];
export const names = { krabka: 'krabka', kafka: 'Apache Kafka', redpanda: 'Redpanda' };
export const colors = {
  krabka: 'var(--color-krab-400)',
  kafka: 'var(--color-sky-400)',
  redpanda: 'var(--color-violet-400)',
};
export const resourceMetrics = [
  ['cpu_cores', 'CPU cores', 'cores'],
  ['cpu_seconds', 'Cumulative CPU', 'CPU s'],
  ['cpu_user_cores', 'User CPU', 'cores'],
  ['cpu_system_cores', 'System CPU', 'cores'],
  ['rss_bytes', 'Resident memory (RSS)', 'MiB'],
  ['working_set_bytes', 'Working set', 'MiB'],
  ['anon_bytes', 'Anonymous memory', 'MiB'],
  ['memory_current_bytes', 'Cgroup memory', 'MiB'],
  ['inactive_file_bytes', 'Inactive file cache', 'MiB'],
  ['oom_kill', 'OOM kills', 'kills'],
];
export function metricInfo(key) {
  const resource = resourceMetrics.find(([id]) => id === key);
  if (resource) return { key, title: resource[1], unit: resource[2] };
  const title = key
    .replace(/9999pct$/, ' p99.99')
    .replace(/999pct$/, ' p99.9')
    .replace(/(\d+)pct$/, ' p$1')
    .replace(/([a-z])([A-Z])/g, '$1 $2');
  return {
    key,
    title: title.charAt(0).toUpperCase() + title.slice(1),
    unit: key.includes('DelayLatency')
      ? 'µs'
      : key.includes('Latency')
        ? 'ms'
        : key === 'backlog'
          ? 'messages'
          : 'messages/s',
  };
}
export function intervalMetrics(result) {
  return Object.entries(result)
    .filter(
      ([, values]) => Array.isArray(values) && values.length > 0 && values.every(Number.isFinite),
    )
    .map(([key]) => metricInfo(key));
}
export function parseResources(text) {
  const samples = text
    .trim()
    .split(/\r?\n/)
    .filter(Boolean)
    .map((line) => JSON.parse(line));
  return samples.map((sample, i) => {
    const previous = samples[i - 1];
    const seconds = previous ? (sample.elapsed_ms - previous.elapsed_ms) / 1000 : null;
    const scopes = sample.brokers.map((broker, b) => {
      const first = samples[0].brokers[b];
      const prior = previous?.brokers[b];
      const rate = (key) =>
        seconds > 0 && prior?.id === broker.id && broker[key] >= prior[key]
          ? (broker[key] - prior[key]) / 1e6 / seconds
          : null;
      return {
        ...broker,
        working_set_bytes: Math.max(0, broker.memory_current_bytes - broker.inactive_file_bytes),
        cpu_seconds: (broker.cpu_usage_us - first.cpu_usage_us) / 1e6,
        cpu_cores: rate('cpu_usage_us'),
        cpu_user_cores: rate('cpu_user_us'),
        cpu_system_cores: rate('cpu_system_us'),
      };
    });
    const cluster = Object.fromEntries(
      resourceMetrics.map(([key]) => [
        key,
        scopes.some((scope) => scope[key] === null)
          ? null
          : scopes.reduce((sum, scope) => sum + scope[key], 0),
      ]),
    );
    return { elapsed_ms: sample.elapsed_ms, brokers: scopes, cluster };
  });
}
export function temporalSeries(trial, data, source, metric, scope = 'cluster') {
  const info = metricInfo(metric);
  if (source === 'omb')
    return (data.result[metric] ?? []).map((value, i) => ({ x: (i + 1) * 10, y: value, index: i }));
  return data.resources.map((sample, i) => {
    const values = scope === 'cluster' ? sample.cluster : sample.brokers[Number(scope)];
    const value = values?.[metric] ?? null;
    return {
      x: sample.elapsed_ms / 1000,
      y: value === null ? null : info.unit === 'MiB' ? value / 1024 ** 2 : value,
      index: i,
    };
  });
}
// Keep first/last and min/max within each bucket so a spike survives drawing.
// Original samples remain available in the table and CSV export.
export function envelope(points, budget = 900) {
  if (points.length <= budget) return points;
  const width = Math.ceil(points.length / Math.max(1, Math.floor(budget / 4)));
  const output = [];
  for (let start = 0; start < points.length; start += width) {
    const bucket = points.slice(start, start + width);
    const valid = bucket.filter((p) => Number.isFinite(p.y));
    const extrema = valid.length
      ? [valid.reduce((a, b) => (a.y < b.y ? a : b)), valid.reduce((a, b) => (a.y > b.y ? a : b))]
      : [];
    const selected = new Set([
      bucket[0],
      ...extrema,
      bucket.at(-1),
      ...bucket.filter((p) => p.y === null),
    ]);
    output.push(...bucket.filter((p) => selected.has(p)));
  }
  return output;
}
export function nearest(points, x) {
  if (!points.length) return null;
  let lo = 0,
    hi = points.length - 1;
  while (lo < hi) {
    const mid = Math.floor((lo + hi) / 2);
    if (points[mid].x < x) lo = mid + 1;
    else hi = mid;
  }
  return lo && Math.abs(points[lo - 1].x - x) < Math.abs(points[lo].x - x)
    ? points[lo - 1]
    : points[lo];
}
export function quantiles(result, family) {
  return Object.entries(result['aggregated' + family + 'Quantiles'] ?? {})
    .map(([percentile, latency]) => ({ x: Number(latency), y: Number(percentile) }))
    .filter((p) => Number.isFinite(p.x) && Number.isFinite(p.y))
    .sort((a, b) => a.x - b.x);
}
export function csv(rows) {
  const escape = (value) => '"' + String(value ?? '').replaceAll('"', '""') + '"';
  return rows.map((row) => row.map(escape).join(',')).join('\r\n') + '\r\n';
}
