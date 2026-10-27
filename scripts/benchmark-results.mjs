import assert from 'node:assert/strict';

export const VENDORS = ['krabka', 'kafka', 'redpanda'];
export function median(values) {
  assert.ok(values.length > 0 && values.every(Number.isFinite), 'expected finite measurements');
  const sorted = [...values].sort((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
}

// Sum simultaneous cluster samples before taking peaks; summing independent
// per-broker peaks would overstate a cluster peak that never actually occurred.
export function aggregateSample(brokers) {
  assert.ok(brokers.length > 0, 'no brokers sampled');
  const total = { cpu_usage_us: 0, rss_bytes: 0, anon_bytes: 0, working_set_bytes: 0 };
  for (const broker of brokers) {
    for (const key of ['cpu_usage_us', 'rss_bytes', 'anon_bytes', 'memory_current_bytes', 'inactive_file_bytes', 'oom_kill']) {
      assert.ok(Number.isFinite(broker[key]) && broker[key] >= 0, `missing or invalid counter: ${key}`);
    }
    assert.ok(broker.rss_bytes > 0, 'broker RSS unavailable');
    assert.equal(broker.oom_kill, 0, 'broker OOM kill');
    total.cpu_usage_us += broker.cpu_usage_us;
    total.rss_bytes += broker.rss_bytes;
    total.anon_bytes += broker.anon_bytes;
    total.working_set_bytes += Math.max(0, broker.memory_current_bytes - broker.inactive_file_bytes);
  }
  return total;
}

function resourceTotals(samples) {
  assert.ok(samples.length >= 2, 'at least two resource samples required');
  const count = samples[0].brokers.length;
  assert.ok(samples.every(s => s.brokers.length === count), 'incomplete cluster samples');
  assert.ok(samples.every(s => Number.isFinite(s.elapsed_ms) && s.elapsed_ms >= 0), 'invalid sample time');
  const totals = samples.map(s => aggregateSample(s.brokers));
  for (let i = 1; i < samples.length; i++) {
    assert.ok(samples[i].elapsed_ms > samples[i - 1].elapsed_ms, 'sample times must increase');
    for (let broker = 0; broker < count; broker++) {
      assert.equal(samples[i].brokers[broker].id, samples[0].brokers[broker].id, 'broker identity changed');
      assert.ok(samples[i].brokers[broker].cpu_usage_us >= samples[i - 1].brokers[broker].cpu_usage_us,
        'CPU counter reset');
    }
  }
  return totals;
}

export function resourceTimeSeries(samples, startedAt) {
  assert.ok(Number.isFinite(Date.parse(startedAt)), 'invalid measurement start');
  const totals = resourceTotals(samples);
  return {
    schema_version: 1,
    started_at: startedAt,
    sampling_interval_ms: 250,
    samples: samples.map((sample, i) => ({
      ...sample,
      cluster: {
        ...totals[i],
        cpu_seconds: (totals[i].cpu_usage_us - totals[0].cpu_usage_us) / 1e6,
        // Average CPU cores used over the actual preceding interval. The first
        // sample is a baseline, not a measured zero-CPU interval.
        cpu_cores: i === 0 ? null : (totals[i].cpu_usage_us - totals[i - 1].cpu_usage_us)
          / ((sample.elapsed_ms - samples[i - 1].elapsed_ms) * 1000),
      },
    })),
  };
}

export function resourceSummary(samples, acknowledged) {
  const totals = resourceTotals(samples);
  const cpuUs = totals.at(-1).cpu_usage_us - totals[0].cpu_usage_us;
  assert.ok(cpuUs > 0 && acknowledged > 0, 'invalid CPU or record count');
  return {
    cpu_seconds: cpuUs / 1e6,
    cpu_us_per_record: cpuUs / acknowledged,
    rss_peak_bytes: Math.max(...totals.map(t => t.rss_bytes)),
    anon_peak_bytes: Math.max(...totals.map(t => t.anon_bytes)),
    working_set_peak_bytes: Math.max(...totals.map(t => t.working_set_bytes)),
    resource_samples: samples.length,
    sampling_interval_ms: 250,
    max_sample_gap_ms: Math.max(...samples.slice(1).map((s, i) => s.elapsed_ms - samples[i].elapsed_ms)),
    measurement_wall_seconds: (samples.at(-1).elapsed_ms - samples[0].elapsed_ms) / 1000,
  };
}

// Block device counters from /proc/diskstats for the named devices. Field
// order after major, minor and name is in the kernel's iostats.rst; sectors
// are 512 bytes whatever the device's block size.
export function parseDiskstats(text, names) {
  const devices = {};
  for (const line of text.split('\n')) {
    const fields = line.trim().split(/\s+/);
    if (fields.length < 14 || !names.includes(fields[2])) continue;
    const n = fields.slice(3).map(Number);
    devices[fields[2]] = { reads: n[0], read_bytes: n[2] * 512, writes: n[4], written_bytes: n[6] * 512,
      io_ms: n[9], weighted_io_ms: n[10] };
  }
  for (const name of names) assert.ok(devices[name], `device counters unavailable: ${name}`);
  return devices;
}

// OMB logs HH:MM:SS.mmm in UTC. Its measurement runs from "Starting benchmark
// traffic" to the next aggregated latency line; warm-up, backlog fill and
// drain come before it. `startedAt` is the resource sampler's clock anchor,
// which precedes the client, so it gives the date and orders midnight.
export function ombMeasurementWindow(stdout, startedAt) {
  const lines = stdout.split('\n');
  const begin = lines.findIndex(line => line.includes('----- Starting benchmark traffic'));
  assert.ok(begin >= 0, 'OMB measurement start not found');
  const end = lines.findIndex((line, i) => i > begin && line.includes('----- Aggregated Pub Latency'));
  assert.ok(end > begin, 'OMB measurement end not found');
  const anchor = Date.parse(startedAt);
  assert.ok(Number.isFinite(anchor), 'invalid sampler anchor');
  let previous = anchor;
  const at = line => {
    const match = line.match(/^(\d\d):(\d\d):(\d\d)\.(\d{3}) /);
    assert.ok(match, `OMB log line has no time: ${line.slice(0, 40)}`);
    const day = new Date(previous);
    let time = Date.UTC(day.getUTCFullYear(), day.getUTCMonth(), day.getUTCDate(), ...match.slice(1, 4).map(Number), Number(match[4]));
    // The sampler starts before the client; allow a few seconds of skew.
    while (time < previous - 5_000) time += 86_400_000;
    previous = time;
    return time;
  };
  const started = at(lines[begin]);
  const ended = at(lines[end]);
  assert.ok(ended > started, 'empty OMB measurement window');
  return { started_at: new Date(started).toISOString(), ended_at: new Date(ended).toISOString(), seconds: (ended - started) / 1000 };
}

// Linear interpolation of a counter at `t` ms, from samples with elapsed_ms.
function counterAt(samples, value, t) {
  assert.ok(t >= samples[0].elapsed_ms && t <= samples.at(-1).elapsed_ms, 'measurement window outside the samples');
  const i = samples.findIndex(s => s.elapsed_ms >= t);
  if (samples[i].elapsed_ms === t || i === 0) return value(samples[i]);
  const [a, b] = [samples[i - 1], samples[i]];
  return value(a) + (value(b) - value(a)) * (t - a.elapsed_ms) / (b.elapsed_ms - a.elapsed_ms);
}

// Broker CPU, published messages and device use over the OMB measurement
// window only. Messages are OMB's mean publish rate times the window, because
// OMB reports rates per interval, not counts.
export function windowMetrics({ samples, deviceSamples, hostSamples = [], startedAt, window, result }) {
  const anchor = Date.parse(startedAt);
  const from = Date.parse(window.started_at) - anchor;
  const to = Date.parse(window.ended_at) - anchor;
  const cpu = s => aggregateSample(s.brokers).cpu_usage_us;
  const cpuSeconds = (counterAt(samples, cpu, to) - counterAt(samples, cpu, from)) / 1e6;
  const meanRate = result.publishRate.reduce((a, b) => a + b, 0) / result.publishRate.length;
  const messages = meanRate * window.seconds;
  assert.ok(cpuSeconds > 0 && messages > 0, 'invalid window CPU or message count');
  const devices = {};
  for (const name of Object.keys(deviceSamples[0]?.devices ?? {})) {
    const delta = key => counterAt(deviceSamples, s => s.devices[name][key], to) - counterAt(deviceSamples, s => s.devices[name][key], from);
    devices[name] = { utilization: delta('io_ms') / (to - from), read_bytes: delta('read_bytes'), written_bytes: delta('written_bytes'),
      reads: delta('reads'), writes: delta('writes') };
  }
  return { ...window, cpu_seconds: cpuSeconds, published_messages: Math.round(messages),
    messages_per_cpu_second: messages / cpuSeconds, devices, ...(hostSamples.length ? { host: hostWindow(hostSamples, from, to) } : {}) };
}

// What the host did to the brokers during the window: pages the hypervisor
// took through the virtio balloon (null when the guest has no balloon
// driver), the least memory the kernel considered available, and the
// host-wide reclaim that falls on whoever allocates, which is the brokers'
// page cache. A nonzero balloon or direct reclaim marks the trial in the
// summary: its latency came from the host, not from the broker.
function hostWindow(hostSamples, from, to) {
  const delta = key => hostSamples[0].host[key] === null ? null : counterAt(hostSamples, s => s.host[key], to) - counterAt(hostSamples, s => s.host[key], from);
  const inside = hostSamples.filter(s => s.elapsed_ms >= from && s.elapsed_ms <= to);
  return { balloon_inflated_pages: delta('balloon_inflate'),
    available_min_bytes: Math.min(...inside.map(s => s.host.mem_available_bytes)),
    pgsteal_kswapd: delta('pgsteal_kswapd'), pgsteal_direct: delta('pgsteal_direct'), allocstall: delta('allocstall') };
}

// True when the host, not the broker, shaped a trial's latency: the
// hypervisor inflated the balloon or the brokers' allocations fell into
// direct reclaim during the measurement window.
export function hostInterfered(trial) {
  const host = trial.metrics?.measurement_window?.host;
  return Boolean(host && (host.balloon_inflated_pages > 0 || host.allocstall > 0));
}

// O_DIRECT check for a broker's open files. `listing` is "path flags [size]"
// lines with fdinfo's octal flags and, when the listing has it, the file's
// length in bytes. Every partition directory of `topicPrefix` whose newest
// .log holds bytes must have it open with O_DIRECT; sealed segments may be
// open for reads without it. A newest segment of zero bytes has had no
// append yet, and krabka opens a segment's O_DIRECT handle at its first
// append, so it is `unwritten` rather than `missing`: at a smoke run's
// 5,000 records/s the client's sticky partitioner leaves some of 100
// partitions untouched for the whole minute. A listing without sizes treats
// every segment as written.
export const O_DIRECT = { x64: 0o40000, arm64: 0o200000 };
export function directIoCheck(listing, arch, topicPrefix = 'test-topic-') {
  assert.ok(O_DIRECT[arch], `O_DIRECT flag unknown for ${arch}`);
  const newest = new Map();
  const direct = new Set();
  const sizes = new Map();
  for (const line of listing.trim().split('\n').filter(Boolean)) {
    const [file, flags, size] = line.split(' ');
    const match = file.match(/^\/data\/([^/]+)\/(\d{20})\.log$/);
    if (!match || !match[1].startsWith(topicPrefix)) continue;
    const key = `${match[1]}/${match[2]}`;
    if (!newest.has(match[1]) || match[2] > newest.get(match[1])) newest.set(match[1], match[2]);
    if (Number.parseInt(flags, 8) & O_DIRECT[arch]) direct.add(key);
    if (/^\d+$/.test(size ?? '')) sizes.set(key, Math.max(sizes.get(key) ?? 0, Number(size)));
  }
  const open = [...newest].filter(([partition, base]) => !direct.has(`${partition}/${base}`));
  const unwritten = open.filter(([partition, base]) => sizes.get(`${partition}/${base}`) === 0).map(([p]) => p).sort();
  const missing = open.filter(([partition, base]) => sizes.get(`${partition}/${base}`) !== 0).map(([p]) => p).sort();
  const verified = [...newest.keys()].filter(partition => !missing.includes(partition) && !unwritten.includes(partition)).sort();
  return { partitions: newest.size, direct: verified.length, unwritten, missing, verified };
}
