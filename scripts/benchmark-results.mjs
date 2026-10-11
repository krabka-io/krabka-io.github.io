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
