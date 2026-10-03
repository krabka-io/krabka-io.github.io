import assert from 'node:assert/strict';

const GIB = 1024 ** 3;
export function curveCases(smoke = false) {
  const common = { bytes: 1024, payload: 'random', compression: 'lz4',
    warmup_seconds: smoke ? 2 : 5, seconds: smoke ? 3 : 30 };
  return [
    ...[20_000, 100_000, 250_000, 500_000].map(rate => ({ ...common,
      id: `latency-${rate}`, kind: 'latency', rf: 1, memory_gib: 4, rate })),
    ...[2, 4, 8].map(memory_gib => ({ ...common,
      id: `memory-${memory_gib}g`, kind: 'memory', rf: 1, memory_gib, rate: -1 })),
    { ...common, id: 'recovery', kind: 'recovery', rf: 3, memory_gib: 4, rate: 20_000,
      seconds: smoke ? 16 : 60, pause_after_seconds: smoke ? 4 : 15, pause_seconds: smoke ? 5 : 10 },
  ];
}

export function curveBudget(workload, vendor) {
  const memory = workload.memory_gib * GIB;
  return { broker_memory_bytes: memory, broker_cpus: 4,
    kafka_heap_bytes: vendor === 'kafka' ? Math.min(GIB, memory / 4) : null,
    redpanda_memory_bytes: vendor === 'redpanda' ? memory * 0.75 : null,
    redpanda_reserve_bytes: vendor === 'redpanda' ? memory / 8 : null };
}

export function validateTimeline(workload, summary, timeline, events) {
  assert.equal(timeline.schema_version, 1);
  assert.ok(Number.isFinite(Date.parse(timeline.started_at)), 'workload start missing');
  assert.ok(timeline.samples.length >= 2, 'workload timeline missing');
  let previous = { elapsed_ms: 0, acknowledged: 0, consumed: 0, submitted: 0, offered_records: 0 };
  for (const sample of timeline.samples) {
    assert.ok(sample.elapsed_ms > previous.elapsed_ms, 'workload sample times must increase');
    assert.ok(Math.abs(sample.interval_ms - (sample.elapsed_ms - previous.elapsed_ms)) < 0.001,
      'workload interval differs from timestamps');
    assert.ok(['warmup', 'measure', 'drain'].includes(sample.phase), 'invalid workload phase');
    assert.equal(sample.offered_records_per_second, workload.rate);
    for (const key of ['acknowledged', 'consumed', 'submitted', 'offered_records', 'consumer_lag_records',
      'offered_backlog_records', 'errors', 'latency_samples']) {
      assert.ok(Number.isSafeInteger(sample[key]) && sample[key] >= 0, `invalid ${key}`);
    }
    for (const key of ['acknowledged', 'consumed', 'submitted', 'offered_records']) {
      assert.ok(sample[key] >= previous[key], `workload counter reset: ${key}`);
    }
    assert.equal(sample.errors, 0, 'workload errors');
    assert.equal(sample.consumer_lag_records, Math.max(0, sample.acknowledged - sample.consumed));
    assert.equal(sample.offered_backlog_records, Math.max(0, sample.offered_records - sample.consumed));
    for (const [metric, counter] of [['ack_records_per_second', 'acknowledged'], ['consume_records_per_second', 'consumed']]) {
      assert.ok(Math.abs(sample[metric] - (sample[counter] - previous[counter]) * 1000 / sample.interval_ms) < 0.01,
        `invalid interval ${metric}`);
    }
    for (const key of ['latency_ms_p99', 'ack_latency_ms_p99']) {
      assert.ok(sample[key] === null || (Number.isFinite(sample[key]) && sample[key] >= 0), `invalid ${key}`);
    }
    assert.equal(sample.latency_ms_p99 === null, sample.latency_samples === 0);
    for (const key of ['client_cpu_seconds', 'client_cpu_cores', 'client_heap_used_bytes']) {
      assert.ok(Number.isFinite(sample[key]) && sample[key] >= 0, `invalid ${key}`);
    }
    previous = sample;
  }
  assert.equal(previous.acknowledged, summary.sent, 'final telemetry acknowledgment count differs');
  assert.equal(previous.consumed, summary.consumed, 'final telemetry consumer count differs');
  assert.equal(previous.submitted, summary.sent, 'submitted records did not all acknowledge');
  assert.equal(summary.sent, summary.consumed, 'delivery did not drain');
  assert.ok(summary.sent > 0 && summary.measured_latency_samples > 0, 'empty measured workload');
  assert.equal(summary.errors, 0);
  assert.equal(summary.duplicates, 0);
  assert.ok(previous.elapsed_ms >= (workload.warmup_seconds + workload.seconds) * 1000 - 100,
    'workload ended early');
  for (const key of ['latency_ms_p50', 'latency_ms_p95', 'latency_ms_p99', 'ack_latency_ms_p99']) {
    assert.ok(Number.isFinite(summary[key]) && summary[key] >= 0, `invalid summary ${key}`);
  }
  assert.ok(summary.latency_ms_p50 <= summary.latency_ms_p95 && summary.latency_ms_p95 <= summary.latency_ms_p99,
    'summary latency quantiles out of order');
  if (workload.kind === 'recovery') {
    assert.equal(events.length, 2, 'recovery events missing');
    assert.equal(events[0].action, 'pause');
    assert.equal(events[1].action, 'unpause');
    assert.equal(events[0].broker_id, events[1].broker_id);
    assert.ok(events[0].elapsed_ms >= workload.warmup_seconds * 1000);
    assert.ok(events[1].elapsed_ms >= events[0].elapsed_ms + workload.pause_seconds * 1000 - 100);
    assert.ok(events[1].elapsed_ms < (workload.warmup_seconds + workload.seconds) * 1000);
    assert.ok(events.every(e => Number.isFinite(Date.parse(e.at))), 'recovery event timestamp missing');
  } else assert.equal(events.length, 0);
}

export function curveSummary(workload, summary, timeline, events) {
  validateTimeline(workload, summary, timeline, events);
  const measured = timeline.samples.filter(s => s.phase === 'measure');
  const seconds = measured.reduce((n, s) => n + s.interval_ms / 1000, 0);
  assert.ok(seconds > 0, 'no measurement intervals');
  const acknowledged = measured.reduce((n, s) => n + s.ack_records_per_second * s.interval_ms / 1000, 0);
  const consumed = measured.reduce((n, s) => n + s.consume_records_per_second * s.interval_ms / 1000, 0);
  const last = measured.at(-1);
  let recoverySeconds = null;
  if (events.length) {
    const resumed = events[1].elapsed_ms;
    const after = measured.filter(s => s.elapsed_ms - s.interval_ms >= resumed);
    const index = after.findIndex((s, i) => after.slice(i, i + 3).length === 3
      && after.slice(i, i + 3).every(p => p.ack_records_per_second >= workload.rate * 0.9
        && p.consumer_lag_records <= workload.rate * 0.1));
    if (index >= 0) recoverySeconds = (after[index + 2].elapsed_ms - resumed) / 1000;
  }
  return { records_per_second: acknowledged / seconds, consume_records_per_second: consumed / seconds,
    client_cpu_cores: measured.reduce((n, s) => n + s.client_cpu_cores * s.interval_ms / 1000, 0) / seconds,
    measurement_seconds: seconds, latency_ms_p99: summary.latency_ms_p99,
    ack_latency_ms_p99: summary.ack_latency_ms_p99,
    consumer_lag_peak_records: Math.max(...measured.map(s => s.consumer_lag_records)),
    consumer_lag_at_measurement_end: last.consumer_lag_records,
    offered_backlog_at_measurement_end: last.offered_backlog_records,
    recovery_seconds: recoverySeconds };
}

export function curveReport(provenance, trials) {
  const lines = ['# Local latency, memory, and recovery curves', '',
    `Run: ${provenance.run_id}. Completed: ${provenance.completed_at}.`, '',
    'One vendor at a time on a shared host. RF1 load/memory curves; RF3 leader pause/resume recovery. These are buffered-write local measurements, not machine-failure or power-loss tests.', '',
    ...Object.entries(provenance.images).map(([v, i]) => `- ${v}: \`${i.reference}\``), '',
    'Each trial retains broker resource samples, one-second workload intervals, warm-up/drain phases, memory limits, client/host provenance, and actual fault timestamps. Lag is acknowledged-but-unconsumed records; offered backlog additionally includes producer pacing/backpressure. Empty latency intervals are null. Summary p99 is an HDR quantile over measured scheduled records, including drain; interval p99 values are never averaged.', '',
    '| Case | Broker | Offered records/s | Memory GiB/broker | Ack records/s | End-to-end p99 ms | Ack p99 ms | Peak lag | End offered backlog | Recovery seconds |',
    '|---|---|---:|---:|---:|---:|---:|---:|---:|---:|'];
  for (const c of provenance.cases) for (const vendor of Object.keys(provenance.images)) {
    const selected = trials.filter(t => t.case.id === c.id && t.vendor === vendor);
    const range = key => {
      const values = selected.map(t => t.curve[key]).filter(v => v !== null).sort((a, b) => a - b);
      return values.length !== selected.length ? 'not observed' : `${values[1].toFixed(2)} (${values[0].toFixed(2)}–${values[2].toFixed(2)})`;
    };
    lines.push(`| ${c.id} | ${vendor} | ${c.rate < 0 ? 'unlimited' : c.rate} | ${c.memory_gib} | ${range('records_per_second')} | ${range('latency_ms_p99')} | ${range('ack_latency_ms_p99')} | ${range('consumer_lag_peak_records')} | ${range('offered_backlog_at_measurement_end')} | ${c.kind === 'recovery' ? range('recovery_seconds') : '—'} |`);
  }
  return `${lines.join('\n')}\n`;
}
