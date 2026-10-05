import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import { viewerCases, viewerData, provenanceURL, curveProvenanceURL } from '../src/utils/benchmarks.mjs';
import { GRAPH_GROUPS, averageData, averagePoints, timelineSeries, timelineExtent, faultEvents, overviewSeries, plot, nearestPoint, tickLabel, niceTicks } from '../src/utils/benchmark-chart.mjs';

test('independent clocks align at measurement start; null intervals remain gaps', () => {
  const data = { case: { suite: 'curves', warmup_seconds: 5, seconds: 30 }, trials: [{
    id: 'krabka', name: 'krabka',
    workload_time_series: { started_at: '2026-10-03T00:00:01.750Z', samples: [
      { elapsed_ms: 5000, latency_ms_p99: 1 }, { elapsed_ms: 6000, latency_ms_p99: null }, { elapsed_ms: 7000, latency_ms_p99: 3 },
    ] },
    time_series: { started_at: '2026-10-03T00:00:00.000Z', samples: [
      { elapsed_ms: 0, cluster: { cpu_cores: null } }, { elapsed_ms: 6750, cluster: { cpu_cores: 4 } },
    ] },
    events: [{ action: 'pause', elapsed_ms: 20000 }, { action: 'unpause', elapsed_ms: 30000 }],
  }] };
  const cpu = timelineSeries(data, GRAPH_GROUPS.find(g => g.id === 'cpu').metrics[0]);
  assert.deepEqual(cpu[0].points.map(p => [p.x, p.y]), [[-6.75, null], [0, 4]]);
  const latency = timelineSeries(data, GRAPH_GROUPS.find(g => g.id === 'latency').metrics[0]);
  assert.deepEqual(latency[0].points.map(p => [p.x, p.y]), [[0, 1], [1, null], [2, 3]]);
  assert.equal(plot(latency, [0, 2]).lines[0].path.match(/M/g).length, 2);
  assert.deepEqual(timelineExtent(data), [-6.75, 30]);
  assert.deepEqual(timelineExtent(data, 'measurement'), [0, 30]);
  assert.deepEqual(faultEvents(data).map(e => e.x), [15, 25]);
  assert.equal(nearestPoint(latency[0].points, 1).y, null);
  assert.equal(nearestPoint(latency[0].points, 3), null);
});

test('log scale omits zeros and preserves gaps; subunit tick labels remain distinct', () => {
  const graph = plot([{ points: [{ x: 0, y: 1 }, { x: 1, y: 0 }, { x: 2, y: 0.01 }] }], [0, 2], true);
  assert.equal(graph.lines[0].coordinates.length, 2);
  assert.equal(graph.lines[0].path.match(/M/g).length, 2);
  assert.ok(!graph.lines[0].path.includes('NaN'));
  assert.equal(tickLabel(0.01), '0.01');
  assert.equal(tickLabel(0.1), '0.1');
});

test('averages weight each repetition equally, preserve missing intervals, and stop at shared capture bounds', () => {
  const points = averagePoints([
    [{ x: 0, y: 10 }, { x: 0.2, y: 14 }, { x: 1.2, y: 50 }, { x: 2.2, y: 0 }, { x: 3.2, y: 60 }],
    [{ x: 0, y: 30 }, { x: 1.1, y: null }, { x: 2.1, y: 0 }, { x: 4.1, y: 900 }],
    [{ x: 0, y: 40 }, { x: 1.3, y: 80 }, { x: 2.3, y: 0 }, { x: 3.3, y: 70 }],
  ]);
  assert.deepEqual(points, [
    { x: 0.5, y: (12 + 30 + 40) / 3, contributors: 3 },
    { x: 1.5, y: null, contributors: 2 },
    { x: 2.5, y: 0, contributors: 3 },
  ]);
  assert.equal(plot([{ points }], [0, 4]).lines[0].path.match(/M/g).length, 2);
  assert.deepEqual(averagePoints([
    [{ x: 0.1, y: 999 }, { x: 1.2, y: 10 }, { x: 2.8, y: 999 }],
    [{ x: 0.8, y: 999 }, { x: 1.5, y: 20 }, { x: 2.5, y: 999 }],
    [{ x: 0.2, y: 999 }, { x: 1.8, y: 30 }, { x: 2.9, y: 999 }],
  ]), [{ x: 1.5, y: 20, contributors: 3 }]);
});

test('all average views retain three sources and average whole-trial values and fault times', () => {
  for (const workload of viewerCases) {
    const captures = [1, 2, 3].map(r => viewerData(workload.key, r));
    const data = averageData(captures);
    assert.equal(data.repetition, 'average');
    assert.equal(data.trials.length, 3);
    for (const [index, trial] of data.trials.entries()) {
      assert.equal(trial.repetitions.length, 3);
      assert.equal(trial.metrics.working_set_peak_bytes, captures.reduce((n, c) => n + c.trials[index].metrics.working_set_peak_bytes, 0) / 3);
      assert.equal((trial.curve ?? trial.metrics).records_per_second,
        captures.reduce((n, c) => n + (c.trials[index].curve ?? c.trials[index].metrics).records_per_second, 0) / 3);
      for (const event of trial.events) {
        const raw = captures.map(c => c.trials[index].events.find(e => e.action === event.action));
        assert.equal(event.elapsed_ms, raw.reduce((n, e) => n + e.elapsed_ms, 0) / 3);
        assert.equal(event.broker_id, undefined);
      }
    }
    for (const group of GRAPH_GROUPS.filter(g => workload.suite === 'curves' || g.metrics[0].source === 'resource')) {
      for (const metric of group.metrics) {
        const series = timelineSeries(data, metric);
        const graph = plot(series, timelineExtent(data));
        assert.ok(graph.lines.every(line => !/NaN|Infinity/.test(line.path)));
        assert.ok(series.every(line => line.points.filter(p => p.y !== null).every(p => p.contributors === 3)));
      }
    }
    assert.ok(faultEvents(data).every(e => e.average));
    if (workload.kind === 'recovery') assert.equal(faultEvents(data).length, 6);
  }
  assert.throws(() => averageData([]));
  const captures = structuredClone([1, 2, 3].map(r => viewerData('curves-recovery', r)));
  captures[1].trials[0].curve.recovery_seconds = null;
  assert.equal(averageData(captures).trials[0].curve.recovery_seconds, null);
});

test('published matrices expose every capture, exact values, source links and summary ranges', () => {
  assert.equal(viewerCases.length, 20);
  for (const url of [provenanceURL, curveProvenanceURL]) {
    assert.match(url, /^https:\/\/github\.com\/krabka-io\/krabka-io\.github\.io\/blob\/main\/benchmarks\/results\/[^/]+\/provenance\.json$/);
    assert.ok(fs.existsSync('benchmarks/' + url.split('/benchmarks/')[1]));
  }
  for (const workload of viewerCases) for (const repetition of [1, 2, 3]) {
    const data = viewerData(workload.key, repetition);
    assert.match(data.case.sourceURL, /\/tree\/main\/benchmarks\/results\/[^/]+$/);
    assert.equal(data.trials.length, 3);
    const extent = timelineExtent(data);
    assert.ok(extent[0] < extent[1]);
    for (const trial of data.trials) {
      assert.match(trial.sourceURL, /^https:\/\/github\.com\/krabka-io\/krabka-io\.github\.io\/blob\/main\/benchmarks\/results\/[^/]+\/trials\/[^/]+\.json$/);
      const source = trial.sourceURL.split('/benchmarks/')[1];
      const raw = JSON.parse(fs.readFileSync('benchmarks/' + source, 'utf8'));
      assert.deepEqual(trial.metrics, raw.metrics);
      assert.deepEqual(trial.workload_time_series, raw.workload_time_series);
      assert.equal(trial.time_series.samples.length, raw.time_series.samples.length);
      for (const [index, sample] of trial.time_series.samples.entries()) {
        assert.equal(sample.elapsed_ms, raw.time_series.samples[index].elapsed_ms);
        for (const [key, value] of Object.entries(sample.cluster)) assert.equal(value, raw.time_series.samples[index].cluster[key]);
      }
    }
    for (const group of GRAPH_GROUPS.filter(g => workload.suite === 'curves' || g.metrics[0].source === 'resource')) {
      for (const metric of group.metrics) {
        const series = timelineSeries(data, metric);
        const graph = plot(series, extent, true);
        assert.ok(graph.lines.every(line => !/NaN|Infinity/.test(line.path)));
        assert.ok(series.every(line => line.points.every((p, i, all) => i === 0 || p.x >= all[i - 1].x)));
      }
    }
  }
  const metric = { key: 'latency_ms_p99', scale: 1 };
  const series = overviewSeries(viewerCases, 'load', metric);
  const cases = viewerCases.filter(c => c.category === 'load');
  for (const [vendor, line] of series.entries()) for (const [index, point] of line.points.entries()) {
    const values = [1, 2, 3].map(r => viewerData(cases[index].key, r).trials[vendor].curve.latency_ms_p99).sort((a, b) => a - b);
    assert.deepEqual([point.low, point.y, point.high], values);
  }
  assert.throws(() => viewerData('diagnostics-failed', 1));
  assert.throws(() => viewerData('curves-recovery', 4));
});

test('time ticks land on round values inside the extent', () => {
  assert.deepEqual(niceTicks(-5.7, 62), [0, 20, 40, 60]);
  assert.deepEqual(niceTicks(0, 1), [0, 0.25, 0.5, 0.75, 1]);
  assert.deepEqual(niceTicks(10, 10.3), [10, 10.1, 10.2, 10.3]);
});
