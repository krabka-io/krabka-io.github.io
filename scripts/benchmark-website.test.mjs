import assert from 'node:assert/strict';
import fs from 'node:fs';
import { spawnSync } from 'node:child_process';
import test from 'node:test';
import { openmessaging } from '../src/utils/benchmarks.mjs';
import { median } from './benchmark-results.mjs';

test('website figures match the published OMB capture for every workload and RF', () => {
  assert.ok(openmessaging);
  const p = openmessaging.provenance;
  assert.equal(p.suite, 'openmessaging');
  assert.equal(p.mode, 'full');
  assert.equal(p.status, 'complete');
  const raw = JSON.parse(fs.readFileSync(`benchmarks/openmessaging/${p.run_id}/trials.json`, 'utf8'));
  assert.equal(raw.length, p.cases.length * p.replication_factors.length * 3 * p.repetitions);
  for (const group of openmessaging.rows) for (const workload of group.cases) for (const vendor of workload.vendors) {
    const selected = raw.filter(t => t.rf === group.rf && t.case.id === workload.id && t.vendor === vendor.id);
    assert.equal(selected.length, p.repetitions);
    assert.equal(vendor.publish_rate, median(selected.map(t => t.publish_rate)));
    assert.equal(vendor.publish_p99_ms, median(selected.map(t => t.publish_latency_ms.p99)));
    assert.equal(vendor.end_to_end_p99_ms, median(selected.map(t => t.end_to_end_latency_ms.p99)));
    assert.equal(vendor.rss_peak_bytes, median(selected.map(t => t.rss_peak_bytes)));
    assert.equal(vendor.cpu_seconds, median(selected.map(t => t.cpu_seconds)));
  }
  for (const url of [openmessaging.summaryURL, openmessaging.provenanceURL]) {
    assert.ok(fs.existsSync('benchmarks/' + url.split('/benchmarks/')[1]));
  }
});

test('custom suites are refused before creating artifacts or starting Docker', () => {
  for (const suite of ['throughput', 'curves']) {
    const result = spawnSync(process.execPath, ['scripts/benchmark.mjs', '--suite', suite], { encoding: 'utf8' });
    assert.equal(result.status, 1);
    assert.match(result.stderr, /only the OpenMessaging suite is supported/);
  }
  const help = spawnSync(process.execPath, ['scripts/benchmark.mjs', '--help'], { encoding: 'utf8' });
  assert.equal(help.status, 0);
  assert.match(help.stdout, /OpenMessaging/);
  assert.doesNotMatch(help.stdout, /throughput\|curves/);
});
