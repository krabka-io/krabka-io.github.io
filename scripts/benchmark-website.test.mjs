import assert from 'node:assert/strict';
import fs from 'node:fs';
import { spawnSync } from 'node:child_process';
import test from 'node:test';
import { openmessaging, benchmarkHistory } from '../src/utils/benchmarks.mjs';
import { comparison, directComparison, matrixValue } from '../src/utils/benchmark-story.mjs';
import { benchmarkHint } from '../src/utils/benchmark-hints.mjs';
import { median } from './benchmark-results.mjs';

test('every workload has a simple explanation of the test and its purpose', () => {
  for (const workload of openmessaging.provenance.cases)
    for (const rf of openmessaging.provenance.replication_factors) {
      const hint = benchmarkHint(workload, rf);
      assert.match(hint, /What happens/);
      assert.match(hint, /Why this test is useful/);
      assert.ok(hint.includes(`RF${rf}`));
      assert.ok(hint.includes(workload.config.messageSize === 100 ? '100 bytes' : '1 KiB'));
      if (workload.config.consumerBacklogSizeGB)
        assert.match(hint, /delay includes time spent waiting/);
      else if (workload.config.producerRate > 0 && workload.config.producerRate < 1_000_000)
        assert.match(hint, /do not prove equal top speed/);
      else if (workload.config.producerRate > 0) assert.match(hint, /offers up to 10,000,000/);
      if (workload.id.endsWith('-3producers')) assert.match(hint, /Three senders/);
    }
});

test('release comparisons use paired max-rate ratios and exclude omissions', () => {
  for (const run of benchmarkHistory) {
    const summary = comparison(run);
    const pairs = run.rows
      .flatMap((g) => g.cases)
      .filter((c) => c.config.producerRate === 0 || c.config.producerRate >= 1_000_000)
      .map((c) => [
        c.vendors.find((v) => v.id === 'krabka'),
        c.vendors.find((v) => v.id === 'kafka'),
      ]);
    assert.equal(summary.count, pairs.length);
    assert.equal(
      summary.throughput,
      median(pairs.map(([a, b]) => a.publish_rate / b.publish_rate)),
    );
  }
  const omission = openmessaging.rows
    .flatMap((g) => g.cases)
    .find((c) => c.vendors.some((v) => v.omitted));
  assert.equal(matrixValue(omission.vendors, 'redpanda', 'publish_rate'), null);
  assert.equal(
    matrixValue(
      [
        { id: 'krabka', cpu_seconds: 0 },
        { id: 'kafka', cpu_seconds: 4 },
      ],
      'kafka',
      'cpu_seconds',
    ).ratio,
    0,
  );
  assert.equal(
    matrixValue(
      [
        { id: 'krabka', publish_rate: 1 },
        { id: 'kafka', publish_rate: 0 },
      ],
      'kafka',
      'publish_rate',
    ),
    null,
  );
  const slower = {
    rows: [
      {
        rf: 1,
        cases: [
          {
            config: { producerRate: 0 },
            vendors: [
              { id: 'krabka', publish_rate: 50, cpu_seconds: 100 },
              { id: 'kafka', publish_rate: 100, cpu_seconds: 50 },
            ],
          },
        ],
      },
    ],
  };
  assert.equal(comparison(slower).throughput, 0.5);
  assert.equal(comparison(slower).efficiency, 0.25);
  assert.equal(comparison(slower).efficientCount, 0);
  assert.equal(comparison({ rows: [] }), null);
});

test('direct I/O parity wording requires every reported round to be within the bound', () => {
  const evidence = JSON.parse(fs.readFileSync('benchmarks/experiments/direct-io-1370.json'));
  assert.equal(directComparison(evidence).nearParity, true);
  assert.equal(directComparison(evidence).gap, 3);
  assert.equal(directComparison({ rounds: [] }), null);
  assert.equal(
    directComparison({ rounds: [{ krabka_rate: 90, redpanda_rate: 100 }] }).nearParity,
    false,
  );
  assert.equal(directComparison({ rounds: [{ krabka_rate: 100, redpanda_rate: 0 }] }), null);
});

test('website figures match the published OMB capture for every workload and RF', () => {
  assert.ok(openmessaging);
  const p = openmessaging.provenance;
  assert.equal(p.suite, 'openmessaging');
  assert.equal(p.mode, 'full');
  assert.equal(p.status, 'complete');
  const raw = JSON.parse(
    fs.readFileSync(`benchmarks/openmessaging/${p.run_id}/trials.json`, 'utf8'),
  );
  const omitted = p.omitted ?? [];
  assert.equal(
    raw.length,
    (p.cases.length * p.replication_factors.length * 3 - omitted.length) * p.repetitions,
  );
  for (const group of openmessaging.rows)
    for (const workload of group.cases)
      for (const vendor of workload.vendors) {
        const selected = raw.filter(
          (t) => t.rf === group.rf && t.case.id === workload.id && t.vendor === vendor.id,
        );
        const omission = omitted.find(
          (o) => o.rf === group.rf && o.workload === workload.id && o.vendor === vendor.id,
        );
        if (omission) {
          // An omitted trial has no figures and carries its reason to the page.
          assert.deepEqual(
            [selected.length, vendor.omitted, vendor.publish_rate, vendor.end_to_end_p99_ms],
            [0, omission.reason, null, null],
          );
          continue;
        }
        assert.equal(selected.length, p.repetitions);
        assert.equal(vendor.publish_rate, median(selected.map((t) => t.publish_rate)));
        assert.equal(vendor.publish_p99_ms, median(selected.map((t) => t.publish_latency_ms.p99)));
        assert.equal(
          vendor.end_to_end_p99_ms,
          median(selected.map((t) => t.end_to_end_latency_ms.p99)),
        );
        assert.equal(vendor.rss_peak_bytes, median(selected.map((t) => t.rss_peak_bytes)));
        assert.equal(vendor.cpu_seconds, median(selected.map((t) => t.cpu_seconds)));
      }
  for (const url of [openmessaging.summaryURL, openmessaging.provenanceURL]) {
    assert.ok(fs.existsSync('benchmarks/' + url.split('/benchmarks/')[1]));
  }
});

test('custom suites are refused before creating artifacts or starting Docker', () => {
  for (const suite of ['throughput', 'curves']) {
    const result = spawnSync(process.execPath, ['scripts/benchmark.mjs', '--suite', suite], {
      encoding: 'utf8',
    });
    assert.equal(result.status, 1);
    assert.match(result.stderr, /only the OpenMessaging suite is supported/);
  }
  const help = spawnSync(process.execPath, ['scripts/benchmark.mjs', '--help'], {
    encoding: 'utf8',
  });
  assert.equal(help.status, 0);
  assert.match(help.stdout, /OpenMessaging/);
  assert.doesNotMatch(help.stdout, /throughput\|curves/);
});
