import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { gunzipSync, gzipSync } from 'node:zlib';
import test from 'node:test';
import { prepareArchive, digest, trialFiles } from './prepare-benchmark-archive.mjs';
import {
  parseResources,
  temporalSeries,
  envelope,
  nearest,
  quantiles,
  intervalMetrics,
  metricInfo,
  csv,
} from '../src/utils/benchmark-explorer.mjs';

const broker = (id, cpu, memory) => ({
  id,
  cpu_usage_us: cpu,
  cpu_user_us: cpu / 2,
  cpu_system_us: cpu / 2,
  rss_bytes: memory,
  anon_bytes: memory / 2,
  memory_current_bytes: memory * 2,
  inactive_file_bytes: memory / 4,
  oom_kill: 0,
});
const rows = [
  { elapsed_ms: 2, brokers: [broker('a', 10, 1024 ** 2), broker('b', 20, 2 * 1024 ** 2)] },
  {
    elapsed_ms: 502,
    brokers: [broker('a', 250010, 2 * 1024 ** 2), broker('b', 500020, 3 * 1024 ** 2)],
  },
];
const resourceText = rows.map((row) => JSON.stringify(row)).join('\n') + '\n';

test('resource counters use actual gaps, cluster sums, baseline null, and MiB units', () => {
  const samples = parseResources(resourceText);
  assert.equal(samples[0].cluster.cpu_cores, null);
  assert.equal(samples[1].cluster.cpu_cores, 1.5);
  assert.equal(samples[1].brokers[0].cpu_cores, 0.5);
  assert.equal(samples[1].cluster.cpu_user_cores, 0.75);
  assert.equal(samples[1].cluster.cpu_seconds, 0.75);
  assert.equal(samples[1].cluster.rss_bytes, 5 * 1024 ** 2);
  assert.equal(samples[1].cluster.working_set_bytes, 8.75 * 1024 ** 2);
  assert.deepEqual(
    temporalSeries({}, { resources: samples }, 'resources', 'rss_bytes', '0').map((p) => [
      p.x,
      p.y,
    ]),
    [
      [0.002, 1],
      [0.502, 2],
    ],
  );
  assert.equal(temporalSeries({}, { resources: samples }, 'resources', 'cpu_cores')[0].y, null);
  const reset = parseResources(
    JSON.stringify(rows[1]) + '\n' + JSON.stringify({ ...rows[0], elapsed_ms: 1002 }),
  );
  assert.equal(reset[1].cluster.cpu_cores, null);
});

test('OMB series retain every value and upstream latency units, without invented absolute times', () => {
  const result = {
    publishRate: [10, 11],
    publishDelayLatency99pct: [120, 130],
    publishLatency9999pct: [1, 2],
    label: ['a'],
    aggregatedPublishLatency99pct: 1,
  };
  assert.equal(intervalMetrics(result).length, 3);
  assert.deepEqual(temporalSeries({}, { result }, 'omb', 'publishRate'), [
    { x: 10, y: 10, index: 0 },
    { x: 20, y: 11, index: 1 },
  ]);
  assert.equal(metricInfo('publishDelayLatency99pct').unit, 'µs');
  assert.equal(metricInfo('endToEndLatency99pct').unit, 'ms');
  assert.match(metricInfo('publishLatency9999pct').title, /p99.99/);
  assert.deepEqual(
    quantiles({ aggregatedPublishLatencyQuantiles: { 95: 1, 80: 0.5, 100: 3 } }, 'PublishLatency'),
    [
      { x: 0.5, y: 80 },
      { x: 1, y: 95 },
      { x: 3, y: 100 },
    ],
  );
});

test('drawing retains spikes, troughs and gaps, cursor picks closest original sample', () => {
  const points = Array.from({ length: 5000 }, (_, x) => ({
    x,
    y: x === 2521 ? 9999 : x === 3521 ? -20 : x === 1021 ? null : 1,
  }));
  const reduced = envelope(points);
  assert.ok(reduced.length < points.length);
  assert.ok(reduced.includes(points[2521]));
  assert.ok(reduced.includes(points[3521]));
  assert.ok(reduced.includes(points[1021]));
  assert.equal(reduced[0], points[0]);
  assert.equal(reduced.at(-1), points.at(-1));
  assert.equal(nearest(points, 2519.8), points[2520]);
  assert.equal(nearest(points, -1), points[0]);
  assert.equal(nearest([], 1), null);
  assert.equal(csv([['a,b', 'a"b', null, 1.123456]]), '"a,b","a""b","","1.123456"\r\n');
});

test('archive staging deduplicates README checksums and preserves every file byte, including empty logs', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'omb-stage-test-'));
  try {
    const id = 'fixture',
      archiveRoot = path.join(root, 'benchmarks/archive'),
      source = path.join(root, id);
    const workload = { id: 'simple', config: {} };
    const provenance = {
      suite: 'openmessaging',
      mode: 'full',
      status: 'complete',
      completed_at: '2026-10-11',
      run_id: id,
      replication_factors: [1],
      repetitions: 1,
      cases: [workload],
    };
    const result = {
      publishRate: [10, 20],
      consumeRate: [9, 20],
      backlog: [1, 0],
      aggregatedPublishLatency50pct: 1,
      aggregatedPublishLatency99pct: 2,
      aggregatedPublishLatency999pct: 3,
      aggregatedEndToEndLatency50pct: 1,
      aggregatedEndToEndLatency99pct: 2,
      aggregatedEndToEndLatency999pct: 3,
    };
    await fs.mkdir(archiveRoot, { recursive: true });
    await fs.mkdir(source);
    await fs.writeFile(path.join(source, 'provenance.json'), JSON.stringify(provenance));
    await fs.writeFile(path.join(source, 'summary.md'), '# Capture\n');
    for (const vendor of ['krabka', 'kafka', 'redpanda']) {
      const directory = path.join(source, `rf1-${vendor}-1-simple`);
      await fs.mkdir(directory);
      const trial = {
        vendor,
        rf: 1,
        repetition: 1,
        case: workload,
        omb: result,
        metrics: { cpu_seconds: 1, rss_peak_bytes: 2, working_set_peak_bytes: 3 },
      };
      const files = {
        'trial.json': JSON.stringify(trial),
        'result.json': JSON.stringify(result),
        'resources.jsonl': resourceText,
        'resources.jsonl.metadata.json':
          '{"started_at":"2026-10-10T00:00:00Z","sampling_interval_ms":250}',
        'workload.stdout': 'a\n<unsafe>\n',
        'workload.stderr': '',
      };
      for (const [name, text] of Object.entries(files))
        await fs.writeFile(path.join(directory, name), text);
    }
    const filename = id + '.tar.gz';
    execFileSync('tar', ['-czf', path.join(archiveRoot, filename), '-C', root, id]);
    const hash = digest(await fs.readFile(path.join(archiveRoot, filename)));
    await fs.writeFile(
      path.join(archiveRoot, 'README.md'),
      `${hash}  ${filename}\n\n${hash}  ${filename}\n`,
    );
    const index = await prepareArchive(root);
    assert.equal(index.runs.length, 1);
    assert.equal(index.runs[0].trials.length, 3);
    for (const trial of index.runs[0].trials) {
      assert.equal(trial.resource_samples, 2);
      assert.equal(trial.interval_samples, 2);
      assert.deepEqual(
        trial.files.map((f) => f.name),
        trialFiles,
      );
      for (const file of trial.files) {
        const bytes = gunzipSync(
          await fs.readFile(path.join(root, 'public/benchmarks/archive-data', id, file.file)),
        );
        assert.deepEqual(bytes, await fs.readFile(path.join(source, trial.id, file.name)));
        assert.equal(digest(bytes), file.sha256);
        assert.equal(bytes.length, file.bytes);
      }
    }
    const published = path.join(root, 'benchmarks/openmessaging/newer');
    await fs.mkdir(path.join(published, 'omb-results'), { recursive: true });
    const compressed = gzipSync(JSON.stringify(result));
    const interval = {
      vendor: 'krabka',
      rf: 1,
      repetition: 1,
      workload: 'simple',
      file: 'omb-results/trial.json.gz',
      sha256: digest(compressed),
      samples: 2,
    };
    await fs.writeFile(path.join(published, interval.file), compressed);
    await fs.writeFile(
      path.join(published, 'provenance.json'),
      JSON.stringify({ ...provenance, run_id: 'newer', completed_at: '2026-10-14' }),
    );
    await fs.writeFile(path.join(published, 'summary.md'), '# Published\n');
    await fs.writeFile(
      path.join(published, 'trials.json'),
      JSON.stringify([{ vendor: 'krabka', rf: 1, repetition: 1, case: workload }]),
    );
    await fs.writeFile(
      path.join(published, 'intervals.json'),
      JSON.stringify({ run_id: 'newer', trials: [interval] }),
    );
    const combined = await prepareArchive(root);
    assert.deepEqual(
      JSON.parse(
        await fs.readFile(path.join(root, 'public/benchmarks/archive-data/index.json'), 'utf8'),
      ),
      combined,
    );
    assert.equal(combined.runs[0].id, 'newer');
    const newer = combined.runs[0].trials[0];
    assert.equal(newer.resource_samples, 0);
    assert.equal(newer.started_at, undefined);
    assert.deepEqual(
      newer.files.map((file) => file.name),
      ['result.json', 'trial.json'],
    );
    const staged = gunzipSync(
      await fs.readFile(
        path.join(root, 'public/benchmarks/archive-data/newer', newer.files[0].file),
      ),
    );
    assert.deepEqual(JSON.parse(staged), result);
    await fs.writeFile(path.join(published, interval.file), 'corrupted');
    await assert.rejects(prepareArchive(root), /published interval checksum mismatch/);
    await fs.writeFile(path.join(published, interval.file), compressed);
    await fs.writeFile(
      path.join(archiveRoot, filename),
      'version https://git-lfs.github.com/spec/v1\n',
    );
    await assert.rejects(prepareArchive(root), /LFS pointer/);
    await fs.writeFile(path.join(archiveRoot, filename), 'corrupted archive');
    await assert.rejects(prepareArchive(root), /checksum mismatch/);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test('archive staging skips a trial the run omitted with a reason', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'omb-stage-omit-'));
  try {
    const id = 'omitted-fixture',
      archiveRoot = path.join(root, 'benchmarks/archive'),
      source = path.join(root, id);
    const workload = { id: 'simple', config: {} };
    const omitted = [
      { vendor: 'redpanda', rf: 1, workload: 'simple', reason: 'disk reserve reached' },
    ];
    const provenance = {
      suite: 'openmessaging',
      mode: 'full',
      status: 'complete',
      completed_at: '2026-10-14',
      run_id: id,
      replication_factors: [1],
      repetitions: 1,
      cases: [workload],
      omitted,
    };
    const result = {
      publishRate: [10, 20],
      consumeRate: [9, 20],
      backlog: [1, 0],
      aggregatedPublishLatency50pct: 1,
      aggregatedPublishLatency99pct: 2,
      aggregatedPublishLatency999pct: 3,
      aggregatedEndToEndLatency50pct: 1,
      aggregatedEndToEndLatency99pct: 2,
      aggregatedEndToEndLatency999pct: 3,
    };
    await fs.mkdir(archiveRoot, { recursive: true });
    await fs.mkdir(source);
    await fs.writeFile(path.join(source, 'provenance.json'), JSON.stringify(provenance));
    await fs.writeFile(path.join(source, 'summary.md'), '# Capture\n');
    for (const vendor of ['krabka', 'kafka']) {
      const directory = path.join(source, `rf1-${vendor}-1-simple`);
      await fs.mkdir(directory);
      const trial = {
        vendor,
        rf: 1,
        repetition: 1,
        case: workload,
        omb: result,
        metrics: { cpu_seconds: 1, rss_peak_bytes: 2, working_set_peak_bytes: 3 },
      };
      const files = {
        'trial.json': JSON.stringify(trial),
        'result.json': JSON.stringify(result),
        'resources.jsonl': resourceText,
        'resources.jsonl.metadata.json':
          '{"started_at":"2026-10-14T00:00:00Z","sampling_interval_ms":250}',
        'workload.stdout': 'a\n',
        'workload.stderr': '',
      };
      for (const [name, text] of Object.entries(files))
        await fs.writeFile(path.join(directory, name), text);
    }
    const filename = id + '.tar.gz';
    execFileSync('tar', ['-czf', path.join(archiveRoot, filename), '-C', root, id]);
    const hash = digest(await fs.readFile(path.join(archiveRoot, filename)));
    await fs.writeFile(path.join(archiveRoot, 'README.md'), `${hash}  ${filename}\n`);
    const index = await prepareArchive(root);
    assert.deepEqual(
      index.runs[0].trials.map((t) => t.vendor),
      ['krabka', 'kafka'],
    );
    assert.deepEqual(index.runs[0].provenance.omitted, omitted);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test('captured OMB quantile maps use percentile keys and latency values across the complete matrix', async (t) => {
  const indexFile = 'src/data/benchmark-archive.json';
  try {
    await fs.access(indexFile);
  } catch {
    t.skip('Run prepare:benchmark-archive to check the measured capture.');
    return;
  }
  const index = JSON.parse(await fs.readFile(indexFile, 'utf8'));
  for (const run of index.runs)
    for (const trial of run.trials) {
      const file = trial.files.find((f) => f.name === 'result.json');
      const result = JSON.parse(
        gunzipSync(
          await fs.readFile(path.join('public/benchmarks/archive-data', run.id, file.file)),
        ),
      );
      for (const family of ['PublishLatency', 'EndToEndLatency', 'PublishDelayLatency']) {
        const points = quantiles(result, family);
        assert.ok(points.length > 0, `${trial.id}: missing ${family}`);
        assert.ok(
          points.every((p) => p.y >= 0 && p.y <= 100 && p.x >= 0),
          `${trial.id}: invalid percentile or latency`,
        );
        assert.equal(points.at(-1).y, 100, `${trial.id}: incomplete distribution`);
        assert.equal(
          points.at(-1).x,
          result['aggregated' + family + 'Max'],
          `${trial.id}: max latency differs from distribution`,
        );
      }
    }
});
