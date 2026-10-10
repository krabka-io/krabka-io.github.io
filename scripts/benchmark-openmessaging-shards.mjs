// Splits an OpenMessaging run across runners and merges the shards back.
//
//   node scripts/benchmark-openmessaging-shards.mjs plan --workloads all --replication-factors 1,3 [--lanes 4]
//   node scripts/benchmark-openmessaging-shards.mjs merge --out DIR SHARD_RUN_DIR...
//   node scripts/benchmark-openmessaging-shards.mjs split --rf N --reason TEXT --out DIR RUN_DIR
//
// A shard is one workload at one replication factor, so all three brokers for
// that pair, and every round of them, run on the same machine. The merge
// refuses shards that disagree on images, mode, rounds, contract or OMB build,
// and requires every workload x RF x broker x round exactly once. The merged
// directory has the layout of a single-runner run (provenance.json,
// summary.md, rf*/trial.json), with each shard's host and runner recorded
// under provenance.shards.

import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import { parseArgs } from 'node:util';
import { OMB_WORKLOADS, ombCases, writeOmbReport } from './benchmark-openmessaging.mjs';

const VENDORS = ['krabka', 'kafka', 'redpanda'];
// Fields every shard must agree on for the shards to form one comparison.
const SHARED = ['suite', 'mode', 'repetitions', 'workload_source', 'contract', 'client_jars', 'client_java'];

export function parseFactors(text) {
  const factors = [...new Set(text.split(',').map(Number))].sort((a, b) => a - b);
  assert.ok(factors.length && factors.every(rf => rf === 1 || rf === 3), 'replication factors must be 1 and/or 3');
  return factors;
}

export function plan(workloads = 'all', replicationFactors = '1,3') {
  const factors = parseFactors(replicationFactors);
  return ombCases(workloads).flatMap(({ id }) => factors.map(rf => ({ name: `rf${rf}-${id}`, workload: id, rf })));
}

// Relative running time of a shard, for spreading shards over lanes. A 100 GB
// backlog fill and drain takes about three times as long as the other cases.
export const shardWeight = shard => shard.workload.startsWith('backlog-') ? 3 : 1;

// Assigns shards to a fixed number of lanes, heaviest first onto the lightest
// lane. Each lane is one runner VM that runs its shards one after another, so
// the provisioner sees one queued job per lane rather than one per shard.
export function lanes(shards, count) {
  assert.ok(Number.isInteger(count) && count > 0, 'lane count must be a positive integer');
  const result = Array.from({ length: Math.min(count, shards.length) }, (_, i) => ({ name: `lane-${i + 1}`, weight: 0, shards: [] }));
  const order = shards.map((shard, index) => ({ shard, index }))
    .sort((a, b) => shardWeight(b.shard) - shardWeight(a.shard) || a.index - b.index);
  for (const { shard } of order) {
    const lane = result.reduce((min, l) => l.weight < min.weight ? l : min);
    lane.shards.push(shard);
    lane.weight += shardWeight(shard);
  }
  return result.map(({ name, shards: list }) => ({ name, shards: list.map(s => s.name).join(' '),
    workloads: list.map(s => s.workload).join(' '), rfs: list.map(s => s.rf).join(' ') }));
}

const imageDigest = image => image.reference;
// What defines a shard's OMB build. Each runner builds OMB itself, and Maven
// stamps the jars it builds from /src with the build time, so their hashes
// differ between two builds of the same source. The source is pinned by the
// commit and the patch hashes, the toolchain by the build image, and every
// dependency jar under /m2 is compared byte for byte.
const ombBuild = provenance => {
  const omb = provenance.openmessaging ?? {};
  return { commit: omb.commit, patches: omb.patches, build_image_id: omb.build_image_id,
    dependencies: Object.fromEntries(Object.entries(omb.jars ?? {}).filter(([jar]) => !jar.startsWith('/src/'))) };
};
const caseOrder = id => {
  const index = OMB_WORKLOADS.indexOf(id);
  return index === -1 ? OMB_WORKLOADS.length : index;
};

export async function loadShard(directory) {
  const provenance = JSON.parse(await fs.readFile(path.join(directory, 'provenance.json'), 'utf8'));
  assert.equal(provenance.suite, 'openmessaging', `${directory} is not an OpenMessaging run`);
  assert.equal(provenance.status, 'complete', `shard ${provenance.run_id} is ${provenance.status}`);
  const trials = [];
  for (const rf of provenance.replication_factors) for (const vendor of VENDORS) {
    for (let repetition = 1; repetition <= provenance.repetitions; repetition++) for (const workload of provenance.cases) {
      const name = `rf${rf}-${vendor}-${repetition}-${workload.id}`;
      const trial = JSON.parse(await fs.readFile(path.join(directory, name, 'trial.json'), 'utf8'));
      assert.deepEqual([trial.vendor, trial.rf, trial.repetition, trial.case.id], [vendor, rf, repetition, workload.id],
        `mismatched trial ${name} in shard ${provenance.run_id}`);
      trials.push({ name, trial });
    }
  }
  return { directory, provenance, trials };
}

// Turns one replication factor of a run that did not finish into a complete
// shard. A run interrupted part-way through RF3 has every RF1 trial, run on
// the same host, images and OMB build in one pass; this keeps those trials
// instead of repeating them. Every vendor, round and workload at that RF must
// have a trial.json and no failure.json, so nothing at the RF was skipped or
// failed. The shard records the source run, its status and the reason under
// derived_from, and the merged provenance keeps that record per shard.
export async function split(directory, rf, out, reason) {
  assert.ok(typeof reason === 'string' && reason.trim(), 'split needs a reason');
  const source = JSON.parse(await fs.readFile(path.join(directory, 'provenance.json'), 'utf8'));
  assert.equal(source.suite, 'openmessaging', `${directory} is not an OpenMessaging run`);
  assert.notEqual(source.status, 'complete', `run ${source.run_id} is complete; merge it as it is`);
  assert.ok(source.replication_factors.includes(rf), `run ${source.run_id} did not include RF${rf}`);
  const names = [];
  let completed = 0;
  for (const vendor of VENDORS) for (let repetition = 1; repetition <= source.repetitions; repetition++) {
    for (const workload of source.cases) {
      const name = `rf${rf}-${vendor}-${repetition}-${workload.id}`;
      await assert.rejects(fs.access(path.join(directory, name, 'failure.json')), undefined,
        `${name} failed in run ${source.run_id}`);
      const stat = await fs.stat(path.join(directory, name, 'trial.json')).catch(() => {
        throw new assert.AssertionError({ message: `run ${source.run_id} has no completed ${name}` });
      });
      completed = Math.max(completed, stat.mtimeMs);
      names.push(name);
    }
  }
  const provenance = { ...source, run_id: `${source.run_id}-rf${rf}`, status: 'complete',
    replication_factors: [rf], completed_trials: names.length, failed_trials: 0,
    completed_at: new Date(completed).toISOString(),
    derived_from: { run_id: source.run_id, status: source.status, replication_factor: rf, reason } };
  await fs.mkdir(out, { recursive: false });
  for (const name of names) {
    await fs.cp(path.join(directory, name), path.join(out, name), { recursive: true, errorOnExist: true, force: false });
  }
  await fs.writeFile(path.join(out, 'provenance.json'), `${JSON.stringify(provenance, null, 2)}\n`);
  // loadShard re-reads every trial and checks that it matches its name.
  return loadShard(out);
}

export function mergeProvenance(shards, runId) {
  assert.ok(shards.length > 0, 'no shards to merge');
  const [first] = shards.map(s => s.provenance);
  for (const { provenance } of shards) {
    for (const key of SHARED) assert.deepEqual(provenance[key], first[key], `shard ${provenance.run_id} differs in ${key}`);
    assert.deepEqual(ombBuild(provenance), ombBuild(first), `shard ${provenance.run_id} differs in the OMB build`);
    for (const vendor of VENDORS) {
      assert.equal(imageDigest(provenance.images[vendor]), imageDigest(first.images[vendor]),
        `shard ${provenance.run_id} ran a different ${vendor} image`);
    }
  }
  const cases = new Map();
  const factors = new Set();
  const seen = new Set();
  for (const { provenance } of shards) {
    for (const workload of provenance.cases) {
      if (cases.has(workload.id)) assert.deepEqual(workload, cases.get(workload.id), `workload ${workload.id} differs between shards`);
      cases.set(workload.id, workload);
      for (const rf of provenance.replication_factors) {
        const pair = `${workload.id}@rf${rf}`;
        assert.ok(!seen.has(pair), `${pair} appears in more than one shard`);
        seen.add(pair);
      }
    }
    for (const rf of provenance.replication_factors) factors.add(rf);
  }
  const merged = {
    ...first,
    run_id: runId,
    status: 'complete',
    started_at: shards.map(s => s.provenance.started_at).sort()[0],
    completed_at: shards.map(s => s.provenance.completed_at).sort().at(-1),
    cases: [...cases.values()].sort((a, b) => caseOrder(a.id) - caseOrder(b.id)),
    replication_factors: [...factors].sort((a, b) => a - b),
    completed_trials: shards.reduce((sum, s) => sum + s.trials.length, 0),
    failed_trials: 0,
    // The top-level host, runner and CPU sets are the first shard's; every
    // shard ran on its own machine and keeps its own record here.
    shards: shards.map(({ provenance }) => ({
      run_id: provenance.run_id,
      cases: provenance.cases.map(c => c.id),
      replication_factors: provenance.replication_factors,
      started_at: provenance.started_at,
      completed_at: provenance.completed_at,
      host: provenance.host,
      runner: provenance.runner,
      cpu_sets: provenance.cpu_sets,
      images: provenance.images,
      ...(provenance.derived_from ? { derived_from: provenance.derived_from } : {}),
    })),
  };
  for (const workload of merged.cases) for (const rf of merged.replication_factors) {
    assert.ok(seen.has(`${workload.id}@rf${rf}`), `no shard ran ${workload.id} at RF${rf}`);
  }
  return merged;
}

export async function merge(directories, out, runId = path.basename(out)) {
  const shards = [];
  for (const directory of directories) shards.push(await loadShard(directory));
  const provenance = mergeProvenance(shards, runId);
  await fs.mkdir(out, { recursive: false });
  const trials = [];
  for (const shard of shards) for (const { name, trial } of shard.trials) {
    await fs.cp(path.join(shard.directory, name), path.join(out, name), { recursive: true, errorOnExist: true, force: false });
    trials.push(trial);
  }
  await fs.writeFile(path.join(out, 'provenance.json'), `${JSON.stringify(provenance, null, 2)}\n`);
  // writeOmbReport re-checks the whole matrix and every OMB result.
  await writeOmbReport(out, provenance, trials);
  return { run_id: runId, shards: shards.length, trials: trials.length };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const [command, ...rest] = process.argv.slice(2);
  if (command === 'plan') {
    const { values } = parseArgs({ args: rest, options: { workloads: { type: 'string', default: 'all' },
      'replication-factors': { type: 'string', default: '1,3' }, lanes: { type: 'string' } } });
    const shards = plan(values.workloads, values['replication-factors']);
    console.log(JSON.stringify(values.lanes === undefined ? shards : lanes(shards, Number(values.lanes))));
  } else if (command === 'merge') {
    const { values, positionals } = parseArgs({ args: rest, allowPositionals: true, options: { out: { type: 'string' } } });
    assert.ok(values.out && positionals.length, 'usage: merge --out DIR SHARD_RUN_DIR...');
    const result = await merge(positionals.map(p => path.resolve(p)), path.resolve(values.out));
    console.log(`Merged ${result.shards} shards into ${result.run_id}: ${result.trials} trials.`);
  } else if (command === 'split') {
    const { values, positionals } = parseArgs({ args: rest, allowPositionals: true,
      options: { out: { type: 'string' }, rf: { type: 'string' }, reason: { type: 'string' } } });
    assert.ok(values.out && values.rf && values.reason && positionals.length === 1,
      'usage: split --rf N --reason TEXT --out DIR RUN_DIR');
    const shard = await split(path.resolve(positionals[0]), Number(values.rf), path.resolve(values.out), values.reason);
    console.log(`Split ${shard.trials.length} RF${values.rf} trials into ${shard.provenance.run_id}.`);
  } else {
    throw new Error('usage: benchmark-openmessaging-shards.mjs plan|merge|split ...');
  }
}
