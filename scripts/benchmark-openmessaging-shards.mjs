// Splits an OpenMessaging run across runners and merges the shards back.
//
//   node scripts/benchmark-openmessaging-shards.mjs plan --workloads all --replication-factors 1,3
//   node scripts/benchmark-openmessaging-shards.mjs merge --out DIR SHARD_RUN_DIR...
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

const imageDigest = image => image.reference;
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

export function mergeProvenance(shards, runId) {
  assert.ok(shards.length > 0, 'no shards to merge');
  const [first] = shards.map(s => s.provenance);
  for (const { provenance } of shards) {
    for (const key of SHARED) assert.deepEqual(provenance[key], first[key], `shard ${provenance.run_id} differs in ${key}`);
    assert.deepEqual(provenance.openmessaging?.jars, first.openmessaging?.jars, `shard ${provenance.run_id} differs in the OMB build`);
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
      'replication-factors': { type: 'string', default: '1,3' } } });
    console.log(JSON.stringify(plan(values.workloads, values['replication-factors'])));
  } else if (command === 'merge') {
    const { values, positionals } = parseArgs({ args: rest, allowPositionals: true, options: { out: { type: 'string' } } });
    assert.ok(values.out && positionals.length, 'usage: merge --out DIR SHARD_RUN_DIR...');
    const result = await merge(positionals.map(p => path.resolve(p)), path.resolve(values.out));
    console.log(`Merged ${result.shards} shards into ${result.run_id}: ${result.trials} trials.`);
  } else {
    throw new Error('usage: benchmark-openmessaging-shards.mjs plan|merge ...');
  }
}
