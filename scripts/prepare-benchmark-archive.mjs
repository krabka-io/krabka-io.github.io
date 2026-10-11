// Verify the LFS archive, then stage lossless, individually compressed files.
// The page ships only a small index; trial data is fetched on demand.
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { gzipSync } from 'node:zlib';
import { execFileSync } from 'node:child_process';
import { compactTrial } from './publish-openmessaging.mjs';

export const digest = buffer => createHash('sha256').update(buffer).digest('hex');
export const trialFiles = ['result.json', 'trial.json', 'resources.jsonl', 'resources.jsonl.metadata.json', 'workload.stdout', 'workload.stderr'];

export async function prepareArchive(root = process.cwd()) {
  const archiveRoot = path.join(root, 'benchmarks/archive');
  const readme = await fs.readFile(path.join(archiveRoot, 'README.md'), 'utf8');
  const archives = [...new Map([...readme.matchAll(/^([a-f0-9]{64})\s+([\w.-]+\.tar\.gz)$/gm)].map(match => [match[2], match])).values()];
  assert.ok(archives.length, 'archive README must document checksums');
  const output = path.join(root, 'public/benchmarks/archive-data');
  const index = { schema_version: 1, runs: [] };
  const temporary = await fs.mkdtemp(path.join(os.tmpdir(), 'omb-archive-'));
  try {
    for (const [, checksum, filename] of archives) {
      const archive = await fs.readFile(path.join(archiveRoot, filename));
      assert.ok(!archive.toString('utf8', 0, 50).startsWith('version https://git-lfs'),
        'Benchmark archive is an LFS pointer. Run git lfs install and git lfs pull --include "benchmarks/archive/*.tar.gz".');
      assert.equal(digest(archive), checksum, `checksum mismatch: ${filename}`);
      const entries = execFileSync('tar', ['-tzf', path.join(archiveRoot, filename)], { encoding: 'utf8' }).trim().split('\n');
      const id = filename.replace(/\.tar\.gz$/, '');
      assert.ok(entries.every(entry => entry.startsWith(id + '/') && !entry.split('/').includes('..')), 'unsafe archive path');
      execFileSync('tar', ['-xzf', path.join(archiveRoot, filename), '-C', temporary]);
      const source = path.join(temporary, id);
      const destination = path.join(output, id);
      await fs.rm(destination, { recursive: true, force: true });
      await fs.mkdir(destination, { recursive: true });
      const readJSON = async file => JSON.parse(await fs.readFile(path.join(source, file), 'utf8'));
      const provenance = await readJSON('provenance.json');
      assert.equal(provenance.run_id, id);
      assert.equal(provenance.suite, 'openmessaging');
      assert.equal(provenance.mode, 'full');
      assert.equal(provenance.status, 'complete');
      const run = { id, filename, checksum, provenance, trials: [], files: [] };
      async function stage(relative) {
        const bytes = await fs.readFile(path.join(source, relative));
        const file = relative + '.gz';
        const compressed = gzipSync(bytes);
        await fs.writeFile(path.join(destination, file), compressed);
        return { name: path.basename(relative), file, bytes: bytes.length, compressed_bytes: compressed.length, sha256: digest(bytes) };
      }
      for (const file of ['provenance.json', 'summary.md']) run.files.push(await stage(file));
      for (const rf of provenance.replication_factors) for (const vendor of ['krabka', 'kafka', 'redpanda']) {
        for (let repetition = 1; repetition <= provenance.repetitions; repetition++) for (const workload of provenance.cases) {
          const trialId = `rf${rf}-${vendor}-${repetition}-${workload.id}`;
          const trial = await readJSON(trialId + '/trial.json');
          const result = await readJSON(trialId + '/result.json');
          assert.deepEqual([trial.vendor, trial.rf, trial.repetition, trial.case.id], [vendor, rf, repetition, workload.id]);
          assert.deepEqual(result, trial.omb, `OMB result differs from trial: ${trialId}`);
          const metadata = await readJSON(trialId + '/resources.jsonl.metadata.json');
          const samples = (await fs.readFile(path.join(source, trialId, 'resources.jsonl'), 'utf8')).trim().split('\n');
          const last = JSON.parse(samples.at(-1));
          await fs.mkdir(path.join(destination, trialId), { recursive: true });
          const files = [];
          for (const file of trialFiles) files.push(await stage(trialId + '/' + file));
          run.trials.push({ id: trialId, ...compactTrial(trial), started_at: metadata.started_at,
            sampling_interval_ms: metadata.sampling_interval_ms, resource_samples: samples.length,
            resource_duration_seconds: last.elapsed_ms / 1000, interval_samples: result.publishRate.length, files });
        }
      }
      index.runs.push(run);
    }
    await fs.mkdir(path.join(root, 'src/data'), { recursive: true });
    await fs.writeFile(path.join(root, 'src/data/benchmark-archive.json'), JSON.stringify(index) + '\n');
    console.log(`Staged ${index.runs.length} benchmark archive(s), ${index.runs.reduce((n, r) => n + r.trials.length, 0)} trials.`);
    return index;
  } finally { await fs.rm(temporary, { recursive: true, force: true }); }
}

if (import.meta.url === `file://${process.argv[1]}`) await prepareArchive();
