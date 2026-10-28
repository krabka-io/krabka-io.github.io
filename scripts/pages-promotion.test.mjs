import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { test } from 'node:test';
import { load } from 'js-yaml';
import { findCandidate, lookup, artifactName, workflowPath } from './pages-promotion.mjs';

const sha = 'a'.repeat(40);
const run = {
  id: 10,
  path: workflowPath,
  event: 'merge_group',
  head_sha: sha,
  status: 'completed',
  conclusion: 'success',
  repository: { id: 7, full_name: 'owner/site' },
  head_repository: { id: 7, full_name: 'owner/site' },
};
const artifact = {
  id: 20,
  name: artifactName,
  expired: false,
  expires_at: '2030-01-01T00:00:00Z',
  size_in_bytes: 100,
  digest: `sha256:${'b'.repeat(64)}`,
  workflow_run: { id: 10, repository_id: 7, head_repository_id: 7, head_sha: sha },
};

async function candidate(changeRun = () => {}, changeArtifact = () => {}, extra = {}) {
  const producer = structuredClone(run);
  changeRun(producer);
  const packageMeta = structuredClone(artifact);
  changeArtifact(packageMeta);
  const calls = [];
  const result = await findCandidate({
    event: 'push',
    ref: 'refs/heads/main',
    repository: 'owner/site',
    sha,
    now: Date.parse('2026-10-27'),
    api: async (route) => {
      calls.push(route);
      return route.includes('/artifacts?')
        ? { artifacts: [packageMeta] }
        : { workflow_runs: [producer] };
    },
    ...extra,
  });
  return { result, calls };
}

test('promotion selects only the exact successful queue run and immutable artifact ID', async () => {
  const { result, calls } = await candidate();
  assert.deepEqual(result, { runId: 10, artifactId: 20, reason: 'exact-commit queue artifact' });
  assert.match(
    calls[0],
    new RegExp(`workflows/deploy.yml/runs\\?event=merge_group&status=success&head_sha=${sha}`),
  );
  assert.equal(calls[1], '/repos/owner/site/actions/runs/10/artifacts?per_page=100');
});

for (const [label, mutate] of Object.entries({
  'wrong SHA or intermediate batch': (value) => {
    value.head_sha = 'c'.repeat(40);
  },
  'wrong repository': (value) => {
    value.repository.full_name = 'other/site';
  },
  'fork producer': (value) => {
    value.head_repository.full_name = 'fork/site';
  },
  'wrong workflow': (value) => {
    value.path = '.github/workflows/other.yml';
  },
  'PR-head producer': (value) => {
    value.event = 'pull_request';
  },
  'failed producer': (value) => {
    value.conclusion = 'failure';
  },
  'cancelled producer': (value) => {
    value.conclusion = 'cancelled';
  },
  'incomplete producer': (value) => {
    value.status = 'in_progress';
  },
}))
  test(`promotion rejects ${label}`, async () => {
    const { result, calls } = await candidate(mutate);
    assert.equal(result.artifactId, undefined);
    assert.equal(calls.length, 1);
  });

for (const [label, mutate] of Object.entries({
  'expired artifact': (value) => {
    value.expired = true;
  },
  'elapsed retention': (value) => {
    value.expires_at = '2026-10-26T00:00:00Z';
  },
  'missing digest': (value) => {
    delete value.digest;
  },
  'invalid digest': (value) => {
    value.digest = 'sha256:bad';
  },
  'wrong package name': (value) => {
    value.name = 'github-pages';
  },
  'empty package': (value) => {
    value.size_in_bytes = 0;
  },
  'other run artifact': (value) => {
    value.workflow_run.id = 11;
  },
  'other repository artifact': (value) => {
    value.workflow_run.repository_id = 8;
  },
  'fork artifact': (value) => {
    value.workflow_run.head_repository_id = 8;
  },
  'other SHA artifact': (value) => {
    value.workflow_run.head_sha = 'd'.repeat(40);
  },
}))
  test(`promotion falls back on ${label}`, async () => {
    assert.equal((await candidate(undefined, mutate)).result.artifactId, undefined);
  });

for (const event of [
  'pull_request',
  'merge_group',
  'schedule',
  'repository_dispatch',
  'workflow_dispatch',
]) {
  test(`${event} never attempts promotion or lookup`, async () => {
    assert.equal(
      (await candidate(undefined, undefined, { event, api: () => assert.fail('no lookup') })).result
        .artifactId,
      undefined,
    );
  });
}

test('admin-bypass push without a producer falls back', async () => {
  assert.equal(
    (await candidate(undefined, undefined, { api: async () => ({ workflow_runs: [] }) })).result
      .artifactId,
    undefined,
  );
});

test('batched final SHA selects its own run, never an intermediate group', async () => {
  const { result } = await candidate(undefined, undefined, {
    api: async (route) =>
      route.includes('/artifacts?')
        ? { artifacts: [artifact] }
        : { workflow_runs: [{ ...run, id: 9, head_sha: 'c'.repeat(40) }, run] },
  });
  assert.equal(result.runId, 10);
});

test('successful producer with a missing artifact falls back', async () => {
  const { result } = await candidate(undefined, undefined, {
    api: async (route) =>
      route.includes('/artifacts?') ? { artifacts: [] } : { workflow_runs: [run] },
  });
  assert.equal(result.artifactId, undefined);
});

test('lookup errors and invalid metadata fall back without failing the job', async () => {
  const env = {
    GITHUB_EVENT_NAME: 'push',
    GITHUB_REF: 'refs/heads/main',
    GITHUB_SHA: sha,
    GITHUB_REPOSITORY: 'owner/site',
    GITHUB_TOKEN: 'test-token',
  };
  for (const request of [
    async () => {
      throw new Error('network');
    },
    async () => ({ ok: false, status: 403 }),
    async () => ({ ok: true, json: async () => ({ workflow_runs: null }) }),
  ]) {
    assert.equal((await lookup(env, request)).artifactId, undefined);
  }
});

const workflow = load(
  fs.readFileSync(new URL('../.github/workflows/deploy.yml', import.meta.url), 'utf8'),
);
const parent = new URL('../playground/.tools/', import.meta.url);
fs.mkdirSync(parent, { recursive: true });

test('promotion output stays false for skipped/failed download, unpack, or upload', () => {
  const step = workflow.jobs.promote.steps.find((step) => step.id === 'result');
  const directory = fs.mkdtempSync(new URL('promotion-result-', parent));
  try {
    for (const outcome of ['', 'skipped', 'failure', 'cancelled', 'success']) {
      const output = path.join(directory, `output-${outcome}`);
      const result = spawnSync('bash', ['-e', '-o', 'pipefail', '-c', step.run], {
        encoding: 'utf8',
        env: {
          ...process.env,
          UPLOAD_OUTCOME: outcome,
          GITHUB_OUTPUT: output,
          GITHUB_STEP_SUMMARY: path.join(directory, 'summary'),
        },
      });
      assert.equal(result.status, 0, result.stderr);
      assert.equal(fs.readFileSync(output, 'utf8'), `promoted=${outcome === 'success'}\n`);
    }
  } finally {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

test('queue package validation rejects traversal, links, duplicate/incomplete content, and stale staging', () => {
  const result = spawnSync(
    'python3',
    [
      '-c',
      `
import importlib.util, io, json, tarfile, tempfile
from pathlib import Path
spec = importlib.util.spec_from_file_location('unpack', 'scripts/unpack-queue-site.py')
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
with tempfile.TemporaryDirectory(dir='playground/.tools') as directory:
    root = Path(directory)
    files = ['index.html', 'sitemap-index.xml', 'sitemap-inventory.json', 'playground/krabka_playground.js',
             'playground/krabka_playground_bg.wasm', 'playground/broker/krabka-broker.wasm', 'why3-web/manifest.json']
    def archive(extra=None, omit=None):
        p = root / 'site.tar'
        with tarfile.open(p, 'w') as t:
            for name in files:
                if name == omit: continue
                content = b'{}' if name.endswith('.json') else b'validated bytes'
                member = tarfile.TarInfo('./' + name); member.size = len(content)
                t.addfile(member, io.BytesIO(content))
            if extra: t.addfile(extra)
        return p
    good = archive(); m.unpack(good, root / 'valid')
    assert (root / 'valid/index.html').read_bytes() == b'validated bytes'
    for n, kind in enumerate(['traversal', 'absolute', 'symlink', 'hardlink', 'duplicate', 'missing', 'stale']):
        member = None
        if kind in ['traversal', 'absolute', 'duplicate']:
            member = tarfile.TarInfo({'traversal': '../escape', 'absolute': '/escape', 'duplicate': './index.html'}[kind])
        elif kind in ['symlink', 'hardlink']:
            member = tarfile.TarInfo('link'); member.type = tarfile.SYMTYPE if kind == 'symlink' else tarfile.LNKTYPE; member.linkname = 'index.html'
        p = archive(member, 'index.html' if kind == 'missing' else None)
        destination = root / str(n)
        if kind == 'stale': destination.mkdir()
        try: m.unpack(p, destination)
        except (ValueError, OSError): pass
        else: raise AssertionError(kind + ' was accepted')
`,
    ],
    { encoding: 'utf8', env: { ...process.env, PYTHONDONTWRITEBYTECODE: '1' } },
  );
  assert.equal(result.status, 0, result.stdout + result.stderr);
});
