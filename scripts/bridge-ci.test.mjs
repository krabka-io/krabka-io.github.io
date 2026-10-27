import assert from 'node:assert/strict';
import fs from 'node:fs';
import { execFileSync, spawnSync } from 'node:child_process';
import { test } from 'node:test';
import { load } from 'js-yaml';
import { bridgeInput, selectBridge } from './bridge-ci.mjs';

const pr = { pull_request: { draft: false, base: { sha: 'a'.repeat(40) }, head: { sha: 'b'.repeat(40) } } };
const workflow = load(fs.readFileSync(new URL('../.github/workflows/kafkactl-lab-ci.yml', import.meta.url), 'utf8'));

test('bridge sources, dependencies, workflow and shared selection inputs trigger coverage', () => {
  for (const file of ['kafkactl-lab/go.mod', 'kafkactl-lab/go.sum', 'kafkactl-lab/bridge_test.go',
    '.github/workflows/kafkactl-lab.yml', '.github/workflows/kafkactl-lab-ci.yml',
    'scripts/bridge-ci.mjs', 'scripts/bridge-ci.test.mjs', 'scripts/ci-changes.mjs',
    'scripts/snippets/extract.mjs', 'go.work', 'go.work.sum', 'go.mod', 'go.sum', '.gitattributes']) {
    assert.equal(bridgeInput(file), true, file);
    assert.equal(selectBridge('pull_request', pr, () => [file]).bridge, true, file);
  }
  for (const file of ['docs/ci.md', 'src/pages/docs/lab.astro', 'README.md', 'package-lock.json',
    'src/styles/global.css', 'playground/Cargo.lock', '.github/workflows/snippets.yml']) {
    assert.equal(selectBridge('pull_request', pr, () => [file]).bridge, false, file);
  }
});

test('draft skips without diff; ready-for-review checks the complete PR range', () => {
  const draft = structuredClone(pr);
  draft.pull_request.draft = true;
  assert.equal(selectBridge('pull_request', draft, () => { throw new Error('must not diff'); }).bridge, false);
  let range;
  assert.equal(selectBridge('pull_request', { ...pr, action: 'ready_for_review' }, (...args) => {
    range = args;
    return ['kafkactl-lab/go.mod'];
  }).bridge, true);
  assert.deepEqual(range, [pr.pull_request.base.sha, pr.pull_request.head.sha]);
});

test('push and merge queue compare their full event ranges, including unrelated changes', () => {
  for (const [name, event] of [
    ['push', { before: 'a'.repeat(40), after: 'b'.repeat(40) }],
    ['merge_group', { merge_group: { base_sha: 'a'.repeat(40), head_sha: 'b'.repeat(40) } }],
  ]) {
    for (const [file, selected] of [['docs/ci.md', false], ['kafkactl-lab/main.go', true]]) {
      assert.equal(selectBridge(name, event, (base, head) => {
        assert.equal(base, 'a'.repeat(40));
        assert.equal(head, 'b'.repeat(40));
        return [file];
      }).bridge, selected);
    }
  }
});

test('unknown events, invalid metadata, and missing commits select checks conservatively', () => {
  for (const [name, event] of [['pull_request', {}], ['push', {}], ['merge_group', {}], ['unknown', {}]]) {
    assert.equal(selectBridge(name, event).bridge, true);
  }
  assert.equal(selectBridge('pull_request', pr, () => { throw new Error('missing commit'); }).bridge, true);
  assert.equal(selectBridge('workflow_dispatch', {}).bridge, true);
});

test('malformed event CLI cannot silently skip coverage', () => {
  const output = execFileSync(process.execPath, ['scripts/bridge-ci.mjs'], {
    encoding: 'utf8', env: { ...process.env, GITHUB_EVENT_PATH: 'nonexistent-event.json',
      GITHUB_OUTPUT: '', GITHUB_STEP_SUMMARY: '' },
  });
  assert.equal(JSON.parse(output).bridge, true);
});

test('gate rejects failed, cancelled, missing or unexpectedly skipped required work', () => {
  const gate = workflow.jobs.gate;
  assert.equal(gate.if, 'always()');
  assert.deepEqual(gate.needs, ['changes', 'test-build']);
  assert.equal(gate['continue-on-error'], undefined);
  for (const selection of ['success', 'failure', 'cancelled', 'skipped', '']) {
    for (const selected of ['true', 'false', '']) {
      for (const build of ['success', 'failure', 'cancelled', 'skipped', '']) {
        const result = spawnSync('bash', ['-e', '-o', 'pipefail', '-c', gate.steps[0].run], {
          env: { ...process.env, SELECTION_RESULT: selection, BRIDGE_SELECTED: selected, BUILD_RESULT: build },
        });
        const pass = selection === 'success'
          && ((selected === 'true' && build === 'success') || (selected === 'false' && build === 'skipped'));
        assert.equal(result.status === 0, pass, `${selection}:${selected}:${build}`);
      }
    }
  }
});

test('workflow handles ready/draft transitions and queue without path filters or write permissions', () => {
  assert.deepEqual(workflow.permissions, { contents: 'read' });
  for (const event of ['pull_request', 'push', 'merge_group']) {
    assert.deepEqual(workflow.on[event].branches, ['main']);
    assert.equal(workflow.on[event].paths, undefined);
    assert.equal(workflow.on[event]['paths-ignore'], undefined);
  }
  assert.ok(workflow.on.pull_request.types.includes('ready_for_review'));
  assert.ok(workflow.on.pull_request.types.includes('converted_to_draft'));
  assert.equal(workflow.jobs.changes.steps[0].with['fetch-depth'], 0);
  assert.equal(workflow.jobs['test-build'].if, "needs.changes.outputs.bridge == 'true'");
  assert.equal(workflow.jobs['test-build'].env.CGO_ENABLED, '0');
  const build = workflow.jobs['test-build'].steps;
  assert.equal(build.find(step => step.name === 'Test bridge').run, 'go test -mod=readonly -count=1 -v ./...');
  assert.match(build.find(step => step.name === 'Build supported targets').run,
    /linux\/amd64 linux\/arm64 darwin\/amd64 darwin\/arm64 windows\/amd64/);
  for (const job of Object.values(workflow.jobs)) {
    assert.equal(job['continue-on-error'], undefined);
    for (const step of job.steps) {
      assert.equal(step['continue-on-error'], undefined);
      if (step.uses) assert.match(step.uses, /@[a-f0-9]{40}$/);
    }
  }
});
