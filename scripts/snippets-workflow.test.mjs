import assert from 'node:assert/strict';
import fs from 'node:fs';
import { test } from 'node:test';
import { load } from 'js-yaml';
import { selectChecks } from './ci-changes.mjs';

const workflow = load(
  fs.readFileSync(new URL('../.github/workflows/snippets.yml', import.meta.url), 'utf8'),
);
function contract(doc) {
  const { changes, snippets } = doc.jobs;
  assert.equal(changes.if, undefined);
  assert.equal(changes['continue-on-error'], undefined);
  assert.equal(changes.outputs.snippets, '${{ steps.changes.outputs.snippets }}');
  const checkout = changes.steps.find((step) => step.uses?.startsWith('actions/checkout@'));
  assert.equal(checkout.with['fetch-depth'], 0);
  for (const [name, run, id] of [
    ['Select CI checks', 'node scripts/ci-changes.mjs', 'changes'],
    ['Test snippet extraction and coverage', 'npm run test:snippets', undefined],
  ]) {
    const step = changes.steps.find((step) => step.name === name);
    assert.equal(step.run, run);
    assert.equal(step.id, id);
    assert.equal(step.if, undefined);
    assert.equal(step['continue-on-error'], undefined);
  }
  assert.equal(snippets.needs, 'changes');
  assert.equal(snippets.if, "needs.changes.outputs.snippets == 'true'");
  assert.equal(snippets['continue-on-error'], undefined);
  assert.deepEqual(snippets.strategy.matrix.suite, [
    'check',
    'standalone',
    'compose',
    'helm',
    'rust',
    'go',
    'java',
    'python',
    'javascript',
  ]);
  assert.equal(snippets.strategy['fail-fast'], false);
  const execute = snippets.steps.find((step) => step.name === 'Test the website examples');
  assert.equal(execute.shell, 'bash'); // explicit bash uses -eo pipefail
  assert.equal(
    execute.run.trim(),
    'mkdir -p .snippets\nnpm run check-snippets -- ${{ matrix.suite }} 2>&1 | tee .snippets/${{ matrix.suite }}.log',
  );
  assert.equal(execute['continue-on-error'], undefined);
  assert.equal(execute.if, undefined);
  assert.deepEqual(doc.on.pull_request.types, [
    'opened',
    'synchronize',
    'reopened',
    'ready_for_review',
    'converted_to_draft',
  ]);
  assert.deepEqual(doc.on.pull_request.branches, ['main']);
  assert.deepEqual(doc.on.push.branches, ['main']);
  assert.deepEqual(doc.on.repository_dispatch.types, ['release-published']);
  assert.ok(doc.on.schedule.length);
  assert.ok(Object.hasOwn(doc.on, 'workflow_dispatch'));
  assert.equal(doc.on.pull_request.paths, undefined);
  assert.equal(doc.on.pull_request['paths-ignore'], undefined);
  assert.equal(doc.concurrency['cancel-in-progress'], "${{ github.event_name == 'pull_request' }}");
  assert.equal(
    doc.concurrency.group,
    "snippets-${{ github.event_name == 'pull_request' && github.event.pull_request.number || github.ref }}",
  );
}
test('snippet coverage always runs; only execution shards are conditional', () =>
  contract(workflow));
for (const [name, mutate] of Object.entries({
  'missing ready event': (doc) => {
    doc.on.pull_request.types = ['opened'];
  },
  'skipped extraction': (doc) => {
    doc.jobs.changes.if = 'false';
  },
  'ignored selection failure': (doc) => {
    doc.jobs.changes['continue-on-error'] = true;
  },
  'shallow diff': (doc) => {
    doc.jobs.changes.steps[0].with['fetch-depth'] = 1;
  },
  'nonblocking shard': (doc) => {
    doc.jobs.snippets['continue-on-error'] = true;
  },
  'unconditional shards': (doc) => {
    delete doc.jobs.snippets.if;
  },
  'shards after failed selection': (doc) => {
    doc.jobs.snippets.if = "always() && needs.changes.outputs.snippets == 'true'";
  },
}))
  test(`snippet contract rejects ${name}`, () => {
    const doc = structuredClone(workflow);
    mutate(doc);
    assert.throws(() => contract(doc));
  });
test('snippet scenarios: content, draft and ready; release events select all shards', () => {
  const event = { pull_request: { draft: false } };
  assert.equal(selectChecks('pull_request', event, () => ['docs/architecture.md']).snippets, false);
  assert.equal(
    selectChecks('pull_request', event, () => ['src/pages/docs/broker.astro']).snippets,
    true,
  );
  assert.equal(
    selectChecks('pull_request', event, () => ['public/quickstart/docker-compose.yml']).snippets,
    true,
  );
  event.pull_request.draft = true;
  assert.equal(selectChecks('pull_request', event, () => ['package-lock.json']).snippets, false);
  event.pull_request.draft = false;
  event.action = 'ready_for_review';
  assert.equal(selectChecks('pull_request', event, () => ['package-lock.json']).snippets, true);
  for (const name of ['push', 'schedule', 'workflow_dispatch', 'repository_dispatch']) {
    assert.equal(selectChecks(name, {}).snippets, true);
  }
});
