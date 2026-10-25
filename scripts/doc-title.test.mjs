import test from 'node:test';
import assert from 'node:assert/strict';
import { docTitle } from '../src/utils/doc-title.mjs';

test('archived performance titles distinguish both run IDs and dates', () => {
  const reports = [
    ['2026-10-22-before-after-run-39756166298', 'rf3-regression-v1'],
    ['2026-10-22-before-after-run-39840381315', 'rf3-regression-v1'],
    ['2026-10-22-before-after-run-39865512647', 'rf3-regression-v2'],
    ['2026-10-23-pinned-kafka-run-39865512647', 'rf3-regression-v2'],
  ];
  const titles = reports.map(([run, workload]) => {
    const title = docTitle(`Performance budget: ${workload}`, `broker/operations/performance-budget-runs/${run}/performance-budget`, 'Broker');
    assert.ok(`${title} | krabka docs`.length <= 80);
    assert.ok(title.includes(run.slice(0, 10)));
    assert.ok(title.includes(run.split('-run-')[1]));
    assert.ok(title.includes(workload));
    return title;
  });
  assert.equal(new Set(titles).size, reports.length);
});

test('ordinary guide titles and component labels are unchanged', () => {
  assert.equal(docTitle('Configuration', 'broker/config-reference', 'Broker'), 'Configuration (Broker)');
  assert.equal(docTitle('Performance budget', 'broker/operations/performance-budget', 'Broker'), 'Performance budget (Broker)');
  assert.equal(docTitle('Guide', 'other/guide'), 'Guide');
});
