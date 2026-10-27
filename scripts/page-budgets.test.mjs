import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { auditBudgets, payloadSize } from './check-page-budgets.mjs';

test('budgets fail empty output, missing assets, and raw/compressed payload overruns', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'krabka-page-budgets-'));
  const config = { html: { rawBytes: 10000, gzipBytes: 10000, required: ['index.html'], overrides: {} }, groups: [] };
  try {
    assert.throws(() => auditBudgets(dir, config), /No built HTML/);
    fs.writeFileSync(path.join(dir, 'index.html'), '<main><h1>Budget fixture</h1></main>');
    assert.equal(auditBudgets(dir, config).failures.length, 0);
    const size = payloadSize(path.join(dir, 'index.html'));
    config.html.rawBytes = size.rawBytes - 1;
    config.html.gzipBytes = size.gzipBytes - 1;
    assert.equal(auditBudgets(dir, config).failures.length, 2);
    config.groups = [{ name: 'Required WASM', files: ['app.wasm'], rawBytes: 100, gzipBytes: 100 }];
    assert.ok(auditBudgets(dir, config).failures.some((failure) => failure.includes('Missing required asset')));
    config.html.required.push('docs/index.html');
    assert.ok(auditBudgets(dir, config).failures.some((failure) => failure.includes('Missing required page')));
    fs.writeFileSync(path.join(dir, 'app.wasm'), '');
    assert.ok(auditBudgets(dir, config).failures.some((failure) => failure.includes('Empty required payload')));
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
});

test('lightweight builds omit only generated WASM, retaining all other budgets', t => {
  const parent = path.resolve('artifacts/tmp');
  fs.mkdirSync(parent, { recursive: true, mode: 0o700 });
  const dir = fs.mkdtempSync(path.join(parent, 'budget-selection-'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  fs.writeFileSync(path.join(dir, 'index.html'), '<h1>Fixture</h1>');
  const limit = { rawBytes: 10000, gzipBytes: 10000 };
  const config = { html: { ...limit, required: ['index.html'], overrides: {} }, groups: [
    { ...limit, name: 'Broker WASM', files: ['playground/broker/krabka-broker.wasm'] },
    { ...limit, name: 'Playground WASM', files: ['playground/krabka_playground_bg.wasm'] },
    { ...limit, name: 'Other payload', files: ['required.js'] },
  ] };
  assert.equal(auditBudgets(dir, config).skippedGroups.length, 0);
  const report = auditBudgets(dir, config, { wasm: false });
  assert.deepEqual(report.skippedGroups, ['Broker WASM', 'Playground WASM']);
  assert.ok(report.failures.some(failure => failure.includes('required.js')));
  assert.ok(!report.failures.some(failure => failure.includes('.wasm')));
  fs.writeFileSync(path.join(dir, 'required.js'), 'payload');
  assert.deepEqual(auditBudgets(dir, config, { wasm: false }).failures, []);
  config.html.rawBytes = 1;
  assert.match(auditBudgets(dir, config, { wasm: false }).failures.join('\n'), /index.html.*exceeds/);
});
