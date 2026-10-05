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
