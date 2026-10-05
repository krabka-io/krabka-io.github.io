import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { test } from 'node:test';
import { syncProofs } from './sync-proofs.mjs';

test('proof outputs survive malformed or empty input and valid sessions carry commit provenance', (t) => {
  // This fixture inherits the checked-out site's Git commit; no test commit is made.
  const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
  const dir = fs.mkdtempSync(path.join(root, '.proof-sync-test-'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const verif = path.join(dir, 'verif/krabka_verified_rlib/module');
  const options = { dataOut: path.join(dir, 'data/sessions.json'), comaOut: path.join(dir, 'public/coma'), catalogFile: path.join(dir, 'no-catalog.md') };
  fs.mkdirSync(path.join(verif, 'kernel'), { recursive: true });
  fs.mkdirSync(options.comaOut, { recursive: true });
  fs.mkdirSync(path.dirname(options.dataOut), { recursive: true });
  fs.writeFileSync(options.dataOut, 'previous data');
  fs.writeFileSync(path.join(options.comaOut, 'previous.coma'), 'previous coma');
  fs.writeFileSync(path.join(verif, 'kernel/proof.json'), '{broken');
  assert.throws(() => syncProofs(dir, options), SyntaxError);
  assert.equal(fs.readFileSync(options.dataOut, 'utf8'), 'previous data');
  assert.equal(fs.readFileSync(path.join(options.comaOut, 'previous.coma'), 'utf8'), 'previous coma');
  fs.rmSync(path.join(verif, 'kernel'), { recursive: true });
  assert.throws(() => syncProofs(dir, options), /no proof sessions/);
  assert.equal(fs.readFileSync(options.dataOut, 'utf8'), 'previous data');
  fs.mkdirSync(path.join(verif, 'kernel'));
  fs.writeFileSync(path.join(verif, 'kernel/proof.json'), JSON.stringify({ proofs: { Theory: { Goal: { prover: 'Z3', time: 0.01 } } } }));
  fs.writeFileSync(path.join(verif, 'kernel.coma'), 'let x = 1');
  const data = syncProofs(dir, options);
  assert.match(data.source.commit, /^[a-f\d]{40,64}$/i);
  assert.equal(data.totals.sessions, 1);
  assert.equal(fs.readFileSync(path.join(options.comaOut, 'module/kernel.coma'), 'utf8'), 'let x = 1');
  assert.equal(fs.existsSync(path.join(options.comaOut, 'previous.coma')), false);
});
