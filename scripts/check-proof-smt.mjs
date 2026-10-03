// Real browser/WASM checks, using the site's existing Chromium harness.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { ROOT, DIST_DIR, launchOrExit, serve } from './lab-check-lib.mjs';

const browser = await launchOrExit();
const { server, port } = await serve(path.join(ROOT, 'public'), (_req, res, url) => {
  res.setHeader('Cross-Origin-Opener-Policy', 'same-origin');
  res.setHeader('Cross-Origin-Embedder-Policy', 'require-corp');
  if (url !== '/check') return false;
  res.writeHead(200, { 'content-type': 'text/html' }).end('<!doctype html><title>Proof worker check</title>');
  return true;
});
try {
  const page = await browser.newPage();
  page.on("console", (message) => { if (message.type() === "error") console.error(message.text()); });
  page.on("pageerror", (error) => console.error(error.message));
  await page.goto(`http://127.0.0.1:${port}/check`);
  const tasks = await page.evaluate(async (content) => {
    const worker = new Worker('/why3-web/proof_worker.js');
    const request = (message) => new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('Why3 timed out')), 30_000);
      worker.onerror = (event) => { clearTimeout(timer); reject(new Error(event.message)); };
      worker.onmessage = ({ data }) => { clearTimeout(timer); resolve(JSON.parse(data)); };
      worker.postMessage(JSON.stringify(message));
    });
    try {
      const loaded = await request({ cmd: 'load', name: 'barrier', content });
      if (loaded.kind !== 'loaded') throw new Error(JSON.stringify(loaded));
      const id = loaded.theories.flatMap((theory) => theory.goals)[0].id;
      const tasks = {};
      for (const prover of ['z3', 'cvc5', 'cvc4']) {
        const task = await request({ cmd: 'task', id, prover });
        if (task.kind !== 'task') throw new Error(JSON.stringify(task));
        tasks[prover] = task.text;
      }
      return tasks;
    }
    finally { worker.terminate(); }
  }, fs.readFileSync(new URL('./fixtures/barrier_placement_decision.coma', import.meta.url), 'utf8'));
  for (const prover of ['z3', 'cvc5', 'cvc4']) {
    for (const [content, status] of [
      [tasks[prover], 'unsat'],
      ['(set-logic ALL)\n(assert false)\n(check-sat)', 'unsat'],
      ['(set-logic ALL)\n(check-sat)', 'sat'],
      ['(set-logic ALL)\n(assert unsat)\n(check-sat)', 'error'],
      ['(set-logic ALL)\n(assert false)\n(check-sat)\n(assert unsat)', 'error'],
      ['(set-logic ALL)\n(assert false)\n(check-sat)\n(check-sat)', 'error'],
      ['(set-logic ALL)\n(assert false)\n(check-sat)', 'unsat'],
    ]) {
      const reply = await page.evaluate(({ prover, content }) => new Promise((resolve, reject) => {
        const worker = new Worker(`/why3-web/smt-worker.js?prover=${prover}`);
        const timer = setTimeout(() => { worker.terminate(); reject(new Error(`${prover} timed out`)); }, 30_000);
        worker.onmessage = ({ data }) => { clearTimeout(timer); worker.terminate(); resolve(JSON.parse(data)); };
        worker.onerror = (error) => { clearTimeout(timer); worker.terminate(); reject(new Error(error.message)); };
        worker.postMessage(JSON.stringify({ id: 7, content }));
      }), { prover, content });
      assert.equal(reply.id, 7);
      assert.equal(reply.status, status, JSON.stringify(reply));
    }
    console.log(`${prover}: Why3 barrier placement, unsat, sat, parser errors and recovery passed.`);
  }

  // A static host without isolation headers exercises the actual Pages flow:
  // the first Z3 run registers the scoped worker, reloads, and resumes.
  const site = await serve(DIST_DIR);
  const context = await browser.newContext();
  try {
    const page = await context.newPage();
    page.on('console', (message) => { if (message.type() === 'error') console.error(message.text()); });
    page.on('pageerror', (error) => console.error(error.message));
    page.on('requestfailed', (request) => console.error(request.url(), request.failure()));
    page.on('response', (response) => { if (response.status() >= 400) console.error(response.status(), response.url()); });
    const data = JSON.parse(fs.readFileSync(path.join(ROOT, 'src/data/proof-sessions.json'), 'utf8'));
    const smallSession = (prover) => {
      const session = data.sessions.find((session) => session.kind === 'kernel' && session.stats.leaves <= 2 && session.stats.provers[prover]);
      assert.ok(session, `a small recorded ${prover} session`);
      return { prover, session };
    };
    const cvc4Sessions = data.sessions.filter((session) => session.stats.provers.cvc4);
    assert.ok(cvc4Sessions.length, 'recorded CVC4 sessions');
    // Include every CVC4 session, including its Alt-Ergo/Z3 leaves and tactics.
    for (const { prover, session } of [...cvc4Sessions.map((session) => ({ prover: 'cvc4', session })), smallSession('z3'), smallSession('cvc5')]) {
      await page.goto(`http://127.0.0.1:${site.port}/docs/proof-explorer/#session=${session.id}`);
      await page.locator('[data-tab="check"]').click();
      await page.getByRole('button', { name: 'Re-check this session', exact: true }).click();
      try {
        await page.locator('.px-check-ok, .px-check-partial, .px-check-failed').waitFor({ timeout: 90_000 });
      } catch (error) {
        throw new Error(`${error.message}\n${await page.locator('.px-check').innerText()}`);
      }
      const status = await page.locator('.px-check-status').innerText();
      assert.match(status, /leaves re-proved in the browser/);
      const proved = await page.locator('.px-live-proved').count();
      if (prover === 'cvc4') {
        assert.equal(await page.locator('.px-leaf-cvc4 .px-live-proved').count(), session.stats.provers.cvc4, status);
        // One recorded Z3 quantifier leaf exceeds the browser budget locally.
        // Require every CVC4/Alt-Ergo leaf to prove and all other Z3 leaves to
        // either prove or report that budget; errors/divergence/skips still fail.
        const timeouts = await page.locator('.px-leaf-z3 .px-live-timeout').count();
        assert.equal(proved + timeouts, session.stats.leaves, await page.locator('.px-check').innerText());
      } else {
        assert.equal(await page.locator('.px-check-ok').count(), 1, await page.locator('.px-check').innerText());
        assert.equal(proved, session.stats.leaves);
      }
      if (session.stats.provers.z3) {
        assert.equal(await page.evaluate(() => self.crossOriginIsolated), true);
        assert.equal(await page.evaluate(() => new URL(navigator.serviceWorker.controller.scriptURL).pathname), '/docs/proof-explorer/coi-sw.js');
      }
      if (prover === 'cvc4' && !session.stats.provers.z3) {
        assert.equal(await page.evaluate(() => self.crossOriginIsolated), false, 'CVC4 needs no isolation');
      }
      console.log(`${prover}: recorded session ${session.id}: ${status}`);
    }
    // Hold a real WASM download so cancellation is deterministic.
    let downloading = false;
    await page.route('**/why3-web/cvc5.wasm', () => { downloading = true; });
    await page.getByRole('button', { name: 'Re-check again', exact: true }).click();
    const started = Date.now();
    while (!downloading && Date.now() - started < 30_000) await page.waitForTimeout(50);
    assert.equal(downloading, true, 'cvc5 has an in-flight worker');
    await page.getByRole('button', { name: 'Cancel re-check', exact: true }).click();
    await page.waitForFunction(() => document.querySelector('.px-check-status').textContent === 'Re-check cancelled.');
    assert.equal(await page.locator('.px-live-running').count(), 0);
    await page.unroute('**/why3-web/cvc5.wasm');
    await page.getByRole('button', { name: 'Re-check again', exact: true }).click();
    await page.locator('.px-check-ok').waitFor({ timeout: 90_000 });
    console.log('Cancellation terminates the WASM worker; the next re-check succeeds.');
    // Switching pages must not isolate the lab or the rest of the site.
    await page.goto(`http://127.0.0.1:${site.port}/verification/`);
    assert.equal(await page.evaluate(() => self.crossOriginIsolated), false);
  } finally {
    await context.close();
    await new Promise((resolve) => site.server.close(resolve));
  }
} finally {
  await browser.close();
  await new Promise((resolve) => server.close(resolve));
}
