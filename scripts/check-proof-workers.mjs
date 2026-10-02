import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const bundle = new URL('../public/why3-web/', import.meta.url);

// Browser globals only: exposing Node's process.exit would hide worker failures.
function worker(filename) {
  const replies = [];
  const context = vm.createContext({
    onmessage: null, console, TextDecoder, TextEncoder, performance,
    setTimeout, clearTimeout, postMessage: (text) => replies.push(JSON.parse(text)),
  });
  vm.runInContext(fs.readFileSync(new URL(filename, bundle), 'utf8'), context, { timeout: 30_000 });
  return (message) => {
    context.request = JSON.stringify(message);
    vm.runInContext('onmessage({ data: request })', context, { timeout: 30_000 });
    assert.equal(replies.length, 1, 'one reply per request');
    return replies.shift();
  };
}

const why3 = worker('proof_worker.js');
assert.equal(why3({ cmd: 'ping' }).kind, 'pong');
const loaded = why3({
  cmd: 'load', name: 'barrier_placement_decision',
  content: fs.readFileSync(new URL('./fixtures/barrier_placement_decision.coma', import.meta.url), 'utf8'),
});
assert.equal(loaded.kind, 'loaded');
const goals = loaded.theories.flatMap((theory) => theory.goals);
assert.equal(goals.length, 1);
assert.equal(goals[0].name, 'vc_barrier_placement_decision');
const task = why3({ cmd: 'task', id: goals[0].id });
assert.equal(task.kind, 'task');

const altErgo = worker('alt-ergo-worker.js');
let id = 0;
function prove(content, status, steps = 100_000) {
  const reply = altErgo({ id: ++id, content, steps });
  assert.equal(reply.id, id);
  assert.equal(reply.status, status, JSON.stringify(reply));
  if (status !== 'error' && status !== 'timeout') assert.equal(reply.exception, '');
  return reply;
}

prove(task.text, 'unsat');
prove('(set-logic ALL)\n(check-sat)', 'unknown');
// Error messages containing solver keywords are not solver answers.
const invalid = prove('(set-logic ALL)\n(assert unsat)\n(check-sat)', 'error');
assert.match(invalid.diagnostic, /unsat/);
prove(task.text, 'timeout', 0);
prove(task.text, 'unsat'); // A reused worker recovers after errors and budget exhaustion.
console.log('Proof workers: barrier placement, unknown, error, timeout and reuse passed.');
