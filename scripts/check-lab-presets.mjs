import assert from 'node:assert/strict';

import { PRESETS } from '../public/playground/lab/presets.js';
import { parseConfig } from '../public/playground/lab/external.js';
import { normalizeScenario } from '../public/playground/lab/world.js';

assert.ok(PRESETS.length >= 10);
assert.equal(new Set(PRESETS.map((preset) => preset.id)).size, PRESETS.length);
const encrypted = PRESETS.find((preset) => preset.id === 'sspi-encrypted').scenario;
assert.equal(normalizeScenario(encrypted).security, 'kerberos-encrypted');
assert.equal(normalizeScenario({ ...encrypted, security: 'invalid-protection' }).security, 'invalid-protection', 'leave invalid modes for Rust to reject instead of downgrading');

for (const { id, scenario } of PRESETS) {
  if (scenario.authorization) {
    assert.equal(scenario.security, 'kerberos-encrypted', `${id}: ACLs need authenticated encryption`);
    assert.deepEqual(normalizeScenario(scenario).authorization, scenario.authorization, `${id}: ACL policy was lost`);
  }
  const brokers = scenario.nodes.filter((node) => node.kind === 'krabka-broker');
  assert.ok(brokers.length, `${id}: no real broker`);
  assert.ok(scenario.nodes.every((node) => node.kind !== 'broker'), `${id}: simulated broker`);
  assert.equal(new Set(scenario.nodes.map((node) => node.id)).size, scenario.nodes.length, `${id}: duplicate node id`);
  for (const broker of brokers) parseConfig(broker.config);
  for (const topic of scenario.topics) {
    assert.ok(topic.replication_factor <= brokers.length, `${id}: ${topic.name} has too many replicas`);
  }
}

console.log(`Checked ${PRESETS.length} real-broker presets.`);
