import assert from 'node:assert/strict';

import { PRESETS } from '../public/playground/lab/presets.js';
import { parseConfig } from '../public/playground/lab/external.js';

assert.ok(PRESETS.length >= 10);
assert.equal(new Set(PRESETS.map((preset) => preset.id)).size, PRESETS.length);

for (const { id, scenario } of PRESETS) {
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
