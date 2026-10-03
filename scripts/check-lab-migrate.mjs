// Scenarios saved before the simulated broker was removed still say
// `kind: "broker"`. The page must turn them into real brokers before the crate
// sees them (the crate rejects the old kind). Pure: no browser.

import assert from 'node:assert/strict';

import { upgradeSimulatedBrokers, normalizeScenario } from '../public/playground/lab/world.js';
import { parseConfig, realConfigFromSimulated } from '../public/playground/lab/external.js';

const legacy = () => ({
  version: 1,
  seed: 3,
  nodes: [
    {
      id: 1,
      kind: 'broker',
      config: {
        broker_id: 1,
        rack: 'a',
        voter: false,
        default_partitions: 6,
        default_replication_factor: -1,
        log_retention_ms: 1,
      },
    },
    { id: 2, kind: 'echo', config: { anything: 1 } },
    { id: 3, kind: 'krabka-broker', config: { rack: 'b' } },
  ],
});

const { doc, ids } = upgradeSimulatedBrokers(legacy());
assert.deepEqual(ids, [1]);
assert.equal(doc.nodes[0].kind, 'krabka-broker');
assert.deepEqual(doc.nodes[0].config, { rack: 'a', voter: false, num_partitions: 6 });
assert.deepEqual(doc.nodes.slice(1), legacy().nodes.slice(1), 'other nodes are untouched');
assert.equal(doc.nodes[0].id, 1, 'the node keeps its id');
assert.equal(doc.seed, 3);

// Idempotent: the converted document has nothing left to convert.
const again = upgradeSimulatedBrokers(doc);
assert.deepEqual(again.ids, []);
assert.deepEqual(again.doc, doc);

// The input is not modified, and a document with no simulated broker comes back as is.
assert.equal(legacy().nodes[0].kind, 'broker');
const plain = { nodes: [{ id: 1, kind: 'echo' }] };
assert.equal(upgradeSimulatedBrokers(plain).doc, plain);
assert.deepEqual(upgradeSimulatedBrokers(undefined), { doc: undefined, ids: [] });

// What the page hands the crate and the real validator is valid.
assert.doesNotThrow(() => parseConfig(normalizeScenario(doc).nodes[0].config));

// Config mapping: real names, real validator, unknown keys dropped.
assert.deepEqual(realConfigFromSimulated(undefined), {});
assert.deepEqual(realConfigFromSimulated(null), {});
assert.deepEqual(realConfigFromSimulated({ default_replication_factor: 3, min_insync_replicas: 2, replica_lag_time_max_ms: 30000 }), {
  default_replication_factor: 3,
  min_insync_replicas: 2,
  replica_lag_time_max_ms: 30000,
});
assert.deepEqual(realConfigFromSimulated({ default_replication_factor: -1, rack: '', voter: 'yes', bogus: 1 }), {});

console.log('Checked the simulated-broker migration.');
