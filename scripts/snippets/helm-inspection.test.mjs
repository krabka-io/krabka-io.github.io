import assert from 'node:assert/strict';
import { test } from 'node:test';
import { assertHelmInspection } from './helm-inspection.mjs';

const output = `[topic]
Topic: quickstart TopicId: abc PartitionCount: 1 ReplicationFactor: 1 Configs:
Topic: quickstart Partition: 0 Leader: 0 Replicas: 0 Isr: 0
[quorum]
LeaderId: 0
CurrentVoters: [{"id":0,"directoryId":"abc"},{"id":1},{"id":2}]
[groups]
[api]
broker.default.svc:9092 (id: 0 rack: null) -> (
  Produce(0): 0 to 13 [usable: 13],
  Fetch(1): 0 to 18 [usable: 18]
)`;

test('Kubernetes inspection accepts real fields and an empty group list', () => {
  assert.doesNotThrow(() => assertHelmInspection(output));
  assert.doesNotThrow(() =>
    assertHelmInspection(
      output
        .replace('[groups]\n', '[groups]\nexample-group\n')
        .replace('[{"id":0,"directoryId":"abc"},{"id":1},{"id":2}]', '[0,1,2]'),
    ),
  );
});
test('Kubernetes inspection rejects missing or invalid topic, quorum, and APIs', () => {
  for (const [before, after] of [
    ['Topic: quickstart', 'Topic: other'],
    ['PartitionCount: 1', 'PartitionCount: 0'],
    ['Leader: 0', 'Leader: -1'],
    ['LeaderId: 0', 'LeaderId: -1'],
    ['CurrentVoters: [{"id":0,"directoryId":"abc"},{"id":1},{"id":2}]', 'CurrentVoters: []'],
    ['LeaderId: 0', 'LeaderId: 9'],
    ['Produce(0): 0 to 13', 'Produce(0): UNSUPPORTED'],
    ['Fetch(1): 0 to 18', 'Fetch(1): UNSUPPORTED'],
    ['[groups]', '[missing]'],
  ])
    assert.throws(() => assertHelmInspection(output.replace(before, after)), before);
});
