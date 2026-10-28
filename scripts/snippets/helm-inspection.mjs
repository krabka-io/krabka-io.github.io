import assert from 'node:assert/strict';

// Check each tool's output independently, so another section cannot mask a failure.
export function assertHelmInspection(output) {
  const sections = output.match(
    /\[topic\]\r?\n([\s\S]*?)\[quorum\]\r?\n([\s\S]*?)\[groups\]\r?\n([\s\S]*?)\[api\]\r?\n([\s\S]*)/,
  );
  assert(sections, 'Missing Kubernetes inspection sections');
  const [, topic, quorum, , api] = sections;
  assert.match(
    topic,
    /Topic:\s+quickstart\s+.*PartitionCount:\s+1\b.*ReplicationFactor:\s+1\b/,
    'Expected quickstart topic with one partition and replica',
  );
  assert.match(
    topic,
    /Partition:\s+0\s+Leader:\s+\d+\s+Replicas:\s+\d+\s+Isr:\s+\d+/,
    'Expected an assigned partition leader, replica, and ISR',
  );
  const leader = quorum.match(/^LeaderId:\s+(\d+)\s*$/m)?.[1];
  assert(leader !== undefined, 'Expected a valid quorum leader');
  const voters = quorum.match(/^CurrentVoters:\s*(\[[^\r\n]+\])/m)?.[1];
  assert(voters, 'Expected current quorum voters');
  const parsed = JSON.parse(voters);
  assert(
    parsed.length > 0 &&
      parsed.some(
        (voter) => Number(typeof voter === 'object' ? voter.id : voter) === Number(leader),
      ),
    'Quorum leader must be a current voter',
  );
  assert.match(api, /\(id:\s*\d+\s+rack:/, 'Expected broker API identity');
  assert.match(api, /Produce\(0\):\s*\d+\s+to\s+\d+/, 'Expected Produce API range');
  assert.match(api, /Fetch\(1\):\s*\d+\s+to\s+\d+/, 'Expected Fetch API range');
}
