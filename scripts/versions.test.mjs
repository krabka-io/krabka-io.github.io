import assert from 'node:assert/strict';
import test from 'node:test';
import { getEcosystemVersions, releaseTrack } from '../src/utils/versions.ts';

test('release tracks describe tags without claiming operational readiness', () => {
  assert.equal(releaseTrack(null), 'Unreleased');
  assert.equal(releaseTrack('v0.7.0'), 'Pre-1.0');
  assert.equal(releaseTrack('v1.4.2'), '1.0+');
  assert.equal(releaseTrack('v2.0.0'), '1.0+');
  assert.equal(releaseTrack('v1.5.0-beta.1'), 'Prerelease');
});

test('failed GitHub refresh retains complete cached entries and discloses their scope', async () => {
  const fetchBefore = globalThis.fetch;
  const tokenBefore = process.env.GITHUB_TOKEN;
  process.env.GITHUB_TOKEN = 'test-token';
  globalThis.fetch = async () => new Response('', { status: 503 });
  try {
    const snapshot = await getEcosystemVersions();
    assert.equal(snapshot.source, 'Cached');
    assert.equal(snapshot.checkedAt, null);
    assert.ok(snapshot.repos.length > 0);
    for (const item of snapshot.repos) {
      assert.equal(item.dataSource, 'Cached');
      assert.equal(item.releaseTrack, releaseTrack(item.releaseVersion));
      assert.ok(item.changelogUrl.startsWith('https://github.com/krabka-io/'));
      if (item.releaseVersion) assert.ok(item.compareUrl.endsWith('...main'));
      assert.equal('status' in item, false);
    }
  } finally {
    globalThis.fetch = fetchBefore;
    if (tokenBefore === undefined) delete process.env.GITHUB_TOKEN;
    else process.env.GITHUB_TOKEN = tokenBefore;
  }
});

test('a partial GitHub result marks missing repositories as cached and drops stale comparison counts', async () => {
  const fetchBefore = globalThis.fetch;
  const tokenBefore = process.env.GITHUB_TOKEN;
  process.env.GITHUB_TOKEN = 'test-token';
  globalThis.fetch = async (url) => {
    if (url !== 'https://api.github.com/graphql') throw new Error('Comparison unavailable');
    return Response.json({ data: { organization: { repositories: { nodes: [{
      name: 'krabka-broker', latestRelease: { tagName: 'v0.8.0', url: 'https://github.com/krabka-io/krabka-broker/releases/tag/v0.8.0' },
      defaultBranchRef: { target: { oid: '123456789abcdef' } },
    }] } } } });
  };
  try {
    const snapshot = await getEcosystemVersions();
    assert.equal(snapshot.source, 'Mixed');
    assert.ok(snapshot.checkedAt);
    const broker = snapshot.repos.find((item) => item.repo === 'krabka-broker');
    assert.equal(broker.dataSource, 'GitHub');
    assert.equal(broker.releaseVersion, 'v0.8.0');
    assert.equal(broker.mainCommit, '1234567');
    assert.equal(broker.aheadBy, null);
    assert.ok(snapshot.repos.filter((item) => item.repo !== 'krabka-broker').every((item) => item.dataSource === 'Cached'));
  } finally {
    globalThis.fetch = fetchBefore;
    if (tokenBefore === undefined) delete process.env.GITHUB_TOKEN;
    else process.env.GITHUB_TOKEN = tokenBefore;
  }
});
