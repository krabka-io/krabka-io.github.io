import assert from 'node:assert/strict';
import { test } from 'node:test';
import { siteCheckSuite } from './site-check-suites.mjs';

test('general checks never start a broker or a proof worker', () => {
  assert.deepEqual(siteCheckSuite(['--suite=site']), { suite: 'site', site: true, toolRoutes: [] });
});
test('tool checks are independent and the default retains complete coverage', () => {
  assert.deepEqual(siteCheckSuite(['--suite=lab']).toolRoutes, ['/docs/lab/']);
  assert.deepEqual(siteCheckSuite(['--suite=proofs']).toolRoutes, ['/docs/proof-explorer/']);
  assert.equal(siteCheckSuite(['--suite=lab']).site, false);
  assert.deepEqual(siteCheckSuite().toolRoutes, ['/docs/lab/', '/docs/proof-explorer/']);
  assert.equal(siteCheckSuite().site, true);
  assert.deepEqual(siteCheckSuite(['--headed']), siteCheckSuite());
  assert.deepEqual(siteCheckSuite(['--headed', '--suite=site']), siteCheckSuite(['--suite=site']));
});
test('invalid browser selections fail instead of silently skipping checks', () => {
  for (const args of [['--suite=typo'], ['--suite=site', '--suite=lab'], ['site']]) {
    assert.throws(() => siteCheckSuite(args), /Usage/);
  }
});
