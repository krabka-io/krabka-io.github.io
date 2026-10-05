import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { ROOT, DIST_DIR, launchOrExit, serve } from './lab-check-lib.mjs';
import { flattenSchema, kindSlug } from '../src/utils/operator-crds.mjs';

const snapshot = JSON.parse(fs.readFileSync(path.join(ROOT, 'src/content/docs/operator/crds.json'), 'utf8'));
const routes = ['/docs/operator', '/docs/operator/crds', '/docs/rebalancer', '/docs/gateway',
  '/docs/operator/reference', '/docs/rebalancer/reference', '/docs/rebalancer/broker-tests',
  '/docs/gateway/reference', '/docs/gateway/cloudevents', '/docs/gateway/gitlab-ingestion', '/docs/gateway/github-firehose',
  ...snapshot.resources.map((resource) => `/docs/operator/crds/${kindSlug(resource)}`)];
const output = path.join(ROOT, 'artifacts/component-docs');
fs.mkdirSync(output, { recursive: true });
const browser = await launchOrExit();
const { server, port } = await serve(DIST_DIR);
const base = `http://127.0.0.1:${port}`;
const errors = [];
const field = (page, fieldPath) => page.locator(`[data-field-entry][data-field-path="${fieldPath}"]`).first();
const fieldDetails = (entry) => entry.locator(':scope > details.field-details');
const fieldBody = (entry) => fieldDetails(entry).locator(':scope > .field-body');
const openField = async (entry) => {
  const details = fieldDetails(entry);
  if (await details.getAttribute('open') === null) await details.locator(':scope > summary').click();
};
const closedFieldAncestors = (entry) => {
  const details = [entry.querySelector(':scope > details.field-details')];
  for (let parent = entry.parentElement; parent; parent = parent.parentElement) {
    if (parent.matches('details.field-details')) details.push(parent);
  }
  return details.some((item) => !item?.open);
};
try {
  for (const width of [390, 1440]) for (const theme of ['light', 'dark']) {
    const context = await browser.newContext({ viewport: { width, height: 900 }, colorScheme: theme, reducedMotion: 'reduce' });
    try {
      const page = await context.newPage();
      page.on('pageerror', (error) => errors.push(error.message));
      for (const route of routes) {
        const response = await page.goto(base + route, { waitUntil: 'load' });
        assert.equal(response.status(), 200, route);
        const state = await page.evaluate(() => ({
          overflow: document.documentElement.scrollWidth - innerWidth,
          h1: document.querySelectorAll('h1').length,
          theme: document.documentElement.dataset.theme,
          duplicates: [...document.querySelectorAll('[id]')].map((el) => el.id).filter((id, i, ids) => ids.indexOf(id) !== i),
        }));
        assert.ok(state.overflow <= 1, `${route} ${width} ${theme}: overflow ${state.overflow}px`);
        assert.equal(state.h1, 1, route);
        assert.equal(state.theme, theme, route);
        assert.deepEqual(state.duplicates, [], route);
        if (route.includes('/crds/')) {
          const resource = snapshot.resources.find((item) => route.endsWith('/' + kindSlug(item)));
          const count = resource.spec.versions.reduce((total, version) => total + flattenSchema(version.schema.openAPIV3Schema).length, 0);
          assert.equal(await page.locator('[data-field-entry]').count(), count, route);
          assert.equal(await page.locator('[data-field-example]').count(), count, route);
          assert.equal(await page.locator('[data-field-default]').count(), count, route);
          assert.deepEqual(await page.locator('[data-field-entry]').evaluateAll((entries) => entries.flatMap((entry) => {
            const body = entry.querySelector(':scope > details.field-details > .field-body');
            const example = body?.querySelector('[data-field-example]')?.getAttribute('data-example');
            const defaultValue = body?.querySelector('[data-field-default]')?.getAttribute('data-default');
            try { JSON.parse(example); } catch { return [entry.dataset.fieldPath]; }
            return example !== null && defaultValue?.trim() ? [] : [entry.dataset.fieldPath];
          })), [], `${route}: every entry has a JSON example and default information`);
          assert.equal(await page.getByText('Required child fields', { exact: true }).count(), 0, `${route}: children belong in the field tree`);
          assert.equal(await page.locator('[data-example-disclosure][open]').count(), 0, `${route}: complex examples start collapsed`);
          if (route.endsWith('/kafkarebalance')) {
            const reference = field(page, 'spec.authorizationSecretRef');
            const children = fieldDetails(reference).locator(':scope > [data-field-children] > [data-field-entry]');
            assert.deepEqual(await children.evaluateAll((entries) => entries.map((entry) => entry.dataset.fieldPath)), ['spec.authorizationSecretRef.key', 'spec.authorizationSecretRef.name']);
            assert.deepEqual(await children.evaluateAll((entries) => entries.map((entry) => entry.querySelector(':scope > details > summary .field-name')?.textContent.trim())), ['key', 'name']);
            assert.equal(await fieldBody(reference).locator('[data-example-disclosure]').count(), 1);
          }
          const search = page.getByLabel('Search fields', { exact: true });
          await search.fill('conditions[].type');
          const matches = page.locator('[data-field-entry][data-field-match]');
          const matchCount = await matches.count();
          assert.ok(matchCount > 0, route);
          assert.ok(await page.locator('[data-field-entry][data-field-context]:not([hidden])').count() > 0, `${route}: keep ancestor context`);
          for (const entry of await matches.all()) {
            assert.ok(await entry.isVisible(), `${route}: searched field is visible`);
            assert.equal(await entry.evaluate(closedFieldAncestors), false, `${route}: searched field and every ancestor are open`);
          }
          assert.match(await page.locator('[data-field-count]').textContent(), new RegExp(`^${matchCount}\\b`), `${route}: count exact matches, not ancestors`);
          assert.ok(await field(page, 'status.conditions[].type').isVisible(), `${route}: searched nested field is readable`);
          await search.fill('no-such-crd-field-193485');
          assert.ok(await page.locator('[data-no-fields]').isVisible(), route);
          await page.getByRole('button', { name: 'Reset', exact: true }).click();
          await page.waitForFunction((total) => document.querySelectorAll('[data-field-entry]:not([hidden])').length === total, count);
          assert.equal(await page.locator('[data-field-entry]:not([hidden])').count(), count, route);
          await page.getByLabel('Section', { exact: true }).selectOption('status');
          assert.equal(await page.locator('[data-field-entry]:not([hidden]):not([data-section="status"])').count(), 0, route);
          await page.getByLabel('Requirement', { exact: true }).selectOption('Required');
          assert.ok(await matches.count() > 0, route);
          assert.deepEqual(await matches.evaluateAll((entries) => entries.filter((entry) => !entry.dataset.requirement.startsWith('Required')).map((entry) => entry.dataset.fieldPath)), [], route);
          const target = await page.locator('[data-field-entry][data-section="spec"]').evaluateAll((entries) => entries.sort((a, b) => b.dataset.fieldPath.length - a.dataset.fieldPath.length)[0].querySelector(':scope > details > summary').id);
          await page.evaluate((id) => { location.hash = id; }, target);
          await page.waitForFunction((id) => {
            const entry = document.getElementById(id).closest('[data-field-entry]');
            return !entry.hidden && document.getElementById(id).closest('details').open;
          }, target);
          const linkedField = page.locator(`[id="${target}"]`).locator('xpath=ancestor::*[@data-field-entry][1]');
          assert.ok(await page.locator(`[id="${target}"]`).isVisible(), `${route}: deep link opens the complete field chain`);
          assert.equal(await linkedField.evaluate(closedFieldAncestors), false, `${route}: every deep-link ancestor is open`);
          assert.equal(await search.inputValue(), '', route);
          assert.equal(await page.getByLabel('Section', { exact: true }).inputValue(), '', route);
          assert.equal(await page.getByLabel('Requirement', { exact: true }).inputValue(), '', route);
        }
        if (['/docs/operator/crds/kafka', '/docs/operator/crds/kafkarebalance', '/docs/gateway', '/docs/rebalancer'].includes(route)) {
          if (route.includes('/crds/')) {
            await page.getByLabel('Search fields', { exact: true }).fill(route.endsWith('/kafka') ? 'spec.listeners[].port' : 'authorizationSecretRef');
            if (route.endsWith('/kafkarebalance')) {
              const reference = field(page, 'spec.authorizationSecretRef');
              const description = fieldBody(reference).locator(':scope > .crd-description');
              assert.ok(await description.isVisible(), `${width}px ${theme}: reference description is visible`);
              assert.deepEqual(await description.locator('code').allTextContents(), ['removeBrokers', '<cluster>-rebalancer-auth', 'token', 'krabka.io/cluster']);
              assert.ok(!(await description.textContent()).includes('`'), `${width}px ${theme}: inline code markers render as code`);
              for (const name of ['key', 'name']) {
                const entry = field(page, `spec.authorizationSecretRef.${name}`);
                await openField(entry);
                assert.ok(await fieldBody(entry).locator('[data-field-example] code[data-value]').isVisible(), `${width}px ${theme}: compact ${name} example`);
                assert.equal(await fieldBody(entry).locator('[data-field-default] pre').count(), 0, `${width}px ${theme}: prose ${name} default`);
              }
              await reference.screenshot({ path: path.join(output, `kafkarebalance-group-${width}-${theme}.png`) });
            }
            await page.locator('#field-search').scrollIntoViewIfNeeded();
          }
          assert.ok(await page.evaluate(() => document.documentElement.scrollWidth - innerWidth <= 1), `${route} ${width}px ${theme}: expanded values fit`);
          await page.screenshot({ path: path.join(output, `${route.split('/').at(-1)}-${width}-${theme}.png`), fullPage: route.endsWith('/kafkarebalance') });
        }
      }
      console.log(`Component docs: ${routes.length} routes, ${width}px, ${theme}; filters and deep links passed.`);
    } finally { await context.close(); }
  }
  const context = await browser.newContext({ javaScriptEnabled: false, viewport: { width: 390, height: 900 } });
  try {
    const page = await context.newPage();
    await page.goto(base + '/docs/operator/crds/kafkarebalance');
    assert.ok(await page.locator('[data-field-entry]').count() > 0);
    await openField(field(page, 'spec'));
    const entry = field(page, 'spec.mode');
    const details = fieldDetails(entry);
    await openField(entry);
    assert.equal(await details.getAttribute('open'), '');
    assert.match(await details.textContent(), /removeBrokers/);
    assert.equal(await fieldBody(entry).locator('[data-field-example]').getAttribute('data-example'), '"removeBrokers"');
    assert.equal(await fieldBody(entry).locator('[data-field-default]').getAttribute('data-default'), '"full"');
    await openField(field(page, 'spec.authorizationSecretRef'));
    const key = field(page, 'spec.authorizationSecretRef.key');
    await openField(key);
    assert.ok(await fieldBody(key).locator('[data-field-example] code[data-value]').isVisible());
    assert.equal(await fieldBody(key).locator('[data-field-example]').getAttribute('data-example'), '"token"');
    console.log('CRD fields are readable with JavaScript disabled.');
  } finally { await context.close(); }
  assert.deepEqual(errors, []);
} finally {
  await browser.close();
  await new Promise((resolve) => server.close(resolve));
}
