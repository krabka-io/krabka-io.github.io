// Controlled local browser checks, not field Web Vitals or an accessibility conformance audit.
import fs from 'node:fs';
import path from 'node:path';
import { DIST_DIR, ROOT, checker, launchOrExit, newLabContext, openLab, serve, waitFor, watchErrors } from './lab-check-lib.mjs';

const ROUTES = ['/', '/docs/', '/get-started/', '/docs/quickstart/', '/docs/browser-tools/', '/docs/operations/', '/api/', '/versions/', '/releases/', '/benchmarks/', '/brand/', '/verification/', '/search/'];
const SEARCH = [
  ['quickstart', '/docs/quickstart'], ['backup', '/docs/broker/operations/backup-restore'],
  ['rolling upgrade', '/docs/broker/operations/deploy'], ['migrating', '/docs/migration'],
  ['compatibility', '/versions'], ['Cluster Lab', '/docs/lab'], ['API Reference', '/api'],
  ['schema registry', '/docs/streams-go/schema-registry'], ['verification', '/verification'], ['configuration', '/docs/broker/config-reference'],
];
const output = path.resolve(process.env.SITE_CHECK_ARTIFACTS ?? path.join(ROOT, 'artifacts/site-check'));
const t = checker();
const measurements = [];
const errorBags = [];
const started = Date.now();
const built = (route) => path.join(DIST_DIR, route, 'index.html');
for (const route of [...ROUTES, '/docs/lab/', '/docs/proof-explorer/']) {
  if (!fs.existsSync(built(route))) throw new Error(`Missing ${built(route)}; build the site first.`);
}
if (!fs.existsSync(path.join(DIST_DIR, 'pagefind/pagefind.js'))) throw new Error('Missing Pagefind index; run npm run build:site.');
fs.mkdirSync(output, { recursive: true });
const browser = await launchOrExit();
const { server, port } = await serve(DIST_DIR);
const base = `http://127.0.0.1:${port}`;

async function structure(page, name, theme) {
  const findings = await page.evaluate(() => {
    const visible = (el) => el.getClientRects().length && getComputedStyle(el).visibility !== 'hidden';
    const name = (el) => [
      (el.getAttribute('aria-labelledby') || '').split(/\s+/).map((id) => document.getElementById(id)?.textContent || '').join(' '),
      el.getAttribute('aria-label'), [...(el.labels || [])].map((label) => label.textContent).join(' '), el.getAttribute('title'),
      el.matches('button, [role="button"]') ? el.textContent : '', el.type === 'submit' ? el.value : '',
    ].some((text) => text?.trim());
    return {
      lang: document.documentElement.lang, mains: document.querySelectorAll('main').length,
      h1s: document.querySelectorAll('h1').length, overflow: document.documentElement.scrollWidth - innerWidth,
      theme: document.documentElement.dataset.theme,
      nestedLinks: document.querySelectorAll('a a').length,
      unnamed: [...document.querySelectorAll('button, input:not([type="hidden"]), select, textarea, [role="button"]')]
        .filter(visible).filter((el) => !name(el)).map((el) => `${el.tagName}#${el.id}.${el.className}`),
    };
  });
  t.check(`${name}: English, one main, one H1`, findings.lang === 'en' && findings.mains === 1 && findings.h1s === 1, JSON.stringify(findings));
  t.check(`${name}: no page overflow`, findings.overflow <= 1, `${findings.overflow}px`);
  t.check(`${name}: controls have names`, !findings.unnamed.length, findings.unnamed.join(', '));
  t.check(`${name}: links are not nested`, findings.nestedLinks === 0, `${findings.nestedLinks} nested links`);
  t.check(`${name}: ${theme} theme`, findings.theme === theme);
}

async function timing(page, route, visit, width, theme, navigate) {
  const start = Date.now();
  await navigate();
  await page.evaluate(() => document.fonts.ready);
  await page.evaluate(() => new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve))));
  const values = await page.evaluate(() => {
    const nav = performance.getEntriesByType('navigation')[0];
    return { domContentLoadedMs: nav?.domContentLoadedEventEnd, loadMs: nav?.loadEventEnd,
      appReadyMs: Number(document.querySelector('[data-startup-ms]')?.dataset.startupMs) || null };
  });
  measurements.push({ route, visit, width, theme, elapsedMs: Date.now() - start, ...values });
}

try {
  for (const width of [390, 1440]) for (const theme of ['light', 'dark']) {
    for (const route of ROUTES) await t.flow(`${route} ${width} ${theme}`, async () => {
      const context = await browser.newContext({ viewport: { width, height: 900 }, colorScheme: theme, reducedMotion: 'reduce' });
      try {
        const page = await context.newPage();
        errorBags.push(watchErrors(page, `${route} ${width} ${theme}`, base));
        await timing(page, route, 'cold-context', width, theme, () => page.goto(base + route, { waitUntil: 'load' }));
        if (route === '/search/') await page.waitForSelector('.pagefind-ui__search-input');
        await structure(page, `${route} ${width} ${theme}`, theme);
        await page.screenshot({ path: path.join(output, `${route.replaceAll('/', '_') || 'home'}-${width}-${theme}.png`) });
        if (route === '/') {
          await page.keyboard.press('Tab');
          t.check('first Tab reaches skip link', await page.locator('a[href="#main"]').evaluate((el) => el === document.activeElement));
          await page.keyboard.press('Enter');
          t.check('skip link focuses main', await page.locator('#main').evaluate((el) => el === document.activeElement));
          if (width === 390) {
            const menu = page.locator('#mobile-menu-button');
            await menu.focus(); await page.keyboard.press('Enter'); await page.keyboard.press('Tab');
            t.check('keyboard opens mobile menu and reaches a link', await menu.getAttribute('aria-expanded') === 'true' && await page.locator('#mobile-menu').evaluate((el) => el.contains(document.activeElement)));
            await page.keyboard.press('Escape');
            t.check('Escape closes menu and returns focus', await menu.getAttribute('aria-expanded') === 'false' && await menu.evaluate((el) => el === document.activeElement));
          }
        }
        if (route === '/get-started/') for (const id of ['deploy-tabs', 'client-tabs']) {
          const tabs = page.locator(`#${id} [role="tab"]`);
          await tabs.first().focus(); await page.keyboard.press('ArrowRight');
          const selected = tabs.nth(1);
          const panel = await selected.getAttribute('aria-controls');
          t.check(`${id}: keyboard selects and shows second panel`, await selected.getAttribute('aria-selected') === 'true' && await page.locator(`#${panel}`).isVisible());
        }
        if (route === '/releases/') {
          const summaries = await page.locator('#release-list > li > p:last-child').allTextContents();
          t.check('release summaries contain text or an explicit full-notes action', summaries.length > 0 && summaries.every((text) => text.trim() && !/^(?:full changelog:?|(?:full changelog:\s*)?https?:\/\/\S+)$/i.test(text.trim())));
          const component = await page.locator('#release-component option').nth(1).getAttribute('value');
          await page.locator('#release-component').selectOption(component);
          const visible = page.locator('#release-list > li:visible');
          const repos = await visible.evaluateAll((rows) => rows.map((row) => row.dataset.repo));
          t.check('release component filter and result announcement', repos.length > 0 && repos.every((repo) => repo === component) && (await page.locator('#release-count').textContent()) === `${repos.length} ${repos.length === 1 ? 'release' : 'releases'}`);
          await page.locator('#release-component').selectOption('');
          t.check('release filter restores all entries', await page.locator('#release-list > li[hidden]').count() === 0);
        }
        if (route === '/search/') {
          const recovery = await page.locator('#search-recovery').evaluate((template) => ({ text: template.content.textContent, retry: !!template.content.querySelector('[data-search-retry]'), docs: !!template.content.querySelector('a[href$="/docs"]'), alert: !!template.content.querySelector('[role="alert"]') }));
          t.check('search recovery offers retry and docs without a build command', recovery.retry && recovery.docs && recovery.alert && !/npm\s+run\s+build/i.test(recovery.text));
        }
        await timing(page, route, 'repeat-visit', width, theme, () => page.goto(base + route, { waitUntil: 'load' }));
      } finally { await context.close(); }
    });

    for (const route of ['/docs/lab/', '/docs/proof-explorer/']) await t.flow(`${route} startup ${width} ${theme}`, async () => {
      const context = await newLabContext(browser, { width, height: 900 });
      await context.addInitScript((value) => localStorage.setItem('theme', value), theme);
      try {
        const page = await context.newPage();
        errorBags.push(watchErrors(page, `${route} ${width} ${theme}`, base, { ignore: (_m, url) => url.endsWith('/why3-web/manifest.json') }));
        const open = async () => {
          if (route === '/docs/lab/') {
            await openLab(page, base);
            await waitFor(page, () => window.krabkaLab?.world.snapshot()?.nodes.some((n) => n.kind === 'krabka-broker' && n.state?.process?.state === 'running'), 'a real broker running', 180_000);
          } else {
            await page.goto(base + route, { waitUntil: 'load' });
            await page.waitForSelector('#krabka-proofs[data-ready="true"]');
          }
        };
        await timing(page, route, 'cold-context', width, theme, open);
        await structure(page, `${route} ${width} ${theme}`, theme);
        if (route === '/docs/lab/') {
          await page.locator('#krabka-lab .lab-node').first().focus(); await page.keyboard.press('Enter');
          if (!await page.getByRole('button', { name: 'Back to canvas', exact: true }).isVisible()) await page.getByRole('button', { name: 'Show the inspector', exact: true }).click();
          await page.getByRole('button', { name: 'Back to canvas', exact: true }).click();
          t.check('Back to canvas returns keyboard focus', await page.evaluate(() => document.activeElement?.matches('#krabka-lab .lab-node, #krabka-lab .lab-canvas')));
        }
        await page.screenshot({ path: path.join(output, `${route.replaceAll('/', '_')}-${width}-${theme}.png`) });
        await timing(page, route, 'repeat-visit', width, theme, open);
      } finally { await context.close(); }
    });
  }
  await t.flow('ten maintained search queries', async () => {
    const page = await browser.newPage();
    errorBags.push(watchErrors(page, 'search queries', base));
    await page.goto(`${base}/search/?q=backup`, { waitUntil: 'load' });
    await page.waitForSelector('.pagefind-ui__result-link');
    for (const [query, expected] of SEARCH) {
      const urls = await page.evaluate(async (query) => {
        const pagefind = await import('/pagefind/pagefind.js');
        const result = await pagefind.search(query);
        return Promise.all(result.results.slice(0, 10).map(async (item) => new URL((await item.data()).url, location.href).pathname));
      }, query);
      t.check(`search "${query}" finds ${expected}`, urls.some((url) => url.replace(/\/$/, '') === expected), urls.join(', '));
    }
    await page.close();
  });
} finally {
  const errors = errorBags.flat();
  t.check('no page or local console errors', !errors.length, errors.slice(0, 5).join(' | '));
  fs.writeFileSync(path.join(output, 'measurements.json'), JSON.stringify({ measuredAt: new Date().toISOString(), runtime: { node: process.version, platform: process.platform, browser: browser.version(), deviceScaleFactor: 1, height: 900, samplesPerVisit: 1 }, profile: 'Loopback static server; no throttling; reduced motion; fresh context then repeat visit. Server sends no-store; browser compilation/storage may be warm. Not field Web Vitals.', measurements, failures: t.failures, errors }, null, 2) + '\n');
  await browser.close(); server.close();
}
t.finish(`PASS: site checks; screenshots and controlled timings in ${output}`, started);
