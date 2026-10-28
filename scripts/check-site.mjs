// Controlled local browser checks, not field Web Vitals or an accessibility conformance audit.
import fs from 'node:fs';
import path from 'node:path';
import {
  DIST_DIR,
  ROOT,
  checker,
  launchOrExit,
  newLabContext,
  openLab,
  serve,
  waitFor,
  watchErrors,
} from './lab-check-lib.mjs';

import { websiteSnippets } from './snippets/extract.mjs';
import { siteCheckSuite } from './site-check-suites.mjs';

const selection = siteCheckSuite(process.argv.slice(2));
const ROUTES = [
  '/',
  '/docs/',
  '/get-started/',
  '/docs/quickstart/',
  '/docs/kubernetes-quickstart/',
  '/docs/browser-tools/',
  '/docs/operations/',
  '/api/',
  '/versions/',
  '/releases/',
  '/benchmarks/',
  '/brand/',
  '/verification/',
  '/search/',
];
const SEARCH = [
  ['quickstart', '/docs/quickstart'],
  ['Kubernetes quickstart', '/docs/kubernetes-quickstart'],
  ['backup', '/docs/broker/operations/backup-restore'],
  ['rolling upgrade', '/docs/broker/operations/deploy'],
  ['migrating', '/docs/migration'],
  ['compatibility', '/versions'],
  ['Cluster Lab', '/docs/lab'],
  ['API Reference', '/api'],
  ['schema registry', '/docs/streams-go/schema-registry'],
  ['verification', '/verification'],
  ['configuration', '/docs/broker/config-reference'],
];
const outputRoot = process.env.SITE_CHECK_ARTIFACTS ?? path.join(ROOT, 'artifacts/site-check');
const output = path.resolve(outputRoot, selection.suite === 'all' ? '.' : selection.suite);
const t = checker();
const measurements = [];
const errorBags = [];
const started = Date.now();
const built = (route) => path.join(DIST_DIR, route, 'index.html');
for (const route of [...(selection.site ? ROUTES : []), ...selection.toolRoutes]) {
  if (!fs.existsSync(built(route)))
    throw new Error(`Missing ${built(route)}; build the site first.`);
}
if (selection.site && !fs.existsSync(path.join(DIST_DIR, 'pagefind/pagefind.js')))
  throw new Error('Missing Pagefind index; run npm run build:site.');
fs.mkdirSync(output, { recursive: true });
const browser = await launchOrExit();
const { server, port } = await serve(DIST_DIR);
const base = `http://127.0.0.1:${port}`;

async function structure(page, name, theme) {
  const findings = await page.evaluate(() => {
    const visible = (el) =>
      el.getClientRects().length && getComputedStyle(el).visibility !== 'hidden';
    const name = (el) =>
      [
        (el.getAttribute('aria-labelledby') || '')
          .split(/\s+/)
          .map((id) => document.getElementById(id)?.textContent || '')
          .join(' '),
        el.getAttribute('aria-label'),
        [...(el.labels || [])].map((label) => label.textContent).join(' '),
        el.getAttribute('title'),
        el.matches('button, [role="button"]') ? el.textContent : '',
        el.type === 'submit' ? el.value : '',
      ].some((text) => text?.trim());
    return {
      lang: document.documentElement.lang,
      mains: document.querySelectorAll('main').length,
      h1s: document.querySelectorAll('h1').length,
      overflow: document.documentElement.scrollWidth - innerWidth,
      theme: document.documentElement.dataset.theme,
      nestedLinks: document.querySelectorAll('a a').length,
      unnamed: [
        ...document.querySelectorAll(
          'button, input:not([type="hidden"]), select, textarea, [role="button"]',
        ),
      ]
        .filter(visible)
        .filter((el) => !name(el))
        .map((el) => `${el.tagName}#${el.id}.${el.className}`),
    };
  });
  t.check(
    `${name}: English, one main, one H1`,
    findings.lang === 'en' && findings.mains === 1 && findings.h1s === 1,
    JSON.stringify(findings),
  );
  t.check(`${name}: no page overflow`, findings.overflow <= 1, `${findings.overflow}px`);
  t.check(`${name}: controls have names`, !findings.unnamed.length, findings.unnamed.join(', '));
  t.check(
    `${name}: links are not nested`,
    findings.nestedLinks === 0,
    `${findings.nestedLinks} nested links`,
  );
  t.check(`${name}: ${theme} theme`, findings.theme === theme);
}

async function timing(page, route, visit, width, theme, navigate) {
  const start = Date.now();
  await navigate();
  await page.evaluate(() => document.fonts.ready);
  await page.evaluate(
    () => new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve))),
  );
  const values = await page.evaluate(() => {
    const nav = performance.getEntriesByType('navigation')[0];
    return {
      domContentLoadedMs: nav?.domContentLoadedEventEnd,
      loadMs: nav?.loadEventEnd,
      appReadyMs: Number(document.querySelector('[data-startup-ms]')?.dataset.startupMs) || null,
    };
  });
  measurements.push({ route, visit, width, theme, elapsedMs: Date.now() - start, ...values });
}

try {
  for (const width of [375, 699, 700, 1024, 1440])
    for (const theme of ['light', 'dark']) {
      for (const route of selection.site
        ? [375, 1440].includes(width)
          ? ROUTES
          : ['/get-started/']
        : [])
        await t.flow(`${route} ${width} ${theme}`, async () => {
          const context = await browser.newContext({
            viewport: { width, height: width === 375 ? 667 : width === 1024 ? 768 : 900 },
            userAgent: 'krabka-dev-klaw',
            colorScheme: theme,
            reducedMotion: 'reduce',
          });
          try {
            const page = await context.newPage();
            errorBags.push(watchErrors(page, `${route} ${width} ${theme}`, base));
            await timing(page, route, 'cold-context', width, theme, () =>
              page.goto(base + route, { waitUntil: 'load' }),
            );
            if (route === '/search/') await page.waitForSelector('.pagefind-ui__search-input');
            await structure(page, `${route} ${width} ${theme}`, theme);
            await page.screenshot({
              path: path.join(
                output,
                `${route.replaceAll('/', '_') || 'home'}-${width}-${theme}.png`,
              ),
            });
            if (route === '/') {
              await page.keyboard.press('Tab');
              t.check(
                'first Tab reaches skip link',
                await page
                  .locator('a[href="#main"]')
                  .evaluate((el) => el === document.activeElement),
              );
              await page.keyboard.press('Enter');
              t.check(
                'skip link focuses main',
                await page.locator('#main').evaluate((el) => el === document.activeElement),
              );
              if (width === 375) {
                const menu = page.locator('#mobile-menu-button');
                await menu.focus();
                await page.keyboard.press('Enter');
                await page.keyboard.press('Tab');
                t.check(
                  'keyboard opens mobile menu and reaches a link',
                  (await menu.getAttribute('aria-expanded')) === 'true' &&
                    (await page
                      .locator('#mobile-menu')
                      .evaluate((el) => el.contains(document.activeElement))),
                );
                await page.keyboard.press('Escape');
                t.check(
                  'Escape closes menu and returns focus',
                  (await menu.getAttribute('aria-expanded')) === 'false' &&
                    (await menu.evaluate((el) => el === document.activeElement)),
                );
              }
            }
            if (route === '/get-started/') {
              const pathways = page.locator('#evaluation-pathways');
              t.check(
                'first-run cards precede ordered full-width evaluation routes',
                await pathways.evaluate((root) => {
                  const first = root.querySelector('#first-run-options');
                  const depth = root.querySelector('#evaluation-depth');
                  const sections = [...depth.querySelectorAll('section')];
                  return (
                    first.nextElementSibling === depth &&
                    first.firstElementChild.getAttribute('aria-labelledby') ===
                      'local-validation' &&
                    first.lastElementChild.getAttribute('aria-labelledby') === 'browser-trial' &&
                    JSON.stringify(
                      sections.map((section) => section.getAttribute('aria-labelledby')),
                    ) ===
                      JSON.stringify([
                        'preflight-check',
                        'performance-evaluation',
                        'staging-evaluation',
                        'explore-verification',
                      ]) &&
                    JSON.stringify(
                      sections.map((section) =>
                        section.lastElementChild.lastElementChild.getAttribute('href'),
                      ),
                    ) ===
                      JSON.stringify([
                        '/docs/broker/operations/support-matrix',
                        '/benchmarks',
                        '/docs/broker/operations/migrate-from-kafka',
                        '/verification',
                      ]) &&
                    first.querySelectorAll('h2').length === 2 &&
                    depth.querySelectorAll('h2').length === 1 &&
                    sections.every((section) => section.querySelector('h3')) &&
                    getComputedStyle(depth.querySelector('h2')).fontSize === '24px' &&
                    sections.every(
                      (section) =>
                        getComputedStyle(section.querySelector('h3')).fontSize === '20px',
                    ) &&
                    depth.querySelector('h2').firstChild.textContent.trim() ===
                      'Evaluate in depth' &&
                    sections[3].querySelector('h3').firstChild.textContent.trim() ===
                      'How Krabka is verified'
                  );
                }),
              );
              t.check(
                'preflight retains client, API, registry, tooling and revision checks',
                await page
                  .locator('#compatibility')
                  .evaluate(
                    (section) =>
                      /clients/.test(section.textContent) &&
                      /serves no JMX and loads no JVM plugins/.test(section.textContent) &&
                      /another revision do not establish support/.test(section.textContent) &&
                      [
                        '/docs/broker/kip_matrix',
                        '/docs/broker/operations/migrate-from-kafka#cut-the-schema-registry-over',
                        '/docs/broker/operations/ecosystem-support#what-does-not-work',
                      ].every((href) => section.querySelector(`a[href="${href}"]`)) &&
                      section.lastElementChild.lastElementChild.textContent ===
                        'Check the support matrix',
                  ),
              );
              t.check(
                'quickstart is primary and lab is secondary',
                await page.locator('#first-run-options').evaluate((root) => {
                  const quickstart = root.querySelector('section a:not(.heading-anchor)');
                  const lab = root.querySelector('aside a:not(.heading-anchor)');
                  return (
                    quickstart.getAttribute('href') === '/docs/quickstart' &&
                    lab.getAttribute('href') === '/docs/lab/' &&
                    getComputedStyle(quickstart).backgroundColor !==
                      getComputedStyle(lab).backgroundColor &&
                    lab.classList.contains('border')
                  );
                }),
              );
              t.check(
                'cards align buttons; evaluation rows use 2/5 headings and 3/5 prose on desktop, stacked below lg',
                await page.evaluate(() => {
                  const box = (selector) =>
                    document.querySelector(selector).getBoundingClientRect();
                  const first = box('#first-run-options');
                  const quickstart = box('#first-run-options > section');
                  const lab = box('#first-run-options > aside');
                  const depth = box('#evaluation-depth');
                  const clients = box('#clients');
                  const columns =
                    innerWidth >= 1024
                      ? Math.abs(quickstart.top - lab.top) < 1 &&
                        lab.left >= quickstart.right &&
                        Math.abs(quickstart.height - lab.height) < 1 &&
                        quickstart.width / lab.width > 1.5 &&
                        quickstart.width / lab.width < 1.6 &&
                        Math.abs(
                          box('#first-run-options > section a:not(.heading-anchor)').bottom -
                            box('#first-run-options > aside a:not(.heading-anchor)').bottom,
                        ) < 1
                      : lab.top >= quickstart.bottom && Math.abs(quickstart.width - lab.width) < 1;
                  const fullWidth = [
                    ...document.querySelectorAll('#evaluation-depth section'),
                  ].every((section) => {
                    const rect = section.getBoundingClientRect();
                    const heading = section.querySelector('h3').getBoundingClientRect();
                    const paragraph = section.querySelector('p').getBoundingClientRect();
                    const body = section.lastElementChild.getBoundingClientRect();
                    const lineHeight = parseFloat(
                      getComputedStyle(section.querySelector('h3')).lineHeight,
                    );
                    return (
                      Math.abs(rect.width - first.width) < 1 &&
                      getComputedStyle(section).borderBottomWidth === '1px' &&
                      Math.abs(paragraph.width - body.width) < 1 &&
                      (innerWidth >= 1024
                        ? Math.abs(heading.top - body.top) < 1 &&
                          Math.abs((heading.width + 24) / 2 - (body.width + 24) / 3) < 1 &&
                          heading.height <= 2 * lineHeight + 1
                        : Math.abs(heading.width - rect.width) < 1 && body.top >= heading.bottom)
                    );
                  });
                  return (
                    columns &&
                    fullWidth &&
                    depth.top >= first.bottom &&
                    clients.top >= depth.bottom &&
                    Math.abs(clients.width - first.width) < 1
                  );
                }),
              );
              t.check(
                'lab phone warning matches 699/700 breakpoint; simulation scope stays visible',
                (await page.locator('#lab-phone-limit').isVisible()) === width <= 699 &&
                  (await page.locator('#lab-scope-limit').isVisible()) &&
                  (await page.locator('#lab-scope-limit').textContent()) ===
                    'The simulation does not measure production throughput or disk durability.',
              );
              t.check(
                'local validation includes CI; combined lab caveats precede its bottom-aligned action',
                await page.locator('#first-run-options').evaluate((root) => {
                  const local = root.querySelector('section > p');
                  const caveat = root.querySelector('#lab-scope-limit').parentElement;
                  const lab = root.querySelector('aside');
                  return (
                    local.textContent ===
                      'Run a local broker with the quickstart, then produce and consume your first records. Point your existing Kafka clients, kcat, or integration tests at localhost:9092, and repeat those checks in CI as your application changes.' &&
                    caveat.querySelector('#lab-phone-limit') &&
                    caveat.textContent ===
                      'View-only on phones. The simulation does not measure production throughput or disk durability.' &&
                    caveat.nextElementSibling === lab.lastElementChild &&
                    [...root.children].every((card) =>
                      card.lastElementChild.classList.contains('mt-auto'),
                    )
                  );
                }),
              );
              const clientTabs = page.locator('#client-tabs [role="tab"]');
              t.check(
                'client examples use evaluator order with Java selected initially',
                JSON.stringify((await clientTabs.allTextContents()).map((text) => text.trim())) ===
                  JSON.stringify(['Java', 'Python', 'Go', 'Node.js', 'Rust', 'Kafka CLI']) &&
                  (await clientTabs.first().getAttribute('aria-selected')) === 'true' &&
                  (await clientTabs.first().getAttribute('tabindex')) === '0' &&
                  (await page.locator('#clients [role="tabpanel"]:visible').count()) === 1 &&
                  (await page.locator('#client-c-java').isVisible()),
              );
              t.check(
                'runnable project is visible without a disclosure interaction',
                (await page.locator('#javascript-project').isVisible()) &&
                  (await page
                    .locator('[aria-label="Copy JavaScript example setup"]')
                    .isVisible()) &&
                  (await page.locator('#clients details').count()) === 0,
              );
              for (const id of ['client-tabs']) {
                const tabs = page.locator(`#${id} [role="tab"]`);
                await tabs.first().focus();
                await page.keyboard.press('ArrowRight');
                const selected = tabs.nth(1);
                const panel = await selected.getAttribute('aria-controls');
                t.check(
                  `${id}: keyboard selects and shows second panel`,
                  (await selected.getAttribute('aria-selected')) === 'true' &&
                    (await page.locator(`#${panel}`).isVisible()),
                );
              }
            }
            if (['/docs/', '/get-started/'].includes(route))
              t.check(
                'Kubernetes guide is discoverable',
                (await page.locator('a[href$="/docs/kubernetes-quickstart"]').count()) > 0,
              );
            if (['/docs/quickstart/', '/docs/kubernetes-quickstart/'].includes(route)) {
              const switcher = page.getByRole('navigation', { name: 'Quickstart environment' });
              t.check(
                'environment switch has two plain links and one current page',
                (await switcher.locator('a').count()) === 2 &&
                  JSON.stringify(
                    (await switcher.locator('a').allTextContents()).map((text) => text.trim()),
                  ) === JSON.stringify(['Local (Docker)', 'Kubernetes']) &&
                  (await switcher.locator('a[aria-current="page"]').count()) === 1 &&
                  (await switcher.locator('a[aria-current="page"]').getAttribute('href')).replace(
                    /\/$/,
                    '',
                  ) === route.replace(/\/$/, '') &&
                  (await switcher.locator('[role="tab"]').count()) === 0,
              );
              t.check(
                'next-task cards and table of contents are present',
                (await page.locator('#choose-your-next-task + div a').count()) === 2 &&
                  (await page.locator('a[href="#choose-your-next-task"]').count()) > 0,
              );
              t.check(
                'copy controls stay right-aligned without redundant labels or outer cards',
                await page
                  .locator('main .copy-btn')
                  .evaluateAll((buttons) =>
                    buttons.every(
                      (button) =>
                        !button.parentElement.parentElement.classList.contains('frosted-card') &&
                        button.parentElement.children.length === 1 &&
                        button.parentElement.textContent.trim() === 'Copy' &&
                        getComputedStyle(button.parentElement).justifyContent === 'flex-end' &&
                        !!button.getAttribute('aria-label'),
                    ),
                  ),
              );
            }
            if (route === '/docs/quickstart/') {
              t.check(
                'local quickstart has no Helm commands',
                !(await page
                  .locator('main .copy-btn')
                  .evaluateAll((buttons) =>
                    buttons.some((button) => /helm install|kubectl run/.test(button.dataset.copy)),
                  )),
              );
            }
            if (route === '/docs/kubernetes-quickstart/') {
              t.check(
                'Kubernetes prerequisites start with the Bash terminal requirement',
                (await page.locator('#before-you-start + ul > li').first().textContent()) ===
                  'Use a Bash terminal. On Windows, use a Bash terminal that can run kubectl and helm.',
              );
              const ids = await page
                .locator('main h2[id]')
                .evaluateAll((headings) => headings.map((heading) => heading.id));
              t.check(
                'Kubernetes task order includes separate installation and cluster steps',
                JSON.stringify(ids) ===
                  JSON.stringify([
                    'before-you-start',
                    'install',
                    'create-cluster',
                    'round-trip',
                    'inspect',
                    'cleanup',
                    'choose-your-next-task',
                  ]),
              );
              await context.grantPermissions(['clipboard-read', 'clipboard-write']);
              const expected = websiteSnippets(ROOT).filter((snippet) =>
                snippet.id.startsWith('docs/kubernetes-quickstart/'),
              );
              const buttons = page.locator('main .copy-btn');
              t.check(
                'every Kubernetes snippet has a copy control',
                (await buttons.count()) === expected.length,
              );
              for (let index = 0; index < expected.length; index++) {
                const button = buttons.nth(index);
                t.check(
                  `${expected[index].id}: copy payload matches tested snippet`,
                  (await button.getAttribute('data-copy')) === expected[index].code,
                );
                await button.click();
                await waitFor(
                  page,
                  async (code) => (await navigator.clipboard.readText()) === code,
                  'clipboard update',
                  5000,
                  expected[index].code,
                );
                t.check(
                  `${expected[index].id}: copy reaches clipboard`,
                  (await page.evaluate(() => navigator.clipboard.readText())) ===
                    expected[index].code,
                );
              }
              t.check(
                'Kubernetes snippets contain no Compose commands',
                expected.every(
                  (snippet) =>
                    !/docker compose|localhost:9092|quickstart-events/.test(snippet.code),
                ),
              );
            }
            if (route === '/releases/') {
              const summaries = await page
                .locator('#release-list > li > p:last-child')
                .allTextContents();
              t.check(
                'release summaries contain text or an explicit full-notes action',
                summaries.length > 0 &&
                  summaries.every(
                    (text) =>
                      text.trim() &&
                      !/^(?:full changelog:?|(?:full changelog:\s*)?https?:\/\/\S+)$/i.test(
                        text.trim(),
                      ),
                  ),
              );
              const component = await page
                .locator('#release-component option')
                .nth(1)
                .getAttribute('value');
              await page.locator('#release-component').selectOption(component);
              const visible = page.locator('#release-list > li:visible');
              const repos = await visible.evaluateAll((rows) =>
                rows.map((row) => row.dataset.repo),
              );
              t.check(
                'release component filter and result announcement',
                repos.length > 0 &&
                  repos.every((repo) => repo === component) &&
                  (await page.locator('#release-count').textContent()) ===
                    `${repos.length} ${repos.length === 1 ? 'release' : 'releases'}`,
              );
              await page.locator('#release-component').selectOption('');
              t.check(
                'release filter restores all entries',
                (await page.locator('#release-list > li[hidden]').count()) === 0,
              );
            }
            if (route === '/search/') {
              const recovery = await page.locator('#search-recovery').evaluate((template) => ({
                text: template.content.textContent,
                retry: !!template.content.querySelector('[data-search-retry]'),
                docs: !!template.content.querySelector('a[href$="/docs"]'),
                alert: !!template.content.querySelector('[role="alert"]'),
              }));
              t.check(
                'search recovery offers retry and docs without a build command',
                recovery.retry &&
                  recovery.docs &&
                  recovery.alert &&
                  !/npm\s+run\s+build/i.test(recovery.text),
              );
            }
            await timing(page, route, 'repeat-visit', width, theme, () =>
              page.goto(base + route, { waitUntil: 'load' }),
            );
          } finally {
            await context.close();
          }
        });

      for (const route of [375, 1440].includes(width) ? selection.toolRoutes : [])
        await t.flow(`${route} startup ${width} ${theme}`, async () => {
          const context = await newLabContext(browser, { width, height: 900 });
          await context.addInitScript((value) => localStorage.setItem('theme', value), theme);
          try {
            const page = await context.newPage();
            errorBags.push(
              watchErrors(page, `${route} ${width} ${theme}`, base, {
                ignore: (_m, url) => url.endsWith('/why3-web/manifest.json'),
              }),
            );
            const open = async () => {
              if (route === '/docs/lab/') {
                await openLab(page, base);
                await waitFor(
                  page,
                  () =>
                    window.krabkaLab?.world
                      .snapshot()
                      ?.nodes.some(
                        (n) => n.kind === 'krabka-broker' && n.state?.process?.state === 'running',
                      ),
                  'a real broker running',
                  180_000,
                );
              } else {
                await page.goto(base + route, { waitUntil: 'load' });
                await page.waitForSelector('#krabka-proofs[data-ready="true"]');
              }
            };
            await timing(page, route, 'cold-context', width, theme, open);
            await structure(page, `${route} ${width} ${theme}`, theme);
            if (route === '/docs/lab/') {
              await page.locator('#krabka-lab .lab-node').first().focus();
              await page.keyboard.press('Enter');
              if (
                !(await page
                  .getByRole('button', { name: 'Back to canvas', exact: true })
                  .isVisible())
              )
                await page.getByRole('button', { name: 'Show the inspector', exact: true }).click();
              await page.getByRole('button', { name: 'Back to canvas', exact: true }).click();
              t.check(
                'Back to canvas returns keyboard focus',
                await page.evaluate(() =>
                  document.activeElement?.matches('#krabka-lab .lab-node, #krabka-lab .lab-canvas'),
                ),
              );
            }
            await page.screenshot({
              path: path.join(output, `${route.replaceAll('/', '_')}-${width}-${theme}.png`),
            });
            await timing(page, route, 'repeat-visit', width, theme, open);
          } finally {
            await context.close();
          }
        });
    }
  if (selection.site)
    await t.flow('ten maintained search queries', async () => {
      const page = await browser.newPage();
      errorBags.push(watchErrors(page, 'search queries', base));
      await page.goto(`${base}/search/?q=backup`, { waitUntil: 'load' });
      await page.waitForSelector('.pagefind-ui__result-link');
      for (const [query, expected] of SEARCH) {
        const urls = await page.evaluate(async (query) => {
          const pagefind = await import('/pagefind/pagefind.js');
          const result = await pagefind.search(query);
          return Promise.all(
            result.results
              .slice(0, 10)
              .map(async (item) => new URL((await item.data()).url, location.href).pathname),
          );
        }, query);
        t.check(
          `search "${query}" finds ${expected}`,
          urls.some((url) => url.replace(/\/$/, '') === expected),
          urls.join(', '),
        );
      }
      await page.close();
    });
} finally {
  const errors = errorBags.flat();
  t.check('no page or local console errors', !errors.length, errors.slice(0, 5).join(' | '));
  fs.writeFileSync(
    path.join(output, 'measurements.json'),
    JSON.stringify(
      {
        suite: selection.suite,
        measuredAt: new Date().toISOString(),
        runtime: {
          node: process.version,
          platform: process.platform,
          browser: browser.version(),
          deviceScaleFactor: 1,
          height: 900,
          samplesPerVisit: 1,
        },
        profile:
          'Loopback static server; no throttling; reduced motion; fresh context then repeat visit. Server sends no-store; browser compilation/storage may be warm. Not field Web Vitals.',
        measurements,
        failures: t.failures,
        errors,
      },
      null,
      2,
    ) + '\n',
  );
  await browser.close();
  server.close();
}
t.finish(`PASS: site checks; screenshots and controlled timings in ${output}`, started);
