import assert from 'node:assert/strict';
import fs from 'node:fs';
import { test } from 'node:test';
import { load } from 'js-yaml';

const workflow = load(fs.readFileSync(new URL('../.github/workflows/deploy.yml', import.meta.url), 'utf8'));
const manifest = JSON.parse(fs.readFileSync(new URL('../package.json', import.meta.url), 'utf8'));
const mainOnly = "github.ref == 'refs/heads/main'";

// This is a contract for the current workflow, not a general shell analyzer.
// Keep guarded commands explicit. Review this contract when their shape changes.
function blocking(value, label, condition) {
  assert.ok(value, `${label} exists`);
  assert.ok(value['continue-on-error'] === undefined || value['continue-on-error'] === false,
    `${label} cannot ignore failure`);
  assert.equal(value.if, condition, `${label} condition`);
}

function command(job, name, run, condition) {
  const matches = job.steps.filter((step) => step.name === name);
  assert.equal(matches.length, 1, `${name} occurs once`);
  const step = matches[0];
  blocking(step, name, condition);
  assert.equal(step.run.trim(), run, `${name} uses blocking commands`);
  assert.equal(step.shell, undefined, `${name} uses the runner's fail-fast shell`);
  assert.equal(step['working-directory'], undefined, `${name} runs from the repository root`);
  return job.steps.indexOf(step);
}

function contract(doc, pkg) {
  const { build, deploy, 'production-smoke': smoke } = doc.jobs;
  blocking(build, 'build');
  blocking(deploy, 'deploy', mainOnly);
  blocking(smoke, 'production-smoke', mainOnly);
  for (const job of [doc, build, deploy, smoke]) {
    assert.equal(job.defaults?.run, undefined, 'guarded jobs use runner shell and working-directory defaults');
  }
  assert.deepEqual(doc.on.pull_request.branches, ['main'], 'pull requests run the required build');
  assert.deepEqual(doc.on.push.branches, ['main'], 'main pushes run the release pipeline');
  assert.equal(deploy.needs, 'build', 'deploy needs a successful build');
  assert.equal(smoke.needs, 'deploy', 'production smoke needs a successful deployment');

  const tests = command(build, 'Check website validation and sync guards', 'npm run test:website');
  const compile = command(build, 'Build static website', 'npm run build');
  const checks = command(build, 'Check built links, SEO and page budgets',
    'npm run check-links\nnpm run check-seo\nnpm run check-page-budgets');
  const uploads = build.steps.filter((step) => step.uses?.startsWith('actions/upload-pages-artifact@'));
  assert.equal(uploads.length, 1, 'one Pages artifact upload');
  blocking(uploads[0], 'Pages upload', mainOnly);
  assert.equal(uploads[0].with.path, './dist', 'upload the validated build directory');
  assert.ok(tests < compile && compile < checks && checks < build.steps.indexOf(uploads[0]),
    'regressions and build checks run before the Pages upload');

  const publish = deploy.steps.filter((step) => step.uses?.startsWith('actions/deploy-pages@'));
  assert.equal(publish.length, 1, 'one Pages deployment');
  blocking(publish[0], 'Pages deployment');
  const download = smoke.steps.find((step) => step.uses?.startsWith('actions/download-artifact@'));
  blocking(download, 'release artifact download');
  assert.equal(download.with.name, 'github-pages');
  assert.equal(download.with.path, 'deployed-pages');
  const inventory = command(smoke, 'Read the release URL inventory from the Pages artifact',
    'mkdir -p deployed-inventory\ntar -xf deployed-pages/artifact.tar -C deployed-inventory ./sitemap-inventory.json');
  const live = command(smoke, 'Check published pages, essential assets and component APIs',
    'node scripts/check-production.mjs https://krabka.io deployed-inventory/sitemap-inventory.json\nnode scripts/check-api-links.mjs');
  assert.ok(smoke.steps.indexOf(download) < inventory && inventory < live,
    'smoke compares production with the deployed release inventory');

  for (const [name, run] of Object.entries({
    'check-links': 'node scripts/check-links.mjs',
    'check-seo': 'node scripts/check-seo.mjs',
    'check-page-budgets': 'node scripts/check-page-budgets.mjs',
  })) assert.equal(pkg.scripts[name], run, `${name} runs its validator`);
  assert.equal(pkg.scripts.build,
    'npm run prepare:benchmark-archive && npm run build:playground && node scripts/sync-docs.mjs && node --experimental-strip-types scripts/sync-proofs.mjs && astro build && npm run check-sitemap && pagefind --site dist',
    'release build retains blocking build and sitemap commands');
  const [testCommand, syncCommand, ...extra] = pkg.scripts['test:website'].split(' && ');
  assert.equal(extra.length, 0, 'website test chain has no extra commands');
  assert.equal(syncCommand, 'node scripts/check-sync-links.mjs');
  const args = testCommand.split(' ');
  assert.deepEqual(args.slice(0, 3), ['node', '--experimental-strip-types', '--test']);
  assert.ok(args.slice(3).every((arg) => /^scripts\/[\w.-]+\.test\.mjs$/.test(arg)),
    'website tests contain only test file arguments');
  assert.ok(args.slice(3).includes('scripts/deployment-contract.test.mjs'),
    'website tests execute this contract');
}

test('deployment preserves blocking build, validation, upload and production smoke order', () => {
  contract(workflow, manifest);
});

const step = (doc, name, job = 'build') => doc.jobs[job].steps.find((entry) => entry.name === name);
const validation = 'Check built links, SEO and page budgets';
const mutations = {
  'missing validation': (doc) => { doc.jobs.build.steps = doc.jobs.build.steps.filter((entry) => entry.name !== validation); },
  'missing link check': (doc) => { step(doc, validation).run = 'npm run check-seo\nnpm run check-page-budgets'; },
  'missing SEO check': (doc) => { step(doc, validation).run = 'npm run check-links\nnpm run check-page-budgets'; },
  'ignored validation failure': (doc) => { step(doc, validation)['continue-on-error'] = true; },
  'expression ignores failure': (doc) => { step(doc, validation)['continue-on-error'] = '${{ true }}'; },
  'conditional validation': (doc) => { step(doc, validation).if = 'false'; },
  'shell swallows failure': (doc) => { step(doc, validation).run += ' || true'; },
  'disabled shell failure exit': (doc) => { step(doc, validation).run = 'set +e\n' + step(doc, validation).run; },
  'custom shell': (doc) => { step(doc, validation).shell = 'bash {0}'; },
  'changed validation directory': (doc) => { step(doc, validation)['working-directory'] = 'other'; },
  'custom workflow shell': (doc) => { doc.defaults = { run: { shell: 'bash {0}' } }; },
  'custom job shell': (doc) => { doc.jobs.build.defaults = { run: { shell: 'bash {0}' } }; },
  'nonblocking build job': (doc) => { doc.jobs.build['continue-on-error'] = true; },
  'skipped build job': (doc) => { doc.jobs.build.if = 'false'; },
  'nonblocking static build': (doc) => { step(doc, 'Build static website')['continue-on-error'] = true; },
  'missing regression execution': (doc) => { step(doc, 'Check website validation and sync guards').run = 'true'; },
  'upload before validation': (doc) => {
    const steps = doc.jobs.build.steps;
    const index = steps.findIndex((entry) => entry.uses?.startsWith('actions/upload-pages-artifact@'));
    steps.unshift(steps.splice(index, 1)[0]);
  },
  'validation before build': (doc) => {
    const steps = doc.jobs.build.steps;
    steps.unshift(steps.splice(steps.indexOf(step(doc, validation)), 1)[0]);
  },
  'upload after failure': (doc) => { doc.jobs.build.steps.at(-1).if = 'always()'; },
  'wrong artifact directory': (doc) => { doc.jobs.build.steps.at(-1).with.path = './unvalidated'; },
  'missing deploy dependency': (doc) => { delete doc.jobs.deploy.needs; },
  'deploy after failed build': (doc) => { doc.jobs.deploy.if = `always() && ${mainOnly}`; },
  'deploy from pull request': (doc) => { delete doc.jobs.deploy.if; },
  'skipped deploy action': (doc) => { step(doc, 'Deploy to GitHub Pages', 'deploy').if = 'false'; },
  'missing smoke dependency': (doc) => { delete doc.jobs['production-smoke'].needs; },
  'skipped production smoke': (doc) => { doc.jobs['production-smoke'].if = 'false'; },
  'nonblocking production smoke': (doc) => { doc.jobs['production-smoke']['continue-on-error'] = true; },
  'smoke without release inventory': (doc) => {
    step(doc, 'Check published pages, essential assets and component APIs', 'production-smoke').run =
      'node scripts/check-production.mjs https://krabka.io\nnode scripts/check-api-links.mjs';
  },
  'missing pull request trigger': (doc) => { delete doc.on.pull_request; },
};

for (const [name, mutate] of Object.entries(mutations)) {
  test(`deployment contract rejects ${name}`, () => {
    const doc = structuredClone(workflow);
    mutate(doc);
    assert.throws(() => contract(doc, manifest));
  });
}

for (const name of ['build', 'check-links', 'check-seo', 'check-page-budgets', 'test:website']) {
  test(`deployment contract rejects disabled ${name} script`, () => {
    const pkg = structuredClone(manifest);
    pkg.scripts[name] = 'true';
    assert.throws(() => contract(workflow, pkg));
  });
}

for (const run of ['echo scripts/deployment-contract.test.mjs', manifest.scripts['test:website'] + ' || true']) {
  test(`deployment contract rejects ineffective test command: ${run}`, () => {
    const pkg = structuredClone(manifest);
    pkg.scripts['test:website'] = run;
    assert.throws(() => contract(workflow, pkg));
  });
}
