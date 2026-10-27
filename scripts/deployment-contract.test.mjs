import assert from 'node:assert/strict';
import fs from 'node:fs';
import { test } from 'node:test';
import { load } from 'js-yaml';

const workflow = load(
  fs.readFileSync(new URL('../.github/workflows/deploy.yml', import.meta.url), 'utf8'),
);
const manifest = JSON.parse(fs.readFileSync(new URL('../package.json', import.meta.url), 'utf8'));
const lab = "steps.changes.outputs.lab == 'true'";
const proofs = "steps.changes.outputs.proofs == 'true'";
const mainOnly = "github.ref == 'refs/heads/main'";
const mainPush = "github.event_name == 'push' && github.ref == 'refs/heads/main'";
const buildCondition = "${{ !cancelled() && needs.promote.outputs.promoted != 'true' }}";
const deployCondition =
  "${{ always() && github.ref == 'refs/heads/main' && (needs.build.result == 'success' || (needs.build.result == 'skipped' && needs.promote.outputs.promoted == 'true')) }}";
const normalize = (value) => value?.replace(/\s+/g, ' ').trim();

// Evaluate this workflow's job expressions with GitHub's implicit success gate.
// Expressions come only from the checked-in workflow and test mutations.
function runs(
  expression,
  {
    event = 'push',
    ref = 'refs/heads/main',
    promoted = '',
    promote = 'success',
    build = 'success',
    cancelled = false,
  } = {},
) {
  const source = (expression || 'true').replace(/^\$\{\{\s*|\s*\}\}$/g, '');
  if (
    !/\b(always|cancelled|success|failure)\(/.test(source) &&
    (promote !== 'success' || build !== 'success')
  )
    return false;
  const values = {
    'github.event_name': event,
    'github.ref': ref,
    'needs.promote.outputs.promoted': promoted,
    'needs.promote.result': promote,
    'needs.build.result': build,
  };
  let js = source;
  for (const [key, value] of Object.entries(values)) js = js.replaceAll(key, JSON.stringify(value));
  js = js.replaceAll('always()', 'true').replaceAll('cancelled()', String(cancelled));
  return Boolean(Function(`"use strict"; return (${js});`)());
}

function gateScenarios(doc) {
  for (const event of [
    'pull_request',
    'merge_group',
    'schedule',
    'workflow_dispatch',
    'repository_dispatch',
    'push',
  ]) {
    for (const promote of ['success', 'failure', 'skipped', 'cancelled']) {
      for (const promoted of ['', 'false']) {
        assert.equal(
          runs(doc.jobs.build.if, { event, promote, promoted }),
          true,
          `${event}: ${promote} with ${promoted} cannot skip build`,
        );
        assert.equal(
          runs(doc.jobs.build.if, { event, promote, promoted, cancelled: true }),
          false,
          'cancelled workflow does not build',
        );
      }
    }
  }
  assert.equal(
    runs(doc.jobs.build.if, { promoted: 'true' }),
    false,
    'completed promotion skips rebuild',
  );
  for (const build of ['success', 'failure', 'cancelled', 'skipped']) {
    for (const promoted of ['', 'false', 'true']) {
      assert.equal(
        runs(doc.jobs.deploy.if, { build, promoted }),
        build === 'success' || (build === 'skipped' && promoted === 'true'),
        'only validated bytes can deploy',
      );
      assert.equal(
        runs(doc.jobs.deploy.if, { build, promoted, ref: 'refs/pull/1/merge' }),
        false,
        'PR cannot deploy',
      );
    }
  }
}

// This is a contract for the current workflow, not a general shell analyzer.
// Keep guarded commands explicit. Review this contract when their shape changes.
function blocking(value, label, condition) {
  assert.ok(value, `${label} exists`);
  assert.ok(
    value['continue-on-error'] === undefined || value['continue-on-error'] === false,
    `${label} cannot ignore failure`,
  );
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
  const { promote, build, deploy, 'production-smoke': smoke } = doc.jobs;
  gateScenarios(doc);
  blocking(promote, 'promote');
  assert.deepEqual(promote.permissions, { contents: 'read', actions: 'read' });
  assert.equal(promote.outputs.promoted, '${{ steps.result.outputs.promoted }}');
  assert.deepEqual(doc.permissions, { contents: 'read' });
  assert.deepEqual(deploy.permissions, { pages: 'write', 'id-token': 'write' });
  assert.equal(build.permissions, undefined, 'build inherits read-only permissions');
  blocking(build, 'build', buildCondition);
  assert.equal(normalize(deploy.if), deployCondition);
  blocking(deploy, 'deploy', deploy.if);
  blocking(smoke, 'production-smoke', mainOnly);
  for (const job of [doc, build, deploy, smoke]) {
    assert.equal(
      job.defaults?.run,
      undefined,
      'guarded jobs use runner shell and working-directory defaults',
    );
  }
  assert.deepEqual(doc.on.pull_request.branches, ['main'], 'pull requests run the required build');
  assert.deepEqual(doc.on.push.branches, ['main'], 'main pushes run the release pipeline');
  assert.deepEqual(deploy.needs, ['promote', 'build']);
  assert.equal(smoke.needs, 'deploy', 'production smoke needs a successful deployment');

  const tests = command(build, 'Check website validation and sync guards', 'npm run test:website');
  const selection = command(build, 'Select CI checks', 'node scripts/ci-changes.mjs');
  assert.equal(build.steps[selection].id, 'changes');
  assert.ok(selection < tests, 'selection precedes validation');
  const compile = command(
    build,
    'Build static website',
    'npm run sync-docs && npm run sync-proofs && npm run build:site',
  );
  const checks = command(
    build,
    'Check built links, SEO and page budgets',
    'npm run check-links\nnpm run check-seo\nnpm run check-page-budgets -- --wasm=${{ steps.changes.outputs.lab }}',
  );
  assert.deepEqual(doc.on.pull_request.types, [
    'opened',
    'synchronize',
    'reopened',
    'ready_for_review',
    'converted_to_draft',
  ]);
  for (const trigger of [doc.on.pull_request, doc.on.push]) {
    for (const key of ['paths', 'paths-ignore', 'branches-ignore'])
      assert.equal(trigger[key], undefined, 'no required-check path filters');
  }
  assert.deepEqual(doc.on.schedule, [{ cron: '0 4 * * *' }]);
  assert.deepEqual(doc.on.repository_dispatch.types, ['release-published']);
  assert.ok(Object.hasOwn(doc.on, 'workflow_dispatch'));
  assert.deepEqual(
    doc.on.merge_group.branches,
    ['main'],
    'merge-queue commits run the required build',
  );
  assert.deepEqual(
    doc.concurrency,
    {
      group:
        "${{ github.event_name == 'pull_request' && format('pages-pr-{0}', github.event.pull_request.number) || github.event_name == 'merge_group' && format('pages-merge-group-{0}', github.ref) || 'pages-production' }}",
      'cancel-in-progress':
        "${{ github.event_name == 'pull_request' || github.event_name == 'merge_group' }}",
    },
    'PRs and merge-queue entries cancel only their own superseded runs; production runs stay serialized through smoke',
  );
  const suites = [
    ['Build the real broker for the Cluster Lab', 'npm run build:broker', lab],
    ['Build the consensus playground', 'npm run build:playground', lab],
    ['Check the proof workers', 'npm run check-proof-workers', proofs],
    [
      'Build the Why3 web bundle',
      'npm run build:why3-web',
      `${proofs} && steps.why3-web.outputs.cache-hit != 'true'`,
    ],
    ['Check the Why3 web bundle is in place', 'test -s public/why3-web/manifest.json', proofs],
    ['Install Chromium', 'npx playwright install --with-deps chromium', undefined],
    [
      'Check general website browsers',
      'npm run check-site -- --suite=site\nnpm run check:benchmark-viewer\nnpm run check-component-docs',
      undefined,
    ],
    [
      'Check proof startup and replay with Z3 and cvc5',
      'npm run check-site -- --suite=proofs\nnpm run check-proof-smt',
      proofs,
    ],
    ['Check Cluster Lab browser startup', 'npm run check-site -- --suite=lab', lab],
    [
      "Check the Cluster Lab's preset experiments on real brokers",
      'npm run check-lab-experiments',
      lab,
    ],
    [
      'Check browser SSPI encryption on real brokers',
      'npm run check-lab-clusters -- --preset=sspi-encrypted',
      lab,
    ],
    [
      'Check rebalancing and share consumers on real brokers',
      'npm run check-lab-clusters -- --preset=rebalancer --repeat=2\nnpm run check-lab-clusters -- --preset=share-group --repeat=2\nnpm run check-lab-clusters -- --preset=share-redelivery --repeat=2',
      lab,
    ],
    [
      'Check broker ACL identities and allow/deny examples',
      'npm run check-lab-acls\nnpm run check-lab-clusters -- --preset=broker-acls\nnpm run check-lab-clusters -- --preset=broker-acls-deny',
      lab,
    ],
    ['Test snippet extraction and coverage', 'npm run test:snippets', undefined],
    [
      'Check benchmark data and archive transformations',
      'npm run prepare:benchmark-archive && npm run test:benchmark',
      undefined,
    ],
    ['Check the verification catalog still parses', 'npm run check-catalog', undefined],
    [
      "Check the proof explorer's highlighter and summaries",
      'npm run check-proof-highlight && npm run check-proof-readability',
      undefined,
    ],
    [
      "Check the Cluster Lab's network and storage analyzers",
      'npm run check-lab-analyzer',
      undefined,
    ],
    [
      "Check the Cluster Lab's invariants, charts and record trace",
      'npm run check-lab-observe',
      undefined,
    ],
  ];
  for (const [name, run, condition] of suites) {
    const index = command(build, name, run, condition);
    assert.ok(index > selection, `${name} follows selection`);
    if (name.startsWith('Build ') || name === 'Check the proof workers')
      assert.ok(index < compile, `${name} precedes static build`);
    if (name.includes('startup') || name === 'Check general website browsers')
      assert.ok(index > compile, `${name} uses built site`);
  }
  for (const [name, condition] of [
    ['Setup Rust with the wasm32 targets', lab],
    ['Cache the Rust builds', lab],
    ['Cache the Why3 web bundle', proofs],
  ])
    blocking(
      build.steps.find((step) => step.name === name),
      name,
      condition,
    );
  // A selected failure must fail build. Omitted suites use step-level skips,
  // not skipped dependency jobs that can accidentally suppress the merge gate.
  for (const step of build.steps)
    assert.ok(step['continue-on-error'] === undefined || step['continue-on-error'] === false);
  assert.equal(build.needs, 'promote');
  assert.equal(build.name, undefined, 'required build check keeps its name');
  const uploads = build.steps.filter((step) =>
    step.uses?.startsWith('actions/upload-pages-artifact@'),
  );
  assert.equal(uploads.length, 1, 'one Pages artifact upload');
  blocking(uploads[0], 'Pages upload', mainOnly);
  assert.equal(uploads[0].with.path, './dist', 'upload the validated build directory');
  assert.ok(
    tests < compile && compile < checks && checks < build.steps.indexOf(uploads[0]),
    'regressions and build checks run before the Pages upload',
  );

  for (const name of [
    'Checkout promotion helpers',
    'Setup promotion Node.js',
    'Find validated exact-commit queue site',
  ]) {
    blocking(
      promote.steps.find((step) => step.name === name),
      name,
      mainPush,
    );
  }
  command(
    promote,
    'Find validated exact-commit queue site',
    'node scripts/pages-promotion.mjs',
    mainPush,
  );
  const downloadQueue = promote.steps.find((step) => step.id === 'download');
  assert.equal(
    downloadQueue.uses,
    'actions/download-artifact@9000827ccba6bdab643e8b6fd33ac0654aef8333',
  );
  assert.deepEqual(downloadQueue.with, {
    'artifact-ids': '${{ steps.candidate.outputs.artifact-id }}',
    'run-id': '${{ steps.candidate.outputs.run-id }}',
    'github-token': '${{ github.token }}',
    'digest-mismatch': 'error',
    'merge-multiple': true,
    path: '${{ runner.temp }}/queue-download',
  });
  assert.equal(downloadQueue.if, "steps.candidate.outputs.found == 'true'");
  const unpack = promote.steps.find((step) => step.id === 'unpack');
  assert.equal(unpack.if, "steps.download.outcome == 'success'");
  assert.equal(
    unpack.run,
    'python3 scripts/unpack-queue-site.py "${RUNNER_TEMP}/queue-download/site.tar" "${RUNNER_TEMP}/queue-site"',
  );
  const promotedUpload = promote.steps.find((step) => step.id === 'upload');
  assert.equal(promotedUpload.if, "steps.unpack.outcome == 'success'");
  assert.equal(promotedUpload.uses, uploads[0].uses);
  assert.equal(promotedUpload.with.path, '${{ runner.temp }}/queue-site');
  const result = promote.steps.find((step) => step.id === 'result');
  blocking(result, 'promotion result', 'always()');
  assert.equal(result.env.UPLOAD_OUTCOME, '${{ steps.upload.outcome }}');
  assert.match(
    result.run,
    /if \[\[ "\$\{UPLOAD_OUTCOME\}" == success \]\]; then\n *echo 'promoted=true'/,
  );
  assert.match(result.run, /else\n *echo 'promoted=false'/);
  const freeze = command(
    build,
    'Freeze validated queue site',
    'mkdir -p "${RUNNER_TEMP}/validated-queue-site"\ntar -cf "${RUNNER_TEMP}/validated-queue-site/site.tar" -C dist .',
    "github.event_name == 'merge_group'",
  );
  const queueUpload = build.steps.find((step) => step.name === 'Upload validated queue site');
  blocking(queueUpload, 'queue upload', "github.event_name == 'merge_group'");
  assert.equal(
    queueUpload.uses,
    'actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02',
  );
  assert.deepEqual(queueUpload.with, {
    name: 'validated-queue-site',
    path: '${{ runner.temp }}/validated-queue-site/site.tar',
    'if-no-files-found': 'error',
    'retention-days': 1,
  });
  assert.ok(freeze < build.steps.indexOf(queueUpload));
  for (const [name] of suites)
    assert.ok(
      build.steps.findIndex((step) => step.name === name) < freeze,
      `${name} precedes queue freeze`,
    );

  const publish = deploy.steps.filter((step) => step.uses?.startsWith('actions/deploy-pages@'));
  assert.equal(publish.length, 1, 'one Pages deployment');
  blocking(publish[0], 'Pages deployment');
  const download = smoke.steps.find((step) => step.uses?.startsWith('actions/download-artifact@'));
  blocking(download, 'release artifact download');
  assert.equal(download.with.name, 'github-pages');
  assert.equal(download.with.path, 'deployed-pages');
  const inventory = command(
    smoke,
    'Read the release URL inventory from the Pages artifact',
    'mkdir -p deployed-inventory\ntar -xf deployed-pages/artifact.tar -C deployed-inventory ./sitemap-inventory.json',
  );
  const live = command(
    smoke,
    'Check published pages, essential assets and component APIs',
    'node scripts/check-production.mjs https://krabka.io deployed-inventory/sitemap-inventory.json\nnode scripts/check-api-links.mjs',
  );
  assert.ok(
    smoke.steps.indexOf(download) < inventory && inventory < live,
    'smoke compares production with the deployed release inventory',
  );

  for (const [name, run] of Object.entries({
    'check-links': 'node scripts/check-links.mjs',
    'check-seo': 'node scripts/check-seo.mjs',
    'check-page-budgets': 'node scripts/check-page-budgets.mjs',
  }))
    assert.equal(pkg.scripts[name], run, `${name} runs its validator`);
  assert.equal(
    pkg.scripts.build,
    'npm run prepare:benchmark-archive && npm run build:playground && node scripts/sync-docs.mjs && node --experimental-strip-types scripts/sync-proofs.mjs && astro build && npm run check-sitemap && pagefind --site dist',
    'release build retains blocking build and sitemap commands',
  );
  assert.equal(
    pkg.scripts['build:site'],
    'npm run prepare:benchmark-archive && astro build && npm run check-sitemap && pagefind --site dist',
  );
  assert.equal(pkg.scripts['sync-docs'], 'node scripts/sync-docs.mjs');
  assert.equal(
    pkg.scripts['sync-proofs'],
    'node --experimental-strip-types scripts/sync-proofs.mjs',
  );
  const [testCommand, syncCommand, ...extra] = pkg.scripts['test:website'].split(' && ');
  assert.equal(extra.length, 0, 'website test chain has no extra commands');
  assert.equal(syncCommand, 'node scripts/check-sync-links.mjs');
  const args = testCommand.split(' ');
  assert.deepEqual(args.slice(0, 3), ['node', '--experimental-strip-types', '--test']);
  assert.ok(
    args.slice(3).every((arg) => /^scripts\/[\w.-]+\.test\.mjs$/.test(arg)),
    'website tests contain only test file arguments',
  );
  assert.ok(
    args.slice(3).includes('scripts/deployment-contract.test.mjs'),
    'website tests execute this contract',
  );
}

test('deployment preserves blocking build, validation, upload and production smoke order', () => {
  contract(workflow, manifest);
});

const step = (doc, name, job = 'build') => doc.jobs[job].steps.find((entry) => entry.name === name);
const validation = 'Check built links, SEO and page budgets';
const mutations = {
  'main-only promote job': (doc) => {
    doc.jobs.promote.if = mainPush;
  },
  'PR build skipped': (doc) => {
    doc.jobs.build.if = "${{ !cancelled() && github.event_name != 'pull_request' }}";
  },
  'merge-group build skipped': (doc) => {
    doc.jobs.build.if = "${{ !cancelled() && github.event_name != 'merge_group' }}";
  },
  'promote failure skips build': (doc) => {
    doc.jobs.build.if = "${{ needs.promote.outputs.promoted != 'true' }}";
  },
  'empty promote output skips build': (doc) => {
    doc.jobs.build.if = "${{ !cancelled() && needs.promote.outputs.promoted == 'false' }}";
  },
  'missing digest policy': (doc) => {
    delete doc.jobs.promote.steps.find((step) => step.id === 'download').with['digest-mismatch'];
  },
  'weak digest policy': (doc) => {
    doc.jobs.promote.steps.find((step) => step.id === 'download').with['digest-mismatch'] = 'warn';
  },
  'missing immutable artifact ID': (doc) => {
    delete doc.jobs.promote.steps.find((step) => step.id === 'download').with['artifact-ids'];
  },
  'queue uploaded from PR': (doc) => {
    step(doc, 'Upload validated queue site').if = "github.event_name == 'pull_request'";
  },
  'queue retention expanded': (doc) => {
    step(doc, 'Upload validated queue site').with['retention-days'] = 14;
  },
  'promote without unpack': (doc) => {
    doc.jobs.promote.steps.find((step) => step.id === 'upload').if = 'always()';
  },
  'workflow Pages write': (doc) => {
    doc.permissions.pages = 'write';
  },
  'build OIDC write': (doc) => {
    doc.jobs.build.permissions = { 'id-token': 'write' };
  },
  'missing deploy OIDC': (doc) => {
    delete doc.jobs.deploy.permissions['id-token'];
  },
  'missing ready event': (doc) => {
    doc.on.pull_request.types = ['opened'];
  },
  'required-check path filtering': (doc) => {
    doc.on.pull_request.paths = ['src/**'];
  },
  'production cancellation': (doc) => {
    doc.concurrency['cancel-in-progress'] = true;
  },
  'shared PR queue': (doc) => {
    doc.concurrency.group = 'pages';
  },
  'missing classifier': (doc) => {
    step(doc, 'Select CI checks').run = 'true';
  },
  'lost sync before site build': (doc) => {
    step(doc, 'Build static website').run = 'npm run build:site';
  },
  'lab runs unconditionally': (doc) => {
    delete step(doc, 'Build the real broker for the Cluster Lab').if;
  },
  'proof failures ignored': (doc) => {
    step(doc, 'Check proof startup and replay with Z3 and cvc5')['continue-on-error'] = true;
  },
  'general browser accidentally starts all tools': (doc) => {
    step(doc, 'Check general website browsers').run = 'npm run check-site';
  },
  'missing validation': (doc) => {
    doc.jobs.build.steps = doc.jobs.build.steps.filter((entry) => entry.name !== validation);
  },
  'missing link check': (doc) => {
    step(doc, validation).run = 'npm run check-seo\nnpm run check-page-budgets';
  },
  'missing SEO check': (doc) => {
    step(doc, validation).run = 'npm run check-links\nnpm run check-page-budgets';
  },
  'ignored validation failure': (doc) => {
    step(doc, validation)['continue-on-error'] = true;
  },
  'expression ignores failure': (doc) => {
    step(doc, validation)['continue-on-error'] = '${{ true }}';
  },
  'conditional validation': (doc) => {
    step(doc, validation).if = 'false';
  },
  'shell swallows failure': (doc) => {
    step(doc, validation).run += ' || true';
  },
  'disabled shell failure exit': (doc) => {
    step(doc, validation).run = 'set +e\n' + step(doc, validation).run;
  },
  'custom shell': (doc) => {
    step(doc, validation).shell = 'bash {0}';
  },
  'changed validation directory': (doc) => {
    step(doc, validation)['working-directory'] = 'other';
  },
  'custom workflow shell': (doc) => {
    doc.defaults = { run: { shell: 'bash {0}' } };
  },
  'custom job shell': (doc) => {
    doc.jobs.build.defaults = { run: { shell: 'bash {0}' } };
  },
  'nonblocking build job': (doc) => {
    doc.jobs.build['continue-on-error'] = true;
  },
  'skipped build job': (doc) => {
    doc.jobs.build.if = 'false';
  },
  'nonblocking static build': (doc) => {
    step(doc, 'Build static website')['continue-on-error'] = true;
  },
  'missing regression execution': (doc) => {
    step(doc, 'Check website validation and sync guards').run = 'true';
  },
  'upload before validation': (doc) => {
    const steps = doc.jobs.build.steps;
    const index = steps.findIndex((entry) =>
      entry.uses?.startsWith('actions/upload-pages-artifact@'),
    );
    steps.unshift(steps.splice(index, 1)[0]);
  },
  'validation before build': (doc) => {
    const steps = doc.jobs.build.steps;
    steps.unshift(steps.splice(steps.indexOf(step(doc, validation)), 1)[0]);
  },
  'upload after failure': (doc) => {
    doc.jobs.build.steps.at(-1).if = 'always()';
  },
  'wrong artifact directory': (doc) => {
    doc.jobs.build.steps.at(-1).with.path = './unvalidated';
  },
  'missing deploy dependency': (doc) => {
    delete doc.jobs.deploy.needs;
  },
  'deploy after failed build': (doc) => {
    doc.jobs.deploy.if = `always() && ${mainOnly}`;
  },
  'deploy from pull request': (doc) => {
    delete doc.jobs.deploy.if;
  },
  'skipped deploy action': (doc) => {
    step(doc, 'Deploy to GitHub Pages', 'deploy').if = 'false';
  },
  'missing smoke dependency': (doc) => {
    delete doc.jobs['production-smoke'].needs;
  },
  'skipped production smoke': (doc) => {
    doc.jobs['production-smoke'].if = 'false';
  },
  'nonblocking production smoke': (doc) => {
    doc.jobs['production-smoke']['continue-on-error'] = true;
  },
  'smoke without release inventory': (doc) => {
    step(
      doc,
      'Check published pages, essential assets and component APIs',
      'production-smoke',
    ).run = 'node scripts/check-production.mjs https://krabka.io\nnode scripts/check-api-links.mjs';
  },
  'missing pull request trigger': (doc) => {
    delete doc.on.pull_request;
  },
  'missing merge group trigger': (doc) => {
    delete doc.on.merge_group;
  },
  'merge group cancels production': (doc) => {
    doc.concurrency.group =
      "${{ github.event_name == 'pull_request' && format('pages-pr-{0}', github.event.pull_request.number) || 'pages-production' }}";
  },
};

for (const [name, mutate] of Object.entries(mutations)) {
  test(`deployment contract rejects ${name}`, () => {
    const doc = structuredClone(workflow);
    mutate(doc);
    assert.throws(() => contract(doc, manifest));
  });
}

for (const name of [
  'build',
  'build:site',
  'sync-docs',
  'sync-proofs',
  'check-links',
  'check-seo',
  'check-page-budgets',
  'test:website',
]) {
  test(`deployment contract rejects disabled ${name} script`, () => {
    const pkg = structuredClone(manifest);
    pkg.scripts[name] = 'true';
    assert.throws(() => contract(workflow, pkg));
  });
}

for (const run of [
  'echo scripts/deployment-contract.test.mjs',
  manifest.scripts['test:website'] + ' || true',
]) {
  test(`deployment contract rejects ineffective test command: ${run}`, () => {
    const pkg = structuredClone(manifest);
    pkg.scripts['test:website'] = run;
    assert.throws(() => contract(workflow, pkg));
  });
}

// Skipped suites are allowed only through the classifier's exact conditions.
// A selected failure cannot turn into a successful (or skipped) build check.
for (const entry of workflow.jobs.build.steps.filter((step) =>
  step.if?.startsWith('steps.changes.outputs.'),
)) {
  for (const [label, change] of [
    [
      'skip selected suite',
      (step) => {
        step.if = 'false';
      },
    ],
    [
      'ignore selected failure',
      (step) => {
        step['continue-on-error'] = true;
      },
    ],
  ])
    test(`deployment rejects ${label}: ${entry.name}`, () => {
      const doc = structuredClone(workflow);
      change(step(doc, entry.name));
      assert.throws(() => contract(doc, manifest));
    });
}
