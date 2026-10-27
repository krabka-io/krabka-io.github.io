# Website CI selection

The required `build` check runs on every pull request to `main`. There are no
workflow path filters. The job keeps ordinary validation in one blocking chain;
a skipped optional step cannot skip the merge gate. Selected suites do not use
`continue-on-error`. Their failure fails `build` and prevents Pages upload.

## Pull requests

`scripts/ci-changes.mjs` compares the PR head with the merge base of its base
commit. The checkout includes full history. The diff uses NUL-delimited names
and disables rename detection so deletions and both sides of a move are inputs.
A missing commit, diff error, or unknown path selects full coverage.

Every PR runs website and sync regressions, snippet extraction coverage,
benchmark tests, documentation and proof synchronization, Astro, sitemap XSD
validation, Pagefind, links, SEO, HTML/JavaScript budgets, general browser checks,
and the existing inexpensive proof and lab unit checks.

The classifier selects the expensive work separately:

- Lab and playground changes select both Rust/WASM builds, lab startup, and the
  existing real-broker browser suites. Playground changes also select snippets
  because Rust snippets use its dependency pins and local patches.
- Proof changes select the Why3 bundle, worker checks, proof startup, and browser
  proof replay.
- Snippet inputs select all eight execution shards in `snippets.yml`. The page
  inventory comes from `scripts/snippets/extract.mjs`. Quickstart downloads,
  packaged charts, the harness, and playground dependency inputs also select it.
- Shared layouts, components, styles, data, dependencies, pins, build scripts,
  configuration, and workflows conservatively select all suites.
- Ordinary page copy and repository documentation run the common checks only.

Drafts keep the common checks but omit expensive suites. Both workflows handle
`ready_for_review` and `converted_to_draft`; marking a PR ready selects suites
again from its full diff, not only its last commit.

The workflow summary records the selection. A lightweight build does not create
placeholder binaries or restore an unrelated build. Its budget report explicitly
lists the two unbuilt WASM payloads; all other budgets remain active. Lab-selected
runs and release runs require both binaries and enforce their budgets.

## Release and manual runs

Main pushes, schedules, release dispatches, and manual runs select full coverage.
Only `refs/heads/main` can upload and deploy Pages. The production concurrency
group remains serialized through deployment and smoke checks. Each PR has its
own concurrency group and cancels its superseded runs; it cannot cancel production.
Snippet execution also cancels only superseded PR runs.

`npm run build` and `npm run check-site` retain their complete local behavior.
CI builds the selected binaries first, then always runs `sync-docs`, `sync-proofs`,
and `build:site`. Browser checks accept `--suite=site`, `--suite=lab`, and
`--suite=proofs`; no argument runs all three. Each suite writes separate evidence.

## Verification

`npm run test:website` includes classifier scenarios and mutation tests for the
deploy and snippet contracts. These reject lost sync steps, disabled checks,
ignored failures, path filters, incorrect conditions, missing ready events, and
production cancellation. `npm run test:snippets` checks extraction coverage.

Compare hosted Actions runner time and wall-clock time after push. Local timings
are not a hosted-runner comparison. Keep full scheduled checks as the integration
backstop for dependencies fetched from sibling repositories.
