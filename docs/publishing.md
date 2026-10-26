# Website publishing

Publishing and settings changes need maintainer authorization; this guide describes procedures, not permission to run them. The [deploy workflow](../.github/workflows/deploy.yml) builds and checks pull requests; only `main` uploads and deploys the Pages artifact. Production smoke checks follow deployment and run on the daily schedule. Local checks do not establish deployment success.

## Deployment ownership and checks

The website maintainer owns deployment monitoring and production-smoke triage, as defined in [review ownership](testing.md#review-ownership). Identify a named maintainer for each recovery action. This repository does not name an individual deployment responder.

The `build` job runs website regression tests, builds the site, and checks links, SEO, browser behavior, and other release checks. It uploads the Pages artifact only after those checks succeed. The `deploy` job depends on `build`. The `production-smoke` job depends on `deploy` and compares the published sitemap with the deployed artifact's URL inventory. It also checks pages, essential assets, and component API links.

`npm run test:website` includes [deployment-contract tests](../scripts/deployment-contract.test.mjs). They protect the blocking commands, their order, job dependencies, main-branch publication conditions, and release-inventory comparison. Mutation cases check that representative changes which remove or bypass these checks fail the contract. This contract is not a general shell analyzer or a substitute for workflow review.

Repository rules are separate from these tests. Verify the applicable rules before relying on merge enforcement:

```sh
gh api repos/krabka-io/krabka-io.github.io/rules/branches/main
gh api repos/krabka-io/krabka-io.github.io/rulesets
```

Inspect each applicable ruleset's required checks and bypass actors. The required `build` context should match GitHub Actions. A legacy branch-protection response alone does not establish whether rulesets protect the branch. Keep dated configuration evidence in [the issue #91 record](reviews/issue-91.md#deployment-enforcement-review-26-october-2026).

## Recover from a failed deployment

A failed `production-smoke` job does not automatically roll back the site. Publication has already occurred. Keep the incident open until the replacement deployment and its smoke checks succeed.

1. Identify the failed run, commit, deployment URL, and failing job. Record them in the incident or issue.

   ```sh
   gh run list --repo krabka-io/krabka-io.github.io --workflow deploy.yml --limit 10
   gh run view RUN_ID --repo krabka-io/krabka-io.github.io
   gh run view RUN_ID --repo krabka-io/krabka-io.github.io --log-failed
   ```

2. Compare the last successful run with the failed run. A build failure before upload does not publish that run's artifact. A smoke failure can indicate a site fault, delayed publication, or a separately published component API failure. Check the failed URLs before choosing a site recovery.
3. Choose a repair or revert through a pull request. For a revert, use a separate branch and revert only the responsible website change. Do not reset or force-push `main`. Follow the repository's normal review and required checks.
4. Obtain maintainer authorization before merging or redeploying. A previous successful source commit does not guarantee identical output: builds sync upstream guides and release data. Recheck their provenance and the generated artifact.
5. After the approved change reaches `main`, monitor the new workflow run. Confirm that `build`, `deploy`, and `production-smoke` all succeed for that commit. Compare the published site with that run's artifact inventory. Check the affected page or browser tool manually.
6. Record the recovery commit, successful run URL, observed result, and any remaining component failure. Close the incident only when its acceptance checks pass.

For a confirmed transient failure, a maintainer can authorize a rerun:

```sh
gh run rerun RUN_ID --failed --repo krabka-io/krabka-io.github.io
```

Rerun only after checking which jobs will execute. A failed smoke job rerun does not restore a previous artifact. A build or deployment rerun can publish output and needs deployment authorization. Do not disable validators or change branch rules to recover a release.

## Aggregated Helm chart repository

Users add one repository URL:

```bash
helm repo add krabka https://krabka.io/charts
helm repo update
```

`scripts/build-helm-index.sh` walks the `krabka-io` organization, takes every repository that holds a `charts/` directory, packages each chart (signing it when `HELM_GPG_KEY` is set), and writes one `index.yaml` over the whole set in `public/charts/`. It needs `helm`, an authenticated `gh`, and `python3`. The `helm-index.yml` workflow runs daily and opens a pull request for a maintainer to merge.

A component repository can trigger an immediate index rebuild via repository dispatch:

```bash
gh api repos/krabka-io/krabka-io.github.io/dispatches -f event_type=charts-changed
```

The chart signing public key lives in [krabka-io/tooling](https://github.com/krabka-io/tooling), under `charts/`. No key material is stored here.

## Brand assets and chart icons

`/brand` lists every mark with the URL it is served from. Published Helm charts point their `Chart.yaml` icon at `/logo.png`. Treat a rename under `public/brand` or `public/logo.png` as a breaking change for published charts.

Original brand download URLs are public contracts. The gallery uses small WebP previews; run `npm run build:brand` after changing an original image. Technical URL stability does not grant branding permission: see [trademark policy](../trademark.md) and [license scope](../LICENSE).

## Component releases and API publication

The website links compiler references published by each component repository; it does not copy those trees. See [API ownership](architecture.md#component-api-references). Component Pages settings use GitHub Actions as their source. Release metadata is refreshed by `scripts/sync-docs.mjs`. Release-published dispatches are handled by the deploy workflow. Follow [benchmark publication instructions](../benchmarks/README.md) for validated full OpenMessaging runs; the homepage and benchmark page follow `benchmarks/latest-openmessaging.md`.

## Generated assets

Sync proofs and guides before rendering verification pages. Build the broker and Why3 bundles before the full website build to include them in `dist/`. See [development](development.md) and the [Why3 build guide](../why3-web/README.md). Do not change a page inventory or asset URL without checking its consumers and the [test gates](testing.md).
