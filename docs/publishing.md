# Website publishing

Publishing and settings changes need maintainer authorization; this guide describes procedures, not permission to run them. The [deploy workflow](../.github/workflows/deploy.yml) builds and checks pull requests; only `main` uploads and deploys the Pages artifact. Production smoke checks follow deployment and run on the daily schedule. Local checks do not establish deployment success.

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
