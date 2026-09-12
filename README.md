# krabka.io

The website and documentation portal for the
[Krabka](https://github.com/krabka-io) streaming ecosystem. It is built with
[Astro](https://astro.build) and [Tailwind CSS](https://tailwindcss.com), and it
is served from GitHub Pages.

The site also publishes two things the rest of the organisation depends on: the
aggregated Helm chart repository, and the brand assets that the published charts
point their icon at.

## Quick start

```bash
npm ci                   # install dependencies
npm run dev              # local dev server with hot reload
npm run build            # the WASM playground, then the static bundle in ./dist
npm run preview          # serve ./dist locally
```

`npm run build` needs a Rust toolchain, because it compiles the consensus
playground. `npm run dev` does not. Use `npm run build:site` to build the pages
alone and `npm run build:playground` to build the WASM alone.

## Project structure

```
krabka-io.github.io/
├── playground/                    # krabka-playground: the WASM consensus simulator (Rust)
│   ├── src/lib.rs                 #   the wasm-bindgen shim over krabka-kraft-core
│   └── build.sh                   #   compiles it into public/playground/
├── public/
│   ├── brand/                     # the mark and the lockups, at stable URLs
│   ├── charts/                    # the aggregated Helm repository: index.yaml and the charts
│   ├── logo.png                   # the chart icon that published Chart.yaml files point at
│   ├── playground/app.js          # the playground front-end
│   └── quickstart/                # the manifests the quickstart pages link to
├── scripts/build-helm-index.sh    # rebuilds public/charts from the component repositories
├── src/
│   ├── components/                # Hero, Navbar, MetricStrip, CodeSwitcher, EcosystemGrid, BenchmarkBar
│   ├── layouts/                   # BaseLayout, DocsLayout, ProseLayout
│   ├── pages/                     # landing, get started, features, versions, benchmarks, docs, brand
│   └── styles/                    # the Tailwind entry point and the playground styles
├── astro.config.mjs
└── .github/workflows/
    ├── deploy.yml                 # builds the site and deploys it to GitHub Pages
    └── helm-index.yml             # rebuilds the aggregated chart index
```

## The consensus playground

`/docs/playground` runs Krabka's real KRaft consensus core in the browser.
`playground/` holds the Rust crate that binds that core to JavaScript, and
`playground/build.sh` compiles it to WebAssembly during the site build. See
[`playground/README.md`](playground/README.md).

## The Helm chart repository

Users add one repository URL:

```bash
helm repo add krabka https://krabka-io.github.io/charts
helm repo update
```

`scripts/build-helm-index.sh` walks the krabka-io organisation, takes every
repository that holds a `charts/` directory, packages each chart, and writes one
`index.yaml` over the whole set. The `helm-index.yml` workflow runs it daily and
commits the result. A component repository can ask for an immediate rebuild:

```bash
gh api repos/krabka-io/krabka-io.github.io/dispatches -f event_type=charts-changed
```

The chart signing public key lives in
[krabka-io/tooling](https://github.com/krabka-io/tooling), under `charts/`. No
key material is stored here.

## Brand assets

`/brand` lists every mark with the URL it is served from. Published Helm charts
point their `Chart.yaml` icon at `/logo.png`. Treat a rename under `public/brand`
or `public/logo.png` as a breaking change for those charts.

## Licence

Apache License 2.0.
