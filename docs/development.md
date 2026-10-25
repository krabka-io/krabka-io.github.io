# Website development

Use this guide for setup and local preview. See [testing](testing.md) for review gates and integration prerequisites, and [architecture](architecture.md) for source ownership.

## Prerequisites

Use Node.js 22.12.0 or newer and its bundled npm; CI uses Node 24. Install Git and Git LFS. Sync scripts read component checkouts beside this repository when available and otherwise clone them from GitHub. Release-tag lookup uses the GitHub API; authenticated `gh` is useful for release metadata and required for maintainer chart tasks. Network access is needed for dependencies, archives, and upstream synchronization.

A pages-only preview does not need Rust. Full browser-tool builds have these additional requirements:

- The playground and Cluster Lab simulation use Rust 1.99.0 or newer, the `wasm32-unknown-unknown` target, Bash, and curl. `playground/build.sh` adds the target through rustup and downloads its matching wasm-bindgen CLI.
- The real lab broker uses `wasm32-wasip1` and wasi-sdk 34. When `WASI_SDK_PATH` is unset, its build script downloads the SDK into `playground/broker-wasi/.tools/`; its clang, llvm-ar, and WASI sysroot compile the broker's C dependencies.
- Browser proof re-checking needs the Docker/Bazel build described in [the Why3 guide](../why3-web/README.md).

## Pages-only preview

From the repository root, install dependencies and fetch the benchmark archive:

```sh
npm ci
git lfs pull --include "benchmarks/archive/*.tar.gz"
```

If Git LFS is not initialized, run `git lfs install` first. The archive is required by `start`, `dev`, `build:site`, and `build`, because each prepares the captured-data explorer.

Synchronize the guides and proof sessions, then start Astro:

```sh
npm run sync-docs
npm run sync-proofs
npm run start
```

Open the local URL printed by Astro, normally `http://localhost:4321`. Verify that the expected page renders and that the guide's provenance footer identifies the intended source. This path does not compile Rust or stage the generated playground, real-broker, or Why3 modules. A browser-tool page loading does not establish that its tool works. Without the real-broker module, lab broker nodes report "no build on this site" and refuse connections.

For subsequent work, `npm run dev` synchronizes guides and proofs before starting; `npm run start` starts without synchronizing. Both prepare the archive. `npm run sync-docs` imports Markdown guides and refreshes release metadata; it does not copy compiler API reference trees, which the component repositories publish themselves.

## Build tiers

For pages only, after preparing guides and proofs:

```sh
npm run build:site
npm run preview
```

The output is `dist/`. `build:site` also indexes it with Pagefind, but does not synchronize guides or proofs or build browser modules. `preview` serves the built output rather than the development sources.

For the playground, verified-kernel explorer, and lab simulation, build the Rust/WASM module:

```sh
npm run build:playground
```

To stage the real broker and browser provers, run their separate builds:

```sh
npm run build:broker
npm run build:why3-web
```

The full website command then rebuilds the playground, synchronizes guides and proofs, builds Astro pages, and indexes search:

```sh
npm run build
```

Deployment stages `build:broker` and `build:why3-web` before `build`; the latter does not invoke those separate builds. For real-versus-simulated behavior and persistence, read [the lab design](../playground/docs/lab-design.md) and [real-broker contract](../playground/docs/lab-real-broker.md).

## Update generated inputs

Sync proof sessions and Coma files from the broker:

```sh
npm run sync-proofs
```

After checking that every quoted signature and contract clause still matches the broker source, update the kernel explorer's source links to broker `origin/HEAD`:

```sh
npm run repin-kernels
```

Regenerate the lab analyzers' Kafka schemas, errors, and API keys from krabka-protocol and krabka-broker (siblings fetched at `origin/HEAD`, otherwise cloned):

```sh
npm run gen-kafka-schemas
```

Use [publishing](publishing.md) for brand-image regeneration and stable URL contracts. Benchmark execution, hardware requirements, workload commands, and publication have one canonical home: [the benchmark instructions](../benchmarks/README.md).

## Recover from missing or stale inputs

If archive preparation reports an LFS pointer, fetch the archive using the command above. If a tool reports a missing build, stage that module before rebuilding the site. If a required upstream sync fails, the scripts preserve previous generated output; do not treat stale output as current evidence.

Edit imported guides in their component repositories, not under `src/content/docs/`. Inspect source provenance when using sibling checkouts. Sync may refresh release metadata; review and exclude unrelated generated changes from a documentation pull request. Check the [testing prerequisites](testing.md) before interpreting missing browser dependencies as a test failure.
