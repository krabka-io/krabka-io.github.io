# Website architecture

This repository owns the website, generated guide presentation, browser-hosted tools, chart index, and brand distribution. It does not own the component API trees or the imported guides. See [development](development.md), [testing](testing.md), and [publishing](publishing.md) for procedures.

## Generated documentation

- **Dynamic Content Collections:** Markdown guides authored inside language sub-repositories (`krabka-streams-java`, `krabka-streams-go`, `krabka-broker`) are ingested via `scripts/sync-docs.mjs` into `src/content/docs/`.
- **Dynamic Routing:** `src/pages/docs/[...slug].astro` renders ingested guides with an automated right-hand **"On This Page" Table of Contents**, syntax highlighting, custom markdown typography, and GitHub source provenance footers.
- **Collapsible Symmetrical Navigation:** `src/layouts/DocsLayout.astro` groups ecosystem topics into collapsible accordions with automatic active-state expansion and breadcrumbs.

## SEO and sitemaps

The build derives sitemap URLs from Astro routes and checks the result against
indexable canonical HTML. `robots.txt` advertises `sitemap-index.xml`, which
links to `sitemap-docs-0.xml` for `/docs` and its descendants and
`sitemap-website-0.xml` for the remaining website pages. Each group can acquire
additional numbered shards. Redirects, error pages, `noindex` pages and non-page
artifacts are excluded. Component API deployments own their own inventories.
See [sitemap maintenance and remaining protocol work](sitemaps.md).

### Optional metadata contract

`lastmod`, `changefreq` and `priority` are not required. The shared XML reader
used by local build and published checks validates values when supplied. Invalid
supplied metadata fails; absence never does. Page dates describe significant
page changes, while index dates describe changes to the child XML file.
`changefreq` is an optional frequency hint, not a crawl schedule;
[Google ignores it and priority, but can use accurate lastmod](https://developers.google.com/search/docs/crawling-indexing/sitemaps/build-sitemap#additional-notes-about-xml-sitemaps).
No blanket frequency or synthetic date is generated. Both local build checks and
published checks validate original XML bytes against the vendored official XSDs.
See [validator setup](sitemaps.md#commands).

### Planned site-wide lastmod provenance

The target is every indexable page, not only Markdown guides. Keep date
resolution centralized rather than editing each of the site's page templates:

- Authored pages use verified significant changes to their content and substantive
  shared dependencies, not every layout, style or repository commit.
- Synced guides use the original source path at the exact synced revision, with
  complete history and dirty-source checks as described below.
- Generated pages include their meaningful data inputs: release data and edited
  release notes, benchmark results, proof/catalog data and CRD schemas. The
  current live release query has publication dates, not a reliable last-edit date
  for changed notes; do not treat publication as proof of the last content update.
- Omit dates when provenance is incomplete. Report these gaps without requiring
  metadata coverage. Unchanged rebuilds must leave dates unchanged.

**Scoping estimate (not an implemented diff):** about 12–16 files in a separate
follow-up PR. Likely changes are a central provenance resolver, a route/input
mapping and their tests; `scripts/sync-docs.mjs` and its tests; release metadata in
`src/utils/versions.ts` and its tests; `astro.config.mjs`; sitemap checking and its
tests; `.github/workflows/deploy.yml` for history; and these two SEO design and
maintenance documents. Other data loaders may need provenance hooks, which is
why this is a range. No blanket edit of all page templates is expected. Keep
this separate from XSD validation: source history, data dependencies, unchanged
rebuilds and index-date semantics need independent review and regression cases.

Markdown can provide reliable dates through its source repository, not through
the copied file's filesystem timestamp. This pipeline is not implemented yet:

1. Extend `scripts/sync-docs.mjs` provenance beyond its current per-repository
   `sources.json` date. For each source Markdown path, query
   `git log -1 --format=%cI -- <source-path>` at the exact revision being synced.
   Store a separate per-file record with the source repository, source path,
   pinned revision, last-change commit and date; do not change the existing
   destination-to-source `files` mapping consumed by source links.
2. Establish sufficient history first. The fallback sparse clone currently uses
   `--depth 1`; Git can mistake the shallow boundary for a change to every file.
   Fetch history for the pinned revision, or obtain verified path history from
   the upstream API. When completeness cannot be established, omit the date and
   report unavailable provenance without blocking the build. Never substitute
   the repository HEAD date. Do not use history dates for dirty or untracked
   source content without a verified override.
3. Map the per-file provenance through the same destination/slug normalization
   used by generated documentation routes. Feed dates into Astro's sitemap
   `serialize` callback by canonical URL. Prefer an explicitly maintained,
   validated significant-update date where available; per-file Git dates are a
   fallback candidate, not proof that a change was significant.
4. Account for other meaningful page inputs: generated CRD material, embedded
   data, rewritten links and substantive shared content. Track their meaningful
   changes or omit dates where the dependency mapping is incomplete. A cosmetic
   change, checkout, docs copy or rebuild alone must not refresh page dates.
5. Keep existing dates stable across unchanged rebuilds. Before enabling date
   generation, test unrelated upstream commits, shallow history, source renames,
   dirty files, missing history, changed dependencies and repeat builds. Metadata
   completeness can be reported, but is not a required coverage threshold.

Do not derive index `lastmod` from the newest page date: a child can change when
URLs are removed or hints are edited without a later page date. Persist a child
content hash and its verified change time if index dates are needed; otherwise
omit them. The installed Astro chunk writer derives index dates from the newest
item date, so override or suppress that behavior before enabling page dates.

## Component API references

- `/api` is a directory of every component's compiler-generated reference. It is driven by `src/data/api-reference.json`, one entry per component, and each entry links to where that component's own repository publishes its reference on `https://krabka.io/<repository>/`. This site does not host or copy those trees.
  - **Rust (Bazel `rust_doc`):** `krabka-broker`, `krabka-protocol`, `krabka-client-rs`, `krabka-streams-rs`, `krabka-cli`, `krabka-connect`, `krabka-operator`, `krabka-rebalancer` and `krabka-gateway`. Each repository's `docs-pages.yml` builds `//crates/...:*_doc`, assembles a landing page with `aspect rustdoc-site`, and deploys it to that repository's GitHub Pages. Pages must be enabled with source "GitHub Actions" in each repository's settings.
  - **Java:** Javadoc from `krabka-streams-java`, at `/krabka-streams-java/api/`.
  - **Go:** godoc from `krabka-streams-go`, at `/krabka-streams-go/`.
- `npm run check-api-links` fetches every link in the directory and fails on a non-200 answer or on the old placeholder page. It needs the network, so it is not part of `npm run build`.
- Release tags come from `src/data/versions.json`, which `scripts/sync-docs.mjs` refreshes from the sibling repositories and the GitHub API.

## Consensus playground

- `/docs/playground` runs Krabka's real KRaft consensus quorum directly in the browser.
- `playground/` holds the Rust crate binding `krabka-kraft-core` to WebAssembly via `wasm-bindgen`, compiled during the site build via `playground/build.sh`.

## Verification playground

- The same `playground/` crate also binds `krabka-verified`, the broker's Creusot-proved decision kernels, through one `run_kernel(name, json)` dispatcher. The page evaluates a curated set of kernels in the browser with each function's `requires` and `ensures` contract beside the result; inputs outside a precondition are reported, not evaluated.
- Each kernel's panel carries a "Proof ledger" drilldown quoting the broker's catalog: what the catalog says the kernel proves, which production code calls it, its Why3 proof sessions, and what the caller must establish.
- Below the explorer, an expandable Stateright model ledger built from `src/data/stateright-models.json` and the catalog lists every model entry point with what it drives, its bounds, its properties, its pinned unique-state counts, and the catalog's own description. The models themselves cannot run in a browser (they are test modules of I/O-bearing crates), so the ledger is an inventory, not a checker.

## Proof explorer

- `scripts/sync-proofs.mjs` copies the broker's Why3find proof sessions (`verif/**/proof.json`) and the Coma files Creusot generated into `src/data/proof-sessions.json` and `public/proofs/coma/`. The page is a full-window app: the sessions by module on the left (arrow keys walk them, `/` filters), and for the open session tabs for its proof tree (tactics, provers, times), the `[@expl]` obligations parsed from the Coma with their source spans, and the generated Coma itself, syntax-highlighted by `public/proofs/highlight.js`.
- With the `why3-web` bundle present (`npm run build:why3-web`, built with Bazel in a Docker container; see `why3-web/README.md`), the page re-checks a session in the browser: Why3 compiled with js_of_ocaml loads the Coma file, splits it with the recorded tactics, and Alt-Ergo (JavaScript), Z3, cvc5 or CVC4 (WebAssembly) re-check each supported leaf through its Why3 SMT-LIB driver. Z3 enables shared memory with a scoped service worker, reloads once and resumes the check.

## Cluster Lab

- A distributed-systems playground: real `krabka-broker` processes, a schema registry, producers, consumers and `krabka-client-streams` apps placed on a canvas. The brokers are the real broker compiled for `wasm32-wasip1` (`playground/broker-wasi`, staged by `npm run build:broker`, contract in `playground/docs/lab-real-broker.md`) and run in Web Workers on the browser WASI runtime in `public/playground/wasi/`. Clients and apps run as one sans-IO simulation inside the `playground/` crate (`playground/src/lab/`, contract in `playground/docs/lab-design.md`). The page owns the clock and the virtual network; faults (kill, restart, wipe, isolate, partition, latency, loss) are deliberate. The network, the clients and the apps replay under the seed; the broker processes draw real randomness and do not.

See [the lab design](../playground/docs/lab-design.md#website-integration-and-browser-contracts) for the frontend, persistence, WebRTC, analyzer, logging, and fault/experiment contracts, and [the real-broker contract](../playground/docs/lab-real-broker.md).

## Correctness and verification

- **Verification page (`/verification`):** How the broker establishes correctness: forbidden `unsafe` Rust, Creusot-proved decision kernels in `krabka-verified`, exhaustive Stateright model checking, mutation testing, and differential suites against live Apache Kafka. Its evidence ledger is a tabbed, filterable list of expandable rows: every Creusot ledger row (what it proves, host caller, proof sessions, caller preconditions), every Stateright model, and the other evidence tiers.
- **Verification catalog (`/docs/broker/verification`):** The Creusot proof ledger and Stateright model inventory, synced from `krabka-broker/docs/verification.md`. `src/utils/verification-catalog.ts` parses that file at build time and `src/utils/verification-data.ts` joins it with the site's data files; the counts on the homepage and the verification page come from that parse, so they follow the broker's catalog rather than hand-maintained copy.
- Website performance figures come only from published full OpenMessaging Benchmark runs. Observability (`krabka-o11y`) and Postgres-compatible compute (`gres`) are documented in their own repositories and are linked as the broader Krabka ecosystem.

See [the benchmark instructions](../benchmarks/README.md) for execution and publication. Only published full OpenMessaging runs supply website performance figures.

## Release verification and tracks

- Release tracking across stable, pre-release, and development channels with SLSA Level 3 provenance verification steps and Sigstore signatures.

## Code highlighting

- Every code snippet is highlighted with Shiki and one theme, `src/utils/krabka-shiki-theme.mjs`, whose palette is the one the rustdoc, Javadoc and Go API sites use. Hand-written pages render snippets through `src/components/CodeBlock.astro` (pass the Shiki language id); the synced Markdown guides get the same theme from `markdown.shikiConfig` in `astro.config.mjs`. The proof explorer highlights Coma in the browser with its own small tokenizer, because Shiki runs at build time and the explorer loads each Coma file on demand.

## Source map

| Location | Responsibility |
| --- | --- |
| src/pages, src/layouts, src/components | Routes, page shells, and reusable presentation |
| src/content/docs | Generated upstream guides; edit sources in component repositories |
| src/data and src/utils | Release and verification data, parsing, highlighting, path helpers |
| public | Browser frontends, downloadable manifests, stable assets, generated browser modules |
| playground and why3-web | Rust/WASM and browser-prover build sources |
| scripts and .github/workflows | Sync, validation, benchmark, chart, and publishing tasks |

The former README full tree is replaced by this responsibility map. Consult the source tree for the current file inventory.
