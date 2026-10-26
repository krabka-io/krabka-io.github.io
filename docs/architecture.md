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

### Site-wide lastmod provenance

The resolver covers the final sitemap inventory, not a fixed page count.
`scripts/lastmod-routes.mjs` maps authored routes, synced guides and CRD routes.
The build reports new, unmapped routes without assigning a date.
A mapped route does not imply complete provenance.

`scripts/sitemap-lastmod.mjs` runs after Astro writes its sitemaps and before
`scripts/sitemap-limits.mjs` splits them by bytes. It adds verified page dates
and writes `dist/lastmod-report.json`. The normal sitemap checker then verifies
canonical coverage, protocol rules, schemas and reachability. The report lists
each canonical URL, route family, declared inputs, evidence and omission reason.
Coverage is a diagnostic, not a passing threshold.

#### Significant-update ownership

Website maintainers own `scripts/lastmod-reviewed.json`. Each record declares a
complete set of meaningful inputs, their SHA-256 hashes, an explicit significant
update date, a baseline revision and the review evidence. This explicit date
has precedence over Git candidates. The resolver never promotes a candidate
commit date automatically. All supplied dates are validated, including records
for routes that are absent or omitted. Invalid dates fail the build.

Review changes to the main content, material claims, embedded data, structured
content, useful links and artwork. Consider substantive shared content too.
Formatting, CSS adjustments, ordinary navigation, analytics and the footer's
copyright year do not establish a new significant update. Include conservative
hash guards for these files when they can change rendered content. A changed
hash removes the date until review; it does not assign the new commit date.
After a cosmetic change, refresh the hashes and keep the significant date.
After a substantive change, establish the date from cited evidence first.

A record covers the whole route, not only its newest known input. Review the
static import closure and all non-import inputs, such as file reads, images,
client scripts, configuration, transforms and data. New imported dependencies
that are absent from the record cause omission. Import discovery is a guard,
not a claim that static analysis finds every meaningful input. Do not mark a
record complete while an input or its significant history is unknown.

The two initial records cover the KRaft and operator feature pages. They propose
12 October 2026 as the baseline for the feature-scope descriptions changed in
`a6d74993a9f17a8b67e355a92987a3807d7bc580`. The implementation author inspected
those changes. These records do not claim prior approval by a maintainer.
They include the shared source, render transforms, lockfile, fonts and image
inputs. The imported versions data is unused on these non-homepage routes.
Their current hashes are guards, not evidence that every input changed on
12 October. Review these proposed baselines with the implementation.

The resolver checks complete website history, baseline ancestry, tracked input
paths, clean input and attestation status, exact hashes and intervening commits.
A later input edit followed by a revert still needs review, even with equal hashes. Dirty or untracked inputs, changed
hashes, missing files and unavailable history remove dates with reasons.
An unrelated file or commit does not change a verified date. There is no build,
sync, filesystem-mtime or repository-HEAD fallback. CI checks out full website
history. A source archive or shallow checkout still builds, without those dates.

#### Synced and generated inputs

`sync-docs` publishes `src/content/docs/lastmod-provenance.json` atomically with
the guides. Each source file has its original repository/path, pinned revision,
last-change candidate, history status, dirty-source status and source hash.
Each guide also has a transformed-content hash. CRD source records remain
separate from Markdown records. Candidate significance is explicitly unreviewed.
The existing `sources.json` component fields and destination-to-source mapping
remain unchanged for GitHub footers. The component HEAD date is not a page date.

Fallback sparse clones acquire full history with filtered blobs. Existing
shallow sibling checkouts are not modified or silently trusted. They report
incomplete history. The lookup uses the pinned revision and follows renames.
Non-Git, dirty and untracked source cases retain reasons without guessed dates.
Sync failure leaves both previous content and provenance intact.

Guides currently omit dates because their significant source changes and full
render dependencies have not been reviewed. CRD pages report the schema gap.
The release index omits dates because publication does not establish
edited notes or addition/removal history. Versions omit dates because their
live snapshots lack complete history. Benchmark, catalog and proof views
report their unreviewed data inputs. The homepage includes those data gaps.
News pages use authored content, not live release notes.
These omissions apply online and offline. A release edit timestamp alone would
not prove complete collection history, including removed releases.

Index entries have no `lastmod`. Astro never receives page dates, so its chunk
writer cannot infer child-file dates from page dates. The limits pass rebuilds
index references from parsed locations and preserves page metadata through
splits. URL removal and metadata-only changes still produce undated references.
No dependency under `node_modules` is patched.

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
