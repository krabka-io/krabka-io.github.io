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
`scripts/sitemap-limits.mjs` splits them by bytes. It adds Git-derived page dates
and writes `dist/lastmod-report.json`. The normal sitemap checker then verifies
canonical coverage, protocol rules, schemas and reachability. The report lists
each canonical URL, route family, declared inputs, evidence and omission reason.
Coverage is a diagnostic, not a passing threshold.

#### Automatic content dates

For each mapped route, the resolver queries `git log --follow -1 --format=%cI`
for each content-bearing input and takes the maximum timestamp. The output
uses UTC `YYYY-MM-DD` dates consistently. There is no human attestation file,
input-hash lockfile, or pinned website baseline revision. Squash merges and
rebases use the history present in the checked-out repository.

Static import traversal includes page entries, content components, rendering
logic that changes content, and tracked data. The route map adds file-read
inputs that imports cannot discover. Benchmark pages include tracked report
and trial files; the explorer uses tracked LFS archives rather than generated
archive JSON. Verification pages include the synced broker catalog and tracked
kernel/model data. The homepage also includes the local quickstart source used for
its client excerpts and the tracked version data used in homepage structured
data. Other pages do not inherit that unused layout import. The proof explorer includes its tracked scripts and data,
the sync generator, and the broker catalog rather than treating its generated
sessions JSON as a tracked website input.

Traversal stops at global layouts, navigation, footer, logo, code-block styling,
CSS, fonts, favicons, social images, path/heading/highlighting utilities, and
build/dependency configuration. Changes to these files do not date or suppress
otherwise clean routes. Stopping at layouts also excludes their unused versions
import from non-homepage routes. Content-bearing imports such as `Hero.astro`,
`BenchmarkOverview.astro`, `EcosystemGrid.astro`, and `CrdDescription.astro` do
contribute to routes that use them.

`scripts/lastmod-ignore-revs.txt` optionally lists cosmetic-only commit SHAs or
unambiguous hexadecimal prefixes of at least seven characters. Blank lines and
`#` comments are accepted. The default list is empty. When the list is nonempty,
the resolver walks each file's rename-following history and selects its first
non-ignored commit. The same list applies during upstream guide and CRD sync.
Malformed entries fail validation. This exception list does not require routine
per-page maintenance or suppress a page date after an ordinary content commit.

Dirty, untracked, missing, non-Git or shallow content inputs produce explicit
omission reasons. A tracked LFS file is checked through Git's clean filters,
not by comparing its hydrated bytes with the pointer blob. Unmapped routes
remain undated until added to the route map. `/releases` and `/versions` remain
undated because live API edit/membership history is not pinned in Git; their
offline caches do not establish complete live history. There is no build,
sync, filesystem-mtime or repository-HEAD fallback. CI uses full website history.

#### Synced and generated inputs

`sync-docs` publishes `src/content/docs/lastmod-provenance.json` atomically with
the guides. Each source record has its repository/path, synced revision,
last per-file commit and date, history status, and dirty-source status. Source
and rendered hashes remain sync diagnostics, not a manually maintained date
gate. The existing `sources.json` footer contract is unchanged.

Clean synced guides with complete upstream history use their Markdown file's
record directly. A component HEAD date, link rewrite, global docs layout or
shared guide renderer does not replace the Markdown date. Custom destination
paths and nested guide paths retain their original source-file history.

CRD kind routes combine the matching YAML record with their page source,
content components, and tracked field-description data. The CRD index combines
all YAML records with its own content inputs. The generated `crds.json` snapshot
is not queried as an untracked website source.

Fallback sparse clones acquire full history with filtered blobs. Existing
shallow sibling checkouts are not modified or silently trusted. They report
incomplete history. The source lookup uses the synced revision and follows
renames. Sync failure leaves previous content and provenance intact.

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

- Authored `CodeBlock` components use `src/utils/compact-code-tokens.mjs` to merge adjacent Shiki tokens with identical styles and replace known token colors with shared `.tok-*` classes in `src/styles/custom.css`. The classes use the same `--code-*` theme variables. Unknown or compound styles stay inline. Synced Markdown rendering is unchanged.

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
