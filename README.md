# krabka-io.github.io

The official website and unified documentation hub for the [Krabka](https://github.com/krabka-io) streaming ecosystem, built with [Astro 5](https://astro.build) and [Tailwind CSS](https://tailwindcss.com).

Hosted live at [krabka.io](https://krabka.io) and [krabka-io.github.io](https://krabka-io.github.io).

The site also publishes two things the rest of the organisation depends on: the aggregated Helm chart repository, and the brand assets that published charts point their icon at.

---

## 🚀 Quick Start

### Prerequisites
- **Node.js**: `>= 18.20.8` (Node 22 LTS recommended)
- **npm**: `>= 10.0.0`
- **Rust Toolchain**: Stable with `wasm32-unknown-unknown` target (only required if building the WebAssembly consensus playground)

### Commands

```bash
# Install dependencies
npm install

# Start local development server (automatically syncs docs from sibling repos)
npm run dev

# Manually synchronize markdown guides and compiler API archives
npm run sync-docs

# Build the WASM playground, then the full static bundle in ./dist
npm run build

# Build pages only (skipping playground WASM rebuild)
npm run build:site

# Build the WASM playground only
npm run build:playground

# Run internal link integrity audit (verifies all 6,000+ links resolve)
npm run check-links

# Run technical SEO audit (titles, descriptions, canonicals, social cards, sitemaps)
npm run check-seo

# Sync the broker's proof sessions and Coma files for the proof explorer
npm run sync-proofs

# Build Why3 and Alt-Ergo to JavaScript with Bazel (needs Docker) for the browser re-check
npm run build:why3-web

# Check that the synced broker verification catalog still parses into the ledger rows
npm run check-catalog

# Preview production build locally
npm run preview
```

---

## 🏗️ Architecture & Features

### 1. Decoupled Build + Unified Distribution Documentation
- **Dynamic Content Collections:** Markdown guides authored inside language sub-repositories (`krabka-streams-java`, `krabka-streams-go`, `krabka-broker`) are ingested via `scripts/sync-docs.mjs` into `src/content/docs/`.
- **Astro 5 Dynamic Routing:** `src/pages/docs/[...slug].astro` renders ingested guides with an automated right-hand **"On This Page" Table of Contents**, syntax highlighting, custom markdown typography, and GitHub source provenance footers.
- **Collapsible Symmetrical Navigation:** `src/layouts/DocsLayout.astro` groups ecosystem topics into collapsible accordions with automatic active-state expansion and breadcrumbs.

### 2. Central API Reference Hub (`/api`)
- Hosts compiler-generated, interactive reference trees generated directly by native language compilers:
  - **Core Broker Engine:** Multi-crate Rustdoc via Cargo/Aspect (`/api/broker/latest/`) covering all 19 workspace crates
  - **Java Streams & Arrow:** Multi-module Javadoc via Bazel (`/api/streams-java/latest/`)
  - **Go Streams & Arrow:** Static Godoc via Bazel (`/api/streams-go/latest/`)
- Dynamically pinned to active GitHub release tags (`v0.5.3`, `v1.1.0`, `v0.1.0-dev`) via `src/data/versions.json`.

### 3. Interactive WebAssembly Consensus Playground (`/docs/playground`)
- `/docs/playground` runs Krabka's real KRaft consensus quorum directly in the browser.
- `playground/` holds the Rust crate binding `krabka-kraft-core` to WebAssembly via `wasm-bindgen`, compiled during the site build via `playground/build.sh`.

### 4. Verification Playground (`/docs/verification-playground`)
- The same `playground/` crate also binds `krabka-verified`, the broker's Creusot-proved decision kernels, through one `run_kernel(name, json)` dispatcher. The page evaluates a curated set of kernels in the browser with each function's `requires` and `ensures` contract beside the result; inputs outside a precondition are reported, not evaluated.
- Each kernel's panel carries a "Proof ledger" drilldown quoting the broker's catalog: what the catalog says the kernel proves, which production code calls it, its Why3 proof sessions, and what the caller must establish.
- Below the explorer, an expandable Stateright model ledger built from `src/data/stateright-models.json` and the catalog lists every model entry point with what it drives, its bounds, its properties, its pinned unique-state counts, and the catalog's own description. The models themselves cannot run in a browser (they are test modules of I/O-bearing crates), so the ledger is an inventory, not a checker.

### 4b. Proof Explorer (`/docs/proof-explorer`)
- `scripts/sync-proofs.mjs` copies the broker's Why3find proof sessions (`verif/**/proof.json`) and the Coma files Creusot generated into `src/data/proof-sessions.json` and `public/proofs/coma/`. The page lists every session by module, shows its proof tree (tactics, provers, times), the `[@expl]` obligations parsed from the Coma with their source spans, and the generated Coma itself.
- With the `why3-web` bundle present (`npm run build:why3-web`, built with Bazel in a Docker container; see `why3-web/README.md`), the page re-checks a session in the browser: Why3 compiled with js_of_ocaml loads the Coma file, splits it with the recorded tactics, and Alt-Ergo compiled to JavaScript discharges each leaf through Why3's SMT-LIB driver.

### 5. Cluster Lab (`/docs/lab`)
- A distributed-systems playground: brokers, a schema registry, producers, consumers and `krabka-client-streams` apps placed on a canvas and run as one sans-IO simulation inside the same `playground/` crate (`playground/src/lab/`, contract in `playground/docs/lab-design.md`). The page owns the clock and the virtual network; faults (kill, restart, wipe, isolate, partition, latency, loss) are deliberate and replay under the seed.
- The front-end is `public/playground/lab/` (hand-written ES modules, no bundler): `app.js` boots and wires the panels, `world.js` wraps the `Lab` wasm class and the animation-frame clock, `canvas.js` is the SVG canvas, `kinds.js` the node-kind catalogue (form fields with the nodes' defaults, the values a node added from the palette starts with, control commands, derived edges, status lines), `views.js` the kind-specific inspector views, and `inspector.js` the inspector with its command bar. Presets live in `presets.js`: three-broker KRaft clusters (a consumer group of two, a schema registry with an Avro producer and a decoding consumer, a Kafka Streams word count), five brokers under a network partition, and a network probe on two diagnostic node kinds. A preset that needs a node kind the loaded module does not carry is badged *full build*, decided by probing the module at boot.
- Durable node state (partition logs, the controller log, registry schemas, the echo node's counter) is drained from the module after every step and written to IndexedDB (`storage.js`: database `krabka-lab`, stores `logs`, `kv`, `scenarios`), keyed by the scenario's `id`; reopening a saved scenario folds it back into `loadScenarioWithState`. Scenarios also export/import as JSON and travel whole inside a share link (`#s=`). `storage.js` also folds every op into an in-memory mirror of each node's image, whatever the persistence setting: turning "Persist to this browser" back on replaces the scenario's stored records with the mirror in one transaction, ahead of any later op, so the ops skipped while it was off leave no gap. A node whose stored data is forgotten while it runs is not written again until it restarts from nothing. Share codes decode without `DecompressionStream` through the raw-DEFLATE decoder in `inflate.js`.
- Several tabs can host one cluster over WebRTC data channels (`session.js`): the hub makes a `?join=` invite link, the spoke shows an answer code to paste back, no signalling server. The hub assigns nodes to peers; frames for nodes hosted elsewhere leave through `drainEgress` and arrive through `pushIngress`. Each tab holds the frames it sends to another tab until its own clock reaches the frame's `deliver_at` (`EgressScheduler` in `world.js`), since the receiver delivers on arrival and tabs share no clock; that keeps link latency real across tabs. Only the host edits the scenario: a spoke's inspector shows node configuration read-only.
- `scripts/check-lab.mjs` (`npm run check-lab`, after `npm run build`) drives the built page in headless Chromium: the preset runs, faults take effect, the echo counter survives a reload and a reopen from the Saved list, re-enabling persistence restores exactly the live state (echo counter and a schema registry), Forget holds on a running node, a share link opens without `DecompressionStream` (and the decoder round-trips `CompressionStream` output past 64 KiB), a share link reproduces the scenario, and two pages host a cluster together over WebRTC with the cross-tab round trip matching the same-tab one and the spoke kept read-only (`--no-webrtc` skips that part). The cluster presets run at 20× in a browser context of their own (`--no-cluster` skips them): one KRaft quorum creates the topic and two consumers share it, the command bars work, killing a partition's leader moves the leadership in the inspector while the group keeps consuming, a reload restores the brokers from IndexedDB and the group resumes from its committed offsets, a broker added to the running scenario observes the quorum, the registry preset's consumer decodes every value, and the streams preset counts words into a store with its changelog topic and answers a query. It needs `playwright` or `playwright-core` (project-local or global) and a Chromium Playwright can find (`npx playwright install chromium`, or `PLAYWRIGHT_BROWSERS_PATH`); it exits 2 when either is missing.

### 6. Correctness & Verification (`/verification`)
- **Verification page (`/verification`):** How the broker establishes correctness: forbidden `unsafe` Rust, Creusot-proved decision kernels in `krabka-verified`, exhaustive Stateright model checking, mutation testing, and differential suites against live Apache Kafka. Its evidence ledger is a tabbed, filterable list of expandable rows: every Creusot ledger row (what it proves, host caller, proof sessions, caller preconditions), every Stateright model, and the other evidence tiers.
- **Verification catalog (`/docs/broker/verification`):** The Creusot proof ledger and Stateright model inventory, synced from `krabka-broker/docs/verification.md`. `src/utils/verification-catalog.ts` parses that file at build time and `src/utils/verification-data.ts` joins it with the site's data files; the counts on the homepage and the verification page come from that parse, so they follow the broker's catalog rather than hand-maintained copy.
- The site makes no benchmark or performance claims. Observability (`krabka-o11y`) and Postgres-compatible compute (`gres`) are documented in their own repositories and are linked as the broader Krabka ecosystem.

### 7. Aggregated Helm Chart Repository

Users add one repository URL:

```bash
helm repo add krabka https://krabka.io/charts
helm repo update
```

`scripts/build-helm-index.sh` walks the `krabka-io` organisation, takes every repository that holds a `charts/` directory, packages each chart, and writes one `index.yaml` over the whole set in `public/charts/`. The `helm-index.yml` workflow runs daily and commits the result.

A component repository can trigger an immediate index rebuild via repository dispatch:

```bash
gh api repos/krabka-io/krabka-io.github.io/dispatches -f event_type=charts-changed
```

The chart signing public key lives in [krabka-io/tooling](https://github.com/krabka-io/tooling), under `charts/`. No key material is stored here.

### 8. Brand Assets & Chart Icons

`/brand` lists every mark with the URL it is served from. Published Helm charts point their `Chart.yaml` icon at `/logo.png`. Treat a rename under `public/brand` or `public/logo.png` as a breaking change for published charts.

### 9. Release Verification & Track Resolution (`/versions`)
- Release tracking across stable, pre-release, and development channels with SLSA Level 3 provenance verification steps and Sigstore signatures.

### 10. Automated Verification Suites
- **Link Integrity Crawler (`scripts/check-links.mjs`):** Recursively crawls every built HTML page in `dist/` and asserts that 100% of internal links resolve to valid targets with zero 404s.
- **Technical SEO Auditor (`scripts/check-seo.mjs`):** Validates title tags, meta descriptions, canonical URLs, Open Graph / Twitter Card tags, single H1 hierarchies, and sitemaps.
- **Code Stub Verifier (`scripts/verify-code-stubs.mjs`):** Parses and validates all code snippets across ingested markdown guides and Astro documentation pages.
- **Cluster Lab End-to-End (`scripts/check-lab.mjs`):** Drives `/docs/lab` in headless Chromium, persistence and WebRTC hosting included.
- **Catalog Parse Check (`scripts/check-catalog.mjs`):** Runs the verification-catalog parser against the synced `docs/verification.md` and fails when the ledger table or the model paragraphs no longer parse, so a layout change in the broker's catalog cannot silently empty the site's ledger. The deploy workflow runs it after every build.

---

## 📁 Repository Structure

```
krabka-website/
├── .github/workflows/
│   ├── deploy.yml            # Builds the site and deploys to GitHub Pages
│   ├── helm-index.yml        # Rebuilds the aggregated chart index
│   └── playground.yml        # Playground build verification
├── playground/               # krabka-playground: WASM consensus simulator (Rust)
│   ├── src/lib.rs            # wasm-bindgen shim over krabka-kraft-core
│   └── build.sh              # Compiles to WebAssembly in public/playground/
├── public/
│   ├── api/                  # Synced compiler API references (gitignored, populated at build)
│   ├── brand/                # Stable brand marks and lockups
│   ├── charts/               # Aggregated Helm repository: index.yaml and tarballs
│   ├── quickstart/           # Downloadable manifests (docker-compose.yml, krabka-cluster.yaml, crds.yaml)
│   ├── favicon.svg / .ico    # Geometric Dungeness crab icon suite
│   ├── robots.txt            # Search engine crawler permissions & sitemap reference
│   └── og-image.png          # Social preview image
├── scripts/
│   ├── sync-docs.mjs         # Multi-repo documentation & release asset sync engine
│   ├── check-links.mjs       # Internal link crawl and resolution validator
│   ├── check-seo.mjs         # Production technical SEO audit suite
│   ├── check-catalog.mjs     # Verification catalog parse check (ledger rows and model notes)
│   ├── verify-code-stubs.mjs # Markdown and Astro code snippet syntax checker
│   └── build-helm-index.sh   # Rebuilds public/charts from component repositories
├── src/
│   ├── components/           # Reusable UI components (Navbar, Footer, VerificationBar, etc.)
│   ├── content.config.ts     # Astro 5 Content Collections glob loader schema
│   ├── content/docs/         # Ingested markdown guides (gitignored, populated by sync-docs)
│   ├── data/
│   │   ├── versions.json     # Live synchronized active release version mappings
│   │   └── ecosystem-versions.json # Cached baseline release and commit status
│   ├── layouts/
│   │   ├── BaseLayout.astro  # HTML shell, OpenGraph tags, JSON-LD Schema.org metadata
│   │   ├── DocsLayout.astro  # Documentation shell with collapsible sidebar & sticky TOC
│   │   └── ProseLayout.astro # Article layout for long-form prose pages
│   ├── pages/
│   │   ├── 404.astro         # Custom branded 404 error page
│   │   ├── api/index.astro   # Searchable API Reference Directory table
│   │   ├── docs/             # Hub landing, module home templates, and [...slug].astro
│   │   ├── features/         # Technical architecture pages (KRaft, Tiered Storage, etc.)
│   │   ├── brand.astro       # Brand guidelines and vector assets
│   │   ├── get-started.astro # Interactive quickstart with Docker Compose and Helm
│   │   ├── index.astro       # Primary ecosystem homepage
│   │   ├── verification.astro # Correctness overview and the expandable evidence ledger
│   │   └── versions.astro    # Release tracks and SLSA artifact provenance
│   ├── styles/
│   │   ├── custom.css        # Ocean dark theme, custom scrollbars, markdown typography
│   │   ├── evidence.css      # Expandable evidence ledger (verification page and playground)
│   │   ├── kernels.css       # Verified kernel explorer styling
│   │   └── playground.css    # Interactive consensus simulator styling
│   └── utils/
│       ├── paths.ts          # Base URL path resolution helper
│       ├── verification-catalog.ts # Parser for the synced broker verification catalog
│       ├── verification-data.ts    # Joins the catalog with src/data for the verification pages
│       ├── verification-loader.ts  # Reads the synced catalog and data files at build time
│       └── versions.ts       # GitHub GraphQL live release resolution
├── astro.config.mjs          # Astro static site configuration
├── tailwind.config.mjs       # Tailwind configuration with @tailwindcss/typography
└── package.json              # Scripts and project dependencies
```

---

## 📜 License

Apache License 2.0. Copyright &copy; 2026 The Krabka Authors.
