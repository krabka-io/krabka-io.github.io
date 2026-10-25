# Website testing

## Required contributor gates

After [preparing guides, proofs, and archive data](development.md#pages-only-preview), run the four `AGENTS.md` gates: `npm run build:site`, `npm run test:website`, `npm run check-links`, and `npm run check-seo`. A pages-only build does not create browser assets. Record missing prerequisites separately from executed failures. The complete deployment gates below are broader; a prose-only edit does not require every optional integration suite locally.

## Choose checks by task

| Change | Additional checks and prerequisites |
| --- | --- |
| Snippets or imported guides | test:snippets, check-snippets; language/integration requirements below |
| Browser tools | Full relevant WASM build, Playwright Chromium, applicable lab/proof checks |
| Payloads or presentation | check-page-budgets, check-site; complete browser assets |
| Verification catalog | sync-proofs, check-catalog, relevant proof checks |
| Published endpoints | check-production, check-api-links; network access and deployed site |
| Benchmark data | test:benchmark, check:benchmark-viewer; archive data and built pages |

## Automated verification suites

- **Link Integrity Crawler (`scripts/check-links.mjs`):** Recursively crawls every built HTML page in `dist/` and asserts that 100% of internal links resolve to valid targets with zero 404s.
- **Technical SEO Auditor (`scripts/check-seo.mjs`):** Validates title tags, meta descriptions, canonical URLs, Open Graph / Twitter Card tags, single H1 hierarchies, and sitemaps.
- **Website Snippets (`scripts/check-snippets.mjs`, `snippets.yml`):** Extracts every displayed snippet from `/get-started`, `/docs/quickstart`, and the Rust, Go, and Java client pages, plus the synced Java/Go getting started guides. New or missing snippets fail coverage. Separate CI jobs run on pull requests, main, and daily to catch drift in the imported guides. Sources, dependency locks, and diagnostics are saved as workflow artifacts.
  - Docker Compose and Helm examples execute against the checked-out `public/quickstart` manifests; Helm uses its own kind cluster and kubeconfig. Readiness polling, interactive stdin, and consumer time limits come from the test harness. CLI and client producers must write the expected value, read back with the standard Kafka consumer.
  - All Rust examples compile against the same git revisions and dependency lock as the playground. The streams test driver runs its existing output assertion; the two infinite consumer loops are compile checks. Both Rust producers, franz-go, Apache Kafka Java, confluent-kafka Python, and KafkaJS run against the actual quickstart broker image.
  - Go/Java guide fragments get missing program structure and fixture inputs. Serdes use the libraries' registry stubs; columnar and streams examples run locally, with output assertions where the guide feeds records. Gradle BOM blocks and the Maven XML resolve their published dependencies. Repository build commands (`go test`, `bazel test`, `./gradlew build`) receive shell syntax checks; their execution belongs to the source repositories' CI.
- **Cluster Lab End-to-End (`scripts/check-lab.mjs`):** Drives `/docs/lab` in headless Chromium without a broker, persistence and WebRTC hosting included. `check-lab-clusters.mjs` runs the cluster presets on real brokers; `check-lab-presets.mjs`, `check-lab-external.mjs`, `check-real-broker.mjs` and `check-wasi.mjs` cover the presets, the external-node contract, the real broker and the WASI runtime.
- **Catalog Parse Check (`scripts/check-catalog.mjs`):** Runs the verification-catalog parser against the synced `docs/verification.md` and fails when the ledger table or the model paragraphs no longer parse, so a layout change in the broker's catalog cannot silently empty the site's ledger. The deploy workflow runs it after every build.

## Lab check details

`npm run check-lab-presets` parses presets without a build. The browser suites below need their stated WASM modules and browser dependencies.

- `scripts/check-lab.mjs` (`npm run check-lab`, after `npm run build`) drives the built page in headless Chromium on a scenario of echo and pinger nodes, so it needs no broker module: the preset runs, faults take effect, the echo counter survives a reload and a reopen from the Saved list, re-enabling persistence restores exactly the live state, Forget holds on a running node, a share link opens without `DecompressionStream` (and the decoder round-trips `CompressionStream` output past 64 KiB), a share link reproduces the scenario, and two pages host a scenario together over WebRTC with the cross-tab round trip matching the same-tab one and the spoke kept read-only (`--no-webrtc` skips that part). It needs `playwright` or `playwright-core` (project-local or global) and a Chromium Playwright can find (`npx playwright install chromium`, or `PLAYWRIGHT_BROWSERS_PATH`); it exits 2 when either is missing.
- `scripts/check-lab-logs.mjs` (plain node, 23 checks) tests the log parser, filters and level directives; `scripts/check-lab-logs-ui.mjs` (`npm run check-lab-logs-ui`) drives the Logs tab in headless Chromium against two WASI test guests: level and text filters, the JSON tree, NDJSON download, and a level change that restarts one node on its disk.
- `scripts/check-lab-clusters.mjs` (`npm run check-lab-clusters`, after `npm run build:broker` and `npm run build`) runs the shipped cluster presets on real brokers at 5×, in the same headless Chromium. In the three-broker preset one KRaft quorum creates the topic and two consumers share it, and every command of the command bars works. In the registry preset the consumer decodes every value, a second registry joins as a secondary and serves a write by forwarding it to the primary, and a reload replays the `_schemas` topic. The streams preset counts words into a store with its changelog topic, and the five-broker preset serves from its majority and heals a partition. It exits 2 when Playwright or the broker module is missing.

## Integration prerequisites

Run snippet integration suites sequentially with port 9092 free. Docker is required. Rust, Go, Java, Python, and Node.js use their native tools. The Java suite needs JDK 21, Gradle 9.6.1, and Maven; Helm needs kind, kubectl, and Helm. Use `npm run check-snippets -- compose` (or helm, rust, go, java, python, javascript). Benchmark workloads have separate hardware requirements in [their canonical instructions](../benchmarks/README.md).

## Full website and deployment checks

Run these checks after syncing guides and proofs and building all browser assets:

```bash
npm run test:website
npm run test:snippets
npm run build:site
npm run check-links
npm run check-seo
npm run check-page-budgets
npx playwright install chromium
npm run check-site
# Network checks against the deployed site, independent of local build results:
npm run check-production
npm run check-api-links
```

The deploy workflow runs the local checks before publishing. It retains the browser screenshots, controlled timings, and payload report as the `website-checks` Actions artifact. Production smoke checks run after a main-branch deployment and on the daily schedule. A successful local build does not establish deployment success.

`check-site` checks the entry pages, operations index, releases, search, lab, and proof explorer at 390 and 1440 pixels in both themes. It checks visible control names, page overflow, keyboard navigation, release filtering, inspector focus return, and ten maintained search queries. It writes `artifacts/site-check/measurements.json` and screenshots. `check-page-budgets` writes `build.json` in that directory and rejects missing pages, missing assets, or payload overruns.

Measurements use a loopback static server with no network throttling, one sample per visit, and a fresh browser context followed by a repeat visit. The server sends `no-store`; the repeat visit can reuse browser compilation and storage, but does not measure a warm HTTP cache. These measurements are separate from field Web Vitals. Keep the profile and sample count with any comparison. Record field LCP, INP, and CLS only when there are enough real visits to report their 75th percentile.

Historical measurements and unresolved manual acceptance work remain in [the issue #91 record](reviews/issue-91.md); automated checks do not complete those tasks.

## Review ownership

| Review owner | Content or check | Review interval |
| --- | --- | --- |
| Website maintainer | Quickstart, Get Started, browser-tools guide, navigation, search and budgets | Monthly and after setup or UI changes |
| Broker maintainers | Imported operations, configuration, qualification, compatibility and migration procedures | Every candidate release and after recovery-contract changes |
| Client maintainers | Runnable language example and imported client guides | Every client release |
| Verification maintainers | Proof and kernel scope, evidence links and guided lab checks | After catalog, broker or scenario changes |
| Component maintainers | Compiler API publication and release metadata | Every release; daily availability checks |
| Website maintainer | Production smoke checks and manual task records | Daily automated checks; monthly review of failures |

Assign a named reviewer from each role when scheduling the manual work. The imported guide's source date is the commit date, not a record of a human review.
