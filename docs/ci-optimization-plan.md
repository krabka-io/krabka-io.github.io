# Website CI optimization plan

Status: SDK reconstruction implemented; hosted timing comparison pending. The
remaining workflow and output-cache changes are proposed. Reviewed 27 October
2026 against commit
`60fac298189ea16a438ce75cd39050b3b5cbaa69`, with workflow/installer scope
cross-checked through `b315812336be92a32386052bb2c56dfe428d2ec5`. This layer implements the stable SDK path and its ownership rules; it does not
change the deployment workflow. [CI selection](ci.md) and
[verified tool downloads](tool-downloads.md) describe current behavior.

## Evidence and priorities

The [merge-group run](https://github.com/krabka-io/krabka-io.github.io/actions/runs/40577459974)
and [main push run](https://github.com/krabka-io/krabka-io.github.io/actions/runs/40579980216)
both tested commit `60fac298189ea16a438ce75cd39050b3b5cbaa69`.
The queue build passed at 20:38:50 UTC. Main started a new run at 20:39:24,
which completed deployment and smoke at 20:56:42. From queue-run creation to
main-run completion was 34m14s. This is not a measurement from the merge click.

Use small, focused implementation changes in this order:

1. Stabilize the verified WASI SDK path and measure warm broker builds. Reduce
   workflow token permissions in a separate focused change.
2. Promote fully validated merge-queue site bytes on an exact-commit main push.
3. Add exact-input caches for complete broker and playground outputs.
4. Split preparation and browser checks into balanced jobs only if measured PR
   latency still justifies the checkout, setup, transfer, and runner overhead.

Keep every existing trigger, selected suite, preset, repeat count, budget, and
failure-evidence collection. Keep the required check named `build`. Deployment
remains blocked on successful validation of the bytes being deployed.

## Stable SDK reconstruction

The installer currently uses `mkdtemp` for each installation. The SDK helper
puts that path into `CC_wasm32_wasip1`, `AR_wasm32_wasip1`, and
`CFLAGS_wasm32_wasip1`. Cargo reruns build scripts when tracked environment
values change. This is a concrete invalidation mechanism, not proof that it is
the only reason for every rebuilt crate or all 5m11s of broker compilation.

For managed WASI SDK installations, resolve this layout to an absolute path
under the private tool cache (normally `playground/` relative to the repo):

```text
.tools/verified/wasi-sdk-<version>-<host>-<sha>/wasi-sdk-34.0-<arch>-<os>/
```

The outer directory is the per-version/host/digest installation directory; the
inner directory is the pinned archive root (`34.0` is the current version).
Keep cached `.download` archives outside that installation directory. At the
START of every build, rehash the archive, clear only the owned per-digest
installation directory, and freshly extract the verified archive there. Do not
trust extracted files or marker files. Preserve archive validation and explicit
caller-managed toolchain behavior. A digest change selects a new path.

CI runs one build per runner. Two concurrent local builds need only one
per-digest `flock`, held through reconstruction, compilation, and cleanup. Keep
the lock file outside the wiped directory to preserve the locked inode. Do not
grow this into a locking system. Never remove an SDK another build is using. On exit, either remove the
managed SDK deletion trap or point `WASI_SDK_MANAGED_DIR` only at the outer
per-digest installation directory above. Never at `.tools/verified/` itself.
If exit cleanup removes the installation, the next build still reconstructs it at
the same absolute path. Stable paths help Cargo only if other tracked build
environment values remain unchanged. Do not remove unrelated downloads or tools.

Add installer regressions for stable paths, fresh reconstruction after extracted
file tampering, corrupt archives, cleanup boundaries, and concurrent callers.
Retain malicious-archive and platform tests. Compare matched hosted warm builds
with Cargo fingerprint traces; report remaining invalidations and cold cost.
Update [tool-downloads.md](tool-downloads.md) with the implemented path and
ownership rules in the same change. Do not claim recovered minutes in advance.

## Least privilege and checkouts

The workflow currently grants `pages: write` and `id-token: write` at workflow
scope. Default to `contents: read`; give Pages/OIDC rights only to the deployment
job. Give the promotion lookup/download job `actions: read`. Artifact transfer
does not justify granting build/test jobs Pages or OIDC write permissions.

The current build checkout uses full history and LFS. Keep sufficient history
for selection; downstream jobs normally need only the exact shallow revision.
Fetch LFS objects only where benchmark archive preparation needs
`benchmarks/archive/*.tar.gz`. Verify any other history-sensitive consumer before
making it shallow. Assembly should transfer generated benchmark inputs so
browser jobs do not need their own LFS download.

## Exact-commit promotion

Only a `push` to `refs/heads/main` may attempt this fast path. Query GitHub's
API for a run in this repository whose workflow is `.github/workflows/deploy.yml`,
event is `merge_group`, `head_sha` equals this run's full `GITHUB_SHA`, and
conclusion is `success`. The current selector runs full selection for every
non-PR event, including `merge_group`; retain that invariant in contract tests.
GitHub-authenticated run metadata is the producer trust boundary. Do not add a
custom provenance manifest, companion checksum file, or separate attempt protocol.

Exact-SHA matching also handles batching: only the final group's SHA becomes
main; intermediate groups' artifacts are not candidates. Never match a nearest
ancestor, similar tree, latest successful run, or PR-head SHA. If a future merge
strategy changes SHA identity, the lookup misses and a full build is required.

The queue producer freezes the complete tested site and uploads it as an ordinary
Actions artifact with `retention-days: 1`. A candidate is eligible only after its
producer run succeeds. One-day retention limits storage for unused intermediate
queue groups; expiry is an expected full-build fallback, not a release failure.

The main consumer downloads the selected immutable artifact ID using the smoke
job's existing pinned v8 action:
`actions/download-artifact@9000827ccba6bdab643e8b6fd33ac0654aef8333`.
Supply `artifact-ids`, `run-id`, and the current run's own `github-token`
(`actions: read`), and explicitly set `digest-mismatch: error`. v8 verifies the
server digest itself; no hand-rolled hashing or companion checksum is needed.
Require a present server digest in candidate metadata. Download/extract into
clean staging, validate the package, then re-upload the same files as this main
run's Pages artifact. Set `promoted=true` only after all these steps succeed.
No resynchronization, rebuild, or content mutation occurs after queue validation.
Production smoke stays unchanged; deploy dependencies/conditions change below.

Schedules, release dispatches, and manual runs retain fresh full synchronization,
build, and validation. A main push without an eligible queue producer also uses
that path. Missing/expired artifacts, lookup/download failure, missing or mismatched
server digests, or unusable packages discard the candidate and trigger the full
build. If fallback fails, deployment stays blocked. Never substitute another SHA.
Record the source run and fallback reason in the ordinary workflow summary.

Promotion intentionally uses the queue's synchronized sibling snapshots. Nightly
and release-triggered full builds remain the freshness backstop; the same source
SHA does not imply unchanged mutable siblings. No custom snapshot metadata
protocol is required for this promotion change.

### Gate-safe promotion graph

A skipped required check counts as passing. `promote` must run on every event,
with no main-only job-level condition. It returns `promoted=false` quickly except
on a push to `refs/heads/main`; restrict only its lookup/download steps to that
fast path. Expected lookup/download/validation failures return false rather than
failing the job. Initialize false; publish true only after complete promotion.

Require these job-level dependencies and conditions (job bodies omitted):

```yaml
build:
  needs: promote
  if: ${{ !cancelled() && needs.promote.outputs.promoted != 'true' }}
deploy:
  needs: [promote, build]
  if: >-
    ${{ always() && github.ref == 'refs/heads/main' &&
        (needs.build.result == 'success' ||
         (needs.build.result == 'skipped' && needs.promote.outputs.promoted == 'true')) }}
```

The build status function overrides implicit success gating: failed/skipped
promote or absent outputs still run build unless cancelled. Build must never
skip on `pull_request` or `merge_group`. Failed/cancelled builds block deploy;
a skipped build authorizes deployment only with explicit `promoted=true`.
Scheduled/manual/release main builds still deploy after full validation.

In the implementation PR, replace `assert.equal(build.needs, undefined)` in
`scripts/deployment-contract.test.mjs` with graph-aware contracts and mutations:

- Reject build skipped on `pull_request` or `merge_group`.
- Reject promote failure suppressing build, including removal of `!cancelled()`
  that restores implicit success gating. Cover skipped promote and empty outputs.
- Exercise cancellation, successful promotion, fallback success/failure, and
  deploy rejection when build is skipped without explicit promotion.
- Assert the existing v8 pin, cross-run inputs, and explicit
  `digest-mismatch: error`; reject mutations removing or weakening that setting.

Keep PR 3 limited to queue artifact upload/retention, authenticated lookup,
action-native digest verification, current-run Pages upload, full-build fallback,
these gate conditions, and matching contract/mutation tests. Preserve all triggers
and production serialization through unchanged smoke.

## Generated-output inventory and later job split

Inventory generated files explicitly. The transfer to selected checks includes:

- `dist/`, frozen before its validation.
- `public/playground/broker/krabka-broker.wasm`.
- `public/playground/krabka_playground.js`, `krabka_playground_bg.wasm`, and all
  emitted playground side files.
- The complete `public/why3-web/` bundle and `public/proofs/coma/` tree.
- Synchronized `src/content/docs/` and generated `src/data/` inputs, including
  `proof-sessions.json`, `versions.json`, and `benchmark-archive.json`.
- `public/benchmarks/archive-data/`, needed with its index by benchmark checks.

Confirm this inventory against each consumer when implementing the split.
Do not restore an old whole `public/` tree over tracked frontend files. Build and
sync once, then transfer the exact inputs used by assembly rather than refetching
mutable siblings in browser jobs. Output-cache keys include all source,
dependency, compiler, verified-tool, build-script, profile, and flag inputs;
prefix hits do not authorize skipping a build. Rebuild on a miss or invalid data.

If a split is still needed, prepare broker, playground, and Why3 in parallel,
assemble once, then run site, proof, lab-experiment, rebalance/share, and security
browser lanes. All selected lanes feed the fail-closed `build` aggregate before
deployment. Preserve the independent eight-shard snippet-execution workflow.

## Concurrency and acceptance

The workflow currently serializes main, schedule, and dispatch runs in one
production concurrency group through smoke. Promotion shortens a main run's work
but does not let it bypass an already running nightly build. Keep serialization
for the first promotion change. If contention remains material, prepare outside
the deployment lock and serialize promotion/deployment/smoke, with a stale-head
guard inside the lock so an older completed build cannot overwrite newer main.
Do not cancel an active deployment or let main cancel another release's smoke.

Test exact-match promotion, batched final versus intermediate SHAs, wrong
SHA/repository/workflow/event, failed or cancelled producers, the full-selection
merge-group invariant, one-day retention, expired/missing artifacts, lookup/download
failure, missing/corrupt server digests, admin-bypass pushes, and forced-refresh events.
Mutation tests should reject gate bypasses and altered validation conditions.
Test cache hits, misses, changed pins/flags, tampering, selected skipped jobs,
and concurrent nightly/main behavior in the changes that introduce those paths.

Record hosted queue delay, time to required `build`, post-merge time to deploy
and smoke, runner minutes, transfer cost, and cache/promotion hit rates separately.
Measure promotion separately from a warm full build and cold full build. A
couple-minute post-merge deploy is a hypothesis, not a measured promise. Keep CI
and tool-download documentation synchronized with each implementation change.

## External references

- [GitHub merge-group event and SHA](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#merge_group).
- [Cross-run artifact download inputs and digest handling](https://github.com/actions/download-artifact/blob/v8/README.md).
- [Artifact API metadata and server digest](https://docs.github.com/en/rest/actions/artifacts).
- [Artifact identity, digest outputs, and retention](https://github.com/actions/upload-artifact).
- [Cargo environment change detection](https://doc.rust-lang.org/cargo/reference/build-scripts.html#rerun-if-env-changed).

- [Dependency skips and job conditions](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax#jobsjob_idneeds).
