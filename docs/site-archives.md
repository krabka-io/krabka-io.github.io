# Composed site archive validation

The component catalog in `src/data/api-reference.json` owns the API entry paths.
`scripts/site-archives.mjs` derives the root website and component archive mounts
from that catalog. An API entry can be below the archive root: Java mounts at
`/krabka-streams-java/` and renders `api/index.html`.

Generate the desired composition with:

```sh
node scripts/site-archives.mjs > composition.json
```

Use the pinned tooling checkout to freeze artifact provenance separately:

```sh
python3 .site-tooling/actions/validate-composed-pages/compose.py \
  --snapshot --manifest composition.json --lock-out docs/site-archives.lock.json \
  --work-dir artifacts/site-archives-work --report artifacts/site-archives-snapshot.json
node scripts/site-archives.mjs --check-lock docs/site-archives.lock.json
```

Snapshot creation needs read access to Actions metadata and artifacts for every
component repository. Review its artifact identities, source commits, and digests
before committing the lock. Do not fabricate missing artifact identities or use a
floating latest-successful lookup during validation. Expired artifacts require a
new reviewed snapshot.

## Manual integration check

`Validate pinned site archives` takes the exact website commit and its checked-in
lock path. It verifies that the lock covers the entire current catalog, checks
all pinned archives through shared tooling, and uploads its composition and
validation report. Missing locks or unavailable artifacts fail the check. This
workflow has read-only permissions and does not deploy a website.

There is no bootstrap lock in this change. The initial snapshot still needs real
artifacts for the root website and every component. A successful snapshot is not
a successful validation: run the combined link and rendered-page checks too.

## Production gate activation

This manual check is not yet a production deployment gate. Before enabling that
gate:

1. Populate and validate the complete baseline lock. Resolve failures in all
   mounted archives, not only the candidate component.
2. Run candidate validation with the new archive replacing exactly its baseline
   component. Record the candidate bytes and the full baseline lock identity.
3. Require validation before deployment, both for a fresh website build and
   exact-commit queue promotion. Do not reuse a validation result for a different
   archive set. Preserve independent component deployment ownership.
4. Establish lock refresh and artifact retention policy, so an expired baseline
   fails clearly instead of selecting replacement artifacts implicitly.

The existing `deploy.yml` and its queue promotion behavior are unchanged here.
