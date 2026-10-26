# Sitemap build plan and maintenance

**Plan revised:** 26 October 2026, against the
[Sitemaps protocol](https://www.sitemaps.org/protocol.html) and
[FAQ](https://www.sitemaps.org/faq.html).

Commit `31cf24f` supplies the initial generation and coverage checks. This
revision adds section-specific generation, byte-aware sharding, protocol and
offline XSD validation, static-link reachability, and release-artifact comparison
in post-deploy CI. These changes are local; they have not been deployed.

The [schema and site-wide gap analysis](sitemap-gap-analysis.md) records direct
official-XSD validation, the sitemap tree, tag-by-tag findings and all-page
coverage checks. The build checker now validates the index and every referenced
child against vendored official XSDs using Python lxml. Keep the analysis's
explicit boundary checks, canonical diagnostics and index-date review in the
implementation criteria below.

## Findings

On 25 October 2026, the published index at
<https://krabka.io/sitemap-index.xml> linked to `sitemap-0.xml`. That file
contained 154 URLs. The index is an XML list of sitemap files, not a page list.
The response was not empty. The child sitemap included `/docs/api/`, which
redirects to `/api/`. Exclude that alias from the generated URLs. A browser display alone does not prove its contents.

The Astro sitemap integration already runs in both site build commands.
The SEO check tested file presence only. An empty or stale sitemap could pass.

## Plan

### Protocol baseline

Use these acceptance criteria from the
[XML protocol](https://www.sitemaps.org/protocol.html):

- Validate UTF-8, XML escaping, the sitemap namespace, required elements and
  element structure against the official sitemap and index schemas.
- Check absolute, correctly encoded URLs shorter than 2,048 characters;
  enforce the sitemap's host, scheme, port and directory scope.
- Limit each child to 50,000 URLs and each index to 50,000 sitemap entries;
  limit each file to 52,428,800 uncompressed bytes.
- Keep index references on the same site; retain root-level files.
- Validate optional metadata when supplied; never fail solely because it is
  absent. Page `lastmod` means page modification, while index `lastmod` means
  child-file modification. Validate dates, frequency values and priority range.
  Generate dates only after reliable per-page provenance is available.

The [FAQ](https://www.sitemaps.org/faq.html) recommends one version of each URL,
no session IDs, and discovery through `robots.txt` or search-engine submission.
A single sitemap would be sufficient at this size, but the chosen layout
separates website and documentation URLs for independent inspection. Keep the
existing public index URL and robots advertisement, now with two useful groups.

### Site policy, separate from the protocol

For this site, include each indexable built HTML page once, at its canonical
`https://krabka.io` URL. Exclude redirects, error pages, `noindex` pages and
non-page artifacts. Keep component deployments outside this build's inventory.
The protocol does not require exhaustive HTML coverage or prohibit all query
strings; this site's stricter coverage and clean-URL rules are local policy.

Treat freshness as agreement with the current build, not a hard-coded page
count or a new timestamp on every rebuild. The build checker skips excluded
HTML files without listing them. It reports an unexpected sitemap URL if the
sitemap includes an excluded route.

### Implementation sequence

1. Retain `@astrojs/sitemap` and generation from Astro routes after content sync;
   keep `sitemap-index.xml` and the robots advertisement, with `docs` and
   `website` chunks as described below.
2. Extend validation with the protocol baseline, using pinned local copies of
   the official schemas so builds do not depend on a schema-server request.
3. Keep canonical coverage validation separate from schema validation, with
   diagnostics that identify the file, URL and failed rule.
4. Exercise both validation layers in both builds and the SEO audit, using the
   regression cases below before declaring the revised plan implemented.
5. After an approved deployment, fetch robots, the index and all child bodies;
   apply the same protocol checks and compare their URL set with that release's
   build artifact to detect an outdated or partial deployment.

### Section layout

```text
robots.txt
└── sitemap-index.xml              <sitemapindex>
    ├── sitemap-docs-0.xml         <urlset>: /docs and /docs/**
    └── sitemap-website-0.xml      <urlset>: all other indexable site pages
```

The groups are mutually exclusive and include authored docs pages as well as
synced guides. `/api` belongs to the website group; separately deployed component
API trees remain outside this build. Astro adds numbered shards within each
group by count; a following integration splits serialized XML by uncompressed
bytes too, adding `-part-N` suffixes when necessary. Do not hard-code page counts or filenames in consumers:
follow the index. Empty groups must not create empty indexed children.

Verify the generator's splitting behavior against both size and entry limits;
do not assume that a URL-count split also satisfies the byte limit.

Do not add automatic crawler pings or account submissions in this work.
Keep robots-based discovery; any account submission is a separate authorized
action checked against that engine's current documentation.

### Implemented validation and discovery checks

The build and published checkers apply the same explicit byte, entry, UTF-8,
URL-length/encoding, origin/scope and optional-metadata rules, followed by the
official XSDs. Published XML is validated from its original response bytes.
The local checker rejects multiple canonical links and checks exact canonical
HTML coverage. Both checkers reject indexed routes or sitemap files blocked by
robots rules, including named crawler groups. This is the site's discovery
policy, not a requirement that every crawler must have access under the protocol.
Any intentional crawler exclusion requires an explicit policy change.

Each child must use the docs or website section name and contain only that
section's routes. The local checker also parses HTML anchors with lxml and
requires every indexable page to be reachable from the homepage. It resolves
relative links and `<base>` URLs, decodes HTML entities, ignores fragments and
normalizes trailing slashes. This checks static link traversal, not JavaScript
navigation or separately deployed component sites. The operations page links
the imported small-record profiling and RUSTSEC exception guides without
modifying their generated content.

After successful local validation, `check-sitemap` writes
`dist/sitemap-inventory.json`. This sorted URL inventory is packaged inside the
same Pages artifact as the HTML and XML. Post-deploy CI downloads that run's
`github-pages` artifact, extracts the inventory, follows live robots sitemap
declarations, validates every referenced XML body and compares the complete
published URL set. Missing and extra URLs fail, even if the homepage is present.
This establishes URL-inventory agreement, not byte-identical HTML or metadata.
Standalone production smoke checks without an artifact explicitly report
freshness as unverified. No live deployment is performed by local checks.

## Commands

Local builds and website tests require Python 3 with lxml. Install the pinned
validator in an activated virtual environment before running the commands below:

```sh
python3 -m venv /path/to/validator-venv
. /path/to/validator-venv/bin/activate
python3 -m pip install -r scripts/requirements-sitemap.txt
```

The deploy workflow installs it before tests and both build paths invoke it via
`check-sitemap`. Missing Python/lxml and invalid XML fail the build; validation
never downloads schemas. Sources and hashes are in
[`scripts/schemas/README.md`](../scripts/schemas/README.md).

```sh
# Sync current component guides, then build all assets and sitemap files.
npm run build

# Build pages and sitemap files from the current local content.
npm run build:site

# Check an existing build. The optional argument replaces dist/.
npm run check-sitemap
npm run check-sitemap -- path/to/build

# Check published protocol validity; freshness is unverified without an artifact.
npm run check-production

# Compare against the inventory extracted from the deployed Pages artifact.
npm run check-production -- https://krabka.io path/to/sitemap-inventory.json
```

Both builds generate `dist/sitemap-index.xml` and numbered docs/website children before
validation. They fail if sitemap coverage differs from the indexable HTML output.
New and removed pages therefore update the sitemap without a manual edit.
`build:site` does not sync upstream guides; run `npm run sync-docs` first when
those guides must be refreshed. CI uses the full build, including docs sync.

`public/robots.txt` advertises the index. Publish the whole `dist/` directory,
not the index alone. Component API sites have separate deployments and are
outside this site's sitemap. Do not add their URLs to this build's page list.

### Optional metadata and Markdown dates

See [SEO and sitemap design](architecture.md#seo-and-sitemaps) for the date
pipeline and provenance requirements. Generation uses the maximum per-file Git
commit timestamp across each route's content inputs. Absent dates remain valid. The build writes a per-route
`dist/lastmod-report.json` with evidence and omission reasons.

- `lastmod`: accept a real `YYYY-MM-DD` date or a timezone-qualified W3C timestamp;
  reject impossible calendar dates, invalid times, duplicate values and nesting.
- `changefreq`: accept `always`, `hourly`, `daily`, `weekly`, `monthly`, `yearly`
  or `never` when supplied. It is a hint, not a crawl schedule. Do not invent a
  uniform value. [Google ignores this field and `priority`](https://developers.google.com/search/docs/crawling-indexing/sitemaps/build-sitemap#additional-notes-about-xml-sitemaps),
  but can use consistently accurate `lastmod` values.
- `priority`: remains optional; supplied decimal values must be between 0 and 1.
- Index entries permit `lastmod`, not page-only `changefreq` or `priority`.

Never substitute build time, sync time, filesystem mtime or repository HEAD date
for a page's last significant update. Missing reliable history means omitting
that page's date, not failing the site. No metadata field is mandatory.

## Acceptance checks

- The current build has separate docs and website children; each contains only
  its assigned routes. Every indexed child is nonempty and readable.
- Every indexable built HTML page has exactly one URL across those children.
- Redirect stubs, error pages and `noindex` pages have no sitemap entry.
- Missing or removed page fixtures fail validation until the sitemap changes.
- Production checks read XML bodies, not only HTTP headers.
- Schema fixtures reject invalid namespaces, element order and missing or
  repeated required elements, but accept valid escaped and non-ASCII URLs.
- Boundary fixtures accept the entry and byte limits and reject values above
  them; URL-length fixtures reject values of 2,048 characters or more.
- Scope fixtures reject foreign hosts, wrong schemes, mismatched ports and
  out-of-directory URLs. Optional-metadata regressions accept missing fields and
  reject invalid dates, frequency values, order, duplicates and namespaces;
  index fixtures reject page-only hints. Date-provenance tests check automatic Git
  dates, ignored cosmetic commits, content-only dependencies and unchanged rebuilds.
- A partial or stale published URL set fails comparison with the release
  artifact even when all fetched XML is well formed.

These checks have focused regressions in `scripts/sitemap.test.mjs` and
`scripts/site-checks.test.mjs`. Run them and the repository checks after code
changes. A sitemap supports discovery; it does not guarantee indexing.

Merge and deployment need separate maintainer approval. Local validation does
not prove that a change has reached the live site.

## Maintain page dates

Page dates update automatically from content-file Git history. No per-page
review entry or SHA-256 lockfile is required. Keep new routes in
`scripts/lastmod-routes.mjs` and declare content inputs read outside static
imports. Do not add global layout/chrome, styles or dependency configuration.
Synced Markdown uses upstream per-file records; CRD routes use the matching
YAML records plus website content inputs. Generated benchmark snapshots use
the tracked source archives and reports.

For a bulk cosmetic-only change, optionally add the commit SHA or an unambiguous
prefix of at least seven hexadecimal characters to
`scripts/lastmod-ignore-revs.txt`. The resolver skips those commits when walking
file history, preserving the previous content date. The list also applies to
upstream guide/CRD history at sync time. Leave ordinary content commits out.

Inspect `dist/lastmod-report.json` for each route's inputs, per-file evidence,
computed date or omission reason. Complete clean Git inputs emit dates without
human review. Non-Git, shallow, dirty, untracked or unavailable inputs omit
dates, as do unmapped routes and the live API-backed release/version indexes.
Coverage is diagnostic; do not hide missing provenance to increase it.

Run `node --test scripts/lastmod.test.mjs` with `TMPDIR` set to a private scratch
directory. The fixtures check automatic maximum dates, ignored commits,
content/chrome separation, clean and dirty inputs, shallow history, custom guide
paths, renames, CRD kind matching, rewritten history, byte/count splits, URL
removal and byte-identical rebuilds. The full website suite includes these
tests. Run the four contributor gates before requesting review. No live
deployment is part of these commands.
