# Sitemap schema and coverage gap analysis

**Checked:** 25–26 October 2026 (UTC). **Source:** local commit
`31cf24f218c6a04173fea101ebf405b36d7bc024`, its existing `dist/` output,
and the live website. This is an analysis, not an implementation or deployment.
The local build was not regenerated for this audit.

**Follow-up, 26 October:** this report preserves the pre-change audit snapshot.
The [revised plan](sitemaps.md) and [SEO design](architecture.md#seo-and-sitemaps)
supersede its one-child layout recommendation. Local generation now separates
website and documentation children, and supplied optional metadata is validated.
The build and published checkers now enforce the vendored official XSDs and explicit
non-XSD protocol boundaries for the index and every referenced child. Site-wide date
generation remains planned; none of these changes establishes deployment.

## Conclusions

The generated XML uses the correct Sitemaps 0.9 vocabulary. Both live files
and both local files passed validation with the respective official XSD,
using Python `lxml.etree.XMLSchema`. No required XML tags are missing.
However, the repository checker does not perform XSD validation. Passing
the checker does not establish schema or complete protocol compliance.

The published sitemap contains one inappropriate page entry: `/docs/api/`.
It is a `noindex` meta-refresh redirect with canonical `https://krabka.io/api`.
Remove its sitemap entry, not the redirect page. The local configuration
already excludes it. The local sitemap has no missing or extra URLs relative
to its indexable built HTML inventory.

## Actual sitemap tree

```text
https://krabka.io/robots.txt
  Sitemap: https://krabka.io/sitemap-index.xml
    <sitemapindex>                         [siteindex.xsd]
      <sitemap><loc>…/sitemap-0.xml</loc></sitemap>
        https://krabka.io/sitemap-0.xml
          <urlset>                        [sitemap.xsd]
            <url><loc>…page URL…</loc></url>
```

`sitemap-index.xml` is an index, not the page sitemap. Its only current
child is `sitemap-0.xml`. The installed Astro integration uses this layout
and creates further numbered children when its entry limit is reached.
There are no section-specific or nested indexes in the inspected output.

`sitemap.xml` is not a required filename. Retain the advertised index URL;
renaming it provides no schema correction. One child is sufficient today.

## XML inventory

| Snapshot | File | Root | Entries | Bytes | Official XSD |
| --- | --- | --- | ---: | ---: | --- |
| Live | `sitemap-index.xml` | `sitemapindex` | 1 sitemap | 180 | Pass: `siteindex.xsd` |
| Live | `sitemap-0.xml` | `urlset` | 154 URLs | 11,701 | Pass: `sitemap.xsd` |
| Local | `sitemap-index.xml` | `sitemapindex` | 1 sitemap | 180 | Pass: `siteindex.xsd` |
| Local | `sitemap-0.xml` | `urlset` | 153 URLs | 11,652 | Pass: `sitemap.xsd` |

Both live files returned HTTP 200 with `application/xml`, without an HTTP
redirect. Both use the correct namespace:
`http://www.sitemaps.org/schemas/sitemap/0.9`. Keep that exact namespace;
do not replace its `http` prefix with `https`.

### Tag-by-tag findings

| Element or declaration | Current state | Action |
| --- | --- | --- |
| XML declaration and UTF-8 | Present; inspected XML parses as UTF-8 | Retain; validate input bytes in the build checker |
| Index `sitemapindex / sitemap / loc` | Present, correct namespace, one child | No missing required tags |
| Child `urlset / url / loc` | Present, correct namespace, one location per entry | Remove live `/docs/api/` entry through the existing local filter |
| Page `lastmod` | Absent from every URL entry | Optional; keep omitted until reliable content-modification dates exist |
| Index `lastmod` | Absent | Optional; if introduced, describe child-file modification, not page modification |
| Page `changefreq` | Absent from every URL entry | Optional; do not invent values |
| Page `priority` | Absent from every URL entry | Optional; do not add uniform placeholder values |
| `xmlns:xsi` and `xsi:schemaLocation` | Absent | Not a compliance defect; explicit external-XSD validation succeeds without document hints |
| `news`, `xhtml`, `image`, `video` namespaces | Declared on child; no extension elements used | Harmless generator defaults; optional cleanup, not missing content |
| Alternate-language, image, video or news extension elements | Absent | No base-protocol requirement to add these; assess separately if a concrete discovery need arises |

There is no need to add sitemap XML tags to individual HTML pages. Their
canonical links, indexing directives and route inventory inform sitemap
generation and validation; the XML tags belong in the sitemap files.

## Site-wide page checks

Every one of the 154 published sitemap URLs was fetched and inspected for
HTTP status, final URL, content type, canonical links, meta refresh,
robots metadata and `X-Robots-Tag`. The only flagged entry was `/docs/api/`:

- HTTP 200 with a meta-refresh redirect to `/api`.
- Canonical `https://krabka.io/api`, not its own sitemap URL.
- Robots metadata `noindex`.

The other 153 entries returned HTML successfully with the expected canonical,
without the redirect or indexing-exclusion conditions checked above.

The live inventory covers the homepage and the `/api`, `/benchmarks`, `/brand`,
`/docs`, `/features`, `/get-started`, `/news`, `/releases`, `/verification`
and `/versions` sections. There are 135 `/docs` entries, including the alias
that should be excluded.

The local output contains 165 HTML files: 153 indexable pages, nine redirects,
one error page, one noindex search page and one chart distribution artifact.
The local sitemap matches the 153 indexable canonicals exactly, with no
duplicates, missing pages or unexpected entries. Live minus local is precisely
`https://krabka.io/docs/api/`; local minus live is empty.

Scope: this audits the website repository's complete existing HTML build
and every live sitemap entry. It is not a crawl proving that no additional
unlisted page exists anywhere on the deployed host. Separately deployed
component documentation/API sites under the same host need their own
inventory and ownership review; their coverage was not verified here.

## Tooling gaps and plan changes

1. **Add actual XSD validation.** Pin both official schemas and their source
   hashes. Validate indexes with `siteindex.xsd` and children with `sitemap.xsd`.
   Four probes were accepted by `readSitemap` but rejected by the official
   XSD: an unknown same-namespace tag, an entry in the wrong namespace,
   invalid/out-of-order metadata, and an element nested inside `loc`.
2. **Retain separate protocol checks.** The XSDs do not enforce all protocol
   rules. In particular, their entry declarations use `maxOccurs="unbounded"`;
   enforce the 50,000-entry and 52,428,800-byte limits explicitly. Enforce raw
   UTF-8 validity, URL encoding/length and directory scope. The existing URL
   helper checks origin and clean-URL policy but does not cover all those rules.
3. **Resolve the URL-length boundary explicitly.** The protocol prose says
   fewer than 2,048 characters; the XSD permits 2,048. Follow the plan's stricter
   limit as an additional protocol check, not as a claim about the XSD.
4. **Check splitting by bytes as well as entries.** The installed writer splits
   by entry count. A 50,000-entry chunk is not proof of a valid byte size.
5. **Preserve canonical coverage checks and improve diagnostics.** Report
   excluded pages with their reason. Consider structured HTML parsing and
   explicit rejection of multiple canonicals rather than selecting the first.
6. **Compare deployment against its release artifact.** The current published
   checker reads XML and requires the homepage, but it does not compare the
   complete URL set with the deployed release inventory. It also checks robots
   as an asset rather than validating its body advertisement. Add both checks;
   today's 153/154 comparison is diagnostic, not release-provenance verification.
7. **Test optional metadata if added.** Check order, cardinality, dates,
   frequency enumeration and priority bounds. The installed integration's
   writer can derive index `lastmod` from the maximum page `lastmod` in a chunk;
   review or override that behavior before enabling page dates. It does not
   establish the child file's modification time. No dates are emitted today.

The revised plan already calls for most of these controls. The additions from
this analysis are concrete XSD pass results, demonstrated checker bypasses,
the complete page inventory, the XSD/prose boundary distinction and the
generator's index-date behavior. Do not describe the planned controls as
implemented until they exist and their regressions pass.

## Evidence and verification

The audit used these private, untracked artifacts:

- `xml-evidence.json`: live/local XML results, full URL lists, schema sources
  and SHA-256 hashes, robots response and URL-set comparison.
- `page-evidence.json`: all 154 live page results and all 165 local HTML paths.
- `checker-probes.json`: four malformed fixtures, checker acceptance and XSD errors.
- `audit-pages.py`: repeatable page inventory and HTTP audit.
- `sitemap.xsd`, `siteindex.xsd`, `live-sitemap-index.xml`,
  `live-sitemap-0.xml`, `live-robots.txt`: downloaded source evidence.

`npm run check-sitemap` passed for the existing local output: 153 pages in
one child. No production code changed, and no push or deployment occurred.

Official references:

- [Sitemaps XML protocol and tag definitions](https://www.sitemaps.org/protocol.html)
- [Page sitemap XSD](https://www.sitemaps.org/schemas/sitemap/0.9/sitemap.xsd)
- [Index XSD](https://www.sitemaps.org/schemas/sitemap/0.9/siteindex.xsd)

Live evidence:

- [Index](https://krabka.io/sitemap-index.xml)
- [Page sitemap](https://krabka.io/sitemap-0.xml)
- [Robots discovery](https://krabka.io/robots.txt)
- [Alias to exclude](https://krabka.io/docs/api/)


## Approved follow-up implementation, 26 October 2026

The sitemap branch now implements explicit entry/byte/URL/scope checks,
byte-aware generation, canonical conflict detection, robots access checks,
docs/website section regressions and static homepage reachability. Contextual
links from the website-owned operations page address the two discovered orphan
guides. Published validation now includes the official XSDs and a full URL-set
comparison with an inventory packaged inside the deployed Pages artifact.
See [current implementation and commands](sitemaps.md) for the maintained
behavior. Earlier observations above describe the audited pre-follow-up state.
Site-wide `lastmod` generation remains deferred; no deployment is implied.
