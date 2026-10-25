# Sitemap build plan and maintenance

## Findings

On 25 October 2026, the published index at
<https://krabka.io/sitemap-index.xml> linked to `sitemap-0.xml`. That file
contained 154 URLs. The index is an XML list of sitemap files, not a page list.
The response was not empty. The child sitemap included `/docs/api/`, which
redirects to `/api/`. Exclude that alias from the generated URLs. A browser display alone does not prove its contents.

The Astro sitemap integration already runs in both site build commands.
The SEO check tested file presence only. An empty or stale sitemap could pass.

## Plan

1. Keep `@astrojs/sitemap` as the generator. Generate from Astro routes after
   docs sync in the full build. Do not maintain a separate URL list.
2. Check the generated index and every listed sitemap against the built HTML.
   Reject empty or malformed XML, duplicate URLs, wrong origins, missing pages,
   and URLs for redirects, error pages or pages with `noindex`.
3. Run the check in both build commands and in the SEO audit. Add regression
   fixtures for missing, stale and excluded pages.
4. Fetch the published index and its children in the production smoke check.
   Reject missing, empty or malformed XML and unexpected redirects.

## Commands

```sh
# Sync current component guides, then build all assets and sitemap files.
npm run build

# Build pages and sitemap files from the current local content.
npm run build:site

# Check an existing build. The optional argument replaces dist/.
npm run check-sitemap
npm run check-sitemap -- path/to/build

# Check the published index and child sitemap contents with the other assets.
npm run check-production
```

Both builds generate `dist/sitemap-index.xml` and numbered child files before
validation. They fail if sitemap coverage differs from the indexable HTML output.
New and removed pages therefore update the sitemap without a manual edit.
`build:site` does not sync upstream guides; run `npm run sync-docs` first when
those guides must be refreshed. CI uses the full build, including docs sync.

`public/robots.txt` advertises the index. Publish the whole `dist/` directory,
not the index alone. Component API sites have separate deployments and are
outside this site's sitemap. Do not add their URLs to this build's page list.

Do not set `lastmod` to the build time: a rebuild does not mean that every page
changed. Keep the default omission until reliable per-page dates are available.

## Acceptance checks

- The index lists at least one readable child sitemap.
- Every indexable built HTML page has exactly one URL across those children.
- Redirect stubs, error pages and `noindex` pages have no sitemap entry.
- Missing or removed page fixtures fail validation until the sitemap changes.
- Production checks read XML bodies, not only HTTP headers.

Merge and deployment need separate maintainer approval. Local validation does
not prove that a change has reached the live site.
