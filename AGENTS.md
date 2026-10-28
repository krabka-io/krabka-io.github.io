# krabka.io website: guidance for contributors and coding agents

This is the Astro site at https://krabka.io. Read this file before you change
pages, components, styles or copy. `CLAUDE.md` imports it.

## Website style and authority

Read [the website style guide](docs/style_guides/website_style_guide.md) before changing copy, content structure, or presentation. Its explicit website overrides take precedence over the linked broker prose guide. Choose plain, welcoming English for introductions and benefits and the technical register for technical passages, wherever they occur; the exception is not limited to the homepage. Contributor prose, including commits and pull requests, uses the linked technical prose guide.

The website guide is canonical for editorial and presentation rules, including functional badges and tiles, occasional arrows, reuse approvals, and house conventions. Browser-tool UI design remains outside that guide's scope.

## Claims

- Every claim must be true at the level of the evidence. Do not generalize
  from one workload, one configuration or one measured round.
- Compute numbers from the source data at build time. Do not type them into
  copy. Add a guard that changes the wording when the data stops supporting
  the claim (see `src/utils/home-benchmark.mjs`).
- Do not compare memory against brokers that preallocate it (a JVM heap,
  Redpanda's reservation). Compare CPU or state krabka's own figure.
- Keep the scope words: "selected kernels", "within each model's bounds",
  "supported Kafka APIs". Formal-methods readers check them.

## Presentation safeguards

Use theme variables and Tailwind color classes, not hardcoded hex or rgba colors. Check presentation changes in both themes at 375 px and desktop widths. The browser tools intentionally remain dark in the light theme; do not theme them separately before the shared-token refactor. Follow the style guide's approval requirements for new components/assets, occasional arrows, and monospace exceptions in tables.

## Code samples

- The local quickstart client snippets run against a real broker in CI
  (`scripts/check-snippets.mjs`). Do not change their record values.
- The home page shows excerpts cut from those snippets at build time
  (`src/utils/client-excerpts.mjs`). Do not copy code into the home page by
  hand.
- Every `<CodeBlock>` on a page in `scripts/snippets/extract.mjs` needs CI
  coverage there.

## Docs from other repositories

- `src/content/docs/` is synced from the component repositories by
  `scripts/sync-docs.mjs` and is not committed. Change those guides in their
  own repository.
- Link internal pages with `getPath()`.

## Before you open a pull request

```sh
npm run build:site     # pages only; npm run build also compiles the WebAssembly tools
npm run test:website
npm run check-links
npm run check-seo
```

Keep pull requests small enough to review in one sitting. Describe what you
could not test.
