# krabka.io website: guidance for contributors and coding agents

This is the Astro site at https://krabka.io. Read this file before you change
pages, components, styles or copy. `CLAUDE.md` imports it.

## Prose

- Write all prose in ASD-STE100 Simplified Technical English, as the krabka
  [prose style guide](https://github.com/krabka-io/krabka-broker/blob/main/docs/style_guides/prose_style_guide.md)
  defines it. It governs docs pages, guides, captions, alt text, commit
  messages and pull requests.
- **Exception: the home page** (`src/pages/index.astro` and the `Home*` and
  `Hero` components) can use a more marketing voice. The rules below still
  apply to it.
- Use sentence case for headings, buttons, links and labels. Write "Get
  started", not "Get Started". Do not join names with "&".
- Do not use these patterns. They read as generated text:
  - two-fragment slogans ("Fast broker. Safe Rust.") and "X, not just Y"
  - lists of three for rhythm only
  - filler words: seamless, robust, leverage, powerful, blazing, effortless
  - "Hub", "Directory" and similar labels for ordinary pages
- A link that ends a section starts with a verb and says what the reader does
  next: "Run the complete examples", "Read how krabka is verified".

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

## Visual design

- Do not use gradient text, pill badges, decorative arrows (→), glows, or grids
  of equal task tiles.
- Use the theme variables (`--color-*`, `--code-*`) and Tailwind colour
  classes. Do not hardcode hex or rgba colours in pages or components. The
  light theme redefines the variables in `src/styles/theme-light.css`, so a
  hardcoded colour breaks it.
- Check every change in the light and the dark theme, and at phone width
  (375 px) and desktop width.
- The browser tools (Cluster Lab, the playgrounds, the proof explorer) stay
  dark in the light theme on purpose. Their colours move to shared theme
  tokens in a later refactor; do not theme them one by one.

## Code samples

- The get-started client snippets run against a real broker in CI
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
