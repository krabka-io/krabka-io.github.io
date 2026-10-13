// Sätteri hast plugin: put the site base in front of root-relative links.
//
// A Markdown page cannot call getPath(), so a link such as
// `[benchmarks](/benchmarks)` in a news post would leave the base path when
// the site is built below one. This plugin applies the same rule as getPath()
// in src/utils/paths.ts to every `href` that starts with a single `/`.
// Protocol-relative (`//host`), absolute, fragment-only and relative links
// stay as they are.
//
// Registered on the processor in astro.config.mjs with the same base as the
// site.

export function withBase(base, href) {
  if (typeof href !== 'string' || !href.startsWith('/') || href.startsWith('//')) return href;
  const prefix = base.replace(/\/$/, '');
  if (!prefix || href === prefix || href.startsWith(`${prefix}/`)) return href;
  return `${prefix}${href}`;
}

export default function baseLinks(base) {
  return {
    name: 'base-links',
    element: {
      filter: ['a'],
      visit(node, ctx) {
        const href = node.properties?.href;
        const next = withBase(base, href);
        if (next !== href) ctx.setProperty(node, 'href', next);
      },
    },
  };
}
