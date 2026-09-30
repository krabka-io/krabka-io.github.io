// Sätteri hast plugin: wrap every markdown table in a scroll container.
//
// The synced guides carry wide tables, such as the broker's Creusot proof
// ledger (four columns of paragraphs) and the KIP matrix (many short columns).
// A bare <table> can only be made to fit by letting the page scroll sideways
// or by squeezing every column. The wrapper scrolls on its own instead, and its
// `data-cols` attribute lets the stylesheet give a wide table a minimum width
// so its columns stay readable on a narrow screen.
//
// The plugin also gives the code in a table cell somewhere to break. Chrome
// offers no line break at a slash or an underscore, so a long file path in a
// narrow column can only split mid-word, or holds the column open at the width
// of the whole path. A <wbr> after each `/`, `_` and `::` fixes both, and adds
// nothing to copied text.
//
// Registered on the processor in astro.config.mjs. It is a plain plugin
// object, so the site needs no import from the `satteri` package itself.

function columnCount(table) {
  let widest = 0;
  const walk = (node) => {
    for (const child of node.children ?? []) {
      if (child.type !== 'element') continue;
      if (child.tagName === 'tr') {
        const cells = (child.children ?? []).filter((c) => c.type === 'element' && (c.tagName === 'th' || c.tagName === 'td')).length;
        if (cells > widest) widest = cells;
      } else {
        walk(child);
      }
    }
  };
  walk(table);
  return widest;
}

const wrapTables = {
  name: 'wrap-tables',
  element: {
    filter: ['table'],
    visit(node, ctx) {
      const parent = ctx.parent(node);
      // A table that already sits in a wrapper (raw HTML in a guide) stays as is.
      if (parent && parent.type === 'element' && Array.isArray(parent.properties?.className) && parent.properties.className.includes('table-wrap')) {
        return;
      }
      ctx.wrapNode(node, {
        type: 'element',
        tagName: 'div',
        properties: { className: ['table-wrap'], 'data-cols': String(columnCount(node)) },
        children: [],
      });
    },
  },
  text(node, ctx) {
    const code = ctx.parent(node);
    if (code?.tagName !== 'code') return;
    // The cell holds the code directly, or through a link.
    let cell = ctx.parent(code);
    if (cell?.tagName === 'a') cell = ctx.parent(cell);
    if (cell?.tagName !== 'td') return;
    const parts = node.value.split(/(?<=[/_]|::)(?=\S)/);
    if (parts.length < 2) return;
    ctx.replaceNode(
      node,
      parts.flatMap((part, i) => [
        ...(i ? [{ type: 'element', tagName: 'wbr', properties: {}, children: [] }] : []),
        { type: 'text', value: part },
      ]),
    );
  },
};

export default wrapTables;
