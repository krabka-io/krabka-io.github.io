// Sätteri hast plugin: wrap every markdown table in a scroll container.
//
// The synced guides carry wide tables, such as the broker's Creusot proof
// ledger (four columns of paragraphs) and the KIP matrix (many short columns).
// A bare <table> can only be made to fit by letting the page scroll sideways
// or by squeezing every column. The wrapper scrolls on its own instead, and its
// `data-cols` attribute lets the stylesheet give a wide table a minimum width
// so its columns stay readable on a narrow screen.
//
// A table of three or more columns also carries `data-stack`, and each of its
// cells a `data-label` holding its column's header. On a phone the stylesheet
// lays such a table out as one card per row, each value under its label, because
// a row of long prose cannot be read through a 375px window that scrolls.
//
// The plugin also gives a long name in a table cell, in code or plain text,
// somewhere to break. Chrome offers no line break at a slash, a dot, an
// underscore or a camelCase seam, so a long file path or a qualified name in a
// narrow column can only split mid-word, or holds the column open at the width
// of the whole name. A <wbr> at each of those points fixes both, and adds
// nothing to copied text. A KIP id in a cell gets a span the stylesheet keeps on
// one line, so "KIP-1206 / KIP-1222" breaks at the slash and never inside an id.
//
// Short inline code gets a `code-atom` class. A chip is an inline-block, which
// Chrome may break a line on both sides of, stranding the "(" before it or the
// "." after it; a short one set as plain inline text keeps its punctuation.
//
// Registered on the processor in astro.config.mjs. It is a plain plugin
// object, so the site needs no import from the `satteri` package itself.

// Fewer columns than this and the table is narrow enough to read without stacking.
const STACK_FROM = 3;

// A break point after `/`, `.`, `_` or `::`, and at a lower-to-upper camelCase seam.
const BREAK_AT = /(?<=[/_.]|::)(?=\S)|(?<=[a-z])(?=[A-Z])/;
// In a cell's plain text: a KIP id, or a token long enough to need a break point.
const KIP_OR_LONG = /(KIP-\d+|\S{16,})/;
const KIP_ID = /^KIP-\d+$/;
// Code this short is set on one line (see `.code-atom` in custom.css).
const ATOM_MAX = 28;

const isCell = (el) => el?.tagName === 'td' || el?.tagName === 'th';
const text = (value) => ({ type: 'text', value });
const wbr = () => ({ type: 'element', tagName: 'wbr', properties: {}, children: [] });
const breakable = (value) => value.split(BREAK_AT).flatMap((part, i) => (i ? [wbr(), text(part)] : [text(part)]));

function cellsOf(row) {
  return (row.children ?? []).filter((c) => c.type === 'element' && isCell(c));
}

function rowsOf(table) {
  const rows = [];
  const walk = (node) => {
    for (const child of node.children ?? []) {
      if (child.type !== 'element') continue;
      if (child.tagName === 'tr') rows.push(child);
      else walk(child);
    }
  };
  walk(table);
  return rows;
}

// Copy the header text of each column onto the body cells beneath it.
function labelCells(rows, ctx) {
  const labels = cellsOf(rows[0]).map((th) => ctx.textContent(th).trim());
  for (const row of rows.slice(1)) {
    cellsOf(row).forEach((td, i) => {
      if (labels[i]) ctx.setProperty(td, 'data-label', labels[i]);
    });
  }
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
      const rows = rowsOf(node);
      const cols = Math.max(0, ...rows.map((row) => cellsOf(row).length));
      const properties = { className: ['table-wrap'], 'data-cols': String(cols) };
      if (cols >= STACK_FROM && rows.length > 1) {
        properties['data-stack'] = '';
        labelCells(rows, ctx);
      }
      ctx.wrapNode(node, { type: 'element', tagName: 'div', properties, children: [] });
    },
  },
  text(node, ctx) {
    const parent = ctx.parent(node);
    // The text sits in a cell directly, or through one link; code sits in a
    // cell, a paragraph or a list item the same way.
    let holder = ctx.parent(parent);
    if (holder?.tagName === 'a') holder = ctx.parent(holder);
    if (parent.tagName === 'code') {
      if (!isCell(holder) && holder?.tagName !== 'p' && holder?.tagName !== 'li') return;
      if (node.value.length <= ATOM_MAX) ctx.setProperty(parent, 'className', ['code-atom']);
      if (holder.tagName === 'td') ctx.replaceNode(node, breakable(node.value));
      return;
    }
    if (!isCell(holder) && !isCell(parent)) return;
    const parts = node.value.split(KIP_OR_LONG);
    if (parts.length < 2) return;
    // split() keeps each match at an odd index.
    ctx.replaceNode(
      node,
      parts
        .flatMap((part, i) => {
          if (!(i % 2)) return part ? [text(part)] : [];
          if (!KIP_ID.test(part)) return breakable(part);
          return [{ type: 'element', tagName: 'span', properties: { className: ['kip-id'] }, children: [text(part)] }];
        }),
    );
  },
};

export default wrapTables;
