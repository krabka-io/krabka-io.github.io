// Build-time parser for the broker's verification catalog.
//
// `scripts/sync-docs.mjs` copies `docs/verification.md` from krabka-broker into
// `src/content/docs/broker/verification.md` on every build. That file is the
// authoritative inventory of every Creusot-proved kernel and every Stateright
// model: the Creusot Proof Ledger table (kernel and contract, host caller,
// proof session, caller preconditions) and, under the Stateright tier, one
// paragraph per model entry point. The site renders that inventory as dense,
// expandable rows rather than as one very wide table, so this module turns the
// markdown into structured records with the inline markdown rendered to HTML.
//
// The parser is deliberately small and format-specific. It never throws on a
// shape it does not recognise: an unparseable row or paragraph is dropped and
// `scripts/check-catalog.mjs` reports what was dropped, so a change to the
// catalog's layout surfaces as a failed check rather than a broken page.
//
// Plain TypeScript with no Astro imports, so the same code runs under Node
// (`node --experimental-strip-types`) for the check script.

export interface CatalogLink {
  /** Link text with surrounding backticks removed. */
  label: string;
  url: string;
}

export interface KernelRef extends CatalogLink {
  /** Module file stem under `crates/verified/src/`, e.g. `authz`. */
  module: string;
}

export interface LedgerRow {
  /** Stable anchor id, `<module>-<first kernel>` (deduplicated with a suffix). */
  id: string;
  /** Module of the first kernel named in the row. */
  module: string;
  /** Every `krabka-verified` module the row's kernels live in. */
  modules: string[];
  /** Every kernel function the row names, in catalog order. */
  kernels: KernelRef[];
  /** Site grouping derived from the module; see `MODULE_AREAS`. */
  area: string;
  /** Plain-text rendering of the contract cell, for collapsed rows. */
  claim: string;
  /** Contract cell rendered to inline HTML. */
  contract_html: string;
  /** Host caller cell rendered to inline HTML. */
  host_html: string;
  /** Proof session links from the third cell. */
  proofs: CatalogLink[];
  /** Caller preconditions rendered to inline HTML; empty when the cell is empty. */
  preconditions_html: string;
  /** Lowercased plain text of the whole row, for client-side filtering. */
  search: string;
}

export interface InventoryEntry extends CatalogLink {
  /** Repository-relative path taken from the GitHub URL, e.g. `crates/raft/tests/kraft_model.rs`. */
  path: string;
}

export interface InventoryArea {
  area: string;
  entries: InventoryEntry[];
}

export interface ModelNote {
  path: string;
  url: string;
  label: string;
  area: string;
  /** The catalog's paragraphs about this model, each rendered to inline HTML. */
  paragraphs_html: string[];
  /** Lowercased plain text of the paragraphs. */
  search: string;
}

export interface CatalogSection {
  /** Heading slug as Astro's markdown renderer would produce it, e.g. `outside-both-tiers`. */
  slug: string;
  title: string;
  /** Block-level HTML: paragraphs, lists and fenced code, tables excluded. */
  html: string[];
}

export interface VerificationCatalog {
  ledger: LedgerRow[];
  /** The section's prose around the ledger table, rendered. */
  ledgerNotes: string[];
  /**
   * The Stateright section's prose about bounds and pinned state counts,
   * rendered, without the sentences that introduce the inventory table and
   * the per-model paragraphs (the site renders both as rows).
   */
  modelNotes: string[];
  inventory: InventoryArea[];
  models: ModelNote[];
  /** Model paragraphs that matched no inventory entry; the check script reports these. */
  unmatchedParagraphs: string[];
  sections: CatalogSection[];
}

/** URL of the catalog page on this site, used to resolve fragment-only links. */
export const CATALOG_PAGE = '/docs/broker/verification';

/**
 * Site grouping for the `krabka-verified` modules. A module absent from this
 * table lands in `OTHER_AREA`, and the check script lists it so the table can
 * be extended when the broker adds a module.
 */
export const MODULE_AREAS: Record<string, string> = {
  consensus: 'KRaft consensus',
  vote: 'KRaft consensus',
  voter_set: 'KRaft consensus',
  reconfiguration: 'KRaft consensus',
  quorum_state: 'KRaft consensus',
  raft: 'KRaft consensus',
  epoch: 'KRaft consensus',
  snapshot: 'KRaft consensus',

  isr: 'Replication and failover',
  leader_epoch: 'Replication and failover',
  reassignment: 'Replication and failover',
  stretch: 'Replication and failover',
  broker: 'Replication and failover',
  registration: 'Replication and failover',
  directory: 'Replication and failover',
  wal: 'Replication and failover',
  offset_allocator: 'Replication and failover',

  compaction: 'Storage and log',
  retention: 'Storage and log',
  log_index: 'Storage and log',
  producer_snapshot: 'Storage and log',
  checkpoint: 'Storage and log',
  storage: 'Storage and log',
  timestamp: 'Storage and log',
  list_offsets: 'Storage and log',
  stamp: 'Storage and log',
  delivery: 'Storage and log',

  recovery: 'Recovery and restore',
  restore: 'Recovery and restore',
  restore_sidecar: 'Recovery and restore',
  local_recovery: 'Recovery and restore',

  diskless: 'Diskless and remote storage',
  remote_read: 'Diskless and remote storage',
  remote_metadata: 'Diskless and remote storage',
  remote_txn: 'Diskless and remote storage',

  producer: 'Producers and transactions',
  producer_id: 'Producers and transactions',
  produce: 'Producers and transactions',
  transaction: 'Producers and transactions',

  uniform_assignor: 'Groups and coordination',
  group_migration: 'Groups and coordination',
  share: 'Groups and coordination',
  barrier: 'Groups and coordination',

  authz: 'Security and quotas',
  opa: 'Security and quotas',
  quota: 'Security and quotas',
  throttle: 'Security and quotas',
  scram: 'Security and quotas',
  delegation_token: 'Security and quotas',
  oauth: 'Security and quotas',
  jwks: 'Security and quotas',

  audit: 'Integrity and operations',
  chain: 'Integrity and operations',
  worm: 'Integrity and operations',
  break_glass: 'Integrity and operations',
  break_glass_persistence: 'Integrity and operations',
  features: 'Integrity and operations',
  freeze: 'Integrity and operations',
  schema: 'Integrity and operations',
};

export const OTHER_AREA = 'Other kernels';

/** Display order of the areas; anything unlisted sorts after these. */
export const AREA_ORDER: string[] = [
  'KRaft consensus',
  'Replication and failover',
  'Storage and log',
  'Recovery and restore',
  'Diskless and remote storage',
  'Producers and transactions',
  'Groups and coordination',
  'Security and quotas',
  'Integrity and operations',
  OTHER_AREA,
];

export function areaForModule(module: string): string {
  return MODULE_AREAS[module] ?? OTHER_AREA;
}

export function compareAreas(a: string, b: string): number {
  const ia = AREA_ORDER.indexOf(a);
  const ib = AREA_ORDER.indexOf(b);
  const ra = ia === -1 ? AREA_ORDER.length : ia;
  const rb = ib === -1 ? AREA_ORDER.length : ib;
  return ra === rb ? a.localeCompare(b) : ra - rb;
}

// ---- inline markdown ----------------------------------------------------------

const ESCAPES: Record<string, string> = {
  '&': '&amp;',
  '<': '&lt;',
  '>': '&gt;',
  '"': '&quot;',
  "'": '&#39;',
};

export function escapeHtml(text: string): string {
  return text.replace(/[&<>"']/g, (ch) => ESCAPES[ch] ?? ch);
}

export interface InlineOptions {
  /** Page that fragment-only links (`#slug`) refer to. Defaults to `CATALOG_PAGE`. */
  fragmentBase?: string;
}

const LINK_RE = /\[([^\[\]]+)\]\(([^()\s]+)\)/g;
const CODE_RE = /`([^`\n]+)`/g;
const BOLD_RE = /\*\*([^*\n]+)\*\*/g;
const PLACEHOLDER = '\u0000';

function safeHref(url: string, fragmentBase: string): string | null {
  if (url.startsWith('#')) return `${fragmentBase}${url}`;
  if (url.startsWith('/')) return url;
  if (/^https?:\/\//i.test(url)) return url;
  return null;
}

/**
 * Render the subset of inline markdown the catalog uses (code spans, links,
 * bold) to HTML. Everything else is escaped text. Code spans are lifted out
 * first so brackets and asterisks inside them are never read as syntax.
 */
export function renderInline(markdown: string, options: InlineOptions = {}): string {
  const fragmentBase = options.fragmentBase ?? CATALOG_PAGE;
  const codes: string[] = [];
  const lifted = markdown.replace(CODE_RE, (_m, code: string) => {
    codes.push(`<code>${escapeHtml(code)}</code>`);
    return `${PLACEHOLDER}${codes.length - 1}${PLACEHOLDER}`;
  });

  let html = escapeHtml(lifted);
  html = html.replace(LINK_RE, (whole, label: string, rawUrl: string) => {
    // The URL was escaped along with the text; undo that for validation only.
    const url = rawUrl.replace(/&amp;/g, '&');
    const href = safeHref(url, fragmentBase);
    if (href === null) return whole;
    const external = /^https?:\/\//i.test(href);
    const attrs = external ? ' target="_blank" rel="noopener noreferrer"' : '';
    return `<a href="${escapeHtml(href)}"${attrs}>${label}</a>`;
  });
  html = html.replace(BOLD_RE, '<strong>$1</strong>');

  return html.replace(new RegExp(`${PLACEHOLDER}(\\d+)${PLACEHOLDER}`, 'g'), (_m, i: string) => codes[Number(i)] ?? '');
}

/** Plain text of inline markdown: link labels kept, syntax removed. */
export function stripInline(markdown: string): string {
  return markdown
    .replace(LINK_RE, '$1')
    .replace(CODE_RE, '$1')
    .replace(BOLD_RE, '$1')
    .replace(/\s+/g, ' ')
    .trim();
}

export function extractLinks(markdown: string): CatalogLink[] {
  const links: CatalogLink[] = [];
  for (const match of markdown.matchAll(LINK_RE)) {
    links.push({ label: stripInline(match[1]), url: match[2] });
  }
  return links;
}

/** Repository-relative path of a `github.com/<owner>/<repo>/blob/<ref>/<path>` URL. */
export function repoPathOf(url: string): string | null {
  const match = /^https?:\/\/github\.com\/[^/]+\/[^/]+\/blob\/[^/]+\/(.+?)(?:#.*)?$/.exec(url);
  return match ? match[1] : null;
}

export function slugify(text: string): string {
  return text
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

// ---- block markdown ------------------------------------------------------------

/** Render paragraphs, bullet lists and fenced code; skip table rows. */
export function renderBlocks(lines: string[], options: InlineOptions = {}): string[] {
  const blocks: string[] = [];
  let i = 0;
  while (i < lines.length) {
    const line = lines[i];
    if (line.trim() === '' || line.startsWith('|')) {
      i += 1;
      continue;
    }
    if (line.startsWith('```')) {
      const lang = line.slice(3).trim();
      const code: string[] = [];
      i += 1;
      while (i < lines.length && !lines[i].startsWith('```')) {
        code.push(lines[i]);
        i += 1;
      }
      i += 1; // closing fence
      const cls = lang ? ` class="language-${escapeHtml(lang)}"` : '';
      blocks.push(`<pre><code${cls}>${escapeHtml(code.join('\n'))}</code></pre>`);
      continue;
    }
    if (/^\s*[-*] /.test(line)) {
      const items: string[] = [];
      while (i < lines.length && lines[i].trim() !== '') {
        if (/^\s*[-*] /.test(lines[i])) {
          items.push(lines[i].replace(/^\s*[-*] /, ''));
        } else if (items.length > 0) {
          items[items.length - 1] += ` ${lines[i].trim()}`;
        }
        i += 1;
      }
      blocks.push(`<ul>${items.map((item) => `<li>${renderInline(item, options)}</li>`).join('')}</ul>`);
      continue;
    }
    const para: string[] = [];
    while (i < lines.length && lines[i].trim() !== '' && !lines[i].startsWith('|') && !lines[i].startsWith('```') && !/^\s*[-*] /.test(lines[i]) && !lines[i].startsWith('#')) {
      para.push(lines[i].trim());
      i += 1;
    }
    if (para.length > 0) {
      blocks.push(`<p>${renderInline(para.join(' '), options)}</p>`);
    } else {
      i += 1;
    }
  }
  return blocks;
}

// ---- tables ------------------------------------------------------------------------

/** Split one table row into cells on ` | `, ignoring pipes inside code spans. */
export function splitRow(line: string): string[] {
  let text = line.trim();
  if (text.startsWith('|')) text = text.slice(1);
  if (text.endsWith('|')) text = text.slice(0, -1);
  const cells: string[] = [];
  let current = '';
  let inCode = false;
  for (let i = 0; i < text.length; i += 1) {
    const ch = text[i];
    if (ch === '`') inCode = !inCode;
    if (ch === '|' && !inCode) {
      cells.push(current.trim());
      current = '';
    } else {
      current += ch;
    }
  }
  cells.push(current.trim());
  return cells;
}

function isSeparatorRow(line: string): boolean {
  return /^\|?\s*:?-{3,}/.test(line.trim());
}

interface Table {
  header: string[];
  rows: string[][];
  /** Index of the line after the table. */
  end: number;
}

/** Parse the first table found at or after `start`, if the header matches. */
function readTable(lines: string[], start: number, end: number, firstHeader: string): Table | null {
  for (let i = start; i < end; i += 1) {
    if (!lines[i].startsWith('|')) continue;
    const header = splitRow(lines[i]);
    if (header[0] !== firstHeader) continue;
    if (i + 1 >= end || !isSeparatorRow(lines[i + 1])) continue;
    const rows: string[][] = [];
    let j = i + 2;
    while (j < end && lines[j].startsWith('|')) {
      rows.push(splitRow(lines[j]));
      j += 1;
    }
    return { header, rows, end: j };
  }
  return null;
}

// ---- sections ---------------------------------------------------------------------

interface RawSection {
  title: string;
  slug: string;
  start: number; // first line after the heading
  end: number; // line index of the next `## ` heading or EOF
}

function findSections(lines: string[]): RawSection[] {
  const sections: RawSection[] = [];
  for (let i = 0; i < lines.length; i += 1) {
    const match = /^## (.+?)\s*$/.exec(lines[i]);
    if (!match) continue;
    if (sections.length > 0) sections[sections.length - 1].end = i;
    sections.push({ title: match[1], slug: slugify(match[1]), start: i + 1, end: lines.length });
  }
  return sections;
}

function sectionProse(lines: string[], section: RawSection): string[] {
  // Prose of the section itself: stop at its first `###` subsection.
  let end = section.end;
  for (let i = section.start; i < section.end; i += 1) {
    if (lines[i].startsWith('### ')) {
      end = i;
      break;
    }
  }
  return renderBlocks(lines.slice(section.start, end));
}

// ---- the ledger ---------------------------------------------------------------------

const VERIFIED_SRC_RE = /\/crates\/verified\/src\/([a-z0-9_]+)\.rs(?:#.*)?$/;

function kernelRefs(cell: string): KernelRef[] {
  const refs: KernelRef[] = [];
  for (const link of extractLinks(cell)) {
    const match = VERIFIED_SRC_RE.exec(link.url);
    if (!match) continue;
    refs.push({ label: link.label, url: link.url, module: match[1] });
  }
  return refs;
}

function parseLedger(lines: string[], section: RawSection | undefined): { rows: LedgerRow[]; notes: string[] } {
  if (!section) return { rows: [], notes: [] };
  const table = readTable(lines, section.start, section.end, 'Kernel and contract');
  const notes = sectionProse(lines, section);
  if (!table) return { rows: [], notes };

  const seen = new Map<string, number>();
  const rows: LedgerRow[] = [];
  for (const cells of table.rows) {
    const [contract = '', host = '', proof = '', preconditions = ''] = cells;
    const kernels = kernelRefs(contract);
    if (kernels.length === 0) continue;
    const module = kernels[0].module;
    const base = slugify(`${module}-${kernels[0].label}`);
    const n = (seen.get(base) ?? 0) + 1;
    seen.set(base, n);
    const id = n === 1 ? base : `${base}-${n}`;
    const modules = [...new Set(kernels.map((k) => k.module))];
    const claim = stripInline(contract);
    // The catalog writes "None." for a contract that accepts every value of
    // its Rust input types; the site renders that case with its own wording.
    const noPreconditions = /^none\.?$/i.test(stripInline(preconditions));
    rows.push({
      id,
      module,
      modules,
      kernels,
      area: areaForModule(module),
      claim,
      contract_html: renderInline(contract),
      host_html: renderInline(host),
      proofs: extractLinks(proof),
      preconditions_html: noPreconditions ? '' : renderInline(preconditions),
      search: [claim, stripInline(host), stripInline(preconditions), ...kernels.map((k) => k.label), ...modules, areaForModule(module)]
        .join(' ')
        .toLowerCase(),
    });
  }
  return { rows, notes };
}

// ---- the Stateright tier ----------------------------------------------------------------

function normalizeLabel(text: string): string {
  return text
    .toLowerCase()
    .replace(/\.rs$/, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

interface SubSection {
  title: string;
  paragraphs: string[];
}

function subsections(lines: string[], section: RawSection): SubSection[] {
  const subs: SubSection[] = [];
  let current: SubSection | null = null;
  let para: string[] = [];
  const flush = () => {
    if (current && para.length > 0) current.paragraphs.push(para.join(' '));
    para = [];
  };
  for (let i = section.start; i < section.end; i += 1) {
    const line = lines[i];
    const heading = /^### (.+?)\s*$/.exec(line);
    if (heading) {
      flush();
      current = { title: heading[1], paragraphs: [] };
      subs.push(current);
      continue;
    }
    if (!current) continue;
    if (line.trim() === '') {
      flush();
      continue;
    }
    if (line.startsWith('|') || line.startsWith('```')) continue;
    para.push(line.trim());
  }
  flush();
  return subs;
}

function parseStateright(lines: string[], section: RawSection | undefined): { inventory: InventoryArea[]; models: ModelNote[]; unmatched: string[] } {
  if (!section) return { inventory: [], models: [], unmatched: [] };
  const table = readTable(lines, section.start, section.end, 'Area');
  const inventory: InventoryArea[] = [];
  if (table) {
    for (const cells of table.rows) {
      const [area = '', entryCell = ''] = cells;
      const entries: InventoryEntry[] = [];
      for (const link of extractLinks(entryCell)) {
        const path = repoPathOf(link.url);
        if (!path) continue;
        entries.push({ ...link, path });
      }
      if (area && entries.length > 0) inventory.push({ area, entries });
    }
  }

  const notes = new Map<string, ModelNote>();
  const unmatched: string[] = [];

  for (const sub of subsections(lines, section)) {
    const area = inventory.find((a) => a.area.toLowerCase() === sub.title.toLowerCase());
    const entries = area ? area.entries : inventory.flatMap((a) => a.entries);
    const areaName = area ? area.area : sub.title;
    const pending = new Set(entries.map((e) => e.path));
    const unresolved: string[] = [];
    let last: ModelNote | null = null;

    const attach = (entry: InventoryEntry, paragraph: string) => {
      let note = notes.get(entry.path);
      if (!note) {
        note = { path: entry.path, url: entry.url, label: entry.label, area: areaName, paragraphs_html: [], search: '' };
        notes.set(entry.path, note);
      }
      note.paragraphs_html.push(renderInline(paragraph));
      note.search = `${note.search} ${stripInline(paragraph)}`.trim().toLowerCase();
      pending.delete(entry.path);
      last = note;
    };

    for (const paragraph of sub.paragraphs) {
      // 1. A link to the model file itself.
      const linked = extractLinks(paragraph).map((l) => repoPathOf(l.url)).filter((p): p is string => p !== null);
      let entry = entries.find((e) => linked.includes(e.path));
      // 2. The inventory label appears in the paragraph's opening clause.
      if (!entry) {
        const opening = normalizeLabel(paragraph.slice(0, Math.min(paragraph.length, 160)));
        entry = entries.find((e) => pending.has(e.path) && opening.includes(normalizeLabel(e.label)));
      }
      if (entry) {
        attach(entry, paragraph);
      } else {
        unresolved.push(paragraph);
      }
    }

    // 3. One unmatched paragraph and one unmatched entry in the section pair up.
    if (unresolved.length === 1 && pending.size === 1) {
      const [path] = [...pending];
      const entry = entries.find((e) => e.path === path);
      if (entry) {
        attach(entry, unresolved[0]);
        unresolved.length = 0;
      }
    }
    // Anything left continues the previous note when there is one.
    for (const paragraph of unresolved) {
      if (last) {
        const note: ModelNote = last;
        note.paragraphs_html.push(renderInline(paragraph));
        note.search = `${note.search} ${stripInline(paragraph)}`.trim().toLowerCase();
      } else {
        unmatched.push(paragraph);
      }
    }
  }

  return { inventory, models: [...notes.values()], unmatched };
}

// ---- entry point -------------------------------------------------------------------

export function parseVerificationCatalog(markdown: string): VerificationCatalog {
  const lines = markdown.replace(/\r\n/g, '\n').split('\n');
  const sections = findSections(lines);
  const ledgerSection = sections.find((s) => s.slug === 'creusot-proof-ledger');
  const staterightSection = sections.find((s) => s.slug === 'stateright-model-check-tier');

  const { rows, notes } = parseLedger(lines, ledgerSection);
  const { inventory, models, unmatched } = parseStateright(lines, staterightSection);
  const modelNotes = staterightSection ? sectionProse(lines, staterightSection).filter((block) => !isLeadIn(block)) : [];

  return {
    ledger: rows,
    ledgerNotes: notes,
    modelNotes,
    inventory,
    models,
    unmatchedParagraphs: unmatched,
    sections: sections.map((s) => ({ slug: s.slug, title: s.title, html: sectionProse(lines, s) })),
  };
}

/** A block that only introduces the table or the paragraphs that follow it. */
function isLeadIn(html: string): boolean {
  const text = html.replace(/<[^>]*>/g, '').trim();
  return text.endsWith(':') || /^The paragraphs below take the entry points/.test(text);
}

export function emptyCatalog(): VerificationCatalog {
  return { ledger: [], ledgerNotes: [], modelNotes: [], inventory: [], models: [], unmatchedParagraphs: [], sections: [] };
}
