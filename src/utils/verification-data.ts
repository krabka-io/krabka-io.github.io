// Joins the parsed verification catalog with the site's own data files into
// the records the verification pages render.
//
// Pure: no Astro or filesystem access, so `scripts/check-catalog.mjs` can run
// the same join under Node against the synced catalog and report drift.
// `verification-loader.ts` does the IO for the pages.

import {
  compareAreas,
  emptyCatalog,
  parseVerificationCatalog,
  repoPathOf,
  type CatalogLink,
  type LedgerRow,
  type ModelNote,
  type VerificationCatalog,
} from './verification-catalog.ts';

/** One entry of `src/data/verified-kernels.json`, as far as this module reads it. */
export interface ExplorerSpecInput {
  id: string;
  title: string;
  area: string;
  module: string;
  /** One function name, or several joined with ` + ` for a combined kernel. */
  function: string;
  [key: string]: unknown;
}

/** Ledger facts attached to an explorer spec for the "Proof ledger" drilldown. */
export interface ExplorerLedger {
  row_id: string;
  contract_html: string;
  host_html: string;
  proofs: CatalogLink[];
  preconditions_html: string;
  /** Anchor of the row on the site's verification page. */
  page_url: string;
}

export type ExplorerSpec = ExplorerSpecInput & { ledger?: ExplorerLedger };

/** One entry of `src/data/stateright-models.json`. */
export interface ModelSpecInput {
  id: string;
  name: string;
  area: string;
  repo: string;
  path: string;
  source_url: string;
  drives?: string[];
  bounds?: string;
  properties?: string[];
  pinned_states?: { config: string; count: number }[];
  outside?: string;
  summary: string;
}

export interface KernelRow extends LedgerRow {
  /** Set when one of the row's kernels can be evaluated in the browser explorer. */
  playground?: { id: string; title: string };
}

export interface ModelRow extends ModelSpecInput {
  drives: string[];
  properties: string[];
  pinned_states: { config: string; count: number }[];
  pinnedTotal: number;
  /** The catalog's own paragraphs about the model, when it has any. */
  note?: ModelNote;
  /** Lowercased plain text for client-side filtering. */
  search: string;
}

export interface VerificationStats {
  catalogAvailable: boolean;
  /** Ledger rows; one row can hold several related kernels. */
  rows: number;
  /** Distinct kernel functions named across the ledger. */
  functions: number;
  /** Distinct `krabka-verified` modules. */
  modules: number;
  /** Distinct proof session files linked from the ledger. */
  proofSessions: number;
  models: number;
  /** Models the catalog describes in prose. */
  modelsWithNotes: number;
  pinnedTotal: number;
  explorerKernels: number;
}

export interface VerificationData {
  catalog: VerificationCatalog;
  kernels: KernelRow[];
  kernelAreas: string[];
  models: ModelRow[];
  modelAreas: string[];
  explorerSpecs: ExplorerSpec[];
  stats: VerificationStats;
}

/** Where a ledger row lives on the site. */
export const VERIFICATION_PAGE = '/verification';
export const PLAYGROUND_PAGE = '/docs/verification-playground';

export function kernelRowAnchor(rowId: string): string {
  return `kernel-${rowId}`;
}

export function modelRowAnchor(modelId: string): string {
  return `model-${modelId}`;
}

function functionNames(spec: ExplorerSpecInput): string[] {
  return spec.function
    .split(/[^A-Za-z0-9_]+/)
    .map((s) => s.trim())
    .filter((s) => s !== '');
}

/** The ledger row that proves an explorer kernel: same module, and it names the function. */
export function ledgerRowFor(spec: ExplorerSpecInput, ledger: LedgerRow[]): LedgerRow | undefined {
  const names = functionNames(spec);
  return ledger.find((row) => row.kernels.some((k) => k.module === spec.module && names.includes(k.label)));
}

function catalogNoteFor(model: ModelSpecInput, notes: ModelNote[]): ModelNote | undefined {
  return notes.find((n) => n.path === model.path || repoPathOf(model.source_url) === n.path);
}

export function buildVerificationData(
  markdown: string | null,
  kernelSpecs: ExplorerSpecInput[],
  modelSpecs: ModelSpecInput[],
): VerificationData {
  const catalog = markdown === null ? emptyCatalog() : parseVerificationCatalog(markdown);

  const explorerSpecs: ExplorerSpec[] = kernelSpecs.map((spec) => {
    const row = ledgerRowFor(spec, catalog.ledger);
    if (!row) return { ...spec };
    return {
      ...spec,
      ledger: {
        row_id: row.id,
        contract_html: row.contract_html,
        host_html: row.host_html,
        proofs: row.proofs,
        preconditions_html: row.preconditions_html,
        page_url: `${VERIFICATION_PAGE}#${kernelRowAnchor(row.id)}`,
      },
    };
  });

  const kernels: KernelRow[] = catalog.ledger.map((row) => {
    const spec = kernelSpecs.find((s) => ledgerRowFor(s, [row]) !== undefined);
    return spec ? { ...row, playground: { id: spec.id, title: spec.title } } : { ...row };
  });
  kernels.sort((a, b) => compareAreas(a.area, b.area));
  const kernelAreas = [...new Set(kernels.map((k) => k.area))];

  const models: ModelRow[] = modelSpecs.map((model) => {
    const note = catalogNoteFor(model, catalog.models);
    const drives = model.drives ?? [];
    const properties = model.properties ?? [];
    const pinned = model.pinned_states ?? [];
    const search = [model.name, model.id, model.area, model.repo, model.path, model.summary, model.bounds ?? '', model.outside ?? '', ...drives, ...properties, note?.search ?? '']
      .join(' ')
      .toLowerCase();
    return {
      ...model,
      drives,
      properties,
      pinned_states: pinned,
      pinnedTotal: pinned.reduce((sum, p) => sum + p.count, 0),
      note,
      search,
    };
  });
  const modelAreas = [...new Set(models.map((m) => m.area))];

  const functions = new Set<string>();
  const modules = new Set<string>();
  const proofs = new Set<string>();
  for (const row of catalog.ledger) {
    for (const k of row.kernels) {
      functions.add(`${k.module}::${k.label}`);
      modules.add(k.module);
    }
    for (const p of row.proofs) proofs.add(p.url);
  }

  const stats: VerificationStats = {
    catalogAvailable: catalog.ledger.length > 0,
    rows: catalog.ledger.length,
    functions: functions.size,
    modules: modules.size,
    proofSessions: proofs.size,
    models: models.length,
    modelsWithNotes: models.filter((m) => m.note !== undefined).length,
    pinnedTotal: models.reduce((sum, m) => sum + m.pinnedTotal, 0),
    explorerKernels: kernelSpecs.length,
  };

  return { catalog, kernels, kernelAreas, models, modelAreas, explorerSpecs, stats };
}
