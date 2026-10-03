// Checks that the site still understands the broker's verification catalog.
//
// `src/pages/verification.astro` and the verification playground render the
// synced `docs/verification.md` from krabka-broker as expandable rows. This
// script runs the same parser under Node and fails when the catalog's layout
// has drifted far enough that rows or model notes would silently disappear.
//
// Run after `npm run sync-docs`:  node scripts/check-catalog.mjs

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { MODULE_AREAS, OTHER_AREA, parseVerificationCatalog } from '../src/utils/verification-catalog.ts';
import { buildVerificationData } from '../src/utils/verification-data.ts';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const catalogFile = path.join(root, 'src', 'content', 'docs', 'broker', 'verification.md');

if (!fs.existsSync(catalogFile)) {
  console.error(`✗ ${path.relative(root, catalogFile)} is missing. Run \`npm run sync-docs\` first.`);
  process.exit(1);
}

const markdown = fs.readFileSync(catalogFile, 'utf8');
const kernels = JSON.parse(fs.readFileSync(path.join(root, 'src', 'data', 'verified-kernels.json'), 'utf8'));
const models = JSON.parse(fs.readFileSync(path.join(root, 'src', 'data', 'stateright-models.json'), 'utf8'));

const data = buildVerificationData(markdown, kernels, models);
const { catalog, stats } = data;

const failures = [];
const warnings = [];

// Parser self-test on both ledger layouts: a kernel link may point at a flat
// module file (`opa.rs`) or into a module split across files
// (`authz/acl_decision.rs`), and the table may sit directly under the section
// or under a "### Production kernels" subsection.
{
  const src = 'https://github.com/o/r/blob/main/crates/verified/src';
  const proof = 'https://github.com/o/r/blob/main/verif/krabka_verified_rlib/x/proof.json';
  const row = (file, name) => `| [\`${name}\`](${src}/${file}) proves it. | A host. | [proof](${proof}) | None. |`;
  const table = [
    '| Kernel and contract | Host caller | Proof session | Caller preconditions |',
    '| :--- | :--- | :--- | :--- |',
    row('opa.rs', 'flat'),
    row('authz/acl_decision.rs', 'nested'),
  ];
  const layouts = {
    'flat layout': ['## Creusot Proof Ledger', '', ...table],
    'split layout': ['## Creusot Proof Ledger', '', 'Intro.', '', '### Cross-module theorems', '', 'Prose.', '', '### Production kernels', '', 'Lead-in.', '', ...table, '', 'Trailer.'],
  };
  for (const [name, lines] of Object.entries(layouts)) {
    const modules = parseVerificationCatalog(lines.join('\n')).ledger.map((r) => r.module).join(',');
    if (modules !== 'opa,authz') failures.push(`parser self-test (${name}): expected modules opa,authz, got ${modules || 'none'}`);
  }
}

// Count the ledger table's rows straight from the markdown: a row whose kernel
// link the parser no longer recognises is dropped silently, and a floor alone
// would not notice a few going missing.
{
  const lines = markdown.replace(/\r\n/g, '\n').split('\n');
  const head = lines.findIndex((l) => /^\|\s*Kernel and contract\s*\|/.test(l));
  let tableRows = 0;
  for (let i = head + 2; head !== -1 && i < lines.length && lines[i].startsWith('|'); i += 1) tableRows += 1;
  if (head === -1) failures.push('no "Kernel and contract" ledger table found in the catalog');
  else if (stats.rows !== tableRows) failures.push(`ledger table has ${tableRows} rows but ${stats.rows} parsed; a row's kernel links no longer match crates/verified/src/`);
  else console.log(`Ledger table: ${tableRows} rows, all parsed.`);
}

// Every unique-state count the site pins for a broker model must be one the
// catalog cites for it; a model whose count moved upstream shows up here.
for (const model of data.models) {
  if (model.repo !== 'krabka-broker' || !model.note) continue;
  const prose = model.note.paragraphs_html.join(' ').replace(/<[^>]*>/g, '');
  const cited = new Set([...prose.matchAll(/\d{1,3}(?:,\d{3})+|\d+/g)].map((m) => Number(m[0].replace(/,/g, ''))));
  for (const pin of model.pinned_states) {
    if (!cited.has(pin.count)) failures.push(`model ${model.id} pins ${pin.count.toLocaleString('en-US')} states for ${pin.config}, which its catalog paragraph does not cite`);
  }
}

if (stats.rows < 50) failures.push(`only ${stats.rows} ledger rows parsed (expected the Creusot Proof Ledger table)`);
if (catalog.inventory.length === 0) failures.push('no Stateright inventory table parsed');
if (catalog.models.length === 0) failures.push('no Stateright model paragraphs matched an inventory entry');

for (const row of catalog.ledger) {
  if (row.proofs.length === 0) warnings.push(`ledger row ${row.id} links no proof session`);
  if (row.host_html === '') warnings.push(`ledger row ${row.id} has an empty host caller cell`);
  if (row.area === OTHER_AREA) warnings.push(`module ${row.module} has no entry in MODULE_AREAS (row ${row.id})`);
}

for (const paragraph of catalog.unmatchedParagraphs) {
  warnings.push(`model paragraph matched no inventory entry: ${paragraph.slice(0, 90)}…`);
}

const inventoryPaths = new Set(catalog.inventory.flatMap((a) => a.entries.map((e) => e.path)));
for (const entry of inventoryPaths) {
  if (!catalog.models.some((m) => m.path === entry)) warnings.push(`inventory entry ${entry} has no catalog paragraph`);
}

for (const model of data.models) {
  if (model.repo === 'krabka-broker' && !model.note) warnings.push(`model ${model.id} (${model.path}) has no catalog note`);
  if (model.repo === 'krabka-broker' && !inventoryPaths.has(model.path)) warnings.push(`model ${model.id} is not in the catalog inventory`);
}
for (const entry of inventoryPaths) {
  if (!data.models.some((m) => m.path === entry)) warnings.push(`inventory entry ${entry} is missing from src/data/stateright-models.json`);
}

for (const spec of data.explorerSpecs) {
  if (!spec.ledger) warnings.push(`explorer kernel ${spec.id} (${spec.module}::${spec.function}) matched no ledger row`);
}

const unusedAreas = Object.keys(MODULE_AREAS).filter((m) => !catalog.ledger.some((r) => r.modules.includes(m)));
if (unusedAreas.length > 0) warnings.push(`MODULE_AREAS names modules absent from the ledger: ${unusedAreas.join(', ')}`);

console.log(`Catalog: ${stats.rows} ledger rows, ${stats.functions} kernel functions in ${stats.modules} modules, ${stats.proofSessions} proof sessions.`);
console.log(`Models: ${stats.models} in the site data, ${stats.modelsWithNotes} with catalog notes, ${stats.pinnedTotal.toLocaleString('en-US')} pinned unique states.`);
console.log(`Sections: ${catalog.sections.map((s) => `${s.slug} (${s.html.length} blocks)`).join(', ')}`);
console.log(`Explorer kernels with ledger rows: ${data.explorerSpecs.filter((s) => s.ledger).length}/${data.explorerSpecs.length}.`);

for (const w of warnings) console.warn(`  ⚠ ${w}`);
for (const f of failures) console.error(`  ✗ ${f}`);

if (failures.length > 0) {
  console.error(`\n✗ ${failures.length} failure(s); the catalog layout no longer matches the parser.`);
  process.exit(1);
}
console.log(`\n✓ Catalog parses; ${warnings.length} warning(s).`);
