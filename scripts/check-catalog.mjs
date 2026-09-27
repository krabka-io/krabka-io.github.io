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

import { MODULE_AREAS, OTHER_AREA } from '../src/utils/verification-catalog.ts';
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
