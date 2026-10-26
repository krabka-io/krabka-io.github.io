// Native payload checks: every HTML file plus separate app source and WASM ceilings.
import fs from 'node:fs';
import path from 'node:path';
import { gzipSync } from 'node:zlib';
import { fileURLToPath } from 'node:url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const walk = (dir) => fs.existsSync(dir) ? fs.readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
  const file = path.join(dir, entry.name);
  return entry.isDirectory() ? walk(file) : entry.isFile() ? [file] : [];
}) : [];
export function payloadSize(file) {
  const bytes = fs.readFileSync(file);
  return { rawBytes: bytes.length, gzipBytes: gzipSync(bytes, { level: 9 }).length };
}

export function auditBudgets(dist, config, { wasm = true } = {}) {
  const htmlFiles = walk(dist).filter((file) => file.endsWith('.html'));
  if (!htmlFiles.length) throw new Error(`No built HTML pages in ${dist}. Build the site first.`);
  const rows = [];
  const skippedGroups = [];
  const failures = [];
  for (const file of config.html.required) if (!fs.existsSync(path.join(dist, file))) failures.push(`Missing required page: ${file}`);
  const record = (name, files, limit) => {
    const sizes = files.map(payloadSize);
    const row = { name, files: files.length, rawBytes: sizes.reduce((n, size) => n + size.rawBytes, 0), gzipBytes: sizes.reduce((n, size) => n + size.gzipBytes, 0), limits: { rawBytes: limit.rawBytes, gzipBytes: limit.gzipBytes } };
    rows.push(row);
    if (!files.length) failures.push(`Missing required payload: ${name}`);
    for (let i = 0; i < files.length; i++) if (!sizes[i].rawBytes) failures.push(`Empty required payload: ${path.relative(dist, files[i])}`);
    for (const kind of ['rawBytes', 'gzipBytes']) if (row[kind] > limit[kind]) failures.push(`${name}: ${row[kind]} ${kind} exceeds ${limit[kind]}`);
  };
  for (const file of htmlFiles) {
    const name = path.relative(dist, file).replaceAll('\\', '/');
    record(name, [file], config.html.overrides[name] ?? config.html);
  }
  for (const group of config.groups) {
    // Only the two generated binaries can be absent in a lightweight PR build.
    if (!wasm && group.files?.length === 1 && [
      'playground/broker/krabka-broker.wasm', 'playground/krabka_playground_bg.wasm',
    ].includes(group.files[0])) {
      skippedGroups.push(group.name);
      continue;
    }
    const files = group.files ? group.files.map((file) => path.join(dist, file)) : walk(path.join(dist, group.directory)).filter((file) => file.endsWith(group.extension));
    for (const file of files) if (!fs.existsSync(file)) failures.push(`Missing required asset: ${path.relative(dist, file)}`);
    record(group.name, files.filter((file) => fs.existsSync(file)), group);
  }
  return { measuredAt: new Date().toISOString(), scope: config.note, rows, skippedGroups, failures };
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const args = process.argv.slice(2);
    const wasmArg = args.find(arg => arg.startsWith('--wasm='));
    if (wasmArg && !['--wasm=true', '--wasm=false'].includes(wasmArg)) throw new Error('Invalid --wasm value');
    const positional = args.filter(arg => arg !== wasmArg);
    if (positional.length > 1 || positional.some(arg => arg.startsWith('--'))) throw new Error('Usage: check-page-budgets.mjs [dist] [--wasm=true|false]');
    const dist = path.resolve(positional[0] ?? path.join(ROOT, 'dist'));
    const config = JSON.parse(fs.readFileSync(path.join(ROOT, 'scripts/page-budgets.json'), 'utf8'));
    const report = auditBudgets(dist, config, { wasm: wasmArg !== '--wasm=false' });
    const output = path.resolve(process.env.SITE_CHECK_ARTIFACTS ?? path.join(ROOT, 'artifacts/site-check'));
    fs.mkdirSync(output, { recursive: true });
    fs.writeFileSync(path.join(output, 'build.json'), JSON.stringify(report, null, 2) + '\n');
    console.log(`Measured ${report.rows.length} HTML/app payloads; report: ${path.join(output, 'build.json')}`);
    if (report.skippedGroups.length) console.log(`Not built in this run: ${report.skippedGroups.join(', ')}`);
    for (const failure of report.failures) console.error(failure);
    process.exitCode = report.failures.length ? 1 : 0;
  } catch (error) { console.error(error.message); process.exitCode = 1; }
}
