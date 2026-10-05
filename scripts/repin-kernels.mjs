// Re-points src/data/verified-kernels.json at a krabka-broker revision.
//
//   node scripts/repin-kernels.mjs [rev] [--check]
//
// The revision defaults to origin/HEAD of the krabka-broker checkout beside
// this repository (beside the main checkout when run from a worktree), after
// a fetch. For each kernel it finds the function in that revision's
// crates/verified, checks that the quoted signature and every quoted
// `requires`/`ensures` clause still appear in the source (a helper
// predicate's clause, `name(args) = body`, by its body), and rewrites
// `source_url` to the new revision, file and line. With any mismatch it
// changes nothing and names the kernels; `--check` only reports.

import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';

const ROOT = process.cwd();
const FILE = path.join(ROOT, 'src', 'data', 'verified-kernels.json');
const argv = process.argv.slice(2);
const checkOnly = argv.includes('--check');
const revArg = argv.find((a) => !a.startsWith('--'));
const git = (cwd, ...a) => execFileSync('git', a, { cwd, encoding: 'utf8', maxBuffer: 256 << 20 });
const common = git(ROOT, 'rev-parse', '--path-format=absolute', '--git-common-dir').trim();
const broker = path.join(path.dirname(path.dirname(common)), 'krabka-broker');
if (!fs.existsSync(path.join(broker, '.git'))) throw new Error(`no krabka-broker checkout at ${broker}`);
git(broker, 'fetch', '--quiet', 'origin');
const sha = git(broker, 'rev-parse', revArg || 'origin/HEAD').trim();

// Formatting-blind text: one space for any whitespace, none inside brackets or before a closing one.
const norm = (s) => s.replace(/\s+/g, ' ').replace(/([([{]) /g, '$1').replace(/ ([)\]}])/g, '$1').replace(/,([)\]}])/g, '$1').trim();

const files = git(broker, 'ls-tree', '-r', '--name-only', sha, 'crates/verified/src').split('\n').filter((f) => f.endsWith('.rs') && !/\/tests?\//.test(f));
const sources = new Map(files.map((f) => [f, git(broker, 'show', `${sha}:${f}`)]));
const crate = norm([...sources.values()].join('\n'));

const original = fs.readFileSync(FILE, 'utf8');
const kernels = JSON.parse(original);
const problems = [];
for (const k of kernels) {
  // The catalog names several functions with " + " for a kernel made of two.
  const fn = k.function.split(' + ')[0];
  const pattern = new RegExp(`^\\s*(pub(\\([^)]*\\))? )?(const )?fn ${fn}\\b`, 'm');
  const hit = [...sources].find(([, text]) => pattern.test(text));
  if (!hit) {
    problems.push(`${k.id}: fn ${fn} is not in crates/verified`);
    continue;
  }
  const [file, text] = hit;
  const line = text.split('\n').findIndex((l) => pattern.test(`${l}\n`));
  // A kernel of several functions lists one signature per line and prefixes each clause with its function.
  const names = k.function.split(' + ');
  for (const sig of k.signature.split('\n')) {
    if (!crate.includes(norm(sig).replace(/ ?\{$/, ''))) problems.push(`${k.id}: signature not in source: ${sig}`);
  }
  for (const quoted of [...(k.requires || []), ...(k.ensures || [])]) {
    const owner = /^(\w+): ([\s\S]+)$/.exec(quoted);
    const clause = owner && names.includes(owner[1]) ? owner[2] : quoted;
    const helper = /^\w+\([^)]*\) = ([\s\S]+)$/.exec(clause);
    if (!crate.includes(norm(helper ? helper[1] : clause))) problems.push(`${k.id}: clause not in source: ${quoted.slice(0, 100)}`);
  }
  k.source_url = `https://github.com/krabka-io/krabka-broker/blob/${sha}/${file}#L${line + 1}`;
}
if (problems.length) {
  console.error(`${problems.length} mismatches at krabka-broker ${sha.slice(0, 7)}${checkOnly ? '' : '; verified-kernels.json not changed'}:\n  ${problems.join('\n  ')}`);
  process.exit(1);
}
// Only the links change, in place, so the file keeps its hand formatting.
let text = original;
let cursor = 0;
JSON.parse(original).forEach((k, i) => {
  const old = JSON.stringify(k.source_url);
  const at = text.indexOf(old, cursor);
  const next = JSON.stringify(kernels[i].source_url);
  text = text.slice(0, at) + next + text.slice(at + old.length);
  cursor = at + next.length;
});
if (!checkOnly) fs.writeFileSync(FILE, text);
console.log(`${kernels.length} kernels match krabka-broker ${sha.slice(0, 7)}${checkOnly ? '' : '; source links re-pointed'}`);
