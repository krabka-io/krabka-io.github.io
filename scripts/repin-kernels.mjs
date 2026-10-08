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
// `source_url` to the new revision, file and line. A kernel that a
// `macro_rules!` of the crate generates is checked against the macro's body
// with the invocation's arguments substituted, and linked at the invocation.
// With any mismatch it changes nothing and names the kernels; `--check` only
// reports.

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

// The text between the bracket at `open` and its match, past the brackets of
// strings, char literals and line comments.
function bracketed(text, open) {
  const pairs = { '{': '}', '(': ')', '[': ']' };
  const stack = [];
  for (let i = open; i < text.length; i++) {
    const c = text[i];
    if (c === '"') {
      for (i++; i < text.length && text[i] !== '"'; i++) if (text[i] === '\\') i++;
    } else if (c === "'" && /^'(\\.|[^\\'])'/.test(text.slice(i, i + 4))) {
      i = text.indexOf("'", i + 2);
    } else if (c === '/' && text[i + 1] === '/') {
      i = text.indexOf('\n', i);
      if (i < 0) break;
    } else if (pairs[c]) stack.push(pairs[c]);
    else if (c === stack[stack.length - 1] && stack.pop() && !stack.length) return text.slice(open + 1, i);
  }
  throw new Error(`unbalanced bracket at ${open}`);
}

// Every invocation of a single-arm `macro_rules!` in `text`, expanded: the
// macro's body with each `$param` replaced by the invocation's comma-separated
// argument, after the leading attributes that a `$(#[$doc:meta])*` repetition
// takes. `line` is the line of the first argument (the generated item's name).
function expansions(text) {
  const out = [];
  for (const m of text.matchAll(/macro_rules!\s+(\w+)\s*\{/g)) {
    // A macro this cannot read generates nothing here; its kernels then report as missing.
    try {
    const rules = bracketed(text, m.index + m[0].length - 1);
    const pattern = bracketed(rules, rules.indexOf('('));
    const body = bracketed(rules, rules.indexOf('{', rules.indexOf('=>')));
    const repetition = /\$\((?:[^()]|\([^()]*\))*\)[*+?]/g;
    const params = [...pattern.replace(repetition, '').matchAll(/\$(\w+):\w+/g)].map((p) => p[1]);
    const template = body.replace(repetition, '');
    for (const call of text.matchAll(new RegExp(`(?<!macro_rules!\\s+)\\b${m[1]}!\\s*[{(]`, 'g'))) {
      const start = call.index + call[0].length;
      const inner = bracketed(text, start - 1);
      const attrs = /^(\s*#\[(?:"(?:[^"\\]|\\.)*"|[^\]"])*\])*\s*/.exec(inner)[0];
      const args = inner.slice(attrs.length).split(',').map((a) => a.trim());
      if (args.length !== params.length) continue;
      let expanded = template;
      for (const [i, p] of params.entries()) expanded = expanded.replace(new RegExp(`\\$${p}\\b`, 'g'), args[i]);
      const line = text.slice(0, start + attrs.length).split('\n').length;
      out.push({ text: expanded, line });
    }
    } catch {
      continue;
    }
  }
  return out;
}
const generated = [...sources].flatMap(([file, text]) => expansions(text).map((e) => ({ file, ...e })));
const crate = norm([...sources.values(), ...generated.map((g) => g.text)].join('\n'));

const original = fs.readFileSync(FILE, 'utf8');
const kernels = JSON.parse(original);
const problems = [];
for (const k of kernels) {
  // The catalog names several functions with " + " for a kernel made of two.
  const fn = k.function.split(' + ')[0];
  const pattern = new RegExp(`^\\s*(pub(\\([^)]*\\))? )?(const )?fn ${fn}\\b`, 'm');
  const hit = [...sources].find(([, text]) => pattern.test(text));
  const macro = hit ? null : generated.find((g) => pattern.test(g.text));
  if (!hit && !macro) {
    problems.push(`${k.id}: fn ${fn} is not in crates/verified`);
    continue;
  }
  const file = hit ? hit[0] : macro.file;
  const line = hit ? hit[1].split('\n').findIndex((l) => pattern.test(`${l}\n`)) : macro.line - 1;
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
