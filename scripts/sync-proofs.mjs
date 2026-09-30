// Sync the broker's Creusot proof sessions for the proof explorer.
//
// krabka-broker keeps one Why3find session per proved function under
// `verif/krabka_verified_rlib/<module>/<name>/proof.json`, and beside each the
// Coma file Creusot generated for it. This script copies both in from a sibling
// checkout (`../krabka-broker`) or a sparse clone, then writes:
//
//   src/data/proof-sessions.json   one record per session: the proof tree,
//                                  prover statistics, the obligations the Coma
//                                  labels name, and the ledger row that cites it
//   public/proofs/coma/<module>/<name>.coma   the generated Coma, served for the
//                                  explorer's source view and the browser
//                                  re-check
//
// Both are build outputs (gitignored). The page degrades when they are absent.

import { execSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { parseVerificationCatalog } from '../src/utils/verification-catalog.ts';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const REPO = 'krabka-io/krabka-broker';
const LOCAL_REPO = path.resolve(ROOT, '..', 'krabka-broker');
const VERIF_SUBDIR = path.join('verif', 'krabka_verified_rlib');
const DATA_OUT = path.join(ROOT, 'src', 'data', 'proof-sessions.json');
const COMA_OUT = path.join(ROOT, 'public', 'proofs', 'coma');
const CATALOG = path.join(ROOT, 'src', 'content', 'docs', 'broker', 'verification.md');

console.log('🦀 [sync-proofs] Syncing Creusot proof sessions...');

// ---- locate the broker checkout -------------------------------------------------

function brokerCheckout() {
  if (fs.existsSync(path.join(LOCAL_REPO, VERIF_SUBDIR))) {
    console.log(`  ✓ Using local checkout at ${LOCAL_REPO}`);
    return LOCAL_REPO;
  }
  const tmp = path.join('/tmp', 'krabka-sync-proofs');
  try {
    console.log(`  → Sparse-cloning ${REPO} (verif/, .creusot-version, why3find.json)...`);
    fs.rmSync(tmp, { recursive: true, force: true });
    execSync(`git clone --depth 1 --filter=blob:none --sparse https://github.com/${REPO}.git ${tmp}`, { stdio: 'pipe' });
    execSync(`git -C ${tmp} sparse-checkout set --no-cone /verif /.creusot-version /why3find.json`, { stdio: 'pipe' });
    return tmp;
  } catch (err) {
    console.warn(`  ⚠️ Could not fetch ${REPO}: ${err.message}`);
    return null;
  }
}

function gitCommit(dir) {
  try {
    return execSync(`git -C ${dir} rev-parse HEAD`, { stdio: 'pipe' }).toString().trim();
  } catch {
    return null;
  }
}

// ---- proof.json -----------------------------------------------------------------

/** Walk a why3find certificate tree, returning leaf and tactic statistics. */
function treeStats(node, depth = 0, acc = { leaves: 0, time: 0, maxTime: 0, depth: 0, provers: {}, tactics: {}, stuck: 0 }) {
  if (!node || typeof node !== 'object') return acc;
  if (typeof node.prover === 'string') {
    acc.leaves += 1;
    const t = Number(node.time) || 0;
    acc.time += t;
    if (t > acc.maxTime) acc.maxTime = t;
    acc.provers[node.prover] = (acc.provers[node.prover] ?? 0) + 1;
    if (depth > acc.depth) acc.depth = depth;
    return acc;
  }
  if (typeof node.tactic === 'string') {
    acc.tactics[node.tactic] = (acc.tactics[node.tactic] ?? 0) + 1;
    for (const child of node.children ?? []) treeStats(child, depth + 1, acc);
    return acc;
  }
  // A node with neither is an unproved goal in why3find's format.
  acc.stuck += 1;
  return acc;
}

function round(x) {
  return Math.round(x * 1000) / 1000;
}

// ---- Coma ------------------------------------------------------------------------

/** `let%span sfoo'1 = "/path/file.rs" 16 4 16 19` bindings. */
function parseSpans(coma) {
  const spans = new Map();
  const re = /^let%span\s+(\S+)\s*=\s*"([^"]+)"\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)/gm;
  for (const m of coma.matchAll(re)) {
    spans.set(m[1], { file: relativeSource(m[2]), line: Number(m[3]), col: Number(m[4]), end_line: Number(m[5]), end_col: Number(m[6]) });
  }
  return spans;
}

/** Creusot ran in `/workspace` and the Rust registry; keep paths readable. */
function relativeSource(file) {
  if (file.startsWith('/workspace/')) return file.slice('/workspace/'.length);
  const registry = file.indexOf('/registry/src/');
  if (registry !== -1) {
    const rest = file.slice(registry + '/registry/src/'.length);
    return rest.slice(rest.indexOf('/') + 1);
  }
  return file;
}

/** The function's own span: `(* #"/workspace/.../consensus.rs" 15 0 15 55 *)` on line 1. */
function parseHeaderSpan(coma) {
  const m = /^\(\*\s*#"([^"]+)"\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s*\*\)/.exec(coma);
  return m ? { file: relativeSource(m[1]), line: Number(m[2]), col: Number(m[3]), end_line: Number(m[4]), end_col: Number(m[5]) } : null;
}

/**
 * Every `[@expl:...]` label with its span and the formula that follows it, up
 * to the closing brace of the enclosing `{ ... }` assertion.
 */
function parseObligations(coma, spans) {
  const obligations = [];
  const re = /\[@expl:([^\]]*)\]/g;
  const keyword = /(?<![\w'])(if|else)(?![\w'])/y;
  for (const m of coma.matchAll(re)) {
    const expl = m[1].trim();
    let i = m.index + m[0].length;
    let span = null;
    // Skip further attributes and a span marker.
    for (;;) {
      const rest = coma.slice(i, i + 200);
      const attr = /^\s*\[@[^\]]*\]/.exec(rest);
      if (attr) {
        i += attr[0].length;
        continue;
      }
      const sp = /^\s*\[%#(\S+?)\]/.exec(rest);
      if (sp) {
        span = spans.get(sp[1]) ?? null;
        i += sp[0].length;
        continue;
      }
      break;
    }
    // Formula: until the brace that closes the enclosing `{`, or the `else` of
    // an `if` that opened before the formula (it was the `then` branch).
    let depth = 0;
    let openIfs = 0;
    let j = i;
    while (j < coma.length) {
      const ch = coma[j];
      if (ch === '{' || ch === '[' || ch === '(') depth += 1;
      else if (ch === '}' || ch === ']' || ch === ')') {
        if (depth === 0) break;
        depth -= 1;
      } else if (depth === 0) {
        keyword.lastIndex = j;
        const word = keyword.exec(coma)?.[1];
        if (word === 'if') openIfs += 1;
        else if (word === 'else') {
          if (openIfs === 0) break;
          openIfs -= 1;
        }
      }
      j += 1;
    }
    const formula = coma.slice(i, j).replace(/\s+/g, ' ').trim();
    obligations.push({ expl, span, formula });
  }
  return obligations;
}

function kindOf(name, kernelNames) {
  if (name.startsWith('lemma_')) return 'lemma';
  if (kernelNames.has(name)) return 'kernel';
  return 'helper';
}

// ---- main ----------------------------------------------------------------------

const checkout = brokerCheckout();
if (!checkout) {
  // The proof explorer page imports the sessions file, so a build without it
  // fails later with a less helpful message, and a reused workspace would
  // otherwise publish whatever the previous sync wrote.
  fs.rmSync(COMA_OUT, { recursive: true, force: true });
  fs.rmSync(DATA_OUT, { force: true });
  console.error(`  ✗ No proof sessions: ${REPO} is not checked out beside this repository and could not be cloned. The proof explorer cannot be built without them.`);
  process.exit(1);
}

const verifDir = path.join(checkout, VERIF_SUBDIR);
const creusotVersion = fs.existsSync(path.join(checkout, '.creusot-version')) ? fs.readFileSync(path.join(checkout, '.creusot-version'), 'utf8').trim() : null;
const why3find = fs.existsSync(path.join(checkout, 'why3find.json')) ? JSON.parse(fs.readFileSync(path.join(checkout, 'why3find.json'), 'utf8')) : null;

// Ledger rows citing each proof session, from the synced catalog when present.
const ledgerByProof = new Map();
const kernelNames = new Set();
if (fs.existsSync(CATALOG)) {
  const catalog = parseVerificationCatalog(fs.readFileSync(CATALOG, 'utf8'));
  for (const row of catalog.ledger) {
    for (const k of row.kernels) kernelNames.add(k.label);
    for (const proof of row.proofs) {
      const m = /verif\/krabka_verified_rlib\/([^/]+)\/([^/]+)\/proof\.json/.exec(proof.url);
      if (m) ledgerByProof.set(`${m[1]}/${m[2]}`, { row: row.id, label: proof.label, kernels: row.kernels.map((k) => k.label) });
    }
  }
}

fs.rmSync(COMA_OUT, { recursive: true, force: true });
fs.mkdirSync(COMA_OUT, { recursive: true });

// A session is a directory holding `proof.json`, with the Coma file beside the
// directory: `<module>/<name>/proof.json` and `<module>/<name>.coma`. A derived
// impl nests one level deeper, `<module>/impl_Clone_for_X/clone/proof.json`.
function sessionDirs(dir, rel = '') {
  const found = [];
  for (const entry of fs.readdirSync(dir).sort()) {
    const full = path.join(dir, entry);
    if (!fs.statSync(full).isDirectory()) continue;
    const relPath = rel ? `${rel}/${entry}` : entry;
    if (fs.existsSync(path.join(full, 'proof.json'))) found.push(relPath);
    else found.push(...sessionDirs(full, relPath));
  }
  return found;
}

const sessions = [];
for (const id of sessionDirs(verifDir)) {
  {
    const parts = id.split('/');
    const module = parts[0];
    const name = parts[parts.length - 1];
    const impl = parts.length > 2 ? parts.slice(1, -1).join('/') : null;
    const proofFile = path.join(verifDir, id, 'proof.json');
    const proof = JSON.parse(fs.readFileSync(proofFile, 'utf8'));
    const comaFile = path.join(verifDir, `${id}.coma`);
    const coma = fs.existsSync(comaFile) ? fs.readFileSync(comaFile, 'utf8') : null;

    const goals = [];
    const stats = { leaves: 0, time: 0, maxTime: 0, depth: 0, provers: {}, tactics: {}, stuck: 0 };
    for (const [theory, byGoal] of Object.entries(proof.proofs ?? {})) {
      for (const [goal, tree] of Object.entries(byGoal)) {
        goals.push({ theory, name: goal, tree });
        treeStats(tree, 0, stats);
      }
    }
    stats.time = round(stats.time);
    stats.maxTime = round(stats.maxTime);

    let comaPath = null;
    let obligations = [];
    let source = null;
    if (coma !== null) {
      const outFile = path.join(COMA_OUT, `${id}.coma`);
      fs.mkdirSync(path.dirname(outFile), { recursive: true });
      fs.writeFileSync(outFile, coma);
      comaPath = `proofs/coma/${id}.coma`;
      const spans = parseSpans(coma);
      obligations = parseObligations(coma, spans);
      source = parseHeaderSpan(coma);
    }

    sessions.push({
      id,
      module,
      name,
      impl,
      kind: impl ? 'derived' : kindOf(name, kernelNames),
      goals,
      stats,
      obligations,
      coma: comaPath ? { path: comaPath, bytes: Buffer.byteLength(coma) } : null,
      source,
      ledger: ledgerByProof.get(id) ?? null,
    });
  }
}

const totals = { sessions: sessions.length, leaves: 0, time: 0, provers: {}, tactics: {}, kinds: {}, obligations: 0, modules: new Set(sessions.map((s) => s.module)).size };
for (const s of sessions) {
  totals.leaves += s.stats.leaves;
  totals.time += s.stats.time;
  totals.obligations += s.obligations.length;
  totals.kinds[s.kind] = (totals.kinds[s.kind] ?? 0) + 1;
  for (const [p, n] of Object.entries(s.stats.provers)) totals.provers[p] = (totals.provers[p] ?? 0) + n;
  for (const [t, n] of Object.entries(s.stats.tactics)) totals.tactics[t] = (totals.tactics[t] ?? 0) + n;
}
totals.time = round(totals.time);

const data = {
  source: {
    repo: REPO,
    commit: gitCommit(checkout),
    creusot: creusotVersion,
    why3find: why3find ? { provers: why3find.provers, tactics: why3find.tactics, time: why3find.time, fast: why3find.fast, depth: why3find.depth } : null,
    synced_at: new Date().toISOString(),
  },
  totals,
  sessions,
};

fs.mkdirSync(path.dirname(DATA_OUT), { recursive: true });
fs.writeFileSync(DATA_OUT, JSON.stringify(data));
console.log(`  ✓ ${sessions.length} sessions, ${totals.leaves} prover goals, ${totals.obligations} labelled obligations → ${path.relative(ROOT, DATA_OUT)} (${(fs.statSync(DATA_OUT).size / 1024).toFixed(0)} KB)`);
console.log(`  ✓ Coma sources → ${path.relative(ROOT, COMA_OUT)}`);
