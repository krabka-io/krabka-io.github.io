// Git supplies evidence candidates, not a significant-update policy.
import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { validLastmod } from './sitemap.mjs';

export const contentHash = (content) => createHash('sha256').update(content).digest('hex');
const git = (root, args) => execFileSync('git', ['-C', root, ...args], {
  encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'], timeout: 15_000, maxBuffer: 16 * 1024 * 1024,
}).trim();

export function repositoryState(root) {
  try {
    if (fs.realpathSync(git(root, ['rev-parse', '--show-toplevel'])) !== fs.realpathSync(root)) return { reason: 'non-git-source', historyComplete: false };
    return { historyComplete: git(root, ['rev-parse', '--is-shallow-repository']) === 'false', revision: git(root, ['rev-parse', 'HEAD']) };
  } catch { return { reason: 'non-git-source', historyComplete: false }; }
}

export function sourceFileProvenance(root, repo, sourcePath, revision) {
  const state = repositoryState(root);
  const record = { repo, sourcePath, revision: revision ?? state.revision ?? null, historyComplete: state.historyComplete,
    dirtySource: null, lastChange: null, sourceHash: null, reason: state.reason ?? null };
  try { record.sourceHash = contentHash(fs.readFileSync(path.join(root, sourcePath))); } catch { record.reason = 'missing-input'; return record; }
  if (state.reason) return record;
  try {
    record.dirtySource = !!git(root, ['status', '--porcelain', '--untracked-files=all', '--', sourcePath]);
    if (record.dirtySource) record.reason = 'dirty-or-untracked-source';
    else if (!state.historyComplete) record.reason = 'incomplete-history';
    else {
      // --follow follows renames of this one path; revision pins the lookup.
      const [commit, date] = git(root, ['log', '--follow', '-1', '--format=%H%x09%cI', record.revision, '--', sourcePath]).split('\t');
      if (!commit || !validLastmod(date)) record.reason = 'missing-path-history';
      else record.lastChange = { commit, date, significance: 'unreviewed' };
      // A clean working tree at a different revision is not the pinned input.
      const pinned = execFileSync('git', ['-C', root, 'show', `${record.revision}:${sourcePath}`], { timeout: 15_000, maxBuffer: 16 * 1024 * 1024, stdio: ['ignore', 'pipe', 'pipe'] });
      if (contentHash(pinned) !== record.sourceHash) record.reason = 'source-does-not-match-pinned-revision';
    }
  } catch { record.reason = 'unavailable-path-history'; }
  return record;
}

// Every input is explicit. Import discovery below checks this list; it does not
// claim to infer fs reads, runtime fetches, images or the meaning of a change.
export function validateAttestations(records) {
  for (const [route, record] of Object.entries(records)) {
    if (!validLastmod(record.date)) throw new Error(`${route}: invalid explicit lastmod: ${record.date}`);
    if (record.complete !== true || !record.evidence || !record.owner || !/^[a-f0-9]{40}$/.test(record.revision ?? '') || !Object.keys(record.inputs ?? {}).length) throw new Error(`${route}: incomplete lastmod attestation`);
    for (const [file, hash] of Object.entries(record.inputs)) {
      if (path.isAbsolute(file) || file.split('/').includes('..') || !/^[a-f0-9]{64}$/.test(hash)) throw new Error(`${route}: invalid attested input ${file}`);
    }
  }
}

function resolveImport(root, from, specifier) {
  const base = path.posix.normalize(path.posix.join(path.posix.dirname(from), specifier));
  return [base, ...['.ts', '.mjs', '.js', '.astro', '.json'].map((ext) => base + ext), ...['index.ts', 'index.mjs', 'index.js'].map((file) => `${base}/${file}`)]
    .find((file) => fs.existsSync(path.join(root, file)) && fs.statSync(path.join(root, file)).isFile());
}

export function importedInputs(root, source, seen = new Set()) {
  if (seen.has(source)) return seen;
  seen.add(source);
  const text = fs.readFileSync(path.join(root, source), 'utf8');
  // Deliberately conservative: literal imports in inline scripts count too.
  const imports = [...text.matchAll(/(?:\b(?:import|export)\s+(?:[^'";]*?\s+from\s*)?|\bimport\s*\()(['"])(\.[^'"]+)\1/g)];
  for (const [, , specifier] of imports) {
    const file = resolveImport(root, source, specifier);
    if (!file) throw new Error(`unresolved-import:${source}:${specifier}`);
    importedInputs(root, file, seen);
  }
  return seen;
}

export function resolveLastmod({ root, url, mapping, attestation, state = repositoryState(root) }) {
  const result = { url, family: mapping?.family ?? 'unmapped', inputs: attestation ? Object.keys(attestation.inputs).sort() : mapping?.inputs ?? [], dependencyStatus: attestation?.complete ? 'declared-complete' : 'incomplete', evidence: mapping?.provenance ?? [], date: null, reason: null };
  const omit = (reason) => ({ ...result, reason });
  if (!mapping) return omit('unmapped-route');
  if (mapping.gap) return omit(mapping.gap);
  if (!attestation) return omit('no-reviewed-significant-update');
  if (state.attestationReason) return omit(state.attestationReason);
  if (!state.historyComplete) return omit(state.reason ?? 'incomplete-website-history');
  if (mapping.provenance?.some((record) => !record || record.reason || !record.historyComplete || record.dirtySource !== false)) return omit('incomplete-upstream-provenance');
  const inputs = Object.keys(attestation.inputs).sort();
  if (mapping.inputs.some((file) => !inputs.includes(file))) return omit('unattested-route-input');
  try {
    const discovered = importedInputs(root, mapping.source);
    if ([...discovered].some((file) => !inputs.includes(file))) return omit('unattested-import-dependency');
    // This also checks deletions, staged changes and untracked input files.
    if (git(root, ['status', '--porcelain', '--untracked-files=all', '--', ...inputs])) return omit('dirty-or-untracked-input');
    execFileSync('git', ['-C', root, 'merge-base', '--is-ancestor', attestation.revision, state.revision], { stdio: 'ignore', timeout: 15_000 });
    // Hash equality alone cannot detect a later edit followed by a revert.
    if (git(root, ['log', '--format=%H', `${attestation.revision}..${state.revision}`, '--', ...inputs])) return omit('input-history-changed-since-review');
    for (const file of inputs) {
      if (contentHash(fs.readFileSync(path.join(root, file))) !== attestation.inputs[file]) return omit(`changed-unreviewed-input:${file}`);
      // A tracked baseline is required, even for input paths ignored by Git.
      git(root, ['ls-files', '--error-unmatch', '--', file]);
    }
    result.evidence.push({ owner: attestation.owner, revision: attestation.revision, significantUpdate: attestation.date, review: attestation.evidence, inputs: attestation.inputs });
  } catch { return omit('unavailable-input-evidence'); }
  return { ...result, date: attestation.date };
}
