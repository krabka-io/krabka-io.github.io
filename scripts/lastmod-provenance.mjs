// Page dates come from content-file history, never build time or repository HEAD.
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

export function ignoredRevisions(root) {
  const file = path.join(root, 'scripts/lastmod-ignore-revs.txt');
  if (!fs.existsSync(file)) return [];
  return fs.readFileSync(file, 'utf8').split('\n').map((line) => line.split('#')[0].trim()).filter(Boolean).map((revision) => {
    if (!/^[a-f0-9]{7,40}$/i.test(revision)) throw new Error(`Invalid ignored lastmod revision: ${revision}`);
    return revision.toLowerCase();
  });
}

export function sourceFileProvenance(root, repo, sourcePath, revision, { ignoreRevs = ignoredRevisions(root) } = {}) {
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
      const history = git(root, ['log', '--follow', ...(ignoreRevs.length ? [] : ['-1']), '--format=%H%x09%cI', record.revision, '--', sourcePath]);
      const [commit, date] = (history.split('\n').find((line) => !ignoreRevs.some((ignored) => line.split('\t')[0].startsWith(ignored))) ?? '').split('\t');
      if (!commit || !validLastmod(date)) record.reason = 'missing-path-history';
      else record.lastChange = { commit, date };
      // Compare through Git's clean filters (including LFS), not raw working
      // bytes against stored blobs. A website build normally queries HEAD;
      // upstream callers can explicitly pin a different synced revision.
      if (record.revision !== state.revision && git(root, ['diff', record.revision, '--', sourcePath])) record.reason = 'source-does-not-match-pinned-revision';
    }
  } catch { record.reason = 'unavailable-path-history'; }
  return record;
}

// Stop traversal at global shell/chrome and rendering plumbing. In particular,
// BaseLayout's unused versions import must not date non-homepage routes.
export function isContentInput(file) {
  return !/^(?:src\/layouts\/|src\/styles\/|public\/(?:fonts\/|favicon|apple-touch-icon\.png|og\/)|src\/components\/(?:Navbar|Footer|Logo|CodeBlock)\.astro$|src\/utils\/(?:paths\.ts|slugify\.mjs|headings\.ts|heading-ids\.mjs|satteri-[^/]+\.mjs|krabka-shiki-theme\.mjs|tabs\.ts)$|(?:astro\.config\.mjs|package(?:-lock)?\.json)$)/.test(file);
}

function resolveImport(root, from, specifier) {
  const base = path.posix.normalize(path.posix.join(path.posix.dirname(from), specifier));
  return [base, ...['.ts', '.mjs', '.js', '.astro', '.json'].map((ext) => base + ext), ...['index.ts', 'index.mjs', 'index.js'].map((file) => `${base}/${file}`)]
    .find((file) => fs.existsSync(path.join(root, file)) && fs.statSync(path.join(root, file)).isFile());
}

export function importedInputs(root, source, seen = new Set()) {
  if (seen.has(source) || !isContentInput(source)) return seen;
  seen.add(source);
  if (!/\.(?:astro|[cm]?js|ts|md)$/.test(source)) return seen;
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

export function resolveLastmod({ root, url, mapping, state = repositoryState(root), ignoreRevs = ignoredRevisions(root), cache = new Map() }) {
  const result = { url, family: mapping?.family ?? 'unmapped', inputs: mapping?.inputs ?? [], evidence: [...(mapping?.provenance ?? [])], date: null, reason: null };
  const omit = (reason) => ({ ...result, reason });
  if (!mapping) return omit('unmapped-route');
  if (mapping.gap) return omit(mapping.gap);
  if (!state.historyComplete) return omit(state.reason ?? 'incomplete-website-history');
  for (const file of result.inputs) {
    if (!cache.has(file)) cache.set(file, sourceFileProvenance(root, 'website', file, state.revision, { ignoreRevs }));
    result.evidence.push(cache.get(file));
  }
  if (!result.evidence.length) return omit('missing-content-inputs');
  for (const record of result.evidence) {
    if (!record || record.reason || !record.historyComplete || record.dirtySource !== false || !validLastmod(record.lastChange?.date)) {
      return omit(record?.reason ?? 'incomplete-upstream-provenance');
    }
  }
  // Compare instants (not timezone-dependent lexical timestamps), then use UTC
  // calendar dates consistently for website and upstream inputs.
  const latest = Math.max(...result.evidence.map((record) => Date.parse(record.lastChange.date)));
  return { ...result, date: new Date(latest).toISOString().slice(0, 10) };
}
