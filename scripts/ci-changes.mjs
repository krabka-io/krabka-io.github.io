// Select expensive PR checks. Unknown inputs and diff errors select all suites.
import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { pages } from './snippets/extract.mjs';

const all = () => ({ site: true, lab: true, proofs: true, snippets: true });
const snippetPages = new Set(Object.keys(pages).map(page => `src/pages/${page}.astro`));

export function classifyPaths(paths) {
  const selected = { site: true, lab: false, proofs: false, snippets: false };
  for (const file of paths) {
    let known = false;
    if (snippetPages.has(file) || /^(public\/(quickstart|charts)\/|scripts\/(snippets\/|check-snippets\.mjs))/.test(file)) {
      selected.snippets = known = true;
    }
    if (/^(playground\/|public\/playground\/|kafkactl-lab\/|scripts\/check-(lab|real-broker|wasi))/.test(file)
        || /^src\/pages\/docs\/(lab|playground)\.astro$/.test(file)) {
      selected.lab = known = true;
      // Rust snippets use the playground's dependency pins and local patches.
      if (file.startsWith('playground/')) selected.snippets = true;
    }
    if (/^(why3-web\/|public\/(proofs|why3-web)\/|scripts\/(check-proof|sync-proofs|repin-kernels|fixtures\/.*\.coma))/.test(file)
        || /^src\/pages\/(verification|docs\/(proof-explorer|verification-playground))\.astro$/.test(file)) {
      selected.proofs = known = true;
    }
    // Shared templates, styles, data, dependencies, configuration and scripts
    // fall through to all suites. Only these ordinary content inputs are cheap.
    if (/^(src\/(pages|content)\/|public\/(brand|images|fonts)\/|benchmarks\/|docs\/)/.test(file)
        || /^(README\.md|AGENTS\.md|CLAUDE\.md|CONTRIBUTING\.md|SECURITY\.md|LICENSE|LICENSES\/.*|trademark\.md)$/.test(file)) known = true;
    if (!known) return all();
  }
  return selected;
}

export function changedPaths(base, head, cwd = process.cwd()) {
  for (const sha of [base, head]) {
    if (typeof sha !== 'string' || !/^[a-f\d]{40,64}$/i.test(sha)) throw new Error('Missing or invalid PR commit');
  }
  // Disable rename detection so moving an input out of a suite still selects it.
  return execFileSync('git', ['diff', '--no-renames', '--name-only', '-z', `${base}...${head}`, '--'],
    { cwd, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'], maxBuffer: 16 * 1024 * 1024 }).split('\0').filter(Boolean);
}

export function selectChecks(eventName, event, diff = changedPaths) {
  if (eventName !== 'pull_request') return { ...all(), reason: 'full event' };
  const pr = event.pull_request;
  let result;
  let reason;
  try {
    if (!pr || typeof pr.draft !== 'boolean') throw new Error('Missing PR metadata');
    result = classifyPaths(diff(pr.base?.sha, pr.head?.sha));
    reason = 'PR changed paths';
  } catch (error) {
    result = all();
    reason = `full fallback: ${error.message}`;
  }
  if (pr?.draft === true) {
    result.lab = result.proofs = result.snippets = false;
    reason += '; draft: lightweight checks only';
  }
  return { ...result, reason };
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  let selected;
  try {
    const event = JSON.parse(fs.readFileSync(process.env.GITHUB_EVENT_PATH, 'utf8'));
    selected = selectChecks(process.env.GITHUB_EVENT_NAME, event);
  } catch (error) {
    selected = { ...all(), reason: `full fallback: ${error.message}` };
  }
  const outputs = Object.entries(selected).filter(([key]) => key !== 'reason')
    .map(([key, value]) => `${key}=${value}\n`).join('');
  if (process.env.GITHUB_OUTPUT) fs.appendFileSync(process.env.GITHUB_OUTPUT, outputs);
  console.log(JSON.stringify(selected, null, 2));
  if (process.env.GITHUB_STEP_SUMMARY) fs.appendFileSync(process.env.GITHUB_STEP_SUMMARY,
    `### CI selection\n\n${outputs.replaceAll('\n', '  \n')}\n${selected.reason.replace(/[\r\n]/g, ' ')}\n`);
}
