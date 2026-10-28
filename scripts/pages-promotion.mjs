// GitHub-authenticated run metadata is the trust boundary. No tree/ancestor fallback.
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export const artifactName = 'validated-queue-site';
export const workflowPath = '.github/workflows/deploy.yml';

export async function findCandidate({ event, ref, repository, sha, api, now = Date.now() }) {
  if (event !== 'push' || ref !== 'refs/heads/main') return { reason: 'fresh validation event' };
  if (!/^[\w.-]+\/[\w.-]+$/.test(repository) || !/^[a-f\d]{40}$/.test(sha))
    throw new Error('invalid repository or SHA');
  const prefix = `/repos/${repository}/actions`;
  const query = new URLSearchParams({
    event: 'merge_group',
    status: 'success',
    head_sha: sha,
    per_page: '100',
  });
  const { workflow_runs: runs } = await api(`${prefix}/workflows/deploy.yml/runs?${query}`);
  for (const run of runs) {
    if (
      run.path !== workflowPath ||
      run.event !== 'merge_group' ||
      run.head_sha !== sha ||
      run.status !== 'completed' ||
      run.conclusion !== 'success' ||
      run.repository?.full_name !== repository ||
      run.head_repository?.full_name !== repository ||
      !Number.isSafeInteger(run.id) ||
      run.id <= 0
    )
      continue;
    const { artifacts } = await api(`${prefix}/runs/${run.id}/artifacts?per_page=100`);
    const candidates = artifacts.filter(
      (artifact) =>
        artifact.name === artifactName &&
        artifact.expired === false &&
        Date.parse(artifact.expires_at) > now &&
        artifact.size_in_bytes > 0 &&
        Number.isSafeInteger(artifact.id) &&
        artifact.id > 0 &&
        /^sha256:[a-f\d]{64}$/.test(artifact.digest) &&
        artifact.workflow_run?.id === run.id &&
        artifact.workflow_run?.head_sha === sha &&
        artifact.workflow_run?.repository_id === run.repository.id &&
        artifact.workflow_run?.head_repository_id === run.repository.id,
    );
    if (candidates.length === 1)
      return { runId: run.id, artifactId: candidates[0].id, reason: 'exact-commit queue artifact' };
  }
  return { reason: 'no successful exact-commit queue artifact' };
}

export async function lookup(env = process.env, request = (...args) => fetch(...args)) {
  const api = async (route) => {
    const response = await request(`${env.GITHUB_API_URL || 'https://api.github.com'}${route}`, {
      headers: {
        Authorization: `Bearer ${env.GITHUB_TOKEN}`,
        Accept: 'application/vnd.github+json',
        'X-GitHub-Api-Version': '2026-03-10',
      },
      signal: AbortSignal.timeout(15000),
      redirect: 'error',
    });
    if (!response.ok) throw new Error(`GitHub lookup returned ${response.status}`);
    return response.json();
  };
  try {
    return await findCandidate({
      event: env.GITHUB_EVENT_NAME,
      ref: env.GITHUB_REF,
      repository: env.GITHUB_REPOSITORY,
      sha: env.GITHUB_SHA,
      api,
    });
  } catch {
    return { reason: 'queue lookup failed; full validation required' };
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const candidate = await lookup();
  const found = Boolean(candidate.artifactId);
  if (process.env.GITHUB_OUTPUT)
    fs.appendFileSync(
      process.env.GITHUB_OUTPUT,
      `found=${found}\nrun-id=${candidate.runId || ''}\nartifact-id=${candidate.artifactId || ''}\n`,
    );
  const message = found
    ? `Queue source run: ${candidate.runId}. ${candidate.reason}.`
    : `Full-build fallback: ${candidate.reason}.`;
  console.log(message);
  if (process.env.GITHUB_STEP_SUMMARY)
    fs.appendFileSync(process.env.GITHUB_STEP_SUMMARY, `### Pages promotion\n\n${message}\n`);
}
